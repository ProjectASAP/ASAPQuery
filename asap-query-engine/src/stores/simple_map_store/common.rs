use crate::data_model::{AggregateCore, KeyByLabelValues};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub type MetricID = u32;
pub type EpochID = u64;
pub type TimestampRange = (u64, u64);
pub type MetricBucketMap = HashMap<MetricID, Vec<(TimestampRange, Arc<dyn AggregateCore>)>>;

/// Assigns a compact MetricID (u32) to each unique label combination.
/// Label strings stored once; all internal maps use MetricID (O(1) key ops).
pub struct InternTable {
    label_to_id: HashMap<Option<KeyByLabelValues>, MetricID>,
    id_to_label: Vec<Option<KeyByLabelValues>>,
}

impl InternTable {
    pub fn new() -> Self {
        Self {
            label_to_id: HashMap::new(),
            id_to_label: Vec::new(),
        }
    }

    /// Intern a label, assigning a new MetricID if first seen.
    /// Uses HashMap::entry to avoid double-hashing.
    pub fn intern(&mut self, label: Option<KeyByLabelValues>) -> MetricID {
        let next_id = self.id_to_label.len() as MetricID;
        match self.label_to_id.entry(label) {
            std::collections::hash_map::Entry::Occupied(e) => *e.get(),
            std::collections::hash_map::Entry::Vacant(e) => {
                self.id_to_label.push(e.key().clone());
                *e.insert(next_id)
            }
        }
    }

    /// O(1) resolution by MetricID.
    pub fn resolve(&self, id: MetricID) -> &Option<KeyByLabelValues> {
        &self.id_to_label[id as usize]
    }

    /// Number of interned labels.
    pub fn len(&self) -> usize {
        self.id_to_label.len()
    }
}

/// Mutable (active) epoch: accepts inserts.
///
/// Optimization 1: flat `HashMap<(MetricID, TimestampRange), _>` replaces the nested
///   `HashMap<MetricID, BTreeMap<TimestampRange, _>>`, giving O(1) inserts and lookups.
///
/// Optimization 3 (time-primary index): `window_to_ids` is a `BTreeMap` keyed by
///   TimestampRange so range queries scan windows in order rather than scanning every
///   label.  Each value is a sorted `Vec<MetricID>` (binary-search dedup on insert).
pub struct MutableEpoch {
    /// Primary storage: (MetricID, TimestampRange) → aggregates.
    pub data: HashMap<(MetricID, TimestampRange), Vec<Arc<dyn AggregateCore>>>,
    /// Time-primary index: window → sorted Vec<MetricID>.  BTreeMap enables O(log N)
    /// range scan without touching labels that have no data in the query window.
    pub window_to_ids: BTreeMap<TimestampRange, Vec<MetricID>>,
}

impl MutableEpoch {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            window_to_ids: BTreeMap::new(),
        }
    }

    pub fn window_count(&self) -> usize {
        self.window_to_ids.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.window_to_ids.is_empty()
    }

    pub fn min_tr(&self) -> Option<TimestampRange> {
        self.window_to_ids.keys().next().copied()
    }

    pub fn max_tr(&self) -> Option<TimestampRange> {
        self.window_to_ids.keys().next_back().copied()
    }

    pub fn insert(
        &mut self,
        metric_id: MetricID,
        range: TimestampRange,
        agg: Arc<dyn AggregateCore>,
    ) {
        self.data.entry((metric_id, range)).or_default().push(agg);
        // Maintain sorted Vec<MetricID> in time-primary index.
        let ids = self.window_to_ids.entry(range).or_default();
        let pos = ids.partition_point(|&id| id < metric_id);
        if ids.get(pos) != Some(&metric_id) {
            ids.insert(pos, metric_id);
        }
    }

    /// Seal this epoch into a cache-friendly flat sorted array.
    pub fn seal(self) -> SealedEpoch {
        let mut entries: Vec<(TimestampRange, MetricID, Arc<dyn AggregateCore>)> = self
            .data
            .into_iter()
            .flat_map(|((metric_id, tr), aggs)| {
                aggs.into_iter().map(move |agg| (tr, metric_id, agg))
            })
            .collect();
        // Sort by (TimestampRange, MetricID): windows contiguous, labels ordered within window.
        entries.sort_unstable_by_key(|(tr, metric_id, _)| (*tr, *metric_id));
        let min_tr = entries.first().map(|(tr, _, _)| *tr);
        let max_tr = entries.last().map(|(tr, _, _)| *tr);
        SealedEpoch {
            entries,
            min_tr,
            max_tr,
        }
    }

    /// Stream results for [start, end] into `out` using the time-primary BTreeMap index.
    /// Only visits labels that actually have data in matching windows — O(log N + actual_matches).
    pub fn range_query_into(
        &self,
        start: u64,
        end: u64,
        out: &mut MetricBucketMap,
        matched_windows: &mut Vec<TimestampRange>,
    ) {
        for (&tr, metric_ids) in self.window_to_ids.range((start, 0)..=(end, u64::MAX)) {
            if tr.1 > end {
                continue;
            }
            for &metric_id in metric_ids {
                if let Some(aggs) = self.data.get(&(metric_id, tr)) {
                    let slot = out.entry(metric_id).or_default();
                    for agg in aggs {
                        slot.push((tr, Arc::clone(agg)));
                        matched_windows.push(tr);
                    }
                }
            }
        }
    }

    /// Exact match for a single window using the time-primary index.
    pub fn exact_query(
        &self,
        range: TimestampRange,
    ) -> Option<Vec<(MetricID, Arc<dyn AggregateCore>)>> {
        let ids = self.window_to_ids.get(&range)?;
        if ids.is_empty() {
            return None;
        }
        let mut out = Vec::new();
        for &metric_id in ids {
            if let Some(aggs) = self.data.get(&(metric_id, range)) {
                for agg in aggs {
                    out.push((metric_id, Arc::clone(agg)));
                }
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    /// Remove specific windows (ReadBased cleanup).
    pub fn remove_windows(&mut self, windows: &[TimestampRange]) {
        for &window in windows {
            if let Some(ids) = self.window_to_ids.remove(&window) {
                for metric_id in ids {
                    self.data.remove(&(metric_id, window));
                }
            }
        }
    }
}

/// Sealed (immutable) epoch: flat sorted `Vec` for cache-friendly range scans.
///
/// Optimization 2: once an epoch is full and rotated, it is converted to a contiguous
/// array sorted by `(TimestampRange, MetricID)`.  Range queries use binary search to
/// find the start position and then do a linear scan — no pointer chasing through
/// nested HashMap/BTreeMap nodes.
pub struct SealedEpoch {
    /// Sorted by (TimestampRange, MetricID).  All entries for the same window are
    /// contiguous; within a window entries are ordered by MetricID.
    pub entries: Vec<(TimestampRange, MetricID, Arc<dyn AggregateCore>)>,
    /// Precomputed min/max for O(1) epoch-skip check.
    pub min_tr: Option<TimestampRange>,
    pub max_tr: Option<TimestampRange>,
}

impl SealedEpoch {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Binary-search start + linear scan — O(log N + actual_matches), cache-friendly.
    pub fn range_query_into(
        &self,
        start: u64,
        end: u64,
        out: &mut MetricBucketMap,
        matched_windows: &mut Vec<TimestampRange>,
    ) {
        let start_pos = self.entries.partition_point(|(tr, _, _)| tr.0 < start);
        for (tr, metric_id, agg) in &self.entries[start_pos..] {
            if tr.0 > end {
                break;
            }
            if tr.1 > end {
                continue;
            }
            out.entry(*metric_id)
                .or_default()
                .push((*tr, Arc::clone(agg)));
            matched_windows.push(*tr);
        }
    }

    /// Binary-search exact window match — O(log N + m) where m = labels in that window.
    pub fn exact_query(
        &self,
        range: TimestampRange,
    ) -> Option<Vec<(MetricID, Arc<dyn AggregateCore>)>> {
        let start_pos = self.entries.partition_point(|(tr, _, _)| *tr < range);
        let mut out = Vec::new();
        for (tr, metric_id, agg) in &self.entries[start_pos..] {
            if *tr != range {
                break;
            }
            out.push((*metric_id, Arc::clone(agg)));
        }
        if out.is_empty() {
            None
        } else {
            Some(out)
        }
    }

    /// Remove specific windows (ReadBased cleanup).  Rebuilds the Vec in one pass.
    pub fn remove_windows(&mut self, windows: &[TimestampRange]) {
        let window_set: std::collections::HashSet<TimestampRange> =
            windows.iter().copied().collect();
        self.entries.retain(|(tr, _, _)| !window_set.contains(tr));
        self.min_tr = self.entries.first().map(|(tr, _, _)| *tr);
        self.max_tr = self.entries.last().map(|(tr, _, _)| *tr);
    }

    /// Deduplicated windows (entries are sorted so consecutive dupes are adjacent).
    /// Used to purge `read_counts` when this epoch is dropped.
    pub fn unique_windows(&self) -> Vec<TimestampRange> {
        let mut windows: Vec<TimestampRange> = self.entries.iter().map(|(tr, _, _)| *tr).collect();
        windows.dedup();
        windows
    }
}
