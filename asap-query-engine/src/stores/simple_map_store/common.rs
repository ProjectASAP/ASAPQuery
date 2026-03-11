use crate::data_model::{AggregateCore, KeyByLabelValues};
use std::collections::{HashMap, HashSet};
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

/// Mutable (active) epoch: pure append-only insert, O(1) amortized.
///
/// Raw entries are stored in insertion order — no sorting, no deduplication, no index
/// maintenance during writes.  All ordering work is deferred to `seal()`, which is
/// called at most once per epoch (at rotation time).  This matches VictoriaMetrics'
/// rawRows → in-memory part pipeline.
///
/// Queries on the active epoch do a bounded linear scan (epoch size ≤ epoch_capacity ×
/// labels), which is acceptable because the vast majority of historical data lives in
/// sealed (already-sorted) epochs.
pub struct MutableEpoch {
    /// Append-only raw inserts.  Sorted only at seal() time.
    pub raw: Vec<(TimestampRange, MetricID, Arc<dyn AggregateCore>)>,
    /// Distinct windows for rotation threshold — O(1) insert, O(1) len.
    windows: HashSet<TimestampRange>,
    /// Epoch time bounds for O(1) skip check, updated incrementally on insert.
    min_start: Option<u64>,
    max_end: Option<u64>,
}

impl MutableEpoch {
    pub fn new() -> Self {
        Self {
            raw: Vec::new(),
            windows: HashSet::new(),
            min_start: None,
            max_end: None,
        }
    }

    pub fn window_count(&self) -> usize {
        self.windows.len()
    }

    /// Returns `(min_start, max_end)` across all windows, or `None` if empty.
    /// Used by callers for the epoch-skip check: `min_start > end || max_end < start`.
    pub fn time_bounds(&self) -> Option<(u64, u64)> {
        match (self.min_start, self.max_end) {
            (Some(s), Some(e)) => Some((s, e)),
            _ => None,
        }
    }

    /// O(1) amortized: Vec push + HashSet insert + two scalar comparisons.
    pub fn insert(
        &mut self,
        metric_id: MetricID,
        range: TimestampRange,
        agg: Arc<dyn AggregateCore>,
    ) {
        self.raw.push((range, metric_id, agg));
        self.windows.insert(range);
        self.min_start = Some(self.min_start.map_or(range.0, |m| m.min(range.0)));
        self.max_end = Some(self.max_end.map_or(range.1, |m| m.max(range.1)));
    }

    /// Consume this epoch and produce an immutable SealedEpoch by sorting in-place.
    /// O(M log M) where M = number of raw entries — paid once at rotation, not at query time.
    pub fn seal(self) -> SealedEpoch {
        let min_start = self.min_start;
        let max_end = self.max_end;
        let mut entries = self.raw;
        entries.sort_unstable_by_key(|(tr, metric_id, _)| (*tr, *metric_id));
        SealedEpoch {
            entries,
            min_start,
            max_end,
        }
    }

    /// Linear scan over raw entries for [start, end] — O(M) where M ≤ epoch_capacity × L.
    /// Acceptable because: (a) the epoch is bounded, (b) most data is in sealed epochs.
    pub fn range_query_into(
        &self,
        start: u64,
        end: u64,
        out: &mut MetricBucketMap,
        matched_windows: &mut Vec<TimestampRange>,
    ) {
        for (tr, metric_id, agg) in &self.raw {
            if tr.0 < start || tr.0 > end || tr.1 > end {
                continue;
            }
            out.entry(*metric_id)
                .or_default()
                .push((*tr, Arc::clone(agg)));
            matched_windows.push(*tr);
        }
    }

    /// Linear scan for exact window match — O(M), bounded.
    pub fn exact_query(
        &self,
        range: TimestampRange,
    ) -> Option<Vec<(MetricID, Arc<dyn AggregateCore>)>> {
        let mut out = Vec::new();
        for (tr, metric_id, agg) in &self.raw {
            if *tr == range {
                out.push((*metric_id, Arc::clone(agg)));
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
        let window_set: HashSet<TimestampRange> = windows.iter().copied().collect();
        self.raw.retain(|(tr, _, _)| !window_set.contains(tr));
        self.windows.retain(|tr| !window_set.contains(tr));
        // Recompute bounds (cleanup is rare, linear scan is fine).
        self.min_start = self.raw.iter().map(|(tr, _, _)| tr.0).min();
        self.max_end = self.raw.iter().map(|(tr, _, _)| tr.1).max();
    }
}

/// Sealed (immutable) epoch: flat sorted `Vec` for cache-friendly range scans.
///
/// Produced by `MutableEpoch::seal()`.  Entries are sorted by `(TimestampRange, MetricID)`:
/// all entries for the same window are contiguous, which is cache-friendly for both
/// range queries (binary-search start + linear scan) and exact queries.
pub struct SealedEpoch {
    /// Sorted by (TimestampRange, MetricID).
    pub entries: Vec<(TimestampRange, MetricID, Arc<dyn AggregateCore>)>,
    /// Precomputed for O(1) epoch-skip check.
    pub min_start: Option<u64>,
    pub max_end: Option<u64>,
}

impl SealedEpoch {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns `(min_start, max_end)`, or `None` if empty.
    pub fn time_bounds(&self) -> Option<(u64, u64)> {
        match (self.min_start, self.max_end) {
            (Some(s), Some(e)) => Some((s, e)),
            _ => None,
        }
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

    /// Remove specific windows (ReadBased cleanup).  Rebuilds Vec in one pass.
    pub fn remove_windows(&mut self, windows: &[TimestampRange]) {
        let window_set: HashSet<TimestampRange> = windows.iter().copied().collect();
        self.entries.retain(|(tr, _, _)| !window_set.contains(tr));
        self.min_start = self.entries.iter().map(|(tr, _, _)| tr.0).min();
        self.max_end = self.entries.iter().map(|(tr, _, _)| tr.1).max();
    }

    /// Deduplicated windows (entries sorted, so consecutive dupes are adjacent).
    /// Used to purge `read_counts` when this epoch is dropped.
    pub fn unique_windows(&self) -> Vec<TimestampRange> {
        let mut windows: Vec<TimestampRange> = self.entries.iter().map(|(tr, _, _)| *tr).collect();
        windows.dedup();
        windows
    }
}
