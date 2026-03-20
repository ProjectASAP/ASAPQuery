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
/// # Optimizations applied
///
/// **Opt 5 — Columnar storage**: timestamps, MetricIDs, and aggregates are kept in three
/// separate parallel arrays instead of one array of tuples.  The range-query hot loop only
/// scans `windows_col` (contiguous u64 pairs) and does not touch aggregate pointers unless a
/// window actually matches, cutting cache pressure significantly for sparse range queries.
///
/// **Opt 1 + 2 — Lazy offset index**: `window_to_ids` is built on the *first* `exact_query`
/// after any write batch and stores u32 column offsets rather than Arc clones.  Any `insert`
/// simply sets the field to `None` (one pointer-width write); there are no HashMap lookups,
/// no `HashSet::insert` calls for the index, and no atomic refcount bumps on the hot insert
/// path.  The index is rebuilt in O(M) on demand from `windows_col` alone.
///
/// **Opt 3 — Monotonic ingest fast path**: `last_window` tracks the most recently inserted
/// window.  Consecutive inserts to the same window (multiple label combinations for one time
/// bucket — the common case in ordered TSDB ingestion) skip the `windows_set` HashSet probe
/// entirely.
///
/// **Opt 6 — Pre-allocated column buffers**: `with_capacity(n)` reserves space upfront using
/// the previous epoch's entry count, avoiding Vec reallocation during the next epoch fill.
pub struct MutableEpoch {
    // Columnar storage: three parallel arrays (Opt 5)
    windows_col: Vec<TimestampRange>,
    metric_ids_col: Vec<MetricID>,
    aggregates_col: Vec<Arc<dyn AggregateCore>>,

    // Distinct-window count for epoch rotation threshold
    windows_set: HashSet<TimestampRange>,

    // Monotonic ingest fast path: skip HashSet probe for consecutive same-window inserts (Opt 3)
    last_window: Option<TimestampRange>,

    // Lazy offset index: built on first exact_query, invalidated on any insert (Opt 1 + 2).
    // Stores column indices (u32) instead of Arc clones — zero atomic ops during insert.
    window_to_ids: Option<HashMap<TimestampRange, Vec<u32>>>,

    /// Epoch time bounds for O(1) skip check, updated incrementally on insert.
    min_start: Option<u64>,
    max_end: Option<u64>,
}

impl MutableEpoch {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Pre-allocate column buffers with a capacity hint (Opt 6).
    /// Pass the previous epoch's `len()` to avoid reallocation during the next epoch fill.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            windows_col: Vec::with_capacity(cap),
            metric_ids_col: Vec::with_capacity(cap),
            aggregates_col: Vec::with_capacity(cap),
            windows_set: HashSet::new(),
            last_window: None,
            window_to_ids: None,
            min_start: None,
            max_end: None,
        }
    }

    pub fn window_count(&self) -> usize {
        self.windows_set.len()
    }

    /// Total raw entries across all windows and labels.
    pub fn len(&self) -> usize {
        self.windows_col.len()
    }

    /// Returns `(min_start, max_end)` across all windows, or `None` if empty.
    /// Used for the epoch-skip check: `min_start > end || max_end < start`.
    pub fn time_bounds(&self) -> Option<(u64, u64)> {
        match (self.min_start, self.max_end) {
            (Some(s), Some(e)) => Some((s, e)),
            _ => None,
        }
    }

    /// O(1) amortized insert: three column pushes + conditional HashSet insert + bounds update.
    ///
    /// No secondary-index maintenance and no Arc clone for any index.  The lazy `window_to_ids`
    /// is invalidated by setting it to `None` — a single pointer-width write with no HashMap or
    /// HashSet work.
    pub fn insert(
        &mut self,
        metric_id: MetricID,
        range: TimestampRange,
        agg: Arc<dyn AggregateCore>,
    ) {
        // Opt 3: skip HashSet probe when the incoming window equals the last inserted window.
        // Multiple label combinations arriving for the same time bucket (the common ordered-
        // ingest pattern) cost zero HashSet operations after the first.
        if self.last_window != Some(range) {
            self.windows_set.insert(range);
            self.last_window = Some(range);
        }

        // Opt 5: columnar append — no secondary index, no Arc clone
        self.windows_col.push(range);
        self.metric_ids_col.push(metric_id);
        self.aggregates_col.push(agg);

        // Opt 1: invalidate lazy index at zero cost
        self.window_to_ids = None;

        self.min_start = Some(self.min_start.map_or(range.0, |m| m.min(range.0)));
        self.max_end = Some(self.max_end.map_or(range.1, |m| m.max(range.1)));
    }

    /// Consume this epoch and produce an immutable SealedEpoch by sorting in-place.
    /// Zips the three columns into tuples and sorts — moves Arcs without cloning.
    /// O(M log M) paid once at rotation time, not at query time.
    pub fn seal(self) -> SealedEpoch {
        let min_start = self.min_start;
        let max_end = self.max_end;
        let mut entries: Vec<(TimestampRange, MetricID, Arc<dyn AggregateCore>)> = self
            .windows_col
            .into_iter()
            .zip(self.metric_ids_col)
            .zip(self.aggregates_col)
            .map(|((tr, mid), agg)| (tr, mid, agg))
            .collect();
        entries.sort_unstable_by_key(|(tr, metric_id, _)| (*tr, *metric_id));
        SealedEpoch {
            entries,
            min_start,
            max_end,
        }
    }

    /// Opt 5: scans only `windows_col` for time-range filtering — cache-friendly because
    /// only contiguous TimestampRange values are touched in the hot loop.  Aggregate pointers
    /// are chased only for entries that actually match the range.
    /// O(M) where M ≤ epoch_capacity × labels_per_window.
    pub fn range_query_into(
        &self,
        start: u64,
        end: u64,
        out: &mut MetricBucketMap,
        matched_windows: &mut Vec<TimestampRange>,
    ) {
        for (i, &tr) in self.windows_col.iter().enumerate() {
            if tr.0 < start || tr.0 > end || tr.1 > end {
                continue;
            }
            let metric_id = self.metric_ids_col[i];
            out.entry(metric_id)
                .or_default()
                .push((tr, Arc::clone(&self.aggregates_col[i])));
            matched_windows.push(tr);
        }
    }

    /// Opt 1 + 2: lazy exact match — O(m) after the index is built, O(M) to build once.
    ///
    /// The offset index (`HashMap<TimestampRange, Vec<u32>>`) is constructed from `windows_col`
    /// on the first call after any write batch, then cached.  Building it scans `windows_col`
    /// once with no Arc clones (only integer offsets are stored).  The index remains valid
    /// until the next `insert`, which sets `window_to_ids = None`.
    ///
    /// Takes `&mut self` because building the index mutates `window_to_ids`.
    /// Callers must hold exclusive (write) access to the containing epoch.
    pub fn exact_query(
        &mut self,
        range: TimestampRange,
    ) -> Option<Vec<(MetricID, Arc<dyn AggregateCore>)>> {
        if self.window_to_ids.is_none() {
            let mut idx: HashMap<TimestampRange, Vec<u32>> =
                HashMap::with_capacity(self.windows_set.len());
            for (i, &tr) in self.windows_col.iter().enumerate() {
                idx.entry(tr).or_default().push(i as u32);
            }
            self.window_to_ids = Some(idx);
        }
        let offsets = self.window_to_ids.as_ref().unwrap().get(&range)?;
        Some(
            offsets
                .iter()
                .map(|&i| {
                    let i = i as usize;
                    (self.metric_ids_col[i], Arc::clone(&self.aggregates_col[i]))
                })
                .collect(),
        )
    }

    /// Remove specific windows (ReadBased cleanup).
    /// Drains all three columns in lockstep — moves Arcs without cloning.
    pub fn remove_windows(&mut self, windows: &[TimestampRange]) {
        let window_set: HashSet<TimestampRange> = windows.iter().copied().collect();

        let old_windows = std::mem::take(&mut self.windows_col);
        let old_metrics = std::mem::take(&mut self.metric_ids_col);
        let old_aggs = std::mem::take(&mut self.aggregates_col);

        for ((tr, mid), agg) in old_windows.into_iter().zip(old_metrics).zip(old_aggs) {
            if !window_set.contains(&tr) {
                self.windows_col.push(tr);
                self.metric_ids_col.push(mid);
                self.aggregates_col.push(agg);
            }
        }

        for window in windows {
            self.windows_set.remove(window);
        }

        // Invalidate lazy index and monotonic fast-path hint.
        self.window_to_ids = None;
        self.last_window = None;

        // Recompute bounds (cleanup is rare; linear scan is fine).
        self.min_start = self.windows_col.iter().map(|tr| tr.0).min();
        self.max_end = self.windows_col.iter().map(|tr| tr.1).max();
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
