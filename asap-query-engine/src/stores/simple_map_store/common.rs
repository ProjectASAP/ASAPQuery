use crate::data_model::{AggregateCore, KeyByLabelValues};
use std::collections::{BTreeMap, BTreeSet, HashMap};
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

/// One epoch slot: holds up to `epoch_capacity` distinct time windows.
pub struct EpochData {
    /// Primary inverted index: MetricID → time-sorted aggregates.
    pub label_map: HashMap<MetricID, BTreeMap<TimestampRange, Vec<Arc<dyn AggregateCore>>>>,
    /// Reverse index: window → sorted Vec<MetricID> (Optimization 3).
    pub window_to_ids: HashMap<TimestampRange, Vec<MetricID>>,
    /// All distinct time windows in this epoch, sorted.
    pub time_ranges: BTreeSet<TimestampRange>,
}

impl EpochData {
    pub fn new() -> Self {
        Self {
            label_map: HashMap::new(),
            window_to_ids: HashMap::new(),
            time_ranges: BTreeSet::new(),
        }
    }

    pub fn window_count(&self) -> usize {
        self.time_ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.time_ranges.is_empty()
    }

    /// Insert (metric_id, range, aggregate) into this epoch.
    pub fn insert(
        &mut self,
        metric_id: MetricID,
        range: TimestampRange,
        agg: Arc<dyn AggregateCore>,
    ) {
        self.time_ranges.insert(range);
        self.label_map
            .entry(metric_id)
            .or_default()
            .entry(range)
            .or_default()
            .push(agg);
        // Maintain sorted Vec<MetricID> in reverse index
        let ids = self.window_to_ids.entry(range).or_default();
        let pos = ids.partition_point(|&id| id < metric_id);
        if ids.get(pos) != Some(&metric_id) {
            ids.insert(pos, metric_id);
        }
    }

    /// Remove windows from this epoch (ReadBased cleanup).
    pub fn remove_windows(&mut self, windows: &[TimestampRange]) {
        for &window in windows {
            self.time_ranges.remove(&window);
            let Some(ids) = self.window_to_ids.remove(&window) else {
                continue;
            };
            for metric_id in ids {
                let remove_label = if let Some(btree) = self.label_map.get_mut(&metric_id) {
                    btree.remove(&window);
                    btree.is_empty()
                } else {
                    false
                };
                if remove_label {
                    self.label_map.remove(&metric_id);
                }
            }
        }
    }

    /// Stream results matching [start, end] directly into `out` (grouped by MetricID),
    /// appending each matched window to `matched_windows` for read-count tracking.
    /// Avoids an intermediate Vec allocation compared to returning a flat list.
    pub fn range_query_into(
        &self,
        start: u64,
        end: u64,
        out: &mut MetricBucketMap,
        matched_windows: &mut Vec<TimestampRange>,
    ) {
        for (&metric_id, btree) in &self.label_map {
            for (&tr, aggs) in btree.range((start, 0)..=(end, u64::MAX)) {
                if tr.1 > end {
                    continue;
                }
                let slot = out.entry(metric_id).or_default();
                for agg in aggs {
                    slot.push((tr, Arc::clone(agg)));
                    matched_windows.push(tr);
                }
            }
        }
    }

    /// Collect results for an exact window match using the reverse index.
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
            if let Some(aggs) = self.label_map.get(&metric_id).and_then(|b| b.get(&range)) {
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
}
