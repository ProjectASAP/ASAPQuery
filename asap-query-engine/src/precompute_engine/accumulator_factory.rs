use crate::data_model::{AggregateCore, KeyByLabelValues, Measurement};
use crate::precompute_operators::{
    CountMinSketchAccumulator, DatasketchesKLLAccumulator, HydraKllSketchAccumulator,
    IncreaseAccumulator, MinMaxAccumulator, MultipleIncreaseAccumulator, MultipleMinMaxAccumulator,
    MultipleSumAccumulator, SumAccumulator,
};
use sketch_db_common::aggregation_config::AggregationConfig;

/// Trait for feeding samples into accumulators in the precompute engine.
///
/// This provides a uniform interface over all accumulator types so that the
/// worker loop doesn't need to know which concrete type it's dealing with.
pub trait AccumulatorUpdater: Send {
    /// Feed a single (value, timestamp_ms) pair — for SingleSubpopulation types.
    fn update_single(&mut self, value: f64, timestamp_ms: i64);

    /// Feed a keyed (key, value, timestamp_ms) triple — for MultipleSubpopulation types.
    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, timestamp_ms: i64);

    /// Extract the final accumulator as a boxed `AggregateCore`.
    fn take_accumulator(&mut self) -> Box<dyn AggregateCore>;

    /// Non-destructive read of the current accumulator state (clone without reset).
    /// Used by pane-based sliding windows to read shared panes.
    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore>;

    /// Reset internal state for reuse (avoids re-allocation).
    fn reset(&mut self);

    /// Whether this updater is keyed (MultipleSubpopulation).
    fn is_keyed(&self) -> bool;

    /// Estimated memory usage in bytes.
    fn memory_usage_bytes(&self) -> usize;
}

// ---------------------------------------------------------------------------
// SumAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct SumAccumulatorUpdater {
    acc: SumAccumulator,
}

impl SumAccumulatorUpdater {
    pub fn new() -> Self {
        Self {
            acc: SumAccumulator::new(),
        }
    }
}

impl AccumulatorUpdater for SumAccumulatorUpdater {
    fn update_single(&mut self, value: f64, _timestamp_ms: i64) {
        self.acc.update(value);
    }

    fn update_keyed(&mut self, _key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.update_single(value, timestamp_ms);
    }

    fn take_accumulator(&mut self) -> Box<dyn AggregateCore> {
        let result = Box::new(self.acc.clone());
        self.reset();
        result
    }

    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore> {
        Box::new(self.acc.clone())
    }

    fn reset(&mut self) {
        self.acc = SumAccumulator::new();
    }

    fn is_keyed(&self) -> bool {
        false
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<SumAccumulator>()
    }
}

// ---------------------------------------------------------------------------
// MinMaxAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct MinMaxAccumulatorUpdater {
    acc: MinMaxAccumulator,
    sub_type: String,
}

impl MinMaxAccumulatorUpdater {
    pub fn new(sub_type: String) -> Self {
        Self {
            acc: MinMaxAccumulator::new(sub_type.clone()),
            sub_type,
        }
    }
}

impl AccumulatorUpdater for MinMaxAccumulatorUpdater {
    fn update_single(&mut self, value: f64, _timestamp_ms: i64) {
        self.acc.update(value);
    }

    fn update_keyed(&mut self, _key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.update_single(value, timestamp_ms);
    }

    fn take_accumulator(&mut self) -> Box<dyn AggregateCore> {
        let result = Box::new(self.acc.clone());
        self.reset();
        result
    }

    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore> {
        Box::new(self.acc.clone())
    }

    fn reset(&mut self) {
        self.acc = MinMaxAccumulator::new(self.sub_type.clone());
    }

    fn is_keyed(&self) -> bool {
        false
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<MinMaxAccumulator>()
    }
}

// ---------------------------------------------------------------------------
// IncreaseAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct IncreaseAccumulatorUpdater {
    acc: Option<IncreaseAccumulator>,
}

impl IncreaseAccumulatorUpdater {
    pub fn new() -> Self {
        Self { acc: None }
    }
}

impl AccumulatorUpdater for IncreaseAccumulatorUpdater {
    fn update_single(&mut self, value: f64, timestamp_ms: i64) {
        let measurement = Measurement::new(value);
        match &mut self.acc {
            Some(acc) => acc.update(measurement, timestamp_ms),
            None => {
                self.acc = Some(IncreaseAccumulator::new(
                    measurement.clone(),
                    timestamp_ms,
                    measurement,
                    timestamp_ms,
                ));
            }
        }
    }

    fn update_keyed(&mut self, _key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.update_single(value, timestamp_ms);
    }

    fn take_accumulator(&mut self) -> Box<dyn AggregateCore> {
        let acc = self.acc.take().unwrap_or_else(|| {
            IncreaseAccumulator::new(Measurement::new(0.0), 0, Measurement::new(0.0), 0)
        });
        let result = Box::new(acc);
        self.reset();
        result
    }

    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore> {
        match &self.acc {
            Some(acc) => Box::new(acc.clone()),
            None => Box::new(IncreaseAccumulator::new(
                Measurement::new(0.0),
                0,
                Measurement::new(0.0),
                0,
            )),
        }
    }

    fn reset(&mut self) {
        self.acc = None;
    }

    fn is_keyed(&self) -> bool {
        false
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<Option<IncreaseAccumulator>>()
    }
}

// ---------------------------------------------------------------------------
// KllAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct KllAccumulatorUpdater {
    acc: DatasketchesKLLAccumulator,
    k: u16,
}

impl KllAccumulatorUpdater {
    pub fn new(k: u16) -> Self {
        Self {
            acc: DatasketchesKLLAccumulator::new(k),
            k,
        }
    }
}

impl AccumulatorUpdater for KllAccumulatorUpdater {
    fn update_single(&mut self, value: f64, _timestamp_ms: i64) {
        self.acc._update(value);
    }

    fn update_keyed(&mut self, _key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.update_single(value, timestamp_ms);
    }

    fn take_accumulator(&mut self) -> Box<dyn AggregateCore> {
        let result = Box::new(self.acc.clone());
        self.reset();
        result
    }

    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore> {
        Box::new(self.acc.clone())
    }

    fn reset(&mut self) {
        self.acc = DatasketchesKLLAccumulator::new(self.k);
    }

    fn is_keyed(&self) -> bool {
        false
    }

    fn memory_usage_bytes(&self) -> usize {
        // KLL sketch size is hard to estimate precisely; use a rough estimate
        std::mem::size_of::<DatasketchesKLLAccumulator>() + 4096
    }
}

// ---------------------------------------------------------------------------
// MultipleSumUpdater
// ---------------------------------------------------------------------------

pub struct MultipleSumUpdater {
    acc: MultipleSumAccumulator,
}

impl MultipleSumUpdater {
    pub fn new() -> Self {
        Self {
            acc: MultipleSumAccumulator::new(),
        }
    }
}

impl AccumulatorUpdater for MultipleSumUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        // Multiple-subpopulation — use update_keyed instead
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        self.acc.update(key.clone(), value);
    }

    fn take_accumulator(&mut self) -> Box<dyn AggregateCore> {
        let result = Box::new(self.acc.clone());
        self.reset();
        result
    }

    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore> {
        Box::new(self.acc.clone())
    }

    fn reset(&mut self) {
        self.acc = MultipleSumAccumulator::new();
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<MultipleSumAccumulator>()
            + self.acc.sums.len() * (std::mem::size_of::<KeyByLabelValues>() + 8)
    }
}

// ---------------------------------------------------------------------------
// MultipleMinMaxUpdater
// ---------------------------------------------------------------------------

pub struct MultipleMinMaxUpdater {
    acc: MultipleMinMaxAccumulator,
    sub_type: String,
}

impl MultipleMinMaxUpdater {
    pub fn new(sub_type: String) -> Self {
        Self {
            acc: MultipleMinMaxAccumulator::new(sub_type.clone()),
            sub_type,
        }
    }
}

impl AccumulatorUpdater for MultipleMinMaxUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        // Multiple-subpopulation — use update_keyed instead
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        self.acc.update(key.clone(), value);
    }

    fn take_accumulator(&mut self) -> Box<dyn AggregateCore> {
        let result = Box::new(self.acc.clone());
        self.reset();
        result
    }

    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore> {
        Box::new(self.acc.clone())
    }

    fn reset(&mut self) {
        self.acc = MultipleMinMaxAccumulator::new(self.sub_type.clone());
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<MultipleMinMaxAccumulator>()
            + self.acc.values.len() * (std::mem::size_of::<KeyByLabelValues>() + 8)
    }
}

// ---------------------------------------------------------------------------
// MultipleIncreaseUpdater
// ---------------------------------------------------------------------------

pub struct MultipleIncreaseUpdater {
    acc: MultipleIncreaseAccumulator,
}

impl MultipleIncreaseUpdater {
    pub fn new() -> Self {
        Self {
            acc: MultipleIncreaseAccumulator::new(),
        }
    }
}

impl AccumulatorUpdater for MultipleIncreaseUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        // Multiple-subpopulation — use update_keyed instead
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        let measurement = Measurement::new(value);
        // If key already exists, update it; otherwise create new
        if self.acc.increases.contains_key(key) {
            if let Some(existing) = self.acc.increases.get_mut(key) {
                existing.update(measurement, timestamp_ms);
            }
        } else {
            let new_acc = IncreaseAccumulator::new(
                measurement.clone(),
                timestamp_ms,
                measurement,
                timestamp_ms,
            );
            self.acc.update(key.clone(), new_acc);
        }
    }

    fn take_accumulator(&mut self) -> Box<dyn AggregateCore> {
        let result = Box::new(self.acc.clone());
        self.reset();
        result
    }

    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore> {
        Box::new(self.acc.clone())
    }

    fn reset(&mut self) {
        self.acc = MultipleIncreaseAccumulator::new();
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<MultipleIncreaseAccumulator>()
            + self.acc.increases.len()
                * (std::mem::size_of::<KeyByLabelValues>()
                    + std::mem::size_of::<IncreaseAccumulator>())
    }
}

// ---------------------------------------------------------------------------
// CmsAccumulatorUpdater (CountMinSketch)
// ---------------------------------------------------------------------------

pub struct CmsAccumulatorUpdater {
    acc: CountMinSketchAccumulator,
    row_num: usize,
    col_num: usize,
}

impl CmsAccumulatorUpdater {
    pub fn new(row_num: usize, col_num: usize) -> Self {
        Self {
            acc: CountMinSketchAccumulator::new(row_num, col_num),
            row_num,
            col_num,
        }
    }
}

impl AccumulatorUpdater for CmsAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        // CMS is keyed — use update_keyed
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        // CMS._update is private, so we access the sketch directly
        // We replicate the hash logic from CountMinSketchAccumulator._update
        let key_json = key.serialize_to_json();
        let key_values: Vec<String> = if let Some(obj) = key_json.as_object() {
            obj.values()
                .map(|v| v.as_str().unwrap_or("").to_string())
                .collect()
        } else {
            vec!["".to_string()]
        };
        let key_str = key_values.join(";");
        let key_bytes = key_str.as_bytes();

        for i in 0..self.acc.inner.row_num {
            let hash_value = xxhash_rust::xxh32::xxh32(key_bytes, i as u32);
            let col_index = (hash_value as usize) % self.acc.inner.col_num;
            self.acc.inner.sketch[i][col_index] += value;
        }
    }

    fn take_accumulator(&mut self) -> Box<dyn AggregateCore> {
        let result = Box::new(self.acc.clone());
        self.reset();
        result
    }

    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore> {
        Box::new(self.acc.clone())
    }

    fn reset(&mut self) {
        self.acc = CountMinSketchAccumulator::new(self.row_num, self.col_num);
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<CountMinSketchAccumulator>()
            + self.row_num * self.col_num * std::mem::size_of::<f64>()
    }
}

// ---------------------------------------------------------------------------
// HydraKllAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct HydraKllAccumulatorUpdater {
    acc: HydraKllSketchAccumulator,
    row_num: usize,
    col_num: usize,
    k: u16,
}

impl HydraKllAccumulatorUpdater {
    pub fn new(row_num: usize, col_num: usize, k: u16) -> Self {
        Self {
            acc: HydraKllSketchAccumulator::new(row_num, col_num, k),
            row_num,
            col_num,
            k,
        }
    }
}

impl AccumulatorUpdater for HydraKllAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        // HydraKLL is keyed — use update_keyed
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        self.acc.update(key, value);
    }

    fn take_accumulator(&mut self) -> Box<dyn AggregateCore> {
        let result = Box::new(self.acc.clone());
        self.reset();
        result
    }

    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore> {
        Box::new(self.acc.clone())
    }

    fn reset(&mut self) {
        self.acc = HydraKllSketchAccumulator::new(self.row_num, self.col_num, self.k);
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        // Rough estimate: each cell is a KLL sketch
        std::mem::size_of::<HydraKllSketchAccumulator>() + self.row_num * self.col_num * 4096
    }
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

/// Create an appropriate `AccumulatorUpdater` from an `AggregationConfig`.
pub fn create_accumulator_updater(config: &AggregationConfig) -> Box<dyn AccumulatorUpdater> {
    let agg_type = config.aggregation_type.as_str();
    let sub_type = config.aggregation_sub_type.as_str();

    match agg_type {
        "SingleSubpopulation" => match sub_type {
            "Sum" | "sum" => Box::new(SumAccumulatorUpdater::new()),
            "Min" | "min" => Box::new(MinMaxAccumulatorUpdater::new("min".to_string())),
            "Max" | "max" => Box::new(MinMaxAccumulatorUpdater::new("max".to_string())),
            "Increase" | "increase" => Box::new(IncreaseAccumulatorUpdater::new()),
            "DatasketchesKLL" | "datasketches_kll" | "KLL" | "kll" => {
                let k = config
                    .parameters
                    .get("k")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(200) as u16;
                Box::new(KllAccumulatorUpdater::new(k))
            }
            other => {
                tracing::warn!(
                    "Unknown SingleSubpopulation sub_type '{}', defaulting to Sum",
                    other
                );
                Box::new(SumAccumulatorUpdater::new())
            }
        },
        "MultipleSubpopulation" => match sub_type {
            "Sum" | "sum" => Box::new(MultipleSumUpdater::new()),
            "Min" | "min" => Box::new(MultipleMinMaxUpdater::new("min".to_string())),
            "Max" | "max" => Box::new(MultipleMinMaxUpdater::new("max".to_string())),
            "Increase" | "increase" => Box::new(MultipleIncreaseUpdater::new()),
            "CountMinSketch" | "count_min_sketch" | "CMS" | "cms" => {
                let row_num = config
                    .parameters
                    .get("row_num")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(4) as usize;
                let col_num = config
                    .parameters
                    .get("col_num")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1000) as usize;
                Box::new(CmsAccumulatorUpdater::new(row_num, col_num))
            }
            "HydraKLL" | "hydra_kll" => {
                let row_num = config
                    .parameters
                    .get("row_num")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(4) as usize;
                let col_num = config
                    .parameters
                    .get("col_num")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1000) as usize;
                let k = config
                    .parameters
                    .get("k")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(200) as u16;
                Box::new(HydraKllAccumulatorUpdater::new(row_num, col_num, k))
            }
            other => {
                tracing::warn!(
                    "Unknown MultipleSubpopulation sub_type '{}', defaulting to Sum",
                    other
                );
                Box::new(MultipleSumUpdater::new())
            }
        },
        // Top-level aggregation types (e.g. "DatasketchesKLL" directly in aggregationType)
        "DatasketchesKLL" | "datasketches_kll" | "KLL" | "kll" => {
            let k = config
                .parameters
                .get("K")
                .or_else(|| config.parameters.get("k"))
                .and_then(|v| v.as_u64())
                .unwrap_or(200) as u16;
            Box::new(KllAccumulatorUpdater::new(k))
        }
        "Sum" | "sum" => Box::new(SumAccumulatorUpdater::new()),
        "Min" | "min" => Box::new(MinMaxAccumulatorUpdater::new("min".to_string())),
        "Max" | "max" => Box::new(MinMaxAccumulatorUpdater::new("max".to_string())),
        "Increase" | "increase" => Box::new(IncreaseAccumulatorUpdater::new()),
        "CountMinSketch" | "count_min_sketch" | "CMS" | "cms" => {
            let row_num = config
                .parameters
                .get("row_num")
                .and_then(|v| v.as_u64())
                .unwrap_or(4) as usize;
            let col_num = config
                .parameters
                .get("col_num")
                .and_then(|v| v.as_u64())
                .unwrap_or(1000) as usize;
            Box::new(CmsAccumulatorUpdater::new(row_num, col_num))
        }
        "HydraKLL" | "hydra_kll" => {
            let row_num = config
                .parameters
                .get("row_num")
                .and_then(|v| v.as_u64())
                .unwrap_or(4) as usize;
            let col_num = config
                .parameters
                .get("col_num")
                .and_then(|v| v.as_u64())
                .unwrap_or(1000) as usize;
            let k = config
                .parameters
                .get("K")
                .or_else(|| config.parameters.get("k"))
                .and_then(|v| v.as_u64())
                .unwrap_or(200) as u16;
            Box::new(HydraKllAccumulatorUpdater::new(row_num, col_num, k))
        }
        other => {
            tracing::warn!(
                "Unknown aggregation_type '{}', defaulting to SingleSubpopulation Sum",
                other
            );
            Box::new(SumAccumulatorUpdater::new())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_updater() {
        let mut updater = SumAccumulatorUpdater::new();
        assert!(!updater.is_keyed());

        updater.update_single(1.0, 1000);
        updater.update_single(2.0, 2000);
        updater.update_single(3.0, 3000);

        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "SumAccumulator");
    }

    #[test]
    fn test_minmax_updater() {
        let mut updater = MinMaxAccumulatorUpdater::new("max".to_string());
        updater.update_single(5.0, 1000);
        updater.update_single(3.0, 2000);
        updater.update_single(7.0, 3000);

        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "MinMaxAccumulator");
    }

    #[test]
    fn test_increase_updater() {
        let mut updater = IncreaseAccumulatorUpdater::new();
        updater.update_single(10.0, 1000);
        updater.update_single(15.0, 2000);

        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "IncreaseAccumulator");
    }

    #[test]
    fn test_kll_updater() {
        let mut updater = KllAccumulatorUpdater::new(200);
        for i in 1..=10 {
            updater.update_single(i as f64, i * 1000);
        }

        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "DatasketchesKLLAccumulator");
    }

    #[test]
    fn test_multiple_sum_updater() {
        let mut updater = MultipleSumUpdater::new();
        assert!(updater.is_keyed());

        let key_a = KeyByLabelValues::new_with_labels(vec!["a".to_string()]);
        let key_b = KeyByLabelValues::new_with_labels(vec!["b".to_string()]);

        updater.update_keyed(&key_a, 1.0, 1000);
        updater.update_keyed(&key_b, 2.0, 2000);

        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "MultipleSumAccumulator");
    }

    #[test]
    fn test_reset_clears_state() {
        let mut updater = SumAccumulatorUpdater::new();
        updater.update_single(100.0, 1000);
        updater.reset();
        // After reset, should produce a fresh accumulator
        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "SumAccumulator");
    }
}
