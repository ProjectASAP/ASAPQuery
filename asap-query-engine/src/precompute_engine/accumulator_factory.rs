use crate::data_model::{AggregateCore, KeyByLabelValues, Measurement};
use crate::precompute_operators::{
    CountMinSketchAccumulator, DatasketchesKLLAccumulator, HydraKllSketchAccumulator,
    IncreaseAccumulator, MinMaxAccumulator, MultipleIncreaseAccumulator, MultipleMinMaxAccumulator,
    MultipleSumAccumulator, SumAccumulator,
};
use asap_types::aggregation_config::AggregationConfig;

/// Generate the two boilerplate clone-based `AccumulatorUpdater` methods
/// for updaters whose inner `acc` field implements `Clone + AggregateCore`.
/// Not applicable to `IncreaseAccumulatorUpdater` (its `acc` is `Option<_>`
/// with non-trivial `None` handling).
macro_rules! impl_clone_accumulator_methods {
    ($acc_field:ident) => {
        fn take_accumulator(&mut self) -> Box<dyn AggregateCore> {
            let result = Box::new(self.$acc_field.clone());
            self.reset();
            result
        }

        fn snapshot_accumulator(&self) -> Box<dyn AggregateCore> {
            Box::new(self.$acc_field.clone())
        }
    };
}

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

impl Default for SumAccumulatorUpdater {
    fn default() -> Self {
        Self::new()
    }
}

impl AccumulatorUpdater for SumAccumulatorUpdater {
    fn update_single(&mut self, value: f64, _timestamp_ms: i64) {
        self.acc.update(value);
    }

    fn update_keyed(&mut self, _key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.update_single(value, timestamp_ms);
    }

    impl_clone_accumulator_methods!(acc);

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

    impl_clone_accumulator_methods!(acc);

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

impl Default for IncreaseAccumulatorUpdater {
    fn default() -> Self {
        Self::new()
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

    // Hand-written: acc is Option<_> with non-trivial None handling.
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

    impl_clone_accumulator_methods!(acc);

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

impl Default for MultipleSumUpdater {
    fn default() -> Self {
        Self::new()
    }
}

impl AccumulatorUpdater for MultipleSumUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        // Multiple-subpopulation — use update_keyed instead
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        self.acc.update(key.clone(), value);
    }

    impl_clone_accumulator_methods!(acc);

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

    impl_clone_accumulator_methods!(acc);

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

impl Default for MultipleIncreaseUpdater {
    fn default() -> Self {
        Self::new()
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

    impl_clone_accumulator_methods!(acc);

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
        self.acc.inner.update(&key.to_semicolon_str(), value);
    }

    impl_clone_accumulator_methods!(acc);

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

    impl_clone_accumulator_methods!(acc);

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
// Config helpers
// ---------------------------------------------------------------------------

/// Return `true` if `config` produces a keyed (MultipleSubpopulation) updater,
/// without allocating an updater object.
///
/// **Contract:** this must agree with every concrete `AccumulatorUpdater::is_keyed()`
/// implementation. When a new accumulator type is added, update both here and
/// in the corresponding struct.
pub fn config_is_keyed(config: &AggregationConfig) -> bool {
    matches!(
        config.aggregation_type.as_str(),
        "MultipleSubpopulation"
            | "MultipleSum"
            | "multiple_sum"
            | "MultipleIncrease"
            | "multiple_increase"
            | "MultipleMinMax"
            | "multiple_min_max"
            | "CountMinSketch"
            | "count_min_sketch"
            | "CMS"
            | "cms"
            | "HydraKLL"
            | "hydra_kll"
    )
}

/// Extract the KLL `k` parameter. Capital `"K"` takes precedence over lowercase
/// `"k"` to match the convention used by the top-level aggregation type arms.
fn kll_k_param(config: &AggregationConfig) -> u16 {
    config
        .parameters
        .get("K")
        .or_else(|| config.parameters.get("k"))
        .and_then(|v| v.as_u64())
        .unwrap_or(200) as u16
}

/// Extract `(row_num, col_num)` for CMS / HydraKLL configs.
fn cms_params(config: &AggregationConfig) -> (usize, usize) {
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
    (row_num, col_num)
}

/// Extract `(row_num, col_num, k)` for HydraKLL configs.
fn hydra_kll_params(config: &AggregationConfig) -> (usize, usize, u16) {
    let (row_num, col_num) = cms_params(config);
    (row_num, col_num, kll_k_param(config))
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
                Box::new(KllAccumulatorUpdater::new(kll_k_param(config)))
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
                let (row_num, col_num) = cms_params(config);
                Box::new(CmsAccumulatorUpdater::new(row_num, col_num))
            }
            "HydraKLL" | "hydra_kll" => {
                let (row_num, col_num, k) = hydra_kll_params(config);
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
        // Top-level aggregation type aliases
        "DatasketchesKLL" | "datasketches_kll" | "KLL" | "kll" => {
            Box::new(KllAccumulatorUpdater::new(kll_k_param(config)))
        }
        "MultipleSum" | "multiple_sum" => Box::new(MultipleSumUpdater::new()),
        "MultipleIncrease" | "multiple_increase" => Box::new(MultipleIncreaseUpdater::new()),
        "MultipleMinMax" | "multiple_min_max" => Box::new(MultipleMinMaxUpdater::new(
            if sub_type.eq_ignore_ascii_case("max") {
                "max".to_string()
            } else {
                "min".to_string()
            },
        )),
        "Sum" | "sum" => Box::new(SumAccumulatorUpdater::new()),
        "Min" | "min" => Box::new(MinMaxAccumulatorUpdater::new("min".to_string())),
        "Max" | "max" => Box::new(MinMaxAccumulatorUpdater::new("max".to_string())),
        "Increase" | "increase" => Box::new(IncreaseAccumulatorUpdater::new()),
        "CountMinSketch" | "count_min_sketch" | "CMS" | "cms" => {
            let (row_num, col_num) = cms_params(config);
            Box::new(CmsAccumulatorUpdater::new(row_num, col_num))
        }
        "HydraKLL" | "hydra_kll" => {
            let (row_num, col_num, k) = hydra_kll_params(config);
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

    #[test]
    fn test_config_is_keyed() {
        use std::collections::HashMap;

        let make_config = |agg_type: &str, sub_type: &str| {
            AggregationConfig::new(
                1,
                agg_type.to_string(),
                sub_type.to_string(),
                HashMap::new(),
                promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
                promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
                promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
                String::new(),
                60,
                0,
                "tumbling".to_string(),
                "m".to_string(),
                "m".to_string(),
                None,
                None,
                None,
                None,
            )
        };

        // Non-keyed types
        assert!(!config_is_keyed(&make_config("SingleSubpopulation", "Sum")));
        assert!(!config_is_keyed(&make_config("Sum", "")));
        assert!(!config_is_keyed(&make_config("DatasketchesKLL", "")));
        assert!(!config_is_keyed(&make_config("KLL", "")));
        assert!(!config_is_keyed(&make_config("Increase", "")));

        // Keyed types
        assert!(config_is_keyed(&make_config(
            "MultipleSubpopulation",
            "Sum"
        )));
        assert!(config_is_keyed(&make_config("MultipleSum", "")));
        assert!(config_is_keyed(&make_config("MultipleIncrease", "")));
        assert!(config_is_keyed(&make_config("MultipleMinMax", "")));
        assert!(config_is_keyed(&make_config("CountMinSketch", "")));
        assert!(config_is_keyed(&make_config("CMS", "")));
        assert!(config_is_keyed(&make_config("HydraKLL", "")));

        // Verify agreement with updater.is_keyed()
        for (agg_type, sub_type) in &[
            ("SingleSubpopulation", "Sum"),
            ("MultipleSubpopulation", "Sum"),
            ("MultipleSum", ""),
            ("DatasketchesKLL", ""),
            ("CountMinSketch", ""),
        ] {
            let config = make_config(agg_type, sub_type);
            let updater = create_accumulator_updater(&config);
            assert_eq!(
                config_is_keyed(&config),
                updater.is_keyed(),
                "config_is_keyed disagrees with updater.is_keyed() for type={}",
                agg_type
            );
        }
    }

    #[test]
    fn test_kll_k_param_capital_k() {
        // SingleSubpopulation/KLL with capital "K" param should use it (not default to 200)
        use std::collections::HashMap;
        let mut params = HashMap::new();
        params.insert("K".to_string(), serde_json::Value::from(50_u64));
        let config = AggregationConfig::new(
            1,
            "SingleSubpopulation".to_string(),
            "DatasketchesKLL".to_string(),
            params,
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            60,
            0,
            "tumbling".to_string(),
            "m".to_string(),
            "m".to_string(),
            None,
            None,
            None,
            None,
        );
        let updater = create_accumulator_updater(&config);
        let acc = updater.snapshot_accumulator();
        let kll = acc
            .as_any()
            .downcast_ref::<crate::precompute_operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator>()
            .expect("should be KLL");
        assert_eq!(kll.inner.k, 50, "k should be 50 from capital-K param");
    }
}
