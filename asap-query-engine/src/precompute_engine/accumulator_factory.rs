use crate::data_model::{AggregateCore, AggregationType, KeyByLabelValues, Measurement};
use crate::precompute_operators::{
    CountMinSketchAccumulator, CountMinSketchWithHeapAccumulator, DatasketchesKLLAccumulator,
    DeltaSetAggregatorAccumulator, HllAccumulator, HydraKllSketchAccumulator, IncreaseAccumulator,
    MinMaxAccumulator, MultipleIncreaseAccumulator, MultipleMinMaxAccumulator,
    MultipleSumAccumulator, SetAggregatorAccumulator, SumAccumulator, DEFAULT_HLL_PRECISION,
};
use asap_types::aggregation_config::AggregationConfig;

/// Generate the boilerplate `AccumulatorUpdater` extraction methods
/// (`take_accumulator`/`snapshot_accumulator` clone, `into_accumulator` moves)
/// for updaters whose inner `acc` field implements `Clone + AggregateCore`.
/// Not applicable to `IncreaseAccumulatorUpdater` (its `acc` is `Option<_>`
/// with non-trivial `None` handling).
macro_rules! impl_accumulator_methods {
    ($acc_field:ident) => {
        fn take_accumulator(&mut self) -> Box<dyn AggregateCore> {
            let result = Box::new(self.$acc_field.clone());
            self.reset();
            result
        }

        fn snapshot_accumulator(&self) -> Box<dyn AggregateCore> {
            Box::new(self.$acc_field.clone())
        }

        fn into_accumulator(self: Box<Self>) -> Box<dyn AggregateCore> {
            // Consume the updater and MOVE the accumulator out — no clone.
            // Avoids the expensive `Clone` (a full msgpack serialize/deserialize
            // round-trip for sketch accumulators) when a pane is evicted at
            // window close.
            let this = *self;
            Box::new(this.$acc_field)
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

    /// Feed a keyed (key, value, timestamp_ms) triple — for keyed aggregation types.
    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, timestamp_ms: i64);

    /// Extract the final accumulator as a boxed `AggregateCore`.
    fn take_accumulator(&mut self) -> Box<dyn AggregateCore>;

    /// Non-destructive read of the current accumulator state (clone without reset).
    /// Used by pane-based sliding windows to read shared panes.
    fn snapshot_accumulator(&self) -> Box<dyn AggregateCore>;

    /// Consume the updater and return its accumulator BY MOVE, avoiding the
    /// `Clone` that `take_accumulator`/`snapshot_accumulator` pay (for sketch
    /// accumulators that clone is a full msgpack serialize/deserialize
    /// round-trip). Used by `merge_panes_for_window` when a pane is evicted at
    /// window close. Default falls back to a clone for updaters that can't
    /// cheaply move their inner accumulator out.
    fn into_accumulator(self: Box<Self>) -> Box<dyn AggregateCore> {
        self.snapshot_accumulator()
    }

    /// Reset internal state for reuse (avoids re-allocation).
    fn reset(&mut self);

    /// Whether this updater is keyed (multi-population or key-tracking).
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

    impl_accumulator_methods!(acc);

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
    is_max: bool,
}

impl MinMaxAccumulatorUpdater {
    pub fn new(is_max: bool) -> Self {
        Self {
            acc: if is_max {
                MinMaxAccumulator::new_max()
            } else {
                MinMaxAccumulator::new_min()
            },
            is_max,
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

    impl_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = if self.is_max {
            MinMaxAccumulator::new_max()
        } else {
            MinMaxAccumulator::new_min()
        };
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

    // Hand-written: consume the updater and MOVE the accumulator out (no clone),
    // mirroring `take_accumulator`'s `Option::take`. Overriding the default
    // (which clones via `snapshot_accumulator`) keeps this type consistent with
    // the macro-generated updaters at window close.
    fn into_accumulator(self: Box<Self>) -> Box<dyn AggregateCore> {
        let this = *self;
        Box::new(this.acc.unwrap_or_else(|| {
            IncreaseAccumulator::new(Measurement::new(0.0), 0, Measurement::new(0.0), 0)
        }))
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
        self.acc.update(value);
    }

    fn update_keyed(&mut self, _key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.update_single(value, timestamp_ms);
    }

    impl_accumulator_methods!(acc);

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
// HllAccumulatorUpdater
// ---------------------------------------------------------------------------

/// Updater for `AggregationType::HLL`. Single-population per grouping key —
/// behaves like `KllAccumulatorUpdater` from the worker's perspective: feed
/// raw f64 values, ignore the key argument. Internally hashes each value's
/// little-endian bytes into the wrapped HLL sketch.
pub struct HllAccumulatorUpdater {
    acc: HllAccumulator,
    precision: u32,
}

impl HllAccumulatorUpdater {
    pub fn new(precision: u32) -> Self {
        Self {
            acc: HllAccumulator::new(precision),
            precision,
        }
    }
}

impl AccumulatorUpdater for HllAccumulatorUpdater {
    fn update_single(&mut self, value: f64, _timestamp_ms: i64) {
        self.acc.update(value);
    }

    fn update_keyed(&mut self, _key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        self.update_single(value, timestamp_ms);
    }

    impl_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = HllAccumulator::new(self.precision);
    }

    fn is_keyed(&self) -> bool {
        false
    }

    fn memory_usage_bytes(&self) -> usize {
        // 1 byte per register; register count = 2^precision. Add a small fixed
        // overhead for the HllSketch wrapper (variant, precision, HIP fields).
        let registers = 1usize << self.precision;
        std::mem::size_of::<HllAccumulator>() + registers
    }
}

// ---------------------------------------------------------------------------
// SetAggregatorUpdater
// ---------------------------------------------------------------------------

/// Updater for `AggregationType::SetAggregator`, which records the distinct
/// aggregation keys present in the current window.
pub struct SetAggregatorUpdater {
    acc: SetAggregatorAccumulator,
}

impl SetAggregatorUpdater {
    pub fn new() -> Self {
        Self {
            acc: SetAggregatorAccumulator::new(),
        }
    }
}

impl Default for SetAggregatorUpdater {
    fn default() -> Self {
        Self::new()
    }
}

impl AccumulatorUpdater for SetAggregatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, _value: f64, _timestamp_ms: i64) {
        self.acc.add_key(key.clone());
    }

    impl_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = SetAggregatorAccumulator::new();
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<SetAggregatorAccumulator>()
            + self.acc.added.len() * std::mem::size_of::<KeyByLabelValues>()
    }
}

// ---------------------------------------------------------------------------
// DeltaSetAggregatorUpdater
// ---------------------------------------------------------------------------

/// Updater for `AggregationType::DeltaSetAggregator`, which records keys observed
/// during the current window. The worker's window-finalization step compares that
/// population with the previous window to produce added and removed keys; the
/// accumulator merge path only preserves the correct state when delta buckets are
/// combined.
pub struct DeltaSetAggregatorUpdater {
    acc: DeltaSetAggregatorAccumulator,
}

impl DeltaSetAggregatorUpdater {
    pub fn new() -> Self {
        Self {
            acc: DeltaSetAggregatorAccumulator::new(),
        }
    }
}

impl Default for DeltaSetAggregatorUpdater {
    fn default() -> Self {
        Self::new()
    }
}

impl AccumulatorUpdater for DeltaSetAggregatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, _value: f64, _timestamp_ms: i64) {
        self.acc.add_key(key.clone());
    }

    impl_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = DeltaSetAggregatorAccumulator::new();
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<DeltaSetAggregatorAccumulator>()
            + (self.acc.added.len() + self.acc.removed.len())
                * std::mem::size_of::<KeyByLabelValues>()
    }
}

// ---------------------------------------------------------------------------
// MultipleSumAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct MultipleSumAccumulatorUpdater {
    acc: MultipleSumAccumulator,
}

impl MultipleSumAccumulatorUpdater {
    pub fn new() -> Self {
        Self {
            acc: MultipleSumAccumulator::new(),
        }
    }
}

impl Default for MultipleSumAccumulatorUpdater {
    fn default() -> Self {
        Self::new()
    }
}

impl AccumulatorUpdater for MultipleSumAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        self.acc.update(key.clone(), value);
    }

    impl_accumulator_methods!(acc);

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
// MultipleMinMaxAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct MultipleMinMaxAccumulatorUpdater {
    acc: MultipleMinMaxAccumulator,
    is_max: bool,
}

impl MultipleMinMaxAccumulatorUpdater {
    pub fn new(is_max: bool) -> Self {
        Self {
            acc: if is_max {
                MultipleMinMaxAccumulator::new_max()
            } else {
                MultipleMinMaxAccumulator::new_min()
            },
            is_max,
        }
    }
}

impl AccumulatorUpdater for MultipleMinMaxAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        self.acc.update(key.clone(), value);
    }

    impl_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc = if self.is_max {
            MultipleMinMaxAccumulator::new_max()
        } else {
            MultipleMinMaxAccumulator::new_min()
        };
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
// MultipleIncreaseAccumulatorUpdater
// ---------------------------------------------------------------------------

pub struct MultipleIncreaseAccumulatorUpdater {
    acc: MultipleIncreaseAccumulator,
}

impl MultipleIncreaseAccumulatorUpdater {
    pub fn new() -> Self {
        Self {
            acc: MultipleIncreaseAccumulator::new(),
        }
    }
}

impl Default for MultipleIncreaseAccumulatorUpdater {
    fn default() -> Self {
        Self::new()
    }
}

impl AccumulatorUpdater for MultipleIncreaseAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, timestamp_ms: i64) {
        let measurement = Measurement::new(value);
        match self.acc.increases.entry(key.clone()) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                e.get_mut().update(measurement, timestamp_ms);
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(IncreaseAccumulator::new(
                    measurement.clone(),
                    timestamp_ms,
                    measurement,
                    timestamp_ms,
                ));
            }
        }
    }

    impl_accumulator_methods!(acc);

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
    count_events: bool,
}

impl CmsAccumulatorUpdater {
    pub fn new(row_num: usize, col_num: usize, count_events: bool) -> Self {
        Self {
            acc: CountMinSketchAccumulator::new(row_num, col_num),
            row_num,
            col_num,
            count_events,
        }
    }
}

impl AccumulatorUpdater for CmsAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        let weight = if self.count_events { 1.0 } else { value };
        self.acc.inner.update(&key.to_semicolon_str(), weight);
    }

    impl_accumulator_methods!(acc);

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
// CmsWithHeapAccumulatorUpdater (CountMinSketchWithHeap — top-k)
// ---------------------------------------------------------------------------

/// Keyed updater backing `Statistic::Topk`. Wraps a `CountMinSketchWithHeap`
/// (CMS + heavy-hitter heap) so the query engine can enumerate the top-k keys
/// from the heap and read each key's frequency estimate from the sketch.
///
/// `count_events` selects the per-sample weight fed into the sketch:
///   * `true`  → weight 1 per observation (COUNT semantics, e.g. `COUNT(pkt_len)`),
///   * `false` → the sample value itself (SUM-of-value semantics).
pub struct CmsWithHeapAccumulatorUpdater {
    acc: CountMinSketchWithHeapAccumulator,
    row_num: usize,
    col_num: usize,
    heap_size: usize,
    count_events: bool,
}

impl CmsWithHeapAccumulatorUpdater {
    pub fn new(row_num: usize, col_num: usize, heap_size: usize, count_events: bool) -> Self {
        Self {
            acc: CountMinSketchWithHeapAccumulator::new(row_num, col_num, heap_size),
            row_num,
            col_num,
            heap_size,
            count_events,
        }
    }
}

impl AccumulatorUpdater for CmsWithHeapAccumulatorUpdater {
    fn update_single(&mut self, _value: f64, _timestamp_ms: i64) {
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        let weight = if self.count_events { 1.0 } else { value };
        self.acc.inner.update(&key.to_semicolon_str(), weight);
    }

    impl_accumulator_methods!(acc);

    fn reset(&mut self) {
        self.acc =
            CountMinSketchWithHeapAccumulator::new(self.row_num, self.col_num, self.heap_size);
    }

    fn is_keyed(&self) -> bool {
        true
    }

    fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<CountMinSketchWithHeapAccumulator>()
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
        debug_assert!(
            false,
            "update_single called on keyed updater; use update_keyed"
        );
    }

    fn update_keyed(&mut self, key: &KeyByLabelValues, value: f64, _timestamp_ms: i64) {
        self.acc.update(key, value);
    }

    impl_accumulator_methods!(acc);

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

/// Return `true` if `config` produces a keyed (multi-population or key-tracking) updater,
/// without allocating an updater object.
///
/// **Contract:** this must agree with every concrete `AccumulatorUpdater::is_keyed()`
/// implementation. When a new accumulator type is added, update both here and
/// in the corresponding struct.
pub fn config_is_keyed(config: &AggregationConfig) -> bool {
    matches!(
        config.aggregation_type,
        AggregationType::MultipleSubpopulation
            | AggregationType::MultipleSum
            | AggregationType::MultipleIncrease
            | AggregationType::MultipleMinMax
            | AggregationType::CountMinSketch
            | AggregationType::CountMinSketchWithHeap
            | AggregationType::HydraKLL
            | AggregationType::SetAggregator
            | AggregationType::DeltaSetAggregator
    )
}

/// Extract the KLL `k` parameter. Capital `"K"` takes precedence over lowercase
/// `"k"` to match the convention used by the top-level aggregation type arms.
fn kll_k_param(config: &AggregationConfig) -> Result<u16, String> {
    config
        .parameters
        .get("K")
        .or_else(|| config.parameters.get("k"))
        .and_then(|v| v.as_u64())
        .and_then(|v| u16::try_from(v).ok())
        .ok_or_else(|| "KLL config missing required parameter (tried: K, k)".to_string())
}

/// Extract `(row_num, col_num)` for CMS / HydraKLL configs.
///
/// Accepts the planner-canonical `depth`/`width` names first, then falls back
/// to the `row_num`/`col_num` aliases — mirroring `cms_heap_params()`.
fn cms_params(config: &AggregationConfig) -> Result<(usize, usize), String> {
    let read = |names: &[&str]| -> Result<usize, String> {
        names
            .iter()
            .find_map(|n| config.parameters.get(*n).and_then(|v| v.as_u64()))
            .map(|v| v as usize)
            .ok_or_else(|| {
                format!(
                    "CMS config missing required parameter (tried: {})",
                    names.join(", ")
                )
            })
    };
    let row_num = read(&["depth", "row_num"])?;
    let col_num = read(&["width", "col_num"])?;
    Ok((row_num, col_num))
}

/// Resolve the weighting semantics for a plain Count-Min Sketch.
///
/// Unlike the heap variant, plain CMS uses `aggregation_sub_type` to
/// distinguish approximate SUM from approximate COUNT. Do not silently
/// default malformed configs: the wrong weighting produces plausible but
/// incorrect results.
fn cms_count_events_for_sub_type(sub_type: &str) -> Result<bool, String> {
    if sub_type.eq_ignore_ascii_case("count") {
        Ok(true)
    } else if sub_type.eq_ignore_ascii_case("sum") {
        Ok(false)
    } else {
        Err(format!(
            "CountMinSketch requires aggregation_sub_type 'sum' or 'count', got '{sub_type}'"
        ))
    }
}

/// Validate the aggregation subtype for a heap-backed Count-Min Sketch.
fn validate_cms_with_heap_sub_type(sub_type: &str) -> Result<(), String> {
    if sub_type.eq_ignore_ascii_case("topk") {
        Ok(())
    } else {
        Err(format!(
            "CountMinSketchWithHeap requires aggregation_sub_type 'topk', got '{sub_type}'"
        ))
    }
}

/// Extract `(row_num, col_num, k)` for HydraKLL configs.
fn hydra_kll_params(config: &AggregationConfig) -> Result<(usize, usize, u16), String> {
    let (row_num, col_num) = cms_params(config)?;
    Ok((row_num, col_num, kll_k_param(config)?))
}

/// Extract `(row_num, col_num, heap_size)` for CountMinSketchWithHeap configs.
///
/// Accepts the planner/Arroyo-canonical `depth`/`width`/`heapsize` names first,
/// then falls back to the `row_num`/`col_num`/`heap_size` aliases. All three
/// parameters are required — the planner always emits them and their absence
/// indicates a malformed config.
fn cms_heap_params(config: &AggregationConfig) -> Result<(usize, usize, usize), String> {
    let read = |names: &[&str]| -> Result<usize, String> {
        names
            .iter()
            .find_map(|n| config.parameters.get(*n).and_then(|v| v.as_u64()))
            .map(|v| v as usize)
            .ok_or_else(|| {
                format!(
                    "CountMinSketchWithHeap config missing required parameter (tried: {})",
                    names.join(", ")
                )
            })
    };
    let row_num = read(&["depth", "row_num"])?;
    let col_num = read(&["width", "col_num"])?;
    let heap_size = read(&["heapsize", "heap_size"])?;
    Ok((row_num, col_num, heap_size))
}

/// Whether a CountMinSketchWithHeap config should count events (weight 1 per
/// observation, COUNT semantics) rather than summing the sample value.
/// Defaults to `true` so `COUNT(...)` top-k works out of the box.
fn cms_count_events(config: &AggregationConfig) -> Result<bool, String> {
    match config.parameters.get("count_events") {
        None => Ok(true),
        Some(value) => value.as_bool().ok_or_else(|| {
            format!(
                "CountMinSketchWithHeap parameter 'count_events' must be a boolean, got {value}"
            )
        }),
    }
}

/// Extract the HLL `precision` parameter from a config. Falls back to
/// `DEFAULT_HLL_PRECISION` (14) when absent or non-numeric. The valid range is
/// 4..=18 per the underlying `HllSketch` storage; out-of-range values are
/// clamped and warned about so a typo doesn't crash the streaming worker.
fn hll_precision_param(config: &AggregationConfig) -> u32 {
    let raw = config
        .parameters
        .get("precision")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32);
    match raw {
        Some(p) if (4..=18).contains(&p) => p,
        Some(p) => {
            tracing::warn!(
                "HLL precision {p} is out of range (4..=18); using default {DEFAULT_HLL_PRECISION}"
            );
            DEFAULT_HLL_PRECISION
        }
        None => DEFAULT_HLL_PRECISION,
    }
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

/// Create an appropriate `AccumulatorUpdater` from an `AggregationConfig`.
///
/// Returns `Err` if the config is of a type that requires specific parameters
/// (e.g. `CountMinSketchWithHeap`, `CountMinSketch`, `HydraKLL`, KLL variants)
/// but those parameters are absent or invalid.
pub fn create_accumulator_updater(
    config: &AggregationConfig,
) -> Result<Box<dyn AccumulatorUpdater>, String> {
    let sub_type = config.aggregation_sub_type.as_str();

    match config.aggregation_type {
        AggregationType::SingleSubpopulation => match sub_type {
            "Sum" | "sum" => Ok(Box::new(SumAccumulatorUpdater::new())),
            "Min" | "min" => Ok(Box::new(MinMaxAccumulatorUpdater::new(false))),
            "Max" | "max" => Ok(Box::new(MinMaxAccumulatorUpdater::new(true))),
            "Increase" | "increase" => Ok(Box::new(IncreaseAccumulatorUpdater::new())),
            "DatasketchesKLL" | "datasketches_kll" | "KLL" | "kll" => {
                Ok(Box::new(KllAccumulatorUpdater::new(kll_k_param(config)?)))
            }
            other => Err(format!("Unknown SingleSubpopulation sub_type '{other}'")),
        },
        AggregationType::MultipleSubpopulation => match sub_type {
            "Sum" | "sum" => Ok(Box::new(MultipleSumAccumulatorUpdater::new())),
            "Min" | "min" => Ok(Box::new(MultipleMinMaxAccumulatorUpdater::new(false))),
            "Max" | "max" => Ok(Box::new(MultipleMinMaxAccumulatorUpdater::new(true))),
            "Increase" | "increase" => Ok(Box::new(MultipleIncreaseAccumulatorUpdater::new())),
            "CountMinSketch" | "count_min_sketch" | "CMS" | "cms" => {
                let (row_num, col_num) = cms_params(config)?;
                Ok(Box::new(CmsAccumulatorUpdater::new(
                    row_num, col_num, false,
                )))
            }
            "HydraKLL" | "hydra_kll" => {
                let (row_num, col_num, k) = hydra_kll_params(config)?;
                Ok(Box::new(HydraKllAccumulatorUpdater::new(
                    row_num, col_num, k,
                )))
            }
            other => Err(format!("Unknown MultipleSubpopulation sub_type '{other}'")),
        },
        AggregationType::DatasketchesKLL => {
            Ok(Box::new(KllAccumulatorUpdater::new(kll_k_param(config)?)))
        }
        AggregationType::MultipleSum => Ok(Box::new(MultipleSumAccumulatorUpdater::new())),
        AggregationType::MultipleIncrease => {
            Ok(Box::new(MultipleIncreaseAccumulatorUpdater::new()))
        }
        AggregationType::MultipleMinMax => Ok(Box::new(MultipleMinMaxAccumulatorUpdater::new(
            sub_type.eq_ignore_ascii_case("max"),
        ))),
        AggregationType::Sum => Ok(Box::new(SumAccumulatorUpdater::new())),
        AggregationType::MinMax => Ok(Box::new(MinMaxAccumulatorUpdater::new(
            sub_type.eq_ignore_ascii_case("max"),
        ))),
        AggregationType::Increase => Ok(Box::new(IncreaseAccumulatorUpdater::new())),
        AggregationType::CountMinSketch => {
            let (row_num, col_num) = cms_params(config)?;
            let count_events = cms_count_events_for_sub_type(sub_type)?;
            Ok(Box::new(CmsAccumulatorUpdater::new(
                row_num,
                col_num,
                count_events,
            )))
        }
        AggregationType::CountMinSketchWithHeap => {
            validate_cms_with_heap_sub_type(sub_type)?;
            let (row_num, col_num, heap_size) = cms_heap_params(config)?;
            Ok(Box::new(CmsWithHeapAccumulatorUpdater::new(
                row_num,
                col_num,
                heap_size,
                cms_count_events(config)?,
            )))
        }
        AggregationType::HydraKLL => {
            let (row_num, col_num, k) = hydra_kll_params(config)?;
            Ok(Box::new(HydraKllAccumulatorUpdater::new(
                row_num, col_num, k,
            )))
        }
        AggregationType::HLL => Ok(Box::new(HllAccumulatorUpdater::new(hll_precision_param(
            config,
        )))),
        AggregationType::SetAggregator => Ok(Box::new(SetAggregatorUpdater::new())),
        AggregationType::DeltaSetAggregator => Ok(Box::new(DeltaSetAggregatorUpdater::new())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::enums::{AggregationType, WindowType};

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
        let mut updater = MinMaxAccumulatorUpdater::new(true);
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
        let mut updater = MultipleSumAccumulatorUpdater::new();
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

        let make_config = |agg_type: AggregationType, sub_type: &str| {
            AggregationConfig::new(
                1,
                agg_type,
                sub_type.to_string(),
                HashMap::new(),
                promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
                promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
                promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
                String::new(),
                60_000,
                0,
                WindowType::Tumbling,
                "m".to_string(),
                "m".to_string(),
                None,
                None,
                None,
                None,
            )
        };

        // Non-keyed types
        assert!(!config_is_keyed(&make_config(
            AggregationType::SingleSubpopulation,
            "Sum"
        )));
        assert!(!config_is_keyed(&make_config(AggregationType::Sum, "")));
        assert!(!config_is_keyed(&make_config(
            AggregationType::DatasketchesKLL,
            ""
        )));
        assert!(!config_is_keyed(&make_config(
            AggregationType::Increase,
            ""
        )));

        // Keyed types
        assert!(config_is_keyed(&make_config(
            AggregationType::MultipleSubpopulation,
            "Sum"
        )));
        assert!(config_is_keyed(&make_config(
            AggregationType::MultipleSum,
            ""
        )));
        assert!(config_is_keyed(&make_config(
            AggregationType::MultipleIncrease,
            ""
        )));
        assert!(config_is_keyed(&make_config(
            AggregationType::MultipleMinMax,
            ""
        )));
        assert!(config_is_keyed(&make_config(
            AggregationType::CountMinSketch,
            "sum"
        )));
        assert!(config_is_keyed(&make_config(AggregationType::HydraKLL, "")));

        // Verify agreement with updater.is_keyed() for types that need no sketch params.
        for (agg_type, sub_type) in &[
            (AggregationType::SingleSubpopulation, "Sum"),
            (AggregationType::MultipleSubpopulation, "Sum"),
            (AggregationType::MultipleSum, ""),
        ] {
            let config = make_config(*agg_type, sub_type);
            let updater = create_accumulator_updater(&config).unwrap();
            assert_eq!(
                config_is_keyed(&config),
                updater.is_keyed(),
                "config_is_keyed disagrees with updater.is_keyed() for type={:?}",
                agg_type
            );
        }

        // Sketch types require params — build configs with the required parameters.
        let make_config_with_params =
            |agg_type: AggregationType,
             sub_type: &str,
             params: std::collections::HashMap<String, serde_json::Value>| {
                AggregationConfig::new(
                    1,
                    agg_type,
                    sub_type.to_string(),
                    params,
                    promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
                    promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
                    promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
                    String::new(),
                    60,
                    0,
                    WindowType::Tumbling,
                    "m".to_string(),
                    "m".to_string(),
                    None,
                    None,
                    None,
                    None,
                )
            };
        for (agg_type, sub_type, params) in [
            (AggregationType::DatasketchesKLL, "", kll_params_required()),
            (
                AggregationType::CountMinSketch,
                "sum",
                cms_params_required(),
            ),
        ] {
            let config = make_config_with_params(agg_type, sub_type, params);
            let updater = create_accumulator_updater(&config).unwrap();
            assert_eq!(
                config_is_keyed(&config),
                updater.is_keyed(),
                "config_is_keyed disagrees with updater.is_keyed() for type={:?}",
                agg_type
            );
        }
    }

    // HLL: `AggregationType::HLL` must build `HllAccumulatorUpdater` (hashes samples
    // into a sketch), not fall through to the default `SumAccumulatorUpdater`.

    #[test]
    fn test_hll_updater_via_factory_routes_to_hll_accumulator() {
        use std::collections::HashMap;
        let config = AggregationConfig::new(
            42,
            AggregationType::HLL,
            String::new(),
            HashMap::new(),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            60_000,
            0,
            WindowType::Tumbling,
            "m".to_string(),
            "m".to_string(),
            None,
            None,
            None,
            None,
        );
        let mut updater = create_accumulator_updater(&config).unwrap();
        assert!(
            !updater.is_keyed(),
            "HLL is single-population per grouping key (like KLL), not keyed",
        );

        // Feed 100 distinct values; the resulting accumulator should report an
        // estimate near 100 (not a sum of 0+1+…+99 ≈ 4950, which is what the
        // old SumAccumulatorUpdater fallback would have produced).
        for i in 0..100 {
            updater.update_single(i as f64, i * 1000);
        }
        let acc = updater.take_accumulator();
        assert_eq!(acc.type_name(), "HllAccumulator");
        assert_eq!(acc.get_accumulator_type(), AggregationType::HLL);

        let hll = acc
            .as_any()
            .downcast_ref::<crate::precompute_operators::HllAccumulator>()
            .expect("factory must produce HllAccumulator for AggregationType::HLL");
        let est = hll.estimate();
        assert!(
            est > 90.0 && est < 110.0,
            "100 distinct inserts should yield estimate near 100, got {est}",
        );
    }

    #[test]
    fn test_hll_updater_precision_param_propagates() {
        // `parameters: { precision: 12 }` must flow into the HllAccumulator, not
        // be silently dropped. 12-bit precision yields a 4 KiB register array,
        // serialising to a noticeably smaller msgpack body than the 16 KiB
        // default — that's the property we check (no need to assert exact size).
        use std::collections::HashMap;
        let mut params = HashMap::new();
        params.insert("precision".to_string(), serde_json::Value::from(12_u64));
        let config = AggregationConfig::new(
            7,
            AggregationType::HLL,
            String::new(),
            params,
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            60_000,
            0,
            WindowType::Tumbling,
            "m".to_string(),
            "m".to_string(),
            None,
            None,
            None,
            None,
        );
        let updater = create_accumulator_updater(&config).unwrap();
        let acc = updater.snapshot_accumulator();
        let hll = acc
            .as_any()
            .downcast_ref::<crate::precompute_operators::HllAccumulator>()
            .expect("AggregationType::HLL → HllAccumulator");
        assert_eq!(hll.precision(), 12);
    }

    #[test]
    fn test_hll_updater_default_precision_is_14() {
        // When no `precision` parameter is supplied, the factory must use the
        // documented default (14) — not whatever the type default resolves to.
        use std::collections::HashMap;
        let config = AggregationConfig::new(
            7,
            AggregationType::HLL,
            String::new(),
            HashMap::new(),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            60_000,
            0,
            WindowType::Tumbling,
            "m".to_string(),
            "m".to_string(),
            None,
            None,
            None,
            None,
        );
        let updater = create_accumulator_updater(&config).unwrap();
        let acc = updater.snapshot_accumulator();
        let hll = acc
            .as_any()
            .downcast_ref::<crate::precompute_operators::HllAccumulator>()
            .expect("AggregationType::HLL → HllAccumulator");
        assert_eq!(hll.precision(), 14);
    }

    #[test]
    fn test_hll_updater_reset_clears_state() {
        // After reset(), a freshly-taken accumulator must produce an empty (0.0)
        // estimate — otherwise pane reuse across tumbling windows would leak
        // distinct values from the previous pane into the next.
        use std::collections::HashMap;
        let config = AggregationConfig::new(
            7,
            AggregationType::HLL,
            String::new(),
            HashMap::new(),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            60_000,
            0,
            WindowType::Tumbling,
            "m".to_string(),
            "m".to_string(),
            None,
            None,
            None,
            None,
        );
        let mut updater = create_accumulator_updater(&config).unwrap();
        for i in 0..50 {
            updater.update_single(i as f64, 0);
        }
        updater.reset();
        let acc = updater.take_accumulator();
        let hll = acc
            .as_any()
            .downcast_ref::<crate::precompute_operators::HllAccumulator>()
            .unwrap();
        assert_eq!(hll.estimate(), 0.0);
    }

    #[test]
    fn test_kll_k_param_capital_k() {
        // SingleSubpopulation/KLL with capital "K" param should use it (not default to 200)
        use std::collections::HashMap;
        let mut params = HashMap::new();
        params.insert("K".to_string(), serde_json::Value::from(50_u64));
        let config = AggregationConfig::new(
            1,
            AggregationType::SingleSubpopulation,
            "DatasketchesKLL".to_string(),
            params,
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            60_000,
            0,
            WindowType::Tumbling,
            "m".to_string(),
            "m".to_string(),
            None,
            None,
            None,
            None,
        );
        let updater = create_accumulator_updater(&config).unwrap();
        let acc = updater.snapshot_accumulator();
        let kll = acc
            .as_any()
            .downcast_ref::<crate::precompute_operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator>()
            .expect("should be KLL");
        assert_eq!(kll.inner.k, 50, "k should be 50 from capital-K param");
    }

    #[test]
    fn test_kll_missing_k_param_returns_err() {
        use std::collections::HashMap;
        let config = AggregationConfig::new(
            1,
            AggregationType::DatasketchesKLL,
            String::new(),
            HashMap::new(),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            60_000,
            0,
            WindowType::Tumbling,
            "m".to_string(),
            "m".to_string(),
            None,
            None,
            None,
            None,
        );
        let err = create_accumulator_updater(&config)
            .err()
            .expect("expected Err for missing K param");
        assert!(
            err.contains("K"),
            "error should mention the missing parameter name"
        );
    }

    #[test]
    fn test_cms_missing_params_returns_err() {
        use std::collections::HashMap;
        let config = AggregationConfig::new(
            1,
            AggregationType::CountMinSketch,
            "sum".to_string(),
            HashMap::new(),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            60_000,
            0,
            WindowType::Tumbling,
            "m".to_string(),
            "m".to_string(),
            None,
            None,
            None,
            None,
        );
        let err = create_accumulator_updater(&config)
            .err()
            .expect("expected Err for missing params");
        // Error must mention both aliases so callers know what's accepted.
        assert!(
            err.contains("depth") && err.contains("row_num"),
            "error should mention accepted parameter names, got: {err}"
        );
    }

    #[test]
    fn test_cms_depth_width_params_accepted() {
        // Planner emits depth/width; engine must accept them without error.
        use std::collections::HashMap;
        let mut params = HashMap::new();
        params.insert("depth".to_string(), serde_json::json!(3_u64));
        params.insert("width".to_string(), serde_json::json!(1024_u64));
        let config = AggregationConfig::new(
            21,
            AggregationType::CountMinSketch,
            "sum".to_string(),
            params,
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            15_000,
            0,
            WindowType::Tumbling,
            "fake_metric".to_string(),
            "fake_metric".to_string(),
            None,
            None,
            None,
            None,
        );
        let updater = create_accumulator_updater(&config)
            .expect("depth/width params must be accepted by CountMinSketch");
        assert!(updater.is_keyed());
        assert_eq!(
            updater.snapshot_accumulator().type_name(),
            "CountMinSketchAccumulator"
        );
    }

    fn kll_params_required() -> std::collections::HashMap<String, serde_json::Value> {
        let mut p = std::collections::HashMap::new();
        p.insert("K".to_string(), serde_json::json!(200_u64));
        p
    }

    fn cms_params_required() -> std::collections::HashMap<String, serde_json::Value> {
        let mut p = std::collections::HashMap::new();
        p.insert("row_num".to_string(), serde_json::json!(4_u64));
        p.insert("col_num".to_string(), serde_json::json!(1000_u64));
        p
    }

    fn cms_config(sub_type: &str) -> AggregationConfig {
        AggregationConfig::new(
            100,
            AggregationType::CountMinSketch,
            sub_type.to_string(),
            cms_params_required(),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            1_000,
            1_000,
            WindowType::Tumbling,
            "test_metric".to_string(),
            "test_metric".to_string(),
            None,
            None,
            None,
            None,
        )
    }

    #[test]
    fn test_cms_count_subtype_uses_unit_weight() {
        let config = cms_config("count");
        let mut updater = create_accumulator_updater(&config).unwrap();
        let key = KeyByLabelValues::new_with_labels(vec!["host-a".to_string()]);

        for _ in 0..5 {
            updater.update_keyed(&key, 1_000.0, 0);
        }

        let acc = updater.take_accumulator();
        let cms = acc
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .expect("CountMinSketch accumulator");
        assert_eq!(cms.query_key(&key), 5.0);
    }

    #[test]
    fn test_cms_sum_subtype_uses_sample_weight() {
        let config = cms_config("sum");
        let mut updater = create_accumulator_updater(&config).unwrap();
        let key = KeyByLabelValues::new_with_labels(vec!["host-a".to_string()]);

        for _ in 0..5 {
            updater.update_keyed(&key, 10.0, 0);
        }

        let acc = updater.take_accumulator();
        let cms = acc
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .expect("CountMinSketch accumulator");
        assert_eq!(cms.query_key(&key), 50.0);
    }

    #[test]
    fn test_cms_rejects_empty_subtype() {
        let config = cms_config("");
        let err = match create_accumulator_updater(&config) {
            Ok(_) => panic!("empty CountMinSketch subtype must fail"),
            Err(err) => err,
        };
        assert!(err.contains("sum") && err.contains("count"));
    }

    #[test]
    fn test_cms_rejects_unknown_subtype() {
        let config = cms_config("frequency");
        let err = match create_accumulator_updater(&config) {
            Ok(_) => panic!("unknown CountMinSketch subtype must fail"),
            Err(err) => err,
        };
        assert!(err.contains("frequency"));
    }

    #[test]
    fn test_cms_accepts_case_insensitive_subtype() {
        for sub_type in ["COUNT", "SuM"] {
            create_accumulator_updater(&cms_config(sub_type))
                .unwrap_or_else(|err| panic!("subtype '{sub_type}' should be accepted: {err}"));
        }
    }

    #[test]
    fn test_cms_rejects_whitespace_padded_subtype() {
        let config = cms_config(" count ");
        let err = match create_accumulator_updater(&config) {
            Ok(_) => panic!("whitespace-padded CountMinSketch subtype must fail"),
            Err(err) => err,
        };
        assert!(err.contains(" count "));
    }

    fn cms_heap_params_required() -> std::collections::HashMap<String, serde_json::Value> {
        let mut p = std::collections::HashMap::new();
        p.insert("depth".to_string(), serde_json::json!(3_u64));
        p.insert("width".to_string(), serde_json::json!(1024_u64));
        p.insert("heapsize".to_string(), serde_json::json!(32_u64));
        p
    }

    fn cms_heap_config(
        parameters: std::collections::HashMap<String, serde_json::Value>,
    ) -> AggregationConfig {
        AggregationConfig::new(
            101,
            AggregationType::CountMinSketchWithHeap,
            "topk".to_string(),
            parameters,
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![
                "srcip".to_string()
            ]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            1_000,
            0,
            WindowType::Tumbling,
            "netflow_table".to_string(),
            "netflow_table".to_string(),
            None,
            None,
            None,
            None,
        )
    }

    fn key_aggregation_config(aggregation_type: AggregationType) -> AggregationConfig {
        AggregationConfig::new(
            477,
            aggregation_type,
            String::new(),
            std::collections::HashMap::new(),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            60_000,
            0,
            WindowType::Tumbling,
            "metric".to_string(),
            "metric".to_string(),
            None,
            None,
            None,
            None,
        )
    }

    #[test]
    fn test_cms_with_heap_rejects_empty_subtype() {
        let mut config = cms_heap_config(cms_heap_params_required());
        config.aggregation_sub_type.clear();

        let err = match create_accumulator_updater(&config) {
            Ok(_) => panic!("empty CountMinSketchWithHeap subtype must fail"),
            Err(err) => err,
        };
        assert!(err.contains("topk"));
    }

    #[test]
    fn test_cms_with_heap_rejects_unknown_subtype() {
        let mut config = cms_heap_config(cms_heap_params_required());
        config.aggregation_sub_type = "count".to_string();

        let err = match create_accumulator_updater(&config) {
            Ok(_) => panic!("unknown CountMinSketchWithHeap subtype must fail"),
            Err(err) => err,
        };
        assert!(err.contains("count"));
    }

    #[test]
    fn test_cms_with_heap_rejects_non_boolean_count_events() {
        let mut parameters = cms_heap_params_required();
        parameters.insert("count_events".to_string(), serde_json::json!("true"));
        let config = cms_heap_config(parameters);

        let err = match create_accumulator_updater(&config) {
            Ok(_) => panic!("non-boolean count_events must fail"),
            Err(err) => err,
        };
        assert!(err.contains("count_events") && err.contains("boolean"));
    }

    #[test]
    fn test_cms_with_heap_factory_routes_to_heap_accumulator_and_is_keyed() {
        // CountMinSketchWithHeap must build a CmsWithHeapAccumulatorUpdater whose
        // accumulator exposes the heap (get_keys), NOT a plain CMS (no heap).
        let config = cms_heap_config(cms_heap_params_required());
        let updater = create_accumulator_updater(&config).unwrap();
        assert!(updater.is_keyed(), "CMS-with-heap top-k is keyed by srcip");

        let acc = updater.snapshot_accumulator();
        assert_eq!(acc.type_name(), "CountMinSketchWithHeapAccumulator");
        assert_eq!(
            acc.get_accumulator_type(),
            AggregationType::CountMinSketchWithHeap
        );
        assert!(
            acc.get_keys().is_some(),
            "heap accumulator must enumerate top-k candidate keys"
        );
    }

    #[test]
    fn test_cms_with_heap_count_events_uses_unit_weight() {
        // count_events (the default) → each observation contributes weight 1, so
        // the per-key estimate is the EVENT COUNT, not the sum of sample values.
        let config = cms_heap_config(cms_heap_params_required());
        let mut updater = create_accumulator_updater(&config).unwrap();

        let key = KeyByLabelValues::new_with_labels(vec!["10.0.0.1".to_string()]);
        // Feed 5 events with large values; count semantics must yield ~5, not ~Σvalue.
        for _ in 0..5 {
            updater.update_keyed(&key, 1000.0, 0);
        }
        let acc = updater.take_accumulator();
        let cms = acc
            .as_any()
            .downcast_ref::<CountMinSketchWithHeapAccumulator>()
            .expect("CountMinSketchWithHeap accumulator");
        assert_eq!(
            cms.query_key(&key),
            5.0,
            "count_events should count events (5), not sum values (5000)"
        );
    }

    #[test]
    fn test_cms_with_heap_count_events_false_sums_values() {
        // count_events=false → weight is the sample value, giving SUM semantics.
        let mut params = cms_heap_params_required();
        params.insert("count_events".to_string(), serde_json::json!(false));
        let config = cms_heap_config(params);
        let mut updater = create_accumulator_updater(&config).unwrap();

        let key = KeyByLabelValues::new_with_labels(vec!["10.0.0.1".to_string()]);
        for _ in 0..5 {
            updater.update_keyed(&key, 10.0, 0);
        }
        let acc = updater.take_accumulator();
        let cms = acc
            .as_any()
            .downcast_ref::<CountMinSketchWithHeapAccumulator>()
            .unwrap();
        assert_eq!(cms.query_key(&key), 50.0, "sum of 5×10 == 50");
    }

    #[test]
    fn test_cms_heap_params_reads_depth_width_heapsize() {
        let mut params = std::collections::HashMap::new();
        params.insert("depth".to_string(), serde_json::json!(4));
        params.insert("width".to_string(), serde_json::json!(2048));
        params.insert("heapsize".to_string(), serde_json::json!(40));
        let config = cms_heap_config(params);
        assert_eq!(cms_heap_params(&config).unwrap(), (4, 2048, 40));
        assert!(
            cms_count_events(&config).unwrap(),
            "count_events defaults to true"
        );
    }

    #[test]
    fn test_cms_with_heap_reset_clears_state() {
        let config = cms_heap_config(cms_heap_params_required());
        let mut updater = create_accumulator_updater(&config).unwrap();
        let key = KeyByLabelValues::new_with_labels(vec!["k".to_string()]);
        for _ in 0..10 {
            updater.update_keyed(&key, 1.0, 0);
        }
        updater.reset();
        let acc = updater.take_accumulator();
        let cms = acc
            .as_any()
            .downcast_ref::<CountMinSketchWithHeapAccumulator>()
            .unwrap();
        assert_eq!(cms.query_key(&key), 0.0, "reset must clear the sketch");
        assert!(cms.get_topk_keys().is_empty(), "reset must clear the heap");
    }

    #[test]
    fn test_issue_477_key_aggregator_factory_routes_and_tracks_keys() {
        // Regression for #477: planner-generated key aggregations must not fall
        // through the factory's scalar Sum updater, or keyed approximate queries
        // lose the subpopulation keys needed for enumeration.
        let key_a = KeyByLabelValues::new_with_labels(vec!["a".to_string()]);
        let key_b = KeyByLabelValues::new_with_labels(vec!["b".to_string()]);

        for (aggregation_type, expected_type_name) in [
            (AggregationType::SetAggregator, "SetAggregatorAccumulator"),
            (
                AggregationType::DeltaSetAggregator,
                "DeltaSetAggregatorAccumulator",
            ),
        ] {
            let config = key_aggregation_config(aggregation_type);
            assert!(config_is_keyed(&config));
            assert!(aggregation_type.is_keyed());

            let mut updater = create_accumulator_updater(&config).unwrap();
            assert!(updater.is_keyed());
            updater.update_keyed(&key_a, 10.0, 1_000);
            updater.update_keyed(&key_b, 20.0, 2_000);
            updater.update_keyed(&key_a, 30.0, 3_000);

            let accumulator = updater.take_accumulator();
            assert_eq!(accumulator.type_name(), expected_type_name);
            assert_eq!(accumulator.get_accumulator_type(), aggregation_type);
            let keys = accumulator
                .get_keys()
                .expect("key aggregators must enumerate tracked keys");
            assert_eq!(keys.len(), 2);
            assert!(keys.contains(&key_a));
            assert!(keys.contains(&key_b));
        }
    }

    #[test]
    fn test_key_aggregator_updater_reset_clears_keys() {
        let key = KeyByLabelValues::new_with_labels(vec!["reset-me".to_string()]);

        for aggregation_type in [
            AggregationType::SetAggregator,
            AggregationType::DeltaSetAggregator,
        ] {
            let config = key_aggregation_config(aggregation_type);
            let mut updater = create_accumulator_updater(&config).unwrap();
            updater.update_keyed(&key, 1.0, 0);
            updater.reset();

            assert!(
                updater
                    .snapshot_accumulator()
                    .get_keys()
                    .expect("key aggregators must enumerate tracked keys")
                    .is_empty(),
                "reset must clear {aggregation_type:?} keys"
            );
        }
    }

    #[test]
    fn test_factory_rejects_unknown_subpopulation_sub_type() {
        let mut config = key_aggregation_config(AggregationType::SingleSubpopulation);
        config.aggregation_sub_type = "not-an-aggregation".to_string();
        let err = create_accumulator_updater(&config)
            .err()
            .expect("unknown subpopulation subtype must not default to Sum");
        assert!(err.contains("Unknown SingleSubpopulation sub_type"));

        let mut config = key_aggregation_config(AggregationType::MultipleSubpopulation);
        config.aggregation_sub_type = "not-an-aggregation".to_string();
        let err = create_accumulator_updater(&config)
            .err()
            .expect("unknown subpopulation subtype must not default to MultipleSum");
        assert!(err.contains("Unknown MultipleSubpopulation sub_type"));
    }
}
