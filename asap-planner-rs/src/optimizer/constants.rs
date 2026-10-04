//! Tunable constants for candidate generation and cost modeling, centralized
//! so they're easy to find and swap for calibrated/profiled values later.

// ponytail: small representative grids; replace with sketch-bench sweep results in Phase 3.
pub const CMS_DEPTHS: &[u64] = &[3, 5];
pub const CMS_WIDTHS: &[u64] = &[512, 1024, 2048];
pub const CMS_HEAP_SIZES: &[u64] = &[40, 200, 1000];
// Temporary CMS-with-heap cost-model assumptions. The reference CPU costs
// come from the fixed-top-k sketch-bench wrapper; these values describe the
// runtime representation until sketch-bench sweeps the actual heap sizes.
pub const CMS_HEAP_REFERENCE_HEAP_SIZE: u64 = 32;
pub const CMS_HEAP_COUNTER_BYTES: f64 = 8.0;
pub const CMS_HEAP_AVERAGE_KEY_BYTES: f64 = 32.0;
pub const CMS_HEAP_ENTRY_OVERHEAD_BYTES: f64 = 32.0;
pub const KLL_KS: &[u64] = &[200, 500];
pub const HYDRA_ROWS: &[u64] = &[3, 5];
pub const HYDRA_COLS: &[u64] = &[512, 1024];
pub const HYDRA_K: u64 = 20;
pub const HLL_PRECISIONS: &[u64] = &[12, 14];

// Per-operation costs for one sketch instance (`AtomicCosts` defaults).
// Stub values for v1 — real numbers come from sketch-bench in Phase 3.
pub const MEM_BYTES_PER_INSTANCE: f64 = 1024.0;
pub const INSERT_CPU_SECS: f64 = 1e-7;
pub const MERGE_CPU_SECS: f64 = 1e-5;
pub const SUBTRACT_CPU_SECS: f64 = 1e-6;
pub const QUERY_CPU_SECS: f64 = 1e-5;
/// Cost of one raw/exact query execution (the EXACT_a fallback's QueryCost).
/// Without this, EXACT always wins since its IngestCost and QueryCost would
/// otherwise both be zero.
pub const EXACT_QUERY_CPU_SECS: f64 = 1e-3;

// Global objective weights (w1..w4 in the design doc), `CostWeights` defaults.
// Real calibration (from actual cloud $/byte-sec and $/cpu-sec) is punted
// post-v1; defaults reflect that RAM-held-over-time is several orders of
// magnitude cheaper per unit than CPU-time (e.g. ~$5/GB-month vs
// ~$0.04/vCPU-hour is roughly a 1e6 ratio), so memory weights are scaled
// down accordingly rather than left equal to CPU weights.
pub const INGEST_MEM_WEIGHT: f64 = 1e-9;
pub const INGEST_CPU_WEIGHT: f64 = 1.0;
pub const QUERY_MEM_WEIGHT: f64 = 1e-9;
pub const QUERY_CPU_WEIGHT: f64 = 1.0;

// Analytical per-group memory for trivial accumulators (Sum/MinMax/Increase and
// their Multiple* maps): one dictionary code per grouping-label value plus the
// value, inflated by hash-table slack. The dictionary itself is amortized over
// time and not charged.
pub const LABEL_VALUE_CODE_BYTES: f64 = 4.0;
/// hashbrown's maximum load factor is 7/8.
pub const HASH_TABLE_SLACK: f64 = 8.0 / 7.0;
/// One f64.
pub const SUM_VALUE_BYTES: f64 = 8.0;
/// One f64.
pub const MIN_MAX_VALUE_BYTES: f64 = 8.0;
/// Start/last measurement and timestamp, sample count, reset adjustment, and
/// an empty reset-event Vec.
// ponytail: ignores counter-reset events; add per-event bytes if resets are frequent.
pub const INCREASE_VALUE_BYTES: f64 = 72.0;
