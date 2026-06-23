//! Tunable constants for candidate generation and cost modeling, centralized
//! so they're easy to find and swap for calibrated/profiled values later.

// ponytail: small representative grids; replace with sketch-bench sweep results in Phase 3.
pub const CMS_DEPTHS: &[u64] = &[3, 5];
pub const CMS_WIDTHS: &[u64] = &[512, 1024, 2048];
pub const CMS_HEAP_SIZES: &[u64] = &[40, 200, 1000];
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
