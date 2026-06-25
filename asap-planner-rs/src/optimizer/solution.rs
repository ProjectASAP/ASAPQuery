use std::collections::HashMap;

use asap_types::aggregation_config::AggregationConfig;
use asap_types::query_requirements::QueryRequirements;

/// An atomic query expression: one leaf aggregation extracted from a QE tree,
/// together with the optimizer-level metadata needed to assign it a config.
#[derive(Debug, Clone)]
pub struct AQE {
    /// What the query needs (metric, statistics, range, labels, spatial filter).
    pub requirements: QueryRequirements,

    /// Original PromQL/SQL query strings from all RQEs that contain this AQE.
    /// Preserved for use by the translator when building InferenceConfig.
    pub query_strings: Vec<String>,

    /// Query frequency in Hz: this field = Σ_{r ∈ R_a} 1/T_r.
    /// Used in the MIP objective to convert per-query QueryCost into a cost
    /// rate (cost/sec) commensurate with the continuously-accruing IngestCost.
    /// Represents the total query load from all dashboards independently
    /// hitting the sketch.
    pub query_frequency_hz: f64,

    /// Minimum repeat interval across all RQEs that reference this AQE (ms).
    /// Determines the freshness constraint on the window size: W ≤ min_t_repeat
    /// ensures a completed window is available for every dashboard's cycle.
    /// When multiple RQEs share this AQE, the fastest dashboard is the binding
    /// constraint.
    pub min_t_repeat_ms: u64,

    /// GCD of all repeat intervals across RQEs that reference this AQE (ms).
    /// The natural candidate for the slide interval S: windows that complete
    /// every GCD ms align harmonically with all dashboard refresh cycles,
    /// ensuring every dashboard can always be served a fresh result on-cycle.
    pub t_repeat_gcd_ms: u64,
}

/// How an AQE is answered from its assigned streaming config.
///
/// Determined by (ingest_type, W vs range_a, sketch algebra) — not a free
/// decision variable. See the compatibility table in the design doc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryMethod {
    /// W = range_a: one completed window covers the query range exactly.
    /// Direct read, no merge or subtract needed.
    Direct,

    /// W < range_a, sketch is mergeable: combine `num_windows` retained
    /// tumbling sub-windows at query time. Cost scales linearly with num_windows.
    Merge { num_windows: u64 },

    /// W < range_a, sketch is subtractable: subtract two prefix-sum checkpoints.
    /// O(1) cost regardless of range_a/W.
    Subtract,

    /// No streaming config deployed for this AQE — query raw/exact data at
    /// query time. Corresponds to the EXACT_a fallback (IngestCost = 0, Error = 0).
    Exact,
}

/// The assignment of a single AQE to a streaming config (or EXACT fallback).
#[derive(Debug, Clone)]
pub struct AQEAssignment {
    pub aqe: AQE,

    /// ID of the deployed config that serves this AQE.
    /// `None` means the EXACT_a fallback (no streaming config, raw query).
    pub aggregation_id: Option<u64>,

    /// How this AQE's answer is derived from the assigned config.
    pub query_method: QueryMethod,

    /// Estimated cost rate for this assignment: QueryCost(a, g) * `aqe.query_frequency_hz`.
    /// Zero for Exact assignments (IngestCost is also zero).
    pub estimated_query_cost_per_sec: f64,
}

/// The output of the optimizer: a complete plan for a given RQE workload.
///
/// Contains the set of streaming configs to deploy and the assignment of every
/// AQE to one of those configs (or to the EXACT fallback). A thin translator
/// converts this into `StreamingConfig + InferenceConfig` deployment artifacts.
#[derive(Debug, Clone)]
pub struct OptimizerSolution {
    /// Deployed streaming configs (y_g = 1 in the MIP). Keyed by aggregation_id.
    /// Empty for all-EXACT solutions (Phase 1 scaffolding).
    pub deployed_configs: HashMap<u64, AggregationConfig>,

    /// One entry per deduplicated AQE across the full RQE workload.
    pub assignments: Vec<AQEAssignment>,

    /// Estimated steady-state ingestion cost rate across all deployed configs
    /// (Σ_{g: y_g=1} IngestCost(g)).
    pub estimated_ingest_cost_per_sec: f64,

    /// Estimated total cost rate: ingest + query components combined.
    pub estimated_total_cost_per_sec: f64,
}

impl OptimizerSolution {
    /// Construct an all-EXACT solution: every AQE falls back to raw data,
    /// no streaming configs are deployed. Used as the Phase 1 scaffolding baseline.
    pub fn all_exact(aqes: Vec<AQE>) -> Self {
        let assignments = aqes
            .into_iter()
            .map(|aqe| AQEAssignment {
                aqe,
                aggregation_id: None,
                query_method: QueryMethod::Exact,
                estimated_query_cost_per_sec: 0.0,
            })
            .collect();

        Self {
            deployed_configs: HashMap::new(),
            assignments,
            estimated_ingest_cost_per_sec: 0.0,
            estimated_total_cost_per_sec: 0.0,
        }
    }

    /// Number of AQEs served by an approximate sketch (not EXACT fallback).
    pub fn num_sketch_served(&self) -> usize {
        self.assignments
            .iter()
            .filter(|a| a.query_method != QueryMethod::Exact)
            .count()
    }

    /// Number of AQEs falling back to exact/raw computation.
    pub fn num_exact_fallback(&self) -> usize {
        self.assignments
            .iter()
            .filter(|a| a.query_method == QueryMethod::Exact)
            .count()
    }
}
