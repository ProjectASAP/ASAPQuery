use asap_types::inference_config::InferenceConfig;
use asap_types::streaming_config::StreamingConfig;

use super::solution::{OptimizerSolution, QueryMethod};

/// Translate an `OptimizerSolution` into the deployment artifacts consumed by
/// Arroyo and the query engine.
///
/// Phase 1 (all-EXACT): deployed_configs is empty, all assignments are Exact,
/// so both output structs are empty/stub. Real translation logic fills in as
/// Phase 2/3 add sketch configs to the solution.
pub fn translate(solution: &OptimizerSolution) -> (StreamingConfig, InferenceConfig) {
    let streaming_config = build_streaming_config(solution);
    let inference_config = build_inference_config(solution);
    (streaming_config, inference_config)
}

fn build_streaming_config(solution: &OptimizerSolution) -> StreamingConfig {
    // Deployed configs map directly to AggregationConfigs — the types are the same.
    StreamingConfig::new(solution.deployed_configs.clone())
}

fn build_inference_config(solution: &OptimizerSolution) -> InferenceConfig {
    use asap_types::enums::{CleanupPolicy, QueryLanguage};

    // TODO (Phase 2+): populate query_configs from non-Exact assignments.
    // For each assignment with a real aggregation_id, emit a QueryConfig that
    // maps the original query strings to that aggregation_id, with the
    // appropriate num_aggregates_to_retain derived from QueryMethod::Merge
    // { num_windows } or 1 for Neither/Subtract.
    //
    // For Phase 1 (all-EXACT), all assignments have QueryMethod::Exact and
    // aggregation_id = None, so no query_configs are emitted — the inference
    // engine will fall back to raw querying for all AQEs.
    let _ = solution
        .assignments
        .iter()
        .filter(|a| a.query_method != QueryMethod::Exact)
        .count(); // placeholder to suppress unused-variable warnings

    InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup)
}

/// For a Merge assignment, the number of retained windows to configure in the
/// inference config (num_aggregates_to_retain on the AggregationReference).
pub fn retention_count_for_assignment(query_method: &QueryMethod) -> u64 {
    match query_method {
        QueryMethod::Neither => 1,
        QueryMethod::Merge { num_windows } => *num_windows,
        // Subtract needs 2 prefix checkpoints per query but we retain
        // ceil(range_a/W) checkpoints total to cover the full lookback.
        // The exact value comes from the config's window parameters; use 1
        // as a placeholder until Phase 3 wires this up properly.
        QueryMethod::Subtract => 1,
        QueryMethod::Exact => 0,
    }
}

/// Summary of what the translator produced, for logging/debugging.
#[derive(Debug)]
pub struct TranslationSummary {
    pub num_deployed_configs: usize,
    pub num_sketch_assignments: usize,
    pub num_exact_fallbacks: usize,
}

impl TranslationSummary {
    pub fn from_solution(solution: &OptimizerSolution) -> Self {
        Self {
            num_deployed_configs: solution.deployed_configs.len(),
            num_sketch_assignments: solution.num_sketch_served(),
            num_exact_fallbacks: solution.num_exact_fallback(),
        }
    }
}
