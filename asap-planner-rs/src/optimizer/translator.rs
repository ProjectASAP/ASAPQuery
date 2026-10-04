use asap_types::aggregation_reference::AggregationReference;
use asap_types::inference_config::InferenceConfig;
use asap_types::query_config::QueryConfig;
use asap_types::streaming_config::StreamingConfig;

use super::solution::{OptimizerSolution, QueryMethod};

/// Translate an `OptimizerSolution` into the deployment artifacts consumed by
/// Arroyo and the query engine.
///
pub fn translate(solution: &OptimizerSolution) -> (StreamingConfig, InferenceConfig) {
    let streaming_config = build_streaming_config(solution);
    let inference_config = build_inference_config(solution);
    (streaming_config, inference_config)
}

fn build_streaming_config(solution: &OptimizerSolution) -> StreamingConfig {
    // Deployed configs map directly to AggregationConfigs — the types are the same.
    StreamingConfig::new(solution.deployed_configs().clone())
}

fn build_inference_config(solution: &OptimizerSolution) -> InferenceConfig {
    use asap_types::enums::{CleanupPolicy, QueryLanguage};

    let mut inference = InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);

    for assignment in &solution.assignments {
        let aggregation_id = assignment.aggregation_id;
        let retain = retention_count_for_assignment(&assignment.query_method);
        let agg_ref = AggregationReference::new(aggregation_id, Some(retain));
        let key_ref = assignment.key_aggregation_id.map(|key_id| {
            let key_retain = solution.deployed_configs()[&key_id].num_aggregates_to_retain;
            AggregationReference::new(key_id, key_retain)
        });

        for query_string in &assignment.item.query_strings {
            let mut query_config =
                QueryConfig::new(query_string.clone()).add_aggregation(agg_ref.clone());
            if let Some(key_ref) = &key_ref {
                query_config = query_config.add_aggregation(key_ref.clone());
            }
            inference.query_configs.push(query_config);
        }
    }

    inference
}

/// For a Merge assignment, the number of retained windows to configure in the
/// inference config (num_aggregates_to_retain on the AggregationReference).
pub fn retention_count_for_assignment(query_method: &QueryMethod) -> u64 {
    match query_method {
        QueryMethod::Direct => 1,
        QueryMethod::Merge { num_windows } => *num_windows,
        // Subtract combines exactly 2 prefix-sum checkpoints per query
        // (current cumulative, and the one from range_a ago), regardless of
        // how many checkpoints the engine retains to make that pair available
        // (see candidate_gen.rs's n_windows, a separate concept: the deployed
        // AggregationConfig's retention depth).
        QueryMethod::Subtract => 2,
    }
}

/// Summary of what the translator produced, for logging/debugging.
#[derive(Debug)]
pub struct TranslationSummary {
    pub num_deployed_configs: usize,
    pub num_sketch_assignments: usize,
}

impl TranslationSummary {
    pub fn from_solution(solution: &OptimizerSolution) -> Self {
        Self {
            num_deployed_configs: solution.deployed_configs().len(),
            num_sketch_assignments: solution.num_sketch_served(),
        }
    }
}
