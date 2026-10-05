use asap_types::aggregation_reference::AggregationReference;
use asap_types::inference_config::InferenceConfig;
use asap_types::query_config::QueryConfig;
use asap_types::streaming_config::StreamingConfig;
use std::collections::HashMap;

use super::solution::{OptimizerSolution, QueryMethod};

/// Translate an `OptimizerSolution` into the deployment artifacts consumed by
/// Arroyo and the query engine.
///
pub fn translate(solution: &OptimizerSolution) -> (StreamingConfig, InferenceConfig) {
    let inference_config = build_inference_config(solution);
    let streaming_config = build_streaming_config(solution, &inference_config);
    (streaming_config, inference_config)
}

fn build_streaming_config(
    solution: &OptimizerSolution,
    inference_config: &InferenceConfig,
) -> StreamingConfig {
    let mut configs = solution.deployed_configs().clone();
    for (aggregation_id, threshold) in read_count_thresholds(inference_config) {
        let config = configs
            .get_mut(&aggregation_id)
            .expect("every assigned aggregation must be deployed");
        config.num_aggregates_to_retain = None;
        config.read_count_threshold = Some(threshold);
    }
    StreamingConfig::new(configs)
}

fn build_inference_config(solution: &OptimizerSolution) -> InferenceConfig {
    use asap_types::enums::{CleanupPolicy, QueryLanguage};

    let mut inference = InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::ReadBased);

    for assignment in &solution.assignments {
        let aggregation_id = assignment.aggregation_id;
        let cleanup_count = cleanup_count_for_assignment(&assignment.query_method);
        let agg_ref =
            AggregationReference::with_read_count_threshold(aggregation_id, Some(cleanup_count));
        let key_ref = assignment.key_aggregation_id.map(|key_id| {
            AggregationReference::with_read_count_threshold(key_id, Some(cleanup_count))
        });

        for query_string in &assignment.item.query_strings {
            let mut query_config =
                QueryConfig::with_plan(query_string.clone(), query_string.clone(), vec![])
                    .add_aggregation(agg_ref.clone());
            if let Some(key_ref) = &key_ref {
                query_config = query_config.add_aggregation(key_ref.clone());
            }
            inference.query_configs.push(query_config);
        }
    }

    inference
}

fn read_count_thresholds(inference_config: &InferenceConfig) -> HashMap<u64, u64> {
    let mut thresholds = HashMap::new();
    for query_config in &inference_config.query_configs {
        for aggregation in &query_config.aggregations {
            let Some(cleanup_count) = aggregation.read_count_threshold else {
                continue;
            };
            let threshold = thresholds
                .entry(aggregation.aggregation_id)
                .or_insert(0_u64);
            *threshold = threshold
                .checked_add(cleanup_count)
                .expect("read-count threshold overflowed");
        }
    }
    thresholds
}

/// Number of aggregate reads an assignment needs before its windows may be cleaned up.
pub fn cleanup_count_for_assignment(query_method: &QueryMethod) -> u64 {
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::{CleanupPolicy, WindowType};
    use asap_types::query_requirements::QueryRequirements;
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::{AggregationType, Statistic};

    use super::super::solution::{AQEAssignment, OptimizerItem};
    use super::*;

    fn assignment(aggregation_id: u64, query_method: QueryMethod) -> AQEAssignment {
        AQEAssignment {
            item: OptimizerItem {
                requirements: QueryRequirements {
                    metric: "metric".into(),
                    statistics: vec![Statistic::Sum],
                    data_range_ms: 60_000,
                    grouping_labels: KeyByLabelNames::empty(),
                    spatial_filter_normalized: String::new(),
                    topk_count_events: None,
                    topk_by_labels: None,
                },
                query_strings: vec!["sum(metric)".into()],
                query_frequency_hz: 1.0,
                t_repeat_ms: 60_000,
                accuracy_sla: 0.01,
                latency_sla: 1.0,
            },
            aggregation_id,
            key_aggregation_id: None,
            query_method,
            estimated_query_cost_per_sec: 0.0,
        }
    }

    #[test]
    fn translation_uses_read_based_cleanup_and_sums_shared_thresholds() {
        let mut solution = OptimizerSolution::empty();
        let aggregation_id = solution.register_config(AggregationConfig::new(
            0,
            AggregationType::Sum,
            "sum".into(),
            HashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60_000,
            60_000,
            WindowType::Tumbling,
            String::new(),
            "metric".into(),
            None,
            None,
            None,
            None,
        ));
        solution.assignments = vec![
            assignment(aggregation_id, QueryMethod::Merge { num_windows: 3 }),
            assignment(aggregation_id, QueryMethod::Direct),
        ];

        let (streaming, inference) = translate(&solution);

        assert_eq!(inference.cleanup_policy, CleanupPolicy::ReadBased);
        assert_eq!(streaming[aggregation_id].read_count_threshold, Some(4));
        assert!(inference.query_configs.iter().all(|query_config| {
            query_config.aggregations.iter().all(|aggregation| {
                aggregation.num_aggregates_to_retain.is_none()
                    && aggregation.read_count_threshold.is_some()
            })
        }));
    }

    #[test]
    fn translation_counts_each_query_string_in_a_shared_threshold() {
        let mut solution = OptimizerSolution::empty();
        let aggregation_id = solution.register_config(AggregationConfig::new(
            0,
            AggregationType::Sum,
            "sum".into(),
            HashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60_000,
            60_000,
            WindowType::Tumbling,
            String::new(),
            "metric".into(),
            None,
            None,
            None,
            None,
        ));
        let mut shared_assignment =
            assignment(aggregation_id, QueryMethod::Merge { num_windows: 3 });
        shared_assignment
            .item
            .query_strings
            .push("sum by (job) (metric)".into());
        solution.assignments = vec![shared_assignment];

        let (streaming, _) = translate(&solution);

        assert_eq!(streaming[aggregation_id].read_count_threshold, Some(6));
    }
}
