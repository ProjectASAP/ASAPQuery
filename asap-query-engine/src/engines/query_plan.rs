//! Explicit, request-specific physical plans for native queries.

use crate::engines::simple_engine::{RangeQueryExecutionContext, StoreQueryParams};
use asap_types::enums::WindowType;
use promql_utilities::query_logics::enums::Statistic;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreReadStrategy {
    WindowGrid,
    SlidingExactCover,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryPlanStep {
    StoreRead {
        metric: String,
        aggregation_id: u64,
        strategy: StoreReadStrategy,
    },
    ComposeWindows {
        output_count: usize,
    },
    ResolveKeys,
    Estimate {
        statistic: Statistic,
    },
    LimitTopK,
    Format,
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    steps: Vec<QueryPlanStep>,
}

#[derive(Debug, Clone, Copy)]
pub struct PlanOptions {
    pub limit_topk: bool,
    pub format_output: bool,
}

impl QueryPlan {
    pub fn compile_range(context: &RangeQueryExecutionContext, options: PlanOptions) -> Self {
        let mut steps = vec![Self::store_read(
            &context.base.store_plan.values_query,
            context.window_type,
        )];
        steps.push(QueryPlanStep::ComposeWindows {
            output_count: context.output_timestamps.len(),
        });
        if let Some(keys) = &context.base.store_plan.keys_query {
            steps.push(Self::store_read(
                keys,
                context.keys_window_type.unwrap_or(context.window_type),
            ));
            steps.push(QueryPlanStep::ComposeWindows {
                output_count: context.output_timestamps.len(),
            });
        }
        steps.push(QueryPlanStep::ResolveKeys);
        steps.push(QueryPlanStep::Estimate {
            statistic: context.base.metadata.statistic_to_compute,
        });
        if options.limit_topk && context.base.metadata.statistic_to_compute == Statistic::Topk {
            steps.push(QueryPlanStep::LimitTopK);
        }
        if options.format_output {
            steps.push(QueryPlanStep::Format);
        }
        Self { steps }
    }

    fn store_read(query: &StoreQueryParams, window_type: WindowType) -> QueryPlanStep {
        let strategy = match window_type {
            WindowType::Tumbling => StoreReadStrategy::WindowGrid,
            WindowType::Sliding => StoreReadStrategy::SlidingExactCover,
        };
        QueryPlanStep::StoreRead {
            metric: query.metric.clone(),
            aggregation_id: query.aggregation_id,
            strategy,
        }
    }

    pub fn steps(&self) -> &[QueryPlanStep] {
        &self.steps
    }

    pub fn explain(&self) -> String {
        self.steps
            .iter()
            .map(|step| match step {
                QueryPlanStep::StoreRead {
                    metric,
                    aggregation_id,
                    strategy,
                } => format!("StoreRead({strategy:?}, {metric}#{aggregation_id})"),
                QueryPlanStep::ComposeWindows { output_count } => {
                    format!("ComposeWindows({output_count} outputs)")
                }
                QueryPlanStep::ResolveKeys => "ResolveKeys".into(),
                QueryPlanStep::Estimate { statistic } => format!("Estimate({statistic})"),
                QueryPlanStep::LimitTopK => "LimitTopK".into(),
                QueryPlanStep::Format => "Format".into(),
            })
            .collect::<Vec<_>>()
            .join(" -> ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::AggregationIdInfo;
    use crate::engines::simple_engine::{QueryExecutionContext, QueryMetadata, StoreQueryPlan};
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::AggregationType;
    use std::collections::HashMap;

    fn context(window_type: WindowType) -> RangeQueryExecutionContext {
        let metadata = QueryMetadata {
            query_output_labels: KeyByLabelNames::empty(),
            statistic_to_compute: Statistic::Sum,
            query_kwargs: HashMap::new(),
            keep_metric_name: false,
        };
        let query = StoreQueryParams {
            metric: "requests".into(),
            aggregation_id: 7,
            start_timestamp: 0,
            end_timestamp: 1_000,
        };
        let base = QueryExecutionContext {
            metric: "requests".into(),
            metadata,
            store_plan: StoreQueryPlan {
                values_query: query,
                keys_query: None,
            },
            agg_info: AggregationIdInfo {
                aggregation_id_for_key: 7,
                aggregation_id_for_value: 7,
                aggregation_type_for_key: AggregationType::Sum,
                aggregation_type_for_value: AggregationType::Sum,
            },
            value_window_type: window_type,
            do_merge: false,
            spatial_filter: String::new(),
            query_time: 1_000,
            grouping_labels: KeyByLabelNames::empty(),
            aggregated_labels: KeyByLabelNames::empty(),
        };
        RangeQueryExecutionContext {
            base,
            output_timestamps: vec![1_000],
            query_range_ms: 1_000,
            buckets_per_step: 1,
            lookback_bucket_count: 1,
            tumbling_window_ms: 1_000,
            window_type,
            window_size_ms: 1_000,
            keys_window_type: None,
            keys_window_size_ms: None,
            keys_lookback_ms: None,
            keys_tumbling_window_ms: None,
        }
    }

    #[test]
    fn compiles_a_leaf_context_into_an_explainable_plan() {
        let plan = QueryPlan::compile_range(
            &context(WindowType::Tumbling),
            PlanOptions {
                limit_topk: false,
                format_output: false,
            },
        );
        assert_eq!(plan.explain(), "StoreRead(WindowGrid, requests#7) -> ComposeWindows(1 outputs) -> ResolveKeys -> Estimate(sum)");
    }

    #[test]
    fn compiles_a_separate_keys_branch_before_key_resolution() {
        let mut context = context(WindowType::Tumbling);
        context.base.store_plan.keys_query = Some(StoreQueryParams {
            metric: "requests".into(),
            aggregation_id: 8,
            start_timestamp: 0,
            end_timestamp: 1_000,
        });
        context.keys_window_type = Some(WindowType::Sliding);

        let plan = QueryPlan::compile_range(
            &context,
            PlanOptions {
                limit_topk: false,
                format_output: false,
            },
        );

        assert_eq!(plan.explain(), "StoreRead(WindowGrid, requests#7) -> ComposeWindows(1 outputs) -> StoreRead(SlidingExactCover, requests#8) -> ComposeWindows(1 outputs) -> ResolveKeys -> Estimate(sum)");
    }

    #[test]
    fn compiles_sliding_windows_as_an_exact_cover_read() {
        let plan = QueryPlan::compile_range(
            &context(WindowType::Sliding),
            PlanOptions {
                limit_topk: false,
                format_output: false,
            },
        );

        assert!(plan
            .explain()
            .starts_with("StoreRead(SlidingExactCover, requests#7)"));
    }

    #[test]
    fn adds_requested_topk_limit_and_formatting() {
        let mut context = context(WindowType::Tumbling);
        context.base.metadata.statistic_to_compute = Statistic::Topk;

        let plan = QueryPlan::compile_range(
            &context,
            PlanOptions {
                limit_topk: true,
                format_output: true,
            },
        );

        assert!(plan
            .explain()
            .ends_with("Estimate(topk) -> LimitTopK -> Format"));
    }
}
