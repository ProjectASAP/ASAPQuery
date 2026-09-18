//! Request-specific native query DAGs.

use crate::engines::simple_engine::{RangeQueryExecutionContext, StoreQueryParams};
use asap_types::enums::WindowType;
use promql_utilities::query_logics::enums::Statistic;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NodeId(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoreReadStrategy {
    WindowGrid,
    SlidingExactCover,
}

#[derive(Debug, Clone)]
pub(crate) enum QueryPlanNode {
    StoreRead {
        query: StoreQueryParams,
        strategy: StoreReadStrategy,
    },
    ComposeWindows {
        input: NodeId,
        output_timestamps: Vec<u64>,
        lookback_ms: u64,
        window_size_ms: u64,
        bucket_step_ms: u64,
    },
    ResolveKeys {
        values: NodeId,
        keys: Option<NodeId>,
    },
    Estimate {
        input: NodeId,
        statistic: Statistic,
        query_kwargs: std::collections::HashMap<String, String>,
    },
    LimitTopK {
        input: NodeId,
        k: String,
    },
    Format {
        input: NodeId,
        include_metric_name: bool,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct QueryPlan {
    nodes: Vec<QueryPlanNode>,
    root: NodeId,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PlanOptions {
    pub limit_topk: bool,
    pub format_output: bool,
}

impl QueryPlan {
    pub(crate) fn compile_range(
        context: &RangeQueryExecutionContext,
        options: PlanOptions,
    ) -> Self {
        let mut nodes = Vec::new();
        let values_read = Self::push_read(
            &mut nodes,
            &context.base.store_plan.values_query,
            context.window_type,
        );
        let values = Self::push_compose(
            &mut nodes,
            values_read,
            &context.output_timestamps,
            context.query_range_ms,
            context.window_size_ms,
            context.tumbling_window_ms,
        );
        let keys = context.base.store_plan.keys_query.as_ref().map(|query| {
            let read = Self::push_read(
                &mut nodes,
                query,
                context.keys_window_type.unwrap_or(context.window_type),
            );
            Self::push_compose(
                &mut nodes,
                read,
                &context.output_timestamps,
                context.keys_lookback_ms.unwrap_or(context.query_range_ms),
                context
                    .keys_window_size_ms
                    .unwrap_or(context.window_size_ms),
                context
                    .keys_tumbling_window_ms
                    .unwrap_or(context.tumbling_window_ms),
            )
        });
        let resolved = Self::push(&mut nodes, QueryPlanNode::ResolveKeys { values, keys });
        let mut root = Self::push(
            &mut nodes,
            QueryPlanNode::Estimate {
                input: resolved,
                statistic: context.base.metadata.statistic_to_compute,
                query_kwargs: context.base.metadata.query_kwargs.clone(),
            },
        );
        if options.limit_topk && context.base.metadata.statistic_to_compute == Statistic::Topk {
            let k = context
                .base
                .metadata
                .query_kwargs
                .get("k")
                .cloned()
                .unwrap_or_else(|| "<missing>".to_string());
            root = Self::push(&mut nodes, QueryPlanNode::LimitTopK { input: root, k });
        }
        if options.format_output {
            root = Self::push(
                &mut nodes,
                QueryPlanNode::Format {
                    input: root,
                    include_metric_name: context.base.metadata.keep_metric_name,
                },
            );
        }
        Self { nodes, root }
    }

    fn push(nodes: &mut Vec<QueryPlanNode>, node: QueryPlanNode) -> NodeId {
        let id = NodeId(nodes.len());
        nodes.push(node);
        id
    }

    fn push_read(
        nodes: &mut Vec<QueryPlanNode>,
        query: &StoreQueryParams,
        window_type: WindowType,
    ) -> NodeId {
        let strategy = match window_type {
            WindowType::Tumbling => StoreReadStrategy::WindowGrid,
            WindowType::Sliding => StoreReadStrategy::SlidingExactCover,
        };
        Self::push(
            nodes,
            QueryPlanNode::StoreRead {
                query: query.clone(),
                strategy,
            },
        )
    }

    fn push_compose(
        nodes: &mut Vec<QueryPlanNode>,
        input: NodeId,
        output_timestamps: &[u64],
        lookback_ms: u64,
        window_size_ms: u64,
        bucket_step_ms: u64,
    ) -> NodeId {
        Self::push(
            nodes,
            QueryPlanNode::ComposeWindows {
                input,
                output_timestamps: output_timestamps.to_vec(),
                lookback_ms,
                window_size_ms,
                bucket_step_ms,
            },
        )
    }

    pub(crate) fn explain(&self) -> String {
        let mut lines = Vec::with_capacity(self.nodes.len() + 1);
        for (index, node) in self.nodes.iter().enumerate() {
            let line = match node {
                QueryPlanNode::StoreRead { query, strategy } => format!(
                    "n{index} StoreRead({strategy:?}, {}#{}, [{}, {}])",
                    query.metric, query.aggregation_id, query.start_timestamp, query.end_timestamp
                ),
                QueryPlanNode::ComposeWindows { input, output_timestamps, lookback_ms, window_size_ms, bucket_step_ms } => format!(
                    "n{index} ComposeWindows(n{}, outputs={:?}, lookback={lookback_ms}ms, window={window_size_ms}ms, step={bucket_step_ms}ms)",
                    input.0, output_timestamps
                ),
                QueryPlanNode::ResolveKeys { values, keys } => format!(
                    "n{index} ResolveKeys(values=n{}, keys={})",
                    values.0,
                    keys.map(|id| format!("n{}", id.0)).unwrap_or_else(|| "self".to_string())
                ),
                QueryPlanNode::Estimate { input, statistic, query_kwargs } => format!(
                    "n{index} Estimate(n{}, {statistic}, {query_kwargs:?})", input.0
                ),
                QueryPlanNode::LimitTopK { input, k } => format!("n{index} LimitTopK(n{}, k={k})", input.0),
                QueryPlanNode::Format { input, include_metric_name } => format!(
                    "n{index} Format(n{}, include_metric_name={include_metric_name})", input.0
                ),
            };
            lines.push(line);
        }
        lines.push(format!("root: n{}", self.root.0));
        lines.join("\n")
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

    fn context() -> RangeQueryExecutionContext {
        RangeQueryExecutionContext {
            base: QueryExecutionContext {
                metric: "requests".into(),
                metadata: QueryMetadata {
                    query_output_labels: KeyByLabelNames::empty(),
                    statistic_to_compute: Statistic::Sum,
                    query_kwargs: HashMap::new(),
                    keep_metric_name: false,
                },
                store_plan: StoreQueryPlan {
                    values_query: StoreQueryParams {
                        metric: "requests".into(),
                        aggregation_id: 7,
                        start_timestamp: 0,
                        end_timestamp: 1_000,
                    },
                    keys_query: None,
                },
                agg_info: AggregationIdInfo {
                    aggregation_id_for_key: 7,
                    aggregation_id_for_value: 7,
                    aggregation_type_for_key: AggregationType::Sum,
                    aggregation_type_for_value: AggregationType::Sum,
                },
                value_window_type: WindowType::Tumbling,
                do_merge: false,
                spatial_filter: String::new(),
                query_time: 1_000,
                grouping_labels: KeyByLabelNames::empty(),
                aggregated_labels: KeyByLabelNames::empty(),
            },
            output_timestamps: vec![1_000],
            query_range_ms: 1_000,
            buckets_per_step: 1,
            lookback_bucket_count: 1,
            tumbling_window_ms: 1_000,
            window_type: WindowType::Tumbling,
            window_size_ms: 1_000,
            keys_window_type: None,
            keys_window_size_ms: None,
            keys_lookback_ms: None,
            keys_tumbling_window_ms: None,
        }
    }

    #[test]
    fn separate_key_branch_fans_into_key_resolution() {
        let mut context = context();
        context.base.store_plan.keys_query = Some(StoreQueryParams {
            metric: "requests".into(),
            aggregation_id: 8,
            start_timestamp: 0,
            end_timestamp: 1_000,
        });
        context.keys_window_type = Some(WindowType::Sliding);

        let explanation = QueryPlan::compile_range(
            &context,
            PlanOptions {
                limit_topk: false,
                format_output: false,
            },
        )
        .explain();

        assert!(explanation.contains("n4 ResolveKeys(values=n1, keys=n3)"));
        assert!(explanation.contains("n2 StoreRead(SlidingExactCover, requests#8"));
        assert!(explanation.ends_with("root: n5"));
    }

    #[test]
    fn range_plan_keeps_every_output_timestamp() {
        let mut context = context();
        context.output_timestamps = vec![1_000, 2_000, 3_000];

        let explanation = QueryPlan::compile_range(
            &context,
            PlanOptions {
                limit_topk: false,
                format_output: false,
            },
        )
        .explain();

        assert!(explanation.contains("outputs=[1000, 2000, 3000]"));
    }

    #[test]
    fn topk_formatting_is_the_plan_root() {
        let mut context = context();
        context.base.metadata.statistic_to_compute = Statistic::Topk;
        context
            .base
            .metadata
            .query_kwargs
            .insert("k".to_string(), "3".to_string());
        context.base.metadata.keep_metric_name = true;

        let explanation = QueryPlan::compile_range(
            &context,
            PlanOptions {
                limit_topk: true,
                format_output: true,
            },
        )
        .explain();

        assert!(explanation.contains("LimitTopK(n3, k=3)"));
        assert!(explanation.contains("Format(n4, include_metric_name=true)"));
        assert!(explanation.ends_with("root: n5"));
    }
}
