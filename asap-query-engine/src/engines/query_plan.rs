//! Request-specific native query DAGs.

use crate::engines::query_time_aggregation::output_labels_for_aggregation;
use crate::engines::simple_engine::{RangeQueryExecutionContext, StoreQueryParams};
use asap_types::enums::WindowType;
use asap_types::query_config::QueryTimeAggregation;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{AggregationType, Statistic};
use std::sync::Arc;
use tracing::debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NodeId(usize);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoreReadStrategy {
    WindowGrid,
    SlidingExactCover(WindowCompositionSpec),
}

impl StoreReadStrategy {
    fn for_window(window: &WindowCompositionSpec) -> Self {
        match window.window_type {
            WindowType::Tumbling => Self::WindowGrid,
            WindowType::Sliding => Self::SlidingExactCover(window.clone()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StoreReadRole {
    Values,
    Keys,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WindowCompositionSpec {
    pub output_timestamps: Arc<[u64]>,
    pub lookback_ms: u64,
    pub window_type: WindowType,
    pub window_size_ms: u64,
    pub bucket_step_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ValueEstimateInput {
    pub aggregation_type: AggregationType,
    pub window: WindowCompositionSpec,
}

#[derive(Debug, Clone)]
pub(crate) enum KeyInputSpec {
    FromValues,
    Separate {
        aggregation_type: AggregationType,
        window: WindowCompositionSpec,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct RangeEstimateSpec {
    pub query_range_ms: u64,
    pub values: ValueEstimateInput,
    pub keys: KeyInputSpec,
    pub grouping_labels: KeyByLabelNames,
    pub aggregated_labels: KeyByLabelNames,
    pub row_label_order: KeyByLabelNames,
}

#[derive(Debug, Clone)]
pub(crate) enum QueryPlanNode {
    StoreRead {
        query: StoreQueryParams,
        role: StoreReadRole,
        strategy: StoreReadStrategy,
    },
    PrepareBuckets {
        input: NodeId,
    },
    ResolveKeys {
        values: NodeId,
        keys: Option<NodeId>,
    },
    Estimate {
        input: NodeId,
        statistic: Statistic,
        query_kwargs: std::collections::HashMap<String, String>,
        output_labels: KeyByLabelNames,
        spec: RangeEstimateSpec,
    },
    AggregateVector {
        input: NodeId,
        aggregation: QueryTimeAggregation,
        input_labels: KeyByLabelNames,
    },
    LimitTopK {
        input: NodeId,
        k: String,
        grouping_labels: KeyByLabelNames,
        row_label_order: KeyByLabelNames,
    },
    Format {
        input: NodeId,
        include_metric_name: bool,
        metric: String,
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

pub(crate) trait QueryPlanRuntime {
    type Output: Clone;
    type Error: std::fmt::Display;

    fn execute_node(
        &self,
        id: NodeId,
        node: &QueryPlanNode,
        inputs: &[Self::Output],
    ) -> Result<Self::Output, Self::Error>;
}

#[derive(Debug)]
pub(crate) enum QueryPlanExecutionError<E> {
    InvalidPlan(String),
    Node { id: NodeId, source: E },
}

impl<E: std::fmt::Display> std::fmt::Display for QueryPlanExecutionError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPlan(error) => write!(formatter, "invalid query plan: {error}"),
            Self::Node { id, source } => {
                write!(formatter, "Query plan node n{} failed: {source}", id.0)
            }
        }
    }
}

impl QueryPlan {
    #[cfg(feature = "native_query_legacy_test_support")]
    pub(crate) fn malformed_for_test() -> Self {
        Self {
            nodes: Vec::new(),
            root: NodeId(0),
        }
    }

    pub(crate) fn compile_range(
        context: &RangeQueryExecutionContext,
        options: PlanOptions,
        query_time_aggregations: &[QueryTimeAggregation],
    ) -> Result<Self, String> {
        let mut nodes = Vec::new();
        let estimate_spec = RangeEstimateSpec::compile(context)?;
        let values_read = Self::push_read(
            &mut nodes,
            &context.base.store_plan.values_query,
            StoreReadRole::Values,
            StoreReadStrategy::for_window(&estimate_spec.values.window),
        );
        let values = Self::push_prepare_buckets(&mut nodes, values_read);
        let keys = match (&context.base.store_plan.keys_query, &estimate_spec.keys) {
            (None, KeyInputSpec::FromValues) => None,
            (Some(query), KeyInputSpec::Separate { window, .. }) => {
                let read = Self::push_read(
                    &mut nodes,
                    query,
                    StoreReadRole::Keys,
                    StoreReadStrategy::for_window(window),
                );
                Some(Self::push_prepare_buckets(&mut nodes, read))
            }
            (Some(_), KeyInputSpec::FromValues) => {
                return Err("Query plan has a keys read without a keys specification".to_string());
            }
            (None, KeyInputSpec::Separate { .. }) => {
                return Err("Query plan has a keys specification without a keys read".to_string());
            }
        };
        let resolved = Self::push(&mut nodes, QueryPlanNode::ResolveKeys { values, keys });
        let mut root = Self::push(
            &mut nodes,
            QueryPlanNode::Estimate {
                input: resolved,
                statistic: context.base.metadata.statistic_to_compute,
                query_kwargs: context.base.metadata.query_kwargs.clone(),
                output_labels: context.base.metadata.query_output_labels.clone(),
                spec: estimate_spec.clone(),
            },
        );
        if options.limit_topk && context.base.metadata.statistic_to_compute == Statistic::Topk {
            let k = context
                .base
                .metadata
                .query_kwargs
                .get("k")
                .cloned()
                .ok_or_else(|| "Topk query is missing required `k` parameter".to_string())?;
            k.parse::<usize>()
                .map_err(|_| "Topk query has an invalid `k` parameter".to_string())?;
            root = Self::push(
                &mut nodes,
                QueryPlanNode::LimitTopK {
                    input: root,
                    k,
                    grouping_labels: context.base.grouping_labels.clone(),
                    row_label_order: estimate_spec.row_label_order.clone(),
                },
            );
        }
        let materialize_metric_name = !query_time_aggregations.is_empty()
            && context.base.metadata.statistic_to_compute == Statistic::Topk
            && context.base.metadata.keep_metric_name;
        if materialize_metric_name {
            root = Self::push(
                &mut nodes,
                QueryPlanNode::Format {
                    input: root,
                    include_metric_name: true,
                    metric: context.base.metric.clone(),
                },
            );
        }
        let mut labels = context.base.metadata.query_output_labels.clone();
        for aggregation in query_time_aggregations {
            root = Self::push(
                &mut nodes,
                QueryPlanNode::AggregateVector {
                    input: root,
                    aggregation: aggregation.clone(),
                    input_labels: labels.clone(),
                },
            );
            labels = output_labels_for_aggregation(&labels, aggregation)?;
        }
        if options.format_output && !materialize_metric_name {
            root = Self::push(
                &mut nodes,
                QueryPlanNode::Format {
                    input: root,
                    include_metric_name: context.base.metadata.statistic_to_compute
                        == Statistic::Topk
                        && context.base.metadata.keep_metric_name,
                    metric: context.base.metric.clone(),
                },
            );
        }
        let plan = Self { nodes, root };
        plan.validate()?;
        Ok(plan)
    }

    /// Rejects plans whose node dependencies cannot be executed safely.
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.nodes.is_empty() {
            return Err("Query plan has no nodes".to_string());
        }
        if self.root.0 != self.nodes.len() - 1 {
            return Err(format!(
                "Query plan root n{} does not include every node",
                self.root.0
            ));
        }
        for (index, node) in self.nodes.iter().enumerate() {
            for input in node.inputs() {
                if input.0 >= index {
                    return Err(format!(
                        "Query plan node n{index} references unavailable input n{}",
                        input.0
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn execute<R: QueryPlanRuntime>(
        &self,
        runtime: &R,
    ) -> Result<R::Output, QueryPlanExecutionError<R::Error>> {
        self.validate()
            .map_err(QueryPlanExecutionError::InvalidPlan)?;
        let mut outputs: Vec<R::Output> = Vec::with_capacity(self.nodes.len());
        for (index, node) in self.nodes.iter().enumerate() {
            let inputs = node
                .inputs()
                .into_iter()
                .map(|input| outputs[input.0].clone())
                .collect::<Vec<_>>();
            debug!(
                node_id = index,
                node_kind = node.kind(),
                input_count = inputs.len(),
                "Executing native query plan node"
            );
            let output = runtime
                .execute_node(NodeId(index), node, &inputs)
                .map_err(|source| QueryPlanExecutionError::Node {
                    id: NodeId(index),
                    source,
                })?;
            debug!(
                node_id = index,
                node_kind = node.kind(),
                "Completed native query plan node"
            );
            outputs.push(output);
        }
        debug!(root_node_id = self.root.0, "Completed native query plan");
        Ok(outputs[self.root.0].clone())
    }

    fn push(nodes: &mut Vec<QueryPlanNode>, node: QueryPlanNode) -> NodeId {
        let id = NodeId(nodes.len());
        nodes.push(node);
        id
    }

    fn push_read(
        nodes: &mut Vec<QueryPlanNode>,
        query: &StoreQueryParams,
        role: StoreReadRole,
        strategy: StoreReadStrategy,
    ) -> NodeId {
        Self::push(
            nodes,
            QueryPlanNode::StoreRead {
                query: query.clone(),
                role,
                strategy,
            },
        )
    }

    fn push_prepare_buckets(nodes: &mut Vec<QueryPlanNode>, input: NodeId) -> NodeId {
        Self::push(nodes, QueryPlanNode::PrepareBuckets { input })
    }

    pub(crate) fn explain(&self) -> String {
        let mut lines = Vec::with_capacity(self.nodes.len() + 1);
        for (index, node) in self.nodes.iter().enumerate() {
            let line = match node {
                QueryPlanNode::StoreRead { query, role, strategy } => format!(
                    "n{index} StoreRead({}, role={role:?}, {}#{}, [{}, {}])",
                    Self::describe_read_strategy(strategy),
                    query.metric, query.aggregation_id, query.start_timestamp, query.end_timestamp
                ),
                QueryPlanNode::PrepareBuckets { input } => {
                    format!("n{index} PrepareBuckets(n{})", input.0)
                }
                QueryPlanNode::ResolveKeys { values, keys } => format!(
                    "n{index} ResolveKeys(values=n{}, keys={})",
                    values.0,
                    keys.map(|id| format!("n{}", id.0)).unwrap_or_else(|| "self".to_string())
                ),
                QueryPlanNode::Estimate {
                    input,
                    statistic,
                    query_kwargs,
                    spec,
                    ..
                } => {
                    let mut kwargs: Vec<_> = query_kwargs.iter().collect();
                    kwargs.sort_unstable_by_key(|(key, _)| *key);
                    format!(
                        "n{index} Estimate(n{}, {statistic}, {kwargs:?}, {})",
                        input.0,
                        Self::describe_timestamps(&spec.values.window.output_timestamps)
                    )
                },
                QueryPlanNode::LimitTopK { input, k, .. } => {
                    format!("n{index} LimitTopK(n{}, k={k})", input.0)
                }
                QueryPlanNode::AggregateVector { input, aggregation, .. } => {
                    format!("n{index} AggregateVector(n{}, {:?})", input.0, aggregation)
                }
                QueryPlanNode::Format { input, include_metric_name, metric } => format!(
                    "n{index} Format(n{}, include_metric_name={include_metric_name}) metric={metric}", input.0
                ),
            };
            lines.push(line);
        }
        lines.push(format!("root: n{}", self.root.0));
        lines.join("\n")
    }

    fn describe_read_strategy(strategy: &StoreReadStrategy) -> String {
        match strategy {
            StoreReadStrategy::WindowGrid => "WindowGrid".to_string(),
            StoreReadStrategy::SlidingExactCover(window) => format!(
                "SlidingExactCover(lookback={}ms, window={}ms, step={}ms, {})",
                window.lookback_ms,
                window.window_size_ms,
                window.bucket_step_ms,
                Self::describe_timestamps(&window.output_timestamps),
            ),
        }
    }

    fn describe_timestamps(timestamps: &[u64]) -> String {
        format!(
            "outputs=count:{}, first:{}, last:{}",
            timestamps.len(),
            timestamps.first().copied().unwrap_or(0),
            timestamps.last().copied().unwrap_or(0),
        )
    }
}

impl RangeEstimateSpec {
    pub(crate) fn compile(context: &RangeQueryExecutionContext) -> Result<Self, String> {
        let output_timestamps: Arc<[u64]> = Arc::from(context.output_timestamps.clone());
        let values = ValueEstimateInput {
            aggregation_type: context.base.agg_info.aggregation_type_for_value,
            window: WindowCompositionSpec {
                output_timestamps: Arc::clone(&output_timestamps),
                lookback_ms: (context.lookback_bucket_count as u64) * context.tumbling_window_ms,
                window_type: context.window_type,
                window_size_ms: context.window_size_ms,
                bucket_step_ms: context.tumbling_window_ms,
            },
        };
        let keys = match &context.base.store_plan.keys_query {
            None => KeyInputSpec::FromValues,
            Some(_) => KeyInputSpec::Separate {
                aggregation_type: context.base.agg_info.aggregation_type_for_key,
                window: WindowCompositionSpec {
                    output_timestamps,
                    lookback_ms: context
                        .keys_lookback_ms
                        .ok_or_else(|| "Separate keys query is missing its lookback".to_string())?,
                    window_type: context.keys_window_type.ok_or_else(|| {
                        "Separate keys query is missing its window type".to_string()
                    })?,
                    window_size_ms: context.keys_window_size_ms.ok_or_else(|| {
                        "Separate keys query is missing its window size".to_string()
                    })?,
                    bucket_step_ms: context.keys_tumbling_window_ms.ok_or_else(|| {
                        "Separate keys query is missing its bucket step".to_string()
                    })?,
                },
            },
        };
        Ok(Self {
            query_range_ms: context.query_range_ms,
            values,
            keys,
            grouping_labels: context.base.grouping_labels.clone(),
            aggregated_labels: context.base.aggregated_labels.clone(),
            row_label_order: crate::engines::simple_engine::SimpleEngine::topk_row_label_order(
                &context.base.metadata,
                &context.base.grouping_labels,
                &context.base.aggregated_labels,
            ),
        })
    }
}

impl QueryPlanNode {
    fn kind(&self) -> &'static str {
        match self {
            Self::StoreRead { .. } => "StoreRead",
            Self::PrepareBuckets { .. } => "PrepareBuckets",
            Self::ResolveKeys { .. } => "ResolveKeys",
            Self::Estimate { .. } => "Estimate",
            Self::AggregateVector { .. } => "AggregateVector",
            Self::LimitTopK { .. } => "LimitTopK",
            Self::Format { .. } => "Format",
        }
    }

    fn inputs(&self) -> Vec<NodeId> {
        match self {
            Self::StoreRead { .. } => Vec::new(),
            Self::PrepareBuckets { input, .. }
            | Self::Estimate { input, .. }
            | Self::AggregateVector { input, .. }
            | Self::LimitTopK { input, .. }
            | Self::Format { input, .. } => vec![*input],
            Self::ResolveKeys { values, keys } => {
                keys.iter().copied().fold(vec![*values], |mut inputs, key| {
                    inputs.push(key);
                    inputs
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::AggregationIdInfo;
    use crate::engines::simple_engine::{QueryExecutionContext, QueryMetadata, StoreQueryPlan};
    use asap_types::query_config::{
        QueryTimeAggregation, QueryTimeAggregationOperator, QueryTimeGrouping,
        QueryTimeGroupingMode,
    };
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::AggregationType;
    use std::cell::RefCell;
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
        context.keys_lookback_ms = Some(2_000);
        context.keys_window_size_ms = Some(1_000);
        context.keys_tumbling_window_ms = Some(500);

        let explanation = QueryPlan::compile_range(
            &context,
            PlanOptions {
                limit_topk: false,
                format_output: false,
            },
            &[],
        )
        .unwrap()
        .explain();

        assert!(explanation.contains("n4 ResolveKeys(values=n1, keys=n3)"));
        assert!(explanation.contains("role=Values"));
        assert!(explanation.contains("role=Keys"));
        assert!(explanation
            .contains("n2 StoreRead(SlidingExactCover(lookback=2000ms, window=1000ms, step=500ms"));
        assert!(explanation.ends_with("root: n5"));
    }

    #[test]
    fn nested_aggregation_pipeline_is_visible_as_ordered_plan_nodes() {
        let explanation = QueryPlan::compile_range(
            &context(),
            PlanOptions {
                limit_topk: false,
                format_output: true,
            },
            &[
                QueryTimeAggregation {
                    operator: QueryTimeAggregationOperator::Sum,
                    grouping: QueryTimeGrouping {
                        mode: QueryTimeGroupingMode::All,
                        labels: Vec::new(),
                    },
                    parameter: None,
                },
                QueryTimeAggregation {
                    operator: QueryTimeAggregationOperator::Max,
                    grouping: QueryTimeGrouping {
                        mode: QueryTimeGroupingMode::All,
                        labels: Vec::new(),
                    },
                    parameter: None,
                },
            ],
        )
        .unwrap()
        .explain();

        assert!(explanation.contains("n4 AggregateVector(n3, QueryTimeAggregation { operator: Sum"));
        assert!(explanation.contains("n5 AggregateVector(n4, QueryTimeAggregation { operator: Max"));
        assert!(explanation.contains("n6 Format(n5"));
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
            &[],
        )
        .unwrap()
        .explain();

        assert!(explanation.contains("outputs=count:3, first:1000, last:3000"));
    }

    #[test]
    fn sliding_value_read_uses_bucket_lookback() {
        let mut context = context();
        context.window_type = WindowType::Sliding;
        context.query_range_ms = 1_500;
        context.lookback_bucket_count = 1;

        let plan = QueryPlan::compile_range(
            &context,
            PlanOptions {
                limit_topk: false,
                format_output: false,
            },
            &[],
        )
        .unwrap();

        match &plan.nodes[0] {
            QueryPlanNode::StoreRead {
                strategy: StoreReadStrategy::SlidingExactCover(window),
                ..
            } => assert_eq!(window.lookback_ms, 1_000),
            _ => panic!("value read must use a sliding exact cover"),
        }

        match &plan.nodes[3] {
            QueryPlanNode::Estimate { spec, .. } => {
                assert_eq!(spec.values.window.lookback_ms, 1_000);
            }
            _ => panic!("fourth node must estimate the resolved value read"),
        }
    }

    #[test]
    fn rejects_separate_keys_with_incomplete_window_settings() {
        let mut context = context();
        context.base.store_plan.keys_query = Some(context.base.store_plan.values_query.clone());
        context.keys_window_type = Some(WindowType::Sliding);

        let error = QueryPlan::compile_range(
            &context,
            PlanOptions {
                limit_topk: false,
                format_output: false,
            },
            &[],
        )
        .expect_err("incomplete separate key settings must fail loudly");

        assert_eq!(error, "Separate keys query is missing its lookback");
    }

    struct PlanOwnedReadRuntime(RefCell<Vec<(StoreReadRole, StoreReadStrategy)>>);

    impl QueryPlanRuntime for PlanOwnedReadRuntime {
        type Output = usize;
        type Error = std::convert::Infallible;

        fn execute_node(
            &self,
            _id: NodeId,
            node: &QueryPlanNode,
            inputs: &[Self::Output],
        ) -> Result<Self::Output, Self::Error> {
            if let QueryPlanNode::StoreRead { role, strategy, .. } = node {
                self.0.borrow_mut().push((*role, strategy.clone()));
            }
            Ok(1 + inputs.iter().sum::<usize>())
        }
    }

    #[test]
    fn compiled_plan_executes_distinct_value_and_key_read_specs_without_context() {
        let mut context = context();
        context.window_type = WindowType::Sliding;
        context.lookback_bucket_count = 2;
        context.base.store_plan.keys_query = Some(StoreQueryParams {
            metric: "requests".into(),
            aggregation_id: 8,
            start_timestamp: 0,
            end_timestamp: 1_000,
        });
        context.keys_window_type = Some(WindowType::Tumbling);
        context.keys_lookback_ms = Some(3_000);
        context.keys_window_size_ms = Some(1_000);
        context.keys_tumbling_window_ms = Some(1_000);

        let plan = QueryPlan::compile_range(
            &context,
            PlanOptions {
                limit_topk: false,
                format_output: false,
            },
            &[],
        )
        .unwrap();
        let runtime = PlanOwnedReadRuntime(RefCell::new(Vec::new()));

        plan.execute(&runtime)
            .expect("compiled plan must execute with a context-free runtime");

        assert_eq!(
            runtime.0.into_inner(),
            vec![
                (
                    StoreReadRole::Values,
                    StoreReadStrategy::SlidingExactCover(WindowCompositionSpec {
                        output_timestamps: Arc::from([1_000]),
                        lookback_ms: 2_000,
                        window_type: WindowType::Sliding,
                        window_size_ms: 1_000,
                        bucket_step_ms: 1_000,
                    }),
                ),
                (StoreReadRole::Keys, StoreReadStrategy::WindowGrid),
            ]
        );
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
            &[],
        )
        .unwrap()
        .explain();

        assert!(explanation.contains("LimitTopK(n3, k=3)"));
        assert!(explanation.contains("Format(n4, include_metric_name=true)"));
        assert!(explanation.ends_with("root: n5"));
    }

    #[test]
    fn rejects_topk_without_a_limit() {
        let mut context = context();
        context.base.metadata.statistic_to_compute = Statistic::Topk;

        let error = QueryPlan::compile_range(
            &context,
            PlanOptions {
                limit_topk: true,
                format_output: false,
            },
            &[],
        )
        .expect_err("topk plan without k must fail loudly");

        assert_eq!(error, "Topk query is missing required `k` parameter");
    }

    #[test]
    fn rejects_a_node_that_references_a_later_node() {
        let plan = QueryPlan {
            nodes: vec![QueryPlanNode::Estimate {
                input: NodeId(1),
                statistic: Statistic::Sum,
                query_kwargs: HashMap::new(),
                output_labels: KeyByLabelNames::empty(),
                spec: RangeEstimateSpec::compile(&context()).unwrap(),
            }],
            root: NodeId(0),
        };

        assert_eq!(
            plan.validate().expect_err("invalid plan must fail loudly"),
            "Query plan node n0 references unavailable input n1"
        );
    }

    struct RecordingRuntime(RefCell<Vec<usize>>);

    impl QueryPlanRuntime for RecordingRuntime {
        type Output = usize;
        type Error = std::convert::Infallible;

        fn execute_node(
            &self,
            id: NodeId,
            _node: &QueryPlanNode,
            inputs: &[Self::Output],
        ) -> Result<Self::Output, Self::Error> {
            self.0.borrow_mut().push(id.0);
            Ok(1 + inputs.iter().sum::<usize>())
        }
    }

    #[test]
    fn executes_nodes_once_in_dependency_order() {
        let plan = QueryPlan {
            nodes: vec![
                QueryPlanNode::StoreRead {
                    query: StoreQueryParams {
                        metric: "requests".into(),
                        aggregation_id: 7,
                        start_timestamp: 0,
                        end_timestamp: 1,
                    },
                    role: StoreReadRole::Values,
                    strategy: StoreReadStrategy::WindowGrid,
                },
                QueryPlanNode::PrepareBuckets { input: NodeId(0) },
            ],
            root: NodeId(1),
        };
        let runtime = RecordingRuntime(RefCell::new(Vec::new()));
        assert_eq!(plan.execute(&runtime).unwrap(), 2);
        assert_eq!(*runtime.0.borrow(), vec![0, 1]);
    }
}
