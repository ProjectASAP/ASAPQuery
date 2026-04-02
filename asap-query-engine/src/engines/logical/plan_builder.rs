//! Plan Builder Module
//!
//! Converts QueryExecutionContext to DataFusion LogicalPlan for OnlySpatial queries.
//! This enables plan-based execution as an alternative to the existing pipeline.

use arrow::datatypes::{DataType, Field};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion_summary_library::{
    InferOperation, PrecomputedSummaryRead, SketchType, SummaryInfer, SummaryMergeMultiple,
};
use promql_utilities::query_logics::enums::Statistic;
use std::sync::Arc;

use crate::engines::simple_engine::{QueryExecutionContext, StoreQueryParams};

/// Extension trait for building DataFusion logical plans from QueryExecutionContext
impl QueryExecutionContext {
    /// Convert this execution context to a DataFusion LogicalPlan.
    ///
    /// The resulting plan structure for single-population queries:
    /// ```text
    /// SummaryInfer (extract values from summaries)
    ///   └─► SummaryMergeMultiple (merge summaries with same group key)
    ///         └─► PrecomputedSummaryRead (read from store)
    /// ```
    ///
    /// For multi-population queries (separate keys_query):
    /// ```text
    /// SummaryInfer (dual-input: value sketch + keys enumeration)
    ///   ├─► input 0: SummaryMergeMultiple (values)
    ///   │     └─► PrecomputedSummaryRead (values agg_id)
    ///   └─► input 1: SummaryMergeMultiple (keys)
    ///         └─► PrecomputedSummaryRead (keys agg_id)
    /// ```
    pub fn to_logical_plan(&self) -> Result<LogicalPlan, DataFusionError> {
        let has_separate_keys = self.store_plan.keys_query.is_some()
            && self.agg_info.aggregation_id_for_key != self.agg_info.aggregation_id_for_value;

        // 1. Map aggregation type to SummaryType (SketchType) for values
        let summary_type = self.map_aggregation_type_to_summary_type()?;

        // Determine labels for the values branch (store read/merge).
        // For multi-population (dual-input or self-keyed): use grouping_labels (store GROUP BY)
        // For single-population: use query_output_labels
        let has_aggregated_labels = !self.aggregated_labels.labels.is_empty();
        let values_labels = if has_separate_keys || has_aggregated_labels {
            self.grouping_labels.labels.to_vec()
        } else {
            self.get_output_label_names()
        };

        // Sub-key labels come from aggregated_labels (labels that key the accumulator internally)
        let sub_key_labels: Vec<String> = self.aggregated_labels.labels.to_vec();

        // 2. Build values branch: Read -> Merge
        let values_merge_plan = self.build_read_merge_branch(
            &self.store_plan.values_query,
            &values_labels,
            &summary_type,
        )?;

        // 3. Map statistic to InferOperation
        let infer_operation = self.map_statistic_to_infer_operation()?;

        if has_separate_keys {
            let keys_query = self.store_plan.keys_query.as_ref().unwrap();

            // Map keys aggregation type to SketchType
            let keys_summary_type = self.map_key_aggregation_type_to_summary_type()?;

            // Build keys branch: Read -> Merge (using same spatial labels)
            let keys_merge_plan =
                self.build_read_merge_branch(keys_query, &values_labels, &keys_summary_type)?;

            // Create dual-input SummaryInfer
            let infer = SummaryInfer::new(
                Arc::new(values_merge_plan),
                vec![infer_operation],
                vec!["value".to_string()],
            )
            .map_err(|e| DataFusionError::Plan(format!("Failed to create SummaryInfer: {}", e)))?
            .with_keys_input(Arc::new(keys_merge_plan))
            .with_group_key_columns(sub_key_labels, None)
            .map_err(|e| {
                DataFusionError::Plan(format!("Failed to set group_key_columns: {}", e))
            })?;

            Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(infer),
            }))
        } else {
            // Single-input path
            let mut infer = SummaryInfer::new(
                Arc::new(values_merge_plan),
                vec![infer_operation],
                vec!["value".to_string()],
            )
            .map_err(|e| DataFusionError::Plan(format!("Failed to create SummaryInfer: {}", e)))?;

            if !sub_key_labels.is_empty() {
                // Self-keyed multi-pop: set sub-key columns so the output schema
                // includes them and the physical operator knows to enumerate keys.
                infer = infer
                    .with_group_key_columns(sub_key_labels, None)
                    .map_err(|e| {
                        DataFusionError::Plan(format!("Failed to set group_key_columns: {}", e))
                    })?;
            }

            Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(infer),
            }))
        }
    }

    /// Build a Read -> Merge branch for a given store query.
    fn build_read_merge_branch(
        &self,
        query_params: &StoreQueryParams,
        labels: &[String],
        summary_type: &SketchType,
    ) -> Result<LogicalPlan, DataFusionError> {
        let read_schema = self.build_read_schema(labels)?;

        let read = PrecomputedSummaryRead::new(
            self.metric.clone(),
            query_params.aggregation_id,
            query_params.start_timestamp,
            query_params.end_timestamp,
            query_params.is_exact_query,
            labels.to_vec(),
            summary_type.clone(),
            read_schema,
        );
        let read_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(read),
        });

        let merge = SummaryMergeMultiple::new(
            Arc::new(read_plan),
            labels.to_vec(),
            "sketch".to_string(),
            summary_type.clone(),
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(merge),
        }))
    }

    /// Get output label names from the query metadata
    fn get_output_label_names(&self) -> Vec<String> {
        self.metadata.query_output_labels.labels.to_vec()
    }

    /// Build schema for PrecomputedSummaryRead: [label columns, sketch column]
    fn build_read_schema(&self, output_labels: &[String]) -> Result<DFSchemaRef, DataFusionError> {
        let mut fields: Vec<(Option<datafusion::common::TableReference>, Arc<Field>)> = Vec::new();

        // Add label columns (Utf8, nullable)
        for label in output_labels {
            fields.push((None, Arc::new(Field::new(label, DataType::Utf8, true))));
        }

        // Add sketch column (Binary, not nullable)
        fields.push((
            None,
            Arc::new(Field::new("sketch", DataType::Binary, false)),
        ));

        let schema = DFSchema::new_with_metadata(fields, Default::default())
            .map_err(|e| DataFusionError::Plan(format!("Failed to create read schema: {}", e)))?;

        Ok(Arc::new(schema))
    }

    /// Map Statistic enum to InferOperation
    pub(crate) fn map_statistic_to_infer_operation(
        &self,
    ) -> Result<InferOperation, DataFusionError> {
        match self.metadata.statistic_to_compute {
            Statistic::Sum => Ok(InferOperation::ExtractSum),
            Statistic::Min => Ok(InferOperation::ExtractMin),
            Statistic::Max => Ok(InferOperation::ExtractMax),
            Statistic::Count => Ok(InferOperation::ExtractCount),
            Statistic::Increase => Ok(InferOperation::ExtractIncrease),
            Statistic::Rate => Ok(InferOperation::ExtractRate),
            Statistic::Quantile => {
                // Extract quantile parameter from query_kwargs
                let q = self
                    .metadata
                    .query_kwargs
                    .get("quantile")
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.5);
                Ok(InferOperation::quantile(q))
            }
            Statistic::Cardinality => Ok(InferOperation::CountDistinct),
            Statistic::Topk => {
                // Extract k parameter from query_kwargs
                let k = self
                    .metadata
                    .query_kwargs
                    .get("k")
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(10);
                Ok(InferOperation::TopK(k))
            }
        }
    }

    /// Map aggregation type string to SketchType (SummaryType) for the value accumulator
    fn map_aggregation_type_to_summary_type(&self) -> Result<SketchType, DataFusionError> {
        Self::agg_type_str_to_sketch_type(&self.agg_info.aggregation_type_for_value)
    }

    /// Map aggregation type string to SketchType (SummaryType) for the key accumulator
    fn map_key_aggregation_type_to_summary_type(&self) -> Result<SketchType, DataFusionError> {
        Self::agg_type_str_to_sketch_type(&self.agg_info.aggregation_type_for_key)
    }

    fn agg_type_str_to_sketch_type(agg_type: &str) -> Result<SketchType, DataFusionError> {
        match agg_type {
            "SumAggregator" | "SumAccumulator" | "Sum" => Ok(SketchType::Sum),
            "IncreaseAggregator" | "IncreaseAccumulator" | "Increase" => Ok(SketchType::Increase),
            "MinMaxAggregator" | "MinMaxAccumulator" | "MinMax" => Ok(SketchType::MinMax),
            "MultipleSumAccumulator" | "MultipleSum" => Ok(SketchType::MultipleSum),
            "MultipleIncreaseAccumulator" | "MultipleIncrease" => Ok(SketchType::MultipleIncrease),
            "MultipleMinMaxAccumulator" | "MultipleMinMax" => Ok(SketchType::MultipleMinMax),
            "DeltaSetAggregator" => Ok(SketchType::DeltaSetAggregator),
            "SetAggregator" => Ok(SketchType::SetAggregator),
            "DatasketchesKLLAccumulator" | "DatasketchesKLL" | "KLL" => Ok(SketchType::KLL),
            "HydraKllSketchAccumulator" | "HydraKLL" => Ok(SketchType::HydraKLL),
            "CountMinSketchAccumulator" | "CountMinSketch" => Ok(SketchType::CountMinSketch),
            "HyperLogLog" | "HLL" => Ok(SketchType::HLL),
            _ => Err(DataFusionError::Plan(format!(
                "Unknown aggregation type: {}",
                agg_type
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::AggregationIdInfo;
    use crate::engines::simple_engine::{QueryMetadata, StoreQueryParams, StoreQueryPlan};
    use promql_utilities::data_model::KeyByLabelNames;

    use std::collections::HashMap;

    fn create_test_context(
        metric: &str,
        statistic: Statistic,
        output_labels: Vec<&str>,
        aggregation_type: &str,
    ) -> QueryExecutionContext {
        create_test_context_with_keys(metric, statistic, output_labels, aggregation_type, None, "")
    }

    fn create_test_context_with_kwargs(
        metric: &str,
        statistic: Statistic,
        output_labels: Vec<&str>,
        aggregation_type: &str,
        kwargs: HashMap<String, String>,
    ) -> QueryExecutionContext {
        let mut ctx = create_test_context_with_keys(
            metric,
            statistic,
            output_labels,
            aggregation_type,
            None,
            "",
        );
        ctx.metadata.query_kwargs = kwargs;
        ctx
    }

    fn create_test_context_with_keys(
        metric: &str,
        statistic: Statistic,
        output_labels: Vec<&str>,
        aggregation_type_for_value: &str,
        keys_query: Option<StoreQueryParams>,
        aggregation_type_for_key: &str,
    ) -> QueryExecutionContext {
        // Default: grouping_labels == query_output_labels
        create_test_context_with_keys_and_grouping(
            metric,
            statistic,
            output_labels.clone(),
            aggregation_type_for_value,
            keys_query,
            aggregation_type_for_key,
            output_labels,
        )
    }

    fn create_test_context_with_keys_and_grouping(
        metric: &str,
        statistic: Statistic,
        output_labels: Vec<&str>,
        aggregation_type_for_value: &str,
        keys_query: Option<StoreQueryParams>,
        aggregation_type_for_key: &str,
        grouping_label_strs: Vec<&str>,
    ) -> QueryExecutionContext {
        // Default: aggregated_labels = output_labels - grouping_labels
        let aggregated: Vec<&str> = output_labels
            .iter()
            .filter(|l| !grouping_label_strs.contains(l))
            .copied()
            .collect();
        create_test_context_full(
            metric,
            statistic,
            output_labels,
            aggregation_type_for_value,
            keys_query,
            aggregation_type_for_key,
            grouping_label_strs,
            aggregated,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_test_context_full(
        metric: &str,
        statistic: Statistic,
        output_labels: Vec<&str>,
        aggregation_type_for_value: &str,
        keys_query: Option<StoreQueryParams>,
        aggregation_type_for_key: &str,
        grouping_label_strs: Vec<&str>,
        aggregated_label_strs: Vec<&str>,
    ) -> QueryExecutionContext {
        let aggregation_id_for_key = match &keys_query {
            Some(kq) => kq.aggregation_id,
            None => 42, // same as value when no separate keys
        };
        let output_labels_vec: Vec<String> = output_labels.into_iter().map(String::from).collect();
        let query_output_labels = KeyByLabelNames {
            labels: output_labels_vec,
        };
        let grouping_labels = KeyByLabelNames {
            labels: grouping_label_strs.into_iter().map(String::from).collect(),
        };
        let aggregated_labels = KeyByLabelNames {
            labels: aggregated_label_strs
                .into_iter()
                .map(String::from)
                .collect(),
        };
        QueryExecutionContext {
            metric: metric.to_string(),
            metadata: QueryMetadata {
                query_output_labels,
                statistic_to_compute: statistic,
                query_kwargs: HashMap::new(),
            },
            store_plan: StoreQueryPlan {
                values_query: StoreQueryParams {
                    metric: metric.to_string(),
                    aggregation_id: 42,
                    start_timestamp: 1000,
                    end_timestamp: 2000,
                    is_exact_query: true,
                },
                keys_query,
            },
            agg_info: AggregationIdInfo {
                aggregation_id_for_key,
                aggregation_id_for_value: 42,
                aggregation_type_for_key: aggregation_type_for_key.to_string(),
                aggregation_type_for_value: aggregation_type_for_value.to_string(),
            },
            do_merge: false,
            spatial_filter: String::new(),
            query_time: 2000,
            grouping_labels,
            aggregated_labels,
        }
    }

    #[test]
    fn test_to_logical_plan_creates_correct_structure() {
        let context = create_test_context(
            "http_requests",
            Statistic::Sum,
            vec!["host"],
            "SumAggregator",
        );

        let plan = context.to_logical_plan().unwrap();

        // Root should be SummaryInfer
        match &plan {
            LogicalPlan::Extension(ext) => {
                assert_eq!(ext.node.name(), "SummaryInfer");
            }
            _ => panic!("Expected Extension node"),
        }
    }

    #[test]
    fn test_to_logical_plan_with_multiple_labels() {
        let context = create_test_context(
            "http_requests",
            Statistic::Sum,
            vec!["host", "region", "service"],
            "SumAggregator",
        );

        let plan = context.to_logical_plan().unwrap();

        // Verify plan was created successfully
        assert!(matches!(plan, LogicalPlan::Extension(_)));
    }

    #[test]
    fn test_map_statistic_to_infer_operation() {
        let context = create_test_context("test", Statistic::Sum, vec!["host"], "SumAggregator");
        assert!(matches!(
            context.map_statistic_to_infer_operation().unwrap(),
            InferOperation::ExtractSum
        ));

        let context = create_test_context("test", Statistic::Min, vec!["host"], "MinMaxAggregator");
        assert!(matches!(
            context.map_statistic_to_infer_operation().unwrap(),
            InferOperation::ExtractMin
        ));

        let context = create_test_context("test", Statistic::Max, vec!["host"], "MinMaxAggregator");
        assert!(matches!(
            context.map_statistic_to_infer_operation().unwrap(),
            InferOperation::ExtractMax
        ));
    }

    #[test]
    fn test_map_aggregation_type_to_summary_type() {
        let context = create_test_context("test", Statistic::Sum, vec!["host"], "SumAggregator");
        assert_eq!(
            context.map_aggregation_type_to_summary_type().unwrap(),
            SketchType::Sum
        );

        let context = create_test_context(
            "test",
            Statistic::Increase,
            vec!["host"],
            "IncreaseAggregator",
        );
        assert_eq!(
            context.map_aggregation_type_to_summary_type().unwrap(),
            SketchType::Increase
        );

        let context = create_test_context(
            "test",
            Statistic::Quantile,
            vec!["host"],
            "DatasketchesKLLAccumulator",
        );
        assert_eq!(
            context.map_aggregation_type_to_summary_type().unwrap(),
            SketchType::KLL
        );
    }

    // ========================================================================
    // Helper to walk the plan tree and collect node names top-down
    // ========================================================================

    fn collect_plan_node_names(plan: &LogicalPlan) -> Vec<String> {
        let mut names = Vec::new();
        collect_plan_node_names_recursive(plan, &mut names);
        names
    }

    fn collect_plan_node_names_recursive(plan: &LogicalPlan, names: &mut Vec<String>) {
        match plan {
            LogicalPlan::Extension(ext) => {
                names.push(ext.node.name().to_string());
                for input in ext.node.inputs() {
                    collect_plan_node_names_recursive(input, names);
                }
            }
            _ => {
                names.push(
                    format!("{:?}", plan)
                        .split('(')
                        .next()
                        .unwrap_or("Unknown")
                        .to_string(),
                );
            }
        }
    }

    /// Helper to extract the SummaryMergeMultiple node from a plan tree
    fn extract_merge_node(plan: &LogicalPlan) -> Option<&SummaryMergeMultiple> {
        match plan {
            LogicalPlan::Extension(ext) => {
                if let Some(merge) = ext.node.as_any().downcast_ref::<SummaryMergeMultiple>() {
                    return Some(merge);
                }
                for input in ext.node.inputs() {
                    if let Some(merge) = extract_merge_node(input) {
                        return Some(merge);
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Helper to extract the PrecomputedSummaryRead node from a plan tree
    fn extract_read_node(plan: &LogicalPlan) -> Option<&PrecomputedSummaryRead> {
        match plan {
            LogicalPlan::Extension(ext) => {
                if let Some(read) = ext.node.as_any().downcast_ref::<PrecomputedSummaryRead>() {
                    return Some(read);
                }
                for input in ext.node.inputs() {
                    if let Some(read) = extract_read_node(input) {
                        return Some(read);
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Helper to extract the SummaryInfer node from the root of a plan
    fn extract_infer_node(plan: &LogicalPlan) -> Option<&SummaryInfer> {
        match plan {
            LogicalPlan::Extension(ext) => ext.node.as_any().downcast_ref::<SummaryInfer>(),
            _ => None,
        }
    }

    /// Count PrecomputedSummaryRead nodes in the plan tree
    fn count_read_nodes(plan: &LogicalPlan) -> usize {
        let names = collect_plan_node_names(plan);
        names
            .iter()
            .filter(|n| *n == "PrecomputedSummaryRead")
            .count()
    }

    // ========================================================================
    // MultipleSumAccumulator (HydraSum) tests
    // ========================================================================

    #[test]
    fn test_multiple_sum_accumulator_maps_to_hydra_sum() {
        let context = create_test_context(
            "http_requests",
            Statistic::Sum,
            vec!["host"],
            "MultipleSumAccumulator",
        );
        assert_eq!(
            context.map_aggregation_type_to_summary_type().unwrap(),
            SketchType::MultipleSum
        );
    }

    #[test]
    fn test_multiple_sum_accumulator_plan_builds() {
        // MultipleSumAccumulator is a Hydra (multi-population) accumulator.
        // The current plan only builds a single-population SummaryInfer, which
        // won't correctly query sub-populations at execution time.
        let context = create_test_context(
            "http_requests",
            Statistic::Sum,
            vec!["host"],
            "MultipleSumAccumulator",
        );

        let plan = context.to_logical_plan().unwrap();
        let node_names = collect_plan_node_names(&plan);
        assert_eq!(
            node_names,
            vec![
                "SummaryInfer",
                "SummaryMergeMultiple",
                "PrecomputedSummaryRead"
            ]
        );

        // Verify the summary type propagates correctly through the plan
        let merge = extract_merge_node(&plan).expect("Should have a SummaryMergeMultiple node");
        assert_eq!(*merge.summary_type(), SketchType::MultipleSum);

        let read = extract_read_node(&plan).expect("Should have a PrecomputedSummaryRead node");
        assert_eq!(*read.summary_type(), SketchType::MultipleSum);
    }

    #[test]
    fn test_multiple_sum_accumulator_single_pop_no_subkeys() {
        // MultipleSumAccumulator without a separate keys_query stays single-population.
        // No sub-key columns because there's no keys branch to enumerate from.
        // To properly query Hydra types, a keys_query with a DeltaSetAggregator
        // should be provided — see test_delta_set_aggregator_dual_input_plan.
        let context = create_test_context(
            "http_requests",
            Statistic::Sum,
            vec!["host"],
            "MultipleSumAccumulator",
        );

        let plan = context.to_logical_plan().unwrap();

        let infer = extract_infer_node(&plan).expect("Root should be SummaryInfer");
        assert!(
            infer.group_key_columns.is_empty(),
            "Single-pop (no keys_query): group_key_columns should be empty"
        );
        assert!(
            infer.keys_input.is_none(),
            "Single-pop: should not have keys_input"
        );
    }

    // ========================================================================
    // CountMinSketch for values_plan tests
    // ========================================================================

    #[test]
    fn test_count_min_sketch_maps_correctly() {
        let context = create_test_context(
            "http_requests",
            Statistic::Count,
            vec!["host"],
            "CountMinSketchAccumulator",
        );
        assert_eq!(
            context.map_aggregation_type_to_summary_type().unwrap(),
            SketchType::CountMinSketch
        );
    }

    #[test]
    fn test_count_min_sketch_plan_builds() {
        // CountMinSketch is a multi-population frequency sketch.
        // Like Hydra types, it requires a sub-key to query (FrequencyEstimate, etc.)
        let context = create_test_context(
            "http_requests",
            Statistic::Count,
            vec!["host"],
            "CountMinSketchAccumulator",
        );

        let plan = context.to_logical_plan().unwrap();
        let node_names = collect_plan_node_names(&plan);
        assert_eq!(
            node_names,
            vec![
                "SummaryInfer",
                "SummaryMergeMultiple",
                "PrecomputedSummaryRead"
            ]
        );

        // Verify summary type
        let merge = extract_merge_node(&plan).expect("Should have SummaryMergeMultiple");
        assert_eq!(*merge.summary_type(), SketchType::CountMinSketch);
    }

    #[test]
    fn test_count_min_sketch_single_pop_no_subkeys() {
        // CountMinSketch without a separate keys_query stays single-population.
        // Same as MultipleSumAccumulator — need a keys_query for dual-input.
        let context = create_test_context(
            "http_requests",
            Statistic::Count,
            vec!["host"],
            "CountMinSketchAccumulator",
        );

        let plan = context.to_logical_plan().unwrap();

        let infer = extract_infer_node(&plan).expect("Root should be SummaryInfer");
        assert!(infer.group_key_columns.is_empty());
        assert!(infer.keys_input.is_none());
    }

    // ========================================================================
    // DeltaSetAggregator for keys_plan tests
    // ========================================================================

    #[test]
    fn test_delta_set_aggregator_dual_input_plan() {
        // When keys_query is set with a different agg_id (DeltaSetAggregator for key
        // enumeration), to_logical_plan() builds a dual-input SummaryInfer with both
        // a values branch and a keys branch.
        let keys_query = StoreQueryParams {
            metric: "http_requests".to_string(),
            aggregation_id: 99, // Different agg ID for keys
            start_timestamp: 0,
            end_timestamp: 2000,
            is_exact_query: false,
        };
        let context = create_test_context_with_keys(
            "http_requests",
            Statistic::Sum,
            vec!["host"],
            "MultipleSumAccumulator", // values use Hydra
            Some(keys_query),
            "DeltaSetAggregator", // keys use DeltaSet
        );

        let plan = context.to_logical_plan().unwrap();

        // The plan tree should now have 5 nodes: SummaryInfer with 2 branches
        let node_names = collect_plan_node_names(&plan);
        assert_eq!(
            node_names,
            vec![
                "SummaryInfer",
                "SummaryMergeMultiple",   // values branch
                "PrecomputedSummaryRead", // values read
                "SummaryMergeMultiple",   // keys branch
                "PrecomputedSummaryRead", // keys read
            ]
        );

        // Verify there are 2 PrecomputedSummaryRead nodes
        assert_eq!(count_read_nodes(&plan), 2);

        // The SummaryInfer should have a keys_input
        let infer = extract_infer_node(&plan).expect("Root should be SummaryInfer");
        assert!(
            infer.keys_input.is_some(),
            "SummaryInfer should have keys_input"
        );
    }

    // ========================================================================
    // HydraKLL for values_plan + DeltaSetAggregator for keys_plan
    // ========================================================================

    #[test]
    fn test_hydra_kll_with_delta_set_keys_plan_builds() {
        // HydraKLL for quantile queries with DeltaSetAggregator for key enumeration.
        // This is a realistic configuration: HydraKLL stores per-key quantile sketches,
        // and DeltaSetAggregator tracks which keys exist.
        // grouping_labels = ["host"], sub-keys = ["endpoint"]
        let mut kwargs = HashMap::new();
        kwargs.insert("quantile".to_string(), "0.95".to_string());

        let keys_query = StoreQueryParams {
            metric: "request_duration".to_string(),
            aggregation_id: 99,
            start_timestamp: 0,
            end_timestamp: 2000,
            is_exact_query: false,
        };
        let mut context = create_test_context_with_keys_and_grouping(
            "request_duration",
            Statistic::Quantile,
            vec!["host", "endpoint"], // query_output_labels
            "HydraKllSketchAccumulator",
            Some(keys_query),
            "DeltaSetAggregator",
            vec!["host"], // grouping_labels (spatial)
        );
        context.metadata.query_kwargs = kwargs;

        let plan = context.to_logical_plan().unwrap();

        // Verify dual-input plan structure (5 nodes)
        let node_names = collect_plan_node_names(&plan);
        assert_eq!(
            node_names,
            vec![
                "SummaryInfer",
                "SummaryMergeMultiple",   // values branch
                "PrecomputedSummaryRead", // values read
                "SummaryMergeMultiple",   // keys branch
                "PrecomputedSummaryRead", // keys read
            ]
        );

        // Verify types propagate in the values branch
        let merge = extract_merge_node(&plan).expect("Should have SummaryMergeMultiple");
        assert_eq!(*merge.summary_type(), SketchType::HydraKLL);

        let read = extract_read_node(&plan).expect("Should have PrecomputedSummaryRead");
        assert_eq!(*read.summary_type(), SketchType::HydraKLL);
        // Values branch uses grouping_labels, not query_output_labels
        assert_eq!(read.output_labels(), &["host"]);

        // Verify SummaryInfer has sub-key columns
        let infer = extract_infer_node(&plan).expect("Root should be SummaryInfer");
        assert_eq!(infer.group_key_columns, vec!["endpoint"]);
    }

    #[test]
    fn test_hydra_kll_with_delta_set_keys_dual_input_with_subkeys() {
        // HydraKLL with DeltaSetAggregator keys, with sub-key columns.
        // output_labels = ["host", "endpoint"], grouping_labels = ["host"]
        // => sub_key_labels = ["endpoint"]
        let keys_query = StoreQueryParams {
            metric: "request_duration".to_string(),
            aggregation_id: 99,
            start_timestamp: 0,
            end_timestamp: 2000,
            is_exact_query: false,
        };
        let context = create_test_context_with_keys_and_grouping(
            "request_duration",
            Statistic::Quantile,
            vec!["host", "endpoint"], // query_output_labels
            "HydraKllSketchAccumulator",
            Some(keys_query),
            "DeltaSetAggregator",
            vec!["host"], // grouping_labels (spatial store labels)
        );

        let plan = context.to_logical_plan().unwrap();

        // Plan should have 2 PrecomputedSummaryRead nodes
        assert_eq!(count_read_nodes(&plan), 2);

        // SummaryInfer should have keys_input and group_key_columns = ["endpoint"]
        let infer = extract_infer_node(&plan).expect("Root should be SummaryInfer");
        assert!(infer.keys_input.is_some(), "Should have keys_input");
        assert_eq!(
            infer.group_key_columns,
            vec!["endpoint"],
            "Sub-key columns should be query_output_labels minus grouping_labels"
        );
    }

    // ========================================================================
    // Quantile kwargs propagation test
    // ========================================================================

    #[test]
    fn test_quantile_kwargs_propagate_to_infer_operation() {
        let mut kwargs = HashMap::new();
        kwargs.insert("quantile".to_string(), "0.99".to_string());
        let context = create_test_context_with_kwargs(
            "latency",
            Statistic::Quantile,
            vec!["host"],
            "DatasketchesKLLAccumulator",
            kwargs,
        );

        match context.map_statistic_to_infer_operation().unwrap() {
            InferOperation::Quantile(q) => {
                // 0.99 * 10000 = 9900
                assert_eq!(q, 9900, "Expected q=9900 (0.99), got {}", q);
            }
            other => panic!("Expected Quantile, got {:?}", other),
        }
    }

    #[test]
    fn test_quantile_defaults_to_median_when_no_kwargs() {
        let context = create_test_context(
            "latency",
            Statistic::Quantile,
            vec!["host"],
            "DatasketchesKLLAccumulator",
        );

        match context.map_statistic_to_infer_operation().unwrap() {
            InferOperation::Quantile(q) => {
                // 0.5 * 10000 = 5000
                assert_eq!(q, 5000, "Expected q=5000 (0.5), got {}", q);
            }
            other => panic!("Expected Quantile, got {:?}", other),
        }
    }
}
