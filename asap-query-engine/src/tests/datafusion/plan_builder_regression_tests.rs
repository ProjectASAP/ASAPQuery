//! Plan Builder Regression Tests
//!
//! Tests covering gaps in the existing plan_builder.rs inline tests:
//! all Statistic variants, kwargs propagation, error paths, edge cases.

#[cfg(test)]
mod tests {
    use crate::data_model::AggregationIdInfo;
    use crate::engines::simple_engine::{
        QueryExecutionContext, QueryMetadata, StoreQueryParams, StoreQueryPlan,
    };
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::{AggregationType, Statistic};
    use std::collections::HashMap;

    fn create_context(
        statistic: Statistic,
        aggregation_type: AggregationType,
        output_labels: Vec<&str>,
        kwargs: HashMap<String, String>,
    ) -> QueryExecutionContext {
        let output_labels_vec: Vec<String> = output_labels.into_iter().map(String::from).collect();
        let labels = KeyByLabelNames {
            labels: output_labels_vec,
        };
        QueryExecutionContext {
            metric: "test_metric".to_string(),
            metadata: QueryMetadata {
                query_output_labels: labels.clone(),
                statistic_to_compute: statistic,
                query_kwargs: kwargs,
            },
            store_plan: StoreQueryPlan {
                values_query: StoreQueryParams {
                    metric: "test_metric".to_string(),
                    aggregation_id: 1,
                    start_timestamp: 1000,
                    end_timestamp: 2000,
                    is_exact_query: true,
                },
                keys_query: None,
            },
            agg_info: AggregationIdInfo {
                aggregation_id_for_key: 1,
                aggregation_id_for_value: 1,
                aggregation_type_for_key: AggregationType::Sum,
                aggregation_type_for_value: aggregation_type,
            },
            do_merge: false,
            spatial_filter: String::new(),
            query_time: 2000,
            grouping_labels: labels,
            aggregated_labels: KeyByLabelNames::empty(),
        }
    }

    // ========================================================================
    // All Statistic variants map without panic
    // ========================================================================

    #[test]
    fn test_all_statistics_map_without_panic() {
        let statistics = vec![
            (Statistic::Sum, AggregationType::Sum),
            (Statistic::Min, AggregationType::MinMax),
            (Statistic::Max, AggregationType::MinMax),
            (Statistic::Count, AggregationType::Sum),
            (Statistic::Increase, AggregationType::Increase),
            (Statistic::Rate, AggregationType::Increase),
            (Statistic::Quantile, AggregationType::DatasketchesKLL),
            (Statistic::Cardinality, AggregationType::SetAggregator),
            (Statistic::Topk, AggregationType::CountMinSketch),
        ];

        for (stat, agg_type) in statistics {
            let ctx = create_context(stat, agg_type, vec!["host"], HashMap::new());
            let result = ctx.map_statistic_to_infer_operation();
            assert!(
                result.is_ok(),
                "Statistic {:?} should map successfully, got: {:?}",
                stat,
                result.err()
            );
        }
    }

    // ========================================================================
    // TopK kwargs propagation
    // ========================================================================

    #[test]
    fn test_topk_kwargs_propagate() {
        use datafusion_summary_library::InferOperation;
        let mut kwargs = HashMap::new();
        kwargs.insert("k".to_string(), "5".to_string());

        let ctx = create_context(
            Statistic::Topk,
            AggregationType::CountMinSketch,
            vec!["host"],
            kwargs,
        );
        match ctx.map_statistic_to_infer_operation().unwrap() {
            InferOperation::TopK(k) => assert_eq!(k, 5, "Expected k=5, got {}", k),
            other => panic!("Expected TopK, got {:?}", other),
        }
    }

    #[test]
    fn test_topk_default_k() {
        use datafusion_summary_library::InferOperation;
        let ctx = create_context(
            Statistic::Topk,
            AggregationType::CountMinSketch,
            vec!["host"],
            HashMap::new(),
        );
        match ctx.map_statistic_to_infer_operation().unwrap() {
            InferOperation::TopK(k) => assert_eq!(k, 10, "Default k should be 10, got {}", k),
            other => panic!("Expected TopK, got {:?}", other),
        }
    }

    // ========================================================================
    // Statistic-to-operation mapping
    // ========================================================================

    #[test]
    fn test_cardinality_to_count_distinct() {
        use datafusion_summary_library::InferOperation;
        let ctx = create_context(
            Statistic::Cardinality,
            AggregationType::SetAggregator,
            vec!["host"],
            HashMap::new(),
        );
        assert!(matches!(
            ctx.map_statistic_to_infer_operation().unwrap(),
            InferOperation::CountDistinct
        ));
    }

    #[test]
    fn test_rate_to_extract_rate() {
        use datafusion_summary_library::InferOperation;
        let ctx = create_context(
            Statistic::Rate,
            AggregationType::Increase,
            vec!["host"],
            HashMap::new(),
        );
        assert!(matches!(
            ctx.map_statistic_to_infer_operation().unwrap(),
            InferOperation::ExtractRate
        ));
    }

    #[test]
    fn test_count_to_extract_count() {
        use datafusion_summary_library::InferOperation;
        let ctx = create_context(
            Statistic::Count,
            AggregationType::Sum,
            vec!["host"],
            HashMap::new(),
        );
        assert!(matches!(
            ctx.map_statistic_to_infer_operation().unwrap(),
            InferOperation::ExtractCount
        ));
    }

    // ========================================================================
    // Error paths
    // ========================================================================

    #[test]
    fn test_unknown_agg_type_errors() {
        // SingleSubpopulation is a legacy wrapper variant not mapped to a SketchType
        let ctx = create_context(
            Statistic::Sum,
            AggregationType::SingleSubpopulation,
            vec!["host"],
            HashMap::new(),
        );
        let result = ctx.to_logical_plan();
        assert!(result.is_err(), "Unmapped aggregation type should error");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("Unknown"),
            "Error should mention Unknown, got: {}",
            err_msg
        );
    }

    // ========================================================================
    // Edge cases
    // ========================================================================

    #[test]
    fn test_plan_with_empty_labels() {
        let ctx = create_context(Statistic::Sum, AggregationType::Sum, vec![], HashMap::new());
        let result = ctx.to_logical_plan();
        assert!(
            result.is_ok(),
            "Empty labels should still build a plan: {:?}",
            result.err()
        );
    }
}
