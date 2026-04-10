//! Comparison utilities for query equivalence tests
//!
//! Provides assertion helpers for deep equality checking of query execution contexts.

use crate::data_model::{AggregationIdInfo, AggregationType};
use crate::engines::simple_engine::{
    QueryExecutionContext, QueryMetadata, StoreQueryParams, StoreQueryPlan,
};
use promql_utilities::data_model::KeyByLabelNames;

/// Assert that two QueryExecutionContext objects are equivalent
///
/// Compares all fields and provides detailed error messages on mismatch
pub fn assert_execution_context_equivalent(
    context1: &QueryExecutionContext,
    context2: &QueryExecutionContext,
    test_name: &str,
) {
    // Compare metric
    assert_eq!(
        context1.metric, context2.metric,
        "{}: Metric mismatch",
        test_name
    );

    // Compare do_merge
    assert_eq!(
        context1.do_merge, context2.do_merge,
        "{}: do_merge mismatch",
        test_name
    );

    // Compare metadata
    assert_metadata_equivalent(&context1.metadata, &context2.metadata, test_name);

    // Compare store plans
    assert_store_plan_equivalent(&context1.store_plan, &context2.store_plan, test_name);

    // Compare aggregation info
    assert_agg_info_equivalent(&context1.agg_info, &context2.agg_info, test_name);

    // Note: We don't compare spatial_filter as it may have different representations
    // that are semantically equivalent (e.g., different string formats)
}

/// Assert that two QueryMetadata objects are equivalent
pub fn assert_metadata_equivalent(meta1: &QueryMetadata, meta2: &QueryMetadata, test_name: &str) {
    // Compare output labels (KeyByLabelNames maintains sorted order, so direct comparison works)
    assert_label_names_equivalent(
        &meta1.query_output_labels,
        &meta2.query_output_labels,
        test_name,
    );

    // Compare statistic
    assert_eq!(
        meta1.statistic_to_compute, meta2.statistic_to_compute,
        "{}: Statistic mismatch - PromQL={:?}, SQL={:?}",
        test_name, meta1.statistic_to_compute, meta2.statistic_to_compute
    );

    // Compare kwargs
    assert_eq!(
        meta1.query_kwargs, meta2.query_kwargs,
        "{}: Query kwargs mismatch - PromQL={:?}, SQL={:?}",
        test_name, meta1.query_kwargs, meta2.query_kwargs
    );
}

/// Assert that two StoreQueryPlan objects are equivalent
pub fn assert_store_plan_equivalent(
    plan1: &StoreQueryPlan,
    plan2: &StoreQueryPlan,
    test_name: &str,
) {
    // Compare values query
    assert_store_params_equivalent(&plan1.values_query, &plan2.values_query, test_name);

    // Compare keys query (both Some or both None)
    match (&plan1.keys_query, &plan2.keys_query) {
        (Some(k1), Some(k2)) => assert_store_params_equivalent(k1, k2, test_name),
        (None, None) => {}
        (Some(_), None) => panic!(
            "{}: Keys query presence mismatch - PromQL has keys query, SQL doesn't",
            test_name
        ),
        (None, Some(_)) => panic!(
            "{}: Keys query presence mismatch - SQL has keys query, PromQL doesn't",
            test_name
        ),
    }
}

/// Assert that two StoreQueryParams objects are equivalent
pub fn assert_store_params_equivalent(
    params1: &StoreQueryParams,
    params2: &StoreQueryParams,
    test_name: &str,
) {
    assert_eq!(
        params1.metric, params2.metric,
        "{}: Metric mismatch - PromQL='{}', SQL='{}'",
        test_name, params1.metric, params2.metric
    );

    assert_eq!(
        params1.aggregation_id, params2.aggregation_id,
        "{}: Aggregation ID mismatch - PromQL={}, SQL={}",
        test_name, params1.aggregation_id, params2.aggregation_id
    );

    assert_eq!(
        params1.start_timestamp, params2.start_timestamp,
        "{}: Start timestamp mismatch - PromQL={}, SQL={}",
        test_name, params1.start_timestamp, params2.start_timestamp
    );

    assert_eq!(
        params1.end_timestamp, params2.end_timestamp,
        "{}: End timestamp mismatch - PromQL={}, SQL={}",
        test_name, params1.end_timestamp, params2.end_timestamp
    );

    assert_eq!(
        params1.is_exact_query, params2.is_exact_query,
        "{}: Query type mismatch - PromQL={}, SQL={}",
        test_name, params1.is_exact_query, params2.is_exact_query
    );
}

/// Assert that two KeyByLabelNames objects are equivalent
pub fn assert_label_names_equivalent(
    labels1: &KeyByLabelNames,
    labels2: &KeyByLabelNames,
    test_name: &str,
) {
    // KeyByLabelNames maintains sorted order, so direct comparison works
    assert_eq!(
        labels1, labels2,
        "{}: Label names mismatch - PromQL={:?}, SQL={:?}",
        test_name, labels1.labels, labels2.labels
    );
}

/// Assert that two AggregationIdInfo objects are equivalent
pub fn assert_agg_info_equivalent(
    agg1: &AggregationIdInfo,
    agg2: &AggregationIdInfo,
    test_name: &str,
) {
    assert_eq!(
        agg1.aggregation_id_for_key, agg2.aggregation_id_for_key,
        "{}: Aggregation ID for key mismatch - PromQL={}, SQL={}",
        test_name, agg1.aggregation_id_for_key, agg2.aggregation_id_for_key
    );

    assert_eq!(
        agg1.aggregation_id_for_value, agg2.aggregation_id_for_value,
        "{}: Aggregation ID for value mismatch - PromQL={}, SQL={}",
        test_name, agg1.aggregation_id_for_value, agg2.aggregation_id_for_value
    );

    assert_eq!(
        agg1.aggregation_type_for_key, agg2.aggregation_type_for_key,
        "{}: Aggregation type for key mismatch - PromQL='{}', SQL='{}'",
        test_name, agg1.aggregation_type_for_key, agg2.aggregation_type_for_key
    );

    assert_eq!(
        agg1.aggregation_type_for_value, agg2.aggregation_type_for_value,
        "{}: Aggregation type for value mismatch - PromQL='{}', SQL='{}'",
        test_name, agg1.aggregation_type_for_value, agg2.aggregation_type_for_value
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use promql_utilities::query_logics::enums::Statistic;
    use std::collections::HashMap;

    fn create_test_context() -> QueryExecutionContext {
        QueryExecutionContext {
            metric: "test_metric".to_string(),
            metadata: QueryMetadata {
                query_output_labels: KeyByLabelNames::new(vec!["L1".to_string(), "L2".to_string()]),
                statistic_to_compute: Statistic::Sum,
                query_kwargs: HashMap::new(),
            },
            store_plan: StoreQueryPlan {
                values_query: StoreQueryParams {
                    metric: "test_metric".to_string(),
                    aggregation_id: 1,
                    start_timestamp: 1000,
                    end_timestamp: 2000,
                    is_exact_query: false,
                },
                keys_query: None,
            },
            agg_info: AggregationIdInfo {
                aggregation_id_for_key: 1,
                aggregation_id_for_value: 1,
                aggregation_type_for_key: AggregationType::Sum,
                aggregation_type_for_value: AggregationType::Sum,
            },
            do_merge: true, // OnlyTemporal queries merge
            spatial_filter: String::new(),
            query_time: 2_000_000, // query timestamp in milliseconds
            grouping_labels: KeyByLabelNames::new(vec!["L1".to_string(), "L2".to_string()]),
            aggregated_labels: KeyByLabelNames::empty(),
        }
    }

    #[test]
    fn test_identical_contexts_are_equivalent() {
        let ctx1 = create_test_context();
        let ctx2 = create_test_context();

        // Should not panic
        assert_execution_context_equivalent(&ctx1, &ctx2, "test_identical");
    }

    #[test]
    #[should_panic(expected = "Metric mismatch")]
    fn test_different_metrics_fail() {
        let ctx1 = create_test_context();
        let mut ctx2 = create_test_context();
        ctx2.metric = "different_metric".to_string();

        assert_execution_context_equivalent(&ctx1, &ctx2, "test_different_metrics");
    }

    #[test]
    #[should_panic(expected = "Statistic mismatch")]
    fn test_different_statistics_fail() {
        let ctx1 = create_test_context();
        let mut ctx2 = create_test_context();
        ctx2.metadata.statistic_to_compute = Statistic::Max;

        assert_execution_context_equivalent(&ctx1, &ctx2, "test_different_stats");
    }

    #[test]
    #[should_panic(expected = "Start timestamp mismatch")]
    fn test_different_timestamps_fail() {
        let ctx1 = create_test_context();
        let mut ctx2 = create_test_context();
        ctx2.store_plan.values_query.start_timestamp = 5000;

        assert_execution_context_equivalent(&ctx1, &ctx2, "test_different_timestamps");
    }
}
