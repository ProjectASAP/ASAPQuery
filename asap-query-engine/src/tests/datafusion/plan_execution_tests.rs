//! Plan Execution Integration Tests
//!
//! Tests that verify the new DataFusion plan-based execution path
//! produces correct results for spatial queries.
//!
//! These tests use an actual store with test data.

use crate::data_model::{AggregationType, KeyByLabelValues, Measurement};
use crate::engines::simple_engine::SimpleEngine;
use crate::precompute_operators::sum_accumulator::SumAccumulator;
use std::collections::HashMap;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::precompute_operators::{
        DatasketchesKLLAccumulator, IncreaseAccumulator, MinMaxAccumulator,
        MultipleMinMaxAccumulator, MultipleSumAccumulator, SetAggregatorAccumulator,
    };
    use crate::tests::test_utilities::engine_factories::*;

    /// Creates a test engine and store with sample data for spatial sum queries
    fn create_test_engine_with_data() -> SimpleEngine {
        create_engine_single_pop(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            vec![
                (
                    Some(vec!["host-a".to_string()]),
                    Box::new(SumAccumulator::with_sum(100.0)),
                ),
                (
                    Some(vec!["host-b".to_string()]),
                    Box::new(SumAccumulator::with_sum(200.0)),
                ),
            ],
            "sum(http_requests) by (host)",
        )
    }

    #[test]
    fn test_to_logical_plan_produces_valid_structure() {
        let engine = create_test_engine_with_data();

        let query_time_sec = 1000.0;
        let context = engine
            .build_query_execution_context_promql(
                "sum(http_requests) by (host)".to_string(),
                query_time_sec,
            )
            .expect("Failed to build context");

        let plan = context.to_logical_plan();
        assert!(
            plan.is_ok(),
            "Failed to build logical plan: {:?}",
            plan.err()
        );

        let plan = plan.unwrap();
        match &plan {
            datafusion::logical_expr::LogicalPlan::Extension(ext) => {
                assert_eq!(
                    ext.node.name(),
                    "SummaryInfer",
                    "Root should be SummaryInfer"
                );
            }
            _ => panic!("Expected Extension node at root, got {:?}", plan),
        }
    }

    #[tokio::test]
    async fn test_execute_plan_returns_results() {
        let engine = create_test_engine_with_data();

        let query_time_sec = 1000.0;
        let context = engine
            .build_query_execution_context_promql(
                "sum(http_requests) by (host)".to_string(),
                query_time_sec,
            )
            .expect("Failed to build context");

        let result = engine.execute_plan(&context).await;
        assert!(result.is_ok(), "execute_plan failed: {:?}", result.err());

        let results = result.unwrap();
        assert_eq!(
            results.len(),
            2,
            "Expected 2 results, got {}",
            results.len()
        );

        let values: Vec<f64> = results.iter().map(|r| r.value).collect();
        assert!(
            values.contains(&100.0) && values.contains(&200.0),
            "Expected values [100.0, 200.0], got {:?}",
            values
        );
    }

    #[tokio::test]
    async fn test_execute_plan_correct_labels() {
        let engine = create_test_engine_with_data();

        let query_time_sec = 1000.0;
        let context = engine
            .build_query_execution_context_promql(
                "sum(http_requests) by (host)".to_string(),
                query_time_sec,
            )
            .expect("Failed to build context");

        let results = engine
            .execute_plan(&context)
            .await
            .expect("execute_plan failed");

        let result_map: HashMap<String, f64> = results
            .iter()
            .map(|r| (r.labels.labels[0].clone(), r.value))
            .collect();

        assert_eq!(result_map.get("host-a"), Some(&100.0));
        assert_eq!(result_map.get("host-b"), Some(&200.0));
    }

    #[tokio::test]
    async fn test_execute_plan_multiple_timestamps() {
        let engine = create_engine_multi_timestamp(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            vec![
                (
                    999_000,
                    Some(vec!["host-a".to_string()]),
                    Box::new(SumAccumulator::with_sum(50.0)),
                ),
                (
                    1_000_000,
                    Some(vec!["host-a".to_string()]),
                    Box::new(SumAccumulator::with_sum(50.0)),
                ),
            ],
            "sum(http_requests) by (host)",
        );

        let context = engine
            .build_query_execution_context_promql(
                "sum(http_requests) by (host)".to_string(),
                1000.0,
            )
            .expect("Failed to build context");

        let results = engine
            .execute_plan(&context)
            .await
            .expect("execute_plan failed");

        assert_eq!(results.len(), 1, "Expected 1 result");
        assert_eq!(results[0].labels.labels[0], "host-a");
        assert_eq!(
            results[0].value, 50.0,
            "Spatial-only queries use latest timestamp only"
        );
    }

    #[tokio::test]
    async fn test_execute_plan_matches_old_pipeline() {
        let engine = create_test_engine_with_data();
        assert_old_new_match(&engine, "sum(http_requests) by (host)", 1000.0).await;
    }

    // ========================================================================
    // Category 1 - Old-vs-New Comparison for more accumulator types
    // ========================================================================

    #[tokio::test]
    async fn test_old_vs_new_kll_quantile() {
        let mut kll_a = DatasketchesKLLAccumulator::new(200);
        for v in [10.0, 20.0, 30.0, 40.0, 50.0] {
            kll_a._update(v);
        }
        let mut kll_b = DatasketchesKLLAccumulator::new(200);
        for v in [100.0, 200.0, 300.0] {
            kll_b._update(v);
        }

        let engine = create_engine_single_pop(
            "latency",
            AggregationType::DatasketchesKLL,
            vec!["host"],
            vec![
                (Some(vec!["host-a".to_string()]), Box::new(kll_a)),
                (Some(vec!["host-b".to_string()]), Box::new(kll_b)),
            ],
            "quantile(0.5, latency) by (host)",
        );

        assert_old_new_match(&engine, "quantile(0.5, latency) by (host)", 1000.0).await;
    }

    #[tokio::test]
    async fn test_old_vs_new_kll_quantile_p99() {
        let mut kll = DatasketchesKLLAccumulator::new(200);
        for v in 1..=100 {
            kll._update(v as f64);
        }

        let engine = create_engine_single_pop(
            "latency",
            AggregationType::DatasketchesKLL,
            vec!["host"],
            vec![(Some(vec!["host-a".to_string()]), Box::new(kll))],
            "quantile(0.99, latency) by (host)",
        );

        assert_old_new_match(&engine, "quantile(0.99, latency) by (host)", 1000.0).await;
    }

    #[tokio::test]
    async fn test_old_vs_new_kll_quantile_p0() {
        let mut kll = DatasketchesKLLAccumulator::new(200);
        for v in [5.0, 10.0, 15.0, 20.0, 25.0] {
            kll._update(v);
        }

        let engine = create_engine_single_pop(
            "latency",
            AggregationType::DatasketchesKLL,
            vec!["host"],
            vec![(Some(vec!["host-a".to_string()]), Box::new(kll))],
            "quantile(0.0, latency) by (host)",
        );

        assert_old_new_match(&engine, "quantile(0.0, latency) by (host)", 1000.0).await;
    }

    #[tokio::test]
    async fn test_old_vs_new_kll_quantile_p1() {
        let mut kll = DatasketchesKLLAccumulator::new(200);
        for v in [5.0, 10.0, 15.0, 20.0, 25.0] {
            kll._update(v);
        }

        let engine = create_engine_single_pop(
            "latency",
            AggregationType::DatasketchesKLL,
            vec!["host"],
            vec![(Some(vec!["host-a".to_string()]), Box::new(kll))],
            "quantile(1.0, latency) by (host)",
        );

        assert_old_new_match(&engine, "quantile(1.0, latency) by (host)", 1000.0).await;
    }

    #[tokio::test]
    async fn test_old_vs_new_kll_quantile_p25() {
        let mut kll = DatasketchesKLLAccumulator::new(200);
        for v in 1..=1000 {
            kll._update(v as f64);
        }

        let engine = create_engine_single_pop(
            "latency",
            AggregationType::DatasketchesKLL,
            vec!["host"],
            vec![(Some(vec!["host-a".to_string()]), Box::new(kll))],
            "quantile(0.25, latency) by (host)",
        );

        assert_old_new_match(&engine, "quantile(0.25, latency) by (host)", 1000.0).await;
    }

    #[tokio::test]
    #[ignore = "Blocked: SetAggregatorAccumulator does not support old pipeline query"]
    async fn test_old_vs_new_set_aggregator_cardinality() {
        let mut set_a = SetAggregatorAccumulator::new();
        set_a.add_key(KeyByLabelValues {
            labels: vec!["user-1".to_string()],
        });
        set_a.add_key(KeyByLabelValues {
            labels: vec!["user-2".to_string()],
        });
        let mut set_b = SetAggregatorAccumulator::new();
        set_b.add_key(KeyByLabelValues {
            labels: vec!["user-3".to_string()],
        });

        let engine = create_engine_single_pop(
            "active_users",
            AggregationType::SetAggregator,
            vec!["host"],
            vec![
                (Some(vec!["host-a".to_string()]), Box::new(set_a)),
                (Some(vec!["host-b".to_string()]), Box::new(set_b)),
            ],
            "count(active_users) by (host)",
        );

        assert_old_new_match(&engine, "count(active_users) by (host)", 1000.0).await;
    }

    #[tokio::test]
    #[ignore = "Blocked: IncreaseAccumulator has no arroyo serde"]
    async fn test_old_vs_new_increase() {
        let inc_a = IncreaseAccumulator::new(Measurement::new(0.0), 0, Measurement::new(100.0), 10);
        let inc_b = IncreaseAccumulator::new(Measurement::new(0.0), 0, Measurement::new(200.0), 10);

        let engine = create_engine_single_pop(
            "http_requests_total",
            AggregationType::Increase,
            vec!["host"],
            vec![
                (Some(vec!["host-a".to_string()]), Box::new(inc_a)),
                (Some(vec!["host-b".to_string()]), Box::new(inc_b)),
            ],
            "sum(increase(http_requests_total[10s])) by (host)",
        );

        assert_old_new_match(
            &engine,
            "sum(increase(http_requests_total[10s])) by (host)",
            1000.0,
        )
        .await;
    }

    #[tokio::test]
    #[ignore = "Blocked: MinMaxAccumulator has no arroyo serde"]
    async fn test_old_vs_new_minmax_min() {
        let mm_a = MinMaxAccumulator::with_value(10.0, "min".to_string());
        let mm_b = MinMaxAccumulator::with_value(5.0, "min".to_string());

        let engine = create_engine_single_pop(
            "temperature",
            AggregationType::MinMax,
            vec!["host"],
            vec![
                (Some(vec!["host-a".to_string()]), Box::new(mm_a)),
                (Some(vec!["host-b".to_string()]), Box::new(mm_b)),
            ],
            "min(temperature) by (host)",
        );

        assert_old_new_match(&engine, "min(temperature) by (host)", 1000.0).await;
    }

    #[tokio::test]
    #[ignore = "Blocked: MinMaxAccumulator has no arroyo serde"]
    async fn test_old_vs_new_minmax_max() {
        let mm_a = MinMaxAccumulator::with_value(90.0, "max".to_string());
        let mm_b = MinMaxAccumulator::with_value(95.0, "max".to_string());

        let engine = create_engine_single_pop(
            "temperature",
            AggregationType::MinMax,
            vec!["host"],
            vec![
                (Some(vec!["host-a".to_string()]), Box::new(mm_a)),
                (Some(vec!["host-b".to_string()]), Box::new(mm_b)),
            ],
            "max(temperature) by (host)",
        );

        assert_old_new_match(&engine, "max(temperature) by (host)", 1000.0).await;
    }

    #[tokio::test]
    async fn test_old_vs_new_multiple_sum() {
        let mut ms = MultipleSumAccumulator::new();
        let key = KeyByLabelValues {
            labels: vec!["host-a".to_string()],
        };
        ms.update(key, 42.0);

        let engine = create_engine_single_pop(
            "requests",
            AggregationType::MultipleSum,
            vec!["host"],
            vec![(Some(vec!["host-a".to_string()]), Box::new(ms))],
            "sum(requests) by (host)",
        );

        assert_old_new_match(&engine, "sum(requests) by (host)", 1000.0).await;
    }

    #[tokio::test]
    #[ignore = "Blocked: MultipleMinMaxAccumulator has no arroyo serde"]
    async fn test_old_vs_new_multiple_minmax() {
        let mm = MultipleMinMaxAccumulator::new("min".to_string());

        let engine = create_engine_single_pop(
            "latency",
            AggregationType::MultipleMinMax,
            vec!["host"],
            vec![(Some(vec!["host-a".to_string()]), Box::new(mm))],
            "min(latency) by (host)",
        );

        assert_old_new_match(&engine, "min(latency) by (host)", 1000.0).await;
    }

    // ========================================================================
    // Category 3 - Edge Cases
    // ========================================================================

    #[tokio::test]
    async fn test_execute_plan_empty_store() {
        let engine = create_engine_single_pop(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            vec![], // No data
            "sum(http_requests) by (host)",
        );

        let results = execute_new_plan(&engine, "sum(http_requests) by (host)", 1000.0).await;
        assert!(
            results.is_empty(),
            "Empty store should return 0 results, got {}",
            results.len()
        );
    }

    #[tokio::test]
    async fn test_execute_plan_many_groups() {
        #[allow(clippy::type_complexity)]
        let data: Vec<(Option<Vec<String>>, Box<dyn crate::AggregateCore>)> = (0..100)
            .map(|i| {
                (
                    Some(vec![format!("host-{:03}", i)]),
                    Box::new(SumAccumulator::with_sum(i as f64)) as Box<dyn crate::AggregateCore>,
                )
            })
            .collect();

        let engine = create_engine_single_pop(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            "sum(http_requests) by (host)",
        );

        let results = execute_new_plan(&engine, "sum(http_requests) by (host)", 1000.0).await;
        assert_eq!(
            results.len(),
            100,
            "Expected 100 results, got {}",
            results.len()
        );
    }

    #[tokio::test]
    async fn test_execute_plan_multi_timestamp_multi_group() {
        // 3 timestamps x 3 groups; spatial-only uses latest timestamp only -> 3 results
        let mut data = Vec::new();
        for ts in [998_000u64, 999_000, 1_000_000] {
            for i in 0..3 {
                data.push((
                    ts,
                    Some(vec![format!("host-{}", i)]),
                    Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn crate::AggregateCore>,
                ));
            }
        }

        let engine = create_engine_multi_timestamp(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            "sum(http_requests) by (host)",
        );

        let results = execute_new_plan(&engine, "sum(http_requests) by (host)", 1000.0).await;
        assert_eq!(
            results.len(),
            3,
            "3 groups merged across 3 timestamps should give 3 results"
        );
        // Spatial-only queries use latest timestamp only, so each group = 10.0
        for r in &results {
            assert!(
                (r.value - 10.0).abs() < 1e-10,
                "Each group should be 10.0 (latest timestamp only), got {}",
                r.value
            );
        }
    }

    #[tokio::test]
    async fn test_execute_plan_single_group_many_timestamps() {
        // Spatial-only queries only consider the latest timestamp (query time).
        // Include data at the query time (1_000_000) plus older timestamps.
        #[allow(clippy::type_complexity)]
        let mut data: Vec<(u64, Option<Vec<String>>, Box<dyn crate::AggregateCore>)> = (0..9)
            .map(|i| {
                (
                    (991_000 + i * 1000) as u64,
                    Some(vec!["host-a".to_string()]),
                    Box::new(SumAccumulator::with_sum(5.0)) as Box<dyn crate::AggregateCore>,
                )
            })
            .collect();
        data.push((
            1_000_000,
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(5.0)) as Box<dyn crate::AggregateCore>,
        ));

        let engine = create_engine_multi_timestamp(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            "sum(http_requests) by (host)",
        );

        let results = execute_new_plan(&engine, "sum(http_requests) by (host)", 1000.0).await;
        assert_eq!(results.len(), 1, "Single group should give 1 result");
        assert!(
            (results[0].value - 5.0).abs() < 1e-10,
            "Spatial-only uses latest timestamp only, expected 5.0, got {}",
            results[0].value
        );
    }

    // ========================================================================
    // Category 8 - Error Paths
    // ========================================================================

    #[tokio::test]
    async fn test_execute_plan_not_implemented_increase() {
        // IncreaseAccumulator has no arroyo serde, so execute_plan should fail
        let inc = IncreaseAccumulator::new(Measurement::new(0.0), 0, Measurement::new(100.0), 10);

        let engine = create_engine_single_pop(
            "http_requests_total",
            AggregationType::Increase,
            vec!["host"],
            vec![(Some(vec!["host-a".to_string()]), Box::new(inc))],
            "sum(increase(http_requests_total[10s])) by (host)",
        );

        let context = engine
            .build_query_execution_context_promql(
                "sum(increase(http_requests_total[10s])) by (host)".to_string(),
                1000.0,
            )
            .expect("Should build context");

        let result = engine.execute_plan(&context).await;
        assert!(
            result.is_err(),
            "execute_plan should fail for IncreaseAccumulator (no arroyo serde)"
        );
    }

    #[tokio::test]
    async fn test_execute_plan_not_implemented_minmax() {
        let mm = MinMaxAccumulator::with_value(42.0, "min".to_string());

        let engine = create_engine_single_pop(
            "temperature",
            AggregationType::MinMax,
            vec!["host"],
            vec![(Some(vec!["host-a".to_string()]), Box::new(mm))],
            "min(temperature) by (host)",
        );

        let context = engine
            .build_query_execution_context_promql("min(temperature) by (host)".to_string(), 1000.0)
            .expect("Should build context");

        let result = engine.execute_plan(&context).await;
        assert!(
            result.is_err(),
            "execute_plan should fail for MinMaxAccumulator (no arroyo serde)"
        );
    }
}
