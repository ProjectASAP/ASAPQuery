//! Plan Execution Integration Tests — Temporal & Collapsable Queries
//!
//! Tests that verify the DataFusion plan-based execution path produces correct
//! results for temporal queries (sum_over_time, quantile_over_time)
//! and collapsable queries (spatial of temporal, e.g. sum by () (sum_over_time(...))).

use crate::precompute_operators::sum_accumulator::SumAccumulator;
use std::collections::HashMap;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::{AggregationType, WindowType};
    use crate::precompute_operators::DatasketchesKLLAccumulator;
    use crate::tests::test_utilities::engine_factories::*;

    type TemporalData = Vec<(u64, Option<Vec<String>>, Box<dyn crate::AggregateCore>)>;

    // ========================================================================
    // Helper: build temporal data at 5 timestamps within a [5s] window
    // Timestamps: 996_000, 997_000, 998_000, 999_000, 1_000_000
    // Query time: 1000.0 (= 1_000_000 ms)
    // ========================================================================

    const QUERY_TIME: f64 = 1000.0;
    const TEMPORAL_TIMESTAMPS: [u64; 5] = [996_000, 997_000, 998_000, 999_000, 1_000_000];

    // ========================================================================
    // OnlyTemporal Tests
    // ========================================================================

    #[tokio::test]
    async fn test_temporal_sum_over_time_merges_across_timestamps() {
        // Insert SumAccumulator data at 5 timestamps for one label group.
        // sum_over_time should merge (sum) all values across timestamps.
        let data: TemporalData = TEMPORAL_TIMESTAMPS
            .iter()
            .map(|&ts| {
                (
                    ts,
                    Some(vec!["host-a".to_string()]),
                    Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn crate::AggregateCore>,
                )
            })
            .collect();

        let query = "sum_over_time(http_requests[5s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            5,
            WindowType::Tumbling,
        );

        let results = execute_new_plan(&engine, query, QUERY_TIME).await;
        assert_eq!(results.len(), 1, "Expected 1 result for single group");
        // Merged sum: 10.0 * 5 timestamps = 50.0
        assert!(
            (results[0].value - 50.0).abs() < 1e-10,
            "Expected merged sum 50.0, got {}",
            results[0].value
        );
    }

    #[tokio::test]
    async fn test_temporal_sum_over_time_single_timestamp() {
        // Only 1 timestamp in range — should still work.
        let data: TemporalData = vec![(
            1_000_000,
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(42.0)),
        )];

        let query = "sum_over_time(http_requests[5s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            5,
            WindowType::Tumbling,
        );

        let results = execute_new_plan(&engine, query, QUERY_TIME).await;
        assert_eq!(results.len(), 1);
        assert!(
            (results[0].value - 42.0).abs() < 1e-10,
            "Expected 42.0, got {}",
            results[0].value
        );
    }

    #[tokio::test]
    async fn test_temporal_sum_over_time_varying_values() {
        // sum_over_time with different values at each timestamp.
        // Verifies merge sums all values, not just takes latest.
        let data: TemporalData = TEMPORAL_TIMESTAMPS
            .iter()
            .enumerate()
            .map(|(i, &ts)| {
                (
                    ts,
                    Some(vec!["host-a".to_string()]),
                    Box::new(SumAccumulator::with_sum((i as f64 + 1.0) * 10.0))
                        as Box<dyn crate::AggregateCore>,
                )
            })
            .collect();

        let query = "sum_over_time(http_requests[5s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            5,
            WindowType::Tumbling,
        );

        let results = execute_new_plan(&engine, query, QUERY_TIME).await;
        assert_eq!(results.len(), 1);
        // 10 + 20 + 30 + 40 + 50 = 150
        assert!(
            (results[0].value - 150.0).abs() < 1e-10,
            "Expected merged sum 150.0, got {}",
            results[0].value
        );
    }

    #[tokio::test]
    async fn test_temporal_quantile_over_time() {
        // DatasketchesKLL at multiple timestamps, quantile_over_time(0.5, ...).
        // Each KLL sketch has different values; merged sketch should give median.
        let mut data: TemporalData = Vec::new();
        for (i, &ts) in TEMPORAL_TIMESTAMPS.iter().enumerate() {
            let mut kll = DatasketchesKLLAccumulator::new(200);
            // Insert values 10, 20, 30, 40, 50 at successive timestamps
            kll._update((i as f64 + 1.0) * 10.0);
            data.push((
                ts,
                Some(vec!["host-a".to_string()]),
                Box::new(kll) as Box<dyn crate::AggregateCore>,
            ));
        }

        let query = "quantile_over_time(0.5, latency[5s])";
        let engine = create_engine_multi_timestamp_with_window(
            "latency",
            AggregationType::DatasketchesKLL,
            vec!["host"],
            data,
            query,
            5,
            WindowType::Tumbling,
        );

        let results = execute_new_plan(&engine, query, QUERY_TIME).await;
        assert_eq!(results.len(), 1);
        // Median of {10, 20, 30, 40, 50} = 30.0
        assert!(
            (results[0].value - 30.0).abs() < 5.0,
            "Expected median ~30.0, got {}",
            results[0].value
        );
    }

    #[tokio::test]
    async fn test_temporal_sum_over_time_multi_group() {
        // Multiple label groups at multiple timestamps — verify per-group merging.
        let mut data: TemporalData = Vec::new();
        for &ts in &TEMPORAL_TIMESTAMPS {
            data.push((
                ts,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn crate::AggregateCore>,
            ));
            data.push((
                ts,
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(20.0)) as Box<dyn crate::AggregateCore>,
            ));
        }

        let query = "sum_over_time(http_requests[5s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            5,
            WindowType::Tumbling,
        );

        let results = execute_new_plan(&engine, query, QUERY_TIME).await;
        assert_eq!(results.len(), 2, "Expected 2 groups");

        let result_map: HashMap<String, f64> = results
            .iter()
            .map(|r| (r.labels.labels[0].clone(), r.value))
            .collect();

        assert!(
            (result_map["host-a"] - 50.0).abs() < 1e-10,
            "host-a: expected 50.0, got {}",
            result_map["host-a"]
        );
        assert!(
            (result_map["host-b"] - 100.0).abs() < 1e-10,
            "host-b: expected 100.0, got {}",
            result_map["host-b"]
        );
    }

    #[tokio::test]
    async fn test_temporal_sum_over_time_empty_store() {
        let query = "sum_over_time(http_requests[5s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            vec![],
            query,
            5,
            WindowType::Tumbling,
        );

        let results = execute_new_plan(&engine, query, QUERY_TIME).await;
        assert!(
            results.is_empty(),
            "Empty store should return 0 results, got {}",
            results.len()
        );
    }

    #[tokio::test]
    async fn test_temporal_context_has_do_merge_true() {
        let query = "sum_over_time(http_requests[5s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                1_000_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(1.0)),
            )],
            query,
            5,
            WindowType::Tumbling,
        );

        let context = engine
            .build_query_execution_context_promql(query.to_string(), QUERY_TIME)
            .expect("Failed to build context");

        assert!(context.do_merge, "Temporal queries must have do_merge=true");
    }

    #[tokio::test]
    async fn test_temporal_context_has_correct_time_range() {
        let query = "sum_over_time(http_requests[5s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                1_000_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(1.0)),
            )],
            query,
            5,
            WindowType::Tumbling,
        );

        let context = engine
            .build_query_execution_context_promql(query.to_string(), QUERY_TIME)
            .expect("Failed to build context");

        // query_time = 1000.0 -> 1_000_000 ms
        assert_eq!(context.query_time, 1_000_000);
        // For [5s] range: start = end - 5000 = 995_000
        let start = context.store_plan.values_query.start_timestamp;
        let end = context.store_plan.values_query.end_timestamp;
        assert_eq!(end, 1_000_000, "End timestamp should be 1_000_000");
        assert_eq!(
            start, 995_000,
            "Start timestamp should be 995_000 for [5s] range"
        );
    }

    // ========================================================================
    // Collapsable Tests (spatial of temporal)
    // ========================================================================

    #[tokio::test]
    async fn test_collapsable_sum_of_sum_over_time() {
        // sum by (host) (sum_over_time(metric[5s]))
        // Multiple hosts at multiple timestamps.
        let mut data: TemporalData = Vec::new();
        for &ts in &TEMPORAL_TIMESTAMPS {
            data.push((
                ts,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn crate::AggregateCore>,
            ));
            data.push((
                ts,
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(20.0)) as Box<dyn crate::AggregateCore>,
            ));
        }

        let query = "sum by (host) (sum_over_time(http_requests[5s]))";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            5,
            WindowType::Tumbling,
        );

        let results = execute_new_plan(&engine, query, QUERY_TIME).await;
        assert_eq!(results.len(), 2, "Expected 2 groups (host-a, host-b)");

        let result_map: HashMap<String, f64> = results
            .iter()
            .map(|r| (r.labels.labels[0].clone(), r.value))
            .collect();

        // host-a: 10.0 * 5 timestamps = 50.0
        assert!(
            (result_map["host-a"] - 50.0).abs() < 1e-10,
            "host-a: expected 50.0, got {}",
            result_map["host-a"]
        );
        // host-b: 20.0 * 5 timestamps = 100.0
        assert!(
            (result_map["host-b"] - 100.0).abs() < 1e-10,
            "host-b: expected 100.0, got {}",
            result_map["host-b"]
        );
    }

    #[tokio::test]
    async fn test_collapsable_sum_of_sum_over_time_varying_values() {
        // sum by (host) (sum_over_time(metric[5s])) with varying values per timestamp
        let mut data: TemporalData = Vec::new();
        for (i, &ts) in TEMPORAL_TIMESTAMPS.iter().enumerate() {
            data.push((
                ts,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum((i as f64 + 1.0) * 10.0))
                    as Box<dyn crate::AggregateCore>,
            ));
            data.push((
                ts,
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum((i as f64 + 1.0) * 5.0))
                    as Box<dyn crate::AggregateCore>,
            ));
        }

        let query = "sum by (host) (sum_over_time(http_requests[5s]))";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            5,
            WindowType::Tumbling,
        );

        let results = execute_new_plan(&engine, query, QUERY_TIME).await;
        assert_eq!(results.len(), 2, "Expected 2 groups");

        let result_map: HashMap<String, f64> = results
            .iter()
            .map(|r| (r.labels.labels[0].clone(), r.value))
            .collect();

        // host-a: 10 + 20 + 30 + 40 + 50 = 150.0
        assert!(
            (result_map["host-a"] - 150.0).abs() < 1e-10,
            "host-a: expected 150.0, got {}",
            result_map["host-a"]
        );
        // host-b: 5 + 10 + 15 + 20 + 25 = 75.0
        assert!(
            (result_map["host-b"] - 75.0).abs() < 1e-10,
            "host-b: expected 75.0, got {}",
            result_map["host-b"]
        );
    }

    #[tokio::test]
    async fn test_collapsable_context_has_do_merge_true() {
        let query = "sum by (host) (sum_over_time(http_requests[5s]))";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                1_000_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(1.0)),
            )],
            query,
            5,
            WindowType::Tumbling,
        );

        let context = engine
            .build_query_execution_context_promql(query.to_string(), QUERY_TIME)
            .expect("Failed to build context");

        assert!(
            context.do_merge,
            "Collapsable (OneTemporalOneSpatial) queries must have do_merge=true"
        );
    }

    #[tokio::test]
    async fn test_collapsable_output_labels_are_spatial() {
        // Verify output labels are the spatial GROUP BY labels (host), not all labels.
        let query = "sum by (host) (sum_over_time(http_requests[5s]))";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                1_000_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(1.0)),
            )],
            query,
            5,
            WindowType::Tumbling,
        );

        let context = engine
            .build_query_execution_context_promql(query.to_string(), QUERY_TIME)
            .expect("Failed to build context");

        assert_eq!(
            context.metadata.query_output_labels.labels,
            vec!["host".to_string()],
            "Collapsable query output labels should be spatial GROUP BY labels"
        );
    }

    #[tokio::test]
    async fn test_collapsable_empty_store() {
        let query = "sum by (host) (sum_over_time(http_requests[5s]))";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            vec![],
            query,
            5,
            WindowType::Tumbling,
        );

        let results = execute_new_plan(&engine, query, QUERY_TIME).await;
        assert!(
            results.is_empty(),
            "Empty store should return 0 results, got {}",
            results.len()
        );
    }

    // ========================================================================
    // Old-vs-New comparison tests for temporal queries
    // ========================================================================

    #[tokio::test]
    async fn test_temporal_sum_over_time_old_vs_new() {
        let data: TemporalData = TEMPORAL_TIMESTAMPS
            .iter()
            .map(|&ts| {
                (
                    ts,
                    Some(vec!["host-a".to_string()]),
                    Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn crate::AggregateCore>,
                )
            })
            .collect();

        let query = "sum_over_time(http_requests[5s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            5,
            WindowType::Tumbling,
        );

        assert_old_new_match(&engine, query, QUERY_TIME).await;
    }

    #[tokio::test]
    async fn test_collapsable_sum_of_sum_over_time_old_vs_new() {
        let mut data: TemporalData = Vec::new();
        for &ts in &TEMPORAL_TIMESTAMPS {
            data.push((
                ts,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn crate::AggregateCore>,
            ));
            data.push((
                ts,
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(20.0)) as Box<dyn crate::AggregateCore>,
            ));
        }

        let query = "sum by (host) (sum_over_time(http_requests[5s]))";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            5,
            WindowType::Tumbling,
        );

        assert_old_new_match(&engine, query, QUERY_TIME).await;
    }
}
