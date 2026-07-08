//! Range-query binary arithmetic integration tests.
//!
//! Verify that binary arithmetic range queries (vector/vector and
//! scalar/vector) produce numerically correct results end-to-end through
//! `handle_range_query_promql`, which joins each arm's samples by timestamp.
//!
//! Added alongside #507 (promql.rs instant vs range dedup): no test
//! previously exercised `handle_range_query_promql`'s binary path at all.
//! Nested binary over a range (e.g. `(a+b)*c`) is intentionally not covered
//! here — that gap is tracked separately in #516.

#[cfg(test)]
mod tests {
    use crate::data_model::{
        AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
        KeyByLabelValues, PrecomputedOutput, PromQLSchema, QueryConfig, QueryLanguage,
        SchemaConfig, StreamingConfig, WindowType,
    };
    use crate::engines::query_result::{QueryResult, RangeVectorElement};
    use crate::engines::simple_engine::SimpleEngine;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::stores::simple_map_store::SimpleMapStore;
    use crate::stores::Store;
    use crate::AggregateCore;
    use promql_utilities::data_model::KeyByLabelNames;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// One tumbling-window bucket: (bucket end timestamp ms, label values, accumulator).
    type TimeSeriesData = Vec<(u64, Option<Vec<String>>, Box<dyn AggregateCore>)>;

    const WINDOW_MS: u64 = 1000;

    /// Builds a SimpleEngine with two independent metrics, each carrying data
    /// across multiple 1s tumbling-window buckets. Unlike
    /// `engine_factories::create_engine_two_metrics` (single timestamp, instant
    /// queries only), this inserts one bucket per `(timestamp, value)` pair so
    /// range queries have more than one output sample to join across.
    fn create_range_engine_two_metrics(
        metric_a: &str,
        data_a: TimeSeriesData,
        query_a: &str,
        metric_b: &str,
        data_b: TimeSeriesData,
        query_b: &str,
    ) -> SimpleEngine {
        let labels = vec!["host".to_string()];

        let mut aggregation_configs = HashMap::new();
        for (id, metric) in [(1u64, metric_a), (2u64, metric_b)] {
            aggregation_configs.insert(
                id,
                AggregationConfig {
                    aggregation_id: id,
                    aggregation_type: AggregationType::Sum,
                    aggregation_sub_type: String::new(),
                    parameters: HashMap::new(),
                    grouping_labels: KeyByLabelNames::new(labels.clone()),
                    aggregated_labels: KeyByLabelNames::empty(),
                    rollup_labels: KeyByLabelNames::empty(),
                    original_yaml: String::new(),
                    window_size_ms: WINDOW_MS,
                    slide_interval_ms: WINDOW_MS,
                    window_type: WindowType::Tumbling,
                    spatial_filter: String::new(),
                    spatial_filter_normalized: String::new(),
                    metric: metric.to_string(),
                    num_aggregates_to_retain: None,
                    read_count_threshold: None,
                    table_name: None,
                    value_column: None,
                },
            );
        }

        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs,
        });

        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));

        for (agg_id, data) in [(1u64, data_a), (2u64, data_b)] {
            for (timestamp, label_values_opt, acc) in data {
                let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
                let output = PrecomputedOutput::new(timestamp - WINDOW_MS, timestamp, key, agg_id);
                store.insert_precomputed_output(output, acc).unwrap();
            }
        }

        let promql_schema = PromQLSchema::new()
            .add_metric(metric_a.to_string(), KeyByLabelNames::new(labels.clone()))
            .add_metric(metric_b.to_string(), KeyByLabelNames::new(labels));

        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(promql_schema),
            query_configs: vec![
                QueryConfig::new(query_a.to_string())
                    .add_aggregation(AggregationReference::new(1, None)),
                QueryConfig::new(query_b.to_string())
                    .add_aggregation(AggregationReference::new(2, None)),
            ],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };

        SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            WINDOW_MS,
            QueryLanguage::promql,
        )
    }

    /// Builds two 1s-window buckets for host-a with the given `(timestamp, value)` pairs.
    fn host_a_series(values: [(u64, f64); 2]) -> TimeSeriesData {
        values
            .into_iter()
            .map(|(ts, val)| {
                (
                    ts,
                    Some(vec!["host-a".to_string()]),
                    Box::new(SumAccumulator::with_sum(val)) as Box<dyn AggregateCore>,
                )
            })
            .collect()
    }

    fn matrix_values(qr: QueryResult) -> Vec<RangeVectorElement> {
        match qr {
            QueryResult::Matrix(m) => m.values,
            _ => panic!("Expected matrix (range vector) result"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_range_vector_vector_divide_produces_ratio_per_timestamp() {
        // t=1000: errors=100, requests=200 -> 0.5
        // t=2000: errors=150, requests=300 -> 0.5
        let data_errors = host_a_series([(1000, 100.0), (2000, 150.0)]);
        let data_requests = host_a_series([(1000, 200.0), (2000, 300.0)]);
        let engine = create_range_engine_two_metrics(
            "errors_total",
            data_errors,
            "sum(errors_total) by (host)",
            "requests_total",
            data_requests,
            "sum(requests_total) by (host)",
        );

        let query = "sum(errors_total) by (host) / sum(requests_total) by (host)";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("Expected result for range vector-vector query");
        let elements = matrix_values(qr);
        assert_eq!(elements.len(), 1, "Expected 1 series (host-a)");
        assert_eq!(elements[0].samples.len(), 2, "Expected 2 timestamps");
        for sample in &elements[0].samples {
            assert!(
                (sample.value - 0.5).abs() < 1e-10,
                "Expected ratio 0.5 at t={}, got {}",
                sample.timestamp,
                sample.value
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_range_vector_vector_add() {
        // t=1000: a=10, b=20 -> 30
        // t=2000: a=15, b=25 -> 40
        let data_a = host_a_series([(1000, 10.0), (2000, 15.0)]);
        let data_b = host_a_series([(1000, 20.0), (2000, 25.0)]);
        let engine = create_range_engine_two_metrics(
            "metric_a",
            data_a,
            "sum(metric_a) by (host)",
            "metric_b",
            data_b,
            "sum(metric_b) by (host)",
        );

        let query = "sum(metric_a) by (host) + sum(metric_b) by (host)";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("Expected result");
        let elements = matrix_values(qr);
        assert_eq!(elements.len(), 1);
        let samples = &elements[0].samples;
        assert_eq!(samples.len(), 2);
        let by_ts: HashMap<u64, f64> = samples.iter().map(|s| (s.timestamp, s.value)).collect();
        assert!((by_ts[&1000] - 30.0).abs() < 1e-10);
        assert!((by_ts[&2000] - 40.0).abs() < 1e-10);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_range_scalar_right_multiply() {
        // metric_a * 100: t=1000: 5 -> 500, t=2000: 6 -> 600
        let data_a = host_a_series([(1000, 5.0), (2000, 6.0)]);
        let engine = create_range_engine_two_metrics(
            "metric_a",
            data_a,
            "sum(metric_a) by (host)",
            // second metric not used but the helper requires it; empty data.
            "dummy",
            vec![],
            "sum(dummy) by (host)",
        );

        let query = "sum(metric_a) by (host) * 100";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("Expected result for scalar range query");
        let elements = matrix_values(qr);
        assert_eq!(elements.len(), 1);
        let samples = &elements[0].samples;
        assert_eq!(samples.len(), 2);
        let by_ts: HashMap<u64, f64> = samples.iter().map(|s| (s.timestamp, s.value)).collect();
        assert!((by_ts[&1000] - 500.0).abs() < 1e-10);
        assert!((by_ts[&2000] - 600.0).abs() < 1e-10);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_range_scalar_left_subtract() {
        // 1 - metric_a: t=1000: 0.9 -> 0.1, t=2000: 0.75 -> 0.25
        let data_a = host_a_series([(1000, 0.9), (2000, 0.75)]);
        let engine = create_range_engine_two_metrics(
            "metric_a",
            data_a,
            "sum(metric_a) by (host)",
            "dummy",
            vec![],
            "sum(dummy) by (host)",
        );

        let query = "1 - sum(metric_a) by (host)";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("Expected result for scalar-left range query");
        let elements = matrix_values(qr);
        assert_eq!(elements.len(), 1);
        let samples = &elements[0].samples;
        assert_eq!(samples.len(), 2);
        let by_ts: HashMap<u64, f64> = samples.iter().map(|s| (s.timestamp, s.value)).collect();
        assert!((by_ts[&1000] - 0.1).abs() < 1e-10);
        assert!((by_ts[&2000] - 0.25).abs() < 1e-10);
    }
}
