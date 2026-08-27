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
    use crate::precompute_operators::CountMinSketchWithHeapAccumulator;
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
    #[allow(clippy::too_many_arguments)]
    fn create_range_engine_two_metrics(
        metric_a: &str,
        labels_a: Vec<&str>,
        data_a: TimeSeriesData,
        query_a: &str,
        metric_b: &str,
        labels_b: Vec<&str>,
        data_b: TimeSeriesData,
        query_b: &str,
    ) -> SimpleEngine {
        let labels_a: Vec<String> = labels_a.iter().map(|s| s.to_string()).collect();
        let labels_b: Vec<String> = labels_b.iter().map(|s| s.to_string()).collect();

        let mut aggregation_configs = HashMap::new();
        for (id, metric, labels) in [(1u64, metric_a, &labels_a), (2u64, metric_b, &labels_b)] {
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
            .add_metric(metric_a.to_string(), KeyByLabelNames::new(labels_a))
            .add_metric(metric_b.to_string(), KeyByLabelNames::new(labels_b));

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
            vec!["host"],
            data_errors,
            "sum(errors_total) by (host)",
            "requests_total",
            vec!["host"],
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
            vec!["host"],
            data_a,
            "sum(metric_a) by (host)",
            "metric_b",
            vec!["host"],
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
            vec!["host"],
            data_a,
            "sum(metric_a) by (host)",
            // second metric not used but the helper requires it; empty data.
            "dummy",
            vec!["host"],
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
            vec!["host"],
            data_a,
            "sum(metric_a) by (host)",
            "dummy",
            vec!["host"],
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

    // Regression test: handle_binary_expr_range_promql's vector-vector join
    // used to match purely on positional KeyByLabelValues equality (rhs
    // labels discarded), unlike the instant-query combine_vector_vector,
    // which rejects a join between arms grouped by different label sets. Two
    // arms grouped by disjoint labels ((host) vs (region)) that happen to
    // produce the same value could silently join into a wrong-but-plausible
    // result across the whole range.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_range_vector_vector_mismatched_label_sets_return_none() {
        let data_a = host_a_series([(1000, 10.0), (2000, 15.0)]);
        let data_b = host_a_series([(1000, 10.0), (2000, 15.0)]);
        let engine = create_range_engine_two_metrics(
            "metric_a",
            vec!["host"],
            data_a,
            "sum(metric_a) by (host)",
            "metric_b",
            vec!["region"],
            data_b,
            "sum(metric_b) by (region)",
        );

        let query = "sum(metric_a) by (host) + sum(metric_b) by (region)";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        assert!(
            result.is_none(),
            "BUG: arms grouped by different label sets must not join, even when their \
             values coincide, got {result:?}"
        );
    }

    /// Builds a SimpleEngine with one self-keyed topk-capable metric
    /// (`metric_a`, `CountMinSketchWithHeap`, one ungrouped sketch per
    /// bucket) and one plain grouped-sum metric (`metric_b`, `SumAccumulator`
    /// per host), so a mixed `topk(k, metric_a) OP sum(metric_b) by (host)`
    /// range query is constructible. Mirrors `build_range_topk_engine` in
    /// `stage_e_instant_range_equivalence_tests.rs` for the topk side, and
    /// `create_range_engine_two_metrics`'s per-host Sum buckets for the
    /// plain side.
    fn build_range_topk_plus_plain_engine(
        topk_query: &str,
        plain_query: &str,
        topk_candidates: &[(&str, f64)],
        plain_values: &[(&str, f64)],
    ) -> SimpleEngine {
        let mut aggregation_configs = HashMap::new();
        aggregation_configs.insert(
            1u64,
            AggregationConfig {
                aggregation_id: 1,
                aggregation_type: AggregationType::CountMinSketchWithHeap,
                aggregation_sub_type: String::new(),
                parameters: HashMap::new(),
                grouping_labels: KeyByLabelNames::empty(),
                aggregated_labels: KeyByLabelNames::new(vec!["host".to_string()]),
                rollup_labels: KeyByLabelNames::empty(),
                original_yaml: String::new(),
                window_size_ms: WINDOW_MS,
                slide_interval_ms: WINDOW_MS,
                window_type: WindowType::Tumbling,
                spatial_filter: String::new(),
                spatial_filter_normalized: String::new(),
                metric: "metric_a".to_string(),
                num_aggregates_to_retain: None,
                read_count_threshold: None,
                table_name: None,
                value_column: None,
            },
        );
        aggregation_configs.insert(
            2u64,
            AggregationConfig {
                aggregation_id: 2,
                aggregation_type: AggregationType::Sum,
                aggregation_sub_type: String::new(),
                parameters: HashMap::new(),
                grouping_labels: KeyByLabelNames::new(vec!["host".to_string()]),
                aggregated_labels: KeyByLabelNames::empty(),
                rollup_labels: KeyByLabelNames::empty(),
                original_yaml: String::new(),
                window_size_ms: WINDOW_MS,
                slide_interval_ms: WINDOW_MS,
                window_type: WindowType::Tumbling,
                spatial_filter: String::new(),
                spatial_filter_normalized: String::new(),
                metric: "metric_b".to_string(),
                num_aggregates_to_retain: None,
                read_count_threshold: None,
                table_name: None,
                value_column: None,
            },
        );

        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs,
        });
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));

        let mut sketch = CountMinSketchWithHeapAccumulator::new(3, 1024, 32);
        for (host, value) in topk_candidates {
            sketch.inner.update(host, *value);
        }
        let topk_output = PrecomputedOutput::new(0, WINDOW_MS, None, 1);
        store
            .insert_precomputed_output(topk_output, Box::new(sketch))
            .unwrap();

        for (host, value) in plain_values {
            let key = KeyByLabelValues {
                labels: vec![host.to_string()],
            };
            let plain_output = PrecomputedOutput::new(0, WINDOW_MS, Some(key), 2);
            store
                .insert_precomputed_output(
                    plain_output,
                    Box::new(SumAccumulator::with_sum(*value)) as Box<dyn AggregateCore>,
                )
                .unwrap();
        }

        let promql_schema = PromQLSchema::new()
            .add_metric(
                "metric_a".to_string(),
                KeyByLabelNames::new(vec!["host".to_string()]),
            )
            .add_metric(
                "metric_b".to_string(),
                KeyByLabelNames::new(vec!["host".to_string()]),
            );
        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(promql_schema),
            query_configs: vec![
                QueryConfig::new(topk_query.to_string())
                    .add_aggregation(AggregationReference::new(1, None)),
                QueryConfig::new(plain_query.to_string())
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

    // Regression test for issue #631: Topk preserves the original grouping
    // labels for binary matching, while arithmetic drops the metric name.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_range_vector_vector_topk_lhs_plus_plain_rhs_joins_by_original_labels() {
        let query = "topk(2, metric_a) + sum(metric_b) by (host)";
        let engine = build_range_topk_plus_plain_engine(
            "topk(2, metric_a)",
            "sum(metric_b) by (host)",
            &[("host-a", 100.0), ("host-b", 50.0), ("host-c", 10.0)],
            &[("host-a", 1000.0), ("host-b", 2000.0), ("host-c", 3000.0)],
        );

        let (_, qr) = engine
            .handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0)
            .expect("Expected result for range topk/plain query");
        let elements = matrix_values(qr);

        assert_eq!(elements.len(), 2);
        let mut values: HashMap<String, f64> = elements
            .into_iter()
            .map(|element| {
                assert_eq!(element.labels.labels.len(), 1);
                assert_eq!(element.samples.len(), 1);
                (element.labels.labels[0].clone(), element.samples[0].value)
            })
            .collect();
        assert_eq!(values.remove("host-a"), Some(1100.0));
        assert_eq!(values.remove("host-b"), Some(2050.0));
        assert!(values.is_empty());
    }

    /// Builds a SimpleEngine with two independent self-keyed topk-capable
    /// metrics (`metric_a`, `metric_b`, both `CountMinSketchWithHeap`, one
    /// ungrouped sketch per bucket each), so a
    /// `topk(k1, metric_a) OP topk(k2, metric_b)` range query is
    /// constructible. Unlike the topk+plain mix
    /// (`build_range_topk_plus_plain_engine`), both arms get `"__name__"`
    /// prepended to their label *names* identically, so this shape actually
    /// reaches `handle_binary_expr_range_promql`'s vector-vector join --
    /// this is the shape PR #629 review Finding 1's `apply_range_topk`
    /// formatting-before-join bug is reachable through.
    fn build_range_two_topk_engine(
        query_a: &str,
        query_b: &str,
        candidates_a: &[(&str, f64)],
        candidates_b: &[(&str, f64)],
    ) -> SimpleEngine {
        let mut aggregation_configs = HashMap::new();
        for (id, metric) in [(1u64, "metric_a"), (2u64, "metric_b")] {
            aggregation_configs.insert(
                id,
                AggregationConfig {
                    aggregation_id: id,
                    aggregation_type: AggregationType::CountMinSketchWithHeap,
                    aggregation_sub_type: String::new(),
                    parameters: HashMap::new(),
                    grouping_labels: KeyByLabelNames::empty(),
                    aggregated_labels: KeyByLabelNames::new(vec!["host".to_string()]),
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

        for (agg_id, candidates) in [(1u64, candidates_a), (2u64, candidates_b)] {
            let mut sketch = CountMinSketchWithHeapAccumulator::new(3, 1024, 32);
            for (host, value) in candidates {
                sketch.inner.update(host, *value);
            }
            let output = PrecomputedOutput::new(0, WINDOW_MS, None, agg_id);
            store
                .insert_precomputed_output(output, Box::new(sketch))
                .unwrap();
        }

        let promql_schema = PromQLSchema::new()
            .add_metric(
                "metric_a".to_string(),
                KeyByLabelNames::new(vec!["host".to_string()]),
            )
            .add_metric(
                "metric_b".to_string(),
                KeyByLabelNames::new(vec!["host".to_string()]),
            );
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

    #[tokio::test(flavor = "multi_thread")]
    async fn instant_vector_vector_topk_lhs_topk_rhs_joins_by_original_labels() {
        let query = "topk(2, metric_a) + topk(2, metric_b)";
        let engine = build_range_two_topk_engine(
            "topk(2, metric_a)",
            "topk(2, metric_b)",
            &[("host-a", 100.0), ("host-b", 50.0), ("host-c", 10.0)],
            &[("host-a", 5.0), ("host-b", 200.0), ("host-c", 300.0)],
        );

        let (_, qr) = engine
            .handle_query_promql(query.to_string(), 1.0)
            .expect("Expected result for instant topk/topk query");
        let elements = match qr {
            QueryResult::Vector(vector) => vector.values,
            other => panic!("Expected instant vector result, got {other:?}"),
        };

        assert_eq!(elements.len(), 1);
        assert_eq!(elements[0].labels.labels, vec!["host-b".to_string()]);
        assert!((elements[0].value - 250.0).abs() < 1e-10);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn instant_vector_vector_topk_lhs_plus_plain_rhs_joins_by_original_labels() {
        let query = "topk(2, metric_a) + sum(metric_b) by (host)";
        let engine = build_range_topk_plus_plain_engine(
            "topk(2, metric_a)",
            "sum(metric_b) by (host)",
            &[("host-a", 100.0), ("host-b", 50.0), ("host-c", 10.0)],
            &[("host-a", 1000.0), ("host-b", 2000.0), ("host-c", 3000.0)],
        );

        let (_, qr) = engine
            .handle_query_promql(query.to_string(), 1.0)
            .expect("Expected result for instant topk/plain query");
        let elements = match qr {
            QueryResult::Vector(vector) => vector.values,
            other => panic!("Expected instant vector result, got {other:?}"),
        };

        assert_eq!(elements.len(), 2);
        let mut values: HashMap<String, f64> = elements
            .into_iter()
            .map(|element| {
                assert_eq!(element.labels.labels.len(), 1);
                (element.labels.labels[0].clone(), element.value)
            })
            .collect();
        assert_eq!(values.remove("host-a"), Some(1100.0));
        assert_eq!(values.remove("host-b"), Some(2050.0));
        assert!(values.is_empty());
    }

    // Regression test for issue #631: different Topk metric names must not
    // become part of the intermediate vector-matching identity.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_range_vector_vector_topk_lhs_topk_rhs() {
        // topk(2, metric_a): host-a=100, host-b=50 survive; host-c=10 dropped.
        // topk(2, metric_b): host-b=200, host-c=300 survive; host-a=5 dropped.
        // Only host-b survives both topks -> expected combined: host-b = 250.
        let query = "topk(2, metric_a) + topk(2, metric_b)";
        let engine = build_range_two_topk_engine(
            "topk(2, metric_a)",
            "topk(2, metric_b)",
            &[("host-a", 100.0), ("host-b", 50.0), ("host-c", 10.0)],
            &[("host-a", 5.0), ("host-b", 200.0), ("host-c", 300.0)],
        );

        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("Expected result for topk/topk range query");
        let elements = matrix_values(qr);

        assert_eq!(
            elements.len(),
            1,
            "Expected only host-b (present in both topks' surviving sets), got {elements:?}"
        );
        assert!(elements[0].labels.labels.contains(&"host-b".to_string()));
        assert_eq!(elements[0].samples.len(), 1);
        assert!((elements[0].samples[0].value - 250.0).abs() < 1e-10);
    }
}
