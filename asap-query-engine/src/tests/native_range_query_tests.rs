//! Range-query pipeline correctness tests (issue #580).
//!
//! `execute_range_query_pipeline` (`simple_engine/mod.rs`), the pipeline
//! behind `handle_range_query_promql`, has two known bugs relative to the
//! instant-query pipeline (`execute_and_merge_store_queries`):
//!
//! 1. It never reads `keys_query`, so dual-population metrics (separate
//!    value/key aggregations, values keyed `None`, grouping coming entirely
//!    from the keys aggregation's `get_keys()`) silently return an empty
//!    result over a range instead of the expanded key set.
//! 2. `finish_range_context` (`promql.rs`) unconditionally forces
//!    `is_exact_query = false`, ignoring the aggregation's real `WindowType`,
//!    so Sliding-window range queries don't fetch/merge the way the instant
//!    path does.
//!
//! These tests are RED against current code: they mirror instant-query
//! precedents that already pass (`native_binary_instant_tests.rs`'s
//! `binary_expr_vector_vector_dual_population` and
//! `binary_expr_sliding_window_end_to_end_merges_correctly`) but drive the
//! range entrypoint instead.

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
    use crate::precompute_operators::{
        CountMinSketchAccumulator, CountMinSketchWithHeapAccumulator, DeltaSetAggregatorAccumulator,
    };
    use crate::stores::simple_map_store::SimpleMapStore;
    use crate::stores::Store;
    use crate::tests::test_utilities::engine_factories::create_engine_multi_timestamp_with_window;
    use crate::AggregateCore;
    use promql_utilities::data_model::KeyByLabelNames;
    use std::collections::HashMap;
    use std::sync::Arc;

    const WINDOW_MS: u64 = 1000;

    fn matrix_values(qr: QueryResult) -> Vec<RangeVectorElement> {
        match qr {
            QueryResult::Matrix(m) => m.values,
            _ => panic!("Expected matrix (range vector) result"),
        }
    }

    /// One tumbling-window bucket: (bucket end timestamp ms, label values, accumulator).
    type TimeSeriesData = Vec<(u64, Option<Vec<String>>, Box<dyn AggregateCore>)>;

    /// Dual-population counterpart to
    /// datafusion::range_query_arithmetic_tests::create_range_engine_two_metrics:
    /// one metric, separate value/key aggregations, data spread across
    /// multiple 1s tumbling-window buckets so a range query has more than
    /// one output step to expand keys for.
    #[allow(clippy::too_many_arguments)]
    fn create_range_engine_dual_input(
        metric: &str,
        value_agg_type: AggregationType,
        key_agg_type: AggregationType,
        grouping_labels: Vec<&str>,
        aggregated_labels: Vec<&str>,
        value_data: TimeSeriesData,
        keys_data: TimeSeriesData,
        promql_query: &str,
    ) -> SimpleEngine {
        let grouping_label_strings: Vec<String> =
            grouping_labels.iter().map(|s| s.to_string()).collect();
        let aggregated_label_strings: Vec<String> =
            aggregated_labels.iter().map(|s| s.to_string()).collect();
        let all_labels: Vec<String> = grouping_label_strings
            .iter()
            .chain(aggregated_label_strings.iter())
            .cloned()
            .collect();

        let mut aggregation_configs = HashMap::new();
        aggregation_configs.insert(
            1u64,
            AggregationConfig {
                aggregation_id: 1,
                aggregation_type: value_agg_type,
                aggregation_sub_type: String::new(),
                parameters: HashMap::new(),
                grouping_labels: KeyByLabelNames::new(grouping_label_strings.clone()),
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
        aggregation_configs.insert(
            2u64,
            AggregationConfig {
                aggregation_id: 2,
                aggregation_type: key_agg_type,
                aggregation_sub_type: String::new(),
                parameters: HashMap::new(),
                grouping_labels: KeyByLabelNames::new(grouping_label_strings),
                aggregated_labels: KeyByLabelNames::new(aggregated_label_strings),
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

        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs,
        });

        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));

        for (agg_id, data) in [(1u64, value_data), (2u64, keys_data)] {
            for (timestamp, label_values_opt, acc) in data {
                let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
                let output = PrecomputedOutput::new(timestamp - WINDOW_MS, timestamp, key, agg_id);
                store.insert_precomputed_output(output, acc).unwrap();
            }
        }

        let promql_schema =
            PromQLSchema::new().add_metric(metric.to_string(), KeyByLabelNames::new(all_labels));

        let query_config = QueryConfig::new(promql_query.to_string())
            .add_aggregation(AggregationReference::new(1, None))
            .add_aggregation(AggregationReference::new(2, None));

        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(promql_schema),
            query_configs: vec![query_config],
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
    async fn range_query_dual_population_returns_key_expansion() {
        // Same dual-population shape as native_binary_instant_tests::binary_expr_vector_vector_dual_population,
        // but queried as a plain range instead of an instant/binary-expr query.
        // Uses create_range_engine_dual_input (not create_engine_dual_input):
        // the latter places its bucket at (timestamp, timestamp), a zero-width
        // window tuned for the instant-query fetch path, which never lines up
        // with the range pipeline's bucket_map (keyed by timestamp - window_size).
        let cms = CountMinSketchAccumulator::new(2, 3);
        let mut keys = DeltaSetAggregatorAccumulator::new();
        keys.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });

        let engine = create_range_engine_dual_input(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::DeltaSetAggregator,
            vec![],
            vec!["host", "event"],
            vec![(1000, None, Box::new(cms) as Box<dyn AggregateCore>)],
            vec![(1000, None, Box::new(keys) as Box<dyn AggregateCore>)],
            "count(event_frequency) by (host, event)",
        );

        let query = "count(event_frequency) by (host, event)";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        assert!(
            !matrix_values(qr).is_empty(),
            "expected keys_query expansion to produce at least one series over the range, \
             matching the instant-query dual-population path"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_sliding_window_merges_both_buckets() {
        // Same fixture as
        // native_binary_instant_tests::binary_expr_sliding_window_end_to_end_merges_correctly,
        // but queried as a plain range instead of an instant/binary-expr query.
        let data = vec![
            (
                1_000_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
            ),
            (
                1_000_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(5.0)) as Box<dyn AggregateCore>,
            ),
        ];
        let query = "sum_over_time(http_requests[1s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            1_000, // window_size_ms, matches the fixed 1000ms bucket width
            WindowType::Sliding,
        );

        let result = engine.handle_range_query_promql(query.to_string(), 1000.0, 1000.5, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);
        assert_eq!(elements.len(), 1, "expected one merged series for host-a");
        assert_eq!(elements[0].samples.len(), 1);
        assert!(
            (elements[0].samples[0].value - 15.0).abs() < 1e-10,
            "expected both sliding-window buckets merged into 15.0, got {}",
            elements[0].samples[0].value
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_sliding_window_merges_three_buckets_same_timestamp() {
        // Generalizes range_query_sliding_window_merges_both_buckets beyond
        // exactly 2 colliding buckets, mirroring
        // native_pipeline_merge_tests::sliding_bucket_count_mismatch_still_returns_merged_result.
        let data = vec![
            (
                1_000_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
            ),
            (
                1_000_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(5.0)) as Box<dyn AggregateCore>,
            ),
            (
                1_000_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(3.0)) as Box<dyn AggregateCore>,
            ),
        ];
        let query = "sum_over_time(http_requests[1s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            1_000,
            WindowType::Sliding,
        );

        let result = engine.handle_range_query_promql(query.to_string(), 1000.0, 1000.5, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);
        assert_eq!(elements.len(), 1, "expected one merged series for host-a");
        assert_eq!(elements[0].samples.len(), 1);
        assert!(
            (elements[0].samples[0].value - 18.0).abs() < 1e-10,
            "expected all 3 sliding-window buckets merged into 18.0, got {}",
            elements[0].samples[0].value
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_sliding_window_single_bucket_regression() {
        // No-collision counterpart to the merge tests above: a single
        // Sliding bucket per output step must still return its value
        // unchanged once is_exact_query correctly honors WindowType::Sliding
        // for range queries. Mirrors
        // native_pipeline_merge_tests::sliding_single_bucket_returns_its_value.
        let data = vec![(
            1_000_000,
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(42.0)) as Box<dyn AggregateCore>,
        )];
        let query = "sum_over_time(http_requests[1s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            1_000,
            WindowType::Sliding,
        );

        let result = engine.handle_range_query_promql(query.to_string(), 1000.0, 1000.5, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);
        assert_eq!(elements.len(), 1);
        assert_eq!(elements[0].samples.len(), 1);
        assert!(
            (elements[0].samples[0].value - 42.0).abs() < 1e-10,
            "expected the single sliding-window bucket's value unchanged, got {}",
            elements[0].samples[0].value
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_dual_population_expands_keys_across_multiple_steps() {
        // Extends range_query_dual_population_returns_key_expansion across
        // two output timestamps instead of one, so a fix that only expands
        // keys for the first step (rather than every step in the loop)
        // still fails this.
        let cms_1 = CountMinSketchAccumulator::new(2, 3);
        let cms_2 = CountMinSketchAccumulator::new(2, 3);
        let mut keys_1 = DeltaSetAggregatorAccumulator::new();
        keys_1.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });
        let mut keys_2 = DeltaSetAggregatorAccumulator::new();
        keys_2.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });

        let engine = create_range_engine_dual_input(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::DeltaSetAggregator,
            vec![],
            vec!["host", "event"],
            vec![
                (1000, None, Box::new(cms_1) as Box<dyn AggregateCore>),
                (2000, None, Box::new(cms_2) as Box<dyn AggregateCore>),
            ],
            vec![
                (1000, None, Box::new(keys_1) as Box<dyn AggregateCore>),
                (2000, None, Box::new(keys_2) as Box<dyn AggregateCore>),
            ],
            "count(event_frequency) by (host, event)",
        );

        let query = "count(event_frequency) by (host, event)";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);
        assert!(
            !elements.is_empty(),
            "expected keys_query expansion to produce at least one series over the range"
        );
        let timestamps: std::collections::HashSet<u64> = elements
            .iter()
            .flat_map(|e| e.samples.iter().map(|s| s.timestamp))
            .collect();
        assert_eq!(
            timestamps,
            std::collections::HashSet::from([1000, 2000]),
            "expected keys expansion at every output step, not just the first, got {:?}",
            timestamps
        );
    }

    /// Single-population counterpart to `create_range_engine_dual_input`: one
    /// `CountMinSketchWithHeap` (self-keyed, top-k) aggregation, no separate
    /// keys aggregation, values stored with `group_key = None`.
    fn create_range_engine_self_keyed(
        metric: &str,
        aggregated_label: &str,
        data: Vec<(u64, CountMinSketchWithHeapAccumulator)>,
        promql_query: &str,
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
                aggregated_labels: KeyByLabelNames::new(vec![aggregated_label.to_string()]),
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

        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs,
        });

        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));

        for (timestamp, acc) in data {
            let output = PrecomputedOutput::new(timestamp - WINDOW_MS, timestamp, None, 1);
            store
                .insert_precomputed_output(output, Box::new(acc))
                .unwrap();
        }

        let promql_schema = PromQLSchema::new().add_metric(
            metric.to_string(),
            KeyByLabelNames::new(vec![aggregated_label.to_string()]),
        );

        let query_config = QueryConfig::new(promql_query.to_string())
            .add_aggregation(AggregationReference::new(1, None));

        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(promql_schema),
            query_configs: vec![query_config],
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
    async fn range_query_self_keyed_topk_expands_without_keys_query() {
        // #584: execute_range_query_pipeline's single-population branch (no
        // separate keys_query) used each value group's own group_key
        // directly and never called get_keys() on the value accumulator
        // itself. A self-keyed accumulator like CountMinSketchWithHeap
        // (top-k) is stored with group_key = None (single population, no
        // keys_query) and only exposes its output keys via get_keys() on the
        // merged accumulator -- exactly what the instant path
        // (collect_results_same_aggregation) already does. Without that
        // call, this None-keyed group is dropped and the range query returns
        // empty instead of the expanded top-k keys.
        let mut sketch = CountMinSketchWithHeapAccumulator::new(3, 1024, 32);
        sketch.inner.update("host-a", 30.0);
        sketch.inner.update("host-b", 20.0);
        sketch.inner.update("host-c", 10.0);

        let query = "topk(5, transfer_events)";
        let engine =
            create_range_engine_self_keyed("transfer_events", "srcip", vec![(1000, sketch)], query);

        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 1.5, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);

        assert_eq!(
            elements.len(),
            3,
            "expected the self-keyed accumulator's top-k heap to expand into 3 \
             separate series (one per key) instead of being dropped as an empty result, got: {:?}",
            elements
                .iter()
                .map(|e| e.labels.labels.clone())
                .collect::<Vec<_>>()
        );

        let returned: std::collections::HashSet<String> = elements
            .iter()
            .map(|e| e.labels.labels.last().cloned().unwrap_or_default())
            .collect();
        assert_eq!(
            returned,
            std::collections::HashSet::from([
                "host-a".to_string(),
                "host-b".to_string(),
                "host-c".to_string(),
            ]),
            "expected one expanded series per top-k key"
        );

        let host_a = elements
            .iter()
            .find(|e| e.labels.labels.last().map(String::as_str) == Some("host-a"))
            .expect("host-a series should be present among the expanded top-k keys");
        assert_eq!(host_a.samples.len(), 1);
        assert!(
            (host_a.samples[0].value - 30.0).abs() < 1e-9,
            "expected host-a's count-min-sketch estimate (30.0), got {}",
            host_a.samples[0].value
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_self_keyed_topk_expands_with_non_none_outer_key() {
        // Same bug as range_query_self_keyed_topk_expands_without_keys_query,
        // but the value data is stored under a real, non-None outer
        // group_key -- mirrors what real Arroyo/worker.rs ingestion actually
        // writes for "empty grouping" self-keyed accumulators (deserialize_
        // from_json_arroyo always wraps Some(key), even for empty grouping;
        // "None" is specific to this codebase's existing CountMinSketchWithHeap
        // test convention, not a guarantee). A fix that only calls
        // merged.get_keys() when the outer key is None (rather than always,
        // like collect_results_same_aggregation does) would still miss this
        // case.
        let metric = "transfer_events";
        let aggregated_label = "srcip";
        let mut aggregation_configs = HashMap::new();
        aggregation_configs.insert(
            1u64,
            AggregationConfig {
                aggregation_id: 1,
                aggregation_type: AggregationType::CountMinSketchWithHeap,
                aggregation_sub_type: String::new(),
                parameters: HashMap::new(),
                grouping_labels: KeyByLabelNames::empty(),
                aggregated_labels: KeyByLabelNames::new(vec![aggregated_label.to_string()]),
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
        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs,
        });
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));

        let mut sketch = CountMinSketchWithHeapAccumulator::new(3, 1024, 32);
        sketch.inner.update("host-a", 30.0);
        sketch.inner.update("host-b", 20.0);
        sketch.inner.update("host-c", 10.0);

        // Non-None outer key -- e.g. Some(KeyByLabelValues{labels: [""]}),
        // exactly what deserialize_from_json_arroyo produces for "" grouping.
        let non_none_key = Some(KeyByLabelValues::new_with_labels(vec![String::new()]));
        let output = PrecomputedOutput::new(0, 1000, non_none_key, 1);
        store
            .insert_precomputed_output(output, Box::new(sketch))
            .unwrap();

        let promql_schema = PromQLSchema::new().add_metric(
            metric.to_string(),
            KeyByLabelNames::new(vec![aggregated_label.to_string()]),
        );
        let query = "topk(5, transfer_events)";
        let query_config =
            QueryConfig::new(query.to_string()).add_aggregation(AggregationReference::new(1, None));
        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(promql_schema),
            query_configs: vec![query_config],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };
        let engine = SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            WINDOW_MS,
            QueryLanguage::promql,
        );

        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 1.5, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);
        assert_eq!(
            elements.len(),
            3,
            "expected top-k expansion even though the outer group_key is Some(..), not None, got: {:?}",
            elements.iter().map(|e| e.labels.labels.clone()).collect::<Vec<_>>()
        );
    }
}
