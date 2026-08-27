//! Hardening tests for the store-query contract that governs how Sliding vs
//! Tumbling aggregations get fetched/combined, written ahead of an internal
//! refactor of that decision logic (see `exact_window_grid_adversarial_tests.rs`
//! for the original bug writeups this complements: #600, #606, #608).
//!
//! These tests are deliberately NOT shaped around any particular internal
//! function or struct -- every assertion here is made by driving the public
//! PromQL entry points (`handle_query_promql` / `handle_range_query_promql`)
//! and reading back `QueryResult` values, so they stay valid across an
//! internal reshape of how the tolerant-scan vs exact-lookup decision is
//! threaded through the code.
//!
//! Distinct from the existing coverage in `exact_window_grid_adversarial_tests.rs`
//! and `native_range_query_tests.rs`, this file focuses on:
//!
//! 1. Directly cross-checking the INSTANT and RANGE query paths against each
//!    other (not just against a hand-computed literal) for both Sliding and
//!    Tumbling, at the same timestamp -- the exact shape of bug that has
//!    recurred three times (different query code paths drifting out of
//!    sync).
//! 2. An instant-query keys lookup with a Sliding key aggregation (the
//!    existing Sliding-keys coverage in `native_range_query_tests.rs` only
//!    drives the range path).
//! 3. Grid-aligned vs off-grid instant query times for a Sliding aggregation.
//! 4. Query span exactly one bucket wide vs narrower than one bucket,
//!    through the PromQL engine (the existing coverage for this shape lives
//!    at the `Store` trait level in `exact_window_grid_adversarial_tests.rs`,
//!    not through PromQL).

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
        CountMinSketchAccumulator, DeltaSetAggregatorAccumulator, SetAggregatorAccumulator,
    };
    use crate::stores::simple_map_store::SimpleMapStore;
    use crate::stores::Store;
    use crate::tests::test_utilities::engine_factories::create_engine_multi_timestamp_with_window;
    use crate::AggregateCore;
    use promql_utilities::data_model::KeyByLabelNames;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn matrix_values(qr: QueryResult) -> Vec<RangeVectorElement> {
        match qr {
            QueryResult::Matrix(m) => m.values,
            _ => panic!("expected matrix (range vector) result"),
        }
    }

    fn vector_values(qr: QueryResult) -> Vec<(Vec<String>, f64)> {
        match qr {
            QueryResult::Vector(iv) => iv
                .values
                .into_iter()
                .map(|e| (e.labels.labels, e.value))
                .collect(),
            _ => panic!("expected vector (instant) result"),
        }
    }

    fn host_a_samples(elements: &[RangeVectorElement]) -> Vec<(u64, f64)> {
        let mut samples: Vec<(u64, f64)> = elements
            .iter()
            .find(|e| e.labels.labels.contains(&"host-a".to_string()))
            .map(|e| e.samples.iter().map(|s| (s.timestamp, s.value)).collect())
            .unwrap_or_default();
        samples.sort_by_key(|(ts, _)| *ts);
        samples
    }

    fn assert_close(actual: f64, expected: f64, ctx: &str) {
        assert!(
            (actual - expected).abs() < 1e-6,
            "{ctx}: expected {expected}, got {actual}"
        );
    }

    /// Single host-a instant value, or panics if absent/ambiguous -- used
    /// where a test wants "the" value, not merely presence.
    fn single_host_a_value(qr: QueryResult) -> f64 {
        let values = vector_values(qr);
        let matches: Vec<f64> = values
            .into_iter()
            .filter(|(labels, _)| labels.contains(&"host-a".to_string()))
            .map(|(_, v)| v)
            .collect();
        assert_eq!(
            matches.len(),
            1,
            "expected exactly one host-a series in instant result"
        );
        matches[0]
    }

    // ════════════════════════════════════════════════════════════════════
    // 1. Sliding: instant and range paths must AGREE, at every step, on
    //    exactly the correct (non-double-counted) value.
    // ════════════════════════════════════════════════════════════════════

    /// window_size=3000, slide=1000 -- classic overlapping-Sliding-window
    /// shape (the historical 111-vs-12321 bug). Panes: 1000->1, 2000->10,
    /// 3000->100, 4000->1000, 5000->10000.
    ///   step 3000: window [0,3000)    -> 1+10+100=111
    ///   step 4000: window [1000,4000) -> 10+100+1000=1110
    ///   step 5000: window [2000,5000) -> 100+1000+10000=11100
    /// A naive "sum every overlapping window touching the range" substitute
    /// would produce 111+1110+11100=12321 for a query collapsed to one
    /// point -- this test's exact per-step values are far enough apart from
    /// that failure mode (and from each other) that neither the instant nor
    /// the range path can silently drift into it without failing here.
    ///
    /// The key property this test pins beyond `exact_window_grid_adversarial_tests`
    /// is that BOTH paths are driven from the SAME engine/data and compared
    /// directly against each other (`assert_eq!(instant_value, range_value)`),
    /// not just each checked separately against a hand-computed literal --
    /// so a refactor that makes the two paths diverge fails immediately even
    /// if it happens to leave one of them "coincidentally" correct-looking.
    #[tokio::test(flavor = "multi_thread")]
    async fn sliding_instant_and_range_agree_at_every_step_no_double_count() {
        let data = vec![
            (
                1_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(1.0)) as Box<dyn AggregateCore>,
            ),
            (
                2_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
            ),
            (
                3_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(100.0)) as Box<dyn AggregateCore>,
            ),
            (
                4_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(1_000.0)) as Box<dyn AggregateCore>,
            ),
            (
                5_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(10_000.0)) as Box<dyn AggregateCore>,
            ),
        ];
        let query = "sum_over_time(cpu_load[3s])";
        let engine = create_engine_multi_timestamp_with_window(
            "cpu_load",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            3_000,
            1_000,
            WindowType::Sliding,
        );

        let range_result = engine
            .handle_range_query_promql(query.to_string(), 3.0, 5.0, 1.0)
            .expect("range query failed");
        let range_samples = host_a_samples(&matrix_values(range_result.1));

        let expected = vec![(3_000u64, 111.0), (4_000, 1_110.0), (5_000, 11_100.0)];
        assert_eq!(
            range_samples, expected,
            "range path must produce exactly the un-inflated per-step values"
        );

        for (query_time_sec, (ts, expected_value)) in
            [3.0, 4.0, 5.0].into_iter().zip(expected.into_iter())
        {
            let instant_result = engine
                .handle_query_promql(query.to_string(), query_time_sec)
                .unwrap_or_else(|| panic!("instant query at t={query_time_sec} failed"));
            let instant_value = single_host_a_value(instant_result.1);
            assert_close(
                instant_value,
                expected_value,
                &format!("instant value at t={ts}"),
            );

            let range_value = range_samples
                .iter()
                .find(|(s_ts, _)| *s_ts == ts)
                .map(|(_, v)| *v)
                .unwrap_or_else(|| panic!("range path missing sample at t={ts}"));
            assert_close(
                instant_value,
                range_value,
                &format!(
                    "instant and range paths must agree exactly at t={ts} -- \
                     a path that independently re-derives Sliding's window/merge \
                     logic can silently drift out of sync with the other"
                ),
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wider_sliding_instant_query_merges_only_a_non_overlapping_exact_cover() {
        let data = [1.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0]
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                (
                    (index as u64 + 1) * 1_000,
                    Some(vec!["host-a".to_string()]),
                    Box::new(SumAccumulator::with_sum(value)) as Box<dyn AggregateCore>,
                )
            })
            .collect();
        let query = "sum_over_time(cpu_load[6s])";
        let engine = create_engine_multi_timestamp_with_window(
            "cpu_load",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            3_000,
            1_000,
            WindowType::Sliding,
        );

        let result = engine
            .handle_query_promql(query.to_string(), 6.0)
            .expect("the wider Sliding query should be accelerated");

        assert_close(
            single_host_a_value(result.1),
            111_111.0,
            "[0, 6s) must be composed from [0, 3s) and [3s, 6s)",
        );

        let misaligned_result = engine
            .handle_query_promql(query.to_string(), 6.5)
            .expect("the endpoint should align down to the latest complete slide boundary");
        assert_close(
            single_host_a_value(misaligned_result.1),
            111_111.0,
            "a 6.5s evaluation must read the complete cover ending at 6s",
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wider_sliding_query_with_a_missing_constituent_falls_back_as_a_whole() {
        // Omitting the pane ending at 2s prevents [0, 3s) from being emitted,
        // while all panes for [3s, 6s) remain present.
        let data = [
            (1, 1.0),
            (3, 100.0),
            (4, 1_000.0),
            (5, 10_000.0),
            (6, 100_000.0),
        ]
        .into_iter()
        .map(|(second, value)| {
            (
                second * 1_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(value)) as Box<dyn AggregateCore>,
            )
        })
        .collect();
        let query = "sum_over_time(cpu_load[6s])";
        let engine = create_engine_multi_timestamp_with_window(
            "cpu_load",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            3_000,
            1_000,
            WindowType::Sliding,
        );

        assert!(
            engine.handle_query_promql(query.to_string(), 6.0).is_none(),
            "a partial exact cover must fall back instead of returning partial data"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wider_sliding_range_query_composes_each_output_step_without_overlap() {
        let data = [1.0, 10.0, 100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0]
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                (
                    (index as u64 + 1) * 1_000,
                    Some(vec!["host-a".to_string()]),
                    Box::new(SumAccumulator::with_sum(value)) as Box<dyn AggregateCore>,
                )
            })
            .collect();
        let query = "sum_over_time(cpu_load[6s])";
        let engine = create_engine_multi_timestamp_with_window(
            "cpu_load",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            3_000,
            1_000,
            WindowType::Sliding,
        );

        let result = engine
            .handle_range_query_promql(query.to_string(), 6.0, 7.0, 1.0)
            .expect("the wider Sliding range query should be accelerated");

        assert_eq!(
            host_a_samples(&matrix_values(result.1)),
            vec![(6_000, 111_111.0), (7_000, 1_111_110.0)]
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // 2. Tumbling: instant and range paths must AGREE, and both must
    //    correctly SUM every disjoint bucket in the query's window (proving
    //    a fix for Sliding's over-counting can't accidentally break
    //    Tumbling's legitimate summing).
    // ════════════════════════════════════════════════════════════════════

    /// 4 disjoint Tumbling buckets (1000ms each) with distinct values
    /// 1,2,3,4 at ts=1000,2000,3000,4000. A `sum_over_time(metric[4s])`
    /// query spans exactly all 4 -- correct answer is the full sum (10.0).
    /// A buggy "exact-lookup-only" substitute (treating Tumbling like
    /// Sliding: pick one bucket, don't merge) would return just the last
    /// bucket's value (4.0) instead -- distinguishable from the correct sum
    /// by more than 1e-6, so this test would catch that regression too.
    #[tokio::test(flavor = "multi_thread")]
    async fn tumbling_instant_and_range_agree_and_sum_all_n_windows() {
        let data = vec![
            (
                1_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(1.0)) as Box<dyn AggregateCore>,
            ),
            (
                2_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(2.0)) as Box<dyn AggregateCore>,
            ),
            (
                3_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(3.0)) as Box<dyn AggregateCore>,
            ),
            (
                4_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(4.0)) as Box<dyn AggregateCore>,
            ),
        ];
        let query = "sum_over_time(cpu_load[4s])";
        let engine = create_engine_multi_timestamp_with_window(
            "cpu_load",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            1_000, // window_size_ms: each ingest bucket is 1000ms, Tumbling
            1_000, // slide_interval_ms == window_size_ms for Tumbling
            WindowType::Tumbling,
        );

        let instant_result = engine
            .handle_query_promql(query.to_string(), 4.0)
            .expect("instant query failed");
        let instant_value = single_host_a_value(instant_result.1);

        let range_result = engine
            .handle_range_query_promql(query.to_string(), 4.0, 4.5, 1.0)
            .expect("range query failed");
        let range_samples = host_a_samples(&matrix_values(range_result.1));

        assert_eq!(range_samples.len(), 1, "expected exactly one output step");
        let (range_ts, range_value) = range_samples[0];
        assert_eq!(range_ts, 4_000);

        assert_close(
            instant_value,
            10.0,
            "instant value must be the sum of all 4 disjoint Tumbling buckets (1+2+3+4)",
        );
        assert_close(
            range_value,
            10.0,
            "range value must be the sum of all 4 disjoint Tumbling buckets (1+2+3+4)",
        );
        assert_close(
            instant_value,
            range_value,
            "instant and range paths must agree exactly on the Tumbling sum",
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // 3. Keys queries: correct key set through the INSTANT path regardless
    //    of whether the key aggregation is Sliding or Tumbling.
    // ════════════════════════════════════════════════════════════════════

    /// Builds a dual-population engine (separate value/key aggregations)
    /// with ONE key bucket, ending at `end_ts`, whose width and window type
    /// are caller-controlled. For Sliding, the bucket is inserted already
    /// pre-merged and `window_size_ms`-wide -- exactly the shape real
    /// Sliding data takes when it reaches the store (see
    /// `worker.rs::merge_panes_for_window`, and the same convention used by
    /// `native_range_query_tests.rs::create_range_engine_dual_input_sliding_keys`).
    /// SetAggregator is used for the key aggregation (not DeltaSetAggregator,
    /// which #606 restricts to Tumbling only, so it can't express the
    /// Sliding case here).
    fn build_dual_engine_with_key_window(
        end_ts: u64,
        key_window_type: WindowType,
        key_window_size_ms: u64,
        key_slide_interval_ms: u64,
    ) -> SimpleEngine {
        let mut aggregation_configs = HashMap::new();
        aggregation_configs.insert(
            1u64,
            AggregationConfig {
                aggregation_id: 1,
                aggregation_type: AggregationType::CountMinSketch,
                aggregation_sub_type: String::new(),
                parameters: HashMap::new(),
                grouping_labels: KeyByLabelNames::empty(),
                aggregated_labels: KeyByLabelNames::empty(),
                rollup_labels: KeyByLabelNames::empty(),
                original_yaml: String::new(),
                window_size_ms: 1_000,
                slide_interval_ms: 1_000,
                window_type: WindowType::Tumbling,
                spatial_filter: String::new(),
                spatial_filter_normalized: String::new(),
                metric: "event_frequency".to_string(),
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
                aggregation_type: AggregationType::SetAggregator,
                aggregation_sub_type: String::new(),
                parameters: HashMap::new(),
                grouping_labels: KeyByLabelNames::empty(),
                aggregated_labels: KeyByLabelNames::new(vec!["host".to_string()]),
                rollup_labels: KeyByLabelNames::empty(),
                original_yaml: String::new(),
                window_size_ms: key_window_size_ms,
                slide_interval_ms: key_slide_interval_ms,
                window_type: key_window_type,
                spatial_filter: String::new(),
                spatial_filter_normalized: String::new(),
                metric: "event_frequency".to_string(),
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

        // Value bucket: plain Tumbling 1000ms, ending at end_ts.
        let cms = CountMinSketchAccumulator::new(2, 3);
        let value_output = PrecomputedOutput::new(end_ts - 1_000, end_ts, None, 1);
        store
            .insert_precomputed_output(value_output, Box::new(cms))
            .unwrap();

        // Key bucket: window_size_ms-wide, ending at end_ts -- already
        // pre-merged, matching real Sliding data's on-store shape.
        let mut keys = SetAggregatorAccumulator::new();
        keys.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string()],
        });
        let keys_output = PrecomputedOutput::new(end_ts - key_window_size_ms, end_ts, None, 2);
        store
            .insert_precomputed_output(keys_output, Box::new(keys))
            .unwrap();

        let promql_schema = PromQLSchema::new().add_metric(
            "event_frequency".to_string(),
            KeyByLabelNames::new(vec!["host".to_string()]),
        );
        let query_config = QueryConfig::new("count(event_frequency) by (host)".to_string())
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
            1_000,
            QueryLanguage::promql,
        )
    }

    /// Two engines, identical value data and identical logical key ("host-a"
    /// valid over the window ending at t=5000), differing only in whether
    /// the KEY aggregation is Tumbling (window=slide=1000) or Sliding
    /// (window=2000, slide=1000). Both must resolve the SAME key set through
    /// the instant query path -- keys queries conceptually always need to
    /// see the key's own bucket correctly, independent of the value-side
    /// double-counting concern that only applies to Sliding VALUE data.
    #[tokio::test(flavor = "multi_thread")]
    async fn prebound_set_aggregator_on_a_different_grid_is_rejected() {
        let tumbling_engine =
            build_dual_engine_with_key_window(5_000, WindowType::Tumbling, 1_000, 1_000);
        let sliding_engine =
            build_dual_engine_with_key_window(5_000, WindowType::Sliding, 2_000, 1_000);

        let query = "count(event_frequency) by (host)";

        let (_, tumbling_qr) = tumbling_engine
            .handle_query_promql(query.to_string(), 5.0)
            .expect("tumbling-keys instant query failed");
        let tumbling_values = vector_values(tumbling_qr);

        assert!(
            tumbling_values
                .iter()
                .any(|(labels, _)| labels.contains(&"host-a".to_string())),
            "Tumbling key aggregation must resolve host-a, got {tumbling_values:?}"
        );
        assert!(
            sliding_engine
                .handle_query_promql(query.to_string(), 5.0)
                .is_none(),
            "a pre-bound SetAggregator must share the value aggregation's window grid"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wider_sliding_set_aggregator_unions_keys_from_the_same_exact_cover() {
        let query = "sum by (host) (count_over_time(event_frequency[6s]))";
        let common = |aggregation_id, aggregation_type| AggregationConfig {
            aggregation_id,
            aggregation_type,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::empty(),
            aggregated_labels: KeyByLabelNames::new(vec!["host".to_string()]),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size_ms: 3_000,
            slide_interval_ms: 1_000,
            window_type: WindowType::Sliding,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: "event_frequency".to_string(),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        };
        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs: HashMap::from([
                (1, common(1, AggregationType::CountMinSketch)),
                (2, common(2, AggregationType::SetAggregator)),
            ]),
        });
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));
        let host_a = KeyByLabelValues {
            labels: vec!["host-a".to_string()],
        };
        let host_b = KeyByLabelValues {
            labels: vec!["host-b".to_string()],
        };
        for (start, end, a_count, b_count) in [(0, 3_000, 1.0, 2.0), (3_000, 6_000, 10.0, 20.0)] {
            let mut cms = CountMinSketchAccumulator::new(4, 128);
            cms.inner.update(&host_a.to_semicolon_str(), a_count);
            cms.inner.update(&host_b.to_semicolon_str(), b_count);
            store
                .insert_precomputed_output(
                    PrecomputedOutput::new(start, end, None, 1),
                    Box::new(cms),
                )
                .unwrap();

            let mut keys = SetAggregatorAccumulator::new();
            keys.add_key(if start == 0 {
                host_a.clone()
            } else {
                host_b.clone()
            });
            store
                .insert_precomputed_output(
                    PrecomputedOutput::new(start, end, None, 2),
                    Box::new(keys),
                )
                .unwrap();
        }

        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(PromQLSchema::new().add_metric(
                "event_frequency".to_string(),
                KeyByLabelNames::new(vec!["host".to_string()]),
            )),
            query_configs: vec![QueryConfig::new(query.to_string())
                .add_aggregation(AggregationReference::new(1, None))
                .add_aggregation(AggregationReference::new(2, None))],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };
        let engine = SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            1_000,
            QueryLanguage::promql,
        );

        let (_, result) = engine
            .handle_query_promql(query.to_string(), 6.0)
            .expect("the wider dual-population Sliding query should be accelerated");
        let mut values = vector_values(result);
        values.sort_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(
            values,
            vec![
                (vec!["host-a".to_string()], 11.0),
                (vec!["host-b".to_string()], 22.0),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wider_sliding_values_use_compatible_tumbling_delta_set_keys() {
        let query = "sum by (host) (count_over_time(event_frequency[4s]))";
        let value_config = AggregationConfig {
            aggregation_id: 1,
            aggregation_type: AggregationType::CountMinSketch,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::empty(),
            aggregated_labels: KeyByLabelNames::new(vec!["host".to_string()]),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size_ms: 2_000,
            slide_interval_ms: 1_000,
            window_type: WindowType::Sliding,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: "event_frequency".to_string(),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        };
        let delta_config = AggregationConfig {
            aggregation_id: 2,
            aggregation_type: AggregationType::DeltaSetAggregator,
            window_size_ms: 1_000,
            slide_interval_ms: 1_000,
            window_type: WindowType::Tumbling,
            ..value_config.clone()
        };
        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs: HashMap::from([(1, value_config), (2, delta_config)]),
        });
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));
        let host_a = KeyByLabelValues {
            labels: vec!["host-a".to_string()],
        };
        let host_b = KeyByLabelValues {
            labels: vec!["host-b".to_string()],
        };
        let host_c = KeyByLabelValues {
            labels: vec!["host-c".to_string()],
        };
        for (start, end, a_count, b_count) in [(0, 2_000, 1.0, 2.0), (2_000, 4_000, 10.0, 20.0)] {
            let mut cms = CountMinSketchAccumulator::new(4, 128);
            cms.inner.update(&host_a.to_semicolon_str(), a_count);
            cms.inner.update(&host_b.to_semicolon_str(), b_count);
            store
                .insert_precomputed_output(
                    PrecomputedOutput::new(start, end, None, 1),
                    Box::new(cms),
                )
                .unwrap();
        }
        for (start, key) in [
            (0, host_a.clone()),
            (2_000, host_b.clone()),
            // This delta belongs to the next value-grid interval. A query
            // evaluated at 4.5s aligns values down to 4s and must not see it.
            (4_000, host_c),
        ] {
            let mut delta = DeltaSetAggregatorAccumulator::new();
            delta.add_key(key);
            store
                .insert_precomputed_output(
                    PrecomputedOutput::new(start, start + 1_000, None, 2),
                    Box::new(delta),
                )
                .unwrap();
        }
        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(PromQLSchema::new().add_metric(
                "event_frequency".to_string(),
                KeyByLabelNames::new(vec!["host".to_string()]),
            )),
            query_configs: vec![QueryConfig::new(query.to_string())
                .add_aggregation(AggregationReference::new(1, None))
                .add_aggregation(AggregationReference::new(2, None))],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };
        let engine = SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            1_000,
            QueryLanguage::promql,
        );

        let (_, result) = engine
            .handle_query_promql(query.to_string(), 4.0)
            .expect("compatible Tumbling DeltaSet keys should resolve Sliding values");
        let mut values = vector_values(result);
        values.sort_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(
            values,
            vec![
                (vec!["host-a".to_string()], 11.0),
                (vec!["host-b".to_string()], 22.0),
            ]
        );

        let (_, misaligned_result) = engine
            .handle_query_promql(query.to_string(), 4.5)
            .expect("misaligned evaluation should use the latest complete value grid point");
        let mut misaligned_values = vector_values(misaligned_result);
        misaligned_values.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            misaligned_values,
            vec![
                (vec!["host-a".to_string()], 11.0),
                (vec!["host-b".to_string()], 22.0),
            ],
            "DeltaSet replay must stop at the same aligned endpoint as Sliding values"
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // 4. Sliding: a grid position with a genuine (fully-paned) window must
    //    resolve to its exact value; a grid position whose window is
    //    missing a pane (a gap) must resolve to NO data -- not a
    //    fabricated/wrong-window value borrowed from a neighboring window.
    // ════════════════════════════════════════════════════════════════════

    /// window_size=2000, slide=1000. Panes at 1000(v=5), 2000(v=7),
    /// [3000 missing], 4000(v=9), 5000(v=11). Per
    /// `create_engine_multi_timestamp_with_window`'s pane-merge logic, a
    /// window only materializes in the store if ALL its panes are present:
    ///   window_start=0    -> panes {1000,2000} both present -> [0,2000)=12
    ///   window_start=1000 -> panes {2000,3000} -- 3000 missing -> gap
    ///   window_start=2000 -> panes {3000,4000} -- 3000 missing -> gap
    ///   window_start=3000 -> panes {4000,5000} both present -> [3000,5000)=20
    /// query_time=2.0s -> window [0,2000)    -> resolves to 12.0.
    /// query_time=3.0s -> window [1000,3000) -> gap -> must be absent.
    /// query_time=5.0s -> window [3000,5000) -> resolves to 20.0.
    #[tokio::test(flavor = "multi_thread")]
    async fn sliding_instant_query_resolves_present_window_and_returns_no_data_for_gap() {
        let data = vec![
            (
                1_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(5.0)) as Box<dyn AggregateCore>,
            ),
            (
                2_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(7.0)) as Box<dyn AggregateCore>,
            ),
            // gap: no pane ending at 3000
            (
                4_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(9.0)) as Box<dyn AggregateCore>,
            ),
            (
                5_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(11.0)) as Box<dyn AggregateCore>,
            ),
        ];
        let query = "sum_over_time(cpu_load[2s])";
        let engine = create_engine_multi_timestamp_with_window(
            "cpu_load",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            2_000,
            1_000,
            WindowType::Sliding,
        );

        let (_, first_qr) = engine
            .handle_query_promql(query.to_string(), 2.0)
            .expect("instant query for the fully-paned window [0,2000) failed");
        assert_close(
            single_host_a_value(first_qr),
            12.0,
            "window [0,2000) has both required panes (5+7) and must resolve exactly",
        );

        let gap_result = engine.handle_query_promql(query.to_string(), 3.0);
        let gap_values = match gap_result {
            Some((_, qr)) => vector_values(qr),
            None => Vec::new(),
        };
        assert!(
            !gap_values
                .iter()
                .any(|(labels, _)| labels.contains(&"host-a".to_string())),
            "window [1000,3000) is missing its 3000-ending pane -- must yield no data \
             for host-a, not a value borrowed from a neighboring window: got {gap_values:?}"
        );

        let (_, third_qr) = engine
            .handle_query_promql(query.to_string(), 5.0)
            .expect("instant query for the fully-paned window [3000,5000) failed");
        assert_close(
            single_host_a_value(third_qr),
            20.0,
            "window [3000,5000) has both required panes (9+11) and must resolve exactly, \
             unaffected by the gap immediately before it",
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // 5/6. Query span exactly one bucket wide vs narrower than one bucket.
    // ════════════════════════════════════════════════════════════════════

    /// Single Tumbling bucket [0,1000)=42. A `[1s]` query's window is
    /// exactly as wide as the stored bucket -- both instant and range paths
    /// must return the bucket's exact value, not empty and not doubled.
    #[tokio::test(flavor = "multi_thread")]
    async fn tumbling_query_exactly_one_bucket_wide_returns_exact_value() {
        let data = vec![(
            1_000,
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(42.0)) as Box<dyn AggregateCore>,
        )];
        let query = "sum_over_time(cpu_load[1s])";
        let engine = create_engine_multi_timestamp_with_window(
            "cpu_load",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            1_000,
            1_000,
            WindowType::Tumbling,
        );

        let (_, instant_qr) = engine
            .handle_query_promql(query.to_string(), 1.0)
            .expect("instant query failed");
        let instant_value = single_host_a_value(instant_qr);
        assert_close(
            instant_value,
            42.0,
            "query span exactly one bucket wide must return the bucket's exact value",
        );

        let (_, range_qr) = engine
            .handle_range_query_promql(query.to_string(), 1.0, 1.5, 1.0)
            .expect("range query failed");
        let range_samples = host_a_samples(&matrix_values(range_qr));
        assert_eq!(
            range_samples,
            vec![(1_000, 42.0)],
            "range path must agree with the instant path for a query span exactly \
             one bucket wide"
        );
    }

    /// Same single Tumbling bucket [0,1000)=42, but the query's window
    /// (`[500ms]`) is NARROWER than the stored bucket. No stored bucket's
    /// `[window_start, window_end)` fits inside the requested `[500,1000)`
    /// span, so the correct behavior is to return NO data -- not a
    /// fabricated partial value (e.g. neither 42.0 wrongly reused nor some
    /// halved/interpolated 21.0).
    #[tokio::test(flavor = "multi_thread")]
    async fn tumbling_query_narrower_than_bucket_returns_no_data_not_partial() {
        let data = vec![(
            1_000,
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(42.0)) as Box<dyn AggregateCore>,
        )];
        let query = "sum_over_time(cpu_load[500ms])";
        let engine = create_engine_multi_timestamp_with_window(
            "cpu_load",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            1_000,
            1_000,
            WindowType::Tumbling,
        );

        let instant_result = engine.handle_query_promql(query.to_string(), 1.0);
        let instant_values = match instant_result {
            Some((_, qr)) => vector_values(qr),
            None => Vec::new(),
        };
        assert!(
            !instant_values
                .iter()
                .any(|(labels, _)| labels.contains(&"host-a".to_string())),
            "a query window narrower than the stored bucket must yield no data \
             for host-a, not a fabricated/partial value: got {instant_values:?}"
        );

        let range_result = engine.handle_range_query_promql(query.to_string(), 1.0, 1.5, 1.0);
        let range_has_sample_at_1000 = match range_result {
            Some((_, qr)) => host_a_samples(&matrix_values(qr))
                .iter()
                .any(|(ts, _)| *ts == 1_000),
            None => false,
        };
        assert!(
            !range_has_sample_at_1000,
            "range path must likewise yield no sample at t=1000 for a query window \
             narrower than the stored bucket"
        );
    }
}
