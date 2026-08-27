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
//! 2. The range per-step merge logic didn't distinguish Sliding from
//!    Tumbling, so Sliding-window range queries didn't fetch/merge the way
//!    the instant path does (fixed by #608/#621).
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
        CountMinSketchAccumulator, CountMinSketchWithHeapAccumulator,
        DeltaSetAggregatorAccumulator, SetAggregatorAccumulator,
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

    /// True if the output series for `host_label` (matched by label value) has
    /// a sample at `ts`. False if the series is absent entirely, or present
    /// but without a sample at that timestamp.
    fn key_has_sample_at(elements: &[RangeVectorElement], host_label: &str, ts: u64) -> bool {
        elements
            .iter()
            .find(|e| e.labels.labels.contains(&host_label.to_string()))
            .is_some_and(|e| e.samples.iter().any(|s| s.timestamp == ts))
    }

    /// Like `key_has_sample_at`, but matches a series only if ALL given label
    /// values are present -- needed once there's more than one grouping
    /// label (e.g. region + host), since a bare host value alone can't tell
    /// "host-b under region=eu" apart from "host-b under region=us".
    fn labels_have_sample_at(
        elements: &[RangeVectorElement],
        label_values: &[&str],
        ts: u64,
    ) -> bool {
        elements
            .iter()
            .find(|e| {
                label_values
                    .iter()
                    .all(|lv| e.labels.labels.contains(&lv.to_string()))
            })
            .is_some_and(|e| e.samples.iter().any(|s| s.timestamp == ts))
    }

    /// Checks every `(label_values, ts, expected_present, reason)` case
    /// against `elements` and reports ALL mismatches in one panic, instead of
    /// stopping at the first failing `assert!` -- each of these tests makes
    /// several independent claims (about different keys/timestamps), and a
    /// fix attempt that gets some right and some wrong is much easier to
    /// debug when every divergence is visible at once.
    fn assert_all_at(elements: &[RangeVectorElement], cases: &[(&[&str], u64, bool, &str)]) {
        let mismatches: Vec<String> = cases
            .iter()
            .filter_map(|(labels, ts, expected, reason)| {
                let actual = labels_have_sample_at(elements, labels, *ts);
                (actual != *expected).then(|| {
                    format!(
                        "{labels:?}@{ts}: expected present={expected}, got {actual} -- {reason}"
                    )
                })
            })
            .collect();
        assert!(
            mismatches.is_empty(),
            "diverged from expectations at:\n{}",
            mismatches.join("\n")
        );
    }

    /// One tumbling-window bucket: (bucket end timestamp ms, label values, accumulator).
    type TimeSeriesData = Vec<(u64, Option<Vec<String>>, Box<dyn AggregateCore>)>;

    /// Dual-population counterpart to
    /// range_query_arithmetic_tests::create_range_engine_two_metrics:
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
        create_range_engine_dual_input_with_windows(
            metric,
            value_agg_type,
            key_agg_type,
            grouping_labels,
            aggregated_labels,
            value_data,
            keys_data,
            promql_query,
            WINDOW_MS,
            WINDOW_MS,
        )
    }

    /// Same as `create_range_engine_dual_input`, but lets the value and key
    /// aggregations use different bucket widths — the value aggregation's
    /// `tumbling_window_ms` must not be assumed to also be the key
    /// aggregation's bucket width when scanning the keys `bucket_map` (#583).
    #[allow(clippy::too_many_arguments)]
    fn create_range_engine_dual_input_with_windows(
        metric: &str,
        value_agg_type: AggregationType,
        key_agg_type: AggregationType,
        grouping_labels: Vec<&str>,
        aggregated_labels: Vec<&str>,
        value_data: TimeSeriesData,
        keys_data: TimeSeriesData,
        promql_query: &str,
        value_window_ms: u64,
        key_window_ms: u64,
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
                window_size_ms: value_window_ms,
                slide_interval_ms: value_window_ms,
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
                window_size_ms: key_window_ms,
                slide_interval_ms: key_window_ms,
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

        for (agg_id, window_ms, data) in [
            (1u64, value_window_ms, value_data),
            (2u64, key_window_ms, keys_data),
        ] {
            for (timestamp, label_values_opt, acc) in data {
                let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
                let output = PrecomputedOutput::new(timestamp - window_ms, timestamp, key, agg_id);
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

    /// Same dual-population shape as `create_range_engine_dual_input_with_windows`,
    /// but the KEY aggregation is a Sliding window with
    /// `key_slide_interval_ms < key_window_size_ms` (#600). Real Sliding
    /// buckets are persisted on the slide_interval_ms grid, not the
    /// window_size_ms grid (`precompute_engine/window_manager.rs`), and the
    /// value side uses the same Sliding grid for these tests.
    #[allow(clippy::too_many_arguments)]
    fn create_range_engine_dual_input_sliding_keys(
        metric: &str,
        value_agg_type: AggregationType,
        key_agg_type: AggregationType,
        grouping_labels: Vec<&str>,
        aggregated_labels: Vec<&str>,
        value_data: TimeSeriesData,
        keys_data: TimeSeriesData,
        promql_query: &str,
        value_window_ms: u64,
        key_window_size_ms: u64,
        key_slide_interval_ms: u64,
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
                window_size_ms: value_window_ms,
                slide_interval_ms: key_slide_interval_ms,
                window_type: WindowType::Sliding,
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
                window_size_ms: key_window_size_ms,
                slide_interval_ms: key_slide_interval_ms,
                window_type: WindowType::Sliding,
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

        for (agg_id, bucket_span_ms, data) in [
            (1u64, value_window_ms, value_data),
            // Keys buckets are window_size_ms wide, same as values -- real
            // Sliding data (any aggregation type) reaches the store already
            // pre-merged by worker.rs::merge_panes_for_window, one entry per
            // window_size_ms-wide window on the slide_interval_ms grid.
            (2u64, key_window_size_ms, keys_data),
        ] {
            for (timestamp, label_values_opt, acc) in data {
                let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
                let output =
                    PrecomputedOutput::new(timestamp - bucket_span_ms, timestamp, key, agg_id);
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
    async fn range_query_sliding_keys_bucket_found_on_slide_interval_grid() {
        // #600: the keys-side sum_window must step by the KEY aggregation's
        // own slide_interval_ms, not its window_size_ms. Key aggregation:
        // window_size_ms=2000, slide_interval_ms=1000 (Sliding) -- real
        // buckets land on the 1000ms grid (start=3000), which isn't on the
        // 2000ms window_size_ms grid ({0, 2000, 4000, ...}) at all. A scan
        // that steps by window_size_ms never visits t=3000 and silently
        // drops host-a's key.
        //
        // Where the numbers come from:
        // - key_window_size_ms=2000, key_slide_interval_ms=1000 are the
        //   scenario's fixed inputs (the Sliding shape #600 is about).
        // - The bucket must be genuinely window_size_ms wide (2000) -- real
        //   Sliding data reaches the store already pre-merged by
        //   worker.rs::merge_panes_for_window, one window_size_ms-wide entry
        //   per window, never a raw slide-width pane -- while its *start*
        //   must be off the window_size_ms grid ({0,2000,4000,...}) but on
        //   the slide_interval_ms grid ({0,1000,2000,...}). The smallest
        //   such start is 1000, but 3000 is used instead because a bucket
        //   starting at 1000 with width 2000 ends at 3000, which also works
        //   -- 3000 was simply picked without checking whether 1000 (with a
        //   smaller timestamp/query shift) would have worked equally well.
        // - keys_data/value_data timestamp=5000 makes the factory (which
        //   computes each bucket as [timestamp - width, timestamp)) place
        //   the keys bucket at exactly [3000,5000).
        // - The query (5.0s-5.5s, step=1.0s) produces one output step at
        //   t=5000, whose keys lookback window is
        //   [5000 - key_window_size_ms, 5000) = [3000,5000) -- lining up
        //   exactly with the inserted bucket.
        // - value_data is also placed at timestamp=5000 (Sliding, 2000ms
        //   wide -> bucket [3000,5000)) purely so the CountMinSketch value
        //   side resolves at the same t=5000 step; it's unrelated to #600.
        let mut keys_add = SetAggregatorAccumulator::new();
        keys_add.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });

        let engine = create_range_engine_dual_input_sliding_keys(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::SetAggregator,
            vec![],
            vec!["host", "event"],
            vec![(
                5000,
                None,
                Box::new(CountMinSketchAccumulator::new(2, 3)) as Box<dyn AggregateCore>,
            )],
            // Keys bucket spans [3000, 5000) -- window_size_ms=2000 wide (a
            // real pre-merged window, matching worker.rs's output shape),
            // starting at 3000: on the slide_interval_ms=1000 grid, but not
            // on the window_size_ms=2000 grid ({0, 2000, 4000, ...}).
            vec![(5000, None, Box::new(keys_add) as Box<dyn AggregateCore>)],
            "sum by (host, event) (count_over_time(event_frequency[2s]))",
            2000, // value_window_ms, same Sliding grid as SetAggregator
            2000, // key_window_size_ms
            1000, // key_slide_interval_ms
        );

        let query = "sum by (host, event) (count_over_time(event_frequency[2s]))";
        let result = engine.handle_range_query_promql(query.to_string(), 5.0, 5.5, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);

        assert!(
            key_has_sample_at(&elements, "host-a", 5000),
            "BUG #600: host-a's keys delta bucket (start=3000, on the \
             slide_interval_ms=1000 grid but not the window_size_ms=2000 \
             grid) was not found -- the keys-side sum_window is stepping \
             by window_size_ms instead of slide_interval_ms"
        );
    }

    /// #608's keys-side counterpart: SetAggregator is a real Sliding-capable
    /// keys aggregation (unlike DeltaSetAggregator, restricted to Tumbling
    /// by #606), so the keys-side per-step composition has the same
    /// double-count risk as the value side. Three keys buckets, each
    /// window_size_ms(=2000)-wide and already fully merged (matching
    /// worker.rs's real output shape), starting 1000ms (slide_interval_ms)
    /// apart, each with a DISTINCT key so a leaked neighbor is directly
    /// observable rather than masked by set-union idempotence:
    ///   bucket [1000,3000) -> {host-a,evt-1}
    ///   bucket [2000,4000) -> {host-a,evt-2}
    ///   bucket [3000,5000) -> {host-a,evt-3}
    /// Step 3000: lookback window [1000,3000) -> exactly bucket [1000,3000)
    ///   -> only evt-1. A scan-and-sum over every grid position in
    ///   [1000,3000) would also visit t=2000 and wrongly pull in evt-2 (a
    ///   window that only starts becoming valid data at t=4000).
    /// Step 4000: lookback window [2000,4000) -> exactly bucket [2000,4000)
    ///   -> only evt-2 (evt-1 must have rolled off, evt-3 must not leak in).
    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_sliding_keys_no_double_count_across_steps() {
        let mut keys_1 = SetAggregatorAccumulator::new();
        keys_1.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });
        let mut keys_2 = SetAggregatorAccumulator::new();
        keys_2.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-2".to_string()],
        });
        let mut keys_3 = SetAggregatorAccumulator::new();
        keys_3.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-3".to_string()],
        });

        let engine = create_range_engine_dual_input_sliding_keys(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::SetAggregator,
            vec![],
            vec!["host", "event"],
            vec![
                (
                    3000,
                    None,
                    Box::new(CountMinSketchAccumulator::new(2, 3)) as Box<dyn AggregateCore>,
                ),
                (
                    4000,
                    None,
                    Box::new(CountMinSketchAccumulator::new(2, 3)) as Box<dyn AggregateCore>,
                ),
            ],
            vec![
                (3000, None, Box::new(keys_1) as Box<dyn AggregateCore>),
                (4000, None, Box::new(keys_2) as Box<dyn AggregateCore>),
                (5000, None, Box::new(keys_3) as Box<dyn AggregateCore>),
            ],
            "sum by (host, event) (count_over_time(event_frequency[2s]))",
            2000, // value_window_ms, same Sliding grid as SetAggregator
            2000, // key_window_size_ms
            1000, // key_slide_interval_ms
        );

        let query = "sum by (host, event) (count_over_time(event_frequency[2s]))";
        let result = engine.handle_range_query_promql(query.to_string(), 3.0, 4.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);

        assert!(
            labels_have_sample_at(&elements, &["host-a", "evt-1"], 3000),
            "step 3000 must include evt-1 (its own window)"
        );
        assert!(
            !labels_have_sample_at(&elements, &["host-a", "evt-2"], 3000),
            "#608: step 3000 must NOT include evt-2 -- that key only becomes valid \
             at t=4000; a scan-and-sum over [1000,3000) wrongly visits evt-2's grid \
             position too and leaks it in one step early"
        );
        assert!(
            labels_have_sample_at(&elements, &["host-a", "evt-2"], 4000),
            "step 4000 must include evt-2 (its own window)"
        );
        assert!(
            !labels_have_sample_at(&elements, &["host-a", "evt-1"], 4000),
            "step 4000 must NOT still include evt-1 -- it rolled off"
        );
        assert!(
            !labels_have_sample_at(&elements, &["host-a", "evt-3"], 4000),
            "#608: step 4000 must NOT include evt-3 -- that key only becomes valid \
             at t=5000; a scan-and-sum over [2000,4000) wrongly visits evt-3's grid \
             position too and leaks it in one step early"
        );
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
    async fn range_query_dual_population_self_keyed_value_still_uses_keys_query() {
        // Real, tested capability-matching config (sql.rs): CountMinSketchWithHeap
        // (self-keyed) as the VALUE aggregation, paired with a separate
        // DeltaSetAggregator KEYS aggregation. collect_results_separate_keys
        // (instant path) never consults the value accumulator's own
        // get_keys() -- it always expands via the keys aggregation. A range
        // pipeline that calls merged.get_keys() unconditionally on the merged
        // VALUE accumulator would incorrectly let the heap's own internal
        // top-k keys override the keys_query's expansion.
        //
        // Extended (not just the original single-timestamp check) to also
        // exercise #583's per-step keys resolution in the SAME test: host-b
        // is added to the keys aggregation only at t=2000, so a correct
        // implementation must show it absent at t=1000 and present at
        // t=2000 -- while the self-keyed heap's own top-k keys (host-x,
        // host-y, present in EVERY value bucket) must never appear at
        // either step. The two mechanisms are provably orthogonal (dual-
        // population never touches the value accumulator's own get_keys()
        // at all), but this pins both invariants holding simultaneously in
        // one test rather than trusting that orthogonality alone.
        let mut heap_1 = CountMinSketchWithHeapAccumulator::new(3, 1024, 32);
        let mut heap_2 = CountMinSketchWithHeapAccumulator::new(3, 1024, 32);
        // Heap's own top-k keys are deliberately disjoint from the keys_query's
        // keys, at every step, so a wrong implementation is caught by
        // key-set mismatch, not just a wrong value.
        for heap in [&mut heap_1, &mut heap_2] {
            heap.inner.update("host-x;evt-x", 30.0);
            heap.inner.update("host-y;evt-y", 20.0);
        }

        let mut keys_1000 = DeltaSetAggregatorAccumulator::new();
        keys_1000.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });
        let mut keys_2000 = DeltaSetAggregatorAccumulator::new();
        keys_2000.add_key(KeyByLabelValues {
            labels: vec!["host-b".to_string(), "evt-1".to_string()],
        });

        let engine = create_range_engine_dual_input(
            "event_frequency",
            AggregationType::CountMinSketchWithHeap,
            AggregationType::DeltaSetAggregator,
            vec![],
            vec!["host", "event"],
            vec![
                (1000, None, Box::new(heap_1) as Box<dyn AggregateCore>),
                (2000, None, Box::new(heap_2) as Box<dyn AggregateCore>),
            ],
            vec![
                (1000, None, Box::new(keys_1000) as Box<dyn AggregateCore>),
                (2000, None, Box::new(keys_2000) as Box<dyn AggregateCore>),
            ],
            "count(event_frequency) by (host, event)",
        );

        let query = "count(event_frequency) by (host, event)";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);

        assert_all_at(
            &elements,
            &[
                (
                    &["host-a"],
                    1000,
                    true,
                    "host-a is the keys_query's key from the start",
                ),
                (
                    &["host-b"],
                    1000,
                    false,
                    "host-b's key delta only appears at t=2000",
                ),
                (
                    &["host-x"],
                    1000,
                    false,
                    "the self-keyed heap's own top-k keys must never override the \
                     keys_query's expansion for a dual-population group",
                ),
                (
                    &["host-y"],
                    1000,
                    false,
                    "the self-keyed heap's own top-k keys must never override the \
                     keys_query's expansion for a dual-population group",
                ),
                (
                    &["host-a"],
                    2000,
                    true,
                    "host-a persists (DeltaSetAggregator never removes it)",
                ),
                (&["host-b"], 2000, true, "host-b was added by t=2000"),
                (
                    &["host-x"],
                    2000,
                    false,
                    "the self-keyed heap's own top-k keys must never override the \
                     keys_query's expansion for a dual-population group",
                ),
                (
                    &["host-y"],
                    2000,
                    false,
                    "the self-keyed heap's own top-k keys must never override the \
                     keys_query's expansion for a dual-population group",
                ),
            ],
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
            1_000, // slide_interval_ms
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
            1_000, // slide_interval_ms
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
        // unchanged now that the per-step merge logic correctly honors
        // WindowType::Sliding for range queries. Mirrors
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
            1_000, // slide_interval_ms
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
    async fn range_query_sliding_window_slide_lt_size_merges_bucket_off_window_size_grid() {
        // #600: value-side counterpart of
        // range_query_sliding_keys_bucket_found_on_slide_interval_grid.
        // window_size_ms=2000, but create_engine_multi_timestamp_with_window
        // fixes the bucket span at slide_interval_ms=1000, so the two
        // buckets below land at start=0 and start=1000 -- only one of which
        // is on the window_size_ms=2000 grid ({0, 2000, ...}). A sum_window
        // that steps by window_size_ms instead of slide_interval_ms never
        // visits start=1000 and silently drops that bucket from the merge.
        let data = vec![
            (
                1000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
            ),
            (
                2000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(5.0)) as Box<dyn AggregateCore>,
            ),
        ];
        let query = "sum_over_time(http_requests[2s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            2000, // window_size_ms
            1000, // slide_interval_ms
            WindowType::Sliding,
        );

        let result = engine.handle_range_query_promql(query.to_string(), 2.0, 2.5, 2.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);
        assert_eq!(elements.len(), 1, "expected one series for host-a");
        assert!(
            (elements[0].samples[0].value - 15.0).abs() < 1e-9,
            "BUG #600: expected both buckets (10.0 + 5.0 = 15.0) merged, got {} \
             -- tumbling_window_ms is stepping by window_size_ms=2000 instead \
             of slide_interval_ms=1000, so the bucket at start=1000 is dropped",
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

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_dual_population_key_appearing_midrange_has_no_phantom_earlier_sample() {
        // Issue #583: execute_range_query_pipeline fetches/merges keys_query
        // once (anchored at the range's end) and reuses that single snapshot
        // for every output step. host-b's DeltaSetAggregator "added" delta
        // only appears in the bucket at t=2000, so the correct per-step key
        // set at t=1000 must not include host-b yet — but the current
        // single-snapshot merge folds host-b's later add into the whole
        // range, giving it a phantom sample at t=1000 before it existed.
        let cms_1 = CountMinSketchAccumulator::new(2, 3);
        let cms_2 = CountMinSketchAccumulator::new(2, 3);
        let mut keys_1 = DeltaSetAggregatorAccumulator::new();
        keys_1.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });
        let mut keys_2 = DeltaSetAggregatorAccumulator::new();
        keys_2.add_key(KeyByLabelValues {
            labels: vec!["host-b".to_string(), "evt-1".to_string()],
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

        // DeltaSetAggregator accumulates: once added, a key stays in the
        // reconstructed set at every later step too (no removal here).
        // host-a is added at t=1000, so it should be present at both steps.
        // host-b is added only at t=2000, so it must be absent at t=1000
        // (not yet added) and present at t=2000.
        assert_all_at(
            &elements,
            &[
                (&["host-a"], 1000, true, "host-a was added at t=1000"),
                (
                    &["host-a"],
                    2000,
                    true,
                    "host-a was added at t=1000 and DeltaSetAggregator never removes it",
                ),
                (
                    &["host-b"],
                    1000,
                    false,
                    "host-b's key delta only appears at t=2000, so it must not have a \
                     phantom sample at t=1000 (before it existed)",
                ),
                (
                    &["host-b"],
                    2000,
                    true,
                    "host-b should have a sample at t=2000, once its key delta appears",
                ),
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_set_aggregator_earlier_key_not_silently_dropped() {
        // Issue #583 (second half): for SetAggregator ("latest window only"),
        // create_keys_query_params scopes keys_query to [end-window_size, end]
        // — a single instant-anchored window at the *range's* end, not each
        // step's own window. host-a's full-snapshot bucket only exists at
        // t=1000; with range end=2000 and window_size=1000, the keys fetch
        // window is [1000, 2000], which excludes host-a's bucket (0..1000)
        // entirely — its start (0) falls before the query window's start (1000).
        // Since the range loop iterates merged_keys (not all_data), host-a
        // never gets iterated at all — its whole series silently vanishes
        // from the output, even though it had a real sample at t=1000.
        let cms_1 = CountMinSketchAccumulator::new(2, 3);
        let cms_2 = CountMinSketchAccumulator::new(2, 3);
        let mut keys_1 = SetAggregatorAccumulator::new();
        keys_1.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });
        let mut keys_2 = SetAggregatorAccumulator::new();
        keys_2.add_key(KeyByLabelValues {
            labels: vec!["host-b".to_string(), "evt-1".to_string()],
        });

        let engine = create_range_engine_dual_input(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::SetAggregator,
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

        // SetAggregator is a full snapshot per window, not a delta: host-a is
        // the live set only for the window ending at t=1000, and is replaced
        // by host-b in the window ending at t=2000 — each step's own snapshot
        // is disjoint from the other's, unlike DeltaSetAggregator's accumulation.
        assert_all_at(
            &elements,
            &[
                (
                    &["host-a"],
                    1000,
                    true,
                    "host-a existed at t=1000 (its own window) but was dropped entirely \
                     from the range output because the final keys snapshot (scoped to \
                     the range's end window) no longer contains it",
                ),
                (
                    &["host-a"],
                    2000,
                    false,
                    "host-a's SetAggregator snapshot at t=2000 no longer contains it \
                     (host-b replaced it)",
                ),
                (
                    &["host-b"],
                    1000,
                    false,
                    "host-b doesn't appear until the window ending at t=2000",
                ),
                (
                    &["host-b"],
                    2000,
                    true,
                    "host-b is the live set for the window ending at t=2000",
                ),
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_binary_expr_arm_key_appearing_midrange_has_no_phantom_earlier_sample() {
        // Issue #583's "check other candidates" section names
        // build_arm_range_context specifically: handle_binary_expr_range_promql
        // builds each arm's RangeQueryExecutionContext via the same
        // finish_range_context/execute_range_query_pipeline used by the plain
        // range path, so the same single-snapshot keys bug should reproduce
        // through a binary expression's scalar arm. Same fixture as
        // range_query_dual_population_key_appearing_midrange_has_no_phantom_earlier_sample,
        // wrapped in `* 1` so it takes the detect_scalar_arm path in
        // handle_binary_expr_range_promql instead of the plain-query dispatch.
        let cms_1 = CountMinSketchAccumulator::new(2, 3);
        let cms_2 = CountMinSketchAccumulator::new(2, 3);
        let mut keys_1 = DeltaSetAggregatorAccumulator::new();
        keys_1.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });
        let mut keys_2 = DeltaSetAggregatorAccumulator::new();
        keys_2.add_key(KeyByLabelValues {
            labels: vec!["host-b".to_string(), "evt-1".to_string()],
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

        let query = "count(event_frequency) by (host, event) * 1";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);

        assert_all_at(
            &elements,
            &[
                (&["host-a"], 1000, true, "host-a was added at t=1000"),
                (
                    &["host-a"],
                    2000,
                    true,
                    "host-a was added at t=1000 and DeltaSetAggregator never removes it",
                ),
                (
                    &["host-b"],
                    1000,
                    false,
                    "host-b's key delta only appears at t=2000, so it must not have a \
                     phantom sample at t=1000 (before it existed) — through the binary \
                     expr arm path (build_arm_range_context) this time, not the plain range path",
                ),
                (
                    &["host-b"],
                    2000,
                    true,
                    "host-b should have a sample at t=2000, once its key delta appears",
                ),
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_delta_set_aggregator_oscillating_add_remove_across_five_windows() {
        // Generalizes the phantom-sample tests beyond a single add: host-a
        // toggles membership every window (add, remove, add, remove, add)
        // across 5 tumbling windows. A correct per-step fix must replay
        // deltas only up to *each step's own* end, so presence should
        // alternate present/absent/present/absent/present across the 5
        // output steps. The current single end-anchored snapshot instead
        // merges all 5 deltas into one net state (present, since the last
        // delta is an add) and reuses it for every step — so it would wrongly
        // show host-a present at every step, including the two "removed"
        // windows (t=2000, t=4000).
        let value_data: TimeSeriesData = (1..=5)
            .map(|i| {
                (
                    i * 1000,
                    None,
                    Box::new(CountMinSketchAccumulator::new(2, 3)) as Box<dyn AggregateCore>,
                )
            })
            .collect();

        let mut keys_add = DeltaSetAggregatorAccumulator::new();
        keys_add.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });
        let mut keys_remove = DeltaSetAggregatorAccumulator::new();
        keys_remove.remove_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });
        let keys_data: TimeSeriesData = vec![
            (
                1000,
                None,
                Box::new(keys_add.clone()) as Box<dyn AggregateCore>,
            ),
            (
                2000,
                None,
                Box::new(keys_remove.clone()) as Box<dyn AggregateCore>,
            ),
            (
                3000,
                None,
                Box::new(keys_add.clone()) as Box<dyn AggregateCore>,
            ),
            (
                4000,
                None,
                Box::new(keys_remove.clone()) as Box<dyn AggregateCore>,
            ),
            (5000, None, Box::new(keys_add) as Box<dyn AggregateCore>),
        ];

        let engine = create_range_engine_dual_input(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::DeltaSetAggregator,
            vec![],
            vec!["host", "event"],
            value_data,
            keys_data,
            "count(event_frequency) by (host, event)",
        );

        let query = "count(event_frequency) by (host, event)";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 5.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);

        let expected_present = [
            (1000, true),
            (2000, false),
            (3000, true),
            (4000, false),
            (5000, true),
        ];
        let mismatches: Vec<String> = expected_present
            .iter()
            .filter_map(|&(ts, expected)| {
                let actual = key_has_sample_at(&elements, "host-a", ts);
                (actual != expected).then(|| format!("t={ts}: expected {expected}, got {actual}"))
            })
            .collect();
        assert!(
            mismatches.is_empty(),
            "host-a's net membership diverged from the per-step expectation (deltas \
             replayed only up to each step's own end) at: {mismatches:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_binary_expr_arm_set_aggregator_earlier_key_not_silently_dropped() {
        // SetAggregator counterpart to
        // range_query_binary_expr_arm_key_appearing_midrange_has_no_phantom_earlier_sample:
        // closes the matrix by driving
        // range_query_set_aggregator_earlier_key_not_silently_dropped's fixture
        // through the binary-expr arm path (build_arm_range_context) too,
        // instead of only the plain range dispatch.
        let cms_1 = CountMinSketchAccumulator::new(2, 3);
        let cms_2 = CountMinSketchAccumulator::new(2, 3);
        let mut keys_1 = SetAggregatorAccumulator::new();
        keys_1.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });
        let mut keys_2 = SetAggregatorAccumulator::new();
        keys_2.add_key(KeyByLabelValues {
            labels: vec!["host-b".to_string(), "evt-1".to_string()],
        });

        let engine = create_range_engine_dual_input(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::SetAggregator,
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

        let query = "count(event_frequency) by (host, event) * 1";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);

        assert_all_at(
            &elements,
            &[
                (
                    &["host-a"],
                    1000,
                    true,
                    "host-a existed at t=1000 (its own window) but was dropped entirely \
                     from the range output because the final keys snapshot (scoped to \
                     the range's end window) no longer contains it — through the binary \
                     expr arm path (build_arm_range_context) this time",
                ),
                (
                    &["host-a"],
                    2000,
                    false,
                    "host-a's SetAggregator snapshot at t=2000 no longer contains it \
                     (host-b replaced it)",
                ),
                (
                    &["host-b"],
                    1000,
                    false,
                    "host-b doesn't appear until the window ending at t=2000",
                ),
                (
                    &["host-b"],
                    2000,
                    true,
                    "host-b is the live set for the window ending at t=2000",
                ),
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_delta_set_aggregator_key_change_on_middle_step_not_just_boundary() {
        // All prior #583 tests query exactly 2 output steps, where the key
        // change lands on one of the range's own boundaries (start or end).
        // A fix that special-cases the first/last iteration of the per-step
        // loop (rather than genuinely scoping every iteration) could pass
        // those while still getting an interior step wrong. This test spans
        // 3 output steps (1000, 2000, 3000) with the key added only on the
        // *middle* one, so t=1000's snapshot must differ from t=2000's and
        // t=3000's purely by virtue of being an interior loop iteration.
        let value_data: TimeSeriesData = vec![
            (
                1000,
                None,
                Box::new(CountMinSketchAccumulator::new(2, 3)) as Box<dyn AggregateCore>,
            ),
            (
                2000,
                None,
                Box::new(CountMinSketchAccumulator::new(2, 3)) as Box<dyn AggregateCore>,
            ),
            (
                3000,
                None,
                Box::new(CountMinSketchAccumulator::new(2, 3)) as Box<dyn AggregateCore>,
            ),
        ];

        let mut keys_2000 = DeltaSetAggregatorAccumulator::new();
        keys_2000.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });
        let keys_data: TimeSeriesData = vec![
            (
                1000,
                None,
                Box::new(DeltaSetAggregatorAccumulator::new()) as Box<dyn AggregateCore>,
            ),
            (2000, None, Box::new(keys_2000) as Box<dyn AggregateCore>),
            (
                3000,
                None,
                Box::new(DeltaSetAggregatorAccumulator::new()) as Box<dyn AggregateCore>,
            ),
        ];

        let engine = create_range_engine_dual_input(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::DeltaSetAggregator,
            vec![],
            vec!["host", "event"],
            value_data,
            keys_data,
            "count(event_frequency) by (host, event)",
        );

        let query = "count(event_frequency) by (host, event)";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 3.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);

        let expected_present = [(1000, false), (2000, true), (3000, true)];
        let mismatches: Vec<String> = expected_present
            .iter()
            .filter_map(|&(ts, expected)| {
                let actual = key_has_sample_at(&elements, "host-a", ts);
                (actual != expected).then(|| format!("t={ts}: expected {expected}, got {actual}"))
            })
            .collect();
        assert!(
            mismatches.is_empty(),
            "host-a's key is added only on the middle step (t=2000), so t=1000 must not \
             see it yet while t=2000 and t=3000 must — diverged at: {mismatches:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_delta_set_aggregator_key_bucket_width_differs_from_value_bucket_width() {
        // Issue #583's fix scans the keys bucket_map using a per-step
        // increment. That increment must come from the KEY aggregation's own
        // window_size_ms, not the VALUE aggregation's tumbling_window_ms — an
        // implementation that (incorrectly) reuses the value side's bucket
        // width to walk the keys bucket_map would silently skip every keys
        // bucket whose start isn't a multiple of that width. All other
        // #583 tests use the same window size (WINDOW_MS) for both
        // aggregations, so none of them would catch that mistake — this one
        // deliberately sets them apart: value buckets are 1000ms wide, key
        // (DeltaSetAggregator) buckets are 500ms wide, and both key delta
        // buckets are placed off the 1000ms grid (starts at 500 and 1500).
        let value_window_ms = 1000;
        let key_window_ms = 500;

        let value_data: TimeSeriesData = vec![
            (
                1000,
                None,
                Box::new(CountMinSketchAccumulator::new(2, 3)) as Box<dyn AggregateCore>,
            ),
            (
                2000,
                None,
                Box::new(CountMinSketchAccumulator::new(2, 3)) as Box<dyn AggregateCore>,
            ),
        ];

        // host-a's delta bucket: start=500, end=1000 (off the 1000ms grid).
        let mut keys_host_a = DeltaSetAggregatorAccumulator::new();
        keys_host_a.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });
        // host-b's delta bucket: start=1500, end=2000 (also off the grid).
        let mut keys_host_b = DeltaSetAggregatorAccumulator::new();
        keys_host_b.add_key(KeyByLabelValues {
            labels: vec!["host-b".to_string(), "evt-1".to_string()],
        });
        let keys_data: TimeSeriesData = vec![
            (1000, None, Box::new(keys_host_a) as Box<dyn AggregateCore>),
            (2000, None, Box::new(keys_host_b) as Box<dyn AggregateCore>),
        ];

        let engine = create_range_engine_dual_input_with_windows(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::DeltaSetAggregator,
            vec![],
            vec!["host", "event"],
            value_data,
            keys_data,
            "count(event_frequency) by (host, event)",
            value_window_ms,
            key_window_ms,
        );

        let query = "count(event_frequency) by (host, event)";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);

        // host-a's bucket (start=500) falls inside [0,1000), so it must be
        // visible at t=1000. host-b's bucket (start=1500) doesn't exist yet
        // at t=1000, but is inside [0,2000) by t=2000, and DeltaSetAggregator
        // accumulates, so host-a stays present at t=2000 too.
        assert_all_at(
            &elements,
            &[
                (
                    &["host-a"],
                    1000,
                    true,
                    "host-a's delta bucket starts at t=500, which is inside [0,1000) -- it \
                     must be found even though 500 isn't a multiple of the value \
                     aggregation's 1000ms bucket width",
                ),
                (
                    &["host-a"],
                    2000,
                    true,
                    "host-a was added by t=1000 and DeltaSetAggregator never removes it",
                ),
                (
                    &["host-b"],
                    1000,
                    false,
                    "host-b's delta bucket starts at t=1500, which is not yet inside [0,1000)",
                ),
                (
                    &["host-b"],
                    2000,
                    true,
                    "host-b's delta bucket starts at t=1500, which is inside [0,2000) -- it \
                     must be found even though 1500 isn't a multiple of the value \
                     aggregation's 1000ms bucket width",
                ),
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_dual_population_per_step_key_change_does_not_leak_across_groups() {
        // All prior #583 tests use grouping_labels: vec![] -- a single
        // implicit group (group_key = None) -- so "the per-step key set" and
        // "the per-step key set for THIS group" are the same computation,
        // and a bug that pools state across groups would be invisible.
        // Here the value aggregation genuinely groups by `region`, giving
        // two independent groups (region=us, region=eu), each with its own
        // keys aggregation state. region=us gains host-b mid-range (added at
        // t=2000); region=eu's host set (host-x) never changes. A fix whose
        // per-step keys merge isn't correctly scoped per group_key -- e.g.
        // one that merges all groups' DeltaSetAggregator buckets together
        // before re-splitting by group -- would leak host-b into region=eu's
        // output at t=2000, since nothing about merge_with prevents merging
        // two different regions' delta-sets together.
        let cms_us_1 = CountMinSketchAccumulator::new(2, 3);
        let cms_us_2 = CountMinSketchAccumulator::new(2, 3);
        let cms_eu_1 = CountMinSketchAccumulator::new(2, 3);
        let cms_eu_2 = CountMinSketchAccumulator::new(2, 3);

        let mut keys_us_1000 = DeltaSetAggregatorAccumulator::new();
        keys_us_1000.add_key(KeyByLabelValues {
            labels: vec!["us".to_string(), "host-a".to_string(), "evt-1".to_string()],
        });
        let mut keys_us_2000 = DeltaSetAggregatorAccumulator::new();
        keys_us_2000.add_key(KeyByLabelValues {
            labels: vec!["us".to_string(), "host-b".to_string(), "evt-1".to_string()],
        });
        let mut keys_eu_1000 = DeltaSetAggregatorAccumulator::new();
        keys_eu_1000.add_key(KeyByLabelValues {
            labels: vec!["eu".to_string(), "host-x".to_string(), "evt-1".to_string()],
        });

        let engine = create_range_engine_dual_input(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::DeltaSetAggregator,
            vec!["region"],
            vec!["host", "event"],
            vec![
                (
                    1000,
                    Some(vec!["us".to_string()]),
                    Box::new(cms_us_1) as Box<dyn AggregateCore>,
                ),
                (
                    2000,
                    Some(vec!["us".to_string()]),
                    Box::new(cms_us_2) as Box<dyn AggregateCore>,
                ),
                (
                    1000,
                    Some(vec!["eu".to_string()]),
                    Box::new(cms_eu_1) as Box<dyn AggregateCore>,
                ),
                (
                    2000,
                    Some(vec!["eu".to_string()]),
                    Box::new(cms_eu_2) as Box<dyn AggregateCore>,
                ),
            ],
            vec![
                (
                    1000,
                    Some(vec!["us".to_string()]),
                    Box::new(keys_us_1000) as Box<dyn AggregateCore>,
                ),
                (
                    2000,
                    Some(vec!["us".to_string()]),
                    Box::new(keys_us_2000) as Box<dyn AggregateCore>,
                ),
                (
                    1000,
                    Some(vec!["eu".to_string()]),
                    Box::new(keys_eu_1000) as Box<dyn AggregateCore>,
                ),
            ],
            "count(event_frequency) by (region, host, event)",
        );

        let query = "count(event_frequency) by (region, host, event)";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);

        assert_all_at(
            &elements,
            &[
                (
                    &["us", "host-a"],
                    1000,
                    true,
                    "region=us has host-a from the start",
                ),
                (
                    &["us", "host-b"],
                    1000,
                    false,
                    "region=us's host-b is only added at t=2000",
                ),
                (
                    &["us", "host-b"],
                    2000,
                    true,
                    "region=us's host-b was added by t=2000",
                ),
                (
                    &["eu", "host-x"],
                    1000,
                    true,
                    "region=eu has host-x from the start",
                ),
                (
                    &["eu", "host-x"],
                    2000,
                    true,
                    "region=eu's host-x is unaffected by region=us's mid-range change",
                ),
                (
                    &["eu", "host-b"],
                    2000,
                    false,
                    "region=us's host-b addition must not leak into region=eu's output -- \
                     each group's per-step key expansion must be scoped to its own \
                     group_key, not pooled across groups",
                ),
                (
                    &["us", "host-x"],
                    1000,
                    false,
                    "region=eu's host-x must not leak into region=us's output either",
                ),
                (
                    &["us", "host-x"],
                    2000,
                    false,
                    "region=eu's host-x must not leak into region=us's output either",
                ),
            ],
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_dual_population_simultaneous_cross_group_adds_stay_isolated() {
        // Sharper variant of ...does_not_leak_across_groups: there, region=eu
        // never changes, so a failure could plausibly be misread as plain
        // Bug 1 (single-snapshot-per-group) rather than cross-group bleed.
        // Here BOTH groups add a DIFFERENT new host at the SAME timestamp
        // (t=2000), so any implementation that pools keys across group_keys
        // before re-splitting would show up as *both* regions gaining *both*
        // new hosts -- a decisive, unambiguous signature distinct from
        // Bug 1's "one group's own key is phantom-early" symptom.
        let cms_us_1 = CountMinSketchAccumulator::new(2, 3);
        let cms_us_2 = CountMinSketchAccumulator::new(2, 3);
        let cms_eu_1 = CountMinSketchAccumulator::new(2, 3);
        let cms_eu_2 = CountMinSketchAccumulator::new(2, 3);

        let mut keys_us_1000 = DeltaSetAggregatorAccumulator::new();
        keys_us_1000.add_key(KeyByLabelValues {
            labels: vec!["us".to_string(), "host-a".to_string(), "evt-1".to_string()],
        });
        let mut keys_us_2000 = DeltaSetAggregatorAccumulator::new();
        keys_us_2000.add_key(KeyByLabelValues {
            labels: vec!["us".to_string(), "host-b".to_string(), "evt-1".to_string()],
        });
        let mut keys_eu_1000 = DeltaSetAggregatorAccumulator::new();
        keys_eu_1000.add_key(KeyByLabelValues {
            labels: vec!["eu".to_string(), "host-p".to_string(), "evt-1".to_string()],
        });
        let mut keys_eu_2000 = DeltaSetAggregatorAccumulator::new();
        keys_eu_2000.add_key(KeyByLabelValues {
            labels: vec!["eu".to_string(), "host-q".to_string(), "evt-1".to_string()],
        });

        let engine = create_range_engine_dual_input(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::DeltaSetAggregator,
            vec!["region"],
            vec!["host", "event"],
            vec![
                (
                    1000,
                    Some(vec!["us".to_string()]),
                    Box::new(cms_us_1) as Box<dyn AggregateCore>,
                ),
                (
                    2000,
                    Some(vec!["us".to_string()]),
                    Box::new(cms_us_2) as Box<dyn AggregateCore>,
                ),
                (
                    1000,
                    Some(vec!["eu".to_string()]),
                    Box::new(cms_eu_1) as Box<dyn AggregateCore>,
                ),
                (
                    2000,
                    Some(vec!["eu".to_string()]),
                    Box::new(cms_eu_2) as Box<dyn AggregateCore>,
                ),
            ],
            vec![
                (
                    1000,
                    Some(vec!["us".to_string()]),
                    Box::new(keys_us_1000) as Box<dyn AggregateCore>,
                ),
                (
                    2000,
                    Some(vec!["us".to_string()]),
                    Box::new(keys_us_2000) as Box<dyn AggregateCore>,
                ),
                (
                    1000,
                    Some(vec!["eu".to_string()]),
                    Box::new(keys_eu_1000) as Box<dyn AggregateCore>,
                ),
                (
                    2000,
                    Some(vec!["eu".to_string()]),
                    Box::new(keys_eu_2000) as Box<dyn AggregateCore>,
                ),
            ],
            "count(event_frequency) by (region, host, event)",
        );

        let query = "count(event_frequency) by (region, host, event)";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);

        let expected = [
            (["us", "host-a"], 1000, true),
            (["us", "host-b"], 1000, false),
            (["us", "host-a"], 2000, true),
            (["us", "host-b"], 2000, true),
            (["eu", "host-p"], 1000, true),
            (["eu", "host-q"], 1000, false),
            (["eu", "host-p"], 2000, true),
            (["eu", "host-q"], 2000, true),
            // Cross-group: neither region's host should ever appear under the other.
            (["us", "host-p"], 1000, false),
            (["us", "host-q"], 1000, false),
            (["us", "host-p"], 2000, false),
            (["us", "host-q"], 2000, false),
            (["eu", "host-a"], 1000, false),
            (["eu", "host-b"], 1000, false),
            (["eu", "host-a"], 2000, false),
            (["eu", "host-b"], 2000, false),
        ];
        let mismatches: Vec<String> = expected
            .iter()
            .filter_map(|(labels, ts, expected_present)| {
                let actual = labels_have_sample_at(&elements, labels, *ts);
                (actual != *expected_present)
                    .then(|| format!("{labels:?}@{ts}: expected {expected_present}, got {actual}"))
            })
            .collect();
        assert!(
            mismatches.is_empty(),
            "simultaneous cross-group adds must stay isolated per group_key -- \
             diverged at: {mismatches:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_dual_population_group_with_no_value_data_is_skipped_not_fatal() {
        // Today, execute_range_query_pipeline hard-fails the ENTIRE range
        // query if any group resolved from merged_keys has no matching entry
        // in all_data: `all_data.get(group_key).ok_or_else(|| "No value for
        // key")?`. region=orphan has keys data (a real DeltaSetAggregator
        // delta) but NEVER has any value/CMS data at all -- under today's
        // code this poisons the WHOLE query, so even region=normal's
        // perfectly good data disappears.
        //
        // Per #583's design discussion: this should become non-fatal --
        // skip the orphaned group (with a loud warning, not asserted here
        // since this is a unit test, not a log-capture test) and still
        // return the rest of the query's results. This test pins the
        // EXPECTED (fixed) behavior, so it's RED today: today the whole
        // `handle_range_query_promql` call returns None and the `.expect()`
        // below panics before any of the assertions run.
        let cms_normal_1 = CountMinSketchAccumulator::new(2, 3);
        let cms_normal_2 = CountMinSketchAccumulator::new(2, 3);

        let mut keys_normal = DeltaSetAggregatorAccumulator::new();
        keys_normal.add_key(KeyByLabelValues {
            labels: vec![
                "normal".to_string(),
                "host-a".to_string(),
                "evt-1".to_string(),
            ],
        });
        let mut keys_orphan = DeltaSetAggregatorAccumulator::new();
        keys_orphan.add_key(KeyByLabelValues {
            labels: vec![
                "orphan".to_string(),
                "host-z".to_string(),
                "evt-1".to_string(),
            ],
        });

        let engine = create_range_engine_dual_input(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::DeltaSetAggregator,
            vec!["region"],
            vec!["host", "event"],
            vec![
                (
                    1000,
                    Some(vec!["normal".to_string()]),
                    Box::new(cms_normal_1) as Box<dyn AggregateCore>,
                ),
                (
                    2000,
                    Some(vec!["normal".to_string()]),
                    Box::new(cms_normal_2) as Box<dyn AggregateCore>,
                ),
                // Deliberately NO value data for region=orphan, at any timestamp.
            ],
            vec![
                (
                    1000,
                    Some(vec!["normal".to_string()]),
                    Box::new(keys_normal) as Box<dyn AggregateCore>,
                ),
                (
                    1000,
                    Some(vec!["orphan".to_string()]),
                    Box::new(keys_orphan) as Box<dyn AggregateCore>,
                ),
            ],
            "count(event_frequency) by (region, host, event)",
        );

        let query = "count(event_frequency) by (region, host, event)";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect(
            "range query should succeed by skipping the value-less region=orphan group, \
             not fail the entire query because of it",
        );
        let elements = matrix_values(qr);

        assert_all_at(
            &elements,
            &[
                (
                    &["normal", "host-a"],
                    1000,
                    true,
                    "region=normal has real value data and should be unaffected by \
                     region=orphan having none",
                ),
                (
                    &["normal", "host-a"],
                    2000,
                    true,
                    "region=normal's host-a persists (DeltaSetAggregator never removes it)",
                ),
            ],
        );
        assert!(
            !elements
                .iter()
                .any(|e| e.labels.labels.contains(&"orphan".to_string())),
            "region=orphan has keys data but no value data anywhere -- it must be \
             silently skipped, not appear as an (empty or otherwise) series"
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
    async fn range_query_set_aggregator_merges_multiple_buckets_within_one_window() {
        // Every other SetAggregator test uses window_size_ms == bucket
        // width, so each window only ever contains exactly ONE bucket --
        // DeltaSetAggregator gets multi-bucket-per-window coverage "for
        // free" via its always-widened [0,t] window, but SetAggregator's
        // bounded sliding window has never been exercised with more than one
        // bucket in play. Mirrors the values-side sliding-collision fixture
        // (range_query_sliding_window_merges_both_buckets: two buckets
        // sharing the same (start,end)) but for keys: two SetAggregator
        // buckets both at t=1000 (start=0,end=1000) must UNION into
        // {host-a, host-b}, exactly like the existing bucket_map handling
        // for values (`bucket_map.entry(*start).or_default().push(..)`,
        // #567/#570) -- a fix that naively does `.insert()` instead of
        // accumulating for the keys side would silently drop one of them.
        //
        // t=2000 adds a third key (host-c) via a single non-colliding
        // bucket; since SetAggregator is "latest window only" (not
        // cumulative), it replaces the t=1000 pair entirely -- this also
        // keeps the test genuinely RED against today's code (Bug 2: the
        // single end-anchored keys fetch window [1000,2000] excludes the
        // t=1000 collision pair's bucket, whose start=0 falls outside it).
        let cms_1 = CountMinSketchAccumulator::new(2, 3);
        let cms_2 = CountMinSketchAccumulator::new(2, 3);

        let mut keys_1_a = SetAggregatorAccumulator::new();
        keys_1_a.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });
        let mut keys_1_b = SetAggregatorAccumulator::new();
        keys_1_b.add_key(KeyByLabelValues {
            labels: vec!["host-b".to_string(), "evt-1".to_string()],
        });
        let mut keys_2 = SetAggregatorAccumulator::new();
        keys_2.add_key(KeyByLabelValues {
            labels: vec!["host-c".to_string(), "evt-1".to_string()],
        });

        let engine = create_range_engine_dual_input(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::SetAggregator,
            vec![],
            vec!["host", "event"],
            vec![
                (1000, None, Box::new(cms_1) as Box<dyn AggregateCore>),
                (2000, None, Box::new(cms_2) as Box<dyn AggregateCore>),
            ],
            vec![
                // Both buckets share (start=0, end=1000): a genuine collision.
                (1000, None, Box::new(keys_1_a) as Box<dyn AggregateCore>),
                (1000, None, Box::new(keys_1_b) as Box<dyn AggregateCore>),
                (2000, None, Box::new(keys_2) as Box<dyn AggregateCore>),
            ],
            "count(event_frequency) by (host, event)",
        );

        let query = "count(event_frequency) by (host, event)";
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);

        assert_all_at(
            &elements,
            &[
                (
                    &["host-a"],
                    1000,
                    true,
                    "host-a's bucket collides with host-b's at (start=0,end=1000) -- \
                     both must be merged (unioned), not one dropped in favor of the other",
                ),
                (
                    &["host-b"],
                    1000,
                    true,
                    "host-b's bucket collides with host-a's at (start=0,end=1000) -- \
                     both must be merged (unioned), not one dropped in favor of the other",
                ),
                (
                    &["host-c"],
                    1000,
                    false,
                    "host-c's bucket doesn't exist until t=2000",
                ),
                (
                    &["host-a"],
                    2000,
                    false,
                    "SetAggregator is latest-window-only: host-c's window replaces \
                     host-a/host-b's, it doesn't accumulate alongside them",
                ),
                (
                    &["host-b"],
                    2000,
                    false,
                    "SetAggregator is latest-window-only: host-c's window replaces \
                     host-a/host-b's, it doesn't accumulate alongside them",
                ),
                (
                    &["host-c"],
                    2000,
                    true,
                    "host-c is the live set for the window ending at t=2000",
                ),
            ],
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
