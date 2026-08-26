//! Generic multi-step range/instant equivalence oracle (issue #590).
//!
//! Every hand-crafted regression test elsewhere in this directory
//! (`native_range_query_tests.rs`, `exact_window_grid_adversarial_tests.rs`,
//! ...) pins one specific bug shape at one specific pair of timestamps. What
//! none of them do -- and what #590 explicitly calls out as the test that
//! would have caught #584 "without needing a one-off regression case per
//! bug" -- is the generic property:
//!
//! > For a stable key set over `[start, end]`, `range(start, end, step)`
//! > equals `{instant(t) for t in steps}`.
//!
//! i.e. run ONE range query covering N steps, run N separate instant
//! queries (one per step timestamp), and assert the range query's
//! per-timestamp series exactly match the corresponding instant query's
//! result set -- for EVERY step, not just the first/last.
//!
//! This is deliberately the easy/happy-path case: the key set is stable
//! (never appears/disappears mid-range). Changing key sets are already
//! covered by the hand-crafted tests referenced above -- duplicating that
//! here would just be more of the same, not a new kind of coverage.
//!
//! Covers {Tumbling, Sliding} x {single-population, dual-population} x
//! {Sum, Count}, at least 4 steps each:
//! - `range_multistep_tumbling_single_population_sum`
//! - `range_multistep_sliding_single_population_sum`
//! - `range_multistep_tumbling_dual_population_count`
//! - `range_multistep_sliding_dual_population_count`
//!
//! If one of these ever fails, that's a real divergence between
//! `execute_range_query_pipeline` and the instant path
//! (`execute_and_merge_store_queries`) -- per the task's ground rules, do
//! NOT weaken the assertion and do NOT patch the engine here; mark the test
//! `#[ignore]` with the failure documented and flag it for separate fix-up.

#[cfg(test)]
mod tests {
    use crate::data_model::{
        AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
        KeyByLabelValues, PrecomputedOutput, PromQLSchema, QueryConfig, QueryLanguage,
        SchemaConfig, StreamingConfig, WindowType,
    };
    use crate::engines::query_result::QueryResult;
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

    /// One slide-interval-wide pane: (pane end timestamp ms, grouping label
    /// values, accumulator). Mirrors the `TimeSeriesData` shape used
    /// throughout `native_range_query_tests.rs` / `engine_factories.rs`.
    type TimeSeriesData = Vec<(u64, Option<Vec<String>>, Box<dyn AggregateCore>)>;

    /// A single output step's result, as an order-independent, sorted-by-
    /// label snapshot of (label values, value) pairs.
    type StepSnapshot = Vec<(Vec<String>, f64)>;

    fn snapshot_vector(qr: QueryResult) -> StepSnapshot {
        match qr {
            QueryResult::Vector(iv) => {
                let mut v: StepSnapshot = iv
                    .values
                    .into_iter()
                    .map(|e| (e.labels.labels, e.value))
                    .collect();
                v.sort_by(|a, b| a.0.cmp(&b.0));
                v
            }
            QueryResult::Matrix(_) => panic!("expected instant vector result, got a matrix"),
        }
    }

    fn snapshot_matrix_at(qr: &QueryResult, ts: u64) -> StepSnapshot {
        match qr {
            QueryResult::Matrix(m) => {
                let mut v: StepSnapshot = m
                    .values
                    .iter()
                    .filter_map(|e| {
                        e.samples
                            .iter()
                            .find(|s| s.timestamp == ts)
                            .map(|s| (e.labels.labels.clone(), s.value))
                    })
                    .collect();
                v.sort_by(|a, b| a.0.cmp(&b.0));
                v
            }
            QueryResult::Vector(_) => panic!("expected matrix (range vector) result, got a vector"),
        }
    }

    fn labels_only(snapshot: &StepSnapshot) -> Vec<&Vec<String>> {
        snapshot.iter().map(|(labels, _)| labels).collect()
    }

    /// The core oracle (#590): run `range(start, end, step)` once, run
    /// `instant(t)` once per `step_timestamps_ms`, and assert the range
    /// query's per-timestamp slice exactly matches (same series, same
    /// values within float tolerance) the corresponding instant query's
    /// result -- at EVERY step, collecting every mismatch before panicking
    /// (house style: see `assert_all_at` in `native_range_query_tests.rs`)
    /// rather than stopping at the first divergence.
    fn assert_range_equals_instants(
        engine: &SimpleEngine,
        query: &str,
        start: f64,
        end: f64,
        step: f64,
        step_timestamps_ms: &[u64],
    ) {
        let (_, range_qr) = engine
            .handle_range_query_promql(query.to_string(), start, end, step)
            .unwrap_or_else(|| {
                panic!("range query `{query}` ({start}..{end} step {step}) returned None")
            });

        let mut mismatches: Vec<String> = Vec::new();
        for &ts in step_timestamps_ms {
            let query_time_s = ts as f64 / 1000.0;
            let (_, instant_qr) = engine
                .handle_query_promql(query.to_string(), query_time_s)
                .unwrap_or_else(|| {
                    panic!("instant query `{query}` at t={query_time_s} returned None")
                });

            let instant_snapshot = snapshot_vector(instant_qr);
            let range_snapshot = snapshot_matrix_at(&range_qr, ts);

            if instant_snapshot.len() != range_snapshot.len() {
                mismatches.push(format!(
                    "t={ts}: series count differs -- range has {} series {:?}, instant has {} series {:?}",
                    range_snapshot.len(),
                    labels_only(&range_snapshot),
                    instant_snapshot.len(),
                    labels_only(&instant_snapshot),
                ));
                continue;
            }

            for ((r_labels, r_val), (i_labels, i_val)) in
                range_snapshot.iter().zip(instant_snapshot.iter())
            {
                if r_labels != i_labels {
                    mismatches.push(format!(
                        "t={ts}: label mismatch -- range={r_labels:?}, instant={i_labels:?}"
                    ));
                } else if (r_val - i_val).abs() >= 1e-6 {
                    mismatches.push(format!(
                        "t={ts}: {r_labels:?} value mismatch -- range={r_val}, instant={i_val}"
                    ));
                }
            }
        }

        assert!(
            mismatches.is_empty(),
            "range({start},{end},{step}) query `{query}` diverged from per-step instant query \
             at:\n{}",
            mismatches.join("\n")
        );
    }

    /// Inserts `data` (slide_interval_ms-wide panes, keyed by pane end
    /// timestamp) into `store` as pre-merged, window_size_ms-wide buckets
    /// for aggregation `agg_id` -- mirroring
    /// `worker.rs::merge_panes_for_window`, the same technique
    /// `create_engine_multi_timestamp_with_window` (`engine_factories.rs`)
    /// uses for a single aggregation. Generalized here (parameterized over
    /// `agg_id`) so it can be called once per aggregation -- value AND keys
    /// -- to build genuinely multi-step, multi-window dual-population
    /// engines, which none of the `pub` factories in `engine_factories.rs`
    /// support (`create_engine_dual_input` is Tumbling-only and inserts at
    /// a single fixed timestamp).
    #[allow(clippy::type_complexity)]
    fn insert_windowed_panes(
        store: &SimpleMapStore,
        agg_id: u64,
        window_size_ms: u64,
        slide_interval_ms: u64,
        data: TimeSeriesData,
    ) {
        let num_panes = window_size_ms / slide_interval_ms;

        let mut per_key: HashMap<Option<Vec<String>>, Vec<(u64, Box<dyn AggregateCore>)>> =
            HashMap::new();
        for (timestamp, label_values_opt, acc) in data {
            per_key
                .entry(label_values_opt)
                .or_default()
                .push((timestamp, acc));
        }

        for (label_values_opt, mut panes) in per_key {
            let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
            panes.sort_by_key(|(ts, _)| *ts);

            if num_panes <= 1 {
                for (ts, acc) in panes {
                    let output =
                        PrecomputedOutput::new(ts - window_size_ms, ts, key.clone(), agg_id);
                    store.insert_precomputed_output(output, acc).unwrap();
                }
                continue;
            }

            let pane_map: HashMap<u64, &Box<dyn AggregateCore>> =
                panes.iter().map(|(ts, acc)| (*ts, acc)).collect();
            let (min_ts, max_ts) = (panes[0].0, panes[panes.len() - 1].0);

            let mut window_start = min_ts.saturating_sub(window_size_ms);
            while window_start + window_size_ms <= max_ts {
                let pane_ends: Vec<u64> = (1..=num_panes)
                    .map(|i| window_start + i * slide_interval_ms)
                    .collect();
                if pane_ends.iter().all(|t| pane_map.contains_key(t)) {
                    let mut merged = pane_map[&pane_ends[0]].clone_boxed_core();
                    for t in &pane_ends[1..] {
                        merged = merged.merge_with(pane_map[t].as_ref()).unwrap();
                    }
                    let output = PrecomputedOutput::new(
                        window_start,
                        window_start + window_size_ms,
                        key.clone(),
                        agg_id,
                    );
                    store.insert_precomputed_output(output, merged).unwrap();
                }
                window_start += slide_interval_ms;
            }
        }
    }

    /// Dual-population engine builder (separate value/keys aggregations,
    /// `count(metric) by (...)`-shaped queries) with a configurable window
    /// (Tumbling or Sliding) and genuinely multiple output steps -- what
    /// `create_engine_dual_input` (single fixed timestamp, Tumbling only)
    /// doesn't support. Both the value and keys aggregations share the same
    /// window shape here: this test is deliberately the happy-path/stable-
    /// key-set case (#590), not the #600/#583-style mismatched-window-width
    /// edge cases already covered in `native_range_query_tests.rs`.
    #[allow(clippy::too_many_arguments)]
    fn create_dual_pop_engine_with_window(
        metric: &str,
        value_agg_type: AggregationType,
        key_agg_type: AggregationType,
        grouping_labels: Vec<&str>,
        aggregated_labels: Vec<&str>,
        value_data: TimeSeriesData,
        keys_data: TimeSeriesData,
        promql_query: &str,
        window_size_ms: u64,
        slide_interval_ms: u64,
        window_type: WindowType,
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
                window_size_ms,
                slide_interval_ms,
                window_type,
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
                window_size_ms,
                slide_interval_ms,
                window_type,
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

        insert_windowed_panes(&store, 1, window_size_ms, slide_interval_ms, value_data);
        insert_windowed_panes(&store, 2, window_size_ms, slide_interval_ms, keys_data);

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
            slide_interval_ms,
            QueryLanguage::promql,
        )
    }

    // ════════════════════════════════════════════════════════════════════
    // ── Tumbling x single-population (Sum) ──────────────────────────────
    // ════════════════════════════════════════════════════════════════════

    /// Stable 2-key set (host-a, host-b), 4 consecutive 1s Tumbling buckets.
    /// `sum(cpu_usage) by (host)` over range(1.0, 4.0, 1.0) must equal the
    /// 4 separate instant queries at t=1.0, 2.0, 3.0, 4.0.
    #[tokio::test(flavor = "multi_thread")]
    async fn range_multistep_tumbling_single_population_sum() {
        let data: TimeSeriesData = vec![
            (
                1000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)),
            ),
            (
                1000,
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(100.0)),
            ),
            (
                2000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(20.0)),
            ),
            (
                2000,
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(200.0)),
            ),
            (
                3000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(30.0)),
            ),
            (
                3000,
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(300.0)),
            ),
            (
                4000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(40.0)),
            ),
            (
                4000,
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(400.0)),
            ),
        ]
        .into_iter()
        .map(|(ts, labels, acc)| (ts, labels, acc as Box<dyn AggregateCore>))
        .collect();

        let query = "sum(cpu_usage) by (host)";
        let engine = create_engine_multi_timestamp_with_window(
            "cpu_usage",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            1000,
            1000,
            WindowType::Tumbling,
        );

        assert_range_equals_instants(&engine, query, 1.0, 4.0, 1.0, &[1000, 2000, 3000, 4000]);
    }

    // ════════════════════════════════════════════════════════════════════
    // ── Sliding x single-population (Sum) ───────────────────────────────
    // ════════════════════════════════════════════════════════════════════

    /// Stable 2-key set, window_size=2000ms/slide=1000ms Sliding. Panes at
    /// t=1000..5000 (5 panes, 2 per window) produce 4 fully-formed windows
    /// ending at 2000, 3000, 4000, 5000. `sum(cpu_load) by (host)` over
    /// range(2.0, 5.0, 1.0) must equal the 4 separate instant queries.
    #[tokio::test(flavor = "multi_thread")]
    async fn range_multistep_sliding_single_population_sum() {
        let host_a_panes = [1.0, 2.0, 3.0, 4.0, 5.0];
        let host_b_panes = [10.0, 20.0, 30.0, 40.0, 50.0];
        let mut data: TimeSeriesData = Vec::new();
        for (i, ts) in (1000..=5000).step_by(1000).enumerate() {
            data.push((
                ts,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(host_a_panes[i])) as Box<dyn AggregateCore>,
            ));
            data.push((
                ts,
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(host_b_panes[i])) as Box<dyn AggregateCore>,
            ));
        }

        let query = "sum(cpu_load) by (host)";
        let engine = create_engine_multi_timestamp_with_window(
            "cpu_load",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            2000,
            1000,
            WindowType::Sliding,
        );

        assert_range_equals_instants(&engine, query, 2.0, 5.0, 1.0, &[2000, 3000, 4000, 5000]);
    }

    // ════════════════════════════════════════════════════════════════════
    // ── Tumbling x dual-population (Count via DeltaSetAggregator keys) ──
    // ════════════════════════════════════════════════════════════════════

    /// Stable 2-key set ((host-a, evt-1), (host-b, evt-2)), 4 consecutive 1s
    /// Tumbling buckets, dual-population `count(event_frequency) by (host,
    /// event)` (value=CountMinSketch, keys=DeltaSetAggregator -- the same
    /// pairing `native_range_query_tests.rs`'s Tumbling dual-population
    /// tests use). range(1.0, 4.0, 1.0) must equal the 4 separate instant
    /// queries.
    #[tokio::test(flavor = "multi_thread")]
    async fn range_multistep_tumbling_dual_population_count() {
        let mut value_data: TimeSeriesData = Vec::new();
        let mut keys_data: TimeSeriesData = Vec::new();
        for ts in (1000..=4000).step_by(1000) {
            value_data.push((
                ts,
                None,
                Box::new(CountMinSketchAccumulator::new(2, 3)) as Box<dyn AggregateCore>,
            ));

            let mut keys = DeltaSetAggregatorAccumulator::new();
            keys.add_key(KeyByLabelValues {
                labels: vec!["host-a".to_string(), "evt-1".to_string()],
            });
            keys.add_key(KeyByLabelValues {
                labels: vec!["host-b".to_string(), "evt-2".to_string()],
            });
            keys_data.push((ts, None, Box::new(keys) as Box<dyn AggregateCore>));
        }

        let query = "count(event_frequency) by (host, event)";
        let engine = create_dual_pop_engine_with_window(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::DeltaSetAggregator,
            vec![],
            vec!["host", "event"],
            value_data,
            keys_data,
            query,
            1000,
            1000,
            WindowType::Tumbling,
        );

        assert_range_equals_instants(&engine, query, 1.0, 4.0, 1.0, &[1000, 2000, 3000, 4000]);
    }

    // ════════════════════════════════════════════════════════════════════
    // ── Sliding x dual-population (Count via SetAggregator keys) ────────
    // ════════════════════════════════════════════════════════════════════

    /// Stable 2-key set, window_size=2000ms/slide=1000ms Sliding for BOTH
    /// the value and keys aggregations. Keys aggregation uses SetAggregator
    /// (not DeltaSetAggregator, which #606 restricts to Tumbling --
    /// `native_range_query_tests.rs`'s Sliding-keys tests use the same
    /// substitution). Panes at t=1000..5000 produce 4 fully-formed windows
    /// ending at 2000, 3000, 4000, 5000, each containing both stable keys.
    /// range(2.0, 5.0, 1.0) must equal the 4 separate instant queries.
    #[tokio::test(flavor = "multi_thread")]
    async fn range_multistep_sliding_dual_population_count() {
        let mut value_data: TimeSeriesData = Vec::new();
        let mut keys_data: TimeSeriesData = Vec::new();
        for ts in (1000..=5000).step_by(1000) {
            value_data.push((
                ts,
                None,
                Box::new(CountMinSketchAccumulator::new(2, 3)) as Box<dyn AggregateCore>,
            ));

            let mut keys = SetAggregatorAccumulator::new();
            keys.add_key(KeyByLabelValues {
                labels: vec!["host-a".to_string(), "evt-1".to_string()],
            });
            keys.add_key(KeyByLabelValues {
                labels: vec!["host-b".to_string(), "evt-2".to_string()],
            });
            keys_data.push((ts, None, Box::new(keys) as Box<dyn AggregateCore>));
        }

        let query = "count(event_frequency) by (host, event)";
        let engine = create_dual_pop_engine_with_window(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::SetAggregator,
            vec![],
            vec!["host", "event"],
            value_data,
            keys_data,
            query,
            2000,
            1000,
            WindowType::Sliding,
        );

        assert_range_equals_instants(&engine, query, 2.0, 5.0, 1.0, &[2000, 3000, 4000, 5000]);
    }
}
