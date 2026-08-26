//! Adversarial tests for the store-query contract, written directly from the
//! spec in `stores/traits.rs` (`Store::query_precomputed_output` /
//! `query_precomputed_output_exact`) and the window/bucket alignment model in
//! `precompute_engine/window_manager.rs`.
//!
//! These are NOT shaped around any particular implementation. They target the
//! class of bug that shows up when a tolerant range scan
//! (`query_precomputed_output`-shaped: "give me every stored window whose
//! `[window_start, window_end)` fits inside `[start, end]`") gets reimplemented
//! as a series of strict per-window exact lookups
//! (`query_precomputed_output_exact`-shaped): stepping by the wrong grid
//! (`window_size_ms` instead of `slide_interval_ms`) silently drops sliding
//! windows off that grid (issue #600), a naive "enumerate every grid position"
//! substitute for a sparse tolerant scan can go pathological on a wide range
//! with sparse data (the `DeltaSetAggregator` keys query, which spans
//! `[0, end_timestamp]`), and boundary/gap handling can silently differ from
//! the tolerant scan's containment semantics.
//!
//! Two levels of test live here:
//!
//! - **Store-level** (`SimpleMapStore` directly): pin the `Store` trait
//!   contract itself -- empty-range edge cases, boundary containment,
//!   overlapping sliding windows, and sealed/mutable epoch spans. These
//!   exercise the store's own (unchanged) `query_precomputed_output`, so they
//!   document the ground truth the rest of the pipeline must reproduce.
//! - **Engine-level** (`SimpleEngine` + PromQL): exercise the actual query
//!   pipeline (`handle_query_promql` / `handle_range_query_promql`), which is
//!   where a tolerant-scan-to-exact-lookups rewrite would actually live. Each
//!   expected result below is computed by hand from `window_manager.rs`'s
//!   grid math (`window_start_for`, `window_starts_containing`,
//!   `panes_for_window`), not from reading the implementation under test.

#[cfg(test)]
mod tests {
    use crate::data_model::{
        AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
        KeyByLabelValues, LockStrategy, PrecomputedOutput, PromQLSchema, QueryConfig,
        QueryLanguage, SchemaConfig, SerializableToSink, StreamingConfig, WindowType,
    };
    use crate::engines::query_result::{QueryResult, RangeVectorElement};
    use crate::engines::simple_engine::SimpleEngine;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::precompute_operators::{CountMinSketchAccumulator, DeltaSetAggregatorAccumulator};
    use crate::stores::simple_map_store::SimpleMapStore;
    use crate::stores::{Store, TimestampedBucketsMap};
    use crate::tests::test_utilities::engine_factories::create_engine_multi_timestamp_with_window;
    use crate::AggregateCore;
    use promql_utilities::data_model::KeyByLabelNames;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    // ════════════════════════════════════════════════════════════════════
    // ── Store-level tests: pin the `Store` trait contract directly ──────
    // ════════════════════════════════════════════════════════════════════

    fn make_agg_config(
        agg_id: u64,
        aggregation_type: AggregationType,
        window_size_ms: u64,
        slide_interval_ms: u64,
        window_type: WindowType,
        num_aggregates_to_retain: Option<u64>,
    ) -> AggregationConfig {
        AggregationConfig {
            aggregation_id: agg_id,
            aggregation_type,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::empty(),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size_ms,
            slide_interval_ms,
            window_type,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: "cpu_usage".to_string(),
            num_aggregates_to_retain,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        }
    }

    fn make_store(configs: Vec<(u64, AggregationConfig)>, policy: CleanupPolicy) -> SimpleMapStore {
        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs: configs.into_iter().collect(),
        });
        SimpleMapStore::new_with_strategy(streaming_config, policy, LockStrategy::PerKey)
    }

    fn sum_entry(
        agg_id: u64,
        start: u64,
        end: u64,
        value: f64,
    ) -> (PrecomputedOutput, Box<dyn AggregateCore>) {
        (
            PrecomputedOutput::new(start, end, None, agg_id),
            Box::new(SumAccumulator::with_sum(value)),
        )
    }

    fn keyed_delta_entry(
        agg_id: u64,
        start: u64,
        end: u64,
        key_label: &str,
    ) -> (PrecomputedOutput, Box<dyn AggregateCore>) {
        let mut acc = DeltaSetAggregatorAccumulator::new();
        acc.add_key(KeyByLabelValues {
            labels: vec![key_label.to_string()],
        });
        (
            PrecomputedOutput::new(start, end, None, agg_id),
            Box::new(acc),
        )
    }

    fn total_bucket_count(result: &TimestampedBucketsMap) -> usize {
        result.values().map(|v| v.len()).sum()
    }

    fn timestamps_for_none_key(result: &TimestampedBucketsMap) -> Vec<(u64, u64)> {
        let mut ts: Vec<(u64, u64)> = result
            .get(&None)
            .map(|buckets| buckets.iter().map(|(range, _)| *range).collect())
            .unwrap_or_default();
        ts.sort_unstable();
        ts
    }

    /// Spec: `window_start >= start && window_start <= end && window_end <= end`.
    /// With `start == end == window_start`, the third condition
    /// (`window_end <= end`) fails for any window with positive width -- a
    /// degenerate `[t, t)` query range must never partial-match a real window
    /// just because its start lines up.
    #[test]
    fn store_query_start_equals_end_excludes_any_real_window() {
        let store = make_store(
            vec![(
                1,
                make_agg_config(
                    1,
                    AggregationType::Sum,
                    1000,
                    1000,
                    WindowType::Tumbling,
                    None,
                ),
            )],
            CleanupPolicy::NoCleanup,
        );
        let (out, acc) = sum_entry(1, 5_000, 6_000, 7.0);
        store.insert_precomputed_output(out, acc).unwrap();

        let result = store
            .query_precomputed_output("cpu_usage", 1, 5_000, 5_000)
            .unwrap();
        assert!(
            result.is_empty(),
            "degenerate [t, t) range must not partial-match the window starting at t: {:?}",
            timestamps_for_none_key(&result)
        );
    }

    /// Spec conditions are `window_start >= start && window_start <= end &&
    /// window_end <= end`. With `start > end`, no real window can satisfy
    /// `window_start <= end` while also being `>= start` unless start<=end --
    /// this pins that an inverted range never accidentally matches via a
    /// buggy min/max swap or an unchecked subtraction.
    #[test]
    fn store_query_start_greater_than_end_returns_empty_even_with_window_start_between() {
        let store = make_store(
            vec![(
                1,
                make_agg_config(
                    1,
                    AggregationType::Sum,
                    1000,
                    1000,
                    WindowType::Tumbling,
                    None,
                ),
            )],
            CleanupPolicy::NoCleanup,
        );
        // window_start = 5_000 sits numerically "between" end=3_000 and
        // start=7_000, which is exactly the kind of case a start/end swap bug
        // would wrongly match.
        let (out, acc) = sum_entry(1, 5_000, 6_000, 7.0);
        store.insert_precomputed_output(out, acc).unwrap();

        let result = store
            .query_precomputed_output("cpu_usage", 1, 7_000, 3_000)
            .unwrap();
        assert!(
            result.is_empty(),
            "start > end must return empty regardless of window placement: {:?}",
            timestamps_for_none_key(&result)
        );
    }

    /// A window whose `window_start` precedes the query's `start` must be
    /// excluded entirely (not partially matched) even though it overlaps the
    /// query range -- this is the "partial window at the start of the range"
    /// case from the spec.
    #[test]
    fn store_query_excludes_window_whose_start_precedes_query_start() {
        let store = make_store(
            vec![(
                1,
                make_agg_config(
                    1,
                    AggregationType::Sum,
                    1000,
                    1000,
                    WindowType::Tumbling,
                    None,
                ),
            )],
            CleanupPolicy::NoCleanup,
        );
        let (out1, acc1) = sum_entry(1, 1_000, 2_000, 11.0); // starts before query_start
        let (out2, acc2) = sum_entry(1, 2_000, 3_000, 22.0); // fully inside
        store.insert_precomputed_output(out1, acc1).unwrap();
        store.insert_precomputed_output(out2, acc2).unwrap();

        let result = store
            .query_precomputed_output("cpu_usage", 1, 1_500, 3_000)
            .unwrap();
        assert_eq!(
            timestamps_for_none_key(&result),
            vec![(2_000, 3_000)],
            "window [1000,2000) starts before query_start=1500 and must be excluded entirely, \
             not truncated/partially matched"
        );
    }

    /// Symmetric case: a window whose `window_end` exceeds the query's `end`
    /// must be excluded entirely -- the "partial window at the end of the
    /// range" case.
    #[test]
    fn store_query_excludes_window_whose_end_exceeds_query_end() {
        let store = make_store(
            vec![(
                1,
                make_agg_config(
                    1,
                    AggregationType::Sum,
                    1000,
                    1000,
                    WindowType::Tumbling,
                    None,
                ),
            )],
            CleanupPolicy::NoCleanup,
        );
        let (out1, acc1) = sum_entry(1, 3_000, 4_000, 33.0); // fully inside
        let (out2, acc2) = sum_entry(1, 4_000, 5_000, 44.0); // ends after query_end
        store.insert_precomputed_output(out1, acc1).unwrap();
        store.insert_precomputed_output(out2, acc2).unwrap();

        let result = store
            .query_precomputed_output("cpu_usage", 1, 3_000, 4_500)
            .unwrap();
        assert_eq!(
            timestamps_for_none_key(&result),
            vec![(3_000, 4_000)],
            "window [4000,5000) ends after query_end=4500 and must be excluded entirely, \
             not truncated/partially matched"
        );
    }

    /// Three overlapping sliding windows (slide=1000, size=3000) inserted
    /// directly -- a tolerant range scan covering all three must return all
    /// three as DISTINCT entries, not merge/dedup/drop any of them, even
    /// though their ranges heavily overlap.
    #[test]
    fn store_query_overlapping_sliding_windows_all_returned_distinctly() {
        let store = make_store(
            vec![(
                1,
                make_agg_config(
                    1,
                    AggregationType::Sum,
                    3000,
                    1000,
                    WindowType::Sliding,
                    None,
                ),
            )],
            CleanupPolicy::NoCleanup,
        );
        let (o1, a1) = sum_entry(1, 0, 3_000, 1.0);
        let (o2, a2) = sum_entry(1, 1_000, 4_000, 2.0);
        let (o3, a3) = sum_entry(1, 2_000, 5_000, 3.0);
        store.insert_precomputed_output(o1, a1).unwrap();
        store.insert_precomputed_output(o2, a2).unwrap();
        store.insert_precomputed_output(o3, a3).unwrap();

        let result = store
            .query_precomputed_output("cpu_usage", 1, 0, 5_000)
            .unwrap();
        assert_eq!(
            total_bucket_count(&result),
            3,
            "all three overlapping windows must be returned distinctly"
        );
        assert_eq!(
            timestamps_for_none_key(&result),
            vec![(0, 3_000), (1_000, 4_000), (2_000, 5_000)],
        );

        // Confirm values weren't merged/overwritten across the overlapping
        // entries -- each bucket's accumulator must carry its own distinct
        // value (1.0, 2.0, 3.0), not the same one returned three times (which
        // a merge/dedup bug would produce as identical serialized JSON).
        let buckets = result.get(&None).unwrap();
        let json_for = |range: (u64, u64)| -> serde_json::Value {
            buckets
                .iter()
                .find(|(r, _)| *r == range)
                .unwrap_or_else(|| panic!("missing bucket {range:?}"))
                .1
                .serialize_to_json()
        };
        let j1 = json_for((0, 3_000));
        let j2 = json_for((1_000, 4_000));
        let j3 = json_for((2_000, 5_000));
        assert_eq!(j1, SumAccumulator::with_sum(1.0).serialize_to_json());
        assert_eq!(j2, SumAccumulator::with_sum(2.0).serialize_to_json());
        assert_eq!(j3, SumAccumulator::with_sum(3.0).serialize_to_json());
    }

    /// Range query spanning both sealed epochs and the still-open (mutable)
    /// current epoch, in one call. Mirrors
    /// `store_correctness_tests::test_exact_query_correct_after_epoch_rotation`
    /// but drives the tolerant range scan (`query_precomputed_output`)
    /// instead of the exact lookup, and checks correctness across the whole
    /// retained span in a single query rather than probing one window at a
    /// time.
    ///
    /// capacity=2, 10 inserts => retention_limit=8, evicting windows 0 and 1
    /// (oldest sealed epoch). Windows 2..10 remain, spread across multiple
    /// sealed epochs plus the current (newest, still "mutable") epoch.
    #[test]
    fn store_query_range_spans_sealed_and_mutable_epochs() {
        let store = make_store(
            vec![(
                1,
                make_agg_config(
                    1,
                    AggregationType::Sum,
                    60_000,
                    60_000,
                    WindowType::Tumbling,
                    Some(2),
                ),
            )],
            CleanupPolicy::CircularBuffer,
        );
        let n = 10u64;
        for i in 0..n {
            let (out, acc) = sum_entry(1, i * 60_000, (i + 1) * 60_000, i as f64);
            store.insert_precomputed_output(out, acc).unwrap();
        }

        let result = store
            .query_precomputed_output("cpu_usage", 1, 0, n * 60_000)
            .unwrap();
        let ts = timestamps_for_none_key(&result);
        let expected: Vec<(u64, u64)> = (2..n).map(|i| (i * 60_000, (i + 1) * 60_000)).collect();
        assert_eq!(
            ts, expected,
            "range query spanning sealed + mutable epochs must return exactly windows 2..10, \
             with windows 0,1 evicted"
        );

        let buckets = result.get(&None).unwrap();
        for i in 2..n {
            let range = (i * 60_000, (i + 1) * 60_000);
            let acc = &buckets.iter().find(|(r, _)| *r == range).unwrap().1;
            let expected_json = SumAccumulator::with_sum(i as f64).serialize_to_json();
            assert_eq!(
                acc.serialize_to_json(),
                expected_json,
                "window {i} must return its own value across the sealed/mutable epoch boundary"
            );
        }
    }

    /// Baseline/regression pin for `SimpleMapStore` itself (not the engine):
    /// a `DeltaSetAggregator`-shaped keys query spans `[0, end_timestamp]`
    /// per `create_keys_query_params`. With real data confined to a narrow
    /// sliver near a huge `end_timestamp`, the underlying store's tolerant
    /// scan must both (a) return the correct sparse set and (b) complete
    /// quickly -- it must not be a function of the nominal range width. This
    /// documents that the store layer itself was never the risk; the risk is
    /// in whatever composes calls to it (see the engine-level counterpart
    /// below).
    #[test]
    fn store_query_delta_set_wide_range_from_zero_stays_fast_and_correct() {
        let store = make_store(
            vec![(
                1,
                make_agg_config(
                    1,
                    AggregationType::DeltaSetAggregator,
                    1_000,
                    1_000,
                    WindowType::Tumbling,
                    None,
                ),
            )],
            CleanupPolicy::NoCleanup,
        );
        // 1e11 ms (~3170 years) nominal end, with 5 real windows clustered in
        // a 5000ms sliver near it -- deliberately chosen so that a hypothetical
        // "enumerate every grid position from 0" scan (1e8 iterations at
        // window_size_ms=1000 steps) would be obviously, measurably slow,
        // while a real sparse-store scan is not.
        let base: u64 = 100_000_000_000;
        for (i, label) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            let start = base + i as u64 * 1_000;
            let (out, acc) = keyed_delta_entry(1, start, start + 1_000, label);
            store.insert_precomputed_output(out, acc).unwrap();
        }

        let query_start = Instant::now();
        let result = store
            .query_precomputed_output("cpu_usage", 1, 0, base + 5_000)
            .unwrap();
        let elapsed = query_start.elapsed();

        assert_eq!(
            total_bucket_count(&result),
            5,
            "must return exactly the 5 real windows, sparse, not padded"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "store-level wide-range keys query took {:?}, expected near-instant \
             (proportional to real data, not nominal range width)",
            elapsed
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // ── Engine-level tests: drive the actual PromQL query pipeline ──────
    // ════════════════════════════════════════════════════════════════════

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

    /// #600-style off-grid bug, but with THREE panes spanning a wider
    /// window_size (3000ms, slide fixed at 1000ms by the factory) and THREE
    /// output steps, none of whose windows sit on the window_size_ms grid
    /// ({0, 3000, 6000, ...}) except the very first. A `bucket_step_ms` that
    /// steps by `window_size_ms` instead of `slide_interval_ms` would miss
    /// panes at 1000/2000/4000, silently under-summing every step after the
    /// first -- and a broken step could also double count if it re-visits a
    /// pane from an earlier step's window. Each step's expected sum is
    /// distinct, so both failure modes (drop vs double-count) are caught.
    ///
    /// Panes (bucket = [end-1000, end)): 1000->1, 2000->10, 3000->100,
    /// 4000->1000, 5000->10000.
    /// Step 3000: window [0,3000)    -> panes {0,1000,2000}    -> 1+10+100=111
    /// Step 4000: window [1000,4000) -> panes {1000,2000,3000} -> 10+100+1000=1110
    /// Step 5000: window [2000,5000) -> panes {2000,3000,4000} -> 100+1000+10000=11100
    ///
    /// Currently fails: pins the pre-existing bug tracked in
    /// https://github.com/ProjectASAP/ASAPQuery/issues/608 (range queries
    /// over Sliding-window aggregations use the overlap-scan fetch, not
    /// exact-window fetch, then get merged downstream as if Tumbling).
    /// Un-ignore once #608 lands.
    #[ignore = "known bug, see #608"]
    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_sliding_multi_step_off_grid_windows_no_double_count_or_drop() {
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
            1_000, // slide_interval_ms
            WindowType::Sliding,
        );

        let result = engine.handle_range_query_promql(query.to_string(), 3.0, 5.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);
        let samples = host_a_samples(&elements);

        assert_eq!(
            samples,
            vec![(3_000, 111.0), (4_000, 1_110.0), (5_000, 11_100.0)],
            "BUG-#600-class: off-window_size_ms-grid panes must be found at every step, \
             each step summing exactly its own 3 panes, not dropped or double-counted"
        );
    }

    /// Instant-query counterpart of the range test above: the off-grid pane
    /// scan must work identically through the single-timestamp instant path
    /// (`handle_query_promql`), not just the multi-step range path.
    /// query_time=4.0s -> window [1000,4000) -> panes {1000,2000,3000} = 1110.
    #[tokio::test(flavor = "multi_thread")]
    async fn instant_query_sliding_off_grid_window_merges_correctly() {
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
        ];
        let query = "sum_over_time(cpu_load[3s])";
        let engine = create_engine_multi_timestamp_with_window(
            "cpu_load",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            3_000,
            1_000, // slide_interval_ms
            WindowType::Sliding,
        );

        let (_, qr) = engine
            .handle_query_promql(query.to_string(), 4.0)
            .expect("instant query failed");
        let values = vector_values(qr);
        assert_eq!(values.len(), 1, "expected exactly one series for host-a");
        assert_close(
            values[0].1,
            1_110.0,
            "instant query at t=4.0 must merge panes {1000,2000,3000} = 10+100+1000",
        );
    }

    /// Generalizes the off-grid case to check the window genuinely SHIFTS
    /// with each step (window_size=2000, slide=1000) -- i.e. no step
    /// accidentally reuses a neighboring step's pane set (which would show up
    /// as either a dropped pane at the new edge or a stale pane retained past
    /// where it should have rolled off).
    /// Panes: 1000->1, 2000->10, 3000->100, 4000->1000.
    /// Step 2000: window [0,2000)    -> panes {0,1000}    -> 1+10=11
    /// Step 3000: window [1000,3000) -> panes {1000,2000} -> 10+100=110
    /// Step 4000: window [2000,4000) -> panes {2000,3000} -> 100+1000=1100
    ///
    /// Currently fails: same pre-existing bug as the test above, tracked in
    /// https://github.com/ProjectASAP/ASAPQuery/issues/608. Un-ignore once
    /// #608 lands.
    #[ignore = "known bug, see #608"]
    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_sliding_overlap_shifts_correctly_across_steps() {
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
        ];
        let query = "sum_over_time(cpu_load[2s])";
        let engine = create_engine_multi_timestamp_with_window(
            "cpu_load",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            2_000,
            1_000, // slide_interval_ms
            WindowType::Sliding,
        );

        let result = engine.handle_range_query_promql(query.to_string(), 2.0, 4.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);
        let samples = host_a_samples(&elements);

        assert_eq!(
            samples,
            vec![(2_000, 11.0), (3_000, 110.0), (4_000, 1_100.0)],
            "sliding window must shift correctly at every step, without leaking a stale \
             pane forward or dropping the newly-included one"
        );
    }

    /// Tumbling range query spanning 5 windows with a GAP in the middle
    /// (window ending at 3000 was never inserted). Per the tolerant-scan
    /// contract, results are sparse -- the output series must have samples at
    /// every step EXCEPT the missing one, not fail the whole query and not
    /// silently pad the gap with a zero or a stale value.
    #[tokio::test(flavor = "multi_thread")]
    async fn range_query_tumbling_multi_window_gap_returns_sparse_not_padded() {
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
            // gap: no window ending at 3000
            (
                4_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(4.0)) as Box<dyn AggregateCore>,
            ),
            (
                5_000,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(5.0)) as Box<dyn AggregateCore>,
            ),
        ];
        let query = "sum_over_time(cpu_load[1s])";
        let engine = create_engine_multi_timestamp_with_window(
            "cpu_load",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            1_000,
            1_000, // slide_interval_ms == window_size_ms for Tumbling
            WindowType::Tumbling,
        );

        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 5.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);
        let samples = host_a_samples(&elements);

        assert_eq!(
            samples,
            vec![(1_000, 1.0), (2_000, 2.0), (4_000, 4.0), (5_000, 5.0)],
            "gap at t=3000 (never inserted) must be absent from the output -- sparse, \
             not padded with a fabricated/zero sample, and must not fail the whole query"
        );
    }

    /// Custom dual-population engine builder parametrized by a caller-chosen
    /// base timestamp (unlike `create_engine_dual_input`, which hardcodes
    /// 1_000_000). Needed to place real data far from t=0 while exercising
    /// `create_keys_query_params`'s `DeltaSetAggregator` span of
    /// `[0, end_timestamp]` against a genuinely huge nominal range.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::type_complexity)]
    fn make_dual_engine_at_timestamp(
        metric: &str,
        value_agg_type: AggregationType,
        key_agg_type: AggregationType,
        grouping_labels: Vec<&str>,
        aggregated_labels: Vec<&str>,
        timestamp: u64,
        value_data: Vec<(Option<Vec<String>>, Box<dyn AggregateCore>)>,
        keys_data: Vec<(Option<Vec<String>>, Box<dyn AggregateCore>)>,
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
                window_size_ms: 1_000,
                slide_interval_ms: 1_000,
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
                window_size_ms: 1_000,
                slide_interval_ms: 1_000,
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

        for (label_values_opt, acc) in value_data {
            let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
            let output = PrecomputedOutput::new(timestamp - 1_000, timestamp, key, 1);
            store.insert_precomputed_output(output, acc).unwrap();
        }
        for (label_values_opt, acc) in keys_data {
            let key = label_values_opt.map(|labels| KeyByLabelValues { labels });
            let output = PrecomputedOutput::new(timestamp - 1_000, timestamp, key, 2);
            store.insert_precomputed_output(output, acc).unwrap();
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
            1000,
            QueryLanguage::promql,
        )
    }

    /// Engine-level counterpart to `store_query_delta_set_wide_range_from_zero_stays_fast_and_correct`:
    /// this is the test that actually stresses `create_keys_query_params`'s
    /// `[0, end_timestamp]` span for `DeltaSetAggregator` through the real
    /// query pipeline. Real data lives in a single 1000ms window ~1e11 ms
    /// (~3170 years) after t=0; the nominal keys-query range is therefore
    /// ~1e11 ms wide with only that one sliver of real data in it. A
    /// tolerant-scan-shaped composition must still resolve this correctly and
    /// quickly; an "enumerate every grid position from 0" substitute (1e8
    /// iterations at 1000ms steps) would not be quick.
    #[tokio::test(flavor = "multi_thread")]
    async fn instant_query_delta_set_keys_wide_range_from_zero_completes_quickly_and_correctly() {
        let base_ts: u64 = 100_000_000_000; // 1e11 ms
        let cms = CountMinSketchAccumulator::new(2, 3);
        let mut keys = DeltaSetAggregatorAccumulator::new();
        keys.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });

        let engine = make_dual_engine_at_timestamp(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::DeltaSetAggregator,
            vec![],
            vec!["host", "event"],
            base_ts,
            vec![(None, Box::new(cms) as Box<dyn AggregateCore>)],
            vec![(None, Box::new(keys) as Box<dyn AggregateCore>)],
            "count(event_frequency) by (host, event)",
        );

        let query = "count(event_frequency) by (host, event)";
        let query_time_sec = base_ts as f64 / 1000.0;

        let call_start = Instant::now();
        let result = engine.handle_query_promql(query.to_string(), query_time_sec);
        let elapsed = call_start.elapsed();

        let (_, qr) = result.expect("query failed to resolve real data near a huge timestamp");
        let values = vector_values(qr);
        assert!(
            values
                .iter()
                .any(|(labels, _)| labels.contains(&"host-a".to_string())),
            "expected host-a's key to be resolved via the DeltaSetAggregator keys query \
             spanning [0, {base_ts}], got {values:?}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "instant query with a DeltaSetAggregator keys span of [0, {base_ts}] (~1e11ms) \
             took {elapsed:?} -- a per-grid-position enumeration substitute for the tolerant \
             scan would be expected to blow well past this on a range this wide"
        );
    }
}
