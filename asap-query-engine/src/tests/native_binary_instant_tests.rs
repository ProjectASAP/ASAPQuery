//! PromQL binary-expr instant query tests, native execution (issue #567).
//!
//! `handle_query_promql`'s binary-arithmetic path (`handle_binary_expr_promql`
//! → `evaluate_binary_arm` → `combine_vector_vector`/`combine_scalar`) runs
//! natively as of #567's Stage 3 cutover — no more DataFusion involved.
//! These tests were originally written to compare the native path against
//! the (now-removed) DataFusion path before the cutover landed; they now
//! assert the native path's results directly.

#[cfg(test)]
mod tests {
    use crate::data_model::{AggregationType, KeyByLabelValues, WindowType};
    use crate::engines::query_result::QueryResult;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::precompute_operators::{
        CountMinSketchAccumulator, DeltaSetAggregatorAccumulator, MultipleSumAccumulator,
    };
    use crate::tests::test_utilities::engine_factories::{
        create_engine_dual_input, create_engine_multi_timestamp_with_window,
        create_engine_single_pop, create_engine_three_metrics, create_engine_two_metrics,
    };
    use crate::AggregateCore;

    const QUERY_TIME: f64 = 1000.0;

    fn vector_values(qr: QueryResult) -> Vec<(Vec<String>, f64)> {
        match qr {
            QueryResult::Vector(iv) => iv
                .values
                .into_iter()
                .map(|e| (e.labels.labels, e.value))
                .collect(),
            _ => panic!("Expected vector result"),
        }
    }

    fn sorted(mut v: Vec<(Vec<String>, f64)>) -> Vec<(Vec<String>, f64)> {
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn binary_expr_vector_vector_all_ops() {
        // `^` excluded: needs its own accumulator setup (see the dedicated
        // power-operator test below).
        for (op, expected) in [
            ("+", 30.0),
            ("-", -10.0),
            ("*", 200.0),
            ("/", 0.5),
            ("%", 10.0),
        ] {
            let engine = create_engine_two_metrics(
                "metric_a",
                AggregationType::Sum,
                vec!["host"],
                vec![(
                    Some(vec!["host-a".to_string()]),
                    Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
                )],
                "sum(metric_a) by (host)",
                "metric_b",
                AggregationType::Sum,
                vec!["host"],
                vec![(
                    Some(vec!["host-a".to_string()]),
                    Box::new(SumAccumulator::with_sum(20.0)) as Box<dyn AggregateCore>,
                )],
                "sum(metric_b) by (host)",
            );

            let query = format!("sum(metric_a) by (host) {op} sum(metric_b) by (host)");
            let (_, qr) = engine
                .handle_query_promql(query, QUERY_TIME)
                .unwrap_or_else(|| panic!("query failed for op {op}"));

            let values = vector_values(qr);
            assert_eq!(values.len(), 1, "op {op}");
            assert!(
                (values[0].1 - expected).abs() < 1e-10,
                "op {op}: expected {expected}, got {}",
                values[0].1
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn binary_expr_power_operator_computes_correctly() {
        let engine = create_engine_two_metrics(
            "metric_a",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(2.0)) as Box<dyn AggregateCore>,
            )],
            "sum(metric_a) by (host)",
            "metric_b",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
            )],
            "sum(metric_b) by (host)",
        );

        let query = "sum(metric_a) by (host) ^ sum(metric_b) by (host)";
        let (_, qr) = engine
            .handle_query_promql(query.to_string(), QUERY_TIME)
            .expect("query failed");
        let values = vector_values(qr);
        assert!((values[0].1 - 1024.0).abs() < 1e-6, "2^10 = 1024");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn binary_expr_scalar_both_orderings() {
        let engine = create_engine_single_pop(
            "errors_total",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(7.0)) as Box<dyn AggregateCore>,
            )],
            "sum(errors_total) by (host)",
        );

        for query in [
            "sum(errors_total) by (host) * 100",
            "100 * sum(errors_total) by (host)",
        ] {
            let (_, qr) = engine
                .handle_query_promql(query.to_string(), QUERY_TIME)
                .unwrap_or_else(|| panic!("query failed for {query}"));
            let values = vector_values(qr);
            assert!((values[0].1 - 700.0).abs() < 1e-10, "{query}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn binary_expr_nested_binary() {
        // (metric_a + metric_b) * metric_c, all Sum, host-a: (10+20)*3 = 90
        let engine = create_engine_three_metrics(
            "metric_a",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
            )],
            "sum(metric_a) by (host)",
            "metric_b",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(20.0)) as Box<dyn AggregateCore>,
            )],
            "sum(metric_b) by (host)",
            "metric_c",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(3.0)) as Box<dyn AggregateCore>,
            )],
            "sum(metric_c) by (host)",
        );

        let query = "(sum(metric_a) by (host) + sum(metric_b) by (host)) * sum(metric_c) by (host)";
        let (_, qr) = engine
            .handle_query_promql(query.to_string(), QUERY_TIME)
            .expect("query failed");
        let values = vector_values(qr);
        assert!((values[0].1 - 90.0).abs() < 1e-10, "(10+20)*3 = 90");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn binary_expr_no_data_falls_back_to_none() {
        // metric_a is configured (schema + pattern match) but has zero
        // precomputed data. Accepted behavior change (#567, kept from
        // DataFusion's now-removed empty-result behavior): falls back to
        // Prometheus (returns None) for the whole expression — see
        // evaluate_binary_arm's leaf branch, which warns loudly when this
        // happens.
        let engine = create_engine_two_metrics(
            "metric_a",
            AggregationType::Sum,
            vec!["host"],
            vec![], // no data at all for metric_a
            "sum(metric_a) by (host)",
            "metric_b",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(20.0)) as Box<dyn AggregateCore>,
            )],
            "sum(metric_b) by (host)",
        );

        let query = "sum(metric_a) by (host) + sum(metric_b) by (host)";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME);
        assert!(
            result.is_none(),
            "arm with no current precomputed data falls back to Prometheus"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn binary_expr_unsupported_arm_returns_none() {
        // foo() is not a supported PromQL function -> arm lookup fails -> None
        // (graceful fallback to Prometheus).
        let engine = create_engine_single_pop(
            "requests_total",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(200.0)) as Box<dyn AggregateCore>,
            )],
            "sum(requests_total) by (host)",
        );

        let query = "foo(errors_total[5m]) / sum(requests_total) by (host)";
        assert!(engine
            .handle_query_promql(query.to_string(), QUERY_TIME)
            .is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn binary_expr_vector_vector_dual_population() {
        // event_frequency is dual-population (CountMinSketch values +
        // DeltaSetAggregator keys) -- confirms the leaf's keys_query
        // resolution works through the binary-expr path. Wrapped in `+ 0`
        // to route through the binary-expr handler at all.
        let cms = CountMinSketchAccumulator::new(2, 3);
        let mut keys = DeltaSetAggregatorAccumulator::new();
        keys.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });

        let engine = create_engine_dual_input(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::DeltaSetAggregator,
            vec![],
            vec!["host", "event"],
            vec![(None, Box::new(cms))],
            vec![(None, Box::new(keys))],
            "count(event_frequency) by (host, event)",
        );

        let query = "count(event_frequency) by (host, event) + 0";
        let (_, qr) = engine
            .handle_query_promql(query.to_string(), QUERY_TIME)
            .expect("query failed");
        assert!(!sorted(vector_values(qr)).is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn instant_query_dual_population_group_with_no_value_data_is_skipped_not_fatal() {
        // Instant-query counterpart to
        // native_range_query_tests::range_query_dual_population_group_with_no_value_data_is_skipped_not_fatal.
        // collect_results_separate_keys used to hard-fail the ENTIRE instant
        // query if any group resolved from merged_keys had no matching entry
        // in merged_values: `merged_values.get(key).ok_or_else(|| "No value
        // for key")?`. region=orphan has keys data (a real DeltaSetAggregator
        // key) but never has any value/CMS data at all -- that poisoned the
        // WHOLE query, so even region=normal's perfectly good data
        // disappeared. Per #597 (bringing the instant path in line with
        // #583's range-query fix), this is now skipped with a warning
        // instead, and the rest of the query's results still return.
        let cms_normal = CountMinSketchAccumulator::new(2, 3);

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

        let engine = create_engine_dual_input(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::DeltaSetAggregator,
            vec!["region"],
            vec!["host", "event"],
            vec![(
                Some(vec!["normal".to_string()]),
                Box::new(cms_normal) as Box<dyn AggregateCore>,
            )],
            // Deliberately NO value data for region=orphan.
            vec![
                (
                    Some(vec!["normal".to_string()]),
                    Box::new(keys_normal) as Box<dyn AggregateCore>,
                ),
                (
                    Some(vec!["orphan".to_string()]),
                    Box::new(keys_orphan) as Box<dyn AggregateCore>,
                ),
            ],
            "count(event_frequency) by (region, host, event)",
        );

        let query = "count(event_frequency) by (region, host, event)";
        let (_, qr) = engine
            .handle_query_promql(query.to_string(), QUERY_TIME)
            .expect(
                "instant query should succeed by skipping the value-less region=orphan \
                 group, not fail the entire query because of it",
            );
        let values = vector_values(qr);

        assert!(
            values
                .iter()
                .any(|(labels, _)| labels.contains(&"normal".to_string())),
            "region=normal has real value data and should be unaffected by \
             region=orphan having none"
        );
        assert!(
            !values
                .iter()
                .any(|(labels, _)| labels.contains(&"orphan".to_string())),
            "region=orphan has keys data but no value data anywhere -- it must be \
             silently skipped, not appear as an (empty or otherwise) series"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn instant_query_dual_population_unresolvable_key_set_is_skipped_not_fatal() {
        // collect_results_separate_keys used to hard-fail the ENTIRE instant
        // query if the keys precompute's get_keys() returned None:
        // `.ok_or_else(|| "Keys required for separate aggregation")?`.
        // region=broken's DeltaSetAggregator has the same key in both
        // `added` and `removed` -- the invariant get_keys() checks for --
        // so it resolves to None. Per #581, this now skips just that group
        // with a warning instead of failing the whole query.
        let cms_normal = CountMinSketchAccumulator::new(2, 3);
        let cms_broken = CountMinSketchAccumulator::new(2, 3);

        let mut keys_normal = DeltaSetAggregatorAccumulator::new();
        keys_normal.add_key(KeyByLabelValues {
            labels: vec![
                "normal".to_string(),
                "host-a".to_string(),
                "evt-1".to_string(),
            ],
        });
        let broken_key = KeyByLabelValues {
            labels: vec![
                "broken".to_string(),
                "host-z".to_string(),
                "evt-1".to_string(),
            ],
        };
        let mut keys_broken = DeltaSetAggregatorAccumulator::new();
        keys_broken.add_key(broken_key.clone());
        keys_broken.remove_key(broken_key);

        let engine = create_engine_dual_input(
            "event_frequency",
            AggregationType::CountMinSketch,
            AggregationType::DeltaSetAggregator,
            vec!["region"],
            vec!["host", "event"],
            vec![
                (
                    Some(vec!["normal".to_string()]),
                    Box::new(cms_normal) as Box<dyn AggregateCore>,
                ),
                (
                    Some(vec!["broken".to_string()]),
                    Box::new(cms_broken) as Box<dyn AggregateCore>,
                ),
            ],
            vec![
                (
                    Some(vec!["normal".to_string()]),
                    Box::new(keys_normal) as Box<dyn AggregateCore>,
                ),
                (
                    Some(vec!["broken".to_string()]),
                    Box::new(keys_broken) as Box<dyn AggregateCore>,
                ),
            ],
            "count(event_frequency) by (region, host, event)",
        );

        let query = "count(event_frequency) by (region, host, event) + 0";
        let (_, qr) = engine
            .handle_query_promql(query.to_string(), QUERY_TIME)
            .expect(
                "instant query should succeed by skipping the unresolvable region=broken group",
            );
        let values = vector_values(qr);

        assert!(
            values
                .iter()
                .any(|(labels, _)| labels.contains(&"normal".to_string())),
            "region=normal has a resolvable key set and should be unaffected"
        );
        assert!(
            !values
                .iter()
                .any(|(labels, _)| labels.contains(&"broken".to_string())),
            "region=broken's key set is unresolvable (added/removed invariant violated) -- \
             must be silently skipped, not appear or fail the query"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn instant_query_dual_population_key_missing_from_value_accumulator_is_skipped_not_fatal()
    {
        // collect_results_separate_keys used to hard-fail the ENTIRE instant
        // query if a single resolved key's query_precompute_for_statistic
        // call failed: `.map_err(...)?`. Keys and value data come from
        // independently-computed accumulators for a dual-population metric,
        // so they CAN skew: a key the DeltaSetAggregator (keys side) knows
        // about may have no entry in the MultipleSum (value side)
        // accumulator at all. Per #581, that one key is now skipped with a
        // warning instead of failing every other key in the same query.
        let present_key = KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        };
        let missing_key = KeyByLabelValues {
            labels: vec!["host-b".to_string(), "evt-2".to_string()],
        };

        let mut value = MultipleSumAccumulator::new();
        value.add_sum(present_key.clone(), 42.0);
        // Deliberately no entry for `missing_key`.

        let mut keys = DeltaSetAggregatorAccumulator::new();
        keys.add_key(present_key.clone());
        keys.add_key(missing_key.clone());

        let engine = create_engine_dual_input(
            "event_frequency",
            AggregationType::MultipleSum,
            AggregationType::DeltaSetAggregator,
            vec![],
            vec!["host", "event"],
            vec![(None, Box::new(value) as Box<dyn AggregateCore>)],
            vec![(None, Box::new(keys) as Box<dyn AggregateCore>)],
            "sum(event_frequency) by (host, event)",
        );

        let query = "sum(event_frequency) by (host, event) + 0";
        let (_, qr) = engine.handle_query_promql(query.to_string(), QUERY_TIME).expect(
            "instant query should succeed by skipping the one key missing from the value accumulator",
        );
        let values = sorted(vector_values(qr));

        assert_eq!(
            values,
            vec![(vec!["host-a".to_string(), "evt-1".to_string()], 42.0)],
            "host-b/evt-2 has keys data but no entry in the value accumulator -- must be \
             silently skipped, not appear or fail the query"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn binary_expr_sliding_window_end_to_end_merges_correctly() {
        // Ties Stage 1's sliding-bucket merge fix (#570) to the actual
        // production entrypoint this issue changes: 2 buckets for the same
        // key under one Sliding exact window must both be merged, not just
        // the first, when reached through a real binary-expr query.
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
        let leaf_query = "sum_over_time(http_requests[1s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            leaf_query,
            1_000, // window_size_ms, matches the fixed 1000ms bucket width
            WindowType::Sliding,
        );

        let query = format!("{leaf_query} + 0");
        let (_, qr) = engine
            .handle_query_promql(query, QUERY_TIME)
            .expect("query failed");
        let values = vector_values(qr);
        assert_eq!(values.len(), 1);
        assert!(
            (values[0].1 - 15.0).abs() < 1e-10,
            "expected both sliding-window buckets merged into 15.0, got {}",
            values[0].1
        );
    }

    #[tokio::test]
    async fn binary_expr_works_on_current_thread_runtime() {
        // Default (single-threaded) tokio runtime, not `flavor = "multi_thread"`
        // like every other test in this file. The old DataFusion path's
        // tokio::task::block_in_place(...block_on(...)) wrapper panics on a
        // current-thread runtime — this test only passes because that
        // wrapper is actually gone (#567 Stage 3 cutover), not because of
        // any value it computes.
        let engine = create_engine_single_pop(
            "errors_total",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(7.0)) as Box<dyn AggregateCore>,
            )],
            "sum(errors_total) by (host)",
        );

        let result =
            engine.handle_query_promql("sum(errors_total) by (host) * 2".to_string(), QUERY_TIME);
        assert!(result.is_some());
    }

    // --- Regression tests: combine_vector_vector must reject a join between
    // arms with different label sets rather than silently matching on
    // positional KeyByLabelValues equality alone (raw Vec<String> of values,
    // no label names attached -- see key_by_label_values.rs). This mirrors
    // DataFusion's build_binary_vector_plan, which fails to resolve a join
    // column that only exists on one side. These tests originally compared
    // against the DataFusion path to prove the divergence before the fix;
    // now that combine_vector_vector checks label-set equality directly and
    // DataFusion is no longer in the production path (#567 Stage 3), they
    // assert the fixed behavior directly.

    #[tokio::test(flavor = "multi_thread")]
    async fn binary_expr_mismatched_label_sets_with_colliding_values_returns_none() {
        // metric_a grouped by (host), metric_b grouped by (region) -- disjoint
        // label sets -- but both happen to produce the value "us-east". A
        // value-only join would spuriously match ["us-east"] == ["us-east"];
        // must return None instead.
        let engine = create_engine_two_metrics(
            "metric_a",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["us-east".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
            )],
            "sum(metric_a) by (host)",
            "metric_b",
            AggregationType::Sum,
            vec!["region"],
            vec![(
                Some(vec!["us-east".to_string()]),
                Box::new(SumAccumulator::with_sum(20.0)) as Box<dyn AggregateCore>,
            )],
            "sum(metric_b) by (region)",
        );

        let query = "sum(metric_a) by (host) + sum(metric_b) by (region)";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME);

        assert!(
            result.is_none(),
            "mismatched label sets must return None, not a spurious value-matched join: {result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn binary_expr_mismatched_label_sets_multi_label_full_collision_returns_none() {
        // Same case with a 2-label grouping: (host, dc) vs (region, zone), but
        // the *entire* ordered value vector coincides ("us-east", "az1" on
        // both sides) -- confirms the check isn't a single-label fluke.
        let engine = create_engine_two_metrics(
            "metric_a",
            AggregationType::Sum,
            vec!["host", "dc"],
            vec![(
                Some(vec!["us-east".to_string(), "az1".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
            )],
            "sum(metric_a) by (host, dc)",
            "metric_b",
            AggregationType::Sum,
            vec!["region", "zone"],
            vec![(
                Some(vec!["us-east".to_string(), "az1".to_string()]),
                Box::new(SumAccumulator::with_sum(20.0)) as Box<dyn AggregateCore>,
            )],
            "sum(metric_b) by (region, zone)",
        );

        let query = "sum(metric_a) by (host, dc) + sum(metric_b) by (region, zone)";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME);

        assert!(
            result.is_none(),
            "mismatched label sets must return None, not a spurious value-matched join: {result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn binary_expr_mismatched_label_sets_non_colliding_values_returns_none() {
        // Same disjoint label sets (host vs region), but this time the values
        // don't collide ("us-east" vs "eu-west") either -- must still return
        // None because the label sets themselves don't match, not because no
        // values happened to match.
        let engine = create_engine_two_metrics(
            "metric_a",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["us-east".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
            )],
            "sum(metric_a) by (host)",
            "metric_b",
            AggregationType::Sum,
            vec!["region"],
            vec![(
                Some(vec!["eu-west".to_string()]),
                Box::new(SumAccumulator::with_sum(20.0)) as Box<dyn AggregateCore>,
            )],
            "sum(metric_b) by (region)",
        );

        let query = "sum(metric_a) by (host) + sum(metric_b) by (region)";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME);

        assert!(
            result.is_none(),
            "mismatched label sets must return None even when no values happen to match: {result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn binary_expr_same_label_set_no_matching_values_returns_empty() {
        // Control case: identical label sets (host on both sides) but
        // disjoint values -- should resolve to an empty (not None) result.
        // This isolates the label-set check from ordinary "no match" cases.
        let engine = create_engine_two_metrics(
            "metric_a",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
            )],
            "sum(metric_a) by (host)",
            "metric_b",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(20.0)) as Box<dyn AggregateCore>,
            )],
            "sum(metric_b) by (host)",
        );

        let query = "sum(metric_a) by (host) + sum(metric_b) by (host)";
        let (_, qr) = engine
            .handle_query_promql(query.to_string(), QUERY_TIME)
            .expect("query failed");

        assert_eq!(vector_values(qr), Vec::new());
    }
}
