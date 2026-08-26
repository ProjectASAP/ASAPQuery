//! Boundary/validation tests for the range-query entry point (issue #590,
//! "Boundary/validation tests" section).
//!
//! `validate_range_query_params` (asap-query-engine/src/engines/simple_engine/mod.rs)
//! had NO dedicated unit tests before this file -- confirmed via
//! `grep -rn "validate_range_query_params" asap-query-engine/src/`, which
//! turns up only the function's own definition and its single call site in
//! `finish_range_context` (`engines/simple_engine/promql.rs`).
//!
//! That call site is also why these tests can't assert the exact error
//! *string* through the public API: `finish_range_context` folds
//! `validate_range_query_params`'s `Result<(), String>` into an `Option` via
//! `.map_err(|e| { warn!(...); e }).ok()?` -- the message only ever reaches a
//! `warn!` log, never the caller. And `handle_range_query_promql` itself
//! returns `Option<(KeyByLabelNames, QueryResult)>`, so `None` is *all* a
//! caller (including this test file) can ever observe for a validation
//! failure, whether it's "start >= end", "step == 0", or anything else.
//! `validate_range_query_params` is also a private method with no public
//! callers outside its own module, so it can't be invoked directly from
//! `crate::tests` either (Rust's privacy is module-tree scoped, and
//! `crate::tests` isn't a descendant of `engines::simple_engine`).
//!
//! So the exact-error-string assertions live directly next to the function,
//! in `validate_range_query_params_tests` at the bottom of
//! `engines/simple_engine/mod.rs` (a `#[cfg(test)]` module in the same file,
//! which *can* see private items). This file covers the other half: proving
//! end-to-end, through the real public entry point, that each bad-param
//! case is actually rejected (returns `None`) rather than silently
//! misbehaving -- including the `start == end` boundary specifically, which
//! is easy to get wrong (e.g. by treating it as a degenerate single-instant
//! range instead of rejecting it).

#[cfg(test)]
mod tests {
    use crate::data_model::{AggregationType, KeyByLabelValues};
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::precompute_operators::{CountMinSketchAccumulator, DeltaSetAggregatorAccumulator};
    use crate::tests::test_utilities::engine_factories::{
        create_engine_dual_input, create_engine_single_pop,
    };
    use crate::AggregateCore;

    /// Matches `engine_factories`' fixed insert timestamp (1_000_000 ms) and
    /// `native_binary_instant_tests.rs`'s `QUERY_TIME`, so the same engine
    /// can be queried both as an instant query (at this time) and as a
    /// single-bucket range query ending at this time.
    const QUERY_TIME_S: f64 = 1000.0;

    /// `create_engine_single_pop`/`create_engine_dual_input` configure a
    /// Tumbling aggregation with `window_size_ms == slide_interval_ms ==
    /// 1000`, so `bucket_step_ms` (the `tumbling_window_ms` fed into
    /// `validate_range_query_params`) is 1000ms for every engine built here.
    const TUMBLING_WINDOW_MS: u64 = 1000;

    fn single_pop_engine() -> crate::engines::simple_engine::SimpleEngine {
        create_engine_single_pop(
            "requests_total",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(42.0)) as Box<dyn AggregateCore>,
            )],
            "sum(requests_total) by (host)",
        )
    }

    // ---- Happy path: sanity baseline showing the recipe below IS capable
    // ---- of succeeding, so the None results in the tests after it are
    // ---- actually attributable to the validation branch under test, not to
    // ---- some unrelated setup mistake.
    #[tokio::test(flavor = "multi_thread")]
    async fn valid_params_are_accepted() {
        let engine = single_pop_engine();
        let query = "sum(requests_total) by (host)";
        let result = engine.handle_range_query_promql(query.to_string(), 999.0, QUERY_TIME_S, 1.0);
        assert!(
            result.is_some(),
            "start=999.0 < end=1000.0, step=1000ms is a multiple of the \
             1000ms tumbling window -- this must succeed"
        );
    }

    // ---- start >= end ----

    #[tokio::test(flavor = "multi_thread")]
    async fn start_after_end_is_rejected() {
        let engine = single_pop_engine();
        let query = "sum(requests_total) by (host)";
        let result = engine.handle_range_query_promql(query.to_string(), QUERY_TIME_S, 999.0, 1.0);
        assert!(
            result.is_none(),
            "start > end must be rejected by validate_range_query_params \
             ('start must be before end'), surfaced as None from \
             handle_range_query_promql"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn start_equal_to_end_is_rejected() {
        // Issue #590 boundary case: start == end must be rejected outright
        // (validate_range_query_params's `start >= end` check), NOT silently
        // treated as a degenerate single-instant/single-bucket range. This
        // pins that the end-to-end behavior actually matches the validator's
        // stated contract.
        let engine = single_pop_engine();
        let query = "sum(requests_total) by (host)";
        let result =
            engine.handle_range_query_promql(query.to_string(), QUERY_TIME_S, QUERY_TIME_S, 1.0);
        assert!(
            result.is_none(),
            "start == end must be rejected, not treated as a valid \
             1-bucket range -- if this fails, validate_range_query_params's \
             `start >= end` check is not actually being enforced end-to-end \
             (see #590)"
        );
    }

    // ---- step == 0 ----

    #[tokio::test(flavor = "multi_thread")]
    async fn zero_step_is_rejected() {
        let engine = single_pop_engine();
        let query = "sum(requests_total) by (host)";
        let result = engine.handle_range_query_promql(query.to_string(), 999.0, QUERY_TIME_S, 0.0);
        assert!(
            result.is_none(),
            "step == 0 must be rejected by validate_range_query_params \
             ('step must be positive')"
        );
    }

    // ---- step not a multiple of the tumbling window size ----

    #[tokio::test(flavor = "multi_thread")]
    async fn step_not_a_multiple_of_tumbling_window_is_rejected() {
        let engine = single_pop_engine();
        let query = "sum(requests_total) by (host)";
        // 1500ms is not a multiple of the engine's 1000ms tumbling window.
        let result = engine.handle_range_query_promql(query.to_string(), 999.0, QUERY_TIME_S, 1.5);
        assert!(
            result.is_none(),
            "step (1500ms) not a multiple of tumbling_window_ms (1000ms) \
             must be rejected"
        );
    }

    #[test]
    fn tumbling_window_constant_matches_engine_factories_assumption() {
        // Guards the premise the tests above rely on: if engine_factories
        // ever changes its window/slide configuration, TUMBLING_WINDOW_MS
        // here (and the 1.5s "not a multiple" step above) would silently
        // stop testing what they claim to.
        assert_eq!(TUMBLING_WINDOW_MS, 1000);
    }

    // ---- #590/#582: instant vs. range parity on a missing-value-mid-merge
    // ---- group (keys resolve a group, but that group has no value/CMS data
    // ---- anywhere). native_range_query_tests.rs's
    // ---- `range_query_dual_population_group_with_no_value_data_is_skipped_not_fatal`
    // ---- and native_binary_instant_tests.rs's
    // ---- `instant_query_dual_population_group_with_no_value_data_is_skipped_not_fatal`
    // ---- already independently pin "skipped, not fatal" for range and
    // ---- instant respectively (range fixed under #583, instant brought in
    // ---- line under #597 per that test's own comment). Both already pass
    // ---- as of this writing. This test doesn't re-derive that from
    // ---- scratch -- it runs the *same* orphan-group scenario through both
    // ---- entry points on data shaped so both are queryable, and diffs
    // ---- their skip/error behavior explicitly, so a future regression that
    // ---- reintroduces divergence between the two paths (one skips, the
    // ---- other goes fatal) fails loudly right here instead of only in
    // ---- whichever of the two dedicated tests happens to catch it.
    #[tokio::test(flavor = "multi_thread")]
    async fn instant_and_range_agree_on_skipping_value_less_orphan_group() {
        let build_orphan_engine = || {
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

            create_engine_dual_input(
                "event_frequency",
                AggregationType::CountMinSketch,
                AggregationType::DeltaSetAggregator,
                vec!["region"],
                vec!["host", "event"],
                vec![(
                    Some(vec!["normal".to_string()]),
                    Box::new(cms_normal) as Box<dyn AggregateCore>,
                )],
                // Deliberately NO value data for region=orphan, at any timestamp.
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
            )
        };

        let query = "count(event_frequency) by (region, host, event)";

        let instant_engine = build_orphan_engine();
        let instant_result = instant_engine.handle_query_promql(query.to_string(), QUERY_TIME_S);

        let range_engine = build_orphan_engine();
        // Single-bucket range ending at the same instant, so both paths hit
        // the exact same underlying (Tumbling) window.
        let range_result =
            range_engine.handle_range_query_promql(query.to_string(), 999.0, QUERY_TIME_S, 1.0);

        let instant_skipped_not_fatal = instant_result.is_some();
        let range_skipped_not_fatal = range_result.is_some();

        assert_eq!(
            instant_skipped_not_fatal,
            range_skipped_not_fatal,
            "instant and range paths diverge on the value-less orphan-group \
             case: instant {} (is_some={}), range {} (is_some={}). Per #590 \
             (and the #583/#597 fixes it references), both paths must treat \
             a keys-resolved-but-value-less group identically -- either both \
             skip it and return the rest of the query's results, or both \
             fail the whole query. See native_range_query_tests.rs's \
             range_query_dual_population_group_with_no_value_data_is_skipped_not_fatal \
             and native_binary_instant_tests.rs's \
             instant_query_dual_population_group_with_no_value_data_is_skipped_not_fatal.",
            if instant_skipped_not_fatal {
                "succeeded"
            } else {
                "failed (None)"
            },
            instant_skipped_not_fatal,
            if range_skipped_not_fatal {
                "succeeded"
            } else {
                "failed (None)"
            },
            range_skipped_not_fatal,
        );

        // Both dedicated tests currently pin "skipped, not fatal" as the
        // expected behavior for their own path; this asserts that's also
        // what actually happened here, on top of the equality check above.
        assert!(
            instant_skipped_not_fatal && range_skipped_not_fatal,
            "expected the value-less orphan group to be skipped (not fatal) \
             on BOTH paths -- instant is_some={instant_skipped_not_fatal}, \
             range is_some={range_skipped_not_fatal}"
        );
    }
}
