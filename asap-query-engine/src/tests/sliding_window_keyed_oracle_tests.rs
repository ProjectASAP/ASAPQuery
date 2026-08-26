//! Issue #590, "Test suite ideas for instant/range PromQL query paths": a
//! property-style test for `execute_range_query_pipeline`'s Sliding-window
//! and key-expansion behavior, using `simulate_sliding_window_keyed`
//! (`engines::simple_engine::mod.rs`) as an independent, pure-function
//! oracle rather than more hand-picked engine-level regression cases.
//!
//! Those hand-picked cases already exist --
//! `native_range_query_tests::range_query_sliding_window_merges_both_buckets`,
//! `range_query_sliding_window_merges_three_buckets_same_timestamp`, and
//! `range_query_delta_set_aggregator_oscillating_add_remove_across_five_windows`
//! -- and are deliberately not duplicated here. This test instead sweeps a
//! small deterministic grid of (window_size, slide_interval) configs crossed
//! with key-presence patterns (a key present throughout, appearing midway,
//! disappearing midway, oscillating, with a mid-range gap, or present for
//! only a single pane), builds a real `SimpleEngine` for each combination via
//! `create_engine_multi_timestamp_with_window` (the same factory the
//! hand-picked tests use), and checks the engine's actual range-query output
//! against `simulate_sliding_window_keyed`'s independently-computed
//! expectation.
//!
//! No `proptest`/`quickcheck` dependency is used or added (`asap-query-engine`
//! has neither as a dev-dependency) -- the sweep below is a small, fully
//! deterministic set of hand-enumerated configs (3 window configs x 7
//! presence-pattern pairs = 21 scenarios x 2 keys each), not a fuzzer.

#[cfg(test)]
mod tests {
    use crate::data_model::{AggregationType, WindowType};
    use crate::engines::query_result::QueryResult;
    use crate::engines::simple_engine::simulate_sliding_window_keyed;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::tests::test_utilities::engine_factories::create_engine_multi_timestamp_with_window;
    use crate::AggregateCore;
    use std::collections::HashMap;

    /// Presence of a single key across the 6 fixed pane timestamps below --
    /// `true` means the key has a value at that pane, `false` means it's
    /// absent there (simulating appearing/disappearing mid-range).
    type Presence = [bool; 6];

    /// One tumbling-window pane: (pane end timestamp ms, label values,
    /// accumulator) -- same shape as `native_range_query_tests::TimeSeriesData`.
    type PaneData = Vec<(u64, Option<Vec<String>>, Box<dyn AggregateCore>)>;

    const ALWAYS: Presence = [true, true, true, true, true, true];
    const APPEARS_MIDWAY: Presence = [false, false, false, true, true, true];
    const DISAPPEARS_MIDWAY: Presence = [true, true, true, false, false, false];
    const OSCILLATING: Presence = [true, false, true, false, true, false];
    const GAP_IN_MIDDLE: Presence = [true, true, false, true, true, true];
    const SINGLE_PANE: Presence = [false, false, true, false, false, false];

    /// Pane end-timestamps shared by every scenario: 1s-wide panes from
    /// 1000ms to 6000ms.
    const PANE_TIMESTAMPS_MS: [u64; 6] = [1000, 2000, 3000, 4000, 5000, 6000];

    /// Query range end/step shared by every scenario: covers every pane plus
    /// one extra step past the last pane (7000ms), so the sweep also
    /// exercises "no data left to slide into" at the tail end for every
    /// config.
    ///
    /// The query START is deliberately NOT a shared constant -- it is
    /// `scenario.window_size_ms` (set in `check_scenario` below), i.e. the
    /// first output step for which a full window's worth of history could
    /// possibly exist. Starting any earlier hits a real, separately-tracked
    /// engine bug (see `sliding_window_range_query_start_before_window_size_ms_returns_wrong_value`
    /// below, `#[ignore]`d): `execute_range_query_pipeline` computes each
    /// step's window_start as `current_time.saturating_sub(lookback_ms)`,
    /// so *every* current_time < window_size_ms saturates to the same
    /// window_start=0 and aliases onto whatever legitimate window happens to
    /// be stored at start=0 (built for the correct, later, current_time ==
    /// window_size_ms step) instead of correctly finding no data. That bug
    /// is orthogonal to what this sweep is testing (key expansion timing +
    /// multi-pane merge correctness), so the sweep avoids the affected
    /// region rather than let it drown out the signal this test exists to
    /// check.
    const QUERY_END_MS: u64 = 7000;
    const QUERY_STEP_MS: u64 = 1000;

    /// Builds `(pane_end_ms, value)` pairs for a key with the given presence
    /// pattern. Per-pane value is `value_offset + pane_index` (1-indexed) so
    /// every pane has a distinct, hand-verifiable value and the two keys in
    /// a scenario (different `value_offset`s) can never accidentally collide
    /// on a merged sum.
    fn panes_for(presence: Presence, value_offset: f64) -> Vec<(u64, f64)> {
        PANE_TIMESTAMPS_MS
            .iter()
            .zip(presence.iter())
            .enumerate()
            .filter(|(_, (_, present))| **present)
            .map(|(i, (ts, _))| (*ts, value_offset + (i as f64 + 1.0)))
            .collect()
    }

    /// One (window config) x (presence pattern pair) combination to check.
    struct Scenario {
        window_size_ms: u64,
        slide_interval_ms: u64,
        host_a_presence: Presence,
        host_b_presence: Presence,
        label: String,
    }

    fn window_configs() -> Vec<(u64, u64, &'static str)> {
        vec![
            (1000, 1000, "window=1000/slide=1000 (num_panes=1)"),
            (2000, 1000, "window=2000/slide=1000 (num_panes=2)"),
            (3000, 1000, "window=3000/slide=1000 (num_panes=3)"),
        ]
    }

    fn presence_pairs() -> Vec<(Presence, Presence, &'static str)> {
        vec![
            (ALWAYS, APPEARS_MIDWAY, "always vs appears_midway"),
            (ALWAYS, DISAPPEARS_MIDWAY, "always vs disappears_midway"),
            (ALWAYS, OSCILLATING, "always vs oscillating"),
            (
                APPEARS_MIDWAY,
                DISAPPEARS_MIDWAY,
                "appears_midway vs disappears_midway",
            ),
            (OSCILLATING, GAP_IN_MIDDLE, "oscillating vs gap_in_middle"),
            (GAP_IN_MIDDLE, SINGLE_PANE, "gap_in_middle vs single_pane"),
            (
                DISAPPEARS_MIDWAY,
                SINGLE_PANE,
                "disappears_midway vs single_pane",
            ),
        ]
    }

    /// The full sweep: every window config crossed with every presence pair
    /// (3 x 7 = 21 scenarios).
    fn scenarios() -> Vec<Scenario> {
        let mut out = Vec::new();
        for (window_size_ms, slide_interval_ms, window_label) in window_configs() {
            for (host_a_presence, host_b_presence, pair_label) in presence_pairs() {
                out.push(Scenario {
                    window_size_ms,
                    slide_interval_ms,
                    host_a_presence,
                    host_b_presence,
                    label: format!("{window_label} / {pair_label}"),
                });
            }
        }
        out
    }

    fn matrix_values(qr: QueryResult) -> Vec<crate::engines::query_result::RangeVectorElement> {
        match qr {
            QueryResult::Matrix(m) => m.values,
            QueryResult::Vector(_) => panic!("expected matrix (range vector) result"),
        }
    }

    /// Runs one scenario end-to-end: builds the oracle's expectation,
    /// builds a matching real engine, runs the same range query, and
    /// returns a human-readable mismatch description per (key) pair that
    /// diverged, or an empty Vec if the scenario passed cleanly.
    fn check_scenario(scenario: &Scenario) -> Vec<String> {
        let a_panes = panes_for(scenario.host_a_presence, 0.0);
        let b_panes = panes_for(scenario.host_b_presence, 1000.0);

        let mut panes_by_key: HashMap<Option<Vec<String>>, Vec<(u64, f64)>> = HashMap::new();
        panes_by_key.insert(Some(vec!["host-a".to_string()]), a_panes.clone());
        panes_by_key.insert(Some(vec!["host-b".to_string()]), b_panes.clone());

        // See the comment on QUERY_END_MS above: start exactly at
        // window_size_ms, the earliest step not affected by the separately
        // tracked saturating_sub aliasing bug.
        let query_start_ms = scenario.window_size_ms;

        let expected = simulate_sliding_window_keyed(
            &panes_by_key,
            scenario.window_size_ms,
            scenario.slide_interval_ms,
            query_start_ms,
            QUERY_END_MS,
            QUERY_STEP_MS,
        );

        let mut data: PaneData = Vec::new();
        for (ts, v) in &a_panes {
            data.push((
                *ts,
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(*v)) as Box<dyn AggregateCore>,
            ));
        }
        for (ts, v) in &b_panes {
            data.push((
                *ts,
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(*v)) as Box<dyn AggregateCore>,
            ));
        }

        let query = "sum_over_time(http_requests[1s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            scenario.window_size_ms,
            scenario.slide_interval_ms,
            WindowType::Sliding,
        );

        let result = engine.handle_range_query_promql(
            query.to_string(),
            query_start_ms as f64 / 1000.0,
            QUERY_END_MS as f64 / 1000.0,
            QUERY_STEP_MS as f64 / 1000.0,
        );
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);

        let mut mismatches = Vec::new();
        for key_label in ["host-a", "host-b"] {
            let mut expected_samples: Vec<(u64, f64)> = expected
                .get(&Some(vec![key_label.to_string()]))
                .cloned()
                .unwrap_or_default();
            expected_samples.sort_by_key(|(t, _)| *t);

            let mut actual_samples: Vec<(u64, f64)> = elements
                .iter()
                .find(|e| e.labels.labels.contains(&key_label.to_string()))
                .map(|e| e.samples.iter().map(|s| (s.timestamp, s.value)).collect())
                .unwrap_or_default();
            actual_samples.sort_by_key(|(t, _)| *t);

            let matches = actual_samples.len() == expected_samples.len()
                && actual_samples
                    .iter()
                    .zip(expected_samples.iter())
                    .all(|((at, av), (et, ev))| at == et && (av - ev).abs() < 1e-9);

            if !matches {
                mismatches.push(format!(
                    "scenario [{}] key={}: expected={:?}, actual={:?}",
                    scenario.label, key_label, expected_samples, actual_samples
                ));
            }
        }
        mismatches
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sliding_window_keyed_property_sweep() {
        let mut all_mismatches = Vec::new();
        for scenario in scenarios() {
            all_mismatches.extend(check_scenario(&scenario));
        }

        assert!(
            all_mismatches.is_empty(),
            "sliding-window keyed oracle diverged from the real engine in {} case(s):\n{}",
            all_mismatches.len(),
            all_mismatches.join("\n")
        );
    }

    /// Minimal reproducer for a real bug found while building the sweep
    /// above (discovered via the [window=2000/slide=1000] x [always vs
    /// appears_midway] scenario, which originally queried starting at
    /// t=1000 -- see the comment on QUERY_END_MS): a Sliding-window range
    /// query whose FIRST requested output step is earlier than the
    /// aggregation's `window_size_ms` gets a phantom sample at that step,
    /// wrongly duplicating the value of the first *legitimate* step
    /// (current_time == window_size_ms) instead of correctly having no
    /// sample there.
    ///
    /// Root cause (`execute_range_query_pipeline`,
    /// `asap-query-engine/src/engines/simple_engine/mod.rs`): each step's
    /// window_start is computed as `current_time.saturating_sub(lookback_ms)`
    /// (`lookback_ms == window_size_ms` for Sliding, per the active assert
    /// a few lines above that computation). For `current_time <
    /// window_size_ms` this saturates to 0 -- the *same* window_start that
    /// the legitimately-computed step `current_time == window_size_ms`
    /// also resolves to (via ordinary, non-saturating subtraction). Since
    /// `single_window` (used for `WindowType::Sliding`) does a bare
    /// `bucket_map.get(&window_start)` with no independent check that
    /// `current_time` actually had `window_size_ms` worth of history behind
    /// it, both steps collide on the one store entry legitimately built for
    /// `current_time == window_size_ms`, and the earlier step silently
    /// receives that later step's value instead of "no data in window."
    ///
    /// Concretely here: window_size_ms=2000, slide_interval_ms=1000 (2
    /// panes/window), one key with panes at t=1000 (value 10.0) and t=2000
    /// (value 5.0). The only genuine full window is [0, 2000) -> merged
    /// value 15.0, correctly surfaced at t=2000. Querying from t=1000
    /// should show NO sample at t=1000 (a window ending at 1000 would need
    /// history back to t=-1000, which doesn't exist) -- but the engine
    /// currently reports 15.0 there too, identical to t=2000.
    ///
    /// Do not weaken this assertion or patch around it if this test's
    /// premise ever needs revisiting -- it is exact-equality against the
    /// documented intended behavior (no sample when there isn't a full
    /// window's worth of history), not an approximation. See #590.
    #[ignore = "real bug, see #590: current_time.saturating_sub(lookback_ms) aliases every \
                pre-window_size_ms step onto the store's start=0 window instead of skipping it"]
    #[tokio::test(flavor = "multi_thread")]
    async fn sliding_window_range_query_start_before_window_size_ms_returns_wrong_value() {
        let window_size_ms = 2000;
        let slide_interval_ms = 1000;

        let data: PaneData = vec![
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

        let query = "sum_over_time(http_requests[1s])";
        let engine = create_engine_multi_timestamp_with_window(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            data,
            query,
            window_size_ms,
            slide_interval_ms,
            WindowType::Sliding,
        );

        // Query starts at t=1000ms, one slide interval before window_size_ms
        // (2000ms) -- the earliest step for which a full window could
        // possibly exist.
        let result = engine.handle_range_query_promql(query.to_string(), 1.0, 2.0, 1.0);
        let (_, qr) = result.expect("range query failed");
        let elements = matrix_values(qr);

        let host_a_samples: Vec<(u64, f64)> = elements
            .iter()
            .find(|e| e.labels.labels.contains(&"host-a".to_string()))
            .map(|e| e.samples.iter().map(|s| (s.timestamp, s.value)).collect())
            .unwrap_or_default();

        assert_eq!(
            host_a_samples,
            vec![(2000, 15.0)],
            "expected only t=2000 (the first step with a full 2000ms window's worth of \
             history) to have a sample, with the two panes merged into 15.0; t=1000 should \
             have no sample at all (window would need history back to t=-1000). Got {:?} -- \
             if this now shows exactly [(2000, 15.0)], the saturating_sub aliasing bug has \
             been fixed and this test should be un-ignored.",
            host_a_samples
        );
    }
}
