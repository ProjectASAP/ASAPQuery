//! Synthetic differential correctness tests for `rate()` / `increase()`.
//!
//! The PR requirement is that ASAP reproduces Prometheus counter semantics:
//! per-sample counter-reset correction, then `extrapolatedRate` to the
//! range-vector boundaries. ASAP only retains the window *endpoints* plus the
//! running reset correction and sample count — but Prometheus's own
//! `extrapolatedRate` likewise derives its interval estimate from just
//! `(first_ts, last_ts, N)` and its value from `(first, last, correction)`, so
//! ASAP should match Prometheus *exactly* for any sample stream.
//!
//! These tests verify that against an INDEPENDENT reference reimplementation of
//! Prometheus's algorithm that consumes the raw `(timestamp, value)` samples
//! directly (not ASAP's reduced accumulator state). The reference is first
//! anchored to hand-computed Prometheus values, then used as the oracle across
//! hundreds of generated series (varied reset patterns, spacing, magnitude,
//! length, and range alignment). A discrepancy on any case is a real bug in the
//! accumulator's update / merge / extrapolation logic.

#[cfg(test)]
mod tests {
    use crate::data_model::{AggregationType, KeyByLabelValues, Measurement};
    use crate::engines::query_result::QueryResult;
    use crate::precompute_operators::{IncreaseAccumulator, MultipleIncreaseAccumulator};
    use crate::tests::test_utilities::engine_factories::*;
    use promql_utilities::query_logics::enums::{RANGE_END_MS_KWARG, RANGE_START_MS_KWARG};
    use std::collections::HashMap;

    // ========================================================================
    // Independent reference (operates on raw samples, not on the accumulator)
    // ========================================================================

    /// Prometheus `counterCorrection`: sum of the pre-reset value each time the
    /// series drops. The reset-corrected increase is `last - first + correction`.
    fn reference_corrected_increase(samples: &[(i64, f64)]) -> f64 {
        let mut correction = 0.0;
        let mut last = samples[0].1;
        for &(_, v) in &samples[1..] {
            if v < last {
                correction += last;
            }
            last = v;
        }
        samples[samples.len() - 1].1 - samples[0].1 + correction
    }

    /// Independent reimplementation of Prometheus `extrapolatedRate`
    /// (promql/functions.go, `isCounter == true`) over the raw sample stream.
    /// Deliberately written from the Prometheus algorithm, not from ASAP's code.
    fn reference_extrapolated(
        samples: &[(i64, f64)],
        range_start_ms: i64,
        range_end_ms: i64,
        is_rate: bool,
    ) -> Option<f64> {
        if samples.len() < 2 {
            return None;
        }
        let first_value = samples[0].1;
        let result_value = reference_corrected_increase(samples);

        let first_ts = samples[0].0;
        let last_ts = samples[samples.len() - 1].0;

        let mut duration_to_start = (first_ts - range_start_ms) as f64 / 1000.0;
        let mut duration_to_end = (range_end_ms - last_ts) as f64 / 1000.0;

        let sampled_interval = (last_ts - first_ts) as f64 / 1000.0;
        let average_duration_between_samples = sampled_interval / (samples.len() - 1) as f64;
        let extrapolation_threshold = average_duration_between_samples * 1.1;

        if duration_to_start >= extrapolation_threshold {
            duration_to_start = average_duration_between_samples / 2.0;
        }
        if result_value > 0.0 && first_value >= 0.0 {
            let duration_to_zero = sampled_interval * (first_value / result_value);
            if duration_to_zero < duration_to_start {
                duration_to_start = duration_to_zero;
            }
        }
        if duration_to_end >= extrapolation_threshold {
            duration_to_end = average_duration_between_samples / 2.0;
        }

        let mut factor = 1.0;
        if sampled_interval != 0.0 {
            factor = (sampled_interval + duration_to_start + duration_to_end) / sampled_interval;
        }
        if is_rate {
            let range_seconds = (range_end_ms - range_start_ms) as f64 / 1000.0;
            if range_seconds <= 0.0 {
                return None;
            }
            factor /= range_seconds;
        }
        Some(result_value * factor)
    }

    /// Feed a sample stream through the accumulator exactly as ingest does:
    /// `new` on the first sample, then `update` for each subsequent one.
    fn feed(samples: &[(i64, f64)]) -> IncreaseAccumulator {
        let (ts0, v0) = samples[0];
        let mut acc =
            IncreaseAccumulator::new(Measurement::new(v0), ts0, Measurement::new(v0), ts0);
        for &(ts, v) in &samples[1..] {
            acc.update(Measurement::new(v), ts);
        }
        acc
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() <= 1e-9 * (1.0 + a.abs().max(b.abs()))
    }

    // ========================================================================
    // Anchor the reference to externally-known Prometheus values, so the oracle
    // itself is trustworthy before we test ASAP against it.
    // ========================================================================

    #[test]
    fn reference_matches_hand_computed_prometheus() {
        // From increase_accumulator::test_extrapolation_parity: 6 samples spanning
        // [5s,55s] inside [0,60s] => increase 72, rate 1.2.
        let s: Vec<(i64, f64)> = (0..6)
            .map(|i| (5_000 + i * 10_000, 10.0 + i as f64 * 12.0))
            .collect();
        assert!(close(
            reference_extrapolated(&s, 0, 60_000, false).unwrap(),
            72.0
        ));
        assert!(close(
            reference_extrapolated(&s, 0, 60_000, true).unwrap(),
            1.2
        ));

        // The [10s] window used throughout the cross-path tests: 10@992s, 70@997s
        // inside [990s,1000s] => increase 106, rate 10.6.
        let s2 = [(992_000i64, 10.0), (997_000i64, 70.0)];
        assert!(close(
            reference_extrapolated(&s2, 990_000, 1_000_000, false).unwrap(),
            106.0
        ));
        assert!(close(
            reference_extrapolated(&s2, 990_000, 1_000_000, true).unwrap(),
            10.6
        ));

        // counterCorrection anchor: 10 -> 100 -> 5(reset) -> 30 => 30-10+100 = 120.
        let s3 = [(0i64, 10.0), (1i64, 100.0), (2i64, 5.0), (3i64, 30.0)];
        assert!(close(reference_corrected_increase(&s3), 120.0));
    }

    // ========================================================================
    // Deterministic pseudo-random generator (no Math::random; reproducible)
    // ========================================================================

    struct Lcg(u64);
    impl Lcg {
        fn next_u64(&mut self) -> u64 {
            // Numerical-Recipes LCG constants.
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0
        }
        fn unit(&mut self) -> f64 {
            (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
        }
        fn range(&mut self, lo: usize, hi: usize) -> usize {
            lo + (self.next_u64() as usize) % (hi - lo + 1)
        }
    }

    /// Generate a synthetic counter series: `n` samples starting at `start_ts`,
    /// each gap drawn from `[min_gap, max_gap]` ms (uneven spacing allowed),
    /// monotonically increasing with occasional resets to a small value.
    fn synthetic_series(
        rng: &mut Lcg,
        start_ts: i64,
        n: usize,
        min_gap: usize,
        max_gap: usize,
        reset_prob: f64,
    ) -> Vec<(i64, f64)> {
        let mut samples = Vec::with_capacity(n);
        let mut value = rng.unit() * 50.0; // >= 0
        let mut ts = start_ts;
        for _ in 0..n {
            samples.push((ts, value));
            if rng.unit() < reset_prob {
                value = rng.unit() * 10.0; // counter restart (drops below current)
            } else {
                value += rng.unit() * 25.0; // positive increment
            }
            ts += rng.range(min_gap, max_gap) as i64;
        }
        samples
    }

    /// A set of range configurations relative to the series span, to exercise the
    /// tight case, the half-gap extrapolation, and the threshold clamp.
    fn range_configs(samples: &[(i64, f64)]) -> Vec<(i64, i64)> {
        let first = samples[0].0;
        let last = samples[samples.len() - 1].0;
        let span = (last - first).max(1);
        let gap = span / (samples.len() as i64 - 1).max(1);
        vec![
            (first, last),                       // tight: no outward extrapolation
            (first - gap / 2, last + gap / 2),   // within threshold both sides
            (first - gap, last + gap),           // ~ at threshold
            (first - 5 * span, last + 5 * span), // far: threshold clamp both sides
        ]
    }

    // ========================================================================
    // Differential property test: single window
    // ========================================================================

    #[test]
    fn synthetic_single_window_matches_prometheus() {
        let mut rng = Lcg(0x9E3779B97F4A7C15);
        let mut cases = 0usize;
        for iter in 0..1500 {
            let n = rng.range(2, 24);
            let uneven = iter % 3 == 0;
            let (min_gap, max_gap) = if uneven {
                (1_000, 30_000)
            } else {
                (15_000, 15_000)
            };
            let reset_prob = [0.0, 0.1, 0.25, 0.5][iter % 4];
            let samples = synthetic_series(&mut rng, 1_000_000, n, min_gap, max_gap, reset_prob);
            if samples[samples.len() - 1].0 == samples[0].0 {
                continue; // skip degenerate zero-span (guarded elsewhere)
            }

            let acc = feed(&samples);

            // Reset correction must match the independent counterCorrection.
            let ref_inc = reference_corrected_increase(&samples);
            assert!(
                close(acc.corrected_increase(), ref_inc),
                "corrected_increase mismatch: acc={} ref={} samples={:?}",
                acc.corrected_increase(),
                ref_inc,
                samples
            );
            // sample_count must equal the number of samples fed.
            assert_eq!(acc.sample_count, samples.len() as u64);

            // Extrapolation must match Prometheus for every range config & stat.
            for (rs, re) in range_configs(&samples) {
                for is_rate in [false, true] {
                    let got = acc.extrapolated(is_rate, rs, re);
                    let want = reference_extrapolated(&samples, rs, re, is_rate);
                    match (got, want) {
                        (Some(g), Some(w)) => assert!(
                            close(g, w),
                            "extrapolated mismatch (is_rate={is_rate}) got={g} want={w} \
                             range=[{rs},{re}] samples={samples:?}"
                        ),
                        (None, None) => {}
                        (g, w) => panic!(
                            "presence mismatch got={g:?} want={w:?} range=[{rs},{re}] samples={samples:?}"
                        ),
                    }
                    cases += 1;
                }
            }
        }
        assert!(cases > 5_000, "expected broad coverage, only {cases} cases");
    }

    // ========================================================================
    // Named edge cases (guaranteed coverage of the tricky branches)
    // ========================================================================

    #[test]
    fn synthetic_edge_cases_match_prometheus() {
        let g = 15_000i64; // even 15s spacing
        let cases: Vec<Vec<(i64, f64)>> = vec![
            // Monotonic, no reset.
            (0..5).map(|i| (i * g, 10.0 + 7.0 * i as f64)).collect(),
            // Reset on the 2nd sample.
            vec![(0, 100.0), (g, 5.0), (2 * g, 25.0), (3 * g, 40.0)],
            // Reset on the last sample.
            vec![(0, 10.0), (g, 40.0), (2 * g, 90.0), (3 * g, 3.0)],
            // Multiple consecutive resets.
            vec![
                (0, 50.0),
                (g, 5.0),
                (2 * g, 3.0),
                (3 * g, 1.0),
                (4 * g, 60.0),
            ],
            // Counter starting exactly at 0 (zero-point clamp with first_value 0).
            (0..6).map(|i| (i * g, 12.0 * i as f64)).collect(),
            // Near-zero start relative to large increase (zero clamp engages).
            vec![(0, 2.0), (g, 40.0), (2 * g, 80.0), (3 * g, 102.0)],
            // Flat counter (no increase, no reset) => increase 0.
            (0..4).map(|i| (i * g, 42.0)).collect(),
            // Exactly two samples (minimum for a defined rate).
            vec![(0, 10.0), (g, 70.0)],
            // Uneven spacing with a reset.
            vec![(0, 10.0), (2_000, 15.0), (2_500, 4.0), (30_000, 25.0)],
        ];

        for samples in &cases {
            let acc = feed(samples);
            assert!(
                close(
                    acc.corrected_increase(),
                    reference_corrected_increase(samples)
                ),
                "corrected_increase edge mismatch for {samples:?}"
            );
            for (rs, re) in range_configs(samples) {
                for is_rate in [false, true] {
                    let got = acc.extrapolated(is_rate, rs, re);
                    let want = reference_extrapolated(samples, rs, re, is_rate);
                    assert_eq!(
                        got.is_some(),
                        want.is_some(),
                        "presence mismatch for {samples:?}"
                    );
                    if let (Some(g), Some(w)) = (got, want) {
                        assert!(
                            close(g, w),
                            "edge extrapolated mismatch (is_rate={is_rate}) got={g} want={w} \
                             range=[{rs},{re}] samples={samples:?}"
                        );
                    }
                }
            }
        }
    }

    // ========================================================================
    // Merge path: windowed ingest (tumbling buckets) then merge must reproduce
    // the same result as a single window over the full series — including
    // counter resets that straddle a window boundary.
    // ========================================================================

    fn merge_fold(mut windows: Vec<IncreaseAccumulator>) -> IncreaseAccumulator {
        let mut acc = windows.remove(0);
        for w in windows {
            acc = IncreaseAccumulator::merge_pair(&acc, &w);
        }
        acc
    }

    #[test]
    fn synthetic_merge_matches_full_window() {
        let mut rng = Lcg(0xD1B54A32D192ED03);
        for iter in 0..800 {
            let n = rng.range(4, 20);
            let samples = synthetic_series(
                &mut rng,
                1_000_000,
                n,
                10_000,
                10_000,
                [0.0, 0.2, 0.4][iter % 3],
            );
            if samples[samples.len() - 1].0 == samples[0].0 {
                continue;
            }

            // Split into contiguous, non-overlapping windows at random cut points.
            let n_windows = rng.range(1, (n - 1).max(1));
            let mut cuts: Vec<usize> = (1..n).collect();
            // pick `n_windows-1` interior cut points deterministically
            let mut chosen = Vec::new();
            for _ in 0..n_windows.saturating_sub(1) {
                if cuts.is_empty() {
                    break;
                }
                let idx = rng.range(0, cuts.len() - 1);
                chosen.push(cuts.remove(idx));
            }
            chosen.sort_unstable();
            chosen.dedup();

            let mut windows = Vec::new();
            let mut start = 0usize;
            for &cut in &chosen {
                windows.push(feed(&samples[start..cut]));
                start = cut;
            }
            windows.push(feed(&samples[start..]));

            let merged = merge_fold(windows);

            // The merged accumulator must equal a single window over all samples.
            assert!(
                close(
                    merged.corrected_increase(),
                    reference_corrected_increase(&samples)
                ),
                "merged corrected_increase mismatch: got={} ref={} cuts={:?} samples={:?}",
                merged.corrected_increase(),
                reference_corrected_increase(&samples),
                chosen,
                samples
            );
            assert_eq!(merged.sample_count, samples.len() as u64);
            assert_eq!(merged.starting_timestamp, samples[0].0);
            assert_eq!(merged.last_seen_timestamp, samples[samples.len() - 1].0);

            for (rs, re) in range_configs(&samples) {
                let got = merged.extrapolated(false, rs, re);
                let want = reference_extrapolated(&samples, rs, re, false);
                if let (Some(g), Some(w)) = (got, want) {
                    assert!(
                        close(g, w),
                        "merged extrapolated mismatch got={g} want={w} cuts={chosen:?} samples={samples:?}"
                    );
                }
            }
        }
    }

    // ========================================================================
    // End-to-end: synthetic series through the real query paths.
    // ========================================================================

    fn instant_values(qr: QueryResult) -> Vec<f64> {
        match qr {
            QueryResult::Vector(iv) => iv.values.into_iter().map(|e| e.value).collect(),
            other => panic!("expected vector, got {other:?}"),
        }
    }

    /// Drive several synthetic series through the instant pipeline (#1) and the
    /// binary-arithmetic DataFusion path (#3) as single-population increase, and
    /// confirm both equal the independent Prometheus reference over the exact
    /// range boundaries the engine derives.
    #[tokio::test(flavor = "multi_thread")]
    async fn synthetic_end_to_end_paths_match_reference() {
        const QT: f64 = 1000.0; // eval at t=1_000_000 ms
        let g = 20_000i64;
        // A few representative series that fit inside a [100s] window ending at QT.
        let series: Vec<Vec<(i64, f64)>> = vec![
            // monotonic
            (0..5)
                .map(|i| (910_000 + i * g, 10.0 + 30.0 * i as f64))
                .collect(),
            // with a mid-window reset
            vec![
                (910_000, 10.0),
                (930_000, 90.0),
                (950_000, 5.0),
                (970_000, 45.0),
                (990_000, 80.0),
            ],
            // near-zero start (zero clamp)
            vec![(915_000, 1.0), (945_000, 50.0), (975_000, 130.0)],
        ];

        for samples in &series {
            let acc = feed(samples);
            let engine = create_engine_single_pop(
                "m",
                AggregationType::Increase,
                vec!["host"],
                vec![(Some(vec!["host-a".to_string()]), Box::new(acc))],
                "increase(m[100s])",
            );

            // Range boundaries the engine derives for this query.
            let ctx = engine
                .build_query_execution_context_promql("increase(m[100s])".to_string(), QT)
                .unwrap();
            let rs: i64 = ctx.metadata.query_kwargs[RANGE_START_MS_KWARG]
                .parse()
                .unwrap();
            let re: i64 = ctx.metadata.query_kwargs[RANGE_END_MS_KWARG]
                .parse()
                .unwrap();
            let want_inc = reference_extrapolated(samples, rs, re, false).unwrap();

            // Path #1: instant pipeline.
            let p1 = instant_values(
                engine
                    .handle_query_promql("increase(m[100s])".to_string(), QT)
                    .expect("instant increase")
                    .1,
            );
            assert_eq!(p1.len(), 1);
            assert!(
                close(p1[0], want_inc),
                "path#1 got={} want={} samples={:?}",
                p1[0],
                want_inc,
                samples
            );

            // Path #3: binary-arithmetic DataFusion path (× 1 keeps the value).
            let p3 = instant_values(
                engine
                    .handle_query_promql("increase(m[100s]) * 1".to_string(), QT)
                    .expect("binary increase")
                    .1,
            );
            assert_eq!(p3.len(), 1);
            assert!(
                close(p3[0], want_inc),
                "path#3 got={} want={} samples={:?}",
                p3[0],
                want_inc,
                samples
            );

            // rate() via the binary path scaled by 2. Needs an engine whose config
            // structurally matches `rate(...)` (config lookup is by function form),
            // served by the same underlying Increase accumulator.
            let rate_engine = create_engine_single_pop(
                "m",
                AggregationType::Increase,
                vec!["host"],
                vec![(Some(vec!["host-a".to_string()]), Box::new(feed(samples)))],
                "rate(m[100s])",
            );
            let want_rate = reference_extrapolated(samples, rs, re, true).unwrap();
            let p3r = instant_values(
                rate_engine
                    .handle_query_promql("rate(m[100s]) * 2".to_string(), QT)
                    .expect("binary rate")
                    .1,
            );
            assert_eq!(p3r.len(), 1);
            assert!(
                close(p3r[0], want_rate * 2.0),
                "path#3 rate got={} want={} samples={:?}",
                p3r[0],
                want_rate * 2.0,
                samples
            );
        }
    }

    /// Multi-population synthetic coverage: several sub-keys, each an independent
    /// synthetic counter, extrapolated through the DataFusion path and checked
    /// per-key against the reference.
    #[tokio::test(flavor = "multi_thread")]
    async fn synthetic_multipop_paths_match_reference() {
        const QT: f64 = 1000.0;
        let mut rng = Lcg(0x2545F4914F6CDD1D);

        // Build sub-keys with distinct synthetic series inside [900s, 1000s].
        // Key 0 is an explicit reset series so the multi-pop path is always
        // exercised over counter-reset correction, not just monotonic counters.
        let mut per_key_samples: Vec<(String, Vec<(i64, f64)>)> = vec![(
            "host-0".to_string(),
            vec![
                (905_000, 10.0),
                (930_000, 90.0),
                (955_000, 5.0), // reset
                (980_000, 45.0),
            ],
        )];
        let mut increases = HashMap::new();
        for k in 1..5 {
            let n = rng.range(3, 8);
            let samples = synthetic_series(&mut rng, 905_000, n, 8_000, 8_000, 0.4);
            // keep inside the 100s window
            let samples: Vec<(i64, f64)> = samples
                .into_iter()
                .filter(|(t, _)| *t <= 1_000_000)
                .collect();
            if samples.len() < 2 {
                continue;
            }
            per_key_samples.push((format!("host-{k}"), samples));
        }
        for (host, samples) in &per_key_samples {
            let ep = host.replace("host-", "ep-");
            let key = KeyByLabelValues {
                labels: vec![host.clone(), ep],
            };
            increases.insert(key, feed(samples));
        }
        let acc = MultipleIncreaseAccumulator::new_with_increases(increases);

        let engine = create_engine_single_pop_with_aggregated(
            "m",
            AggregationType::MultipleIncrease,
            vec![],
            vec!["host", "endpoint"],
            vec![(None, Box::new(acc))],
            "increase(m[100s])",
        );
        let ctx = engine
            .build_query_execution_context_promql("increase(m[100s])".to_string(), QT)
            .unwrap();
        let rs: i64 = ctx.metadata.query_kwargs[RANGE_START_MS_KWARG]
            .parse()
            .unwrap();
        let re: i64 = ctx.metadata.query_kwargs[RANGE_END_MS_KWARG]
            .parse()
            .unwrap();

        let result = engine
            .handle_query_promql("increase(m[100s]) * 1".to_string(), QT)
            .expect("multipop binary increase");
        let elems = match result.1 {
            QueryResult::Vector(iv) => iv.values,
            other => panic!("expected vector, got {other:?}"),
        };
        assert_eq!(elems.len(), per_key_samples.len(), "one result per sub-key");

        for (host, samples) in &per_key_samples {
            let want = reference_extrapolated(samples, rs, re, false).unwrap();
            let got = elems
                .iter()
                .find(|e| e.labels.labels.first().map(|s| s.as_str()) == Some(host.as_str()))
                .unwrap_or_else(|| panic!("missing result for {host}"))
                .value;
            assert!(
                close(got, want),
                "multipop {host}: got={got} want={want} samples={samples:?}"
            );
        }
    }
}
