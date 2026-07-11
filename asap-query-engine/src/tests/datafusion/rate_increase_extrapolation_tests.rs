//! Cross-path correctness tests for `rate()` / `increase()` counter-reset
//! correction + Prometheus extrapolation.
//!
//! `rate()`/`increase()` are served by three distinct execution paths, and all
//! three must reproduce Prometheus `extrapolatedRate` (counter-reset correction
//! followed by extrapolation to the range-vector boundaries) rather than the
//! plain reset-corrected fallback:
//!
//!   1. **Instant, single expression** — `handle_query_promql("increase(m[w])")`
//!      → `execute_query_pipeline` (in-memory accumulator query).
//!   2. **Range query** — `handle_range_query_promql(...)`
//!      → `execute_range_query_pipeline` (per-step range boundaries).
//!   3. **Instant, binary arithmetic** — `handle_query_promql("increase(m[w]) * k")`
//!      → DataFusion `SummaryInferExec` (bounds embedded in `InferOperation`).
//!
//! The oracle for "correct" is [`IncreaseAccumulator::extrapolated`], which is
//! pinned to hand-computed Prometheus values in `increase_accumulator.rs`
//! (`test_extrapolation_parity` etc.). Each test asserts the path reproduces that
//! value AND that it differs from the non-extrapolated fallback, so a regression
//! back to the old behavior fails loudly.

#[cfg(test)]
mod tests {
    use crate::data_model::{
        AggregationType, KeyByLabelValues, Measurement, SingleSubpopulationAggregate, WindowType,
    };
    use crate::engines::query_result::{InstantVectorElement, QueryResult};
    use crate::precompute_operators::{IncreaseAccumulator, MultipleIncreaseAccumulator};
    use crate::tests::test_utilities::engine_factories::*;
    use crate::AggregateCore;
    use promql_utilities::query_logics::enums::{
        Statistic, RANGE_END_MS_KWARG, RANGE_START_MS_KWARG,
    };
    use std::collections::HashMap;

    const QUERY_TIME: f64 = 1000.0; // seconds → 1_000_000 ms
    const WINDOW: &str = "[10s]";

    /// A monotonic 2-sample counter window [10@992s → 70@997s] (no reset), built
    /// the way ingest does (`new` then `update` ⇒ sample_count == 2).
    fn counter_no_reset() -> IncreaseAccumulator {
        let mut a = IncreaseAccumulator::new(
            Measurement::new(10.0),
            992_000,
            Measurement::new(10.0),
            992_000,
        );
        a.update(Measurement::new(70.0), 997_000);
        a
    }

    /// A 4-sample counter window with one reset: 10 → 50 → 5 (reset, +50) → 30.
    /// Reset-corrected increase = 30 - 10 + 50 = 70.
    fn counter_with_reset() -> IncreaseAccumulator {
        let mut a = IncreaseAccumulator::new(
            Measurement::new(10.0),
            992_000,
            Measurement::new(10.0),
            992_000,
        );
        a.update(Measurement::new(50.0), 994_000);
        a.update(Measurement::new(5.0), 995_000); // reset: +50 correction
        a.update(Measurement::new(30.0), 997_000);
        a
    }

    fn multi_pop(inner: IncreaseAccumulator) -> MultipleIncreaseAccumulator {
        let key = KeyByLabelValues {
            labels: vec!["host-a".to_string(), "endpoint-1".to_string()],
        };
        let mut m = HashMap::new();
        m.insert(key, inner);
        MultipleIncreaseAccumulator::new_with_increases(m)
    }

    fn values(qr: QueryResult) -> Vec<InstantVectorElement> {
        match qr {
            QueryResult::Vector(iv) => iv.values,
            other => panic!("expected instant vector, got {other:?}"),
        }
    }

    /// Range boundaries the engine derives for `<fn>(m[10s])` at `QUERY_TIME`.
    fn engine_bounds(
        engine: &crate::engines::simple_engine::SimpleEngine,
        query: &str,
    ) -> (i64, i64) {
        let ctx = engine
            .build_query_execution_context_promql(query.to_string(), QUERY_TIME)
            .expect("build context");
        (
            ctx.metadata.query_kwargs[RANGE_START_MS_KWARG]
                .parse()
                .unwrap(),
            ctx.metadata.query_kwargs[RANGE_END_MS_KWARG]
                .parse()
                .unwrap(),
        )
    }

    // ========================================================================
    // Anchor: the extrapolation oracle produces the expected absolute values.
    // ========================================================================

    /// Ground-truth anchor independent of the query engine: over [990s, 1000s]
    /// the no-reset counter extrapolates to increase 106 / rate 10.6, well away
    /// from the reset-corrected fallback of 60.
    #[test]
    fn oracle_anchor_values() {
        let inc = counter_no_reset()
            .extrapolated(false, 990_000, 1_000_000)
            .unwrap();
        let rate = counter_no_reset()
            .extrapolated(true, 990_000, 1_000_000)
            .unwrap();
        assert!((inc - 106.0).abs() < 1e-9, "increase = {inc}");
        assert!((rate - 10.6).abs() < 1e-9, "rate = {rate}");
        let fallback =
            SingleSubpopulationAggregate::query(&counter_no_reset(), Statistic::Increase, None)
                .unwrap();
        assert_eq!(fallback, 60.0, "reset-corrected fallback");
    }

    // ========================================================================
    // Path 1: instant single-expression (pipeline)
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn path1_instant_increase_extrapolates() {
        let engine = create_engine_single_pop_with_aggregated(
            "m",
            AggregationType::MultipleIncrease,
            vec![],
            vec!["host", "endpoint"],
            vec![(None, Box::new(multi_pop(counter_no_reset())))],
            &format!("increase(m{WINDOW})"),
        );
        let (s, e) = engine_bounds(&engine, &format!("increase(m{WINDOW})"));
        let expected = counter_no_reset().extrapolated(false, s, e).unwrap();

        let out = values(
            engine
                .handle_query_promql(format!("increase(m{WINDOW})"), QUERY_TIME)
                .expect("path #1 should serve increase")
                .1,
        );
        assert_eq!(out.len(), 1);
        assert!((out[0].value - expected).abs() < 1e-6);
        assert!((out[0].value - 106.0).abs() < 1e-6, "got {}", out[0].value);
    }

    // ========================================================================
    // Path 2: range query (pipeline, per-step boundaries)
    // ========================================================================

    /// Range query over two steps. The pipeline skips `None` keys, so this uses a
    /// single-population Increase bucket with an explicit key. Each output point
    /// extrapolates over its own window: T=1000s → [990s,1000s] → 106; T=999s →
    /// [989s,999s] → 94.
    #[tokio::test(flavor = "multi_thread")]
    async fn path2_range_increase_extrapolates() {
        // Bucket start timestamp is 992_000 (factory stores [ts-1000, ts]).
        let engine = create_engine_multi_timestamp_with_window(
            "m",
            AggregationType::Increase,
            vec!["host"],
            vec![(
                993_000u64,
                Some(vec!["host-a".to_string()]),
                Box::new(counter_no_reset()) as Box<dyn AggregateCore>,
            )],
            &format!("increase(m{WINDOW})"),
            1000,
            WindowType::Tumbling,
        );

        let expected_t1000 = counter_no_reset()
            .extrapolated(false, 990_000, 1_000_000)
            .unwrap();
        let expected_t999 = counter_no_reset()
            .extrapolated(false, 989_000, 999_000)
            .unwrap();

        let (_, qr) = engine
            .handle_range_query_promql(format!("increase(m{WINDOW})"), 999.0, 1000.0, 1.0)
            .expect("path #2 should serve range increase");
        let series = match qr {
            QueryResult::Matrix(rv) => rv.values,
            other => panic!("expected matrix, got {other:?}"),
        };
        assert_eq!(series.len(), 1, "one series");
        let samples: Vec<f64> = series[0].samples.iter().map(|s| s.value).collect();
        assert_eq!(samples.len(), 2, "two output steps: {samples:?}");
        assert!(
            (samples[0] - expected_t999).abs() < 1e-6,
            "T=999s: {samples:?}"
        );
        assert!(
            (samples[1] - expected_t1000).abs() < 1e-6,
            "T=1000s: {samples:?}"
        );
        // Anchor + proof it is extrapolation, not the flat fallback of 60.
        assert!((samples[1] - 106.0).abs() < 1e-6);
        assert!((samples[0] - 94.0).abs() < 1e-6);
    }

    // ========================================================================
    // Path 3: instant binary arithmetic (DataFusion / SummaryInferExec) — the
    // path this fix repairs. Single-pop Increase has no arroyo deserializer, so
    // the executable case is the multi-population accumulator.
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn path3_binary_increase_extrapolates() {
        let engine = create_engine_single_pop_with_aggregated(
            "m",
            AggregationType::MultipleIncrease,
            vec![],
            vec!["host", "endpoint"],
            vec![(None, Box::new(multi_pop(counter_no_reset())))],
            &format!("increase(m{WINDOW})"),
        );
        let (s, e) = engine_bounds(&engine, &format!("increase(m{WINDOW})"));
        let expected = counter_no_reset().extrapolated(false, s, e).unwrap();

        // `* 1` forces the binary-arithmetic instant path (DataFusion) while
        // leaving the extrapolated value unchanged.
        let out = values(
            engine
                .handle_query_promql(format!("increase(m{WINDOW}) * 1"), QUERY_TIME)
                .expect("path #3 should serve binary increase")
                .1,
        );
        assert_eq!(out.len(), 1);
        assert!((out[0].value - expected).abs() < 1e-6);
        assert!((out[0].value - 106.0).abs() < 1e-6, "got {}", out[0].value);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn path3_binary_rate_extrapolates_with_arithmetic() {
        let engine = create_engine_single_pop_with_aggregated(
            "m",
            AggregationType::MultipleIncrease,
            vec![],
            vec!["host", "endpoint"],
            vec![(None, Box::new(multi_pop(counter_no_reset())))],
            &format!("rate(m{WINDOW})"),
        );
        let (s, e) = engine_bounds(&engine, &format!("rate(m{WINDOW})"));
        let expected_rate = counter_no_reset().extrapolated(true, s, e).unwrap();

        let out = values(
            engine
                .handle_query_promql(format!("rate(m{WINDOW}) * 2"), QUERY_TIME)
                .expect("path #3 should serve binary rate")
                .1,
        );
        assert_eq!(out.len(), 1);
        assert!(
            (out[0].value - expected_rate * 2.0).abs() < 1e-6,
            "got {}, expected {}",
            out[0].value,
            expected_rate * 2.0
        );
        assert!((out[0].value - 21.2).abs() < 1e-6, "got {}", out[0].value);
    }

    // ========================================================================
    // Cross-path consistency + counter-reset correctness
    // ========================================================================

    /// The instant pipeline (#1) and the binary DataFusion path (#3) must produce
    /// the identical extrapolated value for the same sub-expression — the property
    /// that was violated before this fix (#3 silently used the flat fallback).
    #[tokio::test(flavor = "multi_thread")]
    async fn paths_1_and_3_agree() {
        let make = || {
            create_engine_single_pop_with_aggregated(
                "m",
                AggregationType::MultipleIncrease,
                vec![],
                vec!["host", "endpoint"],
                vec![(None, Box::new(multi_pop(counter_no_reset())))],
                &format!("increase(m{WINDOW})"),
            )
        };
        let engine = make();
        let p1 = values(
            engine
                .handle_query_promql(format!("increase(m{WINDOW})"), QUERY_TIME)
                .unwrap()
                .1,
        );
        let p3 = values(
            engine
                .handle_query_promql(format!("increase(m{WINDOW}) * 1"), QUERY_TIME)
                .unwrap()
                .1,
        );
        assert_eq!(p1.len(), 1);
        assert_eq!(p3.len(), 1);
        assert!(
            (p1[0].value - p3[0].value).abs() < 1e-9,
            "instant #1 ({}) and binary #3 ({}) must agree",
            p1[0].value,
            p3[0].value
        );
    }

    /// Counter-reset correction must be applied before extrapolation on the binary
    /// path: with a mid-window reset the corrected increase is 70 (not 20), and the
    /// path must reproduce the extrapolation of that corrected value.
    #[tokio::test(flavor = "multi_thread")]
    async fn path3_binary_increase_applies_reset_correction() {
        let engine = create_engine_single_pop_with_aggregated(
            "m",
            AggregationType::MultipleIncrease,
            vec![],
            vec!["host", "endpoint"],
            vec![(None, Box::new(multi_pop(counter_with_reset())))],
            &format!("increase(m{WINDOW})"),
        );
        let (s, e) = engine_bounds(&engine, &format!("increase(m{WINDOW})"));

        // Sanity: the reset correction is in the corrected increase (70, not 20),
        // and the extrapolation of a reset-corrected series differs from a naive
        // last-first (20) as well as from the reset-corrected-but-flat value (70).
        assert_eq!(counter_with_reset().corrected_increase(), 70.0);
        let expected = counter_with_reset().extrapolated(false, s, e).unwrap();
        assert!(
            expected > 70.0,
            "extrapolated {expected} should exceed flat 70"
        );

        let out = values(
            engine
                .handle_query_promql(format!("increase(m{WINDOW}) * 1"), QUERY_TIME)
                .expect("path #3 should serve binary increase with reset")
                .1,
        );
        assert_eq!(out.len(), 1);
        assert!(
            (out[0].value - expected).abs() < 1e-6,
            "got {}, expected {}",
            out[0].value,
            expected
        );
    }

    // ========================================================================
    // Single-population Increase in a binary expression (DataFusion path)
    // ========================================================================

    /// Single-population `IncreaseAccumulator` is deserializable on the DataFusion
    /// path (arroyo/MessagePack serde), so a binary expression with a single-pop
    /// increase arm is accelerated and extrapolates — identically to the same
    /// single-pop increase served by the instant pipeline (#1). This is the
    /// scenario the arroyo-serde addition unlocked; before it, the binary form
    /// returned `None` and fell back to Prometheus.
    #[tokio::test(flavor = "multi_thread")]
    async fn singlepop_increase_binary_extrapolates_like_instant() {
        let engine = create_engine_single_pop(
            "m",
            AggregationType::Increase,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(counter_no_reset()),
            )],
            &format!("increase(m{WINDOW})"),
        );

        let (s, e) = engine_bounds(&engine, &format!("increase(m{WINDOW})"));
        let expected = counter_no_reset().extrapolated(false, s, e).unwrap();

        // Instant pipeline (#1) extrapolates correctly for single-pop Increase.
        let instant = values(
            engine
                .handle_query_promql(format!("increase(m{WINDOW})"), QUERY_TIME)
                .expect("instant single-pop increase should work")
                .1,
        );
        assert_eq!(instant.len(), 1);
        assert!((instant[0].value - expected).abs() < 1e-6);

        // Binary arithmetic (#3, DataFusion) is now accelerated and returns the
        // same extrapolated value — no Prometheus fallback.
        let binary = values(
            engine
                .handle_query_promql(format!("increase(m{WINDOW}) * 1"), QUERY_TIME)
                .expect("single-pop increase in a binary expr should now accelerate")
                .1,
        );
        assert_eq!(binary.len(), 1);
        assert!((binary[0].value - expected).abs() < 1e-6);
        assert!(
            (binary[0].value - instant[0].value).abs() < 1e-9,
            "binary must match instant"
        );
        assert!(
            (binary[0].value - 106.0).abs() < 1e-6,
            "got {}",
            binary[0].value
        );
    }
}
