//! Native instant binary-expr evaluator tests (issue #567, Stage 2).
//!
//! `handle_binary_expr_promql_native`/`evaluate_arm_native` are a new,
//! parallel implementation of PromQL binary-arithmetic instant queries,
//! built to eventually replace the DataFusion-backed
//! `handle_binary_expr_promql` (#567's Stage 3 cutover). These tests compare
//! the two paths directly — both are reachable today (`handle_query_promql`
//! for DataFusion, `handle_query_promql_native` for the new path) so
//! equivalence can be proven before anything is rewired.

#[cfg(test)]
mod tests {
    use crate::data_model::{AggregationType, KeyByLabelValues};
    use crate::engines::query_result::QueryResult;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::precompute_operators::{CountMinSketchAccumulator, DeltaSetAggregatorAccumulator};
    use crate::tests::test_utilities::engine_factories::{
        create_engine_dual_input, create_engine_single_pop, create_engine_three_metrics,
        create_engine_two_metrics,
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
    async fn native_vector_vector_all_ops_match_datafusion() {
        // `^` excluded: DataFusion's build_binary_vector_plan maps T_POW to
        // Operator::BitwiseXor "as a proxy" (plan_builder.rs comment) and
        // fails to even produce a result for it today — nothing to compare
        // native against for that operator. See the dedicated `^` test below.
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
            let (_, old_qr) = engine
                .handle_query_promql(query.clone(), QUERY_TIME)
                .unwrap_or_else(|| panic!("old path failed for op {op}"));
            let (_, new_qr) = engine
                .handle_query_promql_native(query.clone(), QUERY_TIME)
                .unwrap_or_else(|| panic!("new path failed for op {op}"));

            let old_values = sorted(vector_values(old_qr));
            let new_values = sorted(vector_values(new_qr));
            assert_eq!(old_values.len(), 1, "op {op}");
            assert_eq!(
                new_values, old_values,
                "op {op}: native must match DataFusion"
            );
            assert!(
                (new_values[0].1 - expected).abs() < 1e-10,
                "op {op}: expected {expected}, got {}",
                new_values[0].1
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_power_operator_computes_correctly_datafusion_unsupported() {
        // DataFusion's `^` support is broken today (see comment above); this
        // documents that native computes it correctly via f64::powf,
        // independent of a DataFusion comparison.
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
        let (_, new_qr) = engine
            .handle_query_promql_native(query.to_string(), QUERY_TIME)
            .expect("new path failed");
        let new_values = vector_values(new_qr);
        assert!((new_values[0].1 - 1024.0).abs() < 1e-6, "2^10 = 1024");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_vector_scalar_both_orderings_match_datafusion() {
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
            let (_, old_qr) = engine
                .handle_query_promql(query.to_string(), QUERY_TIME)
                .unwrap_or_else(|| panic!("old path failed for {query}"));
            let (_, new_qr) = engine
                .handle_query_promql_native(query.to_string(), QUERY_TIME)
                .unwrap_or_else(|| panic!("new path failed for {query}"));

            let old_values = vector_values(old_qr);
            let new_values = vector_values(new_qr);
            assert_eq!(new_values, old_values, "{query}");
            assert!((new_values[0].1 - 700.0).abs() < 1e-10, "{query}");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_nested_binary_matches_datafusion() {
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
        let (_, old_qr) = engine
            .handle_query_promql(query.to_string(), QUERY_TIME)
            .expect("old path failed");
        let (_, new_qr) = engine
            .handle_query_promql_native(query.to_string(), QUERY_TIME)
            .expect("new path failed");

        let old_values = vector_values(old_qr);
        let new_values = vector_values(new_qr);
        assert_eq!(new_values, old_values);
        assert!((new_values[0].1 - 90.0).abs() < 1e-10, "(10+20)*3 = 90");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_binary_expr_no_data_falls_back_to_none() {
        // metric_a is configured (schema + pattern match) but has zero
        // precomputed data. Accepted behavior change (#567): unlike
        // DataFusion (which would return an empty-but-present result),
        // native falls back to Prometheus (returns None) for the whole
        // expression — see evaluate_arm_native's leaf branch.
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
        let old_result = engine.handle_query_promql(query.to_string(), QUERY_TIME);
        assert!(
            old_result.is_some(),
            "DataFusion returns an empty-but-present result, not None, for a currently-empty arm"
        );

        let new_result = engine.handle_query_promql_native(query.to_string(), QUERY_TIME);
        assert!(
            new_result.is_none(),
            "native falls back to Prometheus (None) for a currently-empty arm"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_binary_expr_unsupported_arm_returns_none() {
        // foo() is not a supported PromQL function -> arm lookup fails -> None,
        // same as today's DataFusion path (dispatch_arithmetic_tests.rs).
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
        assert!(engine
            .handle_query_promql_native(query.to_string(), QUERY_TIME)
            .is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn native_vector_vector_dual_population_matches_datafusion() {
        // event_frequency is dual-population (CountMinSketch values +
        // DeltaSetAggregator keys) -- confirms the leaf swap
        // (ctx.to_logical_plan() -> execute_query_pipeline) still resolves
        // keys_query correctly. Wrapped in `+ 0` to route through the
        // binary-expr handler at all.
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
        let (_, old_qr) = engine
            .handle_query_promql(query.to_string(), QUERY_TIME)
            .expect("old path failed");
        let (_, new_qr) = engine
            .handle_query_promql_native(query.to_string(), QUERY_TIME)
            .expect("new path failed");

        assert_eq!(sorted(vector_values(new_qr)), sorted(vector_values(old_qr)));
    }
}
