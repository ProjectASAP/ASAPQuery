//! Structural PromQL matching tests.
//!
//! Verifies that `find_query_config_promql_structural` can look up query configs
//! by AST-serialised arm strings, which is the mechanism used during binary
//! arithmetic dispatch.

#[cfg(test)]
mod tests {
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::tests::test_utilities::engine_factories::create_engine_single_pop;

    #[test]
    fn test_structural_match_rate_query_finds_config() {
        let engine = create_engine_single_pop(
            "http_requests_total",
            "MultipleIncrease",
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(100.0)),
            )],
            "rate(http_requests_total[5m])",
        );

        let ast =
            promql_parser::parser::parse("rate(http_requests_total[5m])").expect("parse failed");
        let result = engine.find_query_config_promql_structural(&ast);
        assert!(
            result.is_some(),
            "Expected to find config for rate query, got None"
        );
    }

    #[test]
    fn test_structural_match_wrong_metric_returns_none() {
        let engine = create_engine_single_pop(
            "http_requests_total",
            "MultipleIncrease",
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(100.0)),
            )],
            "rate(http_requests_total[5m])",
        );

        let ast = promql_parser::parser::parse("rate(other_metric[5m])").expect("parse failed");
        let result = engine.find_query_config_promql_structural(&ast);
        assert!(
            result.is_none(),
            "Should not find config for different metric"
        );
    }

    #[test]
    fn test_structural_match_wrong_range_returns_none() {
        let engine = create_engine_single_pop(
            "http_requests_total",
            "MultipleIncrease",
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(100.0)),
            )],
            "rate(http_requests_total[5m])",
        );

        let ast =
            promql_parser::parser::parse("rate(http_requests_total[1m])").expect("parse failed");
        let result = engine.find_query_config_promql_structural(&ast);
        assert!(
            result.is_none(),
            "Should not find config for different range"
        );
    }

    #[test]
    fn test_structural_match_wrong_function_returns_none() {
        let engine = create_engine_single_pop(
            "http_requests_total",
            "MultipleIncrease",
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(100.0)),
            )],
            "rate(http_requests_total[5m])",
        );

        let ast = promql_parser::parser::parse("increase(http_requests_total[5m])")
            .expect("parse failed");
        let result = engine.find_query_config_promql_structural(&ast);
        assert!(
            result.is_none(),
            "Should not match a different function name"
        );
    }

    #[test]
    fn test_structural_match_spatial_query() {
        let engine = create_engine_single_pop(
            "http_requests_total",
            "SumAccumulator",
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(100.0)),
            )],
            "sum(http_requests_total) by (host)",
        );

        let ast = promql_parser::parser::parse("sum(http_requests_total) by (host)")
            .expect("parse failed");
        let result = engine.find_query_config_promql_structural(&ast);
        assert!(
            result.is_some(),
            "Expected to find config for sum by (host) query"
        );
    }
}
