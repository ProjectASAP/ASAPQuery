//! Structural PromQL matching tests.
//!
//! Verifies that `find_query_config_promql_structural` can look up query configs
//! by AST-serialised arm strings, which is the mechanism used during binary
//! arithmetic dispatch.

#[cfg(test)]
mod tests {
    use crate::data_model::{
        AggregationReference, AggregationType, CleanupPolicy, InferenceConfig, PromQLSchema,
        QueryConfig, SchemaConfig,
    };
    use crate::engines::QueryResult;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::tests::test_utilities::engine_factories::create_engine_single_pop;
    use asap_types::query_config::{
        QueryTimeAggregation, QueryTimeAggregationOperator, QueryTimeAggregationParameter,
        QueryTimeGrouping, QueryTimeGroupingMode,
    };
    use promql_utilities::data_model::KeyByLabelNames;

    #[test]
    fn test_structural_match_rate_query_finds_config() {
        let engine = create_engine_single_pop(
            "http_requests_total",
            AggregationType::MultipleIncrease,
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
    fn exact_nested_query_config_executes_its_anchor_then_pipeline() {
        let metric = "http_requests_total";
        let anchor = "sum by (job) (http_requests_total)";
        let query = "topk(1, sum by (job) (http_requests_total))";
        let engine = create_engine_single_pop(
            metric,
            AggregationType::Sum,
            vec!["job"],
            vec![
                (
                    Some(vec!["api".to_string()]),
                    Box::new(SumAccumulator::with_sum(4.0)),
                ),
                (
                    Some(vec!["worker".to_string()]),
                    Box::new(SumAccumulator::with_sum(9.0)),
                ),
            ],
            anchor,
        );
        let query_config = QueryConfig::with_plan(
            query.to_string(),
            anchor.to_string(),
            vec![QueryTimeAggregation {
                operator: QueryTimeAggregationOperator::Topk,
                grouping: QueryTimeGrouping {
                    mode: QueryTimeGroupingMode::All,
                    labels: Vec::new(),
                },
                parameter: Some(QueryTimeAggregationParameter::Integer(1)),
            }],
        )
        .add_aggregation(AggregationReference::new(1, None));
        engine.update_inference_config(InferenceConfig {
            schema: SchemaConfig::PromQL(PromQLSchema::new().add_metric(
                metric.to_string(),
                KeyByLabelNames::new(vec!["job".to_string()]),
            )),
            query_configs: vec![query_config],
            cleanup_policy: CleanupPolicy::NoCleanup,
        });

        let (labels, result) = engine
            .handle_query_promql(query.to_string(), 1_000.0)
            .expect("the exact configured query should use its planned anchor");

        assert_eq!(labels.labels, vec!["job"]);
        let QueryResult::Vector(vector) = result else {
            panic!("instant query should produce a vector");
        };
        assert_eq!(vector.values.len(), 1);
        assert_eq!(vector.values[0].labels.labels, vec!["worker"]);
        assert_eq!(vector.values[0].value, 9.0);
    }

    #[test]
    fn test_structural_match_wrong_metric_returns_none() {
        let engine = create_engine_single_pop(
            "http_requests_total",
            AggregationType::MultipleIncrease,
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
            AggregationType::MultipleIncrease,
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
            AggregationType::MultipleIncrease,
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
            AggregationType::Sum,
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
