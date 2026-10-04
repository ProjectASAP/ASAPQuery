//! Public PromQL coverage for query-time aggregation pipelines.

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

    const METRIC: &str = "query_time_aggregation_metric";
    const ANCHOR: &str = "sum by (instance, job, region) (query_time_aggregation_metric)";

    fn engine() -> crate::SimpleEngine {
        create_engine_single_pop(
            METRIC,
            AggregationType::Sum,
            vec!["instance", "job", "region"],
            vec![
                (
                    Some(vec!["a".to_string(), "api".to_string(), "east".to_string()]),
                    Box::new(SumAccumulator::with_sum(1.0)),
                ),
                (
                    Some(vec!["b".to_string(), "api".to_string(), "east".to_string()]),
                    Box::new(SumAccumulator::with_sum(3.0)),
                ),
                (
                    Some(vec![
                        "a".to_string(),
                        "worker".to_string(),
                        "west".to_string(),
                    ]),
                    Box::new(SumAccumulator::with_sum(5.0)),
                ),
                (
                    Some(vec![
                        "b".to_string(),
                        "worker".to_string(),
                        "west".to_string(),
                    ]),
                    Box::new(SumAccumulator::with_sum(7.0)),
                ),
            ],
            ANCHOR,
        )
    }

    fn stage(
        operator: QueryTimeAggregationOperator,
        grouping: QueryTimeGroupingMode,
        labels: &[&str],
        parameter: Option<QueryTimeAggregationParameter>,
    ) -> QueryTimeAggregation {
        QueryTimeAggregation {
            operator,
            grouping: QueryTimeGrouping {
                mode: grouping,
                labels: labels.iter().map(|label| (*label).to_string()).collect(),
            },
            parameter,
        }
    }

    fn execute(
        engine: &crate::SimpleEngine,
        query: String,
        stage: QueryTimeAggregation,
    ) -> Vec<(Vec<String>, f64)> {
        engine.update_inference_config(InferenceConfig {
            schema: SchemaConfig::PromQL(PromQLSchema::new().add_metric(
                METRIC.to_string(),
                KeyByLabelNames::new(vec![
                    "instance".to_string(),
                    "job".to_string(),
                    "region".to_string(),
                ]),
            )),
            query_configs: vec![QueryConfig::with_plan(
                query.clone(),
                ANCHOR.to_string(),
                vec![stage],
            )
            .add_aggregation(AggregationReference::new(1, None))],
            cleanup_policy: CleanupPolicy::NoCleanup,
        });
        let Some((_, result)) = engine
            .handle_query_promql(query, 1_000.0)
            .expect("configured nested query should execute locally")
        else {
            panic!("configured nested query should execute locally");
        };
        let QueryResult::Vector(vector) = result else {
            panic!("instant query should return a vector");
        };
        vector
            .values
            .into_iter()
            .map(|value| (value.labels.labels, value.value))
            .collect()
    }

    #[test]
    fn every_query_time_operator_respects_global_by_and_without_grouping() {
        let engine = engine();
        let cases = [
            (
                "sum",
                QueryTimeAggregationOperator::Sum,
                None,
                vec![(vec![], 16.0)],
                vec![(vec!["api"], 4.0), (vec!["worker"], 12.0)],
            ),
            (
                "count",
                QueryTimeAggregationOperator::Count,
                None,
                vec![(vec![], 4.0)],
                vec![(vec!["api"], 2.0), (vec!["worker"], 2.0)],
            ),
            (
                "avg",
                QueryTimeAggregationOperator::Avg,
                None,
                vec![(vec![], 4.0)],
                vec![(vec!["api"], 2.0), (vec!["worker"], 6.0)],
            ),
            (
                "min",
                QueryTimeAggregationOperator::Min,
                None,
                vec![(vec![], 1.0)],
                vec![(vec!["api"], 1.0), (vec!["worker"], 5.0)],
            ),
            (
                "max",
                QueryTimeAggregationOperator::Max,
                None,
                vec![(vec![], 7.0)],
                vec![(vec!["api"], 3.0), (vec!["worker"], 7.0)],
            ),
            (
                "quantile",
                QueryTimeAggregationOperator::Quantile,
                Some(QueryTimeAggregationParameter::Float(0.5)),
                vec![(vec![], 4.0)],
                vec![(vec!["api"], 2.0), (vec!["worker"], 6.0)],
            ),
            (
                "topk",
                QueryTimeAggregationOperator::Topk,
                Some(QueryTimeAggregationParameter::Integer(3)),
                vec![
                    (vec!["a", "worker", "west"], 5.0),
                    (vec!["b", "api", "east"], 3.0),
                    (vec!["b", "worker", "west"], 7.0),
                ],
                vec![
                    (vec!["a", "api", "east"], 1.0),
                    (vec!["a", "worker", "west"], 5.0),
                    (vec!["b", "api", "east"], 3.0),
                    (vec!["b", "worker", "west"], 7.0),
                ],
            ),
        ];

        for (name, operator, parameter, global_expected, grouped_expected) in cases {
            let parameter_for_global = parameter.clone();
            let global_query = match name {
                "quantile" => format!("quantile(0.5, {ANCHOR})"),
                "topk" => format!("topk(3, {ANCHOR})"),
                _ => format!("{name}({ANCHOR})"),
            };
            assert_eq!(
                execute(
                    &engine,
                    global_query,
                    stage(
                        operator.clone(),
                        QueryTimeGroupingMode::All,
                        &[],
                        parameter_for_global
                    ),
                ),
                expected(global_expected),
                "global {name}"
            );

            let parameter_for_by = parameter.clone();
            let by_query = match name {
                "quantile" => format!("quantile by (job) (0.5, {ANCHOR})"),
                "topk" => format!("topk by (job) (3, {ANCHOR})"),
                _ => format!("{name} by (job) ({ANCHOR})"),
            };
            assert_eq!(
                execute(
                    &engine,
                    by_query,
                    stage(
                        operator.clone(),
                        QueryTimeGroupingMode::By,
                        &["job"],
                        parameter_for_by,
                    ),
                ),
                expected(grouped_expected.clone()),
                "by(job) {name}"
            );

            let without_query = match name {
                "quantile" => format!("quantile without (instance) (0.5, {ANCHOR})"),
                "topk" => format!("topk without (instance) (3, {ANCHOR})"),
                _ => format!("{name} without (instance) ({ANCHOR})"),
            };
            let without_expected = if name == "topk" {
                grouped_expected
            } else {
                grouped_expected
                    .into_iter()
                    .map(|(labels, value)| {
                        (
                            vec![labels[0], if labels[0] == "api" { "east" } else { "west" }],
                            value,
                        )
                    })
                    .collect()
            };
            assert_eq!(
                execute(
                    &engine,
                    without_query,
                    stage(
                        operator,
                        QueryTimeGroupingMode::Without,
                        &["instance"],
                        parameter,
                    ),
                ),
                expected(without_expected),
                "without(instance) {name}"
            );
        }
    }

    #[test]
    fn topk_and_quantile_boundary_parameters_execute_through_the_public_handler() {
        let engine = engine();
        let topk = |k| QueryTimeAggregation {
            operator: QueryTimeAggregationOperator::Topk,
            grouping: QueryTimeGrouping {
                mode: QueryTimeGroupingMode::All,
                labels: Vec::new(),
            },
            parameter: Some(QueryTimeAggregationParameter::Integer(k)),
        };
        let quantile = |phi| QueryTimeAggregation {
            operator: QueryTimeAggregationOperator::Quantile,
            grouping: QueryTimeGrouping {
                mode: QueryTimeGroupingMode::All,
                labels: Vec::new(),
            },
            parameter: Some(QueryTimeAggregationParameter::Float(phi)),
        };

        assert_eq!(
            execute(&engine, format!("topk(1, {ANCHOR})"), topk(1)),
            expected(vec![(vec!["b", "worker", "west"], 7.0)])
        );
        assert_eq!(
            execute(&engine, format!("topk(10, {ANCHOR})"), topk(10)),
            expected(vec![
                (vec!["a", "api", "east"], 1.0),
                (vec!["a", "worker", "west"], 5.0),
                (vec!["b", "api", "east"], 3.0),
                (vec!["b", "worker", "west"], 7.0),
            ])
        );
        assert_eq!(
            execute(&engine, format!("quantile(0, {ANCHOR})"), quantile(0.0)),
            expected(vec![(vec![], 1.0)])
        );
        assert_eq!(
            execute(&engine, format!("quantile(1, {ANCHOR})"), quantile(1.0)),
            expected(vec![(vec![], 7.0)])
        );
    }

    #[test]
    fn grouping_by_a_missing_label_uses_prometheus_empty_label_value() {
        let engine = engine();

        assert_eq!(
            execute(
                &engine,
                format!("sum by (missing) ({ANCHOR})"),
                stage(
                    QueryTimeAggregationOperator::Sum,
                    QueryTimeGroupingMode::By,
                    &["missing"],
                    None,
                ),
            ),
            expected(vec![(vec![""], 16.0)])
        );
    }

    #[test]
    fn query_time_topk_breaks_ties_by_full_label_set() {
        let engine = create_engine_single_pop(
            METRIC,
            AggregationType::Sum,
            vec!["instance", "job", "region"],
            vec![
                (
                    Some(vec!["a".to_string(), "api".to_string(), "east".to_string()]),
                    Box::new(SumAccumulator::with_sum(7.0)),
                ),
                (
                    Some(vec!["b".to_string(), "api".to_string(), "east".to_string()]),
                    Box::new(SumAccumulator::with_sum(7.0)),
                ),
            ],
            ANCHOR,
        );
        let topk = stage(
            QueryTimeAggregationOperator::Topk,
            QueryTimeGroupingMode::All,
            &[],
            Some(QueryTimeAggregationParameter::Integer(1)),
        );

        assert_eq!(
            execute(&engine, format!("topk(1, {ANCHOR})"), topk),
            expected(vec![(vec!["a", "api", "east"], 7.0)])
        );
    }

    fn expected(values: Vec<(Vec<&str>, f64)>) -> Vec<(Vec<String>, f64)> {
        values
            .into_iter()
            .map(|(labels, value)| (labels.into_iter().map(str::to_string).collect(), value))
            .collect()
    }
}
