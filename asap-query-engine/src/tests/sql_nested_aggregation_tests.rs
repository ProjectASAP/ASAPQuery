//! Two-layer SQL queries: the inner layer is served from a precomputed
//! aggregation and the outer layer runs as a query-time aggregation over it.

#[cfg(test)]
mod tests {
    use crate::data_model::{
        AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
        KeyByLabelValues, PrecomputedOutput, QueryConfig, QueryLanguage, SchemaConfig,
        StreamingConfig, WindowType,
    };
    use crate::engines::query_result::QueryResult;
    use crate::engines::simple_engine::SimpleEngine;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::stores::simple_map_store::SimpleMapStore;
    use crate::stores::Store;
    use crate::tests::test_utilities::config_builders::create_test_schema;
    use asap_types::query_config::{
        QueryTimeAggregation, QueryTimeAggregationOperator, QueryTimeAggregationParameter,
        QueryTimeGrouping, QueryTimeGroupingMode,
    };
    use promql_utilities::data_model::KeyByLabelNames;
    use std::collections::HashMap;
    use std::sync::Arc;

    const QUERY_TIME_S: f64 = 1_000.0;
    const WINDOW_MS: u64 = 1_000;
    const INNER: &str = "SELECT job, instance, SUM(value) AS v FROM data \
                         WHERE time BETWEEN DATEADD(s, -1, NOW()) AND NOW() GROUP BY job, instance";

    fn by_job() -> QueryTimeGrouping {
        QueryTimeGrouping {
            mode: QueryTimeGroupingMode::By,
            labels: vec!["job".to_string()],
        }
    }

    fn stage(
        operator: QueryTimeAggregationOperator,
        grouping: QueryTimeGrouping,
        parameter: Option<QueryTimeAggregationParameter>,
    ) -> QueryTimeAggregation {
        QueryTimeAggregation {
            operator,
            grouping,
            parameter,
        }
    }

    /// An engine whose only aggregation is a per-(instance, job) sum holding
    /// (i1, a) = 1, (i2, a) = 2, (i1, b) = 4, planned for `config`.
    fn engine(config: QueryConfig) -> SimpleEngine {
        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs: HashMap::from([(
                1,
                AggregationConfig {
                    aggregation_id: 1,
                    aggregation_type: AggregationType::Sum,
                    aggregation_sub_type: String::new(),
                    parameters: HashMap::new(),
                    grouping_labels: KeyByLabelNames::new(vec![
                        "instance".to_string(),
                        "job".to_string(),
                    ]),
                    aggregated_labels: KeyByLabelNames::empty(),
                    rollup_labels: KeyByLabelNames::empty(),
                    original_yaml: String::new(),
                    window_size_ms: WINDOW_MS,
                    slide_interval_ms: WINDOW_MS,
                    window_type: WindowType::Tumbling,
                    spatial_filter: String::new(),
                    spatial_filter_normalized: String::new(),
                    metric: "data".to_string(),
                    num_aggregates_to_retain: None,
                    read_count_threshold: None,
                    table_name: Some("data".to_string()),
                    value_column: Some("value".to_string()),
                },
            )]),
        });
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));
        let query_time_ms = (QUERY_TIME_S * 1_000.0) as u64;
        for (instance, job, value) in [("i1", "a", 1.0), ("i2", "a", 2.0), ("i1", "b", 4.0)] {
            store
                .insert_precomputed_output(
                    PrecomputedOutput::new(
                        query_time_ms - WINDOW_MS,
                        query_time_ms,
                        Some(KeyByLabelValues {
                            labels: vec![instance.to_string(), job.to_string()],
                        }),
                        1,
                    ),
                    Box::new(SumAccumulator::with_sum(value)),
                )
                .unwrap();
        }
        let inference_config = InferenceConfig {
            schema: SchemaConfig::SQL(create_test_schema(
                "data",
                "time",
                "value",
                vec!["job", "instance"],
            )),
            query_configs: vec![config.add_aggregation(AggregationReference::new(1, None))],
            cleanup_policy: CleanupPolicy::NoCleanup,
            rqe_configs: Default::default(),
        };
        SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            WINDOW_MS,
            QueryLanguage::sql,
        )
    }

    /// `query` planned with `stages` over `INNER`, executed; rows as
    /// (labels, value) sorted by labels.
    fn rows(query: &str, stages: Vec<QueryTimeAggregation>) -> Vec<(Vec<String>, f64)> {
        let engine = engine(QueryConfig::with_plan(
            query.to_string(),
            INNER.to_string(),
            stages,
        ));
        let (_, result) = engine
            .handle_query_sql(query.to_string(), QUERY_TIME_S)
            .expect("nested query should be served locally");
        let QueryResult::Vector(vector) = result else {
            panic!("expected an instant vector");
        };
        let mut rows: Vec<_> = vector
            .values
            .into_iter()
            .map(|element| (element.labels.labels, element.value))
            .collect();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        rows
    }

    fn job_rows(a: f64, b: f64) -> Vec<(Vec<String>, f64)> {
        vec![(vec!["a".to_string()], a), (vec!["b".to_string()], b)]
    }

    #[test]
    fn outer_sum_aggregates_the_inner_rows_by_job() {
        let query = format!("SELECT job, SUM(v) FROM ({INNER}) GROUP BY job");
        assert_eq!(
            rows(
                &query,
                vec![stage(QueryTimeAggregationOperator::Sum, by_job(), None)]
            ),
            job_rows(3.0, 4.0)
        );
    }

    #[test]
    fn outer_quantile_aggregates_the_inner_rows_by_job() {
        let query = format!("SELECT job, QUANTILE(0.5, v) FROM ({INNER}) GROUP BY job");
        assert_eq!(
            rows(
                &query,
                vec![stage(
                    QueryTimeAggregationOperator::Quantile,
                    by_job(),
                    Some(QueryTimeAggregationParameter::Float(0.5)),
                )]
            ),
            job_rows(1.5, 4.0)
        );
    }

    #[test]
    fn outer_aggregation_without_group_by_aggregates_every_inner_row() {
        let query = format!("SELECT SUM(v) FROM ({INNER})");
        let all = QueryTimeGrouping {
            mode: QueryTimeGroupingMode::All,
            labels: Vec::new(),
        };
        assert_eq!(
            rows(
                &query,
                vec![stage(QueryTimeAggregationOperator::Sum, all, None)]
            ),
            vec![(Vec::new(), 7.0)]
        );
    }

    /// A nested query whose config carries no query-time stages must not be
    /// served from the inner layer alone: that would silently drop the outer
    /// aggregation.
    #[test]
    fn nested_query_planned_without_stages_is_not_served() {
        let query = format!("SELECT job, SUM(v) FROM ({INNER}) GROUP BY job");
        let engine = engine(QueryConfig::new(query.clone()));
        assert!(engine.handle_query_sql(query, QUERY_TIME_S).is_none());
    }
}
