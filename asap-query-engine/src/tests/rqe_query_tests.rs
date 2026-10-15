//! RQE expression trees answered by `SimpleEngine::handle_rqe_query` and the
//! `POST /api/v1/rqe_query` route.

#[cfg(test)]
mod tests {
    use crate::data_model::{
        AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
        KeyByLabelValues, PrecomputedOutput, PromQLSchema, QueryConfig, QueryLanguage, RqeConfig,
        SchemaConfig, StreamingConfig, WindowType,
    };
    use crate::drivers::query::adapters::AdapterConfig;
    use crate::drivers::query::servers::http::{HttpServer, HttpServerConfig, RQE_QUERY_ENDPOINT};
    use crate::engines::{QueryResult, RqeQueryError, SimpleEngine};
    use crate::precompute_operators::{
        CountMinSketchWithHeapAccumulator, DDSketchAccumulator, HllAccumulator,
        MultipleSumAccumulator, SumAccumulator,
    };
    use crate::stores::simple_map_store::SimpleMapStore;
    use crate::stores::Store;
    use crate::AggregateCore;
    use asap_types::query_config::{
        QueryTimeAggregation, QueryTimeAggregationOperator, QueryTimeGrouping,
        QueryTimeGroupingMode,
    };
    use asap_types::rqe_expr::RqeExpr;
    use promql_utilities::data_model::KeyByLabelNames;
    use serde_json::{json, Value};
    use std::collections::HashMap;
    use std::sync::Arc;

    const WINDOW_MS: u64 = 1_000;
    const QUERY_TIME_S: f64 = 1_000.0;
    const QUERY_TIME_MS: u64 = 1_000_000;
    /// Two tumbling windows: ending one window before the query time, and at it.
    const TWO_WINDOWS_MS: u64 = 2 * WINDOW_MS;

    const SUM_AGG: u64 = 1;
    const DDSKETCH_AGG: u64 = 2;
    const HLL_AGG: u64 = 3;
    const TOPK_AGG: u64 = 4;
    const MULTI_SUM_AGG: u64 = 5;
    /// One heap per `job`, keyed by `srcip`: serves `topk by (job)`.
    const TOPK_BY_JOB_AGG: u64 = 6;
    const TOPK_BY_JOB_QUERY: &str = "topk by (job) (2, sum_over_time(flows[1s]))";

    type Row = (Vec<String>, f64);

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn aggregation(
        id: u64,
        aggregation_type: AggregationType,
        metric: &str,
        grouping: &[&str],
        aggregated: &[&str],
    ) -> (u64, AggregationConfig) {
        (
            id,
            AggregationConfig {
                aggregation_id: id,
                aggregation_type,
                aggregation_sub_type: String::new(),
                parameters: HashMap::new(),
                grouping_labels: KeyByLabelNames::new(strings(grouping)),
                aggregated_labels: KeyByLabelNames::new(strings(aggregated)),
                rollup_labels: KeyByLabelNames::empty(),
                original_yaml: String::new(),
                window_size_ms: WINDOW_MS,
                slide_interval_ms: WINDOW_MS,
                window_type: WindowType::Tumbling,
                spatial_filter: String::new(),
                spatial_filter_normalized: String::new(),
                metric: metric.to_string(),
                num_aggregates_to_retain: None,
                read_count_threshold: None,
                table_name: None,
                value_column: None,
            },
        )
    }

    fn sum_by_job() -> QueryTimeAggregation {
        QueryTimeAggregation {
            operator: QueryTimeAggregationOperator::Sum,
            grouping: QueryTimeGrouping {
                mode: QueryTimeGroupingMode::By,
                labels: strings(&["job"]),
            },
            parameter: None,
        }
    }

    fn rqe_config(aggregation_id: u64, stages: Vec<QueryTimeAggregation>) -> RqeConfig {
        RqeConfig {
            aggregations: vec![AggregationReference::new(aggregation_id, None)],
            query_time_aggregations: stages,
        }
    }

    fn query_config(query: &str, aggregation_id: u64) -> QueryConfig {
        QueryConfig::new(query.to_string())
            .add_aggregation(AggregationReference::new(aggregation_id, None))
    }

    /// DDSketch, not KLL: its merge is deterministic, so two reads of the
    /// same windows agree exactly.
    fn ddsketch(values: impl IntoIterator<Item = f64>) -> DDSketchAccumulator {
        let mut sketch = DDSketchAccumulator::new(0.01);
        values.into_iter().for_each(|value| sketch.update(value));
        sketch
    }

    fn hll(values: std::ops::Range<u32>) -> HllAccumulator {
        let mut sketch = HllAccumulator::with_default_precision();
        values.for_each(|value| sketch.update(f64::from(value)));
        sketch
    }

    fn multiple_sum(sums: &[(&[&str], f64)]) -> MultipleSumAccumulator {
        let mut accumulator = MultipleSumAccumulator::new();
        for (labels, sum) in sums {
            accumulator.add_sum(KeyByLabelValues::new_with_labels(strings(labels)), *sum);
        }
        accumulator
    }

    /// One engine holding every aggregation the tests read, each RQE id in
    /// `rqe_configs`, and the PromQL queries the equivalence tests compare
    /// against.
    fn engine() -> SimpleEngine {
        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs: HashMap::from([
                aggregation(
                    SUM_AGG,
                    AggregationType::Sum,
                    "http_requests",
                    &["instance", "job"],
                    &[],
                ),
                aggregation(
                    DDSKETCH_AGG,
                    AggregationType::DDSketch,
                    "latency",
                    &["job"],
                    &[],
                ),
                aggregation(HLL_AGG, AggregationType::HLL, "users", &["job"], &[]),
                aggregation(
                    TOPK_AGG,
                    AggregationType::CountMinSketchWithHeap,
                    "transfers",
                    &[],
                    &["srcip"],
                ),
                aggregation(
                    MULTI_SUM_AGG,
                    AggregationType::MultipleSum,
                    "events",
                    &[],
                    &["instance", "job"],
                ),
                aggregation(
                    TOPK_BY_JOB_AGG,
                    AggregationType::CountMinSketchWithHeap,
                    "flows",
                    &["job"],
                    &["srcip"],
                ),
            ]),
        });
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));
        let insert = |aggregation_id: u64,
                      end_ms: u64,
                      key: Option<&[&str]>,
                      accumulator: Box<dyn AggregateCore>| {
            let key = key.map(|labels| KeyByLabelValues::new_with_labels(strings(labels)));
            store
                .insert_precomputed_output(
                    PrecomputedOutput::new(end_ms - WINDOW_MS, end_ms, key, aggregation_id),
                    accumulator,
                )
                .unwrap();
        };
        let previous_window_ms = QUERY_TIME_MS - WINDOW_MS;

        // Sums: (a, api) 1 then 2, (b, api) 3 then 4, (a, web) 5 then 6.
        for (labels, earlier, latest) in [
            (["a", "api"], 1.0, 2.0),
            (["b", "api"], 3.0, 4.0),
            (["a", "web"], 5.0, 6.0),
        ] {
            insert(
                SUM_AGG,
                previous_window_ms,
                Some(&labels),
                Box::new(SumAccumulator::with_sum(earlier)),
            );
            insert(
                SUM_AGG,
                QUERY_TIME_MS,
                Some(&labels),
                Box::new(SumAccumulator::with_sum(latest)),
            );
        }

        for (job, offset) in [("api", 0.0), ("web", 1_000.0)] {
            insert(
                DDSKETCH_AGG,
                previous_window_ms,
                Some(&[job]),
                Box::new(ddsketch((0..500).map(|v| f64::from(v) + offset))),
            );
            insert(
                DDSKETCH_AGG,
                QUERY_TIME_MS,
                Some(&[job]),
                Box::new(ddsketch((500..1_000).map(|v| f64::from(v) + offset))),
            );
        }

        insert(
            HLL_AGG,
            QUERY_TIME_MS,
            Some(&["api"]),
            Box::new(hll(0..1_000)),
        );
        insert(HLL_AGG, QUERY_TIME_MS, Some(&["web"]), Box::new(hll(0..50)));

        let mut heap = CountMinSketchWithHeapAccumulator::new(3, 1024, 32);
        for i in 1..=5u64 {
            heap.inner.update(&format!("10.0.0.{i}"), (i * 10) as f64);
        }
        insert(TOPK_AGG, QUERY_TIME_MS, None, Box::new(heap));

        // `api`'s smallest sources outweigh `web`'s largest, so a global
        // ranking would keep only `api` rows.
        for (job, sums) in [("api", [100.0, 200.0, 300.0]), ("web", [10.0, 20.0, 30.0])] {
            let mut heap = CountMinSketchWithHeapAccumulator::new(3, 1024, 32);
            for (i, sum) in sums.into_iter().enumerate() {
                heap.inner.update(&format!("10.0.0.{}", i + 1), sum);
            }
            insert(TOPK_BY_JOB_AGG, QUERY_TIME_MS, Some(&[job]), Box::new(heap));
        }

        insert(
            MULTI_SUM_AGG,
            previous_window_ms,
            None,
            Box::new(multiple_sum(&[(&["a", "api"], 1.0), (&["b", "api"], 2.0)])),
        );
        insert(
            MULTI_SUM_AGG,
            QUERY_TIME_MS,
            None,
            Box::new(multiple_sum(&[(&["a", "api"], 4.0), (&["a", "web"], 8.0)])),
        );

        let schema = PromQLSchema::new()
            .add_metric(
                "http_requests".to_string(),
                KeyByLabelNames::new(strings(&["instance", "job"])),
            )
            .add_metric(
                "latency".to_string(),
                KeyByLabelNames::new(strings(&["job"])),
            )
            .add_metric("users".to_string(), KeyByLabelNames::new(strings(&["job"])))
            .add_metric(
                "transfers".to_string(),
                KeyByLabelNames::new(strings(&["srcip"])),
            )
            .add_metric(
                "events".to_string(),
                KeyByLabelNames::new(strings(&["instance", "job"])),
            )
            .add_metric(
                "flows".to_string(),
                KeyByLabelNames::new(strings(&["job", "srcip"])),
            );
        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(schema),
            query_configs: vec![
                query_config("sum_over_time(http_requests[2s])", SUM_AGG),
                query_config("sum by (job) (sum_over_time(http_requests[2s]))", SUM_AGG),
                query_config("quantile_over_time(0.9, latency[2s])", DDSKETCH_AGG),
                query_config("topk(3, sum_over_time(transfers[1s]))", TOPK_AGG),
                query_config(TOPK_BY_JOB_QUERY, TOPK_BY_JOB_AGG),
                QueryConfig::with_plan(
                    "sum by (job) (sum_over_time(events[2s]))".to_string(),
                    "sum by (instance, job) (sum_over_time(events[2s]))".to_string(),
                    vec![sum_by_job()],
                )
                .add_aggregation(AggregationReference::new(MULTI_SUM_AGG, None)),
            ],
            cleanup_policy: CleanupPolicy::NoCleanup,
            rqe_configs: HashMap::from([
                ("sum".to_string(), rqe_config(SUM_AGG, vec![])),
                ("sum_by_job".to_string(), rqe_config(SUM_AGG, vec![])),
                ("latest".to_string(), rqe_config(SUM_AGG, vec![])),
                ("p90".to_string(), rqe_config(DDSKETCH_AGG, vec![])),
                ("distinct".to_string(), rqe_config(HLL_AGG, vec![])),
                ("top3".to_string(), rqe_config(TOPK_AGG, vec![])),
                (
                    "top2_by_job".to_string(),
                    rqe_config(TOPK_BY_JOB_AGG, vec![]),
                ),
                (
                    "rolled_up".to_string(),
                    rqe_config(MULTI_SUM_AGG, vec![sum_by_job()]),
                ),
                (
                    // Rolls up to `job` though its leaves group by `instance`.
                    "misrolled".to_string(),
                    rqe_config(MULTI_SUM_AGG, vec![sum_by_job()]),
                ),
            ]),
        };
        SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            WINDOW_MS,
            QueryLanguage::promql,
        )
    }

    fn leaf(
        id: &str,
        metric: &str,
        capability: &str,
        lookback_ms: u64,
        grouping: &[&str],
    ) -> Value {
        json!({"rqe": {"id": id, "metric": metric, "capability": capability,
                       "lookback_ms": lookback_ms, "grouping": grouping}})
    }

    /// `sum_over_time(http_requests[2s])`, or its `sum by` when `grouping`
    /// drops a label.
    fn sum_leaf(id: &str, lookback_ms: u64, grouping: &[&str]) -> Value {
        leaf(id, "http_requests", "sum", lookback_ms, grouping)
    }

    fn series_sum() -> Value {
        sum_leaf("sum", TWO_WINDOWS_MS, &["instance", "job"])
    }

    fn latest_sum() -> Value {
        sum_leaf("latest", WINDOW_MS, &["instance", "job"])
    }

    fn quantile_leaf() -> Value {
        let mut quantile = leaf("p90", "latency", "quantile", TWO_WINDOWS_MS, &["job"]);
        quantile["rqe"]["phi"] = json!(0.9);
        quantile
    }

    fn topk_leaf() -> Value {
        let mut topk = leaf("top3", "transfers", "topk_by_value", WINDOW_MS, &["srcip"]);
        topk["rqe"]["k"] = json!(3);
        topk
    }

    /// `topk by (job) (2, sum_over_time(flows[1s]))`, or with other buckets.
    fn topk_by_job_leaf(topk_by: Option<&[&str]>) -> Value {
        let mut topk = leaf(
            "top2_by_job",
            "flows",
            "topk_by_value",
            WINDOW_MS,
            &["job", "srcip"],
        );
        topk["rqe"]["k"] = json!(2);
        if let Some(topk_by) = topk_by {
            topk["rqe"]["topk_by"] = json!(topk_by);
        }
        topk
    }

    fn expr(tree: Value) -> RqeExpr {
        serde_json::from_value(tree).expect("a well-formed RQE tree")
    }

    /// Label names and rows, sorted by label values.
    fn sorted(labels: KeyByLabelNames, result: QueryResult) -> (Vec<String>, Vec<Row>) {
        let QueryResult::Vector(vector) = result else {
            panic!("expected an instant vector");
        };
        let mut rows: Vec<Row> = vector
            .values
            .into_iter()
            .map(|element| (element.labels.labels, element.value))
            .collect();
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        (labels.labels, rows)
    }

    fn rqe(engine: &SimpleEngine, tree: Value) -> (Vec<String>, Vec<Row>) {
        let (labels, result) = engine
            .handle_rqe_query(&expr(tree), QUERY_TIME_S)
            .expect("the RQE query should be answered");
        sorted(labels, result)
    }

    fn promql(engine: &SimpleEngine, query: &str) -> (Vec<String>, Vec<Row>) {
        let (labels, result) = engine
            .handle_query_promql(query.to_string(), QUERY_TIME_S)
            .expect("the PromQL query should execute")
            .expect("the PromQL query should be served locally");
        sorted(labels, result)
    }

    fn rqe_error(engine: &SimpleEngine, tree: Value, time: f64) -> RqeQueryError {
        engine
            .handle_rqe_query(&expr(tree), time)
            .expect_err("the RQE query should be refused")
    }

    fn row(labels: &[&str], value: f64) -> Row {
        (strings(labels), value)
    }

    // --- leaves ---

    #[test]
    fn sum_leaf_sums_its_lookback_per_series_without_metric_name() {
        assert_eq!(
            rqe(&engine(), series_sum()),
            (
                strings(&["instance", "job"]),
                vec![
                    row(&["a", "api"], 3.0),
                    row(&["a", "web"], 11.0),
                    row(&["b", "api"], 7.0),
                ]
            )
        );
    }

    #[test]
    fn sum_leaf_equals_promql_sum_over_time() {
        let engine = engine();
        assert_eq!(
            rqe(&engine, series_sum()),
            promql(&engine, "sum_over_time(http_requests[2s])")
        );
    }

    #[test]
    fn grouped_sum_leaf_equals_promql_sum_by_of_sum_over_time() {
        let engine = engine();
        let answer = rqe(&engine, sum_leaf("sum_by_job", TWO_WINDOWS_MS, &["job"]));
        assert_eq!(
            answer,
            (
                strings(&["job"]),
                vec![row(&["api"], 10.0), row(&["web"], 11.0)]
            )
        );
        assert_eq!(
            answer,
            promql(&engine, "sum by (job) (sum_over_time(http_requests[2s]))")
        );
    }

    #[test]
    fn quantile_leaf_equals_promql_quantile_over_time() {
        let engine = engine();
        let answer = rqe(&engine, quantile_leaf());
        assert_eq!(answer.0, strings(&["job"]));
        assert_eq!(answer.1.len(), 2);
        for ((labels, value), expected) in answer.1.iter().zip([900.0, 1_900.0]) {
            assert!(
                (value - expected).abs() <= 1_000.0 * 0.02,
                "{labels:?}: p90 {value} should be near {expected}"
            );
        }
        assert_eq!(
            answer,
            promql(&engine, "quantile_over_time(0.9, latency[2s])")
        );
    }

    #[test]
    fn cardinality_leaf_estimates_distinct_values_per_group() {
        let answer = rqe(
            &engine(),
            leaf("distinct", "users", "cardinality", WINDOW_MS, &["job"]),
        );
        assert_eq!(
            answer,
            (
                strings(&["job"]),
                vec![
                    row(&["api"], hll(0..1_000).estimate()),
                    row(&["web"], hll(0..50).estimate()),
                ]
            )
        );
    }

    #[test]
    fn topk_leaf_ranks_globally_and_equals_promql_topk_of_sum_over_time() {
        let engine = engine();
        let (labels, result) = engine
            .handle_rqe_query(&expr(topk_leaf()), QUERY_TIME_S)
            .unwrap();
        let QueryResult::Vector(vector) = &result else {
            panic!("expected an instant vector");
        };
        let ranked: Vec<Row> = vector
            .values
            .iter()
            .map(|element| (element.labels.labels.clone(), element.value))
            .collect();
        assert_eq!(labels.labels, strings(&["srcip"]));
        assert_eq!(
            ranked,
            vec![
                row(&["10.0.0.5"], 50.0),
                row(&["10.0.0.4"], 40.0),
                row(&["10.0.0.3"], 30.0),
            ],
            "top-k rows come back ranked, highest first"
        );
        assert_eq!(
            sorted(labels, result),
            promql(&engine, "topk(3, sum_over_time(transfers[1s]))")
        );
    }

    #[test]
    fn topk_by_leaf_ranks_within_each_bucket_and_equals_promql_topk_by() {
        let engine = engine();
        let answer = rqe(&engine, topk_by_job_leaf(Some(&["job"])));
        assert_eq!(
            answer,
            (
                strings(&["job", "srcip"]),
                vec![
                    row(&["api", "10.0.0.2"], 200.0),
                    row(&["api", "10.0.0.3"], 300.0),
                    row(&["web", "10.0.0.2"], 20.0),
                    row(&["web", "10.0.0.3"], 30.0),
                ]
            ),
            "each job keeps its own top 2"
        );
        assert_eq!(answer, promql(&engine, TOPK_BY_JOB_QUERY));
    }

    /// The buckets are the aggregation's heaps: a leaf ranking other buckets
    /// than its aggregation keeps fails instead of answering another ranking.
    #[test]
    fn a_topk_leaf_whose_buckets_are_not_its_heaps_fails_loudly() {
        let engine = engine();
        for tree in [topk_by_job_leaf(None), topk_by_job_leaf(Some(&["srcip"]))] {
            let error = rqe_error(&engine, tree, QUERY_TIME_S);
            assert!(matches!(&error, RqeQueryError::Execution(_)), "{error:?}");
            assert!(error.to_string().contains("top2_by_job"), "{error}");
            assert!(error.to_string().contains("topk_by"), "{error}");
        }
        let mut global = topk_leaf();
        global["rqe"]["topk_by"] = json!(["srcip"]);
        let error = rqe_error(&engine, global, QUERY_TIME_S);
        assert!(error.to_string().contains("top3"), "{error}");
    }

    #[test]
    fn rolled_up_leaf_applies_its_query_time_stage_like_the_promql_plan() {
        let engine = engine();
        let answer = rqe(
            &engine,
            leaf("rolled_up", "events", "sum", TWO_WINDOWS_MS, &["job"]),
        );
        assert_eq!(
            answer,
            (
                strings(&["job"]),
                vec![row(&["api"], 7.0), row(&["web"], 8.0)]
            )
        );
        assert_eq!(
            answer,
            promql(&engine, "sum by (job) (sum_over_time(events[2s]))")
        );
    }

    #[test]
    fn a_roll_up_to_other_labels_than_the_leaf_fails_loudly() {
        let error = rqe_error(
            &engine(),
            leaf("misrolled", "events", "sum", TWO_WINDOWS_MS, &["instance"]),
            QUERY_TIME_S,
        );
        assert!(matches!(&error, RqeQueryError::Execution(_)), "{error:?}");
        assert!(error.to_string().contains("misrolled"), "{error}");
    }

    /// Between ingestion ticks a leaf reads what the PromQL instant path
    /// reads at the same time.
    #[test]
    fn misaligned_time_answers_like_the_promql_path() {
        let engine = engine();
        let time = QUERY_TIME_S + 0.4;
        let (labels, result) = engine.handle_rqe_query(&expr(series_sum()), time).unwrap();
        let promql_rows = engine
            .handle_query_promql("sum_over_time(http_requests[2s])".to_string(), time)
            .expect("the PromQL query should execute")
            .map(|(labels, result)| sorted(labels, result).1)
            .unwrap_or_default();
        assert_eq!(sorted(labels, result).1, promql_rows);
    }

    // --- agg ---

    #[test]
    fn agg_regroups_its_child_by_each_operator() {
        let engine = engine();
        for (op, api, web) in [
            ("avg", 5.0, 11.0),
            ("min", 3.0, 11.0),
            ("max", 7.0, 11.0),
            ("sum", 10.0, 11.0),
        ] {
            assert_eq!(
                rqe(
                    &engine,
                    json!({"agg": op, "by": ["job"], "of": series_sum()})
                ),
                (
                    strings(&["job"]),
                    vec![row(&["api"], api), row(&["web"], web)]
                ),
                "{op}"
            );
        }
    }

    #[test]
    fn agg_by_nothing_folds_every_series_into_one() {
        assert_eq!(
            rqe(
                &engine(),
                json!({"agg": "avg", "by": [], "of": series_sum()})
            ),
            (vec![], vec![row(&[], 7.0)])
        );
    }

    // --- binary ---

    #[test]
    fn binary_vector_vector_matches_series_by_labels() {
        let engine = engine();
        assert_eq!(
            rqe(
                &engine,
                json!({"op": "-", "lhs": series_sum(), "rhs": latest_sum()})
            ),
            (
                strings(&["instance", "job"]),
                vec![
                    row(&["a", "api"], 1.0),
                    row(&["a", "web"], 5.0),
                    row(&["b", "api"], 3.0),
                ]
            )
        );
        assert_eq!(
            rqe(
                &engine,
                json!({"op": "/", "lhs": series_sum(), "rhs": latest_sum()})
            )
            .1,
            vec![
                row(&["a", "api"], 1.5),
                row(&["a", "web"], 11.0 / 6.0),
                row(&["b", "api"], 7.0 / 4.0),
            ]
        );
    }

    #[test]
    fn binary_vector_scalar_keeps_operand_order() {
        let engine = engine();
        assert_eq!(
            rqe(
                &engine,
                json!({"op": "*", "lhs": latest_sum(), "rhs": {"scalar": 2.0}})
            )
            .1,
            vec![
                row(&["a", "api"], 4.0),
                row(&["a", "web"], 12.0),
                row(&["b", "api"], 8.0),
            ]
        );
        assert_eq!(
            rqe(
                &engine,
                json!({"op": "-", "lhs": {"scalar": 10.0}, "rhs": latest_sum()})
            ),
            (
                strings(&["instance", "job"]),
                vec![
                    row(&["a", "api"], 8.0),
                    row(&["a", "web"], 4.0),
                    row(&["b", "api"], 6.0),
                ]
            )
        );
    }

    #[test]
    fn nested_tree_aggregates_a_binary_of_leaves() {
        let tree = json!({"agg": "max", "by": ["job"], "of":
            {"op": "+", "lhs": series_sum(), "rhs": latest_sum()}});
        assert_eq!(
            rqe(&engine(), tree),
            (
                strings(&["job"]),
                vec![row(&["api"], 11.0), row(&["web"], 17.0)]
            )
        );
    }

    // --- refusals and empty windows ---

    #[test]
    fn refuses_an_invalid_tree() {
        let mut quantile = quantile_leaf();
        quantile["rqe"]["phi"] = json!(1.5);
        let error = rqe_error(&engine(), quantile, QUERY_TIME_S);
        assert!(matches!(error, RqeQueryError::BadRequest(_)), "{error:?}");
        assert!(error.to_string().contains("phi"), "{error}");
    }

    #[test]
    fn refuses_an_unknown_rqe_id_before_reading_anything() {
        let tree = json!({"op": "+", "lhs": series_sum(),
                          "rhs": sum_leaf("nope", WINDOW_MS, &["instance", "job"])});
        let error = rqe_error(&engine(), tree, QUERY_TIME_S);
        assert!(matches!(error, RqeQueryError::BadRequest(_)), "{error:?}");
        assert!(error.to_string().contains("\"nope\""), "{error}");
    }

    #[test]
    fn refuses_a_bad_time() {
        for time in [-1.0, f64::NAN, f64::INFINITY] {
            let error = rqe_error(&engine(), series_sum(), time);
            assert!(matches!(error, RqeQueryError::BadRequest(_)), "{time}");
        }
        // A lookback reaching before the epoch.
        let error = rqe_error(&engine(), series_sum(), 1.0);
        assert!(matches!(error, RqeQueryError::BadRequest(_)), "{error:?}");
    }

    #[test]
    fn refuses_trees_without_a_vector() {
        let engine = engine();
        let error = rqe_error(&engine, json!({"scalar": 1.0}), QUERY_TIME_S);
        assert!(matches!(error, RqeQueryError::BadRequest(_)), "{error:?}");
        let agg_of_scalar = json!({"op": "+",
            "lhs": {"agg": "sum", "by": [], "of": {"scalar": 1.0}},
            "rhs": {"agg": "sum", "by": [], "of": series_sum()}});
        let error = rqe_error(&engine, agg_of_scalar, QUERY_TIME_S);
        assert!(matches!(error, RqeQueryError::BadRequest(_)), "{error:?}");
    }

    #[test]
    fn a_window_without_data_yet_answers_empty() {
        let engine = engine();
        let later = QUERY_TIME_S + 3_600.0;
        for tree in [
            series_sum(),
            topk_leaf(),
            json!({"agg": "sum", "by": ["job"], "of": series_sum()}),
            json!({"op": "*", "lhs": series_sum(), "rhs": {"scalar": 2.0}}),
        ] {
            let parsed = expr(tree.clone());
            let (labels, result) = engine.handle_rqe_query(&parsed, later).unwrap();
            let (labels, rows) = sorted(labels, result);
            assert!(rows.is_empty(), "{tree}: {rows:?}");
            assert_eq!(labels, parsed.output_labels(), "{tree}");
        }
    }

    // --- HTTP route ---

    async fn server() -> (HttpServer, String) {
        let engine = Arc::new(engine());
        let store: Arc<dyn Store> = Arc::new(SimpleMapStore::new(
            Arc::new(StreamingConfig::default()),
            CleanupPolicy::NoCleanup,
        ));
        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            // Forwarding on, so a test would see any fallback attempt fail.
            adapter_config: AdapterConfig::prometheus_promql(
                "http://127.0.0.1:1".to_string(),
                true,
                30,
            ),
        };
        let server = HttpServer::new(config, engine, store, None);
        let port = server.start_test_server().await.unwrap();
        (
            server,
            format!("http://127.0.0.1:{port}{RQE_QUERY_ENDPOINT}"),
        )
    }

    async fn post(url: &str, body: String) -> (u16, Value) {
        let response = reqwest::Client::new()
            .post(url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        (status, response.json().await.unwrap())
    }

    #[tokio::test]
    async fn route_answers_prometheus_vector_json() {
        let (_server, url) = server().await;
        let body = json!({"expr": {"agg": "sum", "by": ["job"], "of": series_sum()},
                          "time": QUERY_TIME_S});
        let (status, response) = post(&url, body.to_string()).await;
        assert_eq!(status, 200);
        assert_eq!(response["status"], "success");
        assert_eq!(response["data"]["resultType"], "vector");
        let mut result = response["data"]["result"].as_array().unwrap().clone();
        result.sort_by_key(|series| series["metric"]["job"].as_str().unwrap().to_string());
        assert_eq!(
            result,
            vec![
                json!({"metric": {"job": "api"}, "value": [QUERY_TIME_S, "10"]}),
                json!({"metric": {"job": "web"}, "value": [QUERY_TIME_S, "11"]}),
            ]
        );
    }

    #[tokio::test]
    async fn route_answers_an_empty_window_with_an_empty_result() {
        let (_server, url) = server().await;
        let body = json!({"expr": series_sum(), "time": QUERY_TIME_S + 3_600.0});
        let (status, response) = post(&url, body.to_string()).await;
        assert_eq!(status, 200);
        assert_eq!(
            response,
            json!({"status": "success", "data": {"resultType": "vector", "result": []}})
        );
    }

    #[tokio::test]
    async fn route_refuses_bad_requests_with_400_bad_data() {
        let (_server, url) = server().await;
        let mut bad_quantile = quantile_leaf();
        bad_quantile["rqe"]["phi"] = json!(2.0);
        for (body, reason) in [
            ("not json".to_string(), "invalid RQE query body"),
            (
                json!({"expr": series_sum()}).to_string(),
                "missing field `time`",
            ),
            (
                json!({"expr": {"rqe": {"id": "sum"}}, "time": 1.0}).to_string(),
                "invalid RQE query body",
            ),
            (
                json!({"expr": series_sum(), "time": 1.0, "extra": 1}).to_string(),
                "unknown field `extra`",
            ),
            (
                json!({"expr": bad_quantile, "time": QUERY_TIME_S}).to_string(),
                "phi",
            ),
            (
                json!({"expr": sum_leaf("nope", WINDOW_MS, &["job"]), "time": QUERY_TIME_S})
                    .to_string(),
                "unknown RQE id",
            ),
        ] {
            let (status, response) = post(&url, body.clone()).await;
            assert_eq!(status, 400, "{body}");
            assert_eq!(response["status"], "error", "{body}");
            assert_eq!(response["errorType"], "bad_data", "{body}");
            let error = response["error"].as_str().unwrap();
            assert!(error.contains(reason), "{body}: {error}");
        }
    }

    #[tokio::test]
    async fn route_reports_an_execution_failure_as_500() {
        let (_server, url) = server().await;
        let body = json!({"expr": leaf("misrolled", "events", "sum", TWO_WINDOWS_MS, &["instance"]),
                          "time": QUERY_TIME_S});
        let (status, response) = post(&url, body.to_string()).await;
        assert_eq!(status, 500);
        assert_eq!(response["status"], "error");
        assert_eq!(response["errorType"], "internal");
    }
}
