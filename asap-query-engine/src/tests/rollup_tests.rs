//! Roll-ups: a query grouped by `Q` served from a single-subpopulation
//! aggregation grouped by `G ⊋ Q`, by merging the states of every `G`-group
//! that projects onto the same `Q`-group.

#[cfg(test)]
mod tests {
    use crate::data_model::{
        AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
        KeyByLabelValues, Measurement, PrecomputedOutput, QueryConfig, QueryLanguage, SchemaConfig,
        StreamingConfig, WindowType,
    };
    use crate::engines::query_result::QueryResult;
    use crate::engines::simple_engine::SimpleEngine;
    use crate::precompute_operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator;
    use crate::precompute_operators::ddsketch_accumulator::DDSketchAccumulator;
    use crate::precompute_operators::hll_accumulator::HllAccumulator;
    use crate::precompute_operators::increase_accumulator::IncreaseAccumulator;
    use crate::precompute_operators::min_max_accumulator::MinMaxAccumulator;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::stores::simple_map_store::SimpleMapStore;
    use crate::stores::Store;
    use crate::tests::test_utilities::config_builders::create_test_schema;
    use crate::tests::test_utilities::engine_factories::{
        create_engine_multi_timestamp_with_window, create_engine_single_pop, AccumulatorData,
    };
    use crate::AggregateCore;
    use promql_utilities::data_model::KeyByLabelNames;
    use std::collections::HashMap;
    use std::sync::Arc;

    const QUERY_TIME_S: f64 = 1_000.0;

    /// Store key values, ordered like the aggregation's grouping label names
    /// (which are kept sorted).
    fn key(labels: &[&str]) -> Option<Vec<String>> {
        Some(labels.iter().map(|label| label.to_string()).collect())
    }

    /// Instant rows as (labels, value), sorted by labels.
    fn instant_rows(engine: &SimpleEngine, query: &str) -> Vec<(Vec<String>, f64)> {
        let (_, result) = engine
            .handle_query_promql(query.to_string(), QUERY_TIME_S)
            .expect("query should execute")
            .expect("query should be served locally");
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

    fn sums(values: &[(&[&str], f64)]) -> AccumulatorData {
        values
            .iter()
            .map(|(labels, value)| {
                (
                    key(labels),
                    Box::new(SumAccumulator::with_sum(*value)) as Box<dyn AggregateCore>,
                )
            })
            .collect()
    }

    fn kll(values: impl IntoIterator<Item = f64>) -> DatasketchesKLLAccumulator {
        let mut sketch = DatasketchesKLLAccumulator::new(200);
        values.into_iter().for_each(|value| sketch.update(value));
        sketch
    }

    fn dd(values: impl IntoIterator<Item = f64>) -> DDSketchAccumulator {
        let mut sketch = DDSketchAccumulator::new(0.01);
        values.into_iter().for_each(|value| sketch.update(value));
        sketch
    }

    #[test]
    fn sum_by_subset_equals_sum_deployed_at_that_grouping() {
        let query = "sum by (service) (requests)";
        let fine = create_engine_single_pop(
            "requests",
            AggregationType::Sum,
            vec!["endpoint", "service"],
            sums(&[
                (&["/x", "a"], 1.0),
                (&["/y", "a"], 2.0),
                (&["/x", "b"], 4.0),
            ]),
            query,
        );
        let coarse = create_engine_single_pop(
            "requests",
            AggregationType::Sum,
            vec!["service"],
            sums(&[(&["a"], 3.0), (&["b"], 4.0)]),
            query,
        );

        let rows = instant_rows(&fine, query);
        assert_eq!(
            rows,
            vec![(vec!["a".to_string()], 3.0), (vec!["b".to_string()], 4.0)]
        );
        assert_eq!(rows, instant_rows(&coarse, query));
    }

    #[test]
    fn sum_without_grouping_rolls_every_group_into_one() {
        let query = "sum(requests)";
        let engine = create_engine_single_pop(
            "requests",
            AggregationType::Sum,
            vec!["endpoint", "service"],
            sums(&[
                (&["/x", "a"], 1.0),
                (&["/y", "a"], 2.0),
                (&["/x", "b"], 4.0),
            ]),
            query,
        );

        assert_eq!(instant_rows(&engine, query), vec![(vec![], 7.0)]);
    }

    /// Dropping a label from the middle of the grouping keeps the remaining
    /// values aligned with the output label names.
    #[test]
    fn roll_up_drops_a_middle_grouping_label() {
        let query = "sum by (az, service) (requests)";
        let engine = create_engine_single_pop(
            "requests",
            AggregationType::Sum,
            vec!["az", "endpoint", "service"],
            sums(&[
                (&["eu", "/x", "a"], 1.0),
                (&["eu", "/y", "a"], 2.0),
                (&["us", "/x", "a"], 4.0),
                (&["eu", "/x", "b"], 8.0),
            ]),
            query,
        );

        let (labels, _) = engine
            .handle_query_promql(query.to_string(), QUERY_TIME_S)
            .expect("query should execute")
            .expect("query should be served locally");
        assert_eq!(labels.labels, vec!["az".to_string(), "service".to_string()]);
        let row =
            |az: &str, service: &str, value| (vec![az.to_string(), service.to_string()], value);
        assert_eq!(
            instant_rows(&engine, query),
            vec![
                row("eu", "a", 3.0),
                row("eu", "b", 8.0),
                row("us", "a", 4.0)
            ]
        );
    }

    #[test]
    fn max_by_subset_is_the_max_over_the_subgroups() {
        let query = "max by (service) (latency)";
        let data: AccumulatorData = [
            (&["/x", "a"][..], 5.0),
            (&["/y", "a"][..], 9.0),
            (&["/x", "b"][..], 2.0),
        ]
        .into_iter()
        .map(|(labels, value)| {
            (
                key(labels),
                Box::new(MinMaxAccumulator::with_value(value, "max".to_string()).unwrap())
                    as Box<dyn AggregateCore>,
            )
        })
        .collect();
        let engine = create_engine_single_pop(
            "latency",
            AggregationType::MinMax,
            vec!["endpoint", "service"],
            data,
            query,
        );

        assert_eq!(
            instant_rows(&engine, query),
            vec![(vec!["a".to_string()], 9.0), (vec!["b".to_string()], 2.0)]
        );
    }

    /// A merged sketch answers for the union of its subgroups.
    #[test]
    fn kll_quantile_by_subset_is_within_rank_error_of_the_union() {
        let query = "quantile by (service) (0.5, latency)";
        let a_x: Vec<f64> = (0..500).map(f64::from).collect();
        let a_y: Vec<f64> = (500..1_000).map(f64::from).collect();
        let b_x: Vec<f64> = (0..100).map(|v| f64::from(v) * 10.0).collect();
        let data: AccumulatorData = vec![
            (key(&["/x", "a"]), Box::new(kll(a_x.clone()))),
            (key(&["/y", "a"]), Box::new(kll(a_y.clone()))),
            (key(&["/x", "b"]), Box::new(kll(b_x.clone()))),
        ];
        let engine = create_engine_single_pop(
            "latency",
            AggregationType::DatasketchesKLL,
            vec!["endpoint", "service"],
            data,
            query,
        );

        // KLL compaction is randomized, so compare against the true median
        // within its rank error rather than against a second merge.
        let rows = instant_rows(&engine, query);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, vec!["a".to_string()]);
        assert!(
            (rows[0].1 - 500.0).abs() <= 1_000.0 * 0.02,
            "median of 0..1000 should be ~500, got {}",
            rows[0].1
        );
        // Below k items a KLL is exact.
        assert_eq!(rows[1], (vec!["b".to_string()], kll(b_x).get_quantile(0.5)));
    }

    #[test]
    fn ddsketch_quantile_by_subset_equals_the_merged_sketch() {
        let query = "quantile by (service) (0.9, latency)";
        let a_x: Vec<f64> = (1..=500).map(f64::from).collect();
        let a_y: Vec<f64> = (501..=1_000).map(f64::from).collect();
        let data: AccumulatorData = vec![
            (key(&["/x", "a"]), Box::new(dd(a_x.clone()))),
            (key(&["/y", "a"]), Box::new(dd(a_y.clone()))),
        ];
        let engine = create_engine_single_pop(
            "latency",
            AggregationType::DDSketch,
            vec!["endpoint", "service"],
            data,
            query,
        );

        let rows = instant_rows(&engine, query);
        let expected = dd(a_x.into_iter().chain(a_y)).get_quantile(0.9);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, vec!["a".to_string()]);
        assert!(
            (rows[0].1 - expected).abs() <= expected * 1e-9,
            "merged DDSketch p90 {} should equal the whole-data sketch's {expected}",
            rows[0].1
        );
        assert!((rows[0].1 - 900.0).abs() <= 900.0 * 0.02);
    }

    /// HLL merges by register max, so the rolled-up sketch is exactly the
    /// sketch of the union. HLL is reached through SQL `COUNT(DISTINCT)`.
    #[test]
    fn hll_count_distinct_by_subset_equals_the_union_sketch() {
        let sql = "SELECT COUNT(DISTINCT value) FROM events WHERE time BETWEEN \
                   DATEADD(s, -1, NOW()) AND NOW() GROUP BY service";
        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs: HashMap::from([(
                1,
                AggregationConfig {
                    aggregation_id: 1,
                    aggregation_type: AggregationType::HLL,
                    aggregation_sub_type: String::new(),
                    parameters: HashMap::new(),
                    grouping_labels: KeyByLabelNames::new(vec![
                        "endpoint".to_string(),
                        "service".to_string(),
                    ]),
                    aggregated_labels: KeyByLabelNames::empty(),
                    rollup_labels: KeyByLabelNames::empty(),
                    original_yaml: String::new(),
                    window_size_ms: 1_000,
                    slide_interval_ms: 1_000,
                    window_type: WindowType::Tumbling,
                    spatial_filter: String::new(),
                    spatial_filter_normalized: String::new(),
                    metric: "events".to_string(),
                    num_aggregates_to_retain: None,
                    read_count_threshold: None,
                    table_name: Some("events".to_string()),
                    value_column: Some("value".to_string()),
                },
            )]),
        });
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));
        let hll = |values: std::ops::Range<u32>| {
            let mut sketch = HllAccumulator::with_default_precision();
            values.for_each(|value| sketch.update(f64::from(value)));
            sketch
        };
        let query_time_ms = (QUERY_TIME_S * 1_000.0) as u64;
        for (labels, values) in [(["/x", "a"], 0..1_000), (["/y", "a"], 500..2_000)] {
            store
                .insert_precomputed_output(
                    PrecomputedOutput::new(
                        query_time_ms - 1_000,
                        query_time_ms,
                        key(&labels).map(|labels| KeyByLabelValues { labels }),
                        1,
                    ),
                    Box::new(hll(values)),
                )
                .unwrap();
        }
        let inference_config = InferenceConfig {
            schema: SchemaConfig::SQL(create_test_schema(
                "events",
                "time",
                "value",
                vec!["endpoint", "service"],
            )),
            query_configs: vec![QueryConfig::new(sql.to_string())
                .add_aggregation(AggregationReference::new(1, None))],
            cleanup_policy: CleanupPolicy::NoCleanup,
            rqe_configs: Default::default(),
        };
        let engine = SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            1_000,
            QueryLanguage::sql,
        );

        let (_, result) = engine
            .handle_query_sql(sql.to_string(), QUERY_TIME_S)
            .expect("the SQL roll-up should be served locally");
        let QueryResult::Vector(vector) = result else {
            panic!("expected an instant vector");
        };
        assert_eq!(vector.values.len(), 1);
        assert_eq!(vector.values[0].labels.labels, vec!["a".to_string()]);
        assert_eq!(vector.values[0].value, hll(0..2_000).estimate());
    }

    /// A SQL quantile served by rolling up a DDSketch grouped by
    /// (endpoint, service), over a lookback of several tumbling windows: the
    /// shape `--planner saved` writes for sketch-bench's p99 CUBE.
    #[test]
    fn sql_quantile_by_subset_over_several_windows_equals_the_merged_sketch() {
        const WINDOW_MS: u64 = 60_000;
        const WINDOWS: u64 = 5;
        // A multiple of the window, as the e2e client floors eval times.
        const ALIGNED_QUERY_TIME_S: f64 = 960.0;
        let sql = "SELECT service, QUANTILE(0.9, value) FROM events WHERE time BETWEEN \
                   DATEADD(s, -300, NOW()) AND NOW() GROUP BY service";
        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs: HashMap::from([(
                1,
                AggregationConfig {
                    aggregation_id: 1,
                    aggregation_type: AggregationType::DDSketch,
                    aggregation_sub_type: String::new(),
                    parameters: HashMap::new(),
                    grouping_labels: KeyByLabelNames::new(vec![
                        "endpoint".to_string(),
                        "service".to_string(),
                    ]),
                    aggregated_labels: KeyByLabelNames::empty(),
                    rollup_labels: KeyByLabelNames::empty(),
                    original_yaml: String::new(),
                    window_size_ms: WINDOW_MS,
                    slide_interval_ms: WINDOW_MS,
                    window_type: WindowType::Tumbling,
                    spatial_filter: String::new(),
                    spatial_filter_normalized: String::new(),
                    metric: "events".to_string(),
                    num_aggregates_to_retain: None,
                    read_count_threshold: None,
                    table_name: Some("events".to_string()),
                    value_column: Some("value".to_string()),
                },
            )]),
        });
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));
        let query_time_ms = (ALIGNED_QUERY_TIME_S * 1_000.0) as u64;
        let mut all = Vec::new();
        for (g, labels) in [["/x", "a"], ["/y", "a"]].iter().enumerate() {
            for w in 0..WINDOWS {
                let values: Vec<f64> = (1..=100)
                    .map(|i| f64::from(i + 100 * (w as u32 * 2 + g as u32)))
                    .collect();
                all.extend(values.iter().copied());
                let end = query_time_ms - w * WINDOW_MS;
                store
                    .insert_precomputed_output(
                        PrecomputedOutput::new(
                            end - WINDOW_MS,
                            end,
                            key(labels).map(|labels| KeyByLabelValues { labels }),
                            1,
                        ),
                        Box::new(dd(values)),
                    )
                    .unwrap();
            }
        }
        let inference_config = InferenceConfig {
            schema: SchemaConfig::SQL(create_test_schema(
                "events",
                "time",
                "value",
                vec!["endpoint", "service"],
            )),
            query_configs: vec![QueryConfig::new(sql.to_string())
                .add_aggregation(AggregationReference::new(1, None))],
            cleanup_policy: CleanupPolicy::NoCleanup,
            rqe_configs: Default::default(),
        };
        let engine = SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            1_000,
            QueryLanguage::sql,
        );

        let (_, result) = engine
            .handle_query_sql(sql.to_string(), ALIGNED_QUERY_TIME_S)
            .expect("the SQL roll-up should be served locally");
        let QueryResult::Vector(vector) = result else {
            panic!("expected an instant vector");
        };
        let expected = dd(all).get_quantile(0.9);
        assert_eq!(vector.values.len(), 1);
        assert_eq!(vector.values[0].labels.labels, vec!["a".to_string()]);
        assert!(
            (vector.values[0].value - expected).abs() <= expected * 1e-9,
            "rolled-up p90 {} should equal the whole-data sketch's {expected}",
            vector.values[0].value
        );
    }

    /// A range query rolls up at every step, and a subgroup that only
    /// appears partway through contributes from then on.
    #[test]
    fn range_sum_by_subset_rolls_up_each_step() {
        let query = "sum by (service) (requests)";
        let mut data = Vec::new();
        for second in 1..=4u64 {
            data.push((
                second * 1_000,
                key(&["/x", "a"]),
                Box::new(SumAccumulator::with_sum(1.0)) as Box<dyn AggregateCore>,
            ));
            if second >= 3 {
                data.push((
                    second * 1_000,
                    key(&["/new", "a"]),
                    Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
                ));
            }
        }
        let engine = create_engine_multi_timestamp_with_window(
            "requests",
            AggregationType::Sum,
            vec!["endpoint", "service"],
            data,
            query,
            1_000,
            1_000,
            WindowType::Tumbling,
        );

        let (_, result) = engine
            .handle_range_query_promql(query.to_string(), 2.0, 4.0, 1.0)
            .expect("query should execute")
            .expect("query should be served locally");
        let QueryResult::Matrix(matrix) = result else {
            panic!("expected a range vector");
        };
        assert_eq!(matrix.values.len(), 1);
        assert_eq!(matrix.values[0].labels.labels, vec!["a".to_string()]);
        let samples: Vec<(u64, f64)> = matrix.values[0]
            .samples
            .iter()
            .map(|sample| (sample.timestamp, sample.value))
            .collect();
        assert_eq!(samples, vec![(2_000, 1.0), (3_000, 11.0), (4_000, 11.0)]);
    }

    /// A Sliding step is complete when every cover window holds data from
    /// some subgroup. A subgroup with no samples in one cover window still
    /// contributes its other windows.
    #[test]
    fn sliding_range_roll_up_keeps_a_subgroup_missing_an_early_cover_window() {
        let query = "sum(sum_over_time(requests[4s])) by (service)";
        let mut data = Vec::new();
        for second in 1..=8u64 {
            data.push((
                second * 1_000,
                key(&["/x", "a"]),
                Box::new(SumAccumulator::with_sum(1.0)) as Box<dyn AggregateCore>,
            ));
            if second >= 5 {
                data.push((
                    second * 1_000,
                    key(&["/new", "a"]),
                    Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn AggregateCore>,
                ));
            }
        }
        let engine = create_engine_multi_timestamp_with_window(
            "requests",
            AggregationType::Sum,
            vec!["endpoint", "service"],
            data,
            query,
            2_000,
            1_000,
            WindowType::Sliding,
        );

        let (_, result) = engine
            .handle_range_query_promql(query.to_string(), 6.0, 8.0, 2.0)
            .expect("query should execute")
            .expect("query should be served locally");
        let QueryResult::Matrix(matrix) = result else {
            panic!("expected a range vector");
        };
        let samples: Vec<(u64, f64)> = matrix.values[0]
            .samples
            .iter()
            .map(|sample| (sample.timestamp, sample.value))
            .collect();
        // At 6s, /new has no samples in [2s, 4s) but 20 in [4s, 6s).
        assert_eq!(samples, vec![(6_000, 24.0), (8_000, 44.0)]);
    }

    #[test]
    fn grouping_outside_the_aggregations_labels_is_an_error() {
        let query = "sum by (endpoint) (requests)";
        let engine = create_engine_single_pop(
            "requests",
            AggregationType::Sum,
            vec!["service"],
            sums(&[(&["a"], 1.0)]),
            query,
        );

        let error = engine
            .handle_query_promql(query.to_string(), QUERY_TIME_S)
            .expect_err("a grouping the aggregation lacks must not be answered");
        assert!(error.to_string().contains("can't serve"), "{error}");
    }

    /// Increase merges pieces of one counter in time, so summing two
    /// series' counters by merging them would be wrong.
    #[test]
    fn roll_up_of_a_type_not_mergeable_across_groups_is_an_error() {
        let query = "sum by (service) (requests)";
        let data: AccumulatorData = vec![(
            key(&["/x", "a"]),
            Box::new(IncreaseAccumulator::new(
                Measurement::new(1.0),
                0,
                Measurement::new(5.0),
                1_000,
            )) as Box<dyn AggregateCore>,
        )];
        let engine = create_engine_single_pop(
            "requests",
            AggregationType::Increase,
            vec!["endpoint", "service"],
            data,
            query,
        );

        let error = engine
            .handle_query_promql(query.to_string(), QUERY_TIME_S)
            .expect_err("an Increase roll-up must not be answered");
        assert!(
            error.to_string().contains("can't be merged across groups"),
            "{error}"
        );
    }
}
