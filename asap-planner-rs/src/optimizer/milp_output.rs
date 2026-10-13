//! Turns a solved MILP plan into the planner's streaming and inference YAML.

use std::collections::HashMap;

use asap_types::enums::{CleanupPolicy, WindowType};
use indexmap::map::Entry;
use indexmap::IndexMap;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::AggregationType;
use rqe_optimizer::milp::MilpSolution;
use rqe_optimizer::{Capability, Deployment};
use serde_json::Value;
use thiserror::Error;

use crate::error::ControllerError;
use crate::generator::{GeneratorOutput, QueryPlanEntry};
use crate::planner::agg_config::IntermediateAggConfig;
use crate::planner::labels::set_subpopulation_labels;
use crate::planner_output::PlannerOutput;
use crate::promql::generator::{build_inference_yaml, build_streaming_yaml};
use crate::sql::generator::{build_sql_inference_yaml, build_sql_streaming_yaml};

use super::aqe_extractor::contains_avg;
use super::milp::{MilpConfig, MilpWorkload};
use super::solution::OptimizerItem;

#[derive(Debug, Error)]
pub enum MilpOutputError {
    #[error("query {0:?} uses avg, which the engine can't answer from sum and count yet")]
    AvgQuery(String),
    #[error(
        "query groups set step_ms or range_duration_ms, whose retention MILP configs \
         don't set yet (#800): {0:?}"
    )]
    RangeQueryOverrides(Vec<String>),
    #[error("sketch-bench variant {variant} ({capability:?}) has no ASAPQuery aggregation type")]
    UndeployableVariant {
        variant: String,
        capability: Capability,
    },
    #[error("sketch-bench variant {variant}: sketch_config has no numeric {param:?}")]
    MissingParam {
        variant: String,
        param: &'static str,
    },
    #[error("sketch-bench variant dd: alpha must be finite and in (0, 1), got {alpha}")]
    InvalidDdsAlpha { alpha: f64 },
    #[error("query {0:?} is served by two deployments; a query string can name only one")]
    QueryOnTwoDeployments(String),
    #[error(
        "query {query:?} is grouped by {raqe:?} but served by a deployment grouped by \
         {deployment:?}: a roll-up, which the engine can't read yet"
    )]
    RollupUnsupported {
        query: String,
        raqe: Vec<String>,
        deployment: Vec<String>,
    },
    #[error(transparent)]
    Generator(#[from] ControllerError),
}

/// Errors on queries whose plan can be costed but not written as configs:
/// PromQL avg queries, and range-query overrides, which only size retention.
pub fn reject_unwritable_queries(config: MilpConfig) -> Result<(), MilpOutputError> {
    let MilpConfig::Promql(config) = config else {
        return Ok(());
    };
    if let Some(query) = config
        .query_groups
        .iter()
        .flat_map(|group| &group.queries)
        .find(|query| contains_avg(query))
    {
        return Err(MilpOutputError::AvgQuery(query.clone()));
    }
    let range_queries: Vec<String> = config
        .query_groups
        .iter()
        .filter(|qg| qg.step_ms.is_some() || qg.range_duration_ms.is_some())
        .flat_map(|qg| qg.queries.iter().cloned())
        .collect();
    if !range_queries.is_empty() {
        return Err(MilpOutputError::RangeQueryOverrides(range_queries));
    }
    Ok(())
}

/// Streaming and inference YAML for `solution`. Aggregation ids follow the
/// plan's deployment order, starting at 1. No cleanup policy yet, so the
/// retained instance counts are not emitted.
pub fn plan_to_planner_output(
    config: MilpConfig,
    workload: &MilpWorkload,
    solution: &MilpSolution,
) -> Result<PlannerOutput, MilpOutputError> {
    let deployments: Vec<Deployment> = solution
        .deployments
        .iter()
        .map(|planned| planned.deployment.clone())
        .collect();
    let served_by: Vec<usize> = solution.raqes.iter().map(|r| r.deployment).collect();
    deployments_to_planner_output(config, workload, &deployments, &served_by)
}

/// [`plan_to_planner_output`] for a plan given as its deployments and, per
/// Raqe of `workload`, the index of the deployment serving it.
pub fn deployments_to_planner_output(
    config: MilpConfig,
    workload: &MilpWorkload,
    deployments: &[Deployment],
    served_by: &[usize],
) -> Result<PlannerOutput, MilpOutputError> {
    assert_eq!(
        served_by.len(),
        workload.raqes.len(),
        "one deployment per Raqe"
    );
    reject_unwritable_queries(config)?;
    // sketch-bench lets a deployment serve a RAQE grouped by a subset of its
    // grouping (a roll-up, sketch-bench#190). The engine reads a deployment
    // only at its own grouping, and `aggregation_config` takes the labels
    // from the first RAQE served, so refuse a roll-up rather than write
    // configs at the wrong grouping.
    for (raqe, &index) in workload.raqes.iter().zip(served_by) {
        let deployment = &deployments[index];
        if raqe.grouping_labels != deployment.grouping_labels {
            return Err(MilpOutputError::RollupUnsupported {
                query: raqe.id.clone(),
                raqe: raqe.grouping_labels.iter().cloned().collect(),
                deployment: deployment.grouping_labels.iter().cloned().collect(),
            });
        }
    }

    // A SQL aggregation names its table and value column, and its metric is
    // the table.
    let sql_columns: HashMap<String, (&str, &str)> = config
        .sql_columns()
        .into_iter()
        .map(|(metric, table, column)| (metric, (table.name.as_str(), column)))
        .collect();

    // One aggregation per (deployment, labels its queries need). A Raqe
    // doesn't carry a keyed sketch's key labels (a topk-of-sum heap's inner
    // `sum by` labels), so the solver may give one deployment to queries
    // needing different keys; those get one aggregation each.
    // ponytail: the plan then prices one sketch where several are deployed;
    // key labels in the Raqe would let the solver see them.
    let mut aggregations: IndexMap<String, IntermediateAggConfig> = IndexMap::new();
    let mut item_keys: HashMap<(usize, usize), String> = HashMap::new();
    for (raqe, &index) in served_by.iter().enumerate() {
        let item = workload.raqe_items[raqe];
        if item_keys.contains_key(&(index, item)) {
            continue;
        }
        let mut aggregation = aggregation_config(&deployments[index], &workload.items[item])?;
        if let Some(&(table, column)) = sql_columns.get(&aggregation.metric) {
            aggregation.metric = table.to_string();
            aggregation.table_name = Some(table.to_string());
            aggregation.value_column = Some(column.to_string());
        }
        let key = aggregation.identifying_key();
        aggregations.entry(key.clone()).or_insert(aggregation);
        item_keys.insert((index, item), key);
    }

    let mut queries: IndexMap<String, QueryPlanEntry> = IndexMap::new();
    for (raqe, &index) in served_by.iter().enumerate() {
        let item = workload.raqe_items[raqe];
        let key = &item_keys[&(index, item)];
        for query in &workload.items[item].query_strings {
            match queries.entry(query.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(QueryPlanEntry::fully_planned(
                        query.clone(),
                        vec![(key.clone(), None)],
                    ));
                }
                Entry::Occupied(entry) if entry.get().aggregation_keys[0].0 == *key => {}
                Entry::Occupied(_) => {
                    return Err(MilpOutputError::QueryOnTwoDeployments(query.clone()))
                }
            }
        }
    }

    let id_map: HashMap<String, u32> = aggregations
        .keys()
        .enumerate()
        .map(|(index, key)| (key.clone(), index as u32 + 1))
        .collect();
    let (streaming_yaml, inference_yaml) = match config {
        MilpConfig::Promql(config) => {
            let schema = config.schema_from_hints();
            (
                build_streaming_yaml(&aggregations, &id_map, &schema)?,
                build_inference_yaml(CleanupPolicy::NoCleanup, &queries, &id_map, &schema)?,
            )
        }
        MilpConfig::Sql(config) => (
            build_sql_streaming_yaml(config, &aggregations, &id_map)?,
            build_sql_inference_yaml(config, CleanupPolicy::NoCleanup, &queries, &id_map)?,
        ),
    };
    Ok(PlannerOutput::from_output(GeneratorOutput {
        punted_queries: Vec::new(),
        streaming_yaml,
        inference_yaml,
        aggregation_count: aggregations.len(),
        query_count: queries.len(),
    }))
}

/// The aggregation `deployment` runs for `item`, one of the optimizer items
/// it serves. Items share the deployment's capability and grouping
/// (`plan_to_planner_output` refuses roll-ups); `item` decides the labels.
fn aggregation_config(
    deployment: &Deployment,
    item: &OptimizerItem,
) -> Result<IntermediateAggConfig, MilpOutputError> {
    let variant = deployment.config.sketch.as_str();
    let integer_param = |name: &'static str| {
        deployment.config.sketch_config["params"][name]
            .as_u64()
            .ok_or_else(|| missing_param(variant, name))
    };
    let float_param = |name: &'static str| {
        deployment.config.sketch_config["params"][name]
            .as_f64()
            .ok_or_else(|| missing_param(variant, name))
    };
    let (aggregation_type, sub_type, parameters) = match (variant, deployment.capability) {
        ("exact-sum", Capability::Sum) => (AggregationType::MultipleSum, "sum", vec![]),
        ("exact-sum", Capability::Count) => (AggregationType::MultipleSum, "count", vec![]),
        ("exact-min", Capability::Min) => (AggregationType::MultipleMinMax, "min", vec![]),
        ("exact-max", Capability::Max) => (AggregationType::MultipleMinMax, "max", vec![]),
        ("exact-increase", Capability::RateOrIncrease) => {
            (AggregationType::MultipleIncrease, "", vec![])
        }
        ("kll-percall", Capability::Quantile) => (
            AggregationType::DatasketchesKLL,
            "",
            vec![("K", Value::from(integer_param("k")?))],
        ),
        ("dd", Capability::Quantile) => {
            let alpha = float_param("alpha")?;
            if !(alpha.is_finite() && alpha > 0.0 && alpha < 1.0) {
                return Err(MilpOutputError::InvalidDdsAlpha { alpha });
            }
            (
                AggregationType::DDSketch,
                "",
                vec![("alpha", Value::from(alpha))],
            )
        }
        ("hll", Capability::Cardinality) => (
            AggregationType::HLL,
            "",
            vec![("precision", Value::from(integer_param("lg_k")?))],
        ),
        (
            "cms-heap-topk-fastpath-vector2d",
            capability @ (Capability::TopKByValue | Capability::TopKByCount),
        ) => (
            AggregationType::CountMinSketchWithHeap,
            if capability == Capability::TopKByCount {
                "count"
            } else {
                "sum"
            },
            vec![
                ("depth", Value::from(integer_param("rows")?)),
                ("width", Value::from(integer_param("cols")?)),
                ("heapsize", Value::from(heap_size(deployment)?)),
            ],
        ),
        (_, capability) => {
            return Err(MilpOutputError::UndeployableVariant {
                variant: variant.to_string(),
                capability,
            })
        }
    };

    let (grouping_labels, aggregated_labels) = subpopulation_labels(item, aggregation_type);

    Ok(IntermediateAggConfig {
        aggregation_type,
        aggregation_sub_type: sub_type.to_string(),
        window_type: if deployment.window_ms == deployment.slide_ms {
            WindowType::Tumbling
        } else {
            WindowType::Sliding
        },
        window_size_ms: deployment.window_ms,
        slide_interval_ms: deployment.slide_ms,
        spatial_filter: deployment.spatial_filter.clone(),
        metric: deployment.metric.clone(),
        table_name: None,
        value_column: None,
        parameters: parameters
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect(),
        rollup_labels: KeyByLabelNames::empty(),
        grouping_labels,
        aggregated_labels,
    })
}

/// The (grouping, aggregated) labels `item`'s query needs from an aggregation
/// of `aggregation_type`.
fn subpopulation_labels(
    item: &OptimizerItem,
    aggregation_type: AggregationType,
) -> (KeyByLabelNames, KeyByLabelNames) {
    let requirements = &item.requirements;
    let mut grouping_labels = KeyByLabelNames::empty();
    let mut aggregated_labels = KeyByLabelNames::empty();
    set_subpopulation_labels(
        requirements.statistics[0],
        aggregation_type,
        &requirements.grouping_labels,
        requirements.topk_by_labels.as_ref(),
        &mut KeyByLabelNames::empty(),
        &mut grouping_labels,
        &mut aggregated_labels,
    );
    (grouping_labels, aggregated_labels)
}

fn missing_param(variant: &str, param: &'static str) -> MilpOutputError {
    MilpOutputError::MissingParam {
        variant: variant.to_string(),
        param,
    }
}

/// The heap the plan priced: sketch-bench's `heap` param, sized `m · k` for
/// the windows the query it was built for merges (or the smallest measured
/// heap above that). Another query sharing it may need more; the plan then
/// priced that query's accuracy as a lossy merge, so the heap is emitted as
/// priced. The engine takes each query's `k` from the query itself.
fn heap_size(deployment: &Deployment) -> Result<u64, MilpOutputError> {
    rqe_optimizer::heap_capacity(&deployment.config)
        .ok_or_else(|| missing_param(&deployment.config.sketch, "heap"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::QueryLanguage;
    use rqe_optimizer::milp::Objective;
    use rqe_optimizer::{AtomicCostEntry, WorkloadFacts};
    use serde_json::json;

    use super::*;
    use crate::config::input::{ControllerConfig, SQLControllerConfig};
    use crate::optimizer::milp::tests::{
        config, costs, facts, group, table_accuracy, FACTS, SCRAPE_MS,
    };
    use crate::optimizer::milp::{build_milp_workload, solve_milp};
    use crate::optimizer::workload_facts::parse_workload_facts;

    fn plan(groups: &str) -> (ControllerConfig, MilpWorkload, MilpSolution) {
        let config = config(groups);
        let (workload, solution) = solve(MilpConfig::Promql(&config), &facts(&config));
        (config, workload, solution)
    }

    fn solve(config: MilpConfig, facts: &WorkloadFacts) -> (MilpWorkload, MilpSolution) {
        let workload = build_milp_workload(config, facts, SCRAPE_MS).unwrap();
        let mut costs = costs();
        for cost in &mut costs {
            cost.sketch_config["params"] = match cost.sketch.as_str() {
                "kll-percall" => json!({"k": 200}),
                "cms-heap-topk-fastpath-vector2d" => json!({
                    "rows": 3,
                    "cols": 2048,
                    "heap": cost.sketch_config["params"]["heap"],
                }),
                _ => json!({}),
            };
        }
        let solution = solve_milp(
            &workload,
            facts,
            &costs,
            Objective::default(),
            false,
            &table_accuracy,
        )
        .unwrap();
        (workload, solution)
    }

    /// The aggregation each query names, parsed back the way the engine reads it.
    fn aggregation_for(output: &PlannerOutput, query: &str) -> AggregationConfig {
        aggregation_in(output, query, QueryLanguage::promql)
    }

    fn aggregation_in(
        output: &PlannerOutput,
        query: &str,
        language: QueryLanguage,
    ) -> AggregationConfig {
        let inference = output.to_inference_config(language).unwrap();
        let streaming = output.to_streaming_config(language).unwrap();
        let query_config = inference
            .query_configs
            .iter()
            .find(|q| q.query == query)
            .unwrap_or_else(|| panic!("no query_config for {query}"));
        let [reference] = query_config.aggregations.as_slice() else {
            panic!("{query} names {:?}", query_config.aggregations);
        };
        streaming
            .get_aggregation_config(reference.aggregation_id)
            .unwrap()
            .clone()
    }

    fn dd_cost(params: serde_json::Value) -> AtomicCostEntry {
        AtomicCostEntry {
            sketch: "dd".into(),
            sketch_config: json!({"algorithm": "dd", "params": params}),
            mem_bytes_per_instance: 100.0,
            insert_cpu_secs: 1e-7,
            merge_cpu_secs: 1e-7,
            query_cpu_secs: 1e-7,
            query_accuracy: BTreeMap::from([("mean_relative_value_error".into(), 0.005)]),
            accuracy_metric: "mean_relative_value_error".into(),
            measured_at: crate::optimizer::atomic_costs::test_measured_at(),
        }
    }

    #[test]
    fn plan_round_trips_into_engine_configs() {
        let sum = "sum by (job) (http_requests_total)";
        let count = "sum by (job) (count_over_time(http_requests_total[1m]))";
        let quantile = "quantile_over_time(0.99, http_requests_total[5m])";
        let topk_value = "topk(5, sum_over_time(http_requests_total[1m]))";
        let topk_count = "topk(5, count_over_time(http_requests_total[1m]))";
        let groups: String = [sum, count, quantile, topk_value, topk_count]
            .iter()
            .map(|q| group(q, 0.99))
            .collect();
        let (config, workload, solution) = plan(&groups);
        let output =
            plan_to_planner_output(MilpConfig::Promql(&config), &workload, &solution).unwrap();
        assert_eq!(output.streaming_aggregation_count(), 5);

        let a = aggregation_for(&output, sum);
        assert_eq!(a.aggregation_type, AggregationType::MultipleSum);
        assert_eq!(a.aggregation_sub_type, "sum");
        assert_eq!(
            a.aggregated_labels,
            KeyByLabelNames::new(vec!["job".into()])
        );
        assert!(a.grouping_labels.is_empty());

        let a = aggregation_for(&output, count);
        assert_eq!(a.aggregation_type, AggregationType::MultipleSum);
        assert_eq!(a.aggregation_sub_type, "count");

        let a = aggregation_for(&output, quantile);
        assert_eq!(a.aggregation_type, AggregationType::DatasketchesKLL);
        assert_eq!(a.parameters["K"], json!(200));
        assert_eq!(
            a.grouping_labels,
            KeyByLabelNames::new(vec!["instance".into(), "job".into()])
        );

        let a = aggregation_for(&output, topk_value);
        assert_eq!(a.aggregation_type, AggregationType::CountMinSketchWithHeap);
        assert_eq!(a.aggregation_sub_type, "sum");
        assert_eq!(a.parameters["depth"], json!(3));
        assert_eq!(a.parameters["width"], json!(2048));
        assert_eq!(
            a.parameters["heapsize"],
            json!(planned_heap(&workload, &solution, topk_value))
        );
        assert_eq!(
            aggregation_for(&output, topk_count).aggregation_sub_type,
            "count"
        );

        for query in [sum, count, quantile, topk_value, topk_count] {
            let a = aggregation_for(&output, query);
            assert_eq!(a.metric, "http_requests_total");
            // No cleanup policy yet: retention is unbounded.
            assert_eq!(a.num_aggregates_to_retain, None);
        }
    }

    /// What the engine builds from an aggregation, without its id, metric
    /// and SQL columns.
    fn layout(a: &AggregationConfig) -> String {
        let parameters: BTreeMap<_, _> = a.parameters.iter().collect();
        format!(
            "{:?}",
            (
                a.aggregation_type,
                &a.aggregation_sub_type,
                parameters,
                &a.grouping_labels,
                &a.aggregated_labels,
                &a.rollup_labels,
                a.window_size_ms,
                a.slide_interval_ms,
                a.window_type,
                &a.spatial_filter_normalized,
                a.num_aggregates_to_retain,
            )
        )
    }

    /// A workload written in SQL, on a table whose value column has
    /// `http_requests_total`'s labels and facts, plans exactly as in PromQL.
    #[test]
    fn sql_twin_plans_like_promql() {
        let last = |secs: u32| format!("time BETWEEN DATEADD(s, -{secs}, NOW()) AND NOW()");
        let twins = [
            (
                "sum by (job) (http_requests_total)",
                format!(
                    "SELECT job, SUM(requests) FROM http WHERE {} GROUP BY job",
                    last(15)
                ),
            ),
            (
                "sum by (job) (count_over_time(http_requests_total[1m]))",
                format!(
                    "SELECT job, COUNT(requests) FROM http WHERE {} GROUP BY job",
                    last(60)
                ),
            ),
            (
                "quantile_over_time(0.99, http_requests_total[5m])",
                format!(
                    "SELECT instance, job, quantile(0.99)(requests) FROM http WHERE {} \
                     GROUP BY instance, job",
                    last(300)
                ),
            ),
            (
                "topk(5, sum_over_time(http_requests_total[1m]))",
                format!(
                    "SELECT instance, job, SUM(requests) AS s FROM http WHERE {} \
                     GROUP BY instance, job ORDER BY s DESC LIMIT 5",
                    last(60)
                ),
            ),
            (
                "topk(5, count_over_time(http_requests_total[1m]))",
                format!(
                    "SELECT instance, job, COUNT(requests) AS c FROM http WHERE {} \
                     GROUP BY instance, job ORDER BY c DESC LIMIT 5",
                    last(60)
                ),
            ),
        ];
        let promql_groups: String = twins.iter().map(|(q, _)| group(q, 0.99)).collect();
        let (promql, promql_workload, promql_solution) = plan(&promql_groups);

        let sql_groups: String = twins.iter().map(|(_, q)| group(q, 0.99)).collect();
        let sql: SQLControllerConfig = serde_yaml::from_str(&format!(
            "tables:\n  - name: http\n    time_column: time\n    value_columns: [requests]\n    \
             metadata_columns: [instance, job]\nquery_groups:\n{sql_groups}"
        ))
        .unwrap();
        let facts = parse_workload_facts(
            &FACTS.replace("http_requests_total", "http.requests"),
            &MilpConfig::Sql(&sql).hints().unwrap(),
            SCRAPE_MS,
        )
        .unwrap();
        let (sql_workload, sql_solution) = solve(MilpConfig::Sql(&sql), &facts);

        let (promql_cost, sql_cost) = (&promql_solution.plan_cost, &sql_solution.plan_cost);
        assert_eq!(sql_cost.cpu_secs_per_sec(), promql_cost.cpu_secs_per_sec());
        assert_eq!(sql_cost.memory_bytes(), promql_cost.memory_bytes());

        let promql_output = plan_to_planner_output(
            MilpConfig::Promql(&promql),
            &promql_workload,
            &promql_solution,
        )
        .unwrap();
        let sql_output =
            plan_to_planner_output(MilpConfig::Sql(&sql), &sql_workload, &sql_solution).unwrap();
        assert_eq!(sql_output.streaming_aggregation_count(), 5);
        for (promql_query, sql_query) in &twins {
            let expected = aggregation_for(&promql_output, promql_query);
            let a = aggregation_in(&sql_output, sql_query, QueryLanguage::sql);
            assert_eq!(layout(&a), layout(&expected), "{sql_query}");
            assert_eq!(a.metric, "http.requests");
            assert_eq!(a.table_name.as_deref(), Some("http"));
            assert_eq!(a.value_column.as_deref(), Some("requests"));
        }
    }

    /// The heap `query`'s deployment needs for it: `m · k` for the windows
    /// its lookback merges and its own `k` (`Deployment::heap_needed`).
    fn heap_needed(workload: &MilpWorkload, solution: &MilpSolution, query: &str) -> u64 {
        let i = workload
            .raqes
            .iter()
            .position(|r| r.id.contains(query))
            .unwrap_or_else(|| panic!("no raqe for {query}"));
        let raqe = &workload.raqes[i];
        let deployment = &solution.deployments[solution.raqes[i].deployment].deployment;
        deployment
            .heap_needed(raqe.lookback_ms, raqe.topk_k())
            .expect("the lookback is whole windows")
    }

    /// The heap the plan priced for `query`'s deployment: its `heap` param.
    fn planned_heap(workload: &MilpWorkload, solution: &MilpSolution, query: &str) -> u64 {
        let i = workload
            .raqes
            .iter()
            .position(|r| r.id.contains(query))
            .unwrap_or_else(|| panic!("no raqe for {query}"));
        let deployment = &solution.deployments[solution.raqes[i].deployment].deployment;
        rqe_optimizer::heap_capacity(&deployment.config).expect("a heap top-k deployment")
    }

    /// Each top-k query plans with its own literal k, and the engine's heap is
    /// the one the plan priced: `m · k` for the windows it merges, or the
    /// smallest measured heap (32) that already holds it.
    #[test]
    fn topk_plans_with_its_own_k_and_heap_m_times_k() {
        for (k, measured_floor) in [(10, true), (100, false)] {
            let query = format!("topk({k}, sum_over_time(http_requests_total[1m]))");
            let (config, workload, solution) = plan(&group(&query, 0.99));
            assert_eq!(workload.raqes[0].topk_k, Some(k));
            let deployment = &solution.deployments[0].deployment;
            assert_eq!(rqe_optimizer::answered_k(&deployment.config), k);
            let needed = heap_needed(&workload, &solution, &query);
            let heap = planned_heap(&workload, &solution, &query);
            if measured_floor {
                assert_eq!(heap, rqe_optimizer::TOPK_K, "k = {k}");
                assert!(heap >= needed);
            } else {
                assert_eq!(heap, needed, "k = {k}: m · k");
            }
            let output =
                plan_to_planner_output(MilpConfig::Promql(&config), &workload, &solution).unwrap();
            assert_eq!(
                aggregation_for(&output, &query).parameters["heapsize"],
                json!(heap),
                "k = {k}"
            );
        }
    }

    /// Queries sharing one heap get the heap the plan priced, which holds the
    /// largest k among them.
    #[test]
    fn a_shared_heap_is_the_planned_heap() {
        let small = "topk(5, sum_over_time(http_requests_total[1m]))";
        let large = "topk(10, sum_over_time(http_requests_total[1m]))";
        let (config, workload, solution) = plan(&(group(small, 0.99) + &group(large, 0.99)));
        assert_eq!(solution.deployments.len(), 1);
        let heap = planned_heap(&workload, &solution, large);
        assert!(heap >= heap_needed(&workload, &solution, large));
        let output =
            plan_to_planner_output(MilpConfig::Promql(&config), &workload, &solution).unwrap();
        for query in [small, large] {
            assert_eq!(
                aggregation_for(&output, query).parameters["heapsize"],
                json!(heap)
            );
        }
    }

    #[test]
    fn a_heap_family_without_a_heap_is_a_missing_param() {
        let query = "topk(5, sum_over_time(http_requests_total[1m]))";
        let (_, _, solution) = plan(&group(query, 0.99));
        let mut deployment = solution.deployments[0].deployment.clone();
        deployment.config.sketch = "cms-fastpath-vector2d".into();
        assert!(matches!(
            heap_size(&deployment),
            Err(MilpOutputError::MissingParam { param: "heap", .. })
        ));
    }

    #[test]
    fn ddsketch_plan_preserves_the_cost_row_alpha() {
        let query = "quantile_over_time(0.99, http_requests_total[5m])";
        let config = config(&group(query, 0.99));
        let facts = facts(&config);
        let workload = build_milp_workload(MilpConfig::Promql(&config), &facts, SCRAPE_MS).unwrap();
        let costs = vec![dd_cost(json!({"alpha": 0.02}))];
        let solution = solve_milp(
            &workload,
            &facts,
            &costs,
            Objective::default(),
            false,
            &table_accuracy,
        )
        .unwrap();

        let output =
            plan_to_planner_output(MilpConfig::Promql(&config), &workload, &solution).unwrap();
        let aggregation = aggregation_for(&output, query);
        assert_eq!(aggregation.aggregation_type, AggregationType::DDSketch);
        assert_eq!(aggregation.parameters["alpha"], json!(0.02));
    }

    #[test]
    fn ddsketch_plan_rejects_a_cost_row_without_alpha() {
        let query = "quantile_over_time(0.99, http_requests_total[5m])";
        let config = config(&group(query, 0.99));
        let facts = facts(&config);
        let workload = build_milp_workload(MilpConfig::Promql(&config), &facts, SCRAPE_MS).unwrap();
        let costs = vec![dd_cost(json!({}))];
        let solution = solve_milp(
            &workload,
            &facts,
            &costs,
            Objective::default(),
            false,
            &table_accuracy,
        )
        .unwrap();

        assert!(matches!(
            plan_to_planner_output(MilpConfig::Promql(&config), &workload, &solution),
            Err(MilpOutputError::MissingParam {
                variant,
                param: "alpha",
            }) if variant == "dd"
        ));
    }

    #[test]
    fn ddsketch_plan_rejects_an_invalid_alpha() {
        let query = "quantile_over_time(0.99, http_requests_total[5m])";
        let config = config(&group(query, 0.99));
        let facts = facts(&config);
        let workload = build_milp_workload(MilpConfig::Promql(&config), &facts, SCRAPE_MS).unwrap();

        for alpha in [0.0, -0.01, 1.0] {
            let costs = vec![dd_cost(json!({"alpha": alpha}))];
            let solution = solve_milp(
                &workload,
                &facts,
                &costs,
                Objective::default(),
                false,
                &table_accuracy,
            )
            .unwrap();
            assert!(matches!(
                plan_to_planner_output(MilpConfig::Promql(&config), &workload, &solution),
                Err(MilpOutputError::InvalidDdsAlpha { .. })
            ));
        }
    }

    #[test]
    fn window_wider_than_slide_is_sliding() {
        let query = "quantile_over_time(0.99, http_requests_total[5m])";
        let (config, workload, mut solution) = plan(&group(query, 0.99));
        let deployment = &mut solution.deployments[0].deployment;
        deployment.window_ms = 300_000;
        deployment.slide_ms = 60_000;
        let output =
            plan_to_planner_output(MilpConfig::Promql(&config), &workload, &solution).unwrap();
        let a = aggregation_for(&output, query);
        assert_eq!(a.window_type, WindowType::Sliding);
        assert_eq!((a.window_size_ms, a.slide_interval_ms), (300_000, 60_000));
    }

    #[test]
    fn avg_query_is_an_error() {
        let query = "avg by (job) (http_requests_total)";
        let (config, workload, solution) = plan(&group(query, 0.99));
        let err = plan_to_planner_output(MilpConfig::Promql(&config), &workload, &solution)
            .err()
            .unwrap();
        assert!(matches!(err, MilpOutputError::AvgQuery(q) if q == query));
    }

    /// Range overrides only size retention, so they block writing configs
    /// but not planning.
    #[test]
    fn range_query_overrides_block_only_the_written_configs() {
        let query = "sum(http_requests_total)";
        for field in ["step_ms: 60000", "range_duration_ms: 3600000"] {
            let (config, workload, solution) =
                plan(&format!("{}    {field}\n", group(query, 0.99)));
            let err = plan_to_planner_output(MilpConfig::Promql(&config), &workload, &solution)
                .err()
                .unwrap();
            assert!(
                matches!(&err, MilpOutputError::RangeQueryOverrides(q) if q == &[query]),
                "{field}: {err}"
            );
        }
    }

    #[test]
    fn hydra_kll_is_not_deployed() {
        // Its DeltaSet key tracker has no ASAPQuery pairing yet.
        let (config, workload, mut solution) = plan(&group(
            "quantile_over_time(0.99, http_requests_total[5m])",
            0.99,
        ));
        solution.deployments[0].deployment.config.sketch = "hydra-kll".into();
        let err = plan_to_planner_output(MilpConfig::Promql(&config), &workload, &solution)
            .err()
            .unwrap();
        assert!(
            matches!(err, MilpOutputError::UndeployableVariant { ref variant, .. } if variant == "hydra-kll")
        );
    }

    #[test]
    fn rollup_is_refused() {
        // A deployment by (instance, job) serving a query by (job): the
        // engine would build the aggregation at the query's grouping.
        let query = "sum by (job) (http_requests_total)";
        let (config, workload, mut solution) = plan(&group(query, 0.99));
        solution.deployments[0]
            .deployment
            .grouping_labels
            .insert("instance".into());
        let err = plan_to_planner_output(MilpConfig::Promql(&config), &workload, &solution)
            .err()
            .unwrap();
        assert!(
            matches!(&err, MilpOutputError::RollupUnsupported { query: q, raqe, deployment }
                if q.contains(query) && raqe == &["job"] && deployment == &["instance", "job"]),
            "{err}"
        );
    }

    /// Both are global top-k Raqes on one metric, so the solver may give them
    /// one deployment, but their heaps are keyed by different labels: each
    /// query gets its own heap.
    #[test]
    fn topk_of_sum_queries_needing_different_heap_keys_get_separate_heaps() {
        let by_job = "topk(5, sum by (job) (sum_over_time(http_requests_total[1m])))";
        let by_instance = "topk(5, sum by (instance) (sum_over_time(http_requests_total[1m])))";
        let (config, workload, mut solution) =
            plan(&(group(by_job, 0.9) + &group(by_instance, 0.9)));
        for raqe in &mut solution.raqes {
            raqe.deployment = 0;
        }
        solution.deployments.truncate(1);

        let output =
            plan_to_planner_output(MilpConfig::Promql(&config), &workload, &solution).unwrap();

        assert_eq!(output.streaming_aggregation_count(), 2);
        for (query, key) in [(by_job, "job"), (by_instance, "instance")] {
            let aggregation = aggregation_for(&output, query);
            assert_eq!(
                aggregation.aggregated_labels,
                KeyByLabelNames::new(vec![key.to_string()]),
                "{query}"
            );
        }
    }

    #[test]
    fn query_split_across_deployments_is_an_error() {
        // The same query string at two cadences is two items; if they land on
        // different deployments, one query_config can't name both.
        let query = "sum by (job) (http_requests_total)";
        let slow = group(query, 0.99).replace("60000", "120000");
        let (config, workload, mut solution) = plan(&(group(query, 0.99) + &slow));
        let mut other = solution.deployments[0].clone();
        other.deployment.window_ms *= 2;
        other.deployment.slide_ms *= 2;
        solution.deployments.push(other);
        solution.raqes[1].deployment = 1;
        let err = plan_to_planner_output(MilpConfig::Promql(&config), &workload, &solution)
            .err()
            .unwrap();
        assert!(matches!(err, MilpOutputError::QueryOnTwoDeployments(q) if q == query));
    }

    #[test]
    fn contains_avg_looks_inside_binary_arms() {
        assert!(contains_avg(
            "rate(http_requests_total[5m]) / avg_over_time(http_requests_total[5m])"
        ));
        assert!(!contains_avg("sum by (job) (http_requests_total) * 2"));
    }
}
