//! Turns a solved MILP plan into the planner's streaming and inference YAML.

use std::collections::{HashMap, HashSet};

use asap_types::enums::{CleanupPolicy, WindowType};
use indexmap::map::Entry;
use indexmap::IndexMap;
use promql_parser::parser::{token, Expr};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::AggregationType;
use rqe_optimizer::milp::MilpSolution;
use rqe_optimizer::{Capability, Deployment};
use serde_json::Value;
use thiserror::Error;

use crate::config::input::ControllerConfig;
use crate::error::ControllerError;
use crate::generator::{GeneratorOutput, QueryPlanEntry};
use crate::planner::agg_config::IntermediateAggConfig;
use crate::planner::labels::set_subpopulation_labels;
use crate::planner_output::PlannerOutput;
use crate::promql::generator::{build_inference_yaml, build_streaming_yaml};

use super::aqe_extractor::contains_avg;
use super::milp::MilpWorkload;
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
    #[error("topk query {0:?} has no literal k")]
    TopkWithoutK(String),
    #[error("query {0:?} is served by two deployments; a query string can name only one")]
    QueryOnTwoDeployments(String),
    #[error(transparent)]
    Generator(#[from] ControllerError),
}

/// Errors on queries whose plan can be costed but not written as configs:
/// avg queries, and range-query overrides, which only size retention.
pub fn reject_unwritable_queries(config: &ControllerConfig) -> Result<(), MilpOutputError> {
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
    config: &ControllerConfig,
    workload: &MilpWorkload,
    solution: &MilpSolution,
) -> Result<PlannerOutput, MilpOutputError> {
    reject_unwritable_queries(config)?;

    let item_of = |raqe: usize| &workload.items[workload.raqe_items[raqe]];
    // Each item once per deployment, however many occurrences it has.
    let mut served: Vec<Vec<&OptimizerItem>> = vec![Vec::new(); solution.deployments.len()];
    let mut seen = HashSet::new();
    for (raqe, planned) in solution.raqes.iter().enumerate() {
        if seen.insert((planned.deployment, workload.raqe_items[raqe])) {
            served[planned.deployment].push(item_of(raqe));
        }
    }

    let mut aggregations: IndexMap<String, IntermediateAggConfig> = IndexMap::new();
    let mut deployment_keys = Vec::with_capacity(served.len());
    for (planned, items) in solution.deployments.iter().zip(&served) {
        let aggregation = aggregation_config(&planned.deployment, items)?;
        let key = aggregation.identifying_key();
        aggregations.entry(key.clone()).or_insert(aggregation);
        deployment_keys.push(key);
    }

    let mut queries: IndexMap<String, QueryPlanEntry> = IndexMap::new();
    for (raqe, planned) in solution.raqes.iter().enumerate() {
        let key = &deployment_keys[planned.deployment];
        for query in &item_of(raqe).query_strings {
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
    let schema = config.schema_from_hints();
    Ok(PlannerOutput::from_output(GeneratorOutput {
        punted_queries: Vec::new(),
        streaming_yaml: build_streaming_yaml(&aggregations, &id_map, &schema)?,
        inference_yaml: build_inference_yaml(CleanupPolicy::NoCleanup, &queries, &id_map, &schema)?,
        aggregation_count: aggregations.len(),
        query_count: queries.len(),
    }))
}

/// `items` are the optimizer items of the Raqes `deployment` serves; they
/// share its capability and grouping, so the first one decides the labels.
fn aggregation_config(
    deployment: &Deployment,
    items: &[&OptimizerItem],
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
                ("heapsize", Value::from(max_topk_k(items)?)),
            ],
        ),
        (_, capability) => {
            return Err(MilpOutputError::UndeployableVariant {
                variant: variant.to_string(),
                capability,
            })
        }
    };

    let requirements = &items
        .first()
        .expect("every planned deployment serves a Raqe")
        .requirements;
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

fn missing_param(variant: &str, param: &'static str) -> MilpOutputError {
    MilpOutputError::MissingParam {
        variant: variant.to_string(),
        param,
    }
}

/// One heap serves every topk query on the deployment, so it is sized for
/// the largest k.
fn max_topk_k(items: &[&OptimizerItem]) -> Result<u64, MilpOutputError> {
    items
        .iter()
        .flat_map(|item| &item.query_strings)
        .map(|query| topk_k(query).ok_or_else(|| MilpOutputError::TopkWithoutK(query.clone())))
        .try_fold(0, |max, k| Ok(max.max(k?)))
}

fn topk_k(query: &str) -> Option<u64> {
    let Ok(Expr::Aggregate(aggregate)) = promql_parser::parser::parse(query) else {
        return None;
    };
    if aggregate.op.id() != token::T_TOPK {
        return None;
    }
    match aggregate.param.as_deref()? {
        Expr::NumberLiteral(number) if number.val >= 1.0 => Some(number.val as u64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::QueryLanguage;
    use rqe_optimizer::milp::Objective;
    use rqe_optimizer::AtomicCostEntry;
    use serde_json::json;

    use super::*;
    use crate::optimizer::milp::tests::{config, costs, facts, group, SCRAPE_MS};
    use crate::optimizer::milp::{build_milp_workload, solve_milp};

    fn plan(groups: &str) -> (ControllerConfig, MilpWorkload, MilpSolution) {
        let config = config(groups);
        let facts = facts(&config);
        let workload = build_milp_workload(&config, &facts, SCRAPE_MS).unwrap();
        let mut costs = costs();
        for cost in &mut costs {
            cost.sketch_config["params"] = match cost.sketch.as_str() {
                "kll-percall" => json!({"k": 200}),
                "cms-heap-topk-fastpath-vector2d" => json!({"rows": 3, "cols": 2048}),
                _ => json!({}),
            };
        }
        let solution = solve_milp(&workload, &facts, &costs, Objective::default(), false).unwrap();
        (config, workload, solution)
    }

    /// The aggregation each query names, parsed back the way the engine reads it.
    fn aggregation_for(output: &PlannerOutput, query: &str) -> AggregationConfig {
        let inference = output.to_inference_config(QueryLanguage::promql).unwrap();
        let streaming = output.to_streaming_config(QueryLanguage::promql).unwrap();
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
            merge_accuracy: BTreeMap::new(),
            measured_at: None,
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
        let output = plan_to_planner_output(&config, &workload, &solution).unwrap();
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
        assert_eq!(a.parameters["heapsize"], json!(5));
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

    #[test]
    fn shared_heap_is_sized_for_the_largest_k() {
        let small = "topk(5, sum_over_time(http_requests_total[1m]))";
        let large = "topk(10, sum_over_time(http_requests_total[1m]))";
        let (config, workload, solution) = plan(&(group(small, 0.99) + &group(large, 0.99)));
        assert_eq!(solution.deployments.len(), 1);
        let output = plan_to_planner_output(&config, &workload, &solution).unwrap();
        for query in [small, large] {
            assert_eq!(
                aggregation_for(&output, query).parameters["heapsize"],
                json!(10)
            );
        }
    }

    #[test]
    fn ddsketch_plan_preserves_the_cost_row_alpha() {
        let query = "quantile_over_time(0.99, http_requests_total[5m])";
        let config = config(&group(query, 0.99));
        let facts = facts(&config);
        let workload = build_milp_workload(&config, &facts, SCRAPE_MS).unwrap();
        let costs = vec![dd_cost(json!({"alpha": 0.02}))];
        let solution = solve_milp(&workload, &facts, &costs, Objective::default(), false).unwrap();

        let output = plan_to_planner_output(&config, &workload, &solution).unwrap();
        let aggregation = aggregation_for(&output, query);
        assert_eq!(aggregation.aggregation_type, AggregationType::DDSketch);
        assert_eq!(aggregation.parameters["alpha"], json!(0.02));
    }

    #[test]
    fn ddsketch_plan_rejects_a_cost_row_without_alpha() {
        let query = "quantile_over_time(0.99, http_requests_total[5m])";
        let config = config(&group(query, 0.99));
        let facts = facts(&config);
        let workload = build_milp_workload(&config, &facts, SCRAPE_MS).unwrap();
        let costs = vec![dd_cost(json!({}))];
        let solution = solve_milp(&workload, &facts, &costs, Objective::default(), false).unwrap();

        assert!(matches!(
            plan_to_planner_output(&config, &workload, &solution),
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
        let workload = build_milp_workload(&config, &facts, SCRAPE_MS).unwrap();

        for alpha in [0.0, -0.01, 1.0] {
            let costs = vec![dd_cost(json!({"alpha": alpha}))];
            let solution =
                solve_milp(&workload, &facts, &costs, Objective::default(), false).unwrap();
            assert!(matches!(
                plan_to_planner_output(&config, &workload, &solution),
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
        let output = plan_to_planner_output(&config, &workload, &solution).unwrap();
        let a = aggregation_for(&output, query);
        assert_eq!(a.window_type, WindowType::Sliding);
        assert_eq!((a.window_size_ms, a.slide_interval_ms), (300_000, 60_000));
    }

    #[test]
    fn avg_query_is_an_error() {
        let query = "avg by (job) (http_requests_total)";
        let (config, workload, solution) = plan(&group(query, 0.99));
        let err = plan_to_planner_output(&config, &workload, &solution)
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
            let err = plan_to_planner_output(&config, &workload, &solution)
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
        let err = plan_to_planner_output(&config, &workload, &solution)
            .err()
            .unwrap();
        assert!(
            matches!(err, MilpOutputError::UndeployableVariant { ref variant, .. } if variant == "hydra-kll")
        );
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
        let err = plan_to_planner_output(&config, &workload, &solution)
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

    #[test]
    fn topk_k_reads_the_literal() {
        assert_eq!(topk_k("topk(7, http_requests_total)"), Some(7));
        assert_eq!(topk_k("topk by (job) (3, http_requests_total)"), Some(3));
        assert_eq!(topk_k("sum(http_requests_total)"), None);
    }
}
