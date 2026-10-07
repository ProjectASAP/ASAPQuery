//! Plans a workload with sketch-bench's rqe-optimizer MILP: converts the
//! workload config into `Raqe`s and solves for the cheapest deployments.

use promql_utilities::query_logics::enums::Statistic;
use rqe_optimizer::candidates::build_all_candidates;
use rqe_optimizer::enumerate::unservable;
use rqe_optimizer::milp::{minimize, MilpSolution, Objective};
use rqe_optimizer::{
    table_accuracy, validate_facts, AtomicCostEntry, Capability, LabelSet, Raqe, WorkloadFacts,
};
use thiserror::Error;

use crate::config::input::ControllerConfig;

use std::path::Path;

use super::aqe_extractor::{extract_aqes, RQE};
use super::atomic_costs::load_flat_atomic_cost_table;
use super::error::OptimizerError;
use super::solution::OptimizerItem;
use super::workload_facts::load_workload_facts;

/// Slack on accuracy tolerances so `1 - sla` rounding (`1 - 0.9 =
/// 0.0999...98`) doesn't reject a row measured exactly at the boundary.
const SLA_EPSILON: f64 = 1e-9;

#[derive(Debug, Error)]
pub enum MilpError {
    #[error("the MILP planner doesn't support these workload config fields: {0:?}")]
    UnsupportedFields(Vec<&'static str>),
    #[error(
        "workload config has no `metrics:` hints; they are required to resolve grouping labels"
    )]
    MissingMetricHints,
    #[error("workload metrics missing from `metrics:` hints: {0:?}")]
    MetricsWithoutHints(Vec<String>),
    #[error(transparent)]
    Extraction(#[from] OptimizerError),
    #[error("query {query:?}: accuracy_sla {accuracy_sla} must be between 0 and 1")]
    AccuracySlaOutOfRange { query: String, accuracy_sla: f64 },
    #[error("invalid MILP inputs:\n{}", .0.join("\n"))]
    InvalidInputs(Vec<String>),
    #[error("query {query:?} needs one statistic, got {statistics:?}")]
    MultipleStatistics {
        query: String,
        statistics: Vec<Statistic>,
    },
    #[error("query {query:?}: topk ranking (by value or by sample count) is unknown")]
    TopkWeightingUnknown { query: String },
    #[error("no eligible deployment for raqes: {0:?}")]
    Unservable(Vec<String>),
    #[error("MILP solve failed: {0}")]
    Solver(String),
}

/// The cheapest plan. Only families in sketch-bench's `DEPLOYABLE_FAMILIES`
/// are candidates unless `allow_undeployable_families`, whose plan can be
/// studied but not deployed.
pub fn solve_milp(
    workload: &MilpWorkload,
    facts: &WorkloadFacts,
    costs: &[AtomicCostEntry],
    objective: Objective,
    allow_undeployable_families: bool,
) -> Result<MilpSolution, MilpError> {
    let deployments = build_all_candidates(
        &workload.raqes,
        costs,
        facts,
        allow_undeployable_families,
        &table_accuracy,
    );
    tracing::debug!(
        candidates = deployments.len(),
        cost_rows = costs.len(),
        "milp: built candidates"
    );
    // Occurrences of one item are identical Raqes (and contiguous), so
    // checking the first of each is enough.
    let one_per_item: Vec<Raqe> = workload
        .raqes
        .iter()
        .enumerate()
        .filter(|&(i, _)| i == 0 || workload.raqe_items[i] != workload.raqe_items[i - 1])
        .map(|(_, raqe)| raqe.clone())
        .collect();
    let missing = unservable(&one_per_item, &deployments, facts, &table_accuracy);
    if !missing.is_empty() {
        return Err(MilpError::Unservable(missing));
    }
    let solution = minimize(
        &workload.raqes,
        &deployments,
        facts,
        objective,
        &table_accuracy,
    )
    .map_err(|e| MilpError::Solver(e.to_string()))?;
    tracing::debug!(
        objective = objective.value(&solution.plan_cost),
        cpu_secs_per_sec = solution.plan_cost.cpu_secs_per_sec(),
        memory_bytes = solution.plan_cost.memory_bytes(),
        deployments = solution.deployments.len(),
        "milp: solved"
    );
    Ok(solution)
}

/// What a MILP planning run reads besides the workload config.
pub struct MilpInputs<'a> {
    pub workload_facts: &'a Path,
    pub atomic_costs: &'a Path,
    pub scrape_interval_ms: u64,
    /// Objective weights; `None` takes rqe-optimizer's default.
    pub w_cpu: Option<f64>,
    pub w_mem: Option<f64>,
    pub allow_undeployable_families: bool,
}

pub struct MilpPlan {
    pub workload: MilpWorkload,
    pub solution: MilpSolution,
    pub objective: Objective,
}

/// Loads the facts and costs and solves for the cheapest plan.
pub fn plan_milp(config: &ControllerConfig, inputs: &MilpInputs) -> anyhow::Result<MilpPlan> {
    config.warn_default_slas();
    let Some(hints) = config.metrics.as_deref() else {
        return Err(MilpError::MissingMetricHints.into());
    };
    let facts = load_workload_facts(inputs.workload_facts, hints, inputs.scrape_interval_ms)?;
    let costs = load_flat_atomic_cost_table(inputs.atomic_costs)?;
    let objective = objective(inputs.w_cpu, inputs.w_mem)?;
    tracing::debug!(?objective, cost_rows = costs.len(), "milp: inputs loaded");

    let workload = build_milp_workload(config, &facts, inputs.scrape_interval_ms)?;
    let solution = solve_milp(
        &workload,
        &facts,
        &costs,
        objective,
        inputs.allow_undeployable_families,
    )?;
    Ok(MilpPlan {
        workload,
        solution,
        objective,
    })
}

fn objective(w_cpu: Option<f64>, w_mem: Option<f64>) -> anyhow::Result<Objective> {
    let Objective::AUCCost {
        w_cpu: default_cpu,
        w_mem: default_mem,
    } = Objective::default();
    let (w_cpu, w_mem) = (w_cpu.unwrap_or(default_cpu), w_mem.unwrap_or(default_mem));
    // All-zero weights make every plan cost 0, so the solver's pick is arbitrary.
    anyhow::ensure!(
        w_cpu > 0.0 || w_mem > 0.0,
        "--w-cpu and --w-mem are both 0; at least one must be positive"
    );
    Ok(Objective::AUCCost { w_cpu, w_mem })
}

/// Parses an objective weight flag. Weights must be finite and non-negative:
/// a negative weight rewards cost, and NaN poisons every coefficient.
pub fn parse_weight(s: &str) -> Result<f64, String> {
    match s.parse::<f64>() {
        Ok(w) if w.is_finite() && w >= 0.0 => Ok(w),
        Ok(w) => Err(format!("must be finite and >= 0, got {w}")),
        Err(e) => Err(e.to_string()),
    }
}

/// The MILP's view of a workload.
#[derive(Debug)]
pub struct MilpWorkload {
    pub items: Vec<OptimizerItem>,
    pub raqes: Vec<Raqe>,
    /// `raqes[i]` is an occurrence of `items[raqe_items[i]]`.
    pub raqe_items: Vec<usize>,
}

/// One Raqe per query occurrence: a `Raqe` has no frequency weight, so two
/// dashboards running the same query at the same cadence become two
/// identical Raqes, which the MILP serves from one shared deployment.
pub fn build_milp_workload(
    config: &ControllerConfig,
    facts: &WorkloadFacts,
    scrape_interval_ms: u64,
) -> Result<MilpWorkload, MilpError> {
    // The MILP chooses windows and sketch parameters itself.
    let unsupported: Vec<&'static str> = [
        ("windowing", config.windowing.is_some()),
        ("sketch_parameters", config.sketch_parameters.is_some()),
        ("aggregate_cleanup", config.aggregate_cleanup.is_some()),
    ]
    .into_iter()
    .filter_map(|(field, set)| set.then_some(field))
    .collect();
    if !unsupported.is_empty() {
        return Err(MilpError::UnsupportedFields(unsupported));
    }
    let mut items = extract_hinted_items(config, scrape_interval_ms)?;
    // Stable Raqe order and ids across runs.
    items.sort_by(|a, b| {
        (&a.query_strings, a.t_repeat_ms)
            .cmp(&(&b.query_strings, b.t_repeat_ms))
            .then(a.accuracy_sla.total_cmp(&b.accuracy_sla))
            .then(latency_key(a).total_cmp(&latency_key(b)))
    });

    let mut raqes = Vec::new();
    let mut raqe_items = Vec::new();
    for (index, item) in items.iter().enumerate() {
        let raqe = item_to_raqe(item)?;
        let count = item.occurrences;
        tracing::debug!(
            item = index,
            queries = ?item.query_strings,
            statistics = ?item.requirements.statistics,
            occurrences = count,
            capability = ?raqe.capability,
            metric = %raqe.metric,
            spatial_filter = %raqe.spatial_filter,
            grouping_labels = ?raqe.grouping_labels,
            lookback_ms = raqe.lookback_ms,
            interval_ms = raqe.interval_ms,
            accuracy_sla = raqe.accuracy_sla,
            latency_sla_ms = ?raqe.latency_sla_ms,
            "milp inputs: item -> raqe"
        );
        for k in 0..count {
            raqes.push(Raqe {
                // The item index keeps ids unique across T and SLAs.
                id: format!("{index}:{}#{k}", raqe.id),
                ..raqe.clone()
            });
            raqe_items.push(index);
        }
    }

    tracing::debug!(
        items = items.len(),
        raqes = raqes.len(),
        metrics_with_facts = facts.len(),
        "milp inputs: built raqes"
    );
    validate_facts(&raqes, facts).map_err(MilpError::InvalidInputs)?;
    Ok(MilpWorkload {
        items,
        raqes,
        raqe_items,
    })
}

/// The workload's optimizer items, with labels resolved from its `metrics:`
/// hints. Every workload metric must have a hint.
fn extract_hinted_items(
    config: &ControllerConfig,
    scrape_interval_ms: u64,
) -> Result<Vec<OptimizerItem>, MilpError> {
    if config.metrics.is_none() {
        return Err(MilpError::MissingMetricHints);
    }
    let schema = config.schema_from_hints();
    let rqes = config_to_rqes(config);
    let aqes = extract_aqes(&rqes, &schema, scrape_interval_ms)?;

    // Requirement extraction treats an unknown metric as having no labels,
    // which would silently mis-resolve `without (...)` and plain selectors.
    let mut unhinted: Vec<String> = aqes
        .iter()
        .map(|aqe| aqe.requirements.metric.clone())
        .filter(|metric| schema.get_labels(metric).is_none())
        .collect();
    if !unhinted.is_empty() {
        unhinted.sort();
        unhinted.dedup();
        return Err(MilpError::MetricsWithoutHints(unhinted));
    }
    Ok(aqes)
}

/// Convert a `ControllerConfig`'s query groups into a flat list of RQEs.
/// Each (query, repetition_delay_ms) pair becomes one RQE.
fn config_to_rqes(config: &ControllerConfig) -> Vec<RQE> {
    config
        .query_groups
        .iter()
        .flat_map(|qg| {
            qg.queries.iter().map(|q| RQE {
                query_string: q.clone(),
                t_repeat_ms: qg.repetition_delay_ms,
                accuracy_sla: qg.controller_options.accuracy_sla,
                latency_sla_ms: qg.controller_options.latency_sla_ms,
            })
        })
        .collect()
}

/// No limit sorts last.
fn latency_key(item: &OptimizerItem) -> f64 {
    item.latency_sla_ms.unwrap_or(f64::INFINITY)
}

fn item_to_raqe(item: &OptimizerItem) -> Result<Raqe, MilpError> {
    let req = &item.requirements;
    let query = item.query_strings.join(" | ");
    let [statistic] = req.statistics.as_slice() else {
        return Err(MilpError::MultipleStatistics {
            query,
            statistics: req.statistics.clone(),
        });
    };
    let Some(capability) = capability(*statistic, req.topk_count_events) else {
        return Err(MilpError::TopkWeightingUnknown { query });
    };
    if !(0.0..=1.0).contains(&item.accuracy_sla) {
        return Err(MilpError::AccuracySlaOutOfRange {
            query,
            accuracy_sla: item.accuracy_sla,
        });
    }
    let accuracy_sla = accuracy_target(capability, item.accuracy_sla);

    // TopK keeps one heap per `topk by` bucket; its `grouping_labels` is the
    // output label set (every label), which would cost one heap per series.
    let grouping_labels: LabelSet = match capability {
        Capability::TopKByValue | Capability::TopKByCount => req
            .topk_by_labels
            .as_ref()
            .map(|labels| labels.labels.iter().cloned().collect())
            .unwrap_or_default(),
        _ => req.grouping_labels.labels.iter().cloned().collect(),
    };

    Ok(Raqe {
        id: query,
        capability,
        lookback_ms: req.data_range_ms,
        interval_ms: item.t_repeat_ms,
        metric: req.metric.clone(),
        spatial_filter: req.spatial_filter_normalized.clone(),
        grouping_labels,
        accuracy_sla,
        latency_sla_ms: item.latency_sla_ms,
    })
}

/// `None` for a topk whose weighting is unknown: value-ranked
/// (`sum_over_time`) and count-ranked (`count_over_time`) top-k need separate
/// heaps.
fn capability(statistic: Statistic, topk_count_events: Option<bool>) -> Option<Capability> {
    Some(match statistic {
        Statistic::Sum => Capability::Sum,
        Statistic::Count => Capability::Count,
        Statistic::Rate | Statistic::Increase => Capability::RateOrIncrease,
        Statistic::Min => Capability::Min,
        Statistic::Max => Capability::Max,
        Statistic::Quantile => Capability::Quantile,
        Statistic::Cardinality => Capability::Cardinality,
        Statistic::Topk => match topk_count_events? {
            true => Capability::TopKByCount,
            false => Capability::TopKByValue,
        },
    })
}

/// `accuracy_sla` is required accuracy: error metrics must stay within
/// `1 - accuracy_sla`, and top-k precision must reach `accuracy_sla`.
fn accuracy_target(capability: Capability, accuracy_sla: f64) -> f64 {
    let max_error = 1.0 - accuracy_sla + SLA_EPSILON;
    match capability {
        Capability::Sum
        | Capability::Count
        | Capability::Min
        | Capability::Max
        | Capability::RateOrIncrease
        | Capability::Cardinality
        | Capability::Quantile => max_error,
        Capability::TopKByValue | Capability::TopKByCount => accuracy_sla - SLA_EPSILON,
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::optimizer::workload_facts::parse_workload_facts;

    pub(crate) const SCRAPE_MS: u64 = 15_000;

    const FACTS: &str = r#"
metrics:
  - metric: http_requests_total
    value_range: [1.0, 1000.0]
    groups:
      - labels: [instance, job]
        cardinality: 100
      - labels: [job]
        cardinality: 10
      - labels: []
        cardinality: 1
"#;

    pub(crate) fn config(groups: &str) -> ControllerConfig {
        let yaml = format!(
            "query_groups:\n{groups}\nmetrics:\n  - metric: http_requests_total\n    labels: [instance, job]\n"
        );
        serde_yaml::from_str(&yaml).unwrap()
    }

    pub(crate) fn group(query: &str, accuracy_sla: f64) -> String {
        format!(
            "  - queries: [\"{query}\"]\n    repetition_delay_ms: 60000\n    controller_options: {{accuracy_sla: {accuracy_sla}}}\n"
        )
    }

    pub(crate) fn facts(config: &ControllerConfig) -> WorkloadFacts {
        parse_workload_facts(FACTS, config.metrics.as_deref().unwrap(), SCRAPE_MS).unwrap()
    }

    fn workload(groups: &str) -> MilpWorkload {
        let config = config(groups);
        build_milp_workload(&config, &facts(&config), SCRAPE_MS).unwrap()
    }

    fn labels(names: &[&str]) -> LabelSet {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn cost(sketch: &str, accuracy: &[(&str, f64)]) -> AtomicCostEntry {
        AtomicCostEntry {
            sketch: sketch.into(),
            sketch_config: serde_json::json!({"algorithm": sketch, "params": {}}),
            mem_bytes_per_instance: 100.0,
            insert_cpu_secs: 1e-7,
            merge_cpu_secs: 1e-7,
            query_cpu_secs: 1e-7,
            query_accuracy: accuracy.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
            merge_accuracy: BTreeMap::new(),
            measured_at: None,
        }
    }

    pub(crate) fn costs() -> Vec<AtomicCostEntry> {
        vec![
            cost("exact-sum", &[("relative_error", 0.0)]),
            cost("kll-percall", &[("mean_rank_err", 0.005)]),
            cost(
                "cms-heap-topk-fastpath-vector2d",
                &[("precision_at_k", 0.995)],
            ),
        ]
    }

    #[test]
    fn repeated_query_becomes_one_raqe_per_occurrence() {
        let query = "sum by (job) (http_requests_total)";
        let w = workload(&(group(query, 0.99) + &group(query, 0.99)));
        assert_eq!(w.items.len(), 1);
        assert_eq!(w.raqe_items, vec![0, 0]);
        let ids: Vec<_> = w.raqes.iter().map(|r| r.id.as_str()).collect();
        assert_eq!(ids, vec![format!("0:{query}#0"), format!("0:{query}#1")]);
    }

    #[test]
    fn accuracy_exactly_at_the_sla_boundary_passes() {
        let w = workload(&group(
            "quantile_over_time(0.99, http_requests_total[5m])",
            0.9,
        ));
        assert!(w.raqes[0].meets_sla("kll-percall", Some(0.1)));
        assert!(w.raqes[0].meets_sla("dd", Some(0.1)));
        assert!(!w.raqes[0].meets_sla("kll-percall", Some(0.1001)));
        assert!(!w.raqes[0].meets_sla("dd", Some(0.1001)));
        let w = workload(&group("topk(5, http_requests_total)", 0.9));
        assert!(w.raqes[0].meets_sla("cms-heap-topk-fastpath-vector2d", Some(0.9)));
        assert!(!w.raqes[0].meets_sla("cms-heap-topk-fastpath-vector2d", Some(0.8999)));
    }

    #[test]
    fn same_query_at_different_cadences_gets_distinct_ids() {
        let query = "sum by (job) (http_requests_total)";
        let slow = group(query, 0.99).replace("60000", "120000");
        let w = workload(&(group(query, 0.99) + &slow));
        assert_eq!(w.raqes.len(), 2);
        assert_ne!(w.raqes[0].id, w.raqes[1].id);
    }

    #[test]
    fn item_with_two_statistics_is_an_error_not_a_panic() {
        let w = workload(&group("sum by (job) (http_requests_total)", 0.99));
        let mut item = w.items[0].clone();
        item.requirements.statistics.push(Statistic::Count);
        assert!(matches!(
            item_to_raqe(&item),
            Err(MilpError::MultipleStatistics { .. })
        ));
    }

    #[test]
    fn sum_maps_to_sum_with_relative_error_ceiling() {
        let w = workload(&group("sum by (job) (http_requests_total)", 0.99));
        let r = &w.raqes[0];
        assert_eq!(r.capability, Capability::Sum);
        assert_eq!(r.grouping_labels, labels(&["job"]));
        assert!((r.accuracy_sla - 0.01).abs() < 1e-6);
        assert_eq!(r.interval_ms, 60_000);
        assert_eq!(r.latency_sla_ms, None);
    }

    #[test]
    fn count_and_topk_weighting_map_to_their_own_capabilities() {
        let cap = |query| workload(&group(query, 0.99)).raqes[0].capability;
        assert_eq!(
            cap("sum by (job) (count_over_time(http_requests_total[1m]))"),
            Capability::Count
        );
        assert_eq!(
            cap("topk(5, sum_over_time(http_requests_total[1m]))"),
            Capability::TopKByValue
        );
        assert_eq!(
            cap("topk(5, count_over_time(http_requests_total[1m]))"),
            Capability::TopKByCount
        );
    }

    #[test]
    fn topk_with_unknown_weighting_is_an_error() {
        let w = workload(&group("topk(5, http_requests_total)", 0.99));
        let mut item = w.items[0].clone();
        item.requirements.topk_count_events = None;
        assert!(matches!(
            item_to_raqe(&item),
            Err(MilpError::TopkWeightingUnknown { .. })
        ));
    }

    #[test]
    fn quantile_uses_the_error_ceiling() {
        let w = workload(&group(
            "quantile_over_time(0.99, http_requests_total[5m])",
            0.99,
        ));
        let r = &w.raqes[0];
        assert_eq!(r.capability, Capability::Quantile);
        assert!((r.accuracy_sla - 0.01).abs() < 1e-6);
        assert_eq!(r.lookback_ms, 300_000);
    }

    #[test]
    fn topk_requires_precision_and_groups_by_its_buckets() {
        let w = workload(&group("topk by (job) (5, http_requests_total)", 0.9));
        let r = &w.raqes[0];
        assert_eq!(r.capability, Capability::TopKByValue);
        assert!((r.accuracy_sla - 0.9).abs() < 1e-6);
        // Not the all-labels output set, which would cost one heap per series.
        assert_eq!(r.grouping_labels, labels(&["job"]));
    }

    #[test]
    fn bare_topk_is_one_global_heap() {
        let w = workload(&group("topk(5, http_requests_total)", 0.9));
        assert_eq!(w.raqes[0].grouping_labels, LabelSet::new());
    }

    #[test]
    fn accuracy_sla_above_one_is_rejected() {
        let config = config(&group("sum by (job) (http_requests_total)", 1.5));
        let err = build_milp_workload(&config, &facts(&config), SCRAPE_MS).unwrap_err();
        assert!(matches!(err, MilpError::AccuracySlaOutOfRange { .. }));
    }

    #[test]
    fn spatial_filter_is_rejected() {
        let config = config(&group(
            "sum by (job) (http_requests_total{job='api'})",
            0.99,
        ));
        let err = build_milp_workload(&config, &facts(&config), SCRAPE_MS).unwrap_err();
        let MilpError::InvalidInputs(problems) = err else {
            panic!("expected InvalidInputs, got {err:?}");
        };
        assert!(problems.iter().any(|p| p.contains("spatial filter")));
    }

    #[test]
    fn missing_cardinality_is_rejected() {
        let config = config(&group("sum by (instance) (http_requests_total)", 0.99));
        let err = build_milp_workload(&config, &facts(&config), SCRAPE_MS).unwrap_err();
        assert!(matches!(err, MilpError::InvalidInputs(_)));
    }

    /// The MILP picks windows and sketch parameters itself, so these would be
    /// dropped silently.
    #[test]
    fn unsupported_config_fields_are_rejected() {
        let mut config = config(&group("sum(http_requests_total)", 0.99));
        config.windowing =
            Some(serde_yaml::from_str("{type: tumbling, window_size_ms: 60000}").unwrap());
        config.sketch_parameters = Some(Default::default());
        let err = build_milp_workload(&config, &facts(&config), SCRAPE_MS).unwrap_err();
        assert!(
            matches!(&err, MilpError::UnsupportedFields(f) if f == &["windowing", "sketch_parameters"]),
            "{err}"
        );
        config.windowing = None;
        config.sketch_parameters = None;
        config.aggregate_cleanup = Some(serde_yaml::from_str("{}").unwrap());
        let err = build_milp_workload(&config, &facts(&config), SCRAPE_MS).unwrap_err();
        assert!(
            matches!(&err, MilpError::UnsupportedFields(f) if f == &["aggregate_cleanup"]),
            "{err}"
        );
    }

    #[test]
    fn solve_shares_one_deployment_across_repeated_queries() {
        let query = "sum by (job) (http_requests_total)";
        let config = config(&(group(query, 0.99) + &group(query, 0.99)));
        let facts = facts(&config);
        let w = build_milp_workload(&config, &facts, SCRAPE_MS).unwrap();
        let solution = solve_milp(&w, &facts, &costs(), Objective::default(), false).unwrap();
        assert_eq!(solution.deployments.len(), 1);
        assert_eq!(solution.raqes[0].deployment, solution.raqes[1].deployment);
        assert_eq!(
            solution.deployments[0].deployment.config.sketch,
            "exact-sum"
        );
    }

    #[test]
    fn solve_rejects_accuracy_no_sketch_meets() {
        // kll's max rank error 0.005 misses a 0.999 requirement (0.001).
        let config = config(&group(
            "quantile_over_time(0.99, http_requests_total[5m])",
            0.999,
        ));
        let facts = facts(&config);
        let w = build_milp_workload(&config, &facts, SCRAPE_MS).unwrap();
        let err = solve_milp(&w, &facts, &costs(), Objective::default(), false).unwrap_err();
        assert!(matches!(err, MilpError::Unservable(ids) if ids.len() == 1));
    }

    #[test]
    fn unservable_query_is_reported_once_per_item() {
        let quantile = group("quantile_over_time(0.99, http_requests_total[5m])", 0.999);
        let config = config(&(quantile.clone() + &quantile));
        let facts = facts(&config);
        let w = build_milp_workload(&config, &facts, SCRAPE_MS).unwrap();
        assert_eq!(w.raqes.len(), 2);
        let err = solve_milp(&w, &facts, &costs(), Objective::default(), false).unwrap_err();
        assert!(matches!(err, MilpError::Unservable(ids) if ids == [w.raqes[0].id.clone()]));
    }

    #[test]
    fn solve_picks_a_deployable_family_per_capability() {
        let groups = group("sum by (job) (http_requests_total)", 0.99)
            + &group("quantile_over_time(0.99, http_requests_total[5m])", 0.99)
            + &group("topk(5, http_requests_total)", 0.99);
        let config = config(&groups);
        let facts = facts(&config);
        let w = build_milp_workload(&config, &facts, SCRAPE_MS).unwrap();
        let solution = solve_milp(&w, &facts, &costs(), Objective::default(), false).unwrap();
        let chosen: BTreeMap<Capability, &str> = w
            .raqes
            .iter()
            .zip(&solution.raqes)
            .map(|(r, p)| {
                let sketch = &solution.deployments[p.deployment].deployment.config.sketch;
                (r.capability, sketch.as_str())
            })
            .collect();
        assert_eq!(chosen[&Capability::Sum], "exact-sum");
        assert_eq!(chosen[&Capability::Quantile], "kll-percall");
        assert_eq!(
            chosen[&Capability::TopKByValue],
            "cms-heap-topk-fastpath-vector2d"
        );
    }

    #[test]
    fn avg_sum_and_count_get_separate_deployments() {
        // One exact-sum accumulator can't answer both halves of the avg
        // rewrite; sharing would undercount ingest and memory.
        let config = config(&group("avg by (job) (http_requests_total)", 0.99));
        let facts = facts(&config);
        let w = build_milp_workload(&config, &facts, SCRAPE_MS).unwrap();
        let caps: Vec<_> = w.raqes.iter().map(|r| r.capability).collect();
        assert_eq!(caps.len(), 2);
        assert!(caps.contains(&Capability::Sum) && caps.contains(&Capability::Count));
        let solution = solve_milp(&w, &facts, &costs(), Objective::default(), false).unwrap();
        assert_eq!(solution.deployments.len(), 2);
    }

    #[test]
    fn weights_must_be_finite_and_non_negative() {
        assert_eq!(parse_weight("0.5"), Ok(0.5));
        assert_eq!(parse_weight("0"), Ok(0.0));
        for bad in ["-1", "NaN", "inf", "x"] {
            assert!(parse_weight(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn workload_requires_metric_hints() {
        let config: ControllerConfig = serde_yaml::from_str(&format!(
            "query_groups:\n{}",
            group("sum(http_requests_total)", 0.99)
        ))
        .unwrap();
        assert!(matches!(
            extract_hinted_items(&config, SCRAPE_MS),
            Err(MilpError::MissingMetricHints)
        ));
    }

    #[test]
    fn workload_metric_without_a_hint_is_rejected() {
        let config = config(&format!(
            "{}{}",
            group("sum(http_requests_total)", 0.99),
            group("max_over_time(unhinted[5m])", 0.99)
        ));
        assert!(matches!(
            extract_hinted_items(&config, SCRAPE_MS),
            Err(MilpError::MetricsWithoutHints(metrics)) if metrics == ["unhinted"]
        ));
    }

    #[test]
    fn spatial_only_aqe_gets_the_scrape_interval_as_its_range() {
        let config = config(&group("sum(http_requests_total)", 0.99));
        let items = extract_hinted_items(&config, SCRAPE_MS).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].requirements.data_range_ms, SCRAPE_MS);
    }

    #[test]
    fn config_to_rqes_flattens_groups() {
        let group_at = |query: &str, t_repeat_ms: u64| {
            format!("  - queries: [\"{query}\"]\n    repetition_delay_ms: {t_repeat_ms}\n")
        };
        let config = config(&format!(
            "{}{}",
            group_at("sum_over_time(a[5m])", 60_000),
            group_at("sum_over_time(b[5m])", 30_000)
        ));
        let rqes = config_to_rqes(&config);
        assert_eq!(rqes.len(), 2);
        assert_eq!(rqes[0].t_repeat_ms, 60_000);
        assert_eq!(rqes[1].t_repeat_ms, 30_000);
    }
}
