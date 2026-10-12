//! Writes a plan solved elsewhere, sketch-bench `autosketch_vs_asap`'s plan
//! file, as the planner's streaming and inference YAML, without re-solving.
//! The workload comes from the eval table's `e2e` block: its PromQL queries,
//! the plan RQEs each reads, and the metrics' labels.

use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::path::Path;

use anyhow::{bail, ensure, Context};
use rqe_optimizer::{AtomicCostEntry, Deployment, LabelSet};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::config::input::ControllerConfig;
use crate::planner_output::PlannerOutput;

use super::aqe_extractor::decompose_to_leaves;
use super::milp::workload_from_config;
use super::milp_output::{deployments_to_planner_output, reject_unwritable_queries};

#[derive(Deserialize)]
struct Table {
    workloads: Vec<TableWorkload>,
}

#[derive(Deserialize)]
struct TableWorkload {
    e2e: Option<E2e>,
}

#[derive(Deserialize)]
struct E2e {
    queries: Vec<E2eQuery>,
    metrics: BTreeMap<String, E2eMetric>,
}

#[derive(Deserialize)]
struct E2eQuery {
    promql: String,
    interval_secs: u64,
    /// Plan RQE ids, one per leaf of `promql` in order.
    rqes: Vec<String>,
}

#[derive(Deserialize)]
struct E2eMetric {
    labels: Vec<E2eLabel>,
}

#[derive(Deserialize)]
struct E2eLabel {
    name: String,
}

#[derive(Deserialize)]
struct PlanFile {
    plans: Vec<Plan>,
}

#[derive(Deserialize)]
struct Plan {
    method: String,
    weights: String,
    kind: String,
    sla_ms: Option<f64>,
    chosen: Vec<Chosen>,
}

#[derive(Deserialize)]
struct Chosen {
    rqe: String,
    sketch: String,
    params: Value,
    window_secs: u64,
    slide_secs: u64,
    /// Index of the plan's deployment.
    deployment: usize,
    /// Present only when the deployment's grouping differs from the RQE's
    /// (a roll-up), which the converter refuses.
    deployment_grouping: Option<LabelSet>,
}

/// Which plan of the file to write.
pub struct PlanSelector<'a> {
    pub method: &'a str,
    pub weights: &'a str,
    /// `None` for the cheapest plan, else the plan at this latency SLA.
    pub sla_ms: Option<f64>,
}

/// The controller config the table's `e2e` block describes: one query group
/// per repeat interval.
fn e2e_config(e2e: &E2e) -> anyhow::Result<ControllerConfig> {
    let mut by_interval: BTreeMap<u64, Vec<&str>> = BTreeMap::new();
    for q in &e2e.queries {
        by_interval
            .entry(q.interval_secs)
            .or_default()
            .push(&q.promql);
    }
    let config = json!({
        "query_groups": by_interval.iter().map(|(secs, queries)| json!({
            "queries": queries,
            "repetition_delay_ms": secs * 1000,
        })).collect::<Vec<_>>(),
        "metrics": e2e.metrics.iter().map(|(metric, m)| json!({
            "metric": metric,
            "labels": m.labels.iter().map(|l| &l.name).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    });
    Ok(serde_json::from_value(config)?)
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> anyhow::Result<T> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let text = if bytes.starts_with(&[0x1f, 0x8b]) {
        let mut text = String::new();
        flate2::read::GzDecoder::new(&bytes[..]).read_to_string(&mut text)?;
        text
    } else {
        String::from_utf8(bytes)?
    };
    serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

/// Streaming and inference YAML for the selected plan of `plan_path` (JSON,
/// gzipped or not) over the workload of `table_path`.
pub fn saved_plan_output(
    table_path: &Path,
    plan_path: &Path,
    selector: &PlanSelector,
    scrape_interval_ms: u64,
) -> anyhow::Result<PlannerOutput> {
    let table: Table = read_json(table_path)?;
    let [TableWorkload { e2e: Some(e2e) }] = table.workloads.as_slice() else {
        bail!(
            "{}: expected one workload with an `e2e` block",
            table_path.display()
        );
    };
    let plans: PlanFile = read_json(plan_path)?;
    let kind = if selector.sla_ms.is_some() {
        "sla"
    } else {
        "cheapest"
    };
    let matching: Vec<&Plan> = plans
        .plans
        .iter()
        .filter(|p| {
            p.method == selector.method
                && p.weights == selector.weights
                && p.kind == kind
                && p.sla_ms == selector.sla_ms
        })
        .collect();
    let [plan] = matching.as_slice() else {
        bail!(
            "{}: {} plans match method {:?}, weights {:?}, {kind} {:?}; expected one",
            plan_path.display(),
            matching.len(),
            selector.method,
            selector.weights,
            selector.sla_ms,
        );
    };
    let mut chosen: HashMap<&str, &Chosen> = HashMap::new();
    for c in &plan.chosen {
        ensure!(
            chosen.insert(&c.rqe, c).is_none(),
            "the plan picks RQE {:?} twice",
            c.rqe
        );
    }
    let config = e2e_config(e2e)?;
    // Before the leaf split, which turns an avg into two leaves.
    reject_unwritable_queries(&config)?;

    // Leaf query string and repeat interval -> the plan RQE it reads.
    let mut rqe_of: HashMap<(String, u64), &str> = HashMap::new();
    for q in &e2e.queries {
        let leaves = decompose_to_leaves(&q.promql);
        ensure!(
            leaves.len() == q.rqes.len(),
            "query {:?} has {} leaves but names {} RQEs",
            q.promql,
            leaves.len(),
            q.rqes.len()
        );
        for (leaf, rqe) in leaves.into_iter().zip(&q.rqes) {
            let key = (leaf, q.interval_secs * 1000);
            if let Some(other) = rqe_of.insert(key.clone(), rqe) {
                ensure!(
                    other == rqe,
                    "leaf {:?} every {} ms reads both {other:?} and {rqe:?}",
                    key.0,
                    key.1
                );
            }
        }
    }

    let workload = workload_from_config(&config, scrape_interval_ms)?;

    // Plan deployment index -> its position in `deployments`, in order of
    // the first Raqe each serves.
    let mut position: HashMap<usize, usize> = HashMap::new();
    let mut deployments: Vec<Deployment> = Vec::new();
    let mut served_by = Vec::with_capacity(workload.raqes.len());
    for (raqe, &item) in workload.raqes.iter().zip(&workload.raqe_items) {
        let item = &workload.items[item];
        // An item's leaves (say p90 and p50 over one window) read one
        // deployment, so their picks must name the same sketch.
        let mut index = None;
        for leaf in &item.query_strings {
            let rqe = rqe_of
                .get(&(leaf.clone(), item.t_repeat_ms))
                .with_context(|| format!("leaf {leaf:?} has no RQE in the table's e2e block"))?;
            let pick = chosen
                .get(rqe)
                .with_context(|| format!("the plan doesn't serve RQE {rqe:?}"))?;
            let (window_ms, slide_ms) = (pick.window_secs * 1000, pick.slide_secs * 1000);
            // sketch-bench's `is_eligible` window rules: a pick that breaks
            // them belongs to another workload table.
            ensure!(
                slide_ms != 0
                    && window_ms.is_multiple_of(slide_ms)
                    && raqe.lookback_ms.is_multiple_of(window_ms)
                    && raqe.interval_ms.is_multiple_of(slide_ms),
                "RQE {rqe:?}'s {}s window sliding every {}s can't serve {leaf:?} \
                 ({} ms lookback every {} ms); does --plan match --workload-table?",
                pick.window_secs,
                pick.slide_secs,
                raqe.lookback_ms,
                raqe.interval_ms
            );
            let deployment = Deployment {
                capability: raqe.capability,
                metric: raqe.metric.clone(),
                spatial_filter: raqe.spatial_filter.clone(),
                grouping_labels: pick
                    .deployment_grouping
                    .clone()
                    .unwrap_or_else(|| raqe.grouping_labels.clone()),
                config: cost_entry(pick)?,
                window_ms,
                slide_ms,
                key_tracker: None,
            };
            let at = *position.entry(pick.deployment).or_insert_with(|| {
                deployments.push(deployment.clone());
                deployments.len() - 1
            });
            // One plan deployment is one sketch: every Raqe it serves must agree.
            ensure!(
                same_sketch(&deployments[at], &deployment),
                "plan deployment {} serves Raqes that need different sketches: {:?} and {deployment:?}",
                pick.deployment,
                deployments[at]
            );
            ensure!(
                index.is_none_or(|index| index == at),
                "query {:?} is served by two deployments",
                item.query_strings
            );
            index = Some(at);
        }
        let index = index.expect("an item has a query string");
        served_by.push(index);
    }
    Ok(deployments_to_planner_output(
        &config,
        &workload,
        &deployments,
        &served_by,
    )?)
}

fn same_sketch(a: &Deployment, b: &Deployment) -> bool {
    (
        a.capability,
        &a.metric,
        &a.spatial_filter,
        &a.grouping_labels,
        &a.config.sketch,
        &a.config.sketch_config,
        a.window_ms,
        a.slide_ms,
    ) == (
        b.capability,
        &b.metric,
        &b.spatial_filter,
        &b.grouping_labels,
        &b.config.sketch,
        &b.config.sketch_config,
        b.window_ms,
        b.slide_ms,
    )
}

/// The sketch config the YAML is written from. Costs and accuracy don't
/// reach the YAML, so they are zero.
fn cost_entry(pick: &Chosen) -> anyhow::Result<AtomicCostEntry> {
    Ok(serde_json::from_value(json!({
        "sketch": pick.sketch,
        "sketch_config": {"algorithm": pick.sketch, "params": pick.params},
        "mem_bytes_per_instance": 0.0,
        "insert_cpu_secs": 0.0,
        "merge_cpu_secs": 0.0,
        "query_cpu_secs": 0.0,
        "query_accuracy": {},
        "accuracy_metric": "",
        "measured_at": {
            "items_per_instance": 0,
            "keys_per_instance": null,
            "value_range": null,
            "merge_operand_items": null,
            "distribution": null,
        },
    }))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: &str = r#"{"workloads": [{"e2e": {
        "queries": [
            {"promql": "sum by (job) (data)", "interval_secs": 1, "rqes": ["sum"]},
            {"promql": "quantile_over_time(0.9, data[5m]) / quantile_over_time(0.5, data[5m])",
             "interval_secs": 60, "rqes": ["p90", "p50"]},
            {"promql": "quantile_over_time(0.5, data[5m])", "interval_secs": 60, "rqes": ["p50"]}
        ],
        "metrics": {"data": {"type": "gauge", "labels": [
            {"name": "job", "cardinality": 10},
            {"name": "label_0", "cardinality": 1000, "child_of": "job"}
        ]}}
    }}]}"#;

    fn pick(rqe: &str, deployment: usize, sketch: &str, params: Value, window: u64) -> Value {
        json!({"rqe": rqe, "deployment": deployment, "sketch": sketch, "params": params,
               "window_secs": window, "slide_secs": 60.min(window)})
    }

    /// The cheapest `asap`/`cpu` plan: the ratio's legs share one sliding
    /// DDSketch. The SLA plan differs only in alpha.
    fn plans(p50: Value) -> Value {
        let dd = |alpha: f64| json!({"alpha": alpha});
        json!({"plans": [
            {"method": "asap", "weights": "cpu", "kind": "cheapest", "chosen": [
                pick("sum", 3, "exact-sum", json!({}), 1),
                pick("p90", 0, "dd", dd(0.02), 300),
                p50,
            ]},
            {"method": "asap", "weights": "cpu", "kind": "sla", "sla_ms": 100.0, "chosen": [
                pick("sum", 3, "exact-sum", json!({}), 1),
                pick("p90", 0, "dd", dd(0.01), 300),
                pick("p50", 0, "dd", dd(0.01), 300),
            ]},
        ]})
    }

    fn run(plan: Value, sla_ms: Option<f64>) -> anyhow::Result<PlannerOutput> {
        run_on(TABLE, plan, sla_ms)
    }

    fn run_on(table: &str, plan: Value, sla_ms: Option<f64>) -> anyhow::Result<PlannerOutput> {
        let dir = tempfile::tempdir().unwrap();
        let (table_path, plan_path) = (dir.path().join("table.json"), dir.path().join("plan.json"));
        std::fs::write(&table_path, table).unwrap();
        std::fs::write(&plan_path, plan.to_string()).unwrap();
        let selector = PlanSelector {
            method: "asap",
            weights: "cpu",
            sla_ms,
        };
        saved_plan_output(&table_path, &plan_path, &selector, 1000)
    }

    fn refusal(plan: Value, sla_ms: Option<f64>) -> String {
        run(plan, sla_ms).err().expect("refused").to_string()
    }

    fn shared_p50() -> Value {
        pick("p50", 0, "dd", json!({"alpha": 0.02}), 300)
    }

    #[test]
    fn writes_one_aggregation_per_plan_deployment() {
        let output = run(plans(shared_p50()), None).unwrap();
        assert_eq!(output.streaming_aggregation_count(), 2);
        // The ratio's two leaves and the sum; the standalone p50 is a leaf already.
        assert_eq!(output.inference_query_count(), 3);
        let yaml = output.to_streaming_yaml_string().unwrap();
        assert!(yaml.contains("windowSizeMs: 300000"), "{yaml}");
        assert!(yaml.contains("slideIntervalMs: 60000"), "{yaml}");
        assert!(yaml.contains("windowType: sliding"), "{yaml}");
        assert!(yaml.contains("alpha: 0.02"), "{yaml}");
    }

    #[test]
    fn sla_ms_selects_the_plan_at_that_sla() {
        let yaml = run(plans(shared_p50()), Some(100.0))
            .unwrap()
            .to_streaming_yaml_string()
            .unwrap();
        assert!(yaml.contains("alpha: 0.01"), "{yaml}");
        let err = refusal(plans(shared_p50()), Some(300.0));
        assert!(err.contains("0 plans match"), "{err}");
    }

    #[test]
    fn refuses_a_roll_up() {
        let mut plan = plans(shared_p50());
        plan["plans"][0]["chosen"][0]["deployment_grouping"] = json!(["job", "label_0"]);
        let err = refusal(plan, None);
        assert!(err.contains("roll-up"), "{err}");
    }

    #[test]
    fn refuses_a_pick_from_another_table() {
        // A 7s window doesn't divide the 5m lookback.
        let p50 = pick("p50", 0, "dd", json!({"alpha": 0.02}), 7);
        let err = refusal(plans(p50), None);
        assert!(err.contains("does --plan match --workload-table"), "{err}");
    }

    #[test]
    fn refuses_an_avg_before_splitting_it() {
        let table = TABLE.replace(
            r#""sum by (job) (data)", "interval_secs": 1"#,
            r#""avg by (job) (data)", "interval_secs": 1"#,
        );
        let err = run_on(&table, plans(shared_p50()), None)
            .err()
            .expect("refused")
            .to_string();
        assert!(err.contains("avg"), "{err}");
        assert!(!err.contains("leaves"), "{err}");
    }

    #[test]
    fn refuses_an_rqe_picked_twice() {
        let mut plan = plans(shared_p50());
        plan["plans"][0]["chosen"]
            .as_array_mut()
            .unwrap()
            .push(shared_p50());
        let err = refusal(plan, None);
        assert!(err.contains(r#"picks RQE "p50" twice"#), "{err}");
    }

    #[test]
    fn refuses_one_deployment_with_two_sketches() {
        let p50 = pick("p50", 0, "dd", json!({"alpha": 0.05}), 300);
        let err = refusal(plans(p50), None);
        assert!(err.contains("different sketches"), "{err}");
    }

    #[test]
    fn refuses_a_plan_missing_an_rqe() {
        let mut plan = plans(shared_p50());
        plan["plans"][0]["chosen"].as_array_mut().unwrap().pop();
        let err = refusal(plan, None);
        assert!(err.contains(r#"doesn't serve RQE "p50""#), "{err}");
    }
}
