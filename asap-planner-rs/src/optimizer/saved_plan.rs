//! Writes a plan solved elsewhere, sketch-bench `autosketch_vs_asap`'s plan
//! file, as the planner's streaming and inference YAML, without re-solving.
//! The workload comes from the eval table's `e2e` block: its PromQL or SQL
//! queries, the plan RQEs each reads, and the metrics' labels.

use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::path::Path;

use anyhow::{bail, ensure, Context};
use asap_types::enums::QueryLanguage;
use rqe_optimizer::{heap_capacity, AtomicCostEntry, Deployment, LabelSet, Raqe};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::config::input::{
    ControllerConfig, ControllerOptions, SQLControllerConfig, SQLQueryGroup, TableDefinition,
};
use crate::planner_output::PlannerOutput;

use super::aqe_extractor::decompose_to_leaves;
use super::milp::{workload_from_config, MilpConfig};
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

/// The time column of the `records` tables the e2e SQL queries read.
const SQL_TIME_COLUMN: &str = "ts";

/// The metric type whose metrics are SQL tables.
const RECORDS_METRIC_TYPE: &str = "records";

/// One query, written in exactly one of PromQL and SQL.
#[derive(Deserialize)]
struct E2eQuery {
    promql: Option<String>,
    sql: Option<String>,
    interval_secs: u64,
    /// Plan RQE ids: for `promql`, one per leaf in order; for `sql`, a single
    /// leaf, every RQE it reads.
    rqes: Vec<String>,
}

impl E2eQuery {
    /// The query if it is written in `language`.
    fn text(&self, language: QueryLanguage) -> anyhow::Result<Option<&str>> {
        let (promql, sql) = match (&self.promql, &self.sql) {
            (Some(_), Some(_)) | (None, None) => bail!(
                "e2e query reading {:?} needs exactly one of `promql` and `sql`",
                self.rqes
            ),
            (promql, sql) => (promql.as_deref(), sql.as_deref()),
        };
        Ok(match language {
            QueryLanguage::promql => promql,
            QueryLanguage::sql => sql,
            other => bail!("--planner saved supports only promql and sql, not {other:?}"),
        })
    }
}

#[derive(Deserialize)]
struct E2eMetric {
    #[serde(rename = "type")]
    kind: String,
    labels: Vec<E2eLabel>,
    /// A `records` metric's value fields: its table's value columns.
    #[serde(default)]
    values: BTreeMap<String, Value>,
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

/// The selected queries' texts by repeat interval in seconds: one query group
/// each.
fn by_interval(selected: &[(&E2eQuery, &str)]) -> BTreeMap<u64, Vec<String>> {
    let mut groups: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    for (q, text) in selected {
        groups
            .entry(q.interval_secs)
            .or_default()
            .push(text.to_string());
    }
    groups
}

/// The PromQL controller config of the selected queries, with every
/// non-`records` metric's labels as hints.
fn e2e_promql_config(
    selected: &[(&E2eQuery, &str)],
    metrics: &BTreeMap<String, E2eMetric>,
) -> anyhow::Result<ControllerConfig> {
    let config = json!({
        "query_groups": by_interval(selected).iter().map(|(secs, queries)| json!({
            "queries": queries,
            "repetition_delay_ms": secs * 1000,
        })).collect::<Vec<_>>(),
        "metrics": metrics.iter().filter(|(_, m)| m.kind != RECORDS_METRIC_TYPE)
            .map(|(metric, m)| json!({
                "metric": metric,
                "labels": m.labels.iter().map(|l| &l.name).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
    });
    Ok(serde_json::from_value(config)?)
}

/// The SQL controller config of the selected queries: each `records` metric
/// is a table, its labels the metadata columns and its values the value
/// columns.
fn e2e_sql_config(
    selected: &[(&E2eQuery, &str)],
    metrics: &BTreeMap<String, E2eMetric>,
) -> SQLControllerConfig {
    SQLControllerConfig {
        query_groups: by_interval(selected)
            .into_iter()
            .map(|(secs, queries)| SQLQueryGroup {
                id: None,
                queries,
                repetition_delay_ms: secs * 1000,
                controller_options: ControllerOptions::default(),
            })
            .collect(),
        tables: metrics
            .iter()
            .filter(|(_, m)| m.kind == RECORDS_METRIC_TYPE)
            .map(|(name, m)| TableDefinition {
                name: name.clone(),
                time_column: SQL_TIME_COLUMN.to_string(),
                value_columns: m.values.keys().cloned().collect(),
                metadata_columns: m.labels.iter().map(|l| l.name.clone()).collect(),
            })
            .collect(),
        windowing: None,
        sketch_parameters: None,
        aggregate_cleanup: None,
    }
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
/// gzipped or not) over the workload of `table_path`: its queries written in
/// `language`.
pub fn saved_plan_output(
    table_path: &Path,
    plan_path: &Path,
    selector: &PlanSelector,
    language: QueryLanguage,
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
    let mut selected: Vec<(&E2eQuery, &str)> = Vec::new();
    for q in &e2e.queries {
        if let Some(text) = q.text(language)? {
            selected.push((q, text));
        }
    }
    ensure!(
        !selected.is_empty(),
        "{}: no e2e query is written in {language:?}",
        table_path.display()
    );
    let (promql, sql);
    let config = match language {
        QueryLanguage::sql => {
            sql = e2e_sql_config(&selected, &e2e.metrics);
            MilpConfig::Sql(&sql)
        }
        _ => {
            promql = e2e_promql_config(&selected, &e2e.metrics)?;
            MilpConfig::Promql(&promql)
        }
    };
    // Before the leaf split, which turns an avg into two leaves.
    reject_unwritable_queries(config)?;

    // Leaf query string and repeat interval -> the plan RQEs it reads.
    let mut rqes_of: HashMap<(String, u64), Vec<&str>> = HashMap::new();
    for &(q, text) in &selected {
        let rqes = q.rqes.iter().map(String::as_str);
        let leaves: Vec<(String, Vec<&str>)> = match config {
            // A SQL query is one leaf.
            MilpConfig::Sql(_) => {
                ensure!(!q.rqes.is_empty(), "query {text:?} names no RQEs");
                vec![(text.to_string(), rqes.collect())]
            }
            MilpConfig::Promql(_) => {
                let leaves = decompose_to_leaves(text);
                ensure!(
                    leaves.len() == q.rqes.len(),
                    "query {text:?} has {} leaves but names {} RQEs",
                    leaves.len(),
                    q.rqes.len()
                );
                leaves.into_iter().zip(rqes.map(|r| vec![r])).collect()
            }
        };
        for (leaf, rqes) in leaves {
            let key = (leaf, q.interval_secs * 1000);
            if let Some(other) = rqes_of.insert(key.clone(), rqes.clone()) {
                ensure!(
                    other == rqes,
                    "leaf {:?} every {} ms reads both {other:?} and {rqes:?}",
                    key.0,
                    key.1
                );
            }
        }
    }

    let workload = workload_from_config(config, scrape_interval_ms)?;

    // Plan deployment index -> its position in `deployments`, in order of
    // the first Raqe each serves.
    let mut position: HashMap<usize, usize> = HashMap::new();
    let mut deployments: Vec<Deployment> = Vec::new();
    let mut served_by = Vec::with_capacity(workload.raqes.len());
    for (raqe, &item) in workload.raqes.iter().zip(&workload.raqe_items) {
        let item = &workload.items[item];
        // An item's leaves (say p90 and p50 over one window), and every RQE
        // a SQL leaf reads, read one deployment, so their picks must name the
        // same sketch.
        let mut index = None;
        let mut reads: Vec<(&String, &str)> = Vec::new();
        for leaf in &item.query_strings {
            let rqes = rqes_of
                .get(&(leaf.clone(), item.t_repeat_ms))
                .with_context(|| format!("leaf {leaf:?} has no RQE in the table's e2e block"))?;
            reads.extend(rqes.iter().map(|&rqe| (leaf, rqe)));
        }
        for (leaf, rqe) in reads {
            let pick = chosen
                .get(rqe)
                .with_context(|| format!("the plan doesn't serve RQE {rqe:?}"))?;
            let deployment = Deployment {
                capability: raqe.capability,
                metric: raqe.metric.clone(),
                spatial_filter: raqe.spatial_filter.clone(),
                grouping_labels: pick
                    .deployment_grouping
                    .clone()
                    .unwrap_or_else(|| raqe.grouping_labels.clone()),
                config: cost_entry(pick)?,
                window_ms: pick.window_secs * 1000,
                slide_ms: pick.slide_secs * 1000,
                key_tracker: None,
            };
            ensure!(
                can_serve(raqe, &deployment),
                "RQE {rqe:?}'s {} {}s window sliding every {}s can't serve {leaf:?} \
                 ({} ms lookback every {} ms); does --plan match --workload-table?",
                pick.sketch,
                pick.window_secs,
                pick.slide_secs,
                raqe.lookback_ms,
                raqe.interval_ms
            );
            let at = *position.entry(pick.deployment).or_insert_with(|| {
                deployments.push(deployment.clone());
                deployments.len() - 1
            });
            // One plan deployment is one sketch: every Raqe it serves must agree.
            ensure!(
                deployments[at] == deployment,
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
        config,
        &workload,
        &deployments,
        &served_by,
    )?)
}

/// The rules of sketch-bench's `is_eligible` that need no workload facts or
/// accuracy: a pick that breaks them belongs to another workload table.
/// Grouping is left to the converter's roll-up check.
fn can_serve(r: &Raqe, d: &Deployment) -> bool {
    let properties = d.properties();
    d.window_ms != 0
        && d.slide_ms != 0
        && d.window_ms.is_multiple_of(d.slide_ms)
        && r.lookback_ms.is_multiple_of(d.window_ms)
        && r.interval_ms.is_multiple_of(d.slide_ms)
        && (properties.mergeable_across_windows || r.lookback_ms == d.window_ms)
        && !properties.needs_delta_set_key_tracker
        && heap_capacity(&d.config).is_none_or(|heap| {
            d.heap_needed(r.lookback_ms, r.topk_k())
                .is_some_and(|need| heap >= need)
        })
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
        run_on(TABLE, plan.to_string().as_bytes(), sla_ms)
    }

    fn run_on(table: &str, plan: &[u8], sla_ms: Option<f64>) -> anyhow::Result<PlannerOutput> {
        run_in(table, plan, sla_ms, QueryLanguage::promql)
    }

    fn run_in(
        table: &str,
        plan: &[u8],
        sla_ms: Option<f64>,
        language: QueryLanguage,
    ) -> anyhow::Result<PlannerOutput> {
        let dir = tempfile::tempdir().unwrap();
        let (table_path, plan_path) = (dir.path().join("table.json"), dir.path().join("plan.json"));
        std::fs::write(&table_path, table).unwrap();
        std::fs::write(&plan_path, plan).unwrap();
        let selector = PlanSelector {
            method: "asap",
            weights: "cpu",
            sla_ms,
        };
        saved_plan_output(&table_path, &plan_path, &selector, language, 1000)
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

    /// A DDSketch grouped by (job, label_0) serves `quantile by (job)` as a
    /// roll-up: one aggregation at the deployment's grouping.
    #[test]
    fn writes_a_sketch_roll_up_at_the_deployment_grouping() {
        let table = r#"{"workloads": [{"e2e": {
            "queries": [
                {"promql": "quantile by (job) (0.5, data)", "interval_secs": 1, "rqes": ["p50"]}
            ],
            "metrics": {"data": {"type": "gauge", "labels": [
                {"name": "job", "cardinality": 10},
                {"name": "label_0", "cardinality": 1000, "child_of": "job"}
            ]}}
        }}]}"#;
        let mut p50 = pick("p50", 0, "dd", json!({"alpha": 0.02}), 1);
        p50["deployment_grouping"] = json!(["job", "label_0"]);
        let plan = json!({"plans": [
            {"method": "asap", "weights": "cpu", "kind": "cheapest", "chosen": [p50]},
        ]});

        let output = run_on(table, plan.to_string().as_bytes(), None).unwrap();

        let streaming = output
            .to_streaming_config(asap_types::enums::QueryLanguage::promql)
            .unwrap();
        let aggregations: Vec<_> = streaming.aggregation_configs.values().collect();
        let [aggregation] = aggregations.as_slice() else {
            panic!("expected one aggregation, got {aggregations:?}");
        };
        assert_eq!(
            aggregation.aggregation_type,
            promql_utilities::query_logics::enums::AggregationType::DDSketch
        );
        assert_eq!(
            aggregation.grouping_labels,
            promql_utilities::data_model::KeyByLabelNames::new(vec![
                "job".into(),
                "label_0".into()
            ])
        );
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

    /// The exact sum is a keyed MultipleSum, so the roll-up reads it at the
    /// deployment's grouping and sums by `job` at query time.
    #[test]
    fn writes_an_exact_sum_roll_up_as_a_query_time_sum() {
        let mut plan = plans(shared_p50());
        plan["plans"][0]["chosen"][0]["deployment_grouping"] = json!(["job", "label_0"]);
        let inference = run(plan, None)
            .unwrap()
            .to_inference_config(asap_types::enums::QueryLanguage::promql)
            .unwrap();
        let query_config = inference
            .query_configs
            .iter()
            .find(|q| q.query == "sum by (job) (data)")
            .expect("a query_config for the roll-up");
        assert_eq!(
            query_config.planned_subquery,
            "sum by (job, label_0) (data)"
        );
        let [stage] = query_config.query_time_aggregations.as_slice() else {
            panic!("{:?}", query_config.query_time_aggregations);
        };
        assert_eq!(
            serde_json::to_value(stage).unwrap(),
            json!({
                "operator": "sum",
                "grouping": {"mode": "by", "labels": ["job"]},
                "parameter": null,
            })
        );
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
        let err = run_on(&table, plans(shared_p50()).to_string().as_bytes(), None)
            .err()
            .expect("refused")
            .to_string();
        assert!(err.contains("avg"), "{err}");
        assert!(!err.contains("leaves"), "{err}");
    }

    #[test]
    fn reads_a_gzipped_plan() {
        use std::io::Write;
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(plans(shared_p50()).to_string().as_bytes())
            .unwrap();
        let output = run_on(TABLE, &gz.finish().unwrap(), None).unwrap();
        assert_eq!(output.streaming_aggregation_count(), 2);
    }

    #[test]
    fn refuses_a_query_naming_too_few_rqes() {
        let table = TABLE.replace(r#""rqes": ["p90", "p50"]"#, r#""rqes": ["p90"]"#);
        let err = run_on(&table, plans(shared_p50()).to_string().as_bytes(), None)
            .err()
            .expect("refused")
            .to_string();
        assert!(err.contains("has 2 leaves but names 1 RQEs"), "{err}");
    }

    #[test]
    fn refuses_a_query_on_two_deployments() {
        // The ratio's legs share one item, so they must share a deployment.
        let p50 = pick("p50", 1, "dd", json!({"alpha": 0.02}), 300);
        let err = refusal(plans(p50), None);
        assert!(err.contains("served by two deployments"), "{err}");
    }

    #[test]
    fn refuses_a_pick_needing_a_key_tracker() {
        let p50 = pick("p50", 0, "hydra-kll", json!({}), 300);
        let err = refusal(plans(p50), None);
        assert!(err.contains("does --plan match --workload-table"), "{err}");
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

    /// An `all`-style table: a classic PromQL query, and SQL queries on the
    /// `records` table `http`. The distinct-users query reads two RQEs.
    const MIXED_TABLE: &str = r#"{"workloads": [{"e2e": {
        "queries": [
            {"promql": "sum by (job) (data)", "interval_secs": 1, "rqes": ["sum"]},
            {"sql": "SELECT service, COUNT(DISTINCT user_id) FROM http WHERE ts BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY service",
             "interval_secs": 60, "rqes": ["users_1pct", "users_5pct"]},
            {"sql": "SELECT region, QUANTILE(0.99, latency) FROM http WHERE ts BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY region",
             "interval_secs": 60, "rqes": ["p99"]}
        ],
        "metrics": {
            "data": {"type": "gauge", "labels": [{"name": "job"}, {"name": "label_0"}]},
            "http": {"type": "records", "labels": [{"name": "region"}, {"name": "service"}],
                     "values": {"latency": {}, "user_id": {}}}
        }
    }}]}"#;

    /// The distinct-users RQEs share an HLL rolled up from (region, service).
    fn mixed_plan(users_5pct_deployment: usize) -> Value {
        let hll = |rqe: &str, deployment: usize| {
            let mut p = pick(rqe, deployment, "hll", json!({"lg_k": 12}), 60);
            p["deployment_grouping"] = json!(["region", "service"]);
            p
        };
        json!({"plans": [{"method": "asap", "weights": "cpu", "kind": "cheapest", "chosen": [
            pick("sum", 3, "exact-sum", json!({}), 1),
            hll("users_1pct", 1),
            hll("users_5pct", users_5pct_deployment),
            pick("p99", 0, "dd", json!({"alpha": 0.01}), 300),
        ]}]})
    }

    fn run_mixed(plan: Value, language: QueryLanguage) -> anyhow::Result<PlannerOutput> {
        run_in(MIXED_TABLE, plan.to_string().as_bytes(), None, language)
    }

    #[test]
    fn writes_the_sql_queries_over_records_tables() {
        let output = run_mixed(mixed_plan(1), QueryLanguage::sql).unwrap();

        let inference = output.to_inference_config(QueryLanguage::sql).unwrap();
        let mut queries: Vec<&str> = inference
            .query_configs
            .iter()
            .map(|q| q.query.as_str())
            .collect();
        queries.sort();
        assert_eq!(
            queries,
            [
                "SELECT region, QUANTILE(0.99, latency) FROM http WHERE ts BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY region",
                "SELECT service, COUNT(DISTINCT user_id) FROM http WHERE ts BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY service",
            ]
        );
        let streaming = output.to_streaming_config(QueryLanguage::sql).unwrap();
        let mut aggregations: Vec<_> = streaming
            .aggregation_configs
            .values()
            .map(|a| {
                (
                    a.aggregation_type,
                    a.grouping_labels.labels.clone(),
                    a.table_name.clone(),
                    a.value_column.clone(),
                )
            })
            .collect();
        aggregations.sort_by_key(|a| a.3.clone());
        assert_eq!(
            aggregations,
            [
                (
                    promql_utilities::query_logics::enums::AggregationType::DDSketch,
                    vec!["region".to_string()],
                    Some("http".to_string()),
                    Some("latency".to_string()),
                ),
                (
                    promql_utilities::query_logics::enums::AggregationType::HLL,
                    vec!["region".to_string(), "service".to_string()],
                    Some("http".to_string()),
                    Some("user_id".to_string()),
                ),
            ]
        );
    }

    /// The PromQL run of the same table skips its SQL queries.
    #[test]
    fn writes_only_the_promql_queries_of_a_mixed_table() {
        let output = run_mixed(mixed_plan(1), QueryLanguage::promql).unwrap();
        assert_eq!(output.inference_query_count(), 1);
        assert_eq!(output.streaming_aggregation_count(), 1);
    }

    #[test]
    fn refuses_a_sql_query_whose_rqes_are_on_two_deployments() {
        let err = run_mixed(mixed_plan(2), QueryLanguage::sql)
            .err()
            .expect("refused")
            .to_string();
        assert!(err.contains("served by two deployments"), "{err}");
    }

    #[test]
    fn refuses_a_sql_pick_from_another_table() {
        let mut plan = mixed_plan(1);
        // A 7s window doesn't divide the 5m lookback.
        plan["plans"][0]["chosen"][3] = pick("p99", 0, "dd", json!({"alpha": 0.01}), 7);
        let err = run_mixed(plan, QueryLanguage::sql)
            .err()
            .expect("refused")
            .to_string();
        assert!(err.contains("does --plan match --workload-table"), "{err}");
    }

    #[test]
    fn refuses_a_table_with_no_query_in_the_language() {
        let err = run_in(
            TABLE,
            plans(shared_p50()).to_string().as_bytes(),
            None,
            QueryLanguage::sql,
        )
        .err()
        .expect("refused")
        .to_string();
        assert!(err.contains("no e2e query is written in sql"), "{err}");
    }

    #[test]
    fn refuses_a_query_in_both_languages() {
        let table = MIXED_TABLE.replace(
            r#"{"promql": "sum by (job) (data)","#,
            r#"{"promql": "sum by (job) (data)", "sql": "SELECT 1","#,
        );
        let err = run_in(
            &table,
            mixed_plan(1).to_string().as_bytes(),
            None,
            QueryLanguage::sql,
        )
        .err()
        .expect("refused")
        .to_string();
        assert!(err.contains("exactly one of `promql` and `sql`"), "{err}");
    }

    #[test]
    fn refuses_a_plan_missing_an_rqe() {
        let mut plan = plans(shared_p50());
        plan["plans"][0]["chosen"].as_array_mut().unwrap().pop();
        let err = refusal(plan, None);
        assert!(err.contains(r#"doesn't serve RQE "p50""#), "{err}");
    }
}
