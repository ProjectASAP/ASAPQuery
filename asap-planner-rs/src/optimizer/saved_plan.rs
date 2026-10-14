//! Writes a plan solved elsewhere, sketch-bench `autosketch_vs_asap`'s plan
//! file, as the planner's streaming and inference YAML, without re-solving.
//! The workload comes from the eval table's `e2e` block: its PromQL or SQL
//! queries, the plan RQEs each reads, and the metrics' labels. A query's RQE
//! expression tree (`expr`) also gets each of its leaves an `rqe_configs`
//! entry, so the engine can answer the tree without parsing PromQL or SQL.

use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::path::Path;

use anyhow::{bail, ensure, Context};
use asap_types::enums::QueryLanguage;
use asap_types::query_requirements::QueryRequirements;
use asap_types::rqe_expr::{RqeCapability, RqeExpr, RqeLeaf};
use asap_types::utils::normalize_spatial_filter;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;
use rqe_optimizer::{heap_capacity, AtomicCostEntry, Deployment, LabelSet, Raqe};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::config::input::{
    ControllerConfig, ControllerOptions, SQLControllerConfig, SQLQueryGroup, TableDefinition,
};
use crate::planner_output::PlannerOutput;

use super::aqe_extractor::{decompose_to_leaves, sql_metric};
use super::milp::{workload_from_config, MilpConfig};
use super::milp_output::{deployments_to_planner_output, reject_unwritable_queries};
use super::solution::OptimizerItem;

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

/// One query: written in at most one of PromQL and SQL, and with an RQE
/// expression tree, `expr`, or not. It needs text or a tree.
#[derive(Deserialize)]
struct E2eQuery {
    promql: Option<String>,
    sql: Option<String>,
    interval_secs: u64,
    /// Plan RQE ids: for `promql`, one per leaf in order; for `sql`, a single
    /// leaf, every RQE it reads. With `expr`, its leaves' ids, which this
    /// may omit.
    #[serde(default)]
    rqes: Vec<String>,
    /// Leaves name plan RQE ids.
    expr: Option<RqeExpr>,
}

impl E2eQuery {
    /// The query's PromQL or SQL text, if any.
    fn text(&self) -> anyhow::Result<Option<&str>> {
        match (&self.promql, &self.sql) {
            (Some(_), Some(_)) => bail!(
                "e2e query reading {:?} needs exactly one of `promql` and `sql`",
                self.rqes
            ),
            (promql, sql) => Ok(promql.as_deref().or(sql.as_deref())),
        }
    }

    /// Whether the query is SQL: written in `sql`, or, with no text, a tree
    /// whose leaves read `records` metrics (SQL tables).
    fn is_sql(&self, metrics: &BTreeMap<String, E2eMetric>) -> anyhow::Result<bool> {
        let text_sql = self.text()?.map(|_| self.sql.is_some());
        let tree_sql = match &self.expr {
            None => None,
            Some(expr) => {
                let mut kinds = Vec::new();
                for leaf in expr.leaves() {
                    let metric = metrics.get(&leaf.metric).with_context(|| {
                        format!("RQE {:?} reads unknown metric {:?}", leaf.id, leaf.metric)
                    })?;
                    kinds.push(metric.kind == RECORDS_METRIC_TYPE);
                }
                kinds.dedup();
                match kinds.as_slice() {
                    [] => bail!("e2e query reading {:?}: its `expr` reads no RQE", self.rqes),
                    [sql] => Some(*sql),
                    _ => bail!(
                        "e2e query reading {:?}: its `expr` mixes records and non-records metrics",
                        self.rqes
                    ),
                }
            }
        };
        match (text_sql, tree_sql) {
            (Some(text), Some(tree)) if text != tree => bail!(
                "e2e query reading {:?} is written in {} but its `expr` reads {} metrics",
                self.rqes,
                if text { "SQL" } else { "PromQL" },
                if tree { "records" } else { "non-records" }
            ),
            (Some(sql), _) | (None, Some(sql)) => Ok(sql),
            (None, None) => bail!(
                "e2e query reading {:?} needs `promql`, `sql` or `expr`",
                self.rqes
            ),
        }
    }

    /// The plan RQEs the query reads: its tree's leaves, if it has one,
    /// which `rqes` must then list in order or omit.
    fn rqe_ids(&self) -> anyhow::Result<Vec<&str>> {
        let rqes: Vec<&str> = self.rqes.iter().map(String::as_str).collect();
        let Some(expr) = &self.expr else {
            return Ok(rqes);
        };
        let leaves: Vec<&str> = expr.leaves().iter().map(|l| l.id.as_str()).collect();
        ensure!(
            rqes.is_empty() || rqes == leaves,
            "e2e query names RQEs {rqes:?} but its `expr` reads {leaves:?}"
        );
        Ok(leaves)
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
    /// Fractional below a second: an instant query's window is one scrape.
    window_secs: f64,
    slide_secs: f64,
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
) -> anyhow::Result<SQLControllerConfig> {
    let mut tables = Vec::new();
    for (name, m) in metrics
        .iter()
        .filter(|(_, m)| m.kind == RECORDS_METRIC_TYPE)
    {
        ensure!(
            !m.values.is_empty(),
            "records metric {name:?} has no values"
        );
        tables.push(TableDefinition {
            name: name.clone(),
            time_column: SQL_TIME_COLUMN.to_string(),
            value_columns: m.values.keys().cloned().collect(),
            metadata_columns: m.labels.iter().map(|l| l.name.clone()).collect(),
        });
    }
    Ok(SQLControllerConfig {
        query_groups: by_interval(selected)
            .into_iter()
            .map(|(secs, queries)| SQLQueryGroup {
                id: None,
                queries,
                repetition_delay_ms: secs * 1000,
                controller_options: ControllerOptions::default(),
            })
            .collect(),
        tables,
        windowing: None,
        sketch_parameters: None,
        aggregate_cleanup: None,
    })
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
    let sql = match language {
        QueryLanguage::sql => true,
        QueryLanguage::promql => false,
        other => {
            bail!("--planner saved supports only --query-language promql or sql, not {other:?}")
        }
    };
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
    // Queries in `language`: those with text, and those with a tree.
    let mut selected: Vec<(&E2eQuery, &str)> = Vec::new();
    let mut trees: Vec<(&E2eQuery, &RqeExpr)> = Vec::new();
    for q in &e2e.queries {
        if let Some(expr) = &q.expr {
            expr.validate()
                .with_context(|| format!("e2e query reading {:?}: invalid `expr`", q.rqes))?;
        }
        if q.is_sql(&e2e.metrics)? != sql {
            continue;
        }
        if let Some(text) = q.text()? {
            selected.push((q, text));
        }
        if let Some(expr) = &q.expr {
            trees.push((q, expr));
        }
    }
    ensure!(
        !selected.is_empty() || !trees.is_empty(),
        "{}: no e2e query is written in {language:?}",
        table_path.display()
    );
    let (promql_config, sql_config);
    let config = if sql {
        sql_config = e2e_sql_config(&selected, &e2e.metrics)?;
        MilpConfig::Sql(&sql_config)
    } else {
        promql_config = e2e_promql_config(&selected, &e2e.metrics)?;
        MilpConfig::Promql(&promql_config)
    };
    // The metric and table checks `--planner milp` makes.
    config.hints()?;
    // Before the leaf split, which turns an avg into two leaves.
    reject_unwritable_queries(config)?;

    // Leaf query string and repeat interval -> the plan RQEs it reads.
    let mut rqes_of: HashMap<(String, u64), Vec<&str>> = HashMap::new();
    for &(q, text) in &selected {
        let rqe_ids = q.rqe_ids()?;
        let leaves: Vec<(String, Vec<&str>)> = match config {
            // A SQL query is one leaf, and its RQEs a set.
            MilpConfig::Sql(_) => {
                ensure!(!rqe_ids.is_empty(), "query {text:?} names no RQEs");
                let mut rqes = rqe_ids;
                rqes.sort_unstable();
                vec![(text.to_string(), rqes)]
            }
            MilpConfig::Promql(_) => {
                let leaves = decompose_to_leaves(text);
                ensure!(
                    leaves.len() == rqe_ids.len(),
                    "query {text:?} has {} leaves but names {} RQEs",
                    leaves.len(),
                    rqe_ids.len()
                );
                leaves
                    .into_iter()
                    .zip(rqe_ids.into_iter().map(|r| vec![r]))
                    .collect()
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

    // Every tree leaf, by RQE id, and the repeat interval of its queries.
    let mut tree_leaves: BTreeMap<&str, (&RqeLeaf, u64)> = BTreeMap::new();
    for (q, expr) in &trees {
        // A tree-only query's `rqes` must agree with its tree too.
        q.rqe_ids()?;
        for leaf in expr.leaves() {
            let interval_ms = q.interval_secs * 1000;
            if let Some((other, other_interval_ms)) =
                tree_leaves.insert(&leaf.id, (leaf, interval_ms))
            {
                ensure!(
                    other == leaf && other_interval_ms == interval_ms,
                    "RQE {:?} is two different leaves: {other:?} every {other_interval_ms} ms \
                     and {leaf:?} every {interval_ms} ms",
                    leaf.id
                );
            }
        }
    }

    let mut workload = workload_from_config(config, scrape_interval_ms)?;
    let text_items = workload.items.len();
    // Each tree leaf is one more item and Raqe, read directly by its RQE
    // id; `leaf_rqes[i]` is item `text_items + i`'s.
    let mut rqe_raqes: BTreeMap<String, usize> = BTreeMap::new();
    let mut leaf_rqes: Vec<&str> = Vec::new();
    for (&id, &(leaf, interval_ms)) in &tree_leaves {
        let item = leaf_item(leaf, interval_ms, &e2e.metrics)?;
        rqe_raqes.insert(id.to_string(), workload.push_item(item, id)?);
        leaf_rqes.push(id);
    }

    // Plan deployment index -> its position in `deployments`, in order of
    // the first Raqe each serves.
    let mut position: HashMap<usize, usize> = HashMap::new();
    let mut deployments: Vec<Deployment> = Vec::new();
    let mut served_by = Vec::with_capacity(workload.raqes.len());
    for (raqe, &item_index) in workload.raqes.iter().zip(&workload.raqe_items) {
        let item = &workload.items[item_index];
        // An item's leaves (say p90 and p50 over one window), and every RQE
        // a SQL leaf reads, read one deployment, so their picks must name the
        // same sketch. Each (reader, RQE): a leaf query, or a tree leaf's id.
        let mut reads: Vec<(&str, &str)> = Vec::new();
        if item_index < text_items {
            for leaf in &item.query_strings {
                let rqes = rqes_of
                    .get(&(leaf.clone(), item.t_repeat_ms))
                    .with_context(|| {
                        format!("leaf {leaf:?} has no RQE in the table's e2e block")
                    })?;
                reads.extend(rqes.iter().map(|&rqe| (leaf.as_str(), rqe)));
            }
        } else {
            let id = leaf_rqes[item_index - text_items];
            reads.push((id, id));
        }
        let mut index = None;
        for &(reader, rqe) in &reads {
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
                window_ms: whole_ms(pick.window_secs)
                    .with_context(|| format!("RQE {rqe:?}'s window"))?,
                slide_ms: whole_ms(pick.slide_secs)
                    .with_context(|| format!("RQE {rqe:?}'s slide"))?,
                key_tracker: None,
            };
            ensure!(
                can_serve(raqe, &deployment),
                "RQE {rqe:?}'s {} {}s window sliding every {}s can't serve {reader:?} \
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
                reads.iter().map(|&(reader, _)| reader).collect::<Vec<_>>()
            );
            index = Some(at);
        }
        let index = index.expect("an item reads an RQE");
        served_by.push(index);
    }
    Ok(deployments_to_planner_output(
        config,
        &workload,
        &deployments,
        &served_by,
        &rqe_raqes,
    )?)
}

/// The optimizer item of an RQE leaf, read every `interval_ms`: what a
/// PromQL or SQL leaf with the same capability, metric, lookback and grouping
/// needs. It has no query string, so it gets no `query_configs` entry.
fn leaf_item(
    leaf: &RqeLeaf,
    interval_ms: u64,
    metrics: &BTreeMap<String, E2eMetric>,
) -> anyhow::Result<OptimizerItem> {
    let id = &leaf.id;
    let m = metrics
        .get(&leaf.metric)
        .with_context(|| format!("RQE {id:?} reads unknown metric {:?}", leaf.metric))?;
    let metric = match (m.kind == RECORDS_METRIC_TYPE, &leaf.value) {
        (true, Some(value)) => {
            ensure!(
                m.values.contains_key(value),
                "RQE {id:?} reads {value:?}, which is not a value of {:?}",
                leaf.metric
            );
            sql_metric(&leaf.metric, value)
        }
        (true, None) => bail!(
            "RQE {id:?} reads records metric {:?} without a `value`",
            leaf.metric
        ),
        (false, Some(_)) => bail!(
            "RQE {id:?} has a `value`, but {:?} is not a records metric",
            leaf.metric
        ),
        (false, None) => leaf.metric.clone(),
    };
    for label in &leaf.grouping {
        ensure!(
            m.labels.iter().any(|l| &l.name == label),
            "RQE {id:?} groups by {label:?}, which is not a label of {:?}",
            leaf.metric
        );
    }
    ensure!(leaf.lookback_ms > 0, "RQE {id:?} looks back 0 ms");
    ensure!(interval_ms > 0, "RQE {id:?} repeats every 0 s");
    let (statistic, topk_count_events) = match leaf.capability {
        RqeCapability::Sum => (Statistic::Sum, None),
        RqeCapability::Quantile => (Statistic::Quantile, None),
        RqeCapability::Cardinality => (Statistic::Cardinality, None),
        // Value-ranked, as `topk(k, sum_over_time(...))` is.
        RqeCapability::TopkByValue => (Statistic::Topk, Some(false)),
    };
    // A text query of a saved plan's table has the default options too.
    let options = ControllerOptions::default();
    Ok(OptimizerItem {
        requirements: QueryRequirements {
            metric,
            statistics: vec![statistic],
            data_range_ms: leaf.lookback_ms,
            grouping_labels: KeyByLabelNames::new(leaf.grouping.clone()),
            spatial_filter_normalized: normalize_spatial_filter(""),
            topk_count_events,
            topk_by_labels: None,
        },
        query_strings: Vec::new(),
        query_frequency_hz: 1000.0 / interval_ms as f64,
        occurrences: 1,
        t_repeat_ms: interval_ms,
        accuracy_sla: options.accuracy_sla,
        latency_sla_ms: options.latency_sla_ms,
        topk_k: leaf.k,
    })
}

/// Largest gap between `secs` in ms and the nearest whole ms that is still
/// float error, not a fraction of a ms.
const WHOLE_MS_TOLERANCE: f64 = 1e-6;

/// `secs` in ms, refusing a negative or non-finite value or a fraction of a ms.
fn whole_ms(secs: f64) -> anyhow::Result<u64> {
    let ms = secs * 1000.0;
    ensure!(
        ms.is_finite() && ms >= 0.0 && (ms - ms.round()).abs() < WHOLE_MS_TOLERANCE,
        "{secs} s is not a whole number of ms"
    );
    Ok(ms.round() as u64)
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

    /// Every RQE of a SQL query is checked, not just the first.
    #[test]
    fn refuses_a_sql_query_whose_rqes_need_different_sketches() {
        let mut plan = mixed_plan(1);
        plan["plans"][0]["chosen"][2]["params"] = json!({"lg_k": 14});
        let err = run_mixed(plan, QueryLanguage::sql)
            .err()
            .expect("refused")
            .to_string();
        assert!(err.contains("different sketches"), "{err}");
    }

    /// A SQL query's RQEs are a set: listing them in another order is the
    /// same query.
    #[test]
    fn reads_a_repeated_sql_query_whose_rqes_are_reordered() {
        let entry = r#"{"sql": "SELECT service, COUNT(DISTINCT user_id) FROM http WHERE ts BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY service",
             "interval_secs": 60, "rqes": ["users_1pct", "users_5pct"]},"#;
        let table = MIXED_TABLE.replace(
            entry,
            &format!(
                "{entry}{}",
                entry.replace(
                    r#"["users_1pct", "users_5pct"]"#,
                    r#"["users_5pct", "users_1pct"]"#
                )
            ),
        );
        assert_ne!(table, MIXED_TABLE);
        run_in(
            &table,
            mixed_plan(1).to_string().as_bytes(),
            None,
            QueryLanguage::sql,
        )
        .unwrap();
    }

    #[test]
    fn refuses_a_records_metric_without_labels_or_values() {
        for (from, to, reason) in [
            (
                r#""labels": [{"name": "region"}, {"name": "service"}],"#,
                r#""labels": [],"#,
                "metadata",
            ),
            (
                r#""values": {"latency": {}, "user_id": {}}"#,
                r#""values": {}"#,
                r#"records metric "http" has no values"#,
            ),
        ] {
            let table = MIXED_TABLE.replace(from, to);
            assert_ne!(table, MIXED_TABLE);
            let err = run_in(
                &table,
                mixed_plan(1).to_string().as_bytes(),
                None,
                QueryLanguage::sql,
            )
            .err()
            .expect("refused")
            .to_string();
            assert!(err.contains(reason), "{err}");
        }
    }

    #[test]
    fn refuses_a_language_other_than_promql_or_sql() {
        let err = run_in(
            TABLE,
            plans(shared_p50()).to_string().as_bytes(),
            None,
            QueryLanguage::elastic_sql,
        )
        .err()
        .expect("refused")
        .to_string();
        assert!(err.contains("only --query-language promql or sql"), "{err}");
    }

    /// An instant query's window is one scrape, which sketch-bench writes in
    /// fractional seconds (0.1 s at 10 samples/s).
    #[test]
    fn writes_a_sub_second_window_in_ms() {
        let mut plan = plans(shared_p50());
        plan["plans"][0]["chosen"][0]["window_secs"] = json!(0.1);
        plan["plans"][0]["chosen"][0]["slide_secs"] = json!(0.1);
        let yaml = run(plan, None).unwrap().to_streaming_yaml_string().unwrap();
        assert!(yaml.contains("windowSizeMs: 100\n"), "{yaml}");

        let mut plan = plans(shared_p50());
        plan["plans"][0]["chosen"][0]["window_secs"] = json!(0.0005);
        let err = format!("{:#}", run(plan, None).err().expect("refused"));
        assert!(
            err.contains("RQE \"sum\"'s window: 0.0005 s is not a whole number of ms"),
            "{err}"
        );
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

    fn leaf(id: &str, capability: &str, lookback_ms: u64, grouping: &[&str]) -> Value {
        json!({"rqe": {"id": id, "metric": "data", "capability": capability,
                       "lookback_ms": lookback_ms, "grouping": grouping}})
    }

    fn quantile_leaf(id: &str, phi: f64, lookback_ms: u64, grouping: &[&str]) -> Value {
        let mut l = leaf(id, "quantile", lookback_ms, grouping);
        l["rqe"]["phi"] = json!(phi);
        l
    }

    /// `TABLE` with each query's RQE tree.
    fn tree_table() -> Value {
        let mut table: Value = serde_json::from_str(TABLE).unwrap();
        let series = ["job", "label_0"];
        let p90 = quantile_leaf("p90", 0.9, 300_000, &series);
        let p50 = quantile_leaf("p50", 0.5, 300_000, &series);
        let exprs = [
            leaf("sum", "sum", 1000, &["job"]),
            json!({"op": "/", "lhs": p90, "rhs": p50.clone()}),
            p50,
        ];
        for (q, expr) in queries(&mut table).iter_mut().zip(exprs) {
            q["expr"] = expr;
        }
        table
    }

    fn queries(table: &mut Value) -> &mut Vec<Value> {
        table["workloads"][0]["e2e"]["queries"]
            .as_array_mut()
            .unwrap()
    }

    fn run_table(
        table: &Value,
        plan: &Value,
        language: QueryLanguage,
    ) -> anyhow::Result<PlannerOutput> {
        run_in(
            &table.to_string(),
            plan.to_string().as_bytes(),
            None,
            language,
        )
    }

    fn inference_of(
        output: &PlannerOutput,
        language: QueryLanguage,
    ) -> asap_types::InferenceConfig {
        output.to_inference_config(language).unwrap()
    }

    fn query_aggregation_ids(inference: &asap_types::InferenceConfig, query: &str) -> Vec<u64> {
        let config = inference
            .query_configs
            .iter()
            .find(|q| q.query == query)
            .unwrap_or_else(|| panic!("no query_config for {query:?}"));
        config
            .aggregations
            .iter()
            .map(|a| a.aggregation_id)
            .collect()
    }

    /// (aggregation id, retained windows) per aggregation the RQE reads.
    fn rqe_aggregations(
        inference: &asap_types::InferenceConfig,
        rqe: &str,
    ) -> Vec<(u64, Option<u64>)> {
        inference.rqe_configs[rqe]
            .aggregations
            .iter()
            .map(|a| (a.aggregation_id, a.num_aggregates_to_retain))
            .collect()
    }

    fn stages(inference: &asap_types::InferenceConfig, rqe: &str) -> Value {
        serde_json::to_value(&inference.rqe_configs[rqe].query_time_aggregations).unwrap()
    }

    /// Each tree leaf reads the aggregation its text query reads. The
    /// ratio's two leaves are two entries on one aggregation.
    #[test]
    fn writes_an_rqe_config_per_tree_leaf() {
        let output = run_table(&tree_table(), &plans(shared_p50()), QueryLanguage::promql).unwrap();
        let inference = inference_of(&output, QueryLanguage::promql);

        let mut ids: Vec<&str> = inference.rqe_configs.keys().map(String::as_str).collect();
        ids.sort();
        assert_eq!(ids, ["p50", "p90", "sum"]);
        let [sum_id] = query_aggregation_ids(&inference, "sum by (job) (data)")[..] else {
            panic!("one aggregation");
        };
        assert_eq!(rqe_aggregations(&inference, "sum"), [(sum_id, Some(1))]);
        let [dd_id] = query_aggregation_ids(&inference, "quantile_over_time(0.5, data[5m])")[..]
        else {
            panic!("one aggregation");
        };
        assert_ne!(dd_id, sum_id);
        // 5m of a 5m window sliding every minute: the newest closed window.
        for rqe in ["p90", "p50"] {
            assert_eq!(
                rqe_aggregations(&inference, rqe),
                [(dd_id, Some(1))],
                "{rqe}"
            );
            assert_eq!(stages(&inference, rqe), json!([]), "{rqe}");
        }
        assert_eq!(stages(&inference, "sum"), json!([]));
    }

    /// Trees only add `rqe_configs`: the deployments and `query_configs`
    /// are those of the same table without trees, which has no `rqe_configs`.
    #[test]
    fn trees_leave_the_text_queries_configs_unchanged() {
        let plan = plans(shared_p50());
        let without = run_on(TABLE, plan.to_string().as_bytes(), None).unwrap();
        let with = run_table(&tree_table(), &plan, QueryLanguage::promql).unwrap();

        assert_eq!(
            with.to_streaming_yaml_string().unwrap(),
            without.to_streaming_yaml_string().unwrap()
        );
        let without_yaml = without.to_inference_yaml_string().unwrap();
        assert!(!without_yaml.contains("rqe_configs"), "{without_yaml}");
        let mut with_inference: serde_yaml::Value =
            serde_yaml::from_str(&with.to_inference_yaml_string().unwrap()).unwrap();
        assert!(with_inference
            .as_mapping_mut()
            .unwrap()
            .remove("rqe_configs")
            .is_some());
        assert_eq!(
            serde_yaml::to_string(&with_inference).unwrap(),
            without_yaml
        );
    }

    #[test]
    fn writes_an_exact_sum_roll_up_as_an_rqe_query_time_sum() {
        let mut plan = plans(shared_p50());
        plan["plans"][0]["chosen"][0]["deployment_grouping"] = json!(["job", "label_0"]);
        let output = run_table(&tree_table(), &plan, QueryLanguage::promql).unwrap();
        let inference = inference_of(&output, QueryLanguage::promql);

        assert_eq!(
            stages(&inference, "sum"),
            json!([{
                "operator": "sum",
                "grouping": {"mode": "by", "labels": ["job"]},
                "parameter": null,
            }])
        );
        assert_eq!(
            rqe_aggregations(&inference, "sum")[0].0,
            query_aggregation_ids(&inference, "sum by (job) (data)")[0]
        );
    }

    /// The engine merges a sketch across the deployment's groups itself, so
    /// the RQE reads the deployment's aggregation with no stage.
    #[test]
    fn writes_a_sketch_roll_up_rqe_without_a_stage() {
        let table = json!({"workloads": [{"e2e": {
            "queries": [{"promql": "quantile by (job) (0.5, data)", "interval_secs": 1,
                         "expr": quantile_leaf("p50", 0.5, 1000, &["job"])}],
            "metrics": {"data": {"type": "gauge", "labels": [{"name": "job"}, {"name": "label_0"}]}}
        }}]});
        let mut p50 = pick("p50", 0, "dd", json!({"alpha": 0.02}), 1);
        p50["deployment_grouping"] = json!(["job", "label_0"]);
        let plan = json!({"plans": [
            {"method": "asap", "weights": "cpu", "kind": "cheapest", "chosen": [p50]},
        ]});

        let output = run_table(&table, &plan, QueryLanguage::promql).unwrap();

        assert_eq!(output.streaming_aggregation_count(), 1);
        let inference = inference_of(&output, QueryLanguage::promql);
        assert_eq!(rqe_aggregations(&inference, "p50"), [(1, Some(1))]);
        assert_eq!(stages(&inference, "p50"), json!([]));
    }

    /// A query with only a tree is planned in the language of the metrics
    /// its leaves read: it gets a deployment and an `rqe_configs` entry, but
    /// no `query_configs` entry.
    #[test]
    fn plans_a_tree_only_query() {
        let mut table = tree_table();
        let tree_only =
            json!({"interval_secs": 1, "expr": quantile_leaf("p50_by_job", 0.5, 1000, &["job"])});
        queries(&mut table).push(tree_only.clone());
        let mut plan = plans(shared_p50());
        plan["plans"][0]["chosen"]
            .as_array_mut()
            .unwrap()
            .push(pick("p50_by_job", 5, "dd", json!({"alpha": 0.05}), 1));

        let output = run_table(&table, &plan, QueryLanguage::promql).unwrap();

        assert_eq!(output.streaming_aggregation_count(), 3);
        assert_eq!(output.inference_query_count(), 3);
        let inference = inference_of(&output, QueryLanguage::promql);
        assert_eq!(rqe_aggregations(&inference, "p50_by_job"), [(3, Some(1))]);
        let streaming = output.to_streaming_config(QueryLanguage::promql).unwrap();
        let dd = &streaming.aggregation_configs[&3];
        assert_eq!(
            dd.aggregation_type,
            promql_utilities::query_logics::enums::AggregationType::DDSketch
        );
        assert_eq!(dd.grouping_labels.labels, ["job"]);

        // A table of tree-only queries converts too.
        let table = json!({"workloads": [{"e2e": {
            "queries": [tree_only],
            "metrics": {"data": {"type": "gauge", "labels": [{"name": "job"}, {"name": "label_0"}]}}
        }}]});
        let output = run_table(&table, &plan, QueryLanguage::promql).unwrap();
        assert_eq!(output.streaming_aggregation_count(), 1);
        assert_eq!(output.inference_query_count(), 0);
        let inference = inference_of(&output, QueryLanguage::promql);
        assert_eq!(rqe_aggregations(&inference, "p50_by_job"), [(1, Some(1))]);
        let err = run_table(&table, &plan, QueryLanguage::sql)
            .err()
            .expect("refused")
            .to_string();
        assert!(err.contains("no e2e query is written in sql"), "{err}");
    }

    fn http_leaf(id: &str, capability: &str, grouping: &[&str], value: &str) -> Value {
        json!({"rqe": {"id": id, "metric": "http", "capability": capability,
                       "lookback_ms": 300_000, "grouping": grouping, "value": value}})
    }

    /// `MIXED_TABLE`'s p99 query with its tree, and a tree-only SQL sum.
    fn mixed_tree_table() -> Value {
        let mut table: Value = serde_json::from_str(MIXED_TABLE).unwrap();
        let mut p99 = http_leaf("p99", "quantile", &["region"], "latency");
        p99["rqe"]["phi"] = json!(0.99);
        queries(&mut table)[2]["expr"] = p99;
        queries(&mut table).push(json!({"interval_secs": 60,
            "expr": http_leaf("latency_sum", "sum", &["region"], "latency")}));
        table
    }

    /// A SQL query can't run a query-time stage, but its RQE can: an exact
    /// roll-up of a tree-only SQL sum is written.
    #[test]
    fn writes_sql_rqe_configs_with_an_exact_roll_up() {
        let mut plan = mixed_plan(1);
        let mut sum = pick("latency_sum", 4, "exact-sum", json!({}), 60);
        sum["deployment_grouping"] = json!(["region", "service"]);
        plan["plans"][0]["chosen"].as_array_mut().unwrap().push(sum);

        let output = run_table(&mixed_tree_table(), &plan, QueryLanguage::sql).unwrap();

        let inference = inference_of(&output, QueryLanguage::sql);
        let mut ids: Vec<&str> = inference.rqe_configs.keys().map(String::as_str).collect();
        ids.sort();
        assert_eq!(ids, ["latency_sum", "p99"]);
        let p99_query = "SELECT region, QUANTILE(0.99, latency) FROM http WHERE ts BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY region";
        // 5m of 5m windows sliding every minute: the newest closed window.
        assert_eq!(
            rqe_aggregations(&inference, "p99"),
            [(query_aggregation_ids(&inference, p99_query)[0], Some(1))]
        );
        assert_eq!(
            stages(&inference, "latency_sum"),
            json!([{
                "operator": "sum",
                "grouping": {"mode": "by", "labels": ["region"]},
                "parameter": null,
            }])
        );
        let streaming = output.to_streaming_config(QueryLanguage::sql).unwrap();
        let (sum_id, _) = rqe_aggregations(&inference, "latency_sum")[0];
        let sum = &streaming.aggregation_configs[&sum_id];
        assert_eq!(sum.table_name.as_deref(), Some("http"));
        assert_eq!(sum.value_column.as_deref(), Some("latency"));
        // A keyed accumulator keeps its groups as keys.
        assert_eq!(sum.aggregated_labels.labels, ["region", "service"]);

        // The PromQL run skips the SQL trees.
        let output = run_table(&mixed_tree_table(), &plan, QueryLanguage::promql).unwrap();
        assert!(inference_rqe_configs_absent(&output));
    }

    fn inference_rqe_configs_absent(output: &PlannerOutput) -> bool {
        !output
            .to_inference_yaml_string()
            .unwrap()
            .contains("rqe_configs")
    }

    fn tree_refusal(table: &Value) -> String {
        format!(
            "{:#}",
            run_table(table, &plans(shared_p50()), QueryLanguage::promql)
                .err()
                .expect("refused")
        )
    }

    #[test]
    fn refuses_a_tree_disagreeing_with_its_rqes() {
        let mut table = tree_table();
        queries(&mut table)[1]["rqes"] = json!(["p50", "p90"]);
        let err = tree_refusal(&table);
        assert!(
            err.contains(r#"names RQEs ["p50", "p90"] but its `expr` reads ["p90", "p50"]"#),
            "{err}"
        );
    }

    /// `rqes` may be omitted: the tree names the RQEs.
    #[test]
    fn reads_the_rqes_of_a_tree_without_rqes() {
        let mut table = tree_table();
        for q in queries(&mut table).iter_mut() {
            q.as_object_mut().unwrap().remove("rqes");
        }
        let output = run_table(&table, &plans(shared_p50()), QueryLanguage::promql).unwrap();
        assert_eq!(output.inference_query_count(), 3);
        assert_eq!(
            inference_of(&output, QueryLanguage::promql)
                .rqe_configs
                .len(),
            3
        );
    }

    #[test]
    fn refuses_an_invalid_tree() {
        let mut table = tree_table();
        queries(&mut table)[0]["expr"] =
            json!({"agg": "sum", "by": ["region"], "of": leaf("sum", "sum", 1000, &["job"])});
        let err = tree_refusal(&table);
        assert!(err.contains("invalid `expr`"), "{err}");
        assert!(err.contains("not a subset"), "{err}");
    }

    #[test]
    fn refuses_one_rqe_as_two_different_leaves() {
        let mut table = tree_table();
        queries(&mut table)[2]["expr"] = quantile_leaf("p50", 0.5, 300_000, &["job"]);
        let err = tree_refusal(&table);
        assert!(
            err.contains(r#"RQE "p50" is two different leaves"#),
            "{err}"
        );
    }

    #[test]
    fn refuses_a_query_without_text_or_tree_or_with_mismatched_languages() {
        let mut mixed: Value = serde_json::from_str(MIXED_TABLE).unwrap();
        let cases = [
            (
                json!({"interval_secs": 1, "rqes": ["sum"]}),
                "needs `promql`, `sql` or `expr`",
            ),
            (
                json!({"promql": "sum by (job) (data)", "interval_secs": 1,
                       "expr": http_leaf("sum", "sum", &[], "latency")}),
                "written in PromQL but its `expr` reads records metrics",
            ),
            (
                json!({"interval_secs": 1, "expr": {"op": "+",
                       "lhs": leaf("sum", "sum", 1000, &[]),
                       "rhs": http_leaf("sum2", "sum", &[], "latency")}}),
                "mixes records and non-records metrics",
            ),
            (
                json!({"interval_secs": 1, "expr": {"scalar": 1.0}}),
                "its `expr` reads no RQE",
            ),
        ];
        for (query, reason) in cases {
            queries(&mut mixed).push(query);
            let err = format!(
                "{:#}",
                run_table(&mixed, &mixed_plan(1), QueryLanguage::sql)
                    .err()
                    .expect("refused")
            );
            assert!(err.contains(reason), "{err}");
            queries(&mut mixed).pop();
        }
    }

    #[test]
    fn refuses_a_leaf_the_table_cannot_describe() {
        let records_leaf = |value: Option<&str>, grouping: &[&str]| {
            let mut l = http_leaf("p99", "quantile", grouping, "latency");
            l["rqe"]["phi"] = json!(0.99);
            match value {
                Some(value) => l["rqe"]["value"] = json!(value),
                None => {
                    l["rqe"].as_object_mut().unwrap().remove("value");
                }
            }
            l
        };
        let mut unknown = records_leaf(Some("latency"), &["region"]);
        unknown["rqe"]["metric"] = json!("nope");
        for (expr, reason) in [
            (unknown, r#"reads unknown metric "nope""#),
            (records_leaf(None, &["region"]), "without a `value`"),
            (
                records_leaf(Some("bytes"), &["region"]),
                r#"which is not a value of "http""#,
            ),
            (
                records_leaf(Some("latency"), &["zone"]),
                r#"groups by "zone""#,
            ),
        ] {
            let mut table: Value = serde_json::from_str(MIXED_TABLE).unwrap();
            queries(&mut table)[2]["expr"] = expr;
            let err = format!(
                "{:#}",
                run_table(&table, &mixed_plan(1), QueryLanguage::sql)
                    .err()
                    .expect("refused")
            );
            assert!(err.contains(reason), "{err}");
        }
        let mut table = tree_table();
        queries(&mut table)[0]["expr"]["rqe"]["value"] = json!("v");
        let err = tree_refusal(&table);
        assert!(err.contains("is not a records metric"), "{err}");
    }

    #[test]
    fn refuses_a_tree_leaf_the_plan_does_not_serve() {
        let mut table = tree_table();
        queries(&mut table).push(json!({"interval_secs": 1,
            "expr": quantile_leaf("unplanned", 0.5, 1000, &["job"])}));
        let err = tree_refusal(&table);
        assert!(err.contains(r#"doesn't serve RQE "unplanned""#), "{err}");
    }

    /// A topk deployment can't roll up, for an RQE as for a query.
    #[test]
    fn refuses_a_tree_only_topk_roll_up() {
        let table = json!({"workloads": [{"e2e": {
            "queries": [{"interval_secs": 60, "expr": {"rqe": {"id": "top", "metric": "data",
                "capability": "topk_by_value", "k": 5, "lookback_ms": 300_000,
                "grouping": ["job", "label_0"]}}}],
            "metrics": {"data": {"type": "gauge", "labels": [{"name": "job"}, {"name": "label_0"}]}}
        }}]});
        let mut top = pick(
            "top",
            0,
            "cms-heap-topk-fastpath-vector2d",
            json!({"rows": 3, "cols": 1024, "heap": 50}),
            300,
        );
        top["deployment_grouping"] = json!(["job"]);
        let plan = json!({"plans": [
            {"method": "asap", "weights": "cpu", "kind": "cheapest", "chosen": [top]},
        ]});
        let err = run_table(&table, &plan, QueryLanguage::promql)
            .err()
            .expect("refused")
            .to_string();
        assert!(err.contains(r#"query "top""#), "{err}");
        assert!(err.contains("roll-ups need an aggregation"), "{err}");
    }
}
