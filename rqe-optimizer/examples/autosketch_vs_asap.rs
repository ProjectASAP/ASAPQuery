//! AutoSketch vs. the ASAP planner (ProjectASAP/ASAPQuery#777, paper §6.3).
//!
//! One invocation evaluates one workload and writes one JSON file:
//!
//! ```text
//! autosketch_vs_asap traces --dataset alibaba_v2022 --saturation-dir DIR --out FILE [--runs 5]
//! autosketch_vs_asap synthetic --table FILE --target p95 --saturation-dir DIR --out FILE [--runs 1]
//!
//! Options for both: --weights cpu,fargate (a subset of the weight
//! settings), --slas-ms 100,1000 (version 2's SLA grid, in ms), --no-chosen
//! (drop per-RQE choices). The synthetic --target is the accuracy level;
//! there is one, p95, which traces use too.
//! ```
//!
//! Every plan is priced by use (`usage::usage_cost`, sketch-bench
//! `docs/rqe_sketch_deployment_v1.md`, "Cost by use and batch latency"):
//! `w_cpu · AUC(CPU) + w_mem · AUC(memory)`, CPU elastic, at each weight
//! setting of #777 §4 (CPU only; Fargate's per-vCPU and per-GB prices). A
//! batch's latency is its longest chain (the newest window's compaction,
//! then the query): the job placement's closed form with elastic CPU. Two
//! versions, in one output file:
//!
//! - **Version 1, no latency constraint** (`results`): each method's cheapest
//!   plan, its latency reported, and ASAP's and PerQuery's cost–latency
//!   frontiers (`bound_ms`).
//! - **Version 2, a batch latency SLA** (`sla_results`): at each SLA of the
//!   grid, ASAP's and PerQuery's cheapest plan whose batch latency is at most
//!   the SLA (`milp::minimize_usage_cost` with `latency_bound_ms`, exact).
//!   An SLA below a method's tightest feasible bound has no plan
//!   (`infeasible`). AutoSketch ignores the SLA; its plan is recorded at
//!   each SLA with `meets_sla`.
//!
//! Methods:
//! - **ASAP:** `milp::minimize_usage_cost` over every RQE jointly, with
//!   sharing.
//! - **AutoSketch-Adapted:** `autosketch::plan`, one search and one dedicated
//!   deployment per RQE, accuracy only; its plan does not depend on the
//!   weights and is priced under each.
//! - **PerQuery-CostAware:** `milp::minimize_usage_cost` over each RQE's own
//!   candidates (separate copies, so no sharing).
//!
//! `traces` reads `data/autosketch-eval/table.json` (alibaba_v2022 and
//! google_2011 are evaluated; boom is left out for now); `synthetic` reads one
//! table written by `export_autosketch_eval_table.py --synthetic`. A table
//! gives the workload: RQEs, and each stream's groups, rate and data shape
//! (the family's `grid_param`/`grid_K`, the worst case rounded to the grid).
//! Each stream (`traces`: dataset, query and kind; `synthetic`: the table's
//! `stream`) is one metric, which is where sharing comes from.
//!
//! Costs and accuracy come from `--saturation-dir`, the same inputs the
//! planner reads (#174): the cost table `optimizer_cost/rqe_atomic_costs.json`
//! (one row per config, at the evaluation dataset's shape) and the saturation
//! curves. ASAP reads `SaturationCurves::accuracy` (the curve at the
//! lookback's items; KLL and top-k merged from `L/x` shards read the merge
//! curve, #158); AutoSketch reads `autosketch_accuracy` (one unmerged sketch
//! per query window, no saturation requirement).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use rqe_optimizer::analytical_cost_model::chain_ms;
use rqe_optimizer::autosketch;
use rqe_optimizer::candidates::{build_all_candidates, eligible_deployments_for, is_eligible};
use rqe_optimizer::milp::minimize_usage_cost;
use rqe_optimizer::saturation::{DataShape, SaturationCurves, COST_TABLE};
use rqe_optimizer::usage::{usage_cost, PlanLoad};
use rqe_optimizer::{
    validate_facts, AtomicCostEntry, AtomicCostTable, Capability, Deployment, LabelSet, Mapping,
    MetricFacts, Millis, Raqe, WorkloadFacts,
};
use serde_json::{json, Value};

const TRACES_TABLE: &str = "rqe-optimizer/data/autosketch-eval/table.json";
/// Objective weights of #777 §4: CPU only, then AWS Fargate (us-east-1,
/// Linux/x86) $/vCPU-hour and $/GB-hour, so the objective reads in $/hour.
const WEIGHTS: [(&str, f64, f64); 2] = [("cpu", 1.0, 0.0), ("fargate", 0.0405, 0.00445)];
/// Every stream is scraped once a second: windows and slides are whole
/// seconds, and the stream's samples/s become its series count.
const SCRAPE_MS: Millis = 1_000;
const BYTES_PER_GIB: f64 = (1u64 << 30) as f64;
const SEED: u64 = 7;
/// AutoSketch's benchmark time per evaluated configuration, at the paper's
/// rate: 1–2 minutes each (NSDI '24, §5.2 and Exp#9); we take 1 minute.
const PAPER_SECS_PER_PROBE: f64 = 60.0;
/// Version 2's batch latency SLAs, ms (`--slas-ms` overrides).
const SLAS_MS: [f64; 5] = [100.0, 300.0, 1_000.0, 3_000.0, 10_000.0];
/// Relative slack for comparing a latency with an SLA. Must equal
/// `milp::minimize_usage_cost`'s (private) slack for chains vs. its bound, so
/// "the tightest bound meets the SLA" and "the MILP has a plan" agree.
const LATENCY_SLACK: f64 = 1e-9;

/// One workload: RQEs, one metric per stream, the cost entries each stream
/// may use, and the curves every accuracy is read from.
struct Workload {
    name: String,
    raqes: Vec<Raqe>,
    facts: WorkloadFacts,
    costs: BTreeMap<String, Vec<AtomicCostEntry>>,
    /// The metric (data) each RQE reads, by RQE id: AutoSketch benchmarks a
    /// config once per metric. Tables without one read one metric per dataset.
    metric_of: BTreeMap<String, String>,
    /// RQEs of streams with more keys than samples/s, left out.
    excluded_high_cardinality: Vec<String>,
    curves: SaturationCurves,
    notes: Vec<String>,
}

impl Workload {
    /// ASAP's accuracy: the planner's own lookup.
    fn asap_accuracy(&self, r: &Raqe, d: &Deployment) -> Option<f64> {
        self.curves.accuracy(r, d, &self.facts)
    }

    /// AutoSketch's: one unmerged sketch per query window.
    fn autosketch_accuracy(&self, r: &Raqe, c: &AtomicCostEntry) -> Option<f64> {
        self.curves.autosketch_accuracy(r, c, &self.facts)
    }
}

fn arg(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let out = arg(&args, "--out").expect("--out FILE is required");
    let runs: usize = arg(&args, "--runs").map_or(5, |r| r.parse().unwrap());
    let saturation_dir = arg(&args, "--saturation-dir").expect("--saturation-dir DIR is required");
    let workload = match args.get(1).map(String::as_str) {
        Some("traces") => from_table(
            TRACES_TABLE,
            Some(&arg(&args, "--dataset").expect("--dataset")),
            Some("p95".to_string()),
            &saturation_dir,
        ),
        Some("synthetic") => from_table(
            &arg(&args, "--table").expect("--table"),
            None,
            Some(arg(&args, "--target").expect("--target")),
            &saturation_dir,
        ),
        _ => panic!("usage: autosketch_vs_asap (traces|synthetic) ... --out FILE"),
    };
    let weights: Vec<(&str, f64, f64)> = WEIGHTS
        .iter()
        .copied()
        .filter(|(name, ..)| {
            arg(&args, "--weights").is_none_or(|list| list.split(',').any(|w| w == *name))
        })
        .collect();
    assert!(
        !weights.is_empty(),
        "--weights names none of {:?}",
        WEIGHTS.map(|(name, ..)| name)
    );
    let slas = arg(&args, "--slas-ms").map_or(SLAS_MS.to_vec(), |list| parse_slas(&list));
    let mut result = evaluate(&workload, runs, &weights, &slas);
    if args.iter().any(|a| a == "--no-chosen") {
        for key in ["results", "sla_results"] {
            for r in result[key].as_array_mut().unwrap() {
                r.as_object_mut().unwrap().remove("chosen");
            }
        }
    }
    std::fs::write(&out, serde_json::to_string_pretty(&result).unwrap()).unwrap();
    eprintln!("wrote {out}");
}

// ---------------------------------------------------------------- workloads

/// Per-capability accuracy target at the evaluation's one accuracy level,
/// p95 (95% in each family's own metric). Exact accumulators must have zero
/// error.
fn strictness_target(level: &str, capability: Capability) -> f64 {
    assert_eq!(level, "p95", "the evaluation has one accuracy level, p95");
    match capability {
        // 95% accuracy in each family's own metric: error at most 0.05 for
        // rank, relative value and relative errors; precision at least 0.95.
        Capability::Quantile | Capability::Cardinality => 0.05,
        Capability::TopKByValue | Capability::TopKByCount => 0.95,
        Capability::Sum
        | Capability::Count
        | Capability::Min
        | Capability::Max
        | Capability::RateOrIncrease => 0.0,
    }
}

/// The table's capability for an RQE. `traces` key queries are per-key sums
/// (`sum by (key)`), served by the cost table's exact accumulators, one per
/// key; the table lists only their sketch families, so their target falls
/// back to 0 (exact).
fn capability_of(r: &Value) -> Capability {
    match (r["capability"].as_str(), r["kind"].as_str().unwrap()) {
        (Some("sum"), _) | (Some("freq"), _) | (None, "keys") => Capability::Sum,
        (Some("rate"), _) => Capability::RateOrIncrease,
        // `topk(k, sum by (...) (...))`: ranked by summed value.
        (Some("topk"), _) => Capability::TopKByValue,
        (Some("quantile"), _) | (None, "values") => Capability::Quantile,
        other => panic!("unknown capability {other:?}"),
    }
}

/// A top-k RQE's k, from its query id (`topk32_...`: the table names the
/// query's literal k there); `None` for other capabilities.
fn topk_k_of(r: &Value, capability: Capability) -> Option<u64> {
    if !matches!(
        capability,
        Capability::TopKByValue | Capability::TopKByCount
    ) {
        return None;
    }
    let query = r["query_id"].as_str().unwrap();
    let digits: String = query
        .strip_prefix("topk")
        .unwrap_or_else(|| panic!("top-k query {query:?} names no k"))
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    Some(
        digits
            .parse()
            .unwrap_or_else(|_| panic!("top-k query {query:?} names no k")),
    )
}

/// The groups an RQE's deployment keeps one instance per. A `traces` key
/// query sums per key: one exact accumulator per key, so its groups are the
/// window's keys (the table's `groups` counts one sketch holding them all).
fn groups_of(r: &Value, capability: Capability) -> u64 {
    let groups = r["label_set"]["groups"].as_u64().unwrap();
    if r["capability"].is_null() && capability == Capability::Sum {
        let keys = r["label_set"]["keys_per_window"].as_f64().unwrap_or(1.0);
        groups.max(keys.ceil() as u64)
    } else {
        groups
    }
}

fn label_set(names: &[&str]) -> LabelSet {
    names.iter().map(|s| s.to_string()).collect()
}

/// One workload of an evaluation table: `traces`'s `dataset`, or a
/// `synthetic` table's only workload. `target` sets every RQE's accuracy
/// target to that level (p95).
fn from_table(
    path: &str,
    dataset: Option<&str>,
    target: Option<String>,
    saturation_dir: &str,
) -> Workload {
    let table: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let dir = std::path::Path::new(saturation_dir);
    let curves = SaturationCurves::load(dir)
        .unwrap_or_else(|e| panic!("couldn't load saturation curves from {saturation_dir}: {e}"));
    let cost_path = dir.join(COST_TABLE);
    let cost_table: AtomicCostTable =
        serde_json::from_str(&std::fs::read_to_string(&cost_path).unwrap_or_else(|e| {
            panic!(
                "couldn't read {} ({e}); run study_saturation.py --phase optimizer-cost",
                cost_path.display()
            )
        }))
        .unwrap();
    let workload = table["workloads"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| dataset.is_none_or(|d| w["dataset"] == d))
        .unwrap_or_else(|| panic!("no workload {dataset:?} in {path}"));
    let dataset = workload["dataset"].as_str().unwrap();
    let mut raqes = Vec::new();
    let mut facts = WorkloadFacts::new();
    // Queries one evaluation issues per instance, per stream (max over its RQEs).
    let mut queries_per_stream: BTreeMap<String, f64> = BTreeMap::new();
    let mut metric_of = BTreeMap::new();
    let mut skipped_families = 0;
    let mut stream_series: BTreeMap<String, u64> = BTreeMap::new();
    for r in workload["rqes"].as_array().unwrap() {
        let id = r["id"].as_str().unwrap().to_string();
        let metric = r["metric"].as_str().unwrap_or(dataset);
        metric_of.insert(id.clone(), metric.to_string());
        let stream = match r["stream"].as_str() {
            Some(stream) => format!("{dataset}/{stream}"),
            None => format!(
                "{}/{}/{}",
                dataset,
                r["query_id"].as_str().unwrap(),
                r["kind"].as_str().unwrap()
            ),
        };
        let capability = capability_of(r);
        let rate = r["label_set"]["arrival_rate_per_sec"].as_f64().unwrap();
        let groups = groups_of(r, capability);
        let grouping = if groups > 1 {
            label_set(&["g"])
        } else {
            LabelSet::new()
        };
        // One metric per stream, labels {g, x}: `g` is the grouping (card =
        // groups), and with one scrape a second `{g, x}` has one series per
        // sample/s. A stream's RQEs differ only in range, so take the
        // largest per-RQE rate (conservative). Rates below `groups`
        // samples/s are rounded up so every group has a series.
        let series = (rate * SCRAPE_MS as f64 / 1000.0).round() as u64;
        let most = stream_series.entry(stream.clone()).or_default();
        *most = (*most).max(series);
        let metric = facts.entry(stream.clone()).or_insert_with(|| MetricFacts {
            labels: label_set(&["g", "x"]),
            scrape_interval_ms: SCRAPE_MS,
            cardinality: [
                (label_set(&["g", "x"]), 0),
                (label_set(&["g"]), groups),
                (LabelSet::new(), 1),
            ]
            .into(),
            value_range: None,
            data_shape: BTreeMap::new(),
        });
        // A stream's RQEs may see different group counts (keys per window
        // grow with the range): take the largest (conservative).
        let card = metric.cardinality.get_mut(&label_set(&["g"])).unwrap();
        *card = (*card).max(groups);
        let total = metric.cardinality.get_mut(&label_set(&["g", "x"])).unwrap();
        *total = (*total).max(series).max(groups);
        // Queries one evaluation issues per instance (the table's q_r); tables
        // written before it existed charge one query phase per evaluation.
        let queries = r["queries_per_instance"].as_f64().unwrap_or(1.0);
        let q = queries_per_stream.entry(stream.clone()).or_default();
        *q = q.max(queries);

        // The first family of the RQE's capability gives its target and the
        // stream's data shape; the table rounds both to the grid.
        let mut rqe_target = None;
        for family in r["families"].as_array().unwrap() {
            let sketch = family["sketch"].as_str().unwrap();
            if !capability.families().contains(&sketch) {
                // e.g. `traces`' frequency sketches, or top-k for a sum query.
                skipped_families += 1;
                continue;
            }
            if rqe_target.is_none() {
                rqe_target = Some(match target.as_deref() {
                    Some(level) => strictness_target(level, capability),
                    None => family["target"].as_f64().unwrap(),
                });
            }
            if let Some(shape) = data_shape(family) {
                // Several RQEs of a stream: the harder side of each parameter.
                metric
                    .data_shape
                    .entry(grouping.clone())
                    .and_modify(|old| {
                        old.zipf_s = old.zipf_s.min(shape.zipf_s);
                        old.distinct_keys = old.distinct_keys.max(shape.distinct_keys);
                        old.tail_index = old.tail_index.min(shape.tail_index);
                    })
                    .or_insert(shape);
            }
        }
        raqes.push(Raqe {
            id: id.clone(),
            capability,
            lookback_ms: (r["lookback_secs"].as_f64().unwrap() * 1000.0) as Millis,
            interval_ms: (r["interval_secs"].as_f64().unwrap() * 1000.0) as Millis,
            metric: stream,
            spatial_filter: String::new(),
            grouping_labels: grouping,
            // An RQE with no family of its capability is unservable; its
            // target is never read.
            accuracy_sla: rqe_target.unwrap_or(0.0),
            latency_sla_ms: None,
            topk_k: topk_k_of(r, capability),
        });
    }
    // Every stream may use every row; one evaluation runs q_r queries per
    // instance.
    let costs = queries_per_stream
        .iter()
        .map(|(stream, &queries)| {
            let rows = cost_table
                .iter()
                .map(|row| AtomicCostEntry {
                    query_cpu_secs: queries * row.query_cpu_secs,
                    ..row.clone()
                })
                .collect();
            (stream.clone(), rows)
        })
        .collect();
    // A stream with more keys than samples per second can't give every key a
    // series at one scrape a second, and rounding its rate up to its key count
    // inflates every method's cost (Alibaba's CallGraph keys: up to 436x).
    // Such streams are left out, and their RQEs listed.
    let high_cardinality: BTreeSet<String> = facts
        .iter()
        .filter(|(stream, m)| m.cardinality[&label_set(&["g"])] > stream_series[*stream])
        .map(|(stream, _)| stream.clone())
        .collect();
    let excluded_high_cardinality: Vec<String> = raqes
        .iter()
        .filter(|r| high_cardinality.contains(&r.metric))
        .map(|r| r.id.clone())
        .collect();
    raqes.retain(|r| !high_cardinality.contains(&r.metric));
    facts.retain(|stream, _| !high_cardinality.contains(stream));
    let name = if dataset.starts_with("synthetic") {
        format!("{dataset}/t{}", target.as_deref().unwrap_or("p95"))
    } else {
        format!("traces/{dataset}")
    };
    Workload {
        name,
        raqes,
        facts,
        costs,
        metric_of,
        excluded_high_cardinality,
        curves,
        notes: vec![
            format!("{skipped_families} families skipped: they serve another capability"),
            "one metric per stream: groups from the table, samples/s = max over the stream's RQEs, scraped once a second".into(),
            format!("streams with more keys than samples/s, left out: {high_cardinality:?}"),
            format!("costs: {COST_TABLE} under the saturation dir, one row per config"),
            "data shape per stream: the families' grid_param/grid_K, the harder side over its RQEs".into(),
            "query_cpu_secs = q_r queries per instance per evaluation".into(),
        ],
    }
}

/// A table family's grid point as a curve key: Zipf θ and K, or the Pareto
/// tail index for quantiles. The unused fields are never read.
fn data_shape(family: &Value) -> Option<DataShape> {
    let param = family["grid_param"].as_f64()?;
    let keys = &family["grid_K"];
    match keys
        .as_f64()
        .or_else(|| keys.as_str().and_then(|k| k.parse().ok()))
    {
        Some(keys) => Some(DataShape {
            zipf_s: param,
            distinct_keys: keys,
            tail_index: f64::INFINITY,
        }),
        None => Some(DataShape {
            zipf_s: f64::INFINITY,
            distinct_keys: 0.0,
            tail_index: param,
        }),
    }
}

// --------------------------------------------------------------- evaluation

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v[v.len() / 2]
}

fn group_by_stream(raqes: &[Raqe]) -> BTreeMap<&str, Vec<Raqe>> {
    let mut out: BTreeMap<&str, Vec<Raqe>> = BTreeMap::new();
    for r in raqes {
        out.entry(r.metric.as_str()).or_default().push(r.clone());
    }
    out
}

/// ASAP candidates, built per stream from that stream's entries.
fn candidates(w: &Workload, raqes: &[Raqe]) -> Vec<Deployment> {
    group_by_stream(raqes)
        .into_iter()
        .flat_map(|(stream, group)| {
            let costs = w.costs.get(stream).map_or(&[][..], Vec::as_slice);
            build_all_candidates(&group, costs, &w.facts, false, &|r, d| {
                w.asap_accuracy(r, d)
            })
        })
        .collect()
}

/// AutoSketch, searched per stream over that stream's entries.
fn autosketch_plan(
    w: &Workload,
    raqes: &[Raqe],
) -> Result<autosketch::AutoSketchPlan, Vec<String>> {
    let mut deployments = Vec::new();
    let mut searches = Vec::new();
    let mut unservable = Vec::new();
    let mut order = Vec::new();
    for (stream, group) in group_by_stream(raqes) {
        let costs = w.costs.get(stream).map_or(&[][..], Vec::as_slice);
        let oracle = |r: &Raqe, c: &AtomicCostEntry| w.autosketch_accuracy(r, c);
        match autosketch::plan(&group, costs, SEED, false, oracle) {
            Ok(plan) => {
                deployments.extend(plan.deployments);
                searches.extend(plan.searches);
                order.extend(group.iter().map(|r| r.id.clone()));
            }
            Err(ids) => unservable.extend(ids),
        }
    }
    if !unservable.is_empty() {
        return Err(unservable);
    }
    // Back to the caller's RQE order.
    let position: BTreeMap<&str, usize> = order
        .iter()
        .enumerate()
        .map(|(i, id)| (id.as_str(), i))
        .collect();
    let mapping: Mapping = raqes.iter().map(|r| position[r.id.as_str()]).collect();
    Ok(autosketch::AutoSketchPlan {
        deployments,
        mapping,
        searches,
    })
}

/// A plan priced by use (`usage::usage_cost`): its cost, CPU and memory by
/// part, its latency (the longest chain) and each RQE's choice.
fn summarize(
    w: &Workload,
    raqes: &[Raqe],
    deployments: &[Deployment],
    mapping: &Mapping,
    w_cpu: f64,
    w_mem: f64,
) -> Value {
    let load = PlanLoad::new(raqes, deployments, mapping, &w.facts);
    let cost = usage_cost(&load, w_cpu, w_mem);
    let secs = |ms: Millis| ms as f64 / 1000.0;
    let gib = |bytes: f64| bytes / BYTES_PER_GIB;
    let ingest_cpu: f64 = load.deployments.iter().map(|d| d.ingest_cpu).sum();
    let compaction_cpu: f64 = load
        .deployments
        .iter()
        .map(|d| d.compaction_secs / secs(d.slide_ms))
        .sum();
    let query_cpu: f64 = load
        .queries
        .iter()
        .map(|q| q.work_secs / secs(q.interval_ms))
        .sum();
    let ingest_bytes: f64 = load.deployments.iter().map(|d| d.ingest_bytes).sum();
    let storage_bytes: f64 = load.deployments.iter().map(|d| d.storage_bytes).sum();
    let compaction_bytes: f64 = load
        .deployments
        .iter()
        .map(|d| d.compaction_bytes * d.compaction_secs / secs(d.slide_ms))
        .sum();
    let query_bytes: f64 = load
        .queries
        .iter()
        .map(|q| q.memory_bytes * q.work_secs / secs(q.interval_ms))
        .sum();
    let latencies: Vec<f64> = load.queries.iter().map(|q| q.chain_ms).collect();
    let active: BTreeSet<usize> = mapping.iter().copied().collect();
    let chosen: Vec<Value> = raqes
        .iter()
        .zip(mapping)
        .zip(&latencies)
        .map(|((r, &di), &latency)| {
            let d = &deployments[di];
            json!({
                "rqe": r.id,
                "sketch": d.config.sketch,
                "params": d.config.sketch_config["params"],
                "window_secs": d.window_ms / 1000,
                "slide_secs": d.slide_ms / 1000,
                "merged_shards": r.lookback_ms / d.window_ms,
                "ingest_workers": rqe_optimizer::analytical_cost_model::ingest_workers(d, &w.facts),
                "latency_ms": latency,
                "deployment": di,
                "asap_accuracy": w.asap_accuracy(r, d),
                "accuracy_source": w
                    .curves
                    .accuracy_with_source(r, d, &w.facts)
                    .map(|(_, source)| format!("{source:?}")),
            })
        })
        .collect();
    json!({
        "objective": cost.value,
        "cpu": cost.cpu,
        "gib": gib(cost.bytes),
        "cpu_parts": {"ingest": ingest_cpu, "compaction": compaction_cpu, "query": query_cpu},
        "gib_parts": {
            "ingest": gib(ingest_bytes),
            "storage": gib(storage_bytes),
            "compaction": gib(compaction_bytes),
            "query": gib(query_bytes),
        },
        "latency_ms": cost.latency_ms,
        "median_latency_ms": (!latencies.is_empty()).then(|| median(latencies.clone())),
        "active_deployments": active.len(),
        "chosen": chosen,
    })
}

/// `--slas-ms`: comma-separated SLAs in ms, each positive and finite.
fn parse_slas(list: &str) -> Vec<f64> {
    let slas: Vec<f64> = list
        .split(',')
        .map(|x| {
            let sla: f64 = x
                .trim()
                .parse()
                .unwrap_or_else(|_| panic!("--slas-ms: {x:?} is not a number"));
            assert!(
                sla.is_finite() && sla > 0.0,
                "--slas-ms: {x:?} must be positive"
            );
            sla
        })
        .collect();
    assert!(!slas.is_empty(), "--slas-ms is empty");
    slas
}

/// Whether a latency meets an SLA, with [`LATENCY_SLACK`]. A method has a plan
/// at an SLA iff its tightest feasible bound meets it.
fn meets_sla(latency_ms: f64, sla_ms: f64) -> bool {
    latency_ms <= sla_ms * (1.0 + LATENCY_SLACK)
}

/// Points on each method's cost–latency frontier: bounds log-spaced from the
/// tightest feasible one to the unbounded plan's latency.
const FRONTIER_POINTS: usize = 12;

/// The tightest latency bound any plan over `candidates` can meet: the
/// largest, over RQEs, of each RQE's fastest chain among its (allowed)
/// eligible candidates.
fn tightest_bound_ms(
    w: &Workload,
    raqes: &[Raqe],
    candidates: &[Deployment],
    allowed: Option<&[Vec<usize>]>,
) -> f64 {
    let accuracy = |r: &Raqe, d: &Deployment| w.asap_accuracy(r, d);
    raqes
        .iter()
        .enumerate()
        .map(|(i, r)| {
            eligible_deployments_for(r, candidates, &w.facts, &accuracy)
                .into_iter()
                .filter(|d| allowed.is_none_or(|allowed| allowed[i].contains(d)))
                .map(|d| chain_ms(r, &candidates[d], &w.facts))
                .fold(f64::INFINITY, f64::min)
        })
        .fold(0.0, f64::max)
}

/// `n` bounds log-spaced over `[lo, hi]`, plus `extra` when it falls inside.
fn frontier_bounds(lo: f64, hi: f64, n: usize, extra: f64) -> Vec<f64> {
    let mut bounds: Vec<f64> = if hi > lo && lo > 0.0 {
        (0..n)
            .map(|k| lo * (hi / lo).powf(k as f64 / (n - 1) as f64))
            .collect()
    } else {
        vec![lo]
    };
    if extra >= lo && extra < hi {
        bounds.push(extra);
    }
    bounds.sort_by(f64::total_cmp);
    bounds
}

/// Every method's plan for `w`, priced by use at each weight setting, with
/// its latency (`docs/rqe_sketch_deployment_v1.md`, "Cost by use and batch
/// latency"): version 1 (no latency constraint; frontier) in `results`,
/// version 2 (each SLA of `slas`) in `sla_results`.
fn evaluate(w: &Workload, runs: usize, weights: &[(&str, f64, f64)], slas: &[f64]) -> Value {
    let asap_accuracy = |r: &Raqe, d: &Deployment| w.asap_accuracy(r, d);
    // Drop RQEs that either method cannot serve, so both plan the same batch.
    let all_candidates = candidates(w, &w.raqes);
    let asap_unservable: Vec<String> = w
        .raqes
        .iter()
        .filter(|r| {
            eligible_deployments_for(r, &all_candidates, &w.facts, &asap_accuracy).is_empty()
        })
        .map(|r| r.id.clone())
        .collect();
    let autosketch_unservable = autosketch_plan(w, &w.raqes).err().unwrap_or_default();
    let dropped: BTreeSet<String> = asap_unservable
        .iter()
        .chain(&autosketch_unservable)
        .cloned()
        .collect();
    let raqes: Vec<Raqe> = w
        .raqes
        .iter()
        .filter(|r| !dropped.contains(&r.id))
        .cloned()
        .collect();
    eprintln!(
        "{}: {} RQEs, {} dropped",
        w.name,
        raqes.len(),
        dropped.len()
    );
    if let Err(problems) = validate_facts(&raqes, &w.facts) {
        panic!("invalid facts: {problems:?}");
    }

    // AutoSketch: one plan, independent of the weights.
    let mut search_secs = Vec::new();
    let mut plan = None;
    for _ in 0..runs {
        let started = Instant::now();
        plan = Some(autosketch_plan(w, &raqes).expect("servable RQEs only"));
        search_secs.push(started.elapsed().as_secs_f64());
    }
    let plan = plan.unwrap();
    let probes: usize = plan.searches.iter().map(|s| s.probes.len()).sum();
    // The distinct probed (metric, config) pairs and the data shape each is
    // benchmarked on: AutoSketch benchmarks a config once per metric, on that
    // metric's data. scripts/autosketch_benchmark_time.py times them.
    let mut probed_configs = BTreeMap::new();
    for s in &plan.searches {
        let r = raqes.iter().find(|r| r.id == s.raqe_id).unwrap();
        let shape = w.facts[&r.metric].data_shape.get(&r.grouping_labels);
        for &p in &s.probes {
            let c = &w.costs[&r.metric][p];
            let metric = &w.metric_of[&r.id];
            probed_configs
                .entry(format!(
                    "{metric}/{}/{}",
                    c.sketch, c.sketch_config["params"]
                ))
                .or_insert_with(|| {
                    json!({
                        "metric": metric,
                        "sketch": c.sketch,
                        "params": c.sketch_config["params"],
                        "zipf_s": shape.map(|d| d.zipf_s),
                        "distinct_keys": shape.map(|d| d.distinct_keys),
                        "tail_index": shape.map(|d| d.tail_index),
                    })
                });
        }
    }
    let distinct_probes = probed_configs.len();
    let paper_secs = distinct_probes as f64 * PAPER_SECS_PER_PROBE;

    // ASAP's candidates, for the whole batch.
    let mut build_secs = Vec::new();
    let mut asap_candidates = Vec::new();
    for _ in 0..runs {
        let started = Instant::now();
        asap_candidates = candidates(w, &raqes);
        build_secs.push(started.elapsed().as_secs_f64());
    }
    // PerQuery's: each RQE's own candidates, as separate copies, so no two
    // RQEs share a deployment.
    let started = Instant::now();
    let mut perquery_candidates: Vec<Deployment> = Vec::new();
    let mut allowed: Vec<Vec<usize>> = Vec::new();
    for r in &raqes {
        let own = candidates(w, std::slice::from_ref(r));
        allowed.push((perquery_candidates.len()..perquery_candidates.len() + own.len()).collect());
        perquery_candidates.extend(own);
    }
    let perquery_build_secs = started.elapsed().as_secs_f64();

    // AutoSketch's plan ignores the weights and latency: one point.
    let auto_latency_ms = usage_cost(
        &PlanLoad::new(&raqes, &plan.deployments, &plan.mapping, &w.facts),
        1.0,
        0.0,
    )
    .latency_ms;
    let asap_tightest = tightest_bound_ms(w, &raqes, &asap_candidates, None);
    let perquery_tightest = tightest_bound_ms(w, &raqes, &perquery_candidates, Some(&allowed));

    let mut results = Vec::new();
    let mut sla_results = Vec::new();
    let mut sanity = Vec::new();
    for &(weight_name, w_cpu, w_mem) in weights {
        let solve =
            |candidates: &[Deployment], allowed: Option<&[Vec<usize>]>, bound: Option<f64>| {
                let started = Instant::now();
                let solved = minimize_usage_cost(
                    &raqes,
                    candidates,
                    &w.facts,
                    w_cpu,
                    w_mem,
                    &asap_accuracy,
                    allowed,
                    bound,
                );
                (solved, started.elapsed().as_secs_f64())
            };
        // ASAP unbounded: the cheapest plan, timed over `runs`.
        let mut solve_secs = Vec::new();
        let mut solution = None;
        for _ in 0..runs {
            let (solved, secs) = solve(&asap_candidates, None, None);
            solve_secs.push(secs);
            solution = Some(solved);
        }
        let asap = match solution.unwrap() {
            Ok(s) => {
                let mut v = summarize(w, &raqes, &asap_candidates, &s.mapping, w_cpu, w_mem);
                v["planning_secs"] = json!(median(build_secs.clone()) + median(solve_secs.clone()));
                v["candidate_build_secs"] = json!(median(build_secs.clone()));
                v["milp_solve_secs"] = json!(median(solve_secs.clone()));
                v["candidates"] = json!(asap_candidates.len());
                v
            }
            Err(e) => json!({"error": e.to_string()}),
        };
        let (perquery, perquery_solve_secs) = solve(&perquery_candidates, Some(&allowed), None);
        let mut perq = match perquery {
            Ok(s) => summarize(w, &raqes, &perquery_candidates, &s.mapping, w_cpu, w_mem),
            Err(e) => json!({"error": e.to_string()}),
        };
        perq["planning_secs"] = json!(perquery_solve_secs + perquery_build_secs);
        let mut auto = summarize(w, &raqes, &plan.deployments, &plan.mapping, w_cpu, w_mem);
        auto["planning_secs"] = json!(median(search_secs.clone()));
        auto["probes"] = json!(probes);
        auto["benchmark_secs_paper_rate"] = json!(paper_secs);

        // ASAP costs no more than PerQuery, nor than AutoSketch when every
        // AutoSketch choice is one ASAP could make.
        let cost = |v: &Value| v["objective"].as_f64();
        if let (Some(a), Some(b)) = (cost(&asap), cost(&perq)) {
            if a > b * (1.0 + 1e-6) {
                sanity.push(json!({"check": "asap <= perquery", "weights": weight_name, "asap": a, "perquery": b}));
            }
        }
        if let (Some(a), Some(b)) = (cost(&asap), cost(&auto)) {
            let not_eligible: Vec<&str> = raqes
                .iter()
                .zip(&plan.mapping)
                .filter(|(r, &d)| !is_eligible(r, &plan.deployments[d], &w.facts, &asap_accuracy))
                .map(|(r, _)| r.id.as_str())
                .collect();
            if not_eligible.is_empty() && a > b * (1.0 + 1e-6) {
                sanity.push(json!({"check": "asap <= autosketch", "weights": weight_name, "asap": a, "autosketch": b}));
            }
        }
        if asap.get("error").is_some() || perq.get("error").is_some() {
            sanity.push(json!({"check": "every method solves", "weights": weight_name, "asap": asap.get("error"), "perquery": perq.get("error")}));
        }

        // The frontiers: the cheapest plan at most `L` slow, for a sweep of
        // `L` from the tightest feasible bound to the unbounded latency, and
        // at AutoSketch's latency.
        for (method, candidates, allowed, tightest, unbounded) in [
            ("asap", &asap_candidates, None, asap_tightest, &asap),
            (
                "perquery",
                &perquery_candidates,
                Some(&allowed[..]),
                perquery_tightest,
                &perq,
            ),
        ] {
            let Some(hi) = unbounded["latency_ms"].as_f64() else {
                continue;
            };
            let mut previous: Option<f64> = None;
            for bound in frontier_bounds(tightest, hi, FRONTIER_POINTS, auto_latency_ms) {
                let (solved, secs) = solve(candidates, allowed, Some(bound));
                let mut v = match solved {
                    Ok(s) => summarize(w, &raqes, candidates, &s.mapping, w_cpu, w_mem),
                    Err(e) => json!({"error": e.to_string()}),
                };
                // A looser bound never costs more.
                if let (Some(prev), Some(now)) = (previous, cost(&v)) {
                    if now > prev * (1.0 + 1e-6) {
                        sanity.push(json!({"check": "frontier is monotone", "method": method, "weights": weight_name, "bound_ms": bound}));
                    }
                }
                previous = cost(&v).or(previous);
                v["method"] = json!(method);
                v["weights"] = json!(weight_name);
                v["bound_ms"] = json!(bound);
                v["milp_solve_secs"] = json!(secs);
                v["rqes"] = json!(raqes.len());
                results.push(v);
            }
        }
        // Version 2: at each SLA, the cheapest plan whose batch latency meets
        // it; AutoSketch's one plan, with whether it meets it.
        for &sla in slas {
            let mut at_sla = Vec::new();
            for (method, candidates, allowed, tightest) in [
                ("asap", &asap_candidates, None, asap_tightest),
                (
                    "perquery",
                    &perquery_candidates,
                    Some(&allowed[..]),
                    perquery_tightest,
                ),
            ] {
                let mut v = if meets_sla(tightest, sla) {
                    let (solved, secs) = solve(candidates, allowed, Some(sla));
                    let mut v = match solved {
                        Ok(s) => summarize(w, &raqes, candidates, &s.mapping, w_cpu, w_mem),
                        Err(e) => json!({"error": e.to_string()}),
                    };
                    v["milp_solve_secs"] = json!(secs);
                    v["infeasible"] = json!(false);
                    if v.get("error").is_some()
                        || v["latency_ms"].as_f64().is_some_and(|l| !meets_sla(l, sla))
                    {
                        sanity.push(json!({"check": "plan meets the SLA", "method": method, "weights": weight_name, "sla_ms": sla, "latency_ms": v.get("latency_ms"), "error": v.get("error")}));
                    }
                    v
                } else {
                    json!({"infeasible": true, "tightest_bound_ms": tightest})
                };
                v["method"] = json!(method);
                at_sla.push(v);
            }
            if let (Some(a), Some(b)) = (cost(&at_sla[0]), cost(&at_sla[1])) {
                if a > b * (1.0 + 1e-6) {
                    sanity.push(json!({"check": "asap <= perquery at the SLA", "weights": weight_name, "sla_ms": sla, "asap": a, "perquery": b}));
                }
            }
            let mut v = auto.clone();
            v["meets_sla"] = json!(meets_sla(auto_latency_ms, sla));
            v["method"] = json!("autosketch");
            at_sla.push(v);
            for mut v in at_sla {
                v["weights"] = json!(weight_name);
                v["sla_ms"] = json!(sla);
                v["rqes"] = json!(raqes.len());
                sla_results.push(v);
            }
        }

        for (method, value) in [("asap", asap), ("autosketch", auto), ("perquery", perq)] {
            let mut v = value;
            v["method"] = json!(method);
            v["weights"] = json!(weight_name);
            v["bound_ms"] = Value::Null;
            v["rqes"] = json!(raqes.len());
            results.push(v);
        }
    }
    json!({
        "workload": w.name,
        "rqes": raqes.len(),
        "dropped_unservable": {"asap": asap_unservable, "autosketch": autosketch_unservable},
        "excluded_high_cardinality": w.excluded_high_cardinality,
        "streams": raqes.iter().map(|r| &r.metric).collect::<BTreeSet<_>>().len(),
        "runs": runs,
        "notes": w.notes,
        "cost_model": "w_cpu * AUC(CPU) + w_mem * AUC(memory), billed by use; latency = longest chain, reported",
        "weights": weights.iter().map(|&(name, w_cpu, w_mem)| {
            json!({"name": name, "w_cpu": w_cpu, "w_mem": w_mem})
        }).collect::<Vec<_>>(),
        "frontier": {
            "points": FRONTIER_POINTS,
            "asap_tightest_bound_ms": asap_tightest,
            "perquery_tightest_bound_ms": perquery_tightest,
            "autosketch_latency_ms": auto_latency_ms,
        },
        "autosketch": {
            "probes": probes,
            "search_secs": median(search_secs),
            "distinct_probes": distinct_probes,
            "probed_configs": probed_configs.values().collect::<Vec<_>>(),
            "benchmark_secs_paper_rate": paper_secs,
        },
        "sla_grid_ms": slas,
        "sanity_violations": sanity,
        "results": results,
        "sla_results": sla_results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trace_key_query_keeps_one_accumulator_per_key() {
        let key_query =
            json!({"kind": "keys", "label_set": {"groups": 1, "keys_per_window": 26019.0}});
        assert_eq!(groups_of(&key_query, capability_of(&key_query)), 26019);
        let quantile =
            json!({"kind": "values", "label_set": {"groups": 1, "keys_per_window": 5.0}});
        assert_eq!(groups_of(&quantile, capability_of(&quantile)), 1);
        let synthetic = json!({"capability": "sum", "kind": "keys", "label_set": {"groups": 10, "keys_per_window": 1e4}});
        assert_eq!(groups_of(&synthetic, Capability::Sum), 10);
    }

    #[test]
    fn topk_k_comes_from_the_query_id() {
        let r = json!({"query_id": "topk32_sum_by_label0_rate"});
        assert_eq!(topk_k_of(&r, Capability::TopKByValue), Some(32));
        let r = json!({"query_id": "topk100_x"});
        assert_eq!(topk_k_of(&r, Capability::TopKByValue), Some(100));
        assert_eq!(topk_k_of(&r, Capability::Quantile), None);
    }

    #[test]
    fn slas_parse_and_reject_nonsense() {
        assert_eq!(parse_slas("100, 1000,2.5"), vec![100.0, 1000.0, 2.5]);
        for bad in ["", "0", "-5", "inf", "fast"] {
            assert!(
                std::panic::catch_unwind(|| parse_slas(bad)).is_err(),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn a_method_has_a_plan_at_an_sla_iff_its_tightest_bound_meets_it() {
        assert!(meets_sla(100.0, 100.0));
        // Float residue in summing µs-scale work is not a miss.
        assert!(meets_sla(100.0 * (1.0 + 1e-12), 100.0));
        assert!(!meets_sla(100.1, 100.0));
    }

    #[test]
    fn one_95_percent_level_in_each_familys_metric() {
        assert_eq!(strictness_target("p95", Capability::Sum), 0.0);
        assert_eq!(strictness_target("p95", Capability::RateOrIncrease), 0.0);
        assert_eq!(strictness_target("p95", Capability::Quantile), 0.05);
        assert_eq!(strictness_target("p95", Capability::TopKByValue), 0.95);
    }
}
