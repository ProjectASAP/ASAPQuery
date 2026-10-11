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
//! `docs/rqe_optimizer_cost_model.md`, "Cost by use and batch latency"):
//! `w_cpu · AUC(CPU) + w_mem · AUC(memory)`, CPU elastic, at each weight
//! setting of #777 §4 (CPU only; Fargate's per-vCPU and per-GB prices). A
//! batch's latency is its longest chain (the newest window's compaction,
//! then the query): the job placement's closed form with elastic CPU. Two
//! versions, in one output file:
//!
//! - **Version 1, no latency constraint** (`results`): each method's cheapest
//!   plan, its latency reported, and ASAP's (with and without roll-ups) and
//!   PerQuery's cost–latency frontiers (`bound_ms`).
//! - **Version 2, a batch latency SLA** (`sla_results`): at each SLA of the
//!   grid, ASAP's (both) and PerQuery's cheapest plan whose batch latency is at most
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
//! - **ASAP (no roll-ups)** (`asap-norollup`), an ablation: ASAP's MILP and
//!   candidates, with each RQE allowed only deployments at its own grouping
//!   or Hydra grids (not a roll-up). Every result counts `rolled_up_rqes`,
//!   the RQEs served from a strictly finer non-Hydra deployment.
//!
//! `traces` reads `data/autosketch-eval/table.json` (alibaba_v2022 and
//! google_2011 are evaluated; boom is left out for now); `synthetic` reads one
//! table written by `export_autosketch_eval_table.py --synthetic`. A table
//! gives the workload: RQEs, and each stream's groups, rate and data shape
//! (the family's `grid_param`/`grid_K`, the worst case rounded to the grid).
//! Each stream (`traces`: dataset, query and kind; `synthetic`: the table's
//! `stream`) is one metric, which is where sharing comes from. A stream of a
//! metric with a label schema (the table's `schemas`) has the schema's labels,
//! and its RQEs name their `grouping`, so one stream carries several
//! groupings and a fine deployment can serve a coarse RQE (a roll-up).
//! Accuracy is read at the mean group's items per window, or, for an RQE with
//! a `covers_share` (`null`: every group), at its smallest and its largest
//! covered group's (`min_covered_share` and `max_covered_share` of the
//! stream's), the worse of the two: a per-group sketch's error need not fall
//! as N grows (sketch-bench#189), so either may be the hardest group.
//!
//! Costs and accuracy come from `--saturation-dir`, the same inputs the
//! planner reads (#174): the cost table `optimizer_cost/rqe_atomic_costs.json`
//! (one row per config, at the evaluation dataset's shape) and the saturation
//! curves. ASAP reads `SaturationCurves::accuracy` (the curve at the
//! lookback's items; KLL and top-k merged from `L/x` shards read the merge
//! curve, #158); AutoSketch reads `autosketch_accuracy` (one unmerged sketch
//! per query window, no saturation requirement).
//!
//! Hydra: ASAP and PerQuery plan with undeployable families allowed, so a
//! Hydra grid over a schema metric's full schema competes with per-group
//! sketches (sketch-bench `docs/rqe_optimizer_hydra.md`). Its cost rows and
//! accuracy (`hydra_saturation.csv` under the saturation dir) are measured
//! on the dataset shaped like the stream's metric ([`hydra_dataset`]), at
//! that full schema; without them no Hydra deployment is eligible. Hydra is
//! held to the worst case (max over covered groups, worst over N and seeds),
//! per-group sketches to seed-mean curves. AutoSketch allows undeployable
//! families too but skips Hydra itself (#159).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use rqe_optimizer::analytical_cost_model::chain_ms;
use rqe_optimizer::autosketch;
use rqe_optimizer::candidates::{build_all_candidates, eligible_deployments_for, is_eligible};
use rqe_optimizer::milp::minimize_usage_cost;
use rqe_optimizer::saturation::{AccuracySource, DataShape, SaturationCurves, COST_TABLE};
use rqe_optimizer::usage::{usage_cost, PlanLoad};
use rqe_optimizer::{
    accuracy_key, validate_facts, AccuracyDirection, AtomicCostEntry, AtomicCostTable, Capability,
    Deployment, LabelSet, Mapping, MetricFacts, Millis, Raqe, WorkloadFacts,
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
    /// The table's `covers_share` by RQE id, for RQEs that name one (`None`:
    /// every group).
    covers_share: BTreeMap<String, Option<f64>>,
    /// Facts for reading an RQE's accuracy at its smallest covered group, by
    /// RQE id, for RQEs with a `covers_share` (`null` too): its stream's,
    /// with the series scaled so the mean group gets that group's items.
    covered_facts: BTreeMap<String, WorkloadFacts>,
    /// The same, at its largest covered group.
    largest_facts: BTreeMap<String, WorkloadFacts>,
    curves: SaturationCurves,
    notes: Vec<String>,
}

impl Workload {
    /// The facts `r`'s accuracy is read with.
    fn accuracy_facts(&self, r: &Raqe) -> &WorkloadFacts {
        self.covered_facts.get(&r.id).unwrap_or(&self.facts)
    }

    /// `read` at `r`'s accuracy facts, and for an RQE with a largest covered
    /// group also at its, the worse in `sketch`'s direction; unknown if
    /// either is.
    fn worse_read<T>(
        &self,
        r: &Raqe,
        sketch: &str,
        read: impl Fn(&WorkloadFacts) -> Option<(f64, T)>,
    ) -> Option<(f64, T)> {
        let smallest = read(self.accuracy_facts(r))?;
        let Some(facts) = self.largest_facts.get(&r.id) else {
            return Some(smallest);
        };
        let largest = read(facts)?;
        let largest_worse = match accuracy_key(sketch).1 {
            AccuracyDirection::LowerIsBetter => largest.0 > smallest.0,
            AccuracyDirection::HigherIsBetter => largest.0 < smallest.0,
        };
        Some(if largest_worse { largest } else { smallest })
    }

    /// ASAP's accuracy: the planner's own lookup, and where it came from.
    fn accuracy_with_source(&self, r: &Raqe, d: &Deployment) -> Option<(f64, AccuracySource)> {
        self.worse_read(r, &d.config.sketch, |facts| {
            self.curves.accuracy_with_source(r, d, facts)
        })
    }

    /// ASAP's accuracy.
    fn asap_accuracy(&self, r: &Raqe, d: &Deployment) -> Option<f64> {
        self.accuracy_with_source(r, d).map(|(value, _)| value)
    }

    /// AutoSketch's: one unmerged sketch per query window.
    fn autosketch_accuracy(&self, r: &Raqe, c: &AtomicCostEntry) -> Option<f64> {
        self.worse_read(r, &c.sketch, |facts| {
            Some((self.curves.autosketch_accuracy(r, c, facts)?, ()))
        })
        .map(|(value, _)| value)
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
        (Some("cardinality"), _) => Capability::Cardinality,
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

/// The `hydra_saturation.csv` dataset measured with a schema metric's
/// labels, shares and values (`configs/datagen/hydra_*.yaml`): `http`'s
/// latency for quantiles, its user ids otherwise, and `flows`' sources.
/// `None` for any other metric, which no Hydra deployment serves.
fn hydra_dataset(metric: &str, capability: Capability) -> Option<String> {
    let dataset = match (metric.rsplit('/').next()?, capability) {
        ("http", Capability::Quantile) => "hydra_http_latency",
        ("http", _) => "hydra_http",
        ("flows", _) => "hydra_flows",
        _ => return None,
    };
    Some(dataset.to_string())
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
    let mut covers_share = BTreeMap::new();
    let mut min_covered_share = BTreeMap::new();
    let mut max_covered_share = BTreeMap::new();
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
        // A schema metric's RQE names its grouping (its `groups` are that
        // grouping's); otherwise the stream has one label `g`, the grouping.
        let schema = &workload["schemas"][metric];
        let grouping: LabelSet = match r["grouping"].as_array() {
            Some(labels) => labels
                .iter()
                .map(|l| l.as_str().unwrap().to_string())
                .collect(),
            None if groups > 1 => label_set(&["g"]),
            None => LabelSet::new(),
        };
        // `null` covers every group, so its smallest is read too.
        if let Some(share) = r.get("covers_share") {
            covers_share.insert(id.clone(), share.as_f64());
            min_covered_share.insert(id.clone(), r["min_covered_share"].as_f64().unwrap());
            max_covered_share.insert(id.clone(), r["max_covered_share"].as_f64().unwrap());
        }
        // One metric per stream, labels {g, x} (or the schema's and x), and
        // with one scrape a second the full label set has one series per
        // sample/s. A stream's RQEs differ only in range and grouping, so
        // take the largest per-RQE rate (conservative). Rates below `groups`
        // samples/s are rounded up so every group has a series.
        let series = (rate * SCRAPE_MS as f64 / 1000.0).round() as u64;
        let most = stream_series.entry(stream.clone()).or_default();
        *most = (*most).max(series);
        let most = *most;
        // A schema metric's full schema (its labels without `x`) and its
        // groups: the product of every label's fan-out, as the generator's
        // grouping_cardinality (the full set holds every child's parent).
        let full_schema: Option<(LabelSet, u64)> = schema["labels"].as_array().map(|labels| {
            let names = labels
                .iter()
                .map(|l| l["name"].as_str().unwrap().to_string())
                .collect();
            let groups = labels
                .iter()
                .map(|l| l["cardinality"].as_u64().unwrap())
                .product();
            (names, groups)
        });
        let metric = facts.entry(stream.clone()).or_insert_with(|| {
            let mut labels = full_schema
                .as_ref()
                .map_or_else(|| label_set(&["g"]), |(names, _)| names.clone());
            labels.insert("x".to_string());
            MetricFacts {
                cardinality: [(labels.clone(), 0), (LabelSet::new(), 1)].into(),
                labels,
                scrape_interval_ms: SCRAPE_MS,
                value_range: None,
                data_shape: BTreeMap::new(),
                // A stream is one value of its metric, so one capability.
                hydra_dataset: full_schema.as_ref().and_then(|(names, _)| {
                    Some((hydra_dataset(metric, capability)?, names.clone()))
                }),
            }
        });
        // A stream's RQEs may see different group counts (keys per window
        // grow with the range): take the largest (conservative).
        let card = metric.cardinality.entry(grouping.clone()).or_default();
        *card = (*card).max(groups);
        let total = metric.cardinality.get_mut(&metric.labels).unwrap();
        *total = (*total).max(series).max(groups);
        // The full schema (a Hydra grid's, and its tracker's keys), capped at
        // the stream's series so it doesn't make the stream high-cardinality.
        if let Some((names, groups)) = full_schema {
            let card = metric.cardinality.entry(names).or_default();
            *card = (*card).max(groups.min(most));
        }
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
            accuracy_covers_share: r.get("covers_share").and_then(Value::as_f64),
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
        .filter(|(stream, m)| {
            m.cardinality
                .iter()
                .any(|(labels, &card)| labels != &m.labels && card > stream_series[*stream])
        })
        .map(|(stream, _)| stream.clone())
        .collect();
    let excluded_high_cardinality: Vec<String> = raqes
        .iter()
        .filter(|r| high_cardinality.contains(&r.metric))
        .map(|r| r.id.clone())
        .collect();
    raqes.retain(|r| !high_cardinality.contains(&r.metric));
    facts.retain(|stream, _| !high_cardinality.contains(stream));
    // Only the items per group read the series count: scaled by share·groups,
    // the mean group gets the smallest (largest) covered group's items.
    let scaled = |shares: &BTreeMap<String, f64>| {
        raqes
            .iter()
            .filter_map(|r| {
                let share = shares.get(&r.id)?;
                let mut m = facts[&r.metric].clone();
                let groups = m.cardinality[&r.grouping_labels] as f64;
                let series = m.cardinality.get_mut(&m.labels).unwrap();
                *series = (*series as f64 * share * groups).round() as u64;
                Some((r.id.clone(), [(r.metric.clone(), m)].into()))
            })
            .collect()
    };
    let covered_facts = scaled(&min_covered_share);
    let largest_facts = scaled(&max_covered_share);
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
        covers_share,
        covered_facts,
        largest_facts,
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

/// ASAP candidates, built per stream from that stream's entries, with
/// undeployable families (Hydra) allowed.
fn candidates(w: &Workload, raqes: &[Raqe]) -> Vec<Deployment> {
    group_by_stream(raqes)
        .into_iter()
        .flat_map(|(stream, group)| {
            let costs = w.costs.get(stream).map_or(&[][..], Vec::as_slice);
            build_all_candidates(&group, costs, &w.facts, true, &|r, d| w.asap_accuracy(r, d))
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
        // Undeployable families allowed, as for ASAP: AutoSketch skips
        // Hydra itself (#159), so every method sees the same other families.
        match autosketch::plan(&group, costs, SEED, true, oracle) {
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

/// Whether `d` serves `r` as a roll-up: a non-Hydra deployment at another
/// grouping (one that serves `r` is strictly finer). A Hydra grid answers any
/// subgrouping directly.
fn is_rollup(r: &Raqe, d: &Deployment) -> bool {
    !d.properties().answers_any_subgrouping && d.grouping_labels != r.grouping_labels
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
    let hydra_rqes = mapping
        .iter()
        .filter(|&&d| deployments[d].properties().answers_any_subgrouping)
        .count();
    let rolled_up_rqes = raqes
        .iter()
        .zip(mapping)
        .filter(|(r, &d)| is_rollup(r, &deployments[d]))
        .count();
    let chosen: Vec<Value> = raqes
        .iter()
        .zip(mapping)
        .zip(&latencies)
        .map(|((r, &di), &latency)| {
            let d = &deployments[di];
            let mut choice = json!({
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
                    .accuracy_with_source(r, d)
                    .map(|(_, source)| format!("{source:?}")),
            });
            // A roll-up or a Hydra grid: the deployment is grouped finer than
            // the RQE.
            if d.grouping_labels != r.grouping_labels {
                choice["deployment_grouping"] = json!(d.grouping_labels);
            }
            if let Some(share) = w.covers_share.get(&r.id) {
                choice["covers_share"] = json!(share);
            }
            choice
        })
        .collect();
    let mut v = json!({
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
        "rolled_up_rqes": rolled_up_rqes,
        "chosen": chosen,
    });
    // Only workloads Hydra may serve report it, so others read as before.
    if w.facts.values().any(|m| m.hydra_dataset.is_some()) {
        v["hydra_rqes"] = json!(hydra_rqes);
    }
    v
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
/// its latency (`docs/rqe_optimizer_cost_model.md`, "Cost by use and batch
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
    for (r, facts) in raqes.iter().flat_map(|r| {
        [&w.covered_facts, &w.largest_facts]
            .into_iter()
            .filter_map(move |f| Some((r, f.get(&r.id)?)))
    }) {
        if let Err(problems) = validate_facts(std::slice::from_ref(r), facts) {
            panic!("invalid covered facts: {problems:?}");
        }
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
    // ASAP without roll-ups: ASAP's candidates, each RQE kept off any finer
    // non-Hydra deployment.
    let norollup_allowed: Vec<Vec<usize>> = raqes
        .iter()
        .map(|r| {
            (0..asap_candidates.len())
                .filter(|&d| !is_rollup(r, &asap_candidates[d]))
                .collect()
        })
        .collect();

    // AutoSketch's plan ignores the weights and latency: one point.
    let auto_latency_ms = usage_cost(
        &PlanLoad::new(&raqes, &plan.deployments, &plan.mapping, &w.facts),
        1.0,
        0.0,
    )
    .latency_ms;
    let asap_tightest = tightest_bound_ms(w, &raqes, &asap_candidates, None);
    let perquery_tightest = tightest_bound_ms(w, &raqes, &perquery_candidates, Some(&allowed));
    let norollup_tightest = tightest_bound_ms(w, &raqes, &asap_candidates, Some(&norollup_allowed));

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
        let (norollup, norollup_solve_secs) =
            solve(&asap_candidates, Some(&norollup_allowed), None);
        let norollup = match norollup {
            Ok(s) => {
                let mut v = summarize(w, &raqes, &asap_candidates, &s.mapping, w_cpu, w_mem);
                v["planning_secs"] = json!(median(build_secs.clone()) + norollup_solve_secs);
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

        // ASAP costs no more than without roll-ups, which costs no more than
        // PerQuery (whose own candidates are at each RQE's grouping or
        // Hydra), nor than AutoSketch when every AutoSketch choice is one ASAP
        // could make.
        let cost = |v: &Value| v["objective"].as_f64();
        // `sanity` gets a violation of `cheaper <= dearer`, both solved.
        let ordered = |sanity: &mut Vec<Value>,
                       at: Value,
                       cheaper: (&str, &Value),
                       dearer: (&str, &Value)| {
            if let (Some(a), Some(b)) = (cost(cheaper.1), cost(dearer.1)) {
                if a > b * (1.0 + 1e-6) {
                    let mut v = json!({"check": format!("{} <= {}", cheaper.0, dearer.0), "weights": weight_name, (cheaper.0): a, (dearer.0): b});
                    v.as_object_mut()
                        .unwrap()
                        .extend(at.as_object().unwrap().clone());
                    sanity.push(v);
                }
            }
        };
        for (cheaper, dearer) in [
            (("asap", &asap), ("asap-norollup", &norollup)),
            (("asap-norollup", &norollup), ("perquery", &perq)),
            (("asap", &asap), ("perquery", &perq)),
        ] {
            ordered(&mut sanity, json!({}), cheaper, dearer);
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
        if [&asap, &norollup, &perq]
            .iter()
            .any(|v| v.get("error").is_some())
        {
            sanity.push(json!({"check": "every method solves", "weights": weight_name, "asap": asap.get("error"), "asap-norollup": norollup.get("error"), "perquery": perq.get("error")}));
        }

        // The frontiers: the cheapest plan at most `L` slow, for a sweep of
        // `L` from the tightest feasible bound to the unbounded latency, and
        // at AutoSketch's latency.
        for (method, candidates, allowed, tightest, unbounded) in [
            ("asap", &asap_candidates, None, asap_tightest, &asap),
            (
                "asap-norollup",
                &asap_candidates,
                Some(&norollup_allowed[..]),
                norollup_tightest,
                &norollup,
            ),
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
                    "asap-norollup",
                    &asap_candidates,
                    Some(&norollup_allowed[..]),
                    norollup_tightest,
                ),
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
            for (i, j) in [(0, 1), (1, 2), (0, 2)] {
                ordered(
                    &mut sanity,
                    json!({"sla_ms": sla}),
                    (at_sla[i]["method"].as_str().unwrap(), &at_sla[i]),
                    (at_sla[j]["method"].as_str().unwrap(), &at_sla[j]),
                );
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

        for (method, value) in [
            ("asap", asap),
            ("asap-norollup", norollup),
            ("autosketch", auto),
            ("perquery", perq),
        ] {
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
            "asap_norollup_tightest_bound_ms": norollup_tightest,
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

    /// A saturation dir with the committed cost table and inline HLL curves
    /// (each lg_k, uniform over 1e6 keys), so the test runs without the
    /// study's (gitignored) curves. `tag` keeps parallel tests' dirs apart.
    fn hll_saturation_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("hll-saturation-{tag}-{}", std::process::id()));
        let costs = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/results/autosketch-vs-asap-inputs/saturation/optimizer_cost/rqe_atomic_costs.json"
        );
        std::fs::create_dir_all(dir.join("optimizer_cost")).unwrap();
        std::fs::copy(costs, dir.join(COST_TABLE)).unwrap();
        for run in ["out_grid_1e7_cost", "out_1e9"] {
            let mut summary = "family,sketch,config,dist,param,cardinality,n_sat,final_error,\
                               error_metric,insert_cpu_secs,merge_cpu_secs,query_cpu_secs,memory_bytes\n"
                .to_string();
            let mut curve =
                "family,sketch,config,dist,param,cardinality,n,seed_mean_error,seed_se\n"
                    .to_string();
            for lg_k in [12, 14, 16].iter().filter(|_| run == "out_grid_1e7_cost") {
                let point = format!("cardinality,hll,lg_k={lg_k},zipf,0.0,1000000");
                summary += &format!("{point},1000,0.01,relative_error,,,,\n");
                for n in ["1000", "100000", "10000000"] {
                    curve += &format!("{point},{n},0.01,0.0\n");
                }
            }
            std::fs::create_dir_all(dir.join(run)).unwrap();
            std::fs::write(dir.join(run).join("saturation.csv"), summary).unwrap();
            std::fs::write(dir.join(run).join("saturation_curve.csv"), curve).unwrap();
        }
        dir
    }

    /// An HLL RQE on `http`'s user ids, grouped by `grouping`.
    fn http_rqe(grouping: &[&str], groups: u64, covers: Option<f64>, smallest: f64) -> Value {
        json!({
            "id": format!("t/{}", grouping.join(",")), "query_id": "q", "kind": "keys",
            "capability": "cardinality", "metric": "http", "stream": "http/user_id",
            "grouping": grouping, "covers_share": covers, "min_covered_share": smallest,
            "max_covered_share": 0.5,
            "lookback_secs": 300, "interval_secs": 60, "queries_per_instance": 1,
            "label_set": {"groups": groups, "arrival_rate_per_sec": 2e6},
            "families": [{"family": "hll", "sketch": "hll", "target": 0.02,
                          "grid_param": 0.0, "grid_K": 1e6}],
        })
    }

    /// A workload of `rqes` on the `http` schema, on [`hll_saturation_dir`].
    fn http_workload(tag: &str, rqes: Vec<Value>) -> Workload {
        let table = json!({"workloads": [{
            "dataset": "synthetic/test",
            "schemas": {"http": http_schema()},
            "rqes": rqes,
        }]});
        let inputs = hll_saturation_dir(tag);
        let path = inputs.join("table.json");
        std::fs::write(&path, table.to_string()).unwrap();
        let w = from_table(
            path.to_str().unwrap(),
            None,
            Some("p95".into()),
            inputs.to_str().unwrap(),
        );
        std::fs::remove_dir_all(&inputs).unwrap();
        w
    }

    /// The unbounded plan of `method` at the first weight setting.
    fn unbounded<'a>(result: &'a Value, method: &str) -> &'a Value {
        result["results"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["method"] == method && r["bound_ms"].is_null())
            .unwrap()
    }

    #[test]
    fn a_schema_stream_carries_every_grouping_and_rolls_up() {
        let w = http_workload(
            "schema",
            vec![
                http_rqe(&["region"], 4, Some(0.05), 0.05),
                http_rqe(&["region", "service"], 100, None, 0.001),
            ],
        );
        let facts = &w.facts["synthetic/test/http/user_id"];
        assert_eq!(
            facts.labels,
            label_set(&["region", "service", "endpoint", "status", "x"])
        );
        assert_eq!(facts.cardinality[&label_set(&["region"])], 4);
        assert_eq!(facts.cardinality[&label_set(&["region", "service"])], 100);
        assert_eq!(facts.cardinality[&facts.labels], 2_000_000);
        // Hydra reads `http`'s user ids over its full schema, at each RQE's
        // coverage.
        let (dataset, schema) = facts.hydra_dataset.as_ref().unwrap();
        assert_eq!(dataset, "hydra_http");
        assert_eq!(
            schema,
            &label_set(&["region", "service", "endpoint", "status"])
        );
        assert_eq!(facts.cardinality[schema], 4 * 25 * 25 * 4);
        let covers: Vec<_> = w.raqes.iter().map(|r| r.accuracy_covers_share).collect();
        assert_eq!(covers, [Some(0.05), None]);
        // Accuracy is read at the smallest covered group's items: 5% of the
        // stream's, so 4 · 5% of the series for {region}'s mean group.
        let covered = &w.covered_facts["t/region"]["synthetic/test/http/user_id"];
        assert_eq!(covered.cardinality[&covered.labels], 400_000);
        // `null` covers every group: 100 · 0.1% of the series.
        let every = &w.covered_facts["t/region,service"]["synthetic/test/http/user_id"];
        assert_eq!(every.cardinality[&every.labels], 200_000);
        // One {region, service} deployment serves both RQEs.
        let result = evaluate(&w, 1, &WEIGHTS[..1], &[1e4]);
        let asap = unbounded(&result, "asap");
        assert_eq!(asap["active_deployments"], 1);
        let coarse = &asap["chosen"][0];
        assert_eq!(coarse["rqe"], "t/region");
        assert_eq!(coarse["deployment_grouping"], json!(["region", "service"]));
        assert_eq!(coarse["covers_share"], 0.05);
        // No hydra_saturation.csv: Hydra serves nothing, and says so.
        assert_eq!(asap["hydra_rqes"], 0);
    }

    /// Without roll-ups, the coarse RQE needs its own deployment, which costs
    /// more than reading the fine one; every method counts its roll-ups.
    #[test]
    fn forbidding_roll_ups_costs_more_only_where_one_applies() {
        let cost = |v: &Value| v["objective"].as_f64().unwrap();
        let w = http_workload(
            "rollup",
            vec![
                http_rqe(&["region"], 4, Some(0.05), 0.05),
                http_rqe(&["region", "service"], 100, None, 0.001),
            ],
        );
        let result = evaluate(&w, 1, &WEIGHTS[..1], &[1e4]);
        assert_eq!(result["sanity_violations"], json!([]));
        let [asap, norollup, perquery] =
            ["asap", "asap-norollup", "perquery"].map(|m| unbounded(&result, m));
        assert_eq!(asap["rolled_up_rqes"], 1);
        assert_eq!(norollup["rolled_up_rqes"], 0);
        assert_eq!(perquery["rolled_up_rqes"], 0);
        assert_eq!(norollup["active_deployments"], 2);
        assert!(cost(asap) < cost(norollup), "{asap} vs {norollup}");
        assert!(cost(norollup) <= cost(perquery) * (1.0 + 1e-6));
        // The ablation has a frontier and a plan at each SLA, like ASAP.
        let norollup_rows = |key: &str| {
            result[key]
                .as_array()
                .unwrap()
                .iter()
                .filter(|r| r["method"] == "asap-norollup")
                .count()
        };
        assert!(norollup_rows("results") > 1);
        assert_eq!(norollup_rows("sla_results"), 1);

        // {region} and {service}: neither serves the other, so the two agree.
        let w = http_workload(
            "no-rollup",
            vec![
                http_rqe(&["region"], 4, Some(0.05), 0.05),
                http_rqe(&["service"], 25, Some(0.05), 0.01),
            ],
        );
        let result = evaluate(&w, 1, &WEIGHTS[..1], &[1e4]);
        assert_eq!(result["sanity_violations"], json!([]));
        let [asap, norollup] = ["asap", "asap-norollup"].map(|m| unbounded(&result, m));
        assert_eq!(asap["rolled_up_rqes"], 0);
        assert_eq!(cost(asap), cost(norollup));
    }

    /// The generator's `http` schema: endpoint fans out under service.
    fn http_schema() -> Value {
        json!({"labels": [
            {"name": "region", "cardinality": 4},
            {"name": "service", "cardinality": 25},
            {"name": "endpoint", "cardinality": 25, "child_of": "service"},
            {"name": "status", "cardinality": 4},
        ]})
    }

    /// `flows`' distinct sources and `http`'s p99 latency each get a Hydra
    /// grid over their full schema, from the cost rows measured on their own
    /// dataset. `flows`' 3e6 schema groups exceed its 1e6 series, so its
    /// cardinality is capped there and the stream kept.
    #[test]
    fn flows_and_http_latency_get_a_full_schema_hydra_grid() {
        use rqe_optimizer::candidates::build_all_candidates_unpruned;
        let rqe = |id: &str,
                   metric: &str,
                   value: &str,
                   capability: &str,
                   grouping: &[&str],
                   groups: u64,
                   rate: f64| {
            let (sketch, target) = if capability == "quantile" {
                ("kll-percall", 0.05)
            } else {
                ("hll", 0.02)
            };
            json!({
                "id": id, "query_id": "q", "kind": "keys", "capability": capability,
                "metric": metric, "stream": format!("{metric}/{value}"), "grouping": grouping,
                "covers_share": null, "min_covered_share": 0.01, "max_covered_share": 0.5,
                "lookback_secs": 300, "interval_secs": 60, "queries_per_instance": 1,
                "label_set": {"groups": groups, "arrival_rate_per_sec": rate},
                "families": [{"sketch": sketch, "target": target}],
            })
        };
        let flows = json!({"labels": [
            {"name": "dst_subnet", "cardinality": 1000},
            {"name": "dst_port", "cardinality": 1000},
            {"name": "proto", "cardinality": 3},
        ]});
        let table = json!({"workloads": [{
            "dataset": "synthetic/test",
            "schemas": {"flows": flows, "http": http_schema()},
            "rqes": [
                rqe("t11", "flows", "src_ip", "cardinality", &["dst_subnet"], 1000, 1e6),
                rqe("t16", "http", "latency", "quantile", &["service"], 25, 2e6),
            ],
        }]});
        let path = std::env::temp_dir().join(format!("hydra-table-{}.json", std::process::id()));
        std::fs::write(&path, table.to_string()).unwrap();
        // The committed cost table plus a Hydra row per (variant, dataset),
        // one measured on `http`'s user ids, which `flows` must not use.
        let inputs = hll_saturation_dir("hydra");
        let mut costs: AtomicCostTable =
            serde_json::from_str(&std::fs::read_to_string(inputs.join(COST_TABLE)).unwrap())
                .unwrap();
        let hydra = |from: &str, sketch: &str, dataset: &str| {
            let mut row = costs.iter().find(|c| c.sketch == from).unwrap().clone();
            row.sketch = sketch.into();
            row.measured_at.dataset = Some(dataset.into());
            row
        };
        let rows = [
            hydra("hll", "hydra-hll", "hydra_flows"),
            hydra("hll", "hydra-univmon-cardinality", "hydra_http"),
            hydra("kll-percall", "hydra-kll", "hydra_http_latency"),
        ];
        costs.extend(rows);
        std::fs::write(
            inputs.join(COST_TABLE),
            serde_json::to_string(&costs).unwrap(),
        )
        .unwrap();
        let w = from_table(path.to_str().unwrap(), None, None, inputs.to_str().unwrap());
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_dir_all(&inputs).unwrap();
        assert!(w.excluded_high_cardinality.is_empty());
        let flows_facts = &w.facts["synthetic/test/flows/src_ip"];
        let full = label_set(&["dst_subnet", "dst_port", "proto"]);
        assert_eq!(flows_facts.cardinality[&full], 1_000_000);
        let hydra: Vec<Deployment> = group_by_stream(&w.raqes)
            .into_iter()
            .flat_map(|(stream, group)| {
                build_all_candidates_unpruned(&group, &w.costs[stream], &w.facts, true)
            })
            .filter(|d| d.properties().answers_any_subgrouping)
            .collect();
        // A grid finer than its RQE is not a roll-up; a per-group sketch there
        // would be.
        let t11 = &w.raqes[0];
        let grid = hydra.iter().find(|d| d.metric == t11.metric).unwrap();
        assert!(!is_rollup(t11, grid));
        let mut per_group = grid.clone();
        per_group.config.sketch = "hll".into();
        assert!(is_rollup(t11, &per_group));
        let grids: BTreeSet<(String, LabelSet)> = hydra
            .into_iter()
            .map(|d| (d.config.sketch, d.grouping_labels))
            .collect();
        assert_eq!(
            grids,
            BTreeSet::from([
                ("hydra-hll".to_string(), full),
                (
                    "hydra-kll".to_string(),
                    label_set(&["region", "service", "endpoint", "status"])
                ),
            ])
        );
    }

    #[test]
    fn hydra_reads_the_dataset_shaped_like_the_metric() {
        let at = |metric| hydra_dataset(metric, Capability::Cardinality);
        assert_eq!(at("http").as_deref(), Some("hydra_http"));
        assert_eq!(at("data_1/http").as_deref(), Some("hydra_http"));
        assert_eq!(at("flows").as_deref(), Some("hydra_flows"));
        assert_eq!(
            hydra_dataset("http", Capability::Quantile).as_deref(),
            Some("hydra_http_latency")
        );
        assert_eq!(at("data"), None);
    }

    /// `r`'s HLL accuracy (ASAP's, AutoSketch's) and the reads at its
    /// smallest and largest covered groups, on curves with `errors` at N =
    /// 1e3 .. 1e7; `r` covers groups of 1% and 50% of a 1000 samples/s
    /// stream (3e3 and 1.5e5 items per 5m window, read at the worse of the
    /// bracketing N). A classic RQE on another
    /// stream reads the mean group alone.
    fn covered_reads(errors: [f64; 5]) -> [Option<f64>; 4] {
        let inputs = hll_saturation_dir(&format!("{errors:?}"));
        let mut curve =
            "family,sketch,config,dist,param,cardinality,n,seed_mean_error,seed_se\n".to_string();
        for lg_k in [12, 14, 16] {
            for (n, error) in ["1e3", "1e4", "1e5", "1e6", "1e7"].iter().zip(errors) {
                curve += &format!("cardinality,hll,lg_k={lg_k},zipf,0.0,1000000,{n},{error},0.0\n");
            }
        }
        let curves = inputs
            .join("out_grid_1e7_cost")
            .join("saturation_curve.csv");
        std::fs::write(curves, curve).unwrap();
        let rqe = |id: &str, stream: &str, extra: Value| {
            let mut r = json!({
                "id": id, "query_id": "q", "kind": "keys", "capability": "cardinality",
                "stream": stream, "lookback_secs": 300, "interval_secs": 60,
                "queries_per_instance": 1,
                "label_set": {"groups": 4, "arrival_rate_per_sec": 1000.0},
                "families": [{"family": "hll", "sketch": "hll", "target": 0.02,
                              "grid_param": 0.0, "grid_K": 1e6}],
            });
            r.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            r
        };
        let table = json!({"workloads": [{
            "dataset": "synthetic/test",
            "schemas": {"http": {"labels": [{"name": "region", "cardinality": 4}]}},
            "rqes": [
                rqe("covered", "http/user_id", json!({
                    "metric": "http", "grouping": ["region"], "covers_share": 0.01,
                    "min_covered_share": 0.01, "max_covered_share": 0.5})),
                rqe("classic", "classic/user_id", json!({})),
            ],
        }]});
        let path = inputs.join("table.json");
        std::fs::write(&path, table.to_string()).unwrap();
        let w = from_table(
            path.to_str().unwrap(),
            None,
            Some("p95".into()),
            inputs.to_str().unwrap(),
        );
        std::fs::remove_dir_all(&inputs).unwrap();
        let [covered, classic] = [&w.raqes[0], &w.raqes[1]];
        let hll = |r: &Raqe| {
            let costs = &w.costs[&r.metric];
            rqe_optimizer::candidates::build_all_candidates_unpruned(
                std::slice::from_ref(r),
                costs,
                &w.facts,
                false,
            )
            .into_iter()
            .find(|d| d.config.sketch == "hll")
            .unwrap()
        };
        // A classic RQE: the mean group's read, as before.
        let d = hll(classic);
        assert!(!w.largest_facts.contains_key(&classic.id));
        assert_eq!(
            w.asap_accuracy(classic, &d),
            w.curves.accuracy(classic, &d, &w.facts)
        );
        assert_eq!(
            w.autosketch_accuracy(classic, &d.config),
            w.curves.autosketch_accuracy(classic, &d.config, &w.facts)
        );
        let d = hll(covered);
        let autosketch = w.autosketch_accuracy(covered, &d.config);
        assert_eq!(autosketch, w.asap_accuracy(covered, &d));
        [
            w.asap_accuracy(covered, &d),
            autosketch,
            w.curves.accuracy(covered, &d, &w.covered_facts["covered"]),
            w.curves.accuracy(covered, &d, &w.largest_facts["covered"]),
        ]
    }

    #[test]
    fn a_covered_rqe_is_held_to_the_worse_of_its_smallest_and_largest_group() {
        // Relative error: a 3% target accepts at most 0.03.
        let at = |target: f64, error: Option<f64>| error.unwrap() <= target;
        // Error grows with N (#189): the largest group rejects what the
        // smallest accepts.
        let [asap, autosketch, smallest, largest] = covered_reads([0.01, 0.01, 0.05, 0.05, 0.05]);
        assert!(at(0.03, smallest) && !at(0.03, largest));
        assert_eq!((asap, autosketch), (largest, largest));
        assert!(!at(0.03, asap));
        // Error falls with N: the smallest group rejects.
        let [asap, autosketch, smallest, largest] = covered_reads([0.05, 0.05, 0.01, 0.01, 0.01]);
        assert!(!at(0.03, smallest) && at(0.03, largest));
        assert_eq!((asap, autosketch), (smallest, smallest));
    }

    #[test]
    fn one_95_percent_level_in_each_familys_metric() {
        assert_eq!(strictness_target("p95", Capability::Sum), 0.0);
        assert_eq!(strictness_target("p95", Capability::RateOrIncrease), 0.0);
        assert_eq!(strictness_target("p95", Capability::Quantile), 0.05);
        assert_eq!(strictness_target("p95", Capability::TopKByValue), 0.95);
    }
}
