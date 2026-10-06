//! Toy instance, solved end to end: builds candidates, brute-forces
//! every feasible mapping, scores each one, then reports the
//! Pareto-optimal subset. Picking one mapping *off* the Pareto front is still
//! the parked scalarization question (problem statement, "Open for later").
//!
//! Cost data is real, not fabricated: `scripts/export_rqe_optimizer_costs.sh`
//! measures it via `sketch-bench`/`aqpbm-cli` and writes
//! `DIR/rqe_atomic_costs.json`, an `AtomicCostTable` loaded here verbatim --
//! no translation layer, since `rqe_optimizer::Deployment` embeds
//! `AtomicCostEntry` directly.
//!
//! Every other number this example uses (RAQE definitions, metric labels,
//! cardinalities and scrape intervals) lives in the two tables below
//! (`facts`, `raqes`) and nowhere else in this file.
//!
//! Run: `scripts/export_rqe_optimizer_costs.sh DIR` once, then pass
//! `--cost-dir DIR` (required) to this example. Do not run this example
//! with no mode flag on a large workload: eager mode retains every feasible
//! mapping and can exhaust memory. Use `--milp`, `--candidates-only`, or
//! `--sample-mappings N` instead. Add
//! `--candidates-only` to inspect candidate pruning safely, without starting
//! mapping enumeration. Streaming mode logs progress every one million
//! mappings by default; pass `--progress-every N` to change that interval or
//! `--print-first N` to display example mappings. `--milp` minimizes
//! `w_cpu · CPU + w_mem · memory GiB` without enumerating mappings; set the
//! weights with `--w-cpu X --w-mem Y` (default 1 and 0). Repeat
//! `--latency-sla RAQE_ID=MS` to impose MILP latency SLAs.
//! `--sample-mappings N` prints N feasible mappings and exits.
//! `--allow-undeployable-families` also plans with measured families
//! ASAPQuery can't deploy (dd, univmon-*, countsketch-heap).

use std::collections::BTreeSet;
use std::time::Instant;

use rqe_optimizer::analytical_cost_model::{score, PlanCost};
use rqe_optimizer::candidates::{
    build_all_candidates, build_all_candidates_unpruned, eligible_deployments_for,
};
use rqe_optimizer::enumerate::{brute_force, for_each_mapping, for_each_mapping_while, unservable};
use rqe_optimizer::milp::{minimize, Objective};
use rqe_optimizer::pareto::{pareto_front, ParetoFront};
use rqe_optimizer::{
    validate_facts, AccuracyDirection, AtomicCostTable, Capability, LabelSet, MetricFacts, Raqe,
    WorkloadFacts,
};

/// Accuracy metric keys the real comparators actually report (checked
/// against `aqpbm-core/src/accuracy/{aggregate,quantile,cardinality}.rs`).
/// These three are lower-is-better errors; each RAQE below pairs its metric with the matching
/// [`AccuracyDirection`] so the comparison can't be applied the wrong way
/// round.
///
/// Request rate is served by an exact counter accumulator, so its error
/// should be zero; the bound is there to reject anything that isn't.
const RATE_ERR: &str = "relative_error";
const RANK_ERR: &str = "mean_rank_err";
const CARDINALITY_ERR: &str = "relative_error";
/// Top-k's metric is the odd one out: a *score*, not an error, so its
/// tolerance below is a floor and its direction is `HigherIsBetter`.
const TOPK_PRECISION: &str = "precision_at_k";

const COST_TABLE_FILE: &str = "rqe_atomic_costs.json";

const REQUESTS: &str = "http_requests_total";
const DURATION: &str = "http_request_duration_seconds";

fn label_set(names: &[&str]) -> LabelSet {
    names.iter().map(|s| s.to_string()).collect()
}

fn load_cost_table() -> AtomicCostTable {
    let args: Vec<_> = std::env::args().collect();
    let cost_dir = args
        .iter()
        .position(|arg| arg == "--cost-dir")
        .map(|index| {
            args.get(index + 1)
                .unwrap_or_else(|| panic!("--cost-dir requires a directory"))
        })
        .unwrap_or_else(|| {
            panic!("--cost-dir DIR is required (a directory written by scripts/export_rqe_optimizer_costs.sh)")
        });
    let path = std::path::Path::new(cost_dir).join(COST_TABLE_FILE);
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "couldn't read {} ({e}) -- run \
             `scripts/export_rqe_optimizer_costs.sh {cost_dir}` first to generate it",
            path.display()
        )
    });
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("{} isn't a valid AtomicCostTable: {e}", path.display()))
}

/// Both metrics: 5 services × 10 endpoints × 600 pods = 30,000 series
/// scraped every 15 s, so `λ` = 2,000 samples/sec each. Durations range from
/// 1 ms to 60 s, which sizes DDSketch.
fn facts() -> WorkloadFacts {
    let http_metric_facts = |value_range| MetricFacts {
        labels: label_set(&["service", "endpoint", "pod"]),
        scrape_interval_ms: 15_000,
        cardinality: [
            (label_set(&["service"]), 5),
            (label_set(&["service", "endpoint"]), 50),
            (label_set(&["service", "endpoint", "pod"]), 30_000),
        ]
        .into(),
        value_range,
    };
    [
        (REQUESTS.to_string(), http_metric_facts(None)),
        (DURATION.to_string(), http_metric_facts(Some((0.001, 60.0)))),
    ]
    .into()
}

fn raqes() -> Vec<Raqe> {
    let se = label_set(&["service", "endpoint"]);
    let s = label_set(&["service"]);
    let requests = || REQUESTS.to_string();
    let duration = || DURATION.to_string();
    vec![
        Raqe {
            id: "req_rate_1h".to_string(),
            capability: Capability::RateOrIncrease,
            lookback_ms: 3_600_000,
            interval_ms: 60_000,
            metric: requests(),
            spatial_filter: String::new(),
            grouping_labels: se.clone(),
            accuracy_metric: RATE_ERR.to_string(),
            accuracy_sla: 0.1,
            accuracy_direction: AccuracyDirection::LowerIsBetter,
            latency_sla_ms: None,
        },
        Raqe {
            id: "req_rate_1d".to_string(),
            capability: Capability::RateOrIncrease,
            lookback_ms: 86_400_000,
            interval_ms: 60_000,
            metric: requests(),
            spatial_filter: String::new(),
            grouping_labels: se.clone(),
            accuracy_metric: RATE_ERR.to_string(),
            accuracy_sla: 0.1,
            accuracy_direction: AccuracyDirection::LowerIsBetter,
            latency_sla_ms: None,
        },
        Raqe {
            id: "req_rate_5m_tick".to_string(),
            capability: Capability::RateOrIncrease,
            lookback_ms: 3_600_000,
            interval_ms: 300_000,
            metric: requests(),
            spatial_filter: String::new(),
            grouping_labels: se.clone(),
            accuracy_metric: RATE_ERR.to_string(),
            accuracy_sla: 0.1,
            accuracy_direction: AccuracyDirection::LowerIsBetter,
            latency_sla_ms: None,
        },
        Raqe {
            id: "latency_p99_1h".to_string(),
            capability: Capability::Quantile,
            lookback_ms: 3_600_000,
            interval_ms: 60_000,
            metric: duration(),
            spatial_filter: String::new(),
            grouping_labels: se.clone(),
            accuracy_metric: RANK_ERR.to_string(),
            accuracy_sla: 0.05,
            accuracy_direction: AccuracyDirection::LowerIsBetter,
            latency_sla_ms: None,
        },
        Raqe {
            id: "latency_p99_6h_tick".to_string(),
            capability: Capability::Quantile,
            lookback_ms: 21_600_000,
            interval_ms: 300_000,
            metric: duration(),
            spatial_filter: String::new(),
            grouping_labels: se.clone(),
            accuracy_metric: RANK_ERR.to_string(),
            accuracy_sla: 0.05,
            accuracy_direction: AccuracyDirection::LowerIsBetter,
            latency_sla_ms: None,
        },
        Raqe {
            id: "latency_p99_1d".to_string(),
            capability: Capability::Quantile,
            lookback_ms: 86_400_000,
            interval_ms: 60_000,
            metric: duration(),
            spatial_filter: String::new(),
            grouping_labels: se.clone(),
            accuracy_metric: RANK_ERR.to_string(),
            accuracy_sla: 0.05,
            accuracy_direction: AccuracyDirection::LowerIsBetter,
            latency_sla_ms: None,
        },
        Raqe {
            id: "distinct_services_1h".to_string(),
            capability: Capability::Cardinality,
            lookback_ms: 3_600_000,
            interval_ms: 60_000,
            metric: requests(),
            spatial_filter: String::new(),
            grouping_labels: s.clone(),
            accuracy_metric: CARDINALITY_ERR.to_string(),
            accuracy_sla: 0.1,
            accuracy_direction: AccuracyDirection::LowerIsBetter,
            latency_sla_ms: None,
        },
        // The higher-is-better case: 0.9 is a *floor* on precision@k, not a
        // ceiling on error. Served by cms-heap, whose measured precision is
        // 1.0 at both configs under the zipf workload (it was 0.0 at both
        // under uniform -- top-k is meaningless without real skew).
        Raqe {
            id: "top_endpoints_1h".to_string(),
            capability: Capability::TopK,
            lookback_ms: 3_600_000,
            interval_ms: 60_000,
            metric: requests(),
            spatial_filter: String::new(),
            grouping_labels: se.clone(),
            accuracy_metric: TOPK_PRECISION.to_string(),
            accuracy_sla: 0.9,
            accuracy_direction: AccuracyDirection::HigherIsBetter,
            latency_sla_ms: None,
        },
    ]
}

fn positive_integer_flag(name: &str, default: u64) -> u64 {
    let args: Vec<_> = std::env::args().collect();
    match args.iter().position(|arg| arg == name) {
        Some(index) => args
            .get(index + 1)
            .unwrap_or_else(|| panic!("{name} requires a positive integer"))
            .parse::<u64>()
            .ok()
            .filter(|&value| value > 0)
            .unwrap_or_else(|| panic!("{name} requires a positive integer")),
        None => default,
    }
}

fn print_mapping(
    mapping_number: u64,
    mapping: &[usize],
    raqes: &[Raqe],
    deployments: &[rqe_optimizer::Deployment],
) {
    println!("mapping {mapping_number}:");
    for (raqe, &deployment_index) in raqes.iter().zip(mapping) {
        let deployment = &deployments[deployment_index];
        println!(
            "  {} -> {} {} (x={}ms, y={}ms)",
            raqe.id,
            deployment.config.sketch,
            deployment.config.sketch_config,
            deployment.window_ms,
            deployment.slide_ms,
        );
    }
}

fn print_candidate(candidate_number: usize, deployment: &rqe_optimizer::Deployment) {
    println!(
        "candidate {candidate_number}: {} {} {} by {:?} (x={}ms, y={}ms)",
        deployment.config.sketch,
        deployment.config.sketch_config,
        deployment.metric,
        deployment.grouping_labels,
        deployment.window_ms,
        deployment.slide_ms,
    );
}

/// Only `--milp` enforces latency SLAs, so every other mode rejects the flag.
/// `--candidates-only` and `--sample-mappings` run before the MILP branch.
fn apply_latency_slas(raqes: &mut [Raqe]) {
    let args: Vec<_> = std::env::args().collect();
    let has = |flag: &str| args.iter().any(|arg| arg == flag);
    assert!(
        !has("--latency-sla")
            || (has("--milp") && !has("--candidates-only") && !has("--sample-mappings")),
        "--latency-sla is only enforced with --milp, without --candidates-only or --sample-mappings"
    );
    for pair in args.windows(2).filter(|pair| pair[0] == "--latency-sla") {
        let (id, ms) = pair[1]
            .split_once('=')
            .unwrap_or_else(|| panic!("--latency-sla expects RAQE_ID=MS"));
        let ms = ms
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or_else(|| {
                panic!("latency SLA must be a finite positive number of milliseconds")
            });
        let raqe = raqes
            .iter_mut()
            .find(|raqe| raqe.id == id)
            .unwrap_or_else(|| panic!("unknown RAQE in --latency-sla: {id}"));
        assert!(
            raqe.latency_sla_ms.replace(ms).is_none(),
            "duplicate latency SLA for {id}"
        );
    }
}

fn weight_flag(name: &str, default: f64) -> f64 {
    let args: Vec<_> = std::env::args().collect();
    match args.iter().position(|arg| arg == name) {
        Some(index) => args
            .get(index + 1)
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| *value >= 0.0)
            .unwrap_or_else(|| panic!("{name} requires a non-negative number")),
        None => default,
    }
}

fn print_plan_cost(label: &str, plan_cost: &PlanCost) {
    println!(
        "{label}: cpu={:.3e} cpu-sec/sec, memory={:.1}MB",
        plan_cost.cpu_secs_per_sec(),
        plan_cost.memory_bytes() / 1e6,
    );
    for (phase, cost) in [
        ("ingest", &plan_cost.ingest),
        ("merge", &plan_cost.merge),
        ("query", &plan_cost.query),
        ("storage", &plan_cost.storage),
    ] {
        println!(
            "    {phase:<7} cpu={:.3e} memory={:.1}MB",
            cost.cpu_secs_per_sec,
            cost.memory_bytes / 1e6,
        );
    }
}

/// Flag parsing above only looks up known names, so a typo would be ignored
/// silently; reject anything unrecognized instead.
fn reject_unknown_args() {
    const SWITCHES: &[&str] = &[
        "--allow-undeployable-families",
        "--candidates-only",
        "--milp",
        "--streaming",
    ];
    const WITH_VALUE: &[&str] = &[
        "--cost-dir",
        "--latency-sla",
        "--print-candidates",
        "--print-first",
        "--progress-every",
        "--sample-mappings",
        "--w-cpu",
        "--w-mem",
    ];
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if WITH_VALUE.contains(&arg.as_str()) {
            args.next();
        } else if !SWITCHES.contains(&arg.as_str()) {
            panic!("unknown argument: {arg}");
        }
    }
}

fn main() {
    reject_unknown_args();
    let candidates_only = std::env::args().any(|arg| arg == "--candidates-only");
    let mut raqes = raqes();
    apply_latency_slas(&mut raqes);
    let cost_table = load_cost_table();
    let facts = facts();
    if let Err(problems) = validate_facts(&raqes, &facts) {
        panic!("invalid workload facts: {problems:#?}");
    }

    let allow_undeployable = std::env::args().any(|arg| arg == "--allow-undeployable-families");
    let unpruned_count = candidates_only.then(|| {
        build_all_candidates_unpruned(&raqes, &cost_table, &facts, allow_undeployable).len()
    });
    let deployments = build_all_candidates(&raqes, &cost_table, &facts, allow_undeployable);
    println!(
        "{} RAQEs, {} candidate deployments (from {} real cost-table rows)",
        raqes.len(),
        deployments.len(),
        cost_table.len(),
    );

    if let Some(unpruned_count) = unpruned_count {
        println!(
            "candidate dominance: {unpruned_count} generated -> {} retained ({} removed)",
            deployments.len(),
            unpruned_count - deployments.len(),
        );
        let mut possible_mappings = Some(1_u64);
        for raqe in &raqes {
            let eligible = eligible_deployments_for(raqe, &deployments);
            println!("  {}: {} eligible deployments", raqe.id, eligible.len());
            possible_mappings = possible_mappings.and_then(|count| {
                count.checked_mul(eligible.len().try_into().expect("usize fits in u64"))
            });
        }
        match possible_mappings {
            Some(count) => println!("Cartesian mapping space: {count}"),
            None => println!("Cartesian mapping space: exceeds u64"),
        }
        let print_candidates = positive_integer_flag("--print-candidates", 0);
        for (index, deployment) in deployments
            .iter()
            .take(usize::try_from(print_candidates).expect("u64 fits in usize"))
            .enumerate()
        {
            print_candidate(index, deployment);
        }
        return;
    }

    let missing = unservable(&raqes, &deployments);
    if !missing.is_empty() {
        println!("unservable (no eligible deployment): {missing:?}");
        return;
    }

    let sample_mappings = positive_integer_flag("--sample-mappings", 0);
    if sample_mappings > 0 {
        let mut printed = 0;
        let result = for_each_mapping_while(&raqes, &deployments, |mapping| {
            printed += 1;
            print_mapping(printed, mapping, &raqes, &deployments);
            printed < sample_mappings
        });
        println!(
            "printed {} feasible mapping sample(s); enumeration {}",
            result.visited,
            if result.completed {
                "completed"
            } else {
                "stopped early"
            },
        );
        return;
    }

    if std::env::args().any(|arg| arg == "--milp") {
        let objective = Objective::AUCCost {
            w_cpu: weight_flag("--w-cpu", 1.0),
            w_mem: weight_flag("--w-mem", 0.0),
        };
        let solution = minimize(&raqes, &deployments, &facts, objective)
            .expect("small_problem MILP should be feasible");
        println!(
            "MILP solution for {objective:?}: {:.3e}",
            objective.value(&solution.plan_cost)
        );
        print_plan_cost("  totals", &solution.plan_cost);
        print_mapping(1, &solution.mapping, &raqes, &deployments);
        for (raqe, latency) in raqes.iter().zip(&solution.plan_cost.query_latency_ms) {
            println!("  {}: query_latency={latency:.3e} ms", raqe.id);
        }
        return;
    }

    if std::env::args().any(|arg| arg == "--streaming") {
        let mut front = ParetoFront::new();
        let progress_every = positive_integer_flag("--progress-every", 1_000_000);
        let print_first = positive_integer_flag("--print-first", 0);
        let started = Instant::now();
        let mut processed = 0_u64;
        let mapping_count = for_each_mapping(&raqes, &deployments, |mapping| {
            front.consider(mapping, score(&raqes, &deployments, mapping, &facts));
            processed += 1;
            if processed <= print_first {
                print_mapping(processed, mapping, &raqes, &deployments);
            }
            if processed.is_multiple_of(progress_every) {
                let elapsed_secs = started.elapsed().as_secs_f64();
                eprintln!(
                    "progress: {processed} mappings in {elapsed_secs:.1}s ({:.0}/sec), {} on frontier",
                    processed as f64 / elapsed_secs.max(f64::MIN_POSITIVE),
                    front.entries().len(),
                );
            }
        });
        println!(
            "streamed {mapping_count} feasible mappings in {:.1}s; {} remain on the Pareto front",
            started.elapsed().as_secs_f64(),
            front.entries().len(),
        );
        return;
    }

    eprintln!(
        "WARNING: eager mode will retain every feasible mapping. For larger workloads, use --milp, \
         --candidates-only, or --sample-mappings N instead."
    );
    let mappings = brute_force(&raqes, &deployments);
    println!("{} feasible full mappings", mappings.len());

    let plan_costs: Vec<_> = mappings
        .iter()
        .map(|m| score(&raqes, &deployments, m, &facts))
        .collect();
    let front = pareto_front(&plan_costs);
    println!("{} on the Pareto front\n", front.len());

    let mut front = front;
    front.sort_unstable();
    for &i in &front {
        let plan_cost = &plan_costs[i];
        let distinct_deployments: BTreeSet<usize> = mappings[i].iter().copied().collect();
        print_plan_cost(
            &format!("mapping {i}: {} deployments", distinct_deployments.len()),
            plan_cost,
        );
        for (raqe, latency) in raqes.iter().zip(&plan_cost.query_latency_ms) {
            let raqe_id = &raqe.id;
            println!("    {raqe_id}: query_latency={latency:.3e} ms");
        }
    }
}
