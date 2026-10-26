//! How the optimal deployment moves as the query workload changes: query
//! types, groupings, lookbacks, repetition intervals, and accuracy and
//! latency SLAs. Each sweep steps one knob of a base workload; every step is
//! one workload solved with the MILP and printed as the deployments it runs
//! and the queries each one serves.
//!
//! A deployment serves only queries with its own capability and metric, and
//! its own grouping or, for a family that merges across groups, a subset of
//! it (a roll-up; `candidates::is_eligible`). So the mixed-type sweeps cost
//! the same as solving each part alone, while the mixed-grouping sweeps can
//! share one fine deployment across groupings.
//!
//! Run:
//! `cargo run --release -p rqe-optimizer --example workload_scenarios --
//!  --saturation-dir DIR [--w-cpu X] [--w-mem Y]
//!  [--allow-undeployable-families] [--sweep NAME]`

use rqe_optimizer::candidates::build_all_candidates;
use rqe_optimizer::enumerate::unservable;
use rqe_optimizer::milp::{minimize, Objective};
use rqe_optimizer::saturation::{DataShape, SaturationCurves, COST_TABLE};
use rqe_optimizer::{
    accuracy_key, validate_facts, AtomicCostTable, Capability, Deployment, LabelSet, MetricFacts,
    Raqe, WorkloadFacts,
};

const MINUTE_MS: u64 = 60_000;
const HOUR_MS: u64 = 60 * MINUTE_MS;
const DAY_MS: u64 = 24 * HOUR_MS;

const REQUESTS: &str = "http_requests_total";
const DURATION: &str = "http_request_duration_seconds";
/// The lookback × interval sweep's queries each read their own metric, so
/// none of them share a deployment.
const PER_SERVICE_METRIC_COUNT: usize = 10;

fn per_service_metric(service_index: usize) -> String {
    format!("service_{service_index}_request_duration_seconds")
}

fn label_set(names: &[&str]) -> LabelSet {
    names.iter().map(|name| name.to_string()).collect()
}

fn by_service() -> LabelSet {
    label_set(&["service"])
}

fn by_service_endpoint() -> LabelSet {
    label_set(&["service", "endpoint"])
}

/// One query, with a default accuracy SLA for its capability. Each candidate
/// checks the SLA against its own family's accuracy metric
/// (`rqe_optimizer::accuracy_key`). Sweeps adjust the returned value.
fn query(
    id: &str,
    capability: Capability,
    metric: &str,
    grouping_labels: LabelSet,
    lookback_ms: u64,
    interval_ms: u64,
) -> Raqe {
    let accuracy_sla = match capability {
        Capability::Quantile => 0.05,
        Capability::TopKByValue => 0.9,
        _ => 0.1,
    };
    Raqe {
        id: id.to_string(),
        capability,
        lookback_ms,
        interval_ms,
        metric: metric.to_string(),
        spatial_filter: String::new(),
        grouping_labels,
        accuracy_sla,
        latency_sla_ms: None,
        topk_k: None,
        accuracy_covers_share: None,
    }
}

fn p99(id: &str, lookback_ms: u64) -> Raqe {
    query(
        id,
        Capability::Quantile,
        DURATION,
        by_service_endpoint(),
        lookback_ms,
        MINUTE_MS,
    )
}

fn distinct_endpoints(id: &str, lookback_ms: u64) -> Raqe {
    query(
        id,
        Capability::Cardinality,
        REQUESTS,
        by_service(),
        lookback_ms,
        MINUTE_MS,
    )
}

/// p99 latency over three lookbacks of one metric: candidates for sharing
/// one deployment.
fn shared_p99_workload() -> Vec<Raqe> {
    vec![
        p99("p99_1h", HOUR_MS),
        p99("p99_6h", 6 * HOUR_MS),
        p99("p99_1d", DAY_MS),
    ]
}

/// Distinct endpoints per service over three lookbacks of one metric.
fn shared_distinct_workload() -> Vec<Raqe> {
    vec![
        distinct_endpoints("distinct_1h", HOUR_MS),
        distinct_endpoints("distinct_6h", 6 * HOUR_MS),
        distinct_endpoints("distinct_1d", DAY_MS),
    ]
}

struct Scenario {
    label: String,
    raqes: Vec<Raqe>,
}

struct Sweep {
    name: &'static str,
    description: &'static str,
    scenarios: Vec<Scenario>,
}

fn lookback_interval_sweep() -> Sweep {
    let mut scenarios = Vec::new();
    for lookback_ms in [5 * MINUTE_MS, HOUR_MS, 6 * HOUR_MS, DAY_MS, 7 * DAY_MS] {
        for interval_ms in [15_000, MINUTE_MS, 5 * MINUTE_MS] {
            let raqes = (0..PER_SERVICE_METRIC_COUNT)
                .map(|service_index| {
                    query(
                        &format!("p99_service_{service_index}"),
                        Capability::Quantile,
                        &per_service_metric(service_index),
                        by_service_endpoint(),
                        lookback_ms,
                        interval_ms,
                    )
                })
                .collect();
            scenarios.push(Scenario {
                label: format!(
                    "lookback {} interval {}",
                    duration_label(lookback_ms),
                    duration_label(interval_ms)
                ),
                raqes,
            });
        }
    }
    Sweep {
        name: "lookback-interval",
        description: "10 p99 queries on distinct metrics, all with one lookback and interval",
        scenarios,
    }
}

fn mixed_grouping_sweep() -> Sweep {
    let p99_by = |grouping_labels: LabelSet, id: &str| {
        query(
            id,
            Capability::Quantile,
            DURATION,
            grouping_labels,
            HOUR_MS,
            MINUTE_MS,
        )
    };
    let by_service_query = || p99_by(by_service(), "p99_1h_by_service");
    let by_service_endpoint_query = || p99_by(by_service_endpoint(), "p99_1h_by_endpoint");
    Sweep {
        name: "mixed-grouping",
        description: "1h p99 by (service), by (service, endpoint), and both together",
        scenarios: vec![
            Scenario {
                label: "by service".to_string(),
                raqes: vec![by_service_query()],
            },
            Scenario {
                label: "by service, endpoint".to_string(),
                raqes: vec![by_service_endpoint_query()],
            },
            Scenario {
                label: "both".to_string(),
                raqes: vec![by_service_query(), by_service_endpoint_query()],
            },
        ],
    }
}

fn mixed_type_sweep() -> Sweep {
    let rate = || {
        query(
            "rate_1h",
            Capability::RateOrIncrease,
            REQUESTS,
            by_service_endpoint(),
            HOUR_MS,
            MINUTE_MS,
        )
    };
    let quantile = || p99("p99_1h", HOUR_MS);
    let distinct = || distinct_endpoints("distinct_1h", HOUR_MS);
    let top_endpoints = || {
        query(
            "topk_1h",
            Capability::TopKByValue,
            REQUESTS,
            by_service_endpoint(),
            HOUR_MS,
            MINUTE_MS,
        )
    };
    let alone = |raqe: Raqe| Scenario {
        label: format!("{} alone", raqe.id),
        raqes: vec![raqe],
    };
    Sweep {
        name: "mixed-type",
        description: "1h rate, p99, distinct count and top-k, alone and together",
        scenarios: vec![
            alone(rate()),
            alone(quantile()),
            alone(distinct()),
            alone(top_endpoints()),
            Scenario {
                label: "rate + p99 + distinct".to_string(),
                raqes: vec![rate(), quantile(), distinct()],
            },
            Scenario {
                label: "all four".to_string(),
                raqes: vec![rate(), quantile(), distinct(), top_endpoints()],
            },
        ],
    }
}

fn latency_sla_sweep() -> Sweep {
    let scenarios = [None, Some(5.0), Some(2.0), Some(1.0), Some(0.5), Some(0.2)]
        .into_iter()
        .map(|latency_sla_ms| {
            let mut raqes = shared_p99_workload();
            raqes[0].latency_sla_ms = latency_sla_ms;
            Scenario {
                label: match latency_sla_ms {
                    Some(limit) => format!("p99_1h latency <= {limit} ms"),
                    None => "no latency SLA".to_string(),
                },
                raqes,
            }
        })
        .collect();
    Sweep {
        name: "latency-sla",
        description: "p99 over 1h/6h/1d of one metric; tighten the latency SLA of p99_1h",
        scenarios,
    }
}

fn accuracy_sla_sweep() -> Sweep {
    let tightened = |mut raqes: Vec<Raqe>, accuracy_sla: f64| {
        raqes[0].accuracy_sla = accuracy_sla;
        Scenario {
            label: format!("{} accuracy SLA {accuracy_sla}", raqes[0].id),
            raqes,
        }
    };
    let quantile_scenarios = [0.05, 0.01, 0.005, 0.002, 0.001]
        .into_iter()
        .map(|accuracy_sla| tightened(shared_p99_workload(), accuracy_sla));
    let distinct_scenarios = [0.1, 0.05, 0.02, 0.01]
        .into_iter()
        .map(|accuracy_sla| tightened(shared_distinct_workload(), accuracy_sla));
    Sweep {
        name: "accuracy-sla",
        description: "p99 and distinct count over 1h/6h/1d of one metric; tighten the 1h query's accuracy SLA",
        scenarios: quantile_scenarios.chain(distinct_scenarios).collect(),
    }
}

/// Every metric: 5 services × 10 endpoints × 600 pods = 30,000 series scraped
/// every 15 s, as in `small_problem`. Durations range from 1 ms to 60 s.
fn facts() -> WorkloadFacts {
    let metric_facts = |value_range| MetricFacts {
        labels: label_set(&["service", "endpoint", "pod"]),
        scrape_interval_ms: 15_000,
        cardinality: [
            (by_service(), 5),
            (by_service_endpoint(), 50),
            (label_set(&["service", "endpoint", "pod"]), 30_000),
        ]
        .into(),
        value_range,
        data_shape: [
            (by_service(), data_shape(10_000.0)),
            (by_service_endpoint(), data_shape(1_000.0)),
        ]
        .into(),
        hydra_dataset: None,
    };
    let duration_range = Some((0.001, 60.0));
    let per_service_metrics = (0..PER_SERVICE_METRIC_COUNT).map(|service_index| {
        (
            per_service_metric(service_index),
            metric_facts(duration_range),
        )
    });
    [
        (REQUESTS.to_string(), metric_facts(None)),
        (DURATION.to_string(), metric_facts(duration_range)),
    ]
    .into_iter()
    .chain(per_service_metrics)
    .collect()
}

fn data_shape(distinct_keys: f64) -> DataShape {
    DataShape {
        zipf_s: 1.1,
        distinct_keys,
        tail_index: 1.5,
    }
}

/// The flag's value, or `None` when the flag is absent.
fn flag_value(flag: &str) -> Option<String> {
    let args: Vec<_> = std::env::args().collect();
    let index = args.iter().position(|arg| arg == flag)?;
    Some(
        args.get(index + 1)
            .unwrap_or_else(|| panic!("{flag} requires a value"))
            .clone(),
    )
}

fn weight_flag(flag: &str, default: f64) -> f64 {
    flag_value(flag).map_or(default, |value| {
        value
            .parse::<f64>()
            .ok()
            .filter(|weight| *weight >= 0.0)
            .unwrap_or_else(|| panic!("{flag} requires a non-negative number"))
    })
}

fn reject_unknown_args() {
    const SWITCHES: &[&str] = &["--allow-undeployable-families"];
    const WITH_VALUE: &[&str] = &["--saturation-dir", "--sweep", "--w-cpu", "--w-mem"];
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if WITH_VALUE.contains(&arg.as_str()) {
            args.next();
        } else if !SWITCHES.contains(&arg.as_str()) {
            panic!("unknown argument: {arg}");
        }
    }
}

fn saturation_dir() -> String {
    flag_value("--saturation-dir").expect("--saturation-dir DIR is required")
}

fn load_cost_table() -> AtomicCostTable {
    let path = std::path::Path::new(&saturation_dir()).join(COST_TABLE);
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("couldn't read {} ({e})", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("{} isn't a valid AtomicCostTable: {e}", path.display()))
}

fn load_saturation_curves(cost_table: &AtomicCostTable) -> SaturationCurves {
    let curves = SaturationCurves::load(std::path::Path::new(&saturation_dir()))
        .unwrap_or_else(|e| panic!("couldn't load saturation curves: {e}"));
    // ponytail: warn rather than panic, since a config with no curve is never
    // eligible; panic like small_problem once the cost export matches the grid.
    let check = curves.check_cost_table(cost_table);
    for row in check.unchecked.iter().chain(&check.mismatched) {
        eprintln!("cost table vs. saturation curves: {row}");
    }
    curves
}

fn duration_label(ms: u64) -> String {
    match ms {
        _ if ms.is_multiple_of(DAY_MS) => format!("{}d", ms / DAY_MS),
        _ if ms.is_multiple_of(HOUR_MS) => format!("{}h", ms / HOUR_MS),
        _ if ms.is_multiple_of(MINUTE_MS) => format!("{}m", ms / MINUTE_MS),
        _ => format!("{}s", ms / 1_000),
    }
}

fn describe(deployment: &Deployment) -> String {
    format!(
        "{} {} by {:?} x={} y={}",
        deployment.config.sketch,
        deployment.config.sketch_config,
        deployment.grouping_labels,
        duration_label(deployment.window_ms),
        duration_label(deployment.slide_ms),
    )
}

fn solve_and_print(
    scenario: &Scenario,
    cost_table: &AtomicCostTable,
    curves: &SaturationCurves,
    facts: &WorkloadFacts,
    objective: Objective,
    allow_undeployable: bool,
) {
    let raqes = &scenario.raqes;
    if let Err(problems) = validate_facts(raqes, facts) {
        panic!("invalid workload facts: {problems:#?}");
    }
    let accuracy = |raqe: &Raqe, deployment: &Deployment| curves.accuracy(raqe, deployment, facts);
    let deployments = build_all_candidates(raqes, cost_table, facts, allow_undeployable, &accuracy);
    let missing = unservable(raqes, &deployments, facts, &accuracy);
    if !missing.is_empty() {
        println!("[{}] unservable: {missing:?}", scenario.label);
        return;
    }
    let solution = match minimize(raqes, &deployments, facts, objective, &accuracy) {
        Ok(solution) => solution,
        Err(error) => {
            println!("[{}] infeasible: {error}", scenario.label);
            return;
        }
    };
    let plan_cost = &solution.plan_cost;
    println!(
        "[{}] objective={:.3e} cpu={:.3e} cpu-sec/sec memory={:.1}MB deployments={}",
        scenario.label,
        objective.value(plan_cost),
        plan_cost.cpu_secs_per_sec(),
        plan_cost.memory_bytes() / 1e6,
        solution.deployments.len(),
    );
    for (deployment_index, planned) in solution.deployments.iter().enumerate() {
        println!("  D{deployment_index}: {}", describe(&planned.deployment));
        for ((raqe, planned_raqe), latency_ms) in raqes
            .iter()
            .zip(&solution.raqes)
            .zip(&plan_cost.query_latency_ms)
            .filter(|((_, planned_raqe), _)| planned_raqe.deployment == deployment_index)
        {
            let (accuracy_metric, _) = accuracy_key(&planned.deployment.config.sketch);
            let achieved = accuracy(raqe, &planned.deployment)
                .map_or("unknown".to_string(), |value| format!("{value:.4}"));
            println!(
                "      {:<22} merged={:<4} latency={latency_ms:.3e} ms {}={achieved} (sla {})",
                raqe.id, planned_raqe.merged_instance_count, accuracy_metric, raqe.accuracy_sla,
            );
        }
    }
}

fn main() {
    reject_unknown_args();
    let cost_table = load_cost_table();
    let curves = load_saturation_curves(&cost_table);
    let facts = facts();
    let objective = Objective::AUCCost {
        w_cpu: weight_flag("--w-cpu", 1.0),
        w_mem: weight_flag("--w-mem", 0.0),
    };
    let allow_undeployable = std::env::args().any(|arg| arg == "--allow-undeployable-families");
    let sweeps = [
        lookback_interval_sweep(),
        mixed_grouping_sweep(),
        mixed_type_sweep(),
        latency_sla_sweep(),
        accuracy_sla_sweep(),
    ];
    let selected = flag_value("--sweep");
    if let Some(name) = &selected {
        assert!(
            sweeps.iter().any(|sweep| sweep.name == name),
            "unknown --sweep {name}; expected one of {:?}",
            sweeps.iter().map(|sweep| sweep.name).collect::<Vec<_>>()
        );
    }
    println!("{objective:?}");
    for sweep in sweeps
        .iter()
        .filter(|sweep| selected.as_ref().is_none_or(|name| sweep.name == name))
    {
        println!("\n=== {}: {} ===", sweep.name, sweep.description);
        for scenario in &sweep.scenarios {
            solve_and_print(
                scenario,
                &cost_table,
                &curves,
                &facts,
                objective,
                allow_undeployable,
            );
        }
    }
}
