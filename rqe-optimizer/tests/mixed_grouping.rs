//! A workload asking the same question at two groupings, `(service,
//! endpoint)` and `(service)`, solved end to end. A family whose states merge
//! across groups serves both from one fine deployment; one whose don't keeps a
//! deployment per grouping (sketch-bench#187).

use std::collections::BTreeMap;

use aqpbm_core::MeasuredAt;
use rqe_optimizer::candidates::build_all_candidates;
use rqe_optimizer::milp::{minimize, MilpSolution, Objective};
use rqe_optimizer::{
    accuracy_key, validate_facts, AccuracyDirection, AtomicCostEntry, Capability, Deployment,
    LabelSet, MetricFacts, Raqe, WorkloadFacts,
};

const METRIC: &str = "http_request_duration_seconds";
const HOUR_MS: u64 = 3_600_000;
const MINUTE_MS: u64 = 60_000;

fn labels(names: &[&str]) -> LabelSet {
    names.iter().map(|name| name.to_string()).collect()
}

/// 5 services × 10 endpoints × 600 pods = 30,000 series, scraped every 15 s.
fn facts() -> WorkloadFacts {
    WorkloadFacts::from([(
        METRIC.to_string(),
        MetricFacts {
            labels: labels(&["service", "endpoint", "pod"]),
            scrape_interval_ms: 15_000,
            cardinality: BTreeMap::from([
                (labels(&["service"]), 5),
                (labels(&["service", "endpoint"]), 50),
                (labels(&["service", "endpoint", "pod"]), 30_000),
            ]),
            value_range: None,
            data_shape: BTreeMap::new(),
            hydra_dataset: None,
        },
    )])
}

fn raqe(id: &str, capability: Capability, grouping: &[&str], accuracy_sla: f64) -> Raqe {
    Raqe {
        id: id.into(),
        capability,
        lookback_ms: HOUR_MS,
        interval_ms: MINUTE_MS,
        metric: METRIC.into(),
        spatial_filter: String::new(),
        grouping_labels: labels(grouping),
        accuracy_sla,
        latency_sla_ms: None,
        topk_k: None,
        accuracy_covers_share: None,
    }
}

fn raqes() -> Vec<Raqe> {
    vec![
        raqe(
            "p99_by_endpoint",
            Capability::Quantile,
            &["service", "endpoint"],
            0.05,
        ),
        raqe("p99_by_service", Capability::Quantile, &["service"], 0.05),
        raqe(
            "top_by_endpoint",
            Capability::TopKByValue,
            &["service", "endpoint"],
            0.9,
        ),
        raqe("top_by_service", Capability::TopKByValue, &["service"], 0.9),
    ]
}

/// Ingest dominates merge and query, as it does in the measured table, so
/// sharing one deployment is cheaper than keeping two whenever it is allowed.
fn cost_row(sketch: &str, params: serde_json::Value) -> AtomicCostEntry {
    let (accuracy_metric, _) = accuracy_key(sketch);
    AtomicCostEntry {
        sketch: sketch.into(),
        sketch_config: serde_json::json!({ "params": params }),
        mem_bytes_per_instance: 1_000.0,
        insert_cpu_secs: 1e-6,
        merge_cpu_secs: 1e-6,
        query_cpu_secs: 1e-6,
        query_accuracy: BTreeMap::from([(accuracy_metric.to_string(), 0.0)]),
        accuracy_metric: accuracy_metric.into(),
        measured_at: MeasuredAt {
            items_per_instance: 1_000_000,
            keys_per_instance: Some(10_000),
            value_range: None,
            merge_operand_items: None,
            distribution: None,
            dataset: None,
            schema_width: None,
        },
    }
}

fn cost_table() -> Vec<AtomicCostEntry> {
    vec![
        cost_row("kll-percall", serde_json::json!({ "k": 200 })),
        // A heap large enough for any merge, so heap size never decides the plan.
        cost_row(
            "cms-heap-topk-fastpath-vector2d",
            serde_json::json!({ "rows": 3, "cols": 1024, "heap": 1u64 << 40 }),
        ),
    ]
}

/// Every candidate meets every SLA, so the plan is decided by eligibility and
/// cost alone.
fn perfect_accuracy(_: &Raqe, deployment: &Deployment) -> Option<f64> {
    match accuracy_key(&deployment.config.sketch).1 {
        AccuracyDirection::LowerIsBetter => Some(0.0),
        AccuracyDirection::HigherIsBetter => Some(1.0),
    }
}

fn solve(raqes: &[Raqe]) -> MilpSolution {
    let facts = facts();
    validate_facts(raqes, &facts).expect("valid workload facts");
    let candidates = build_all_candidates(raqes, &cost_table(), &facts, false, &perfect_accuracy);
    minimize(
        raqes,
        &candidates,
        &facts,
        Objective::default(),
        &perfect_accuracy,
    )
    .expect("feasible plan")
}

/// The planned deployment serving the RAQE named `id`.
fn deployment_serving<'a>(solution: &'a MilpSolution, raqes: &[Raqe], id: &str) -> &'a Deployment {
    let index = raqes.iter().position(|raqe| raqe.id == id).unwrap();
    &solution.deployments[solution.raqes[index].deployment].deployment
}

#[test]
fn a_fine_kll_deployment_serves_both_quantile_raqes() {
    let raqes = raqes();
    let solution = solve(&raqes);
    let by_endpoint = deployment_serving(&solution, &raqes, "p99_by_endpoint");
    let by_service = deployment_serving(&solution, &raqes, "p99_by_service");
    assert_eq!(
        by_endpoint, by_service,
        "one KLL deployment should serve both groupings"
    );
    assert_eq!(
        by_endpoint.grouping_labels,
        labels(&["service", "endpoint"])
    );
}

#[test]
fn top_k_never_rolls_up() {
    let raqes = raqes();
    let solution = solve(&raqes);
    let by_endpoint = deployment_serving(&solution, &raqes, "top_by_endpoint");
    let by_service = deployment_serving(&solution, &raqes, "top_by_service");
    assert_eq!(
        by_endpoint.grouping_labels,
        labels(&["service", "endpoint"])
    );
    assert_eq!(by_service.grouping_labels, labels(&["service"]));
}
