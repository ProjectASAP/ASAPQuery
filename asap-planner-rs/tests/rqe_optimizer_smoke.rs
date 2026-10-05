//! Links sketch-bench's rqe-optimizer and solves a one-query MILP with HiGHS.

use std::collections::BTreeMap;

use rqe_optimizer::milp::{minimize_cost, MilpBounds};
use rqe_optimizer::objectives::MachineFamily;
use rqe_optimizer::{
    AccuracyDirection, AtomicCostEntry, Capability, Deployment, LabelSet, LabelSetInfo, Rqe,
};

fn deployment(insert_cpu_secs: f64) -> Deployment {
    Deployment {
        capability: Capability::Quantile,
        labels: LabelSet::new(),
        config: AtomicCostEntry {
            sketch: "kll-percall".into(),
            sketch_config: serde_json::json!(null),
            mem_bytes_per_instance: 1024.0,
            insert_cpu_secs,
            merge_cpu_secs: 1.0,
            query_cpu_secs: 1.0,
            query_accuracy: BTreeMap::from([("err".into(), 0.0)]),
        },
        window_secs: 60,
        slide_secs: 60,
    }
}

#[test]
fn minimize_cost_picks_the_cheaper_deployment() {
    let rqes = vec![Rqe {
        id: "q".into(),
        capability: Capability::Quantile,
        lookback_secs: 60,
        interval_secs: 60,
        labels: LabelSet::new(),
        accuracy_metric: "err".into(),
        accuracy_tolerance: 1.0,
        accuracy_direction: AccuracyDirection::LowerIsBetter,
    }];
    let deployments = vec![deployment(2.0), deployment(1.0)];
    let label_sets = BTreeMap::from([(
        LabelSet::new(),
        LabelSetInfo {
            cardinality: 1,
            arrival_rate_per_sec: 1.0,
        },
    )]);
    let family = MachineFamily {
        family: "test".into(),
        vcpu: 4.0,
        memory_gib: 16.0,
        usd_per_hour: 1.0,
    };

    let solution = minimize_cost(
        &rqes,
        &deployments,
        &label_sets,
        &MilpBounds::default(),
        &family,
    )
    .expect("feasible MILP");

    assert_eq!(solution.mapping, vec![1]);
}
