//! Analytical scoring built from measured per-operation costs (§5).

use crate::{Deployment, LabelSetTable, Mapping, Rqe};
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub struct Objectives {
    pub peak_query_memory_bytes: f64,
    pub ingest_cpu_secs_per_sec: f64,
    pub merge_cpu_secs_per_sec: f64,
    pub query_cpu_secs_per_sec: f64,
    pub tco_cpu_secs_per_sec: f64,
    /// Index-aligned with the input RQE slice; display IDs need not be unique.
    pub query_latency_secs: Vec<f64>,
}

pub fn score(
    rqes: &[Rqe],
    deployments: &[Deployment],
    mapping: &Mapping,
    label_sets: &LabelSetTable,
) -> Objectives {
    assert_eq!(
        mapping.len(),
        rqes.len(),
        "mapping must have one deployment per RQE"
    );
    let active: BTreeSet<usize> = mapping.iter().copied().collect();
    let ingest_cpu_secs_per_sec = active
        .iter()
        .map(|&di| {
            let d = &deployments[di];
            let fanout =
                d.active_instance_count()
                    .expect("eligible deployment aligns window and slide") as f64;
            label_sets[&d.labels].arrival_rate_per_sec * fanout * d.config.insert_cpu_secs
        })
        .sum();

    let mut peak_query_memory_bytes = 0.0_f64;
    let mut merge_cpu_secs_per_sec = 0.0;
    let mut query_cpu_secs_per_sec = 0.0;
    let query_latency_secs = rqes
        .iter()
        .zip(mapping)
        .map(|(r, &di)| {
            let d = &deployments[di];
            let card = label_sets[&d.labels].cardinality as f64;
            peak_query_memory_bytes =
                peak_query_memory_bytes.max(card * d.config.mem_bytes_per_instance);
            let instances = d
                .query_instance_count(r.lookback_secs)
                .expect("mapping only contains eligible pairs");
            let query = card * d.config.query_cpu_secs;
            let merge = card * instances.saturating_sub(1) as f64 * d.config.merge_cpu_secs;
            query_cpu_secs_per_sec += query / r.interval_secs as f64;
            merge_cpu_secs_per_sec += merge / r.interval_secs as f64;
            query + merge
        })
        .collect();
    let tco_cpu_secs_per_sec =
        ingest_cpu_secs_per_sec + merge_cpu_secs_per_sec + query_cpu_secs_per_sec;
    Objectives {
        peak_query_memory_bytes,
        ingest_cpu_secs_per_sec,
        merge_cpu_secs_per_sec,
        query_cpu_secs_per_sec,
        tco_cpu_secs_per_sec,
        query_latency_secs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AccuracyDirection, AtomicCostEntry, Capability, LabelSet, LabelSetInfo};
    use std::collections::BTreeMap;

    #[test]
    fn scores_overlapping_ingest_and_non_overlapping_query_merges() {
        let labels = LabelSet::new();
        let config = AtomicCostEntry {
            sketch: "cms-fastpath-vector2d".into(),
            sketch_config: serde_json::json!(null),
            mem_bytes_per_instance: 10.0,
            insert_cpu_secs: 2.0,
            merge_cpu_secs: 3.0,
            query_cpu_secs: 5.0,
            query_accuracy: BTreeMap::from([("err".into(), 0.0)]),
        };
        let deployment = Deployment {
            capability: Capability::Freq,
            labels: labels.clone(),
            config,
            window_secs: 20,
            slide_secs: 10,
        };
        let rqe = Rqe {
            id: "r".into(),
            capability: Capability::Freq,
            lookback_secs: 60,
            interval_secs: 30,
            labels: labels.clone(),
            accuracy_metric: "err".into(),
            accuracy_tolerance: 1.0,
            accuracy_direction: AccuracyDirection::LowerIsBetter,
        };
        let tables = BTreeMap::from([(
            labels,
            LabelSetInfo {
                cardinality: 4,
                arrival_rate_per_sec: 7.0,
            },
        )]);
        let result = score(&[rqe], &[deployment], &vec![0], &tables);
        assert_eq!(result.peak_query_memory_bytes, 40.0);
        assert_eq!(result.ingest_cpu_secs_per_sec, 28.0); // 7 items × 2 active × 2 CPU
        assert_eq!(result.query_latency_secs, vec![44.0]); // 4 × (5 query + 2 merges × 3)
        assert_eq!(result.query_cpu_secs_per_sec, 20.0 / 30.0);
        assert_eq!(result.merge_cpu_secs_per_sec, 24.0 / 30.0);
    }
}
