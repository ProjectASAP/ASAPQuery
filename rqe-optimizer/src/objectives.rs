//! Analytical scoring built from measured per-operation costs (§5).

use crate::{Deployment, LabelSetTable, Mapping, Rqe};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const BYTES_PER_GIB: f64 = (1u64 << 30) as f64;

#[derive(Debug, Clone)]
pub struct Objectives {
    pub peak_query_memory_bytes: f64,
    pub ingest_cpu_secs_per_sec: f64,
    pub merge_cpu_secs_per_sec: f64,
    pub query_cpu_secs_per_sec: f64,
    pub tco_cpu_secs_per_sec: f64,
    /// State held by all active deployments: each keeps the instances needed
    /// by the longest lookback it serves.
    pub retained_memory_bytes: f64,
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

    let mut retained_instances: BTreeMap<usize, u64> = BTreeMap::new();
    for (r, &di) in rqes.iter().zip(mapping) {
        let count = deployments[di]
            .retained_instance_count(r.lookback_secs)
            .expect("mapping only contains eligible pairs");
        let held = retained_instances.entry(di).or_default();
        *held = (*held).max(count);
    }
    let retained_memory_bytes = retained_instances
        .iter()
        .map(|(&di, &count)| {
            let d = &deployments[di];
            label_sets[&d.labels].cardinality as f64
                * d.config.mem_bytes_per_instance
                * count as f64
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
        retained_memory_bytes,
        query_latency_secs,
    }
}

/// One instance type standing for an EC2 machine family (§4 of the
/// evaluation plan, ProjectASAP/ASAPQuery#777).
#[derive(Debug, Clone, PartialEq)]
pub struct MachineFamily {
    pub family: String,
    pub vcpu: f64,
    pub memory_gib: f64,
    pub usd_per_hour: f64,
}

impl MachineFamily {
    /// Reads the `families` of a `scripts/fetch_ec2_pricing.py` snapshot.
    pub fn from_pricing_json(json: &str) -> Result<Vec<Self>, String> {
        let snapshot: serde_json::Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
        let families = snapshot["families"]
            .as_array()
            .ok_or("pricing snapshot has no families array")?;
        families
            .iter()
            .map(|f| {
                let number = |key: &str| {
                    f[key]
                        .as_f64()
                        .ok_or_else(|| format!("family is missing numeric {key}"))
                };
                Ok(Self {
                    family: f["family"]
                        .as_str()
                        .ok_or("family is missing its name")?
                        .to_string(),
                    vcpu: number("vcpu")?,
                    memory_gib: number("memory_gib")?,
                    usd_per_hour: number("usd_per_hour")?,
                })
            })
            .collect()
    }

    /// Fractional instances needed for the plan: whichever of CPU or memory
    /// runs out first.
    pub fn instances(&self, objectives: &Objectives) -> f64 {
        (objectives.tco_cpu_secs_per_sec / self.vcpu)
            .max(objectives.retained_memory_bytes / BYTES_PER_GIB / self.memory_gib)
    }

    pub fn usd_per_hour(&self, objectives: &Objectives) -> f64 {
        self.usd_per_hour * self.instances(objectives)
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
            sketch: "cms-heap-topk-fastpath-vector2d".into(),
            sketch_config: serde_json::json!(null),
            mem_bytes_per_instance: 10.0,
            insert_cpu_secs: 2.0,
            merge_cpu_secs: 3.0,
            query_cpu_secs: 5.0,
            query_accuracy: BTreeMap::from([("err".into(), 0.0)]),
        };
        let deployment = Deployment {
            capability: Capability::TopK,
            labels: labels.clone(),
            config,
            window_secs: 20,
            slide_secs: 10,
        };
        let rqe = Rqe {
            id: "r".into(),
            capability: Capability::TopK,
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
        assert_eq!(result.retained_memory_bytes, 320.0); // 4 × 10 × (20 + 60) / 10
    }

    #[test]
    fn shared_deployment_retains_for_its_longest_lookback() {
        let labels = LabelSet::new();
        let deployment = Deployment {
            capability: Capability::TopK,
            labels: labels.clone(),
            config: AtomicCostEntry {
                sketch: "cms-heap-topk-fastpath-vector2d".into(),
                sketch_config: serde_json::json!(null),
                mem_bytes_per_instance: 10.0,
                insert_cpu_secs: 1.0,
                merge_cpu_secs: 1.0,
                query_cpu_secs: 1.0,
                query_accuracy: BTreeMap::from([("err".into(), 0.0)]),
            },
            window_secs: 10,
            slide_secs: 10,
        };
        let rqe = |lookback_secs| Rqe {
            id: "r".into(),
            capability: Capability::TopK,
            lookback_secs,
            interval_secs: 10,
            labels: labels.clone(),
            accuracy_metric: "err".into(),
            accuracy_tolerance: 1.0,
            accuracy_direction: AccuracyDirection::LowerIsBetter,
        };
        let tables = BTreeMap::from([(
            labels.clone(),
            LabelSetInfo {
                cardinality: 2,
                arrival_rate_per_sec: 1.0,
            },
        )]);
        let result = score(&[rqe(60), rqe(600)], &[deployment], &vec![0, 0], &tables);
        // One copy of state, sized for the 600 s lookback: 2 × 10 × (10 + 600) / 10.
        assert_eq!(result.retained_memory_bytes, 1220.0);
    }

    #[test]
    fn machine_family_is_sized_by_its_binding_resource() {
        let family = MachineFamily {
            family: "f".into(),
            vcpu: 4.0,
            memory_gib: 8.0,
            usd_per_hour: 2.0,
        };
        let mut objectives = Objectives {
            peak_query_memory_bytes: 0.0,
            ingest_cpu_secs_per_sec: 0.0,
            merge_cpu_secs_per_sec: 0.0,
            query_cpu_secs_per_sec: 0.0,
            tco_cpu_secs_per_sec: 2.0,
            retained_memory_bytes: BYTES_PER_GIB,
            query_latency_secs: Vec::new(),
        };
        assert_eq!(family.instances(&objectives), 0.5); // CPU binds
        objectives.retained_memory_bytes = 16.0 * BYTES_PER_GIB;
        assert_eq!(family.usd_per_hour(&objectives), 4.0); // memory binds: 2 instances
    }

    #[test]
    fn reads_committed_pricing_snapshot() {
        let families =
            MachineFamily::from_pricing_json(include_str!("../data/ec2-pricing-2026-10-04.json"))
                .expect("committed snapshot parses");
        let names: Vec<_> = families.iter().map(|f| f.family.as_str()).collect();
        assert_eq!(
            names,
            ["compute_optimized", "general_purpose", "memory_optimized"]
        );
        assert!(families.iter().all(|f| f.usd_per_hour > 0.0));
    }
}
