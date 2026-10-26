//! A plan's resource use over time, its cost billed by use, and its batch
//! latency (`docs/rqe_optimizer_cost_model.md`, "Cost by use and batch
//! latency").
//!
//! Ingest is a steady load on `⌈ρ⌉` workers per deployment, split by sample.
//! Each closed window is compacted (the workers' partials merged into one),
//! and each RAQE firing runs one query job (merge, then estimate) once the
//! newest window is compacted. A job uses at most one core: sketch-bench's
//! operations are single-threaded, their CPU time equal to their wall time.
//! CPU is elastic, so every job starts when it is ready.

use std::collections::BTreeMap;

use crate::analytical_cost_model::{self, BYTES_PER_GIB};
use crate::{Deployment, Mapping, Millis, Raqe, WorkloadFacts};

/// One active deployment's steady load.
#[derive(Debug, Clone)]
pub struct DeploymentLoad {
    /// Ingest CPU, vCPUs.
    pub ingest_cpu: f64,
    /// Open windows on every worker, bytes.
    pub ingest_bytes: f64,
    /// Closed windows kept for the longest lookback served, bytes.
    pub storage_bytes: f64,
    /// CPU-seconds to compact one closed window (0 with one worker).
    pub compaction_secs: f64,
    /// Bytes a compaction holds while it runs: the closed window's partial
    /// copies (0 with one worker).
    pub compaction_bytes: f64,
    /// Windows close every slide.
    pub slide_ms: Millis,
}

/// One RAQE's query job, run every interval.
#[derive(Debug, Clone)]
pub struct QueryLoad {
    /// Index into [`PlanLoad::deployments`].
    pub deployment: usize,
    pub interval_ms: Millis,
    /// Compaction of the newest window, then this job, on their own cores, ms
    /// ([`analytical_cost_model::chain_ms`]).
    pub chain_ms: f64,
    /// Merge then estimate, CPU-seconds (one core at most).
    pub work_secs: f64,
    /// Merge accumulators and output, held while the job runs.
    pub memory_bytes: f64,
}

/// A plan as load over time.
#[derive(Debug, Clone)]
pub struct PlanLoad {
    pub deployments: Vec<DeploymentLoad>,
    /// Index-aligned with the plan's RAQEs.
    pub queries: Vec<QueryLoad>,
}

impl PlanLoad {
    /// The load of `mapping` (one candidate index per RAQE).
    pub fn new(
        raqes: &[Raqe],
        candidates: &[Deployment],
        mapping: &Mapping,
        facts: &WorkloadFacts,
    ) -> PlanLoad {
        let mut index: BTreeMap<usize, usize> = BTreeMap::new();
        let mut deployments: Vec<DeploymentLoad> = Vec::new();
        let mut queries = Vec::with_capacity(raqes.len());
        for (raqe, &candidate) in raqes.iter().zip(mapping) {
            let deployment = &candidates[candidate];
            let at = *index.entry(candidate).or_insert_with(|| {
                let ingest = analytical_cost_model::ingest(deployment, facts);
                let workers = analytical_cost_model::ingest_workers(deployment, facts);
                deployments.push(DeploymentLoad {
                    ingest_cpu: ingest.cpu_secs_per_sec,
                    ingest_bytes: ingest.memory_bytes * workers,
                    storage_bytes: 0.0,
                    compaction_secs: analytical_cost_model::compaction_secs(deployment, facts),
                    compaction_bytes: analytical_cost_model::compaction_bytes(deployment, facts),
                    slide_ms: deployment.slide_ms,
                });
                deployments.len() - 1
            });
            let storage = analytical_cost_model::storage_bytes(raqe, deployment, facts);
            let load = &mut deployments[at];
            load.storage_bytes = load.storage_bytes.max(storage);
            queries.push(QueryLoad {
                deployment: at,
                interval_ms: raqe.interval_ms,
                chain_ms: analytical_cost_model::chain_ms(raqe, deployment, facts),
                work_secs: analytical_cost_model::query_latency_ms(raqe, deployment, facts)
                    / 1000.0,
                memory_bytes: analytical_cost_model::merge(raqe, deployment, facts).memory_bytes
                    + analytical_cost_model::query(raqe, deployment, facts).memory_bytes,
            });
        }
        PlanLoad {
            deployments,
            queries,
        }
    }

    /// Steady ingest CPU, vCPUs.
    pub fn ingest_cpu(&self) -> f64 {
        self.deployments.iter().map(|d| d.ingest_cpu).sum()
    }

    /// Ingest and storage memory, held at all times, bytes.
    pub fn static_bytes(&self) -> f64 {
        self.deployments
            .iter()
            .map(|d| d.ingest_bytes + d.storage_bytes)
            .sum()
    }

    /// Mean CPU, vCPUs: ingest, compaction and queries, by use.
    pub fn auc_cpu(&self) -> f64 {
        self.ingest_cpu()
            + self
                .deployments
                .iter()
                .map(|d| d.compaction_secs / secs(d.slide_ms))
                .sum::<f64>()
            + self
                .queries
                .iter()
                .map(|q| q.work_secs / secs(q.interval_ms))
                .sum::<f64>()
    }

    /// Mean memory, bytes: ingest and storage always; each compaction's
    /// partial copies for its run on one core every slide; each query's memory
    /// for its run on one core (`work_secs`) every interval.
    pub fn auc_bytes(&self) -> f64 {
        self.static_bytes()
            + self
                .deployments
                .iter()
                .map(|d| d.compaction_bytes * d.compaction_secs / secs(d.slide_ms))
                .sum::<f64>()
            + self
                .queries
                .iter()
                .map(|q| q.memory_bytes * q.work_secs / secs(q.interval_ms))
                .sum::<f64>()
    }

    /// The longest chain, ms: the worst batch latency, since CPU is elastic.
    pub fn longest_chain_ms(&self) -> f64 {
        self.queries.iter().map(|q| q.chain_ms).fold(0.0, f64::max)
    }
}

/// A plan's cost billed by use, and its query latency.
#[derive(Debug, Clone)]
pub struct UsageCost {
    /// Mean vCPUs.
    pub cpu: f64,
    /// Mean bytes.
    pub bytes: f64,
    /// `w_cpu · cpu + w_mem · GiB`.
    pub value: f64,
    /// Worst batch latency, ms: the longest chain, since CPU is elastic.
    pub latency_ms: f64,
}

/// `w_cpu · AUC(CPU) + w_mem · AUC(memory)` of `load`, and its latency.
pub fn usage_cost(load: &PlanLoad, w_cpu: f64, w_mem: f64) -> UsageCost {
    let (cpu, bytes) = (load.auc_cpu(), load.auc_bytes());
    UsageCost {
        cpu,
        bytes,
        value: w_cpu * cpu + w_mem * bytes / BYTES_PER_GIB,
        latency_ms: load.longest_chain_ms(),
    }
}

fn secs(ms: Millis) -> f64 {
    ms as f64 / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(ingest_cpu: f64, compaction_secs: f64, queries: &[(Millis, f64)]) -> PlanLoad {
        PlanLoad {
            deployments: vec![DeploymentLoad {
                ingest_cpu,
                ingest_bytes: 10.0,
                storage_bytes: 5.0,
                compaction_secs,
                compaction_bytes: 50.0,
                slide_ms: 1_000,
            }],
            queries: queries
                .iter()
                .map(|&(interval_ms, work_secs)| QueryLoad {
                    deployment: 0,
                    interval_ms,
                    chain_ms: 1000.0 * (compaction_secs + work_secs),
                    work_secs,
                    memory_bytes: 100.0,
                })
                .collect(),
        }
    }

    #[test]
    fn cpu_and_memory_are_billed_by_use() {
        // Ingest 0.5 vCPU; compaction 0.2 s holding 50 bytes each second; a
        // 0.1 s query holding 100 bytes each second; 15 bytes always.
        let plan = load(0.5, 0.2, &[(1_000, 0.1)]);
        let cost = usage_cost(&plan, 1.0, 0.0);
        assert!((cost.cpu - 0.8).abs() < 1e-12);
        assert!((cost.bytes - (15.0 + 50.0 * 0.2 + 100.0 * 0.1)).abs() < 1e-12);
        assert_eq!(cost.value, cost.cpu);
    }

    #[test]
    fn the_latency_is_the_longest_chain() {
        // Compaction 0.2 s before every query; queries of 0.1 s and 0.5 s.
        let plan = load(0.0, 0.2, &[(1_000, 0.1), (2_000, 0.5)]);
        assert!((usage_cost(&plan, 1.0, 0.0).latency_ms - 700.0).abs() < 1e-9);
    }
}
