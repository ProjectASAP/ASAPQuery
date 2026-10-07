//! Analytical scoring built from measured per-operation costs (§5).
//!
//! Costs split into four phases, each with CPU and memory:
//! - ingest: inserts into open windows, and their state;
//! - merge: folding a query's windows, and the merged state;
//! - query: reading the merged state, and the output;
//! - storage: closed windows kept for lookbacks (no CPU).
//!
//! [`score`] and the MILP share the per-phase functions below.
//!
//! Measured costs are per instance. A deployment holds `card(G)` instances
//! per window, or one if its family keeps [one fixed-size sketch for all
//! groups](crate::FamilyProperties::one_fixed_size_sketch_for_all_groups).
//! Exact accumulators hold many groups in one instance, but the export
//! divides their memory and merge cost by the group count, so `card(G) ×`
//! prices them correctly too. A [key tracker](crate::Deployment::key_tracker)
//! is priced like an exact accumulator on the same windows, in every phase.

use crate::{secs, AtomicCostEntry, Capability, Deployment, Mapping, Millis, Raqe, WorkloadFacts};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const BYTES_PER_GIB: f64 = (1u64 << 30) as f64;

/// Query output per group: one f64.
// ponytail: estimated, not measured; tiny next to sketch state.
pub const OUTPUT_BYTES_PER_VALUE: f64 = 8.0;
/// Query output per top-k entry: a 64-bit key hash and a count.
pub const OUTPUT_BYTES_PER_TOPK_ENTRY: f64 = 16.0;
/// Top-k entries one query outputs per group: the `k` it answers
/// ([`crate::TOPK_K`]), whatever the heap's capacity.
// ponytail: fixed k. A large k needs `k` on the RAQE.
pub const TOPK_ENTRIES: f64 = crate::TOPK_K as f64;
/// Bytes per DDSketch bucket: one `u64` count, as sketch-bench's
/// `dd_footprint` counts them.
pub const DD_BYTES_PER_BUCKET: f64 = 8.0;

/// One phase's resource use.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct PhaseCost {
    /// Mean CPU-seconds per second: the area under the CPU curve over time.
    pub cpu_secs_per_sec: f64,
    pub memory_bytes: f64,
}

impl std::ops::AddAssign for PhaseCost {
    fn add_assign(&mut self, other: Self) {
        self.cpu_secs_per_sec += other.cpu_secs_per_sec;
        self.memory_bytes += other.memory_bytes;
    }
}

#[derive(Debug, Clone)]
pub struct PlanCost {
    /// Paid once per active deployment.
    pub ingest: PhaseCost,
    /// Paid per RAQE; memory as if every query runs at once.
    pub merge: PhaseCost,
    /// Paid per RAQE; memory as if every query runs at once.
    pub query: PhaseCost,
    /// Paid once per active deployment, sized for its longest lookback.
    pub storage: PhaseCost,
    /// Index-aligned with the input RAQE slice; display IDs need not be unique.
    pub query_latency_ms: Vec<f64>,
}

impl PlanCost {
    pub fn cpu_secs_per_sec(&self) -> f64 {
        self.phases().map(|phase| phase.cpu_secs_per_sec).sum()
    }

    pub fn memory_bytes(&self) -> f64 {
        self.phases().map(|phase| phase.memory_bytes).sum()
    }

    fn phases(&self) -> impl Iterator<Item = &PhaseCost> {
        [&self.ingest, &self.merge, &self.query, &self.storage].into_iter()
    }
}

/// `card(G)`: cardinality of `G`, the number of groups.
fn group_count(deployment: &Deployment, facts: &WorkloadFacts) -> f64 {
    facts[&deployment.metric].cardinality[&deployment.grouping_labels] as f64
}

/// The sketch and its key tracker, if any, each with its instances per
/// window: 1 for a fixed-size sketch shared by all groups, else `card(G)`.
fn priced_parts<'a>(
    deployment: &'a Deployment,
    facts: &WorkloadFacts,
) -> impl Iterator<Item = (&'a AtomicCostEntry, f64)> {
    let properties = deployment.properties();
    assert_eq!(
        properties.needs_delta_set_key_tracker,
        deployment.key_tracker.is_some(),
        "{}: key tracker present iff the family needs one, or the plan is mispriced",
        deployment.config.sketch
    );
    let groups = group_count(deployment, facts);
    let sketch_instances = if properties.one_fixed_size_sketch_for_all_groups {
        1.0
    } else {
        groups
    };
    std::iter::once((&deployment.config, sketch_instances)).chain(
        deployment
            .key_tracker
            .iter()
            .map(move |tracker| (tracker, groups)),
    )
}

/// `m`: memory of one `config` instance (`deployment`'s sketch or its key
/// tracker) holding `span_ms` of one group's samples: a
/// window `x`, or the lookback `L` for a query's merge accumulator.
/// DDSketch on a metric with a known value range holds one store (values are
/// positive) of [`dd_bucket_count`] buckets, capped by the values it holds,
/// `λ · span / card(G)`, as `dd_footprint` caps them by count. The range is the metric's, so this is
/// an upper bound for a group whose own values span less. Every other row,
/// and DDSketch without a range, keeps the measured size.
pub(crate) fn instance_memory_bytes(
    config: &AtomicCostEntry,
    deployment: &Deployment,
    facts: &WorkloadFacts,
    span_ms: Millis,
) -> f64 {
    let metric = &facts[&deployment.metric];
    let alpha = config.sketch_config["params"]["alpha"].as_f64();
    match (config.sketch.as_str(), metric.value_range, alpha) {
        ("dd", Some((lo, hi)), Some(alpha)) => {
            let values =
                metric.arrival_rate_per_sec() * secs(span_ms) / group_count(deployment, facts);
            dd_bucket_count(lo, hi, alpha).min(values.ceil().max(1.0)) * DD_BYTES_PER_BUCKET
        }
        _ => config.mem_bytes_per_instance,
    }
}

/// Buckets DDSketch needs for values in `[lo, hi]` at relative accuracy
/// `alpha`: `⌊ln(hi/lo) / ln((1+α)/(1−α))⌋ + 1`, as `dd_footprint` counts them.
pub(crate) fn dd_bucket_count(lo: f64, hi: f64, alpha: f64) -> f64 {
    ((hi / lo).ln() / ((1.0 + alpha) / (1.0 - alpha)).ln()).floor() + 1.0
}

/// `x / y`: windows each sample is inserted into.
fn open_window_count(deployment: &Deployment) -> f64 {
    deployment
        .active_instance_count()
        .expect("candidate window and slide align") as f64
}

/// `L / x`: windows merged per query.
fn merged_window_count(raqe: &Raqe, deployment: &Deployment) -> f64 {
    deployment
        .query_instance_count(raqe.lookback_ms)
        .expect("only eligible pairs are costed") as f64
}

/// Per part: CPU `λ · (x/y) · c_ins`; memory `instances · m · (x/y)`.
pub(crate) fn ingest(deployment: &Deployment, facts: &WorkloadFacts) -> PhaseCost {
    let open_windows = open_window_count(deployment);
    let arrival_rate = facts[&deployment.metric].arrival_rate_per_sec();
    let mut cost = PhaseCost::default();
    for (config, instances) in priced_parts(deployment, facts) {
        cost += PhaseCost {
            cpu_secs_per_sec: arrival_rate * open_windows * config.insert_cpu_secs,
            memory_bytes: instances
                * instance_memory_bytes(config, deployment, facts, deployment.window_ms)
                * open_windows,
        };
    }
    cost
}

/// Per part: CPU `instances · (L/x − 1) · c_mrg / T`; memory `instances · m`,
/// every merged accumulator held at once, each holding the whole lookback. A
/// direct query (`L == x`) merges nothing and costs neither.
pub(crate) fn merge(raqe: &Raqe, deployment: &Deployment, facts: &WorkloadFacts) -> PhaseCost {
    let merges_per_instance = merged_window_count(raqe, deployment) - 1.0;
    if merges_per_instance == 0.0 {
        return PhaseCost::default();
    }
    let mut cost = PhaseCost::default();
    for (config, instances) in priced_parts(deployment, facts) {
        cost += PhaseCost {
            cpu_secs_per_sec: instances * merges_per_instance * config.merge_cpu_secs
                / secs(raqe.interval_ms),
            memory_bytes: instances
                * instance_memory_bytes(config, deployment, facts, raqe.lookback_ms),
        };
    }
    cost
}

/// CPU `card(G) · Σ c_qry / T`, one probe per group of each part; memory
/// `card(G) ·` output bytes per group.
pub(crate) fn query(raqe: &Raqe, deployment: &Deployment, facts: &WorkloadFacts) -> PhaseCost {
    let groups = group_count(deployment, facts);
    let output_bytes_per_group = match raqe.capability {
        Capability::TopKByValue | Capability::TopKByCount => {
            TOPK_ENTRIES * OUTPUT_BYTES_PER_TOPK_ENTRY
        }
        _ => OUTPUT_BYTES_PER_VALUE,
    };
    let query_cpu_secs_per_group: f64 = priced_parts(deployment, facts)
        .map(|(config, _)| config.query_cpu_secs)
        .sum();
    PhaseCost {
        cpu_secs_per_sec: groups * query_cpu_secs_per_group / secs(raqe.interval_ms),
        memory_bytes: groups * output_bytes_per_group,
    }
}

/// Closed windows kept to answer `raqe`, per part:
/// `instances · m · ((L − x)/y + 1)`.
pub(crate) fn storage_bytes(raqe: &Raqe, deployment: &Deployment, facts: &WorkloadFacts) -> f64 {
    let closed_windows = deployment
        .closed_instance_count(raqe.lookback_ms)
        .expect("only eligible pairs are costed") as f64;
    priced_parts(deployment, facts)
        .map(|(config, instances)| {
            instances
                * instance_memory_bytes(config, deployment, facts, deployment.window_ms)
                * closed_windows
        })
        .sum()
}

/// Serial CPU time of one query, in ms, summed over parts:
/// `card(G) · c_qry + instances · (L/x − 1) · c_mrg`.
pub(crate) fn query_latency_ms(raqe: &Raqe, deployment: &Deployment, facts: &WorkloadFacts) -> f64 {
    let groups = group_count(deployment, facts);
    let merges_per_instance = merged_window_count(raqe, deployment) - 1.0;
    1000.0
        * priced_parts(deployment, facts)
            .map(|(config, instances)| {
                groups * config.query_cpu_secs
                    + instances * merges_per_instance * config.merge_cpu_secs
            })
            .sum::<f64>()
}

pub fn score(
    raqes: &[Raqe],
    deployments: &[Deployment],
    mapping: &Mapping,
    facts: &WorkloadFacts,
) -> PlanCost {
    assert_eq!(
        mapping.len(),
        raqes.len(),
        "mapping must have one deployment per RAQE"
    );
    let active_deployments: BTreeSet<usize> = mapping.iter().copied().collect();
    let mut ingest_cost = PhaseCost::default();
    for &deployment_index in &active_deployments {
        ingest_cost += ingest(&deployments[deployment_index], facts);
    }

    let mut storage_bytes_by_deployment: BTreeMap<usize, f64> = BTreeMap::new();
    let mut merge_cost = PhaseCost::default();
    let mut query_cost = PhaseCost::default();
    let query_latency_ms = raqes
        .iter()
        .zip(mapping)
        .map(|(raqe, &deployment_index)| {
            let deployment = &deployments[deployment_index];
            let stored = storage_bytes_by_deployment
                .entry(deployment_index)
                .or_default();
            *stored = stored.max(storage_bytes(raqe, deployment, facts));
            merge_cost += merge(raqe, deployment, facts);
            query_cost += query(raqe, deployment, facts);
            query_latency_ms(raqe, deployment, facts)
        })
        .collect();

    PlanCost {
        ingest: ingest_cost,
        merge: merge_cost,
        query: query_cost,
        storage: PhaseCost {
            cpu_secs_per_sec: 0.0,
            memory_bytes: storage_bytes_by_deployment.values().sum(),
        },
        query_latency_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{deployment, facts, raqe};

    #[test]
    fn dd_bucket_count_is_log_range_over_log_gamma() {
        // alpha = 1/3 gives gamma = 2: buckets [1, 2), [2, 4), ... up to 1000.
        assert_eq!(dd_bucket_count(1.0, 1000.0, 1.0 / 3.0), 10.0);
        assert_eq!(dd_bucket_count(5.0, 5.0, 0.01), 1.0);
        // 1 ms to 60 s at 1%: ln(60000) / ln(1.01 / 0.99) = 550.1.
        assert_eq!(dd_bucket_count(0.001, 60.0, 0.01), 551.0);
    }

    #[test]
    fn dd_memory_comes_from_the_value_range_when_known() {
        let mut dd = deployment(1_000.0, 0.0, 0.0, 0.0, 60_000, 60_000);
        dd.config.sketch = "dd".into();
        dd.config.sketch_config = serde_json::json!({"params": {"alpha": 1.0 / 3.0}});
        let mut ranged = facts(1, 1);
        ranged
            .get_mut(crate::test_support::METRIC)
            .unwrap()
            .value_range = Some((1.0, 1000.0));

        let window = |d: &Deployment| d.window_ms;
        assert_eq!(
            instance_memory_bytes(&dd.config, &dd, &ranged, window(&dd)),
            80.0
        ); // 10 buckets × 8 B
        assert_eq!(ingest(&dd, &ranged).memory_bytes, 80.0);

        // 1 sample/sec into a 5 s window: 5 values, fewer than the 10 buckets.
        let short = Deployment {
            window_ms: 5_000,
            slide_ms: 5_000,
            ..dd.clone()
        };
        assert_eq!(ingest(&short, &ranged).memory_bytes, 40.0);
        // A 15 s query folds 3 windows: its accumulator holds 15 values, so
        // the 10-bucket range is the cap again, not the window's 5.
        assert_eq!(
            merge(&raqe(15_000, 15_000), &short, &ranged).memory_bytes,
            80.0
        );
        assert_eq!(
            merge(&raqe(10_000, 10_000), &short, &ranged).memory_bytes,
            80.0
        );

        // No range, or not DDSketch: the measured size.
        assert_eq!(
            instance_memory_bytes(&dd.config, &dd, &facts(1, 1), window(&dd)),
            1_000.0
        );
        let cms = deployment(1_000.0, 0.0, 0.0, 0.0, 60_000, 60_000);
        assert_eq!(
            instance_memory_bytes(&cms.config, &cms, &ranged, window(&cms)),
            1_000.0
        );
    }

    #[test]
    fn scores_each_phase() {
        // 4 groups and 7 samples/sec. Window 20 s sliding by 10 s: 2 open
        // windows. Lookback 60 s every 30 s: 3 windows merged, and
        // (60 − 20)/10 + 1 = 5 closed windows stored.
        let deployment = deployment(10.0, 2.0, 3.0, 5.0, 20_000, 10_000);
        let result = score(
            &[raqe(60_000, 30_000)],
            &[deployment],
            &vec![0],
            &facts(4, 7),
        );

        assert_eq!(result.ingest.cpu_secs_per_sec, 28.0); // 7 × 2 × 2
        assert_eq!(result.ingest.memory_bytes, 80.0); // 4 × 10 × 2
        assert_eq!(result.merge.cpu_secs_per_sec, 24.0 / 30.0); // 4 × 2 × 3 / 30
        assert_eq!(result.merge.memory_bytes, 40.0); // 4 × 10
        assert_eq!(result.query.cpu_secs_per_sec, 20.0 / 30.0); // 4 × 5 / 30
        assert_eq!(result.query.memory_bytes, 2048.0); // 4 × 32 × 16
        assert_eq!(result.storage.cpu_secs_per_sec, 0.0);
        assert_eq!(result.storage.memory_bytes, 200.0); // 4 × 10 × 5
        assert_eq!(result.query_latency_ms, vec![44_000.0]); // 4 × (5 + 2 × 3) s
        assert!((result.cpu_secs_per_sec() - (28.0 + 44.0 / 30.0)).abs() < 1e-12);
        assert_eq!(result.memory_bytes(), 80.0 + 40.0 + 2048.0 + 200.0);
    }

    #[test]
    fn a_direct_query_stores_one_closed_window_and_never_merges() {
        let deployment = deployment(10.0, 1.0, 1.0, 1.0, 60_000, 60_000);
        let result = score(
            &[raqe(60_000, 60_000)],
            &[deployment],
            &vec![0],
            &facts(1, 1),
        );
        assert_eq!(result.storage.memory_bytes, 10.0);
        assert_eq!(result.merge, PhaseCost::default());
    }

    #[test]
    fn shared_deployment_stores_for_its_longest_lookback() {
        let deployment = deployment(10.0, 1.0, 1.0, 1.0, 10_000, 10_000);
        let raqes = [raqe(60_000, 10_000), raqe(600_000, 10_000)];
        let result = score(&raqes, &[deployment], &vec![0, 0], &facts(2, 2));
        // 2 groups × 10 bytes × ((600 − 10)/10 + 1) closed windows.
        assert_eq!(result.storage.memory_bytes, 1200.0);
        // Ingest is paid once; merge once per RAQE.
        assert_eq!(result.ingest.cpu_secs_per_sec, 2.0);
        // Two RAQEs × 2 groups × 10 bytes.
        assert_eq!(result.merge.memory_bytes, 2.0 * 2.0 * 10.0);
    }
    #[test]
    fn a_shared_sketch_is_priced_once_and_its_tracker_per_group() {
        // Same windows as `scores_each_phase`: 2 open, 3 merged, 5 closed;
        // 4 groups, 7 samples/sec. Hydra: 100 bytes for all groups. Tracker:
        // 10 bytes per group.
        let mut deployment = deployment(100.0, 2.0, 3.0, 5.0, 20_000, 10_000);
        deployment.capability = Capability::Quantile;
        deployment.config.sketch = "hydra-kll".into();
        deployment.key_tracker = Some(AtomicCostEntry {
            sketch: crate::KEY_TRACKER_FAMILY.into(),
            mem_bytes_per_instance: 10.0,
            insert_cpu_secs: 1.0,
            merge_cpu_secs: 1.0,
            query_cpu_secs: 1.0,
            ..deployment.config.clone()
        });
        let raqe = Raqe {
            capability: Capability::Quantile,
            ..raqe(60_000, 30_000)
        };
        let result = score(&[raqe], &[deployment], &vec![0], &facts(4, 7));

        assert_eq!(result.ingest.cpu_secs_per_sec, 7.0 * 2.0 * (2.0 + 1.0));
        assert_eq!(result.ingest.memory_bytes, 100.0 * 2.0 + 4.0 * 10.0 * 2.0);
        assert_eq!(
            result.merge.cpu_secs_per_sec,
            (2.0 * 3.0 + 4.0 * 2.0 * 1.0) / 30.0
        );
        assert_eq!(result.merge.memory_bytes, 100.0 + 4.0 * 10.0);
        assert_eq!(result.query.cpu_secs_per_sec, 4.0 * (5.0 + 1.0) / 30.0);
        assert_eq!(result.query.memory_bytes, 4.0 * OUTPUT_BYTES_PER_VALUE);
        assert_eq!(result.storage.memory_bytes, 100.0 * 5.0 + 4.0 * 10.0 * 5.0);
        // 4 × (5 + 1) query + 2 × 3 Hydra merges + 4 × 2 × 1 tracker merges, in s.
        assert_eq!(result.query_latency_ms, vec![38_000.0]);
    }

    #[test]
    #[should_panic(expected = "key tracker present iff the family needs one")]
    fn a_family_that_needs_a_tracker_is_never_priced_without_one() {
        let mut deployment = deployment(100.0, 1.0, 1.0, 1.0, 60_000, 60_000);
        deployment.config.sketch = "hydra-kll".into();
        ingest(&deployment, &facts(1, 1));
    }
}
