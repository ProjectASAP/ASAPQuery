use asap_types::enums::WindowType;
use promql_utilities::query_logics::enums::{AggregationType, Statistic};

use super::candidate_gen::CandidateConfig;
use super::constants::{
    EXACT_QUERY_CPU_SECS, HASH_TABLE_SLACK, INGEST_CPU_WEIGHT, INGEST_MEM_WEIGHT, INSERT_CPU_SECS,
    LABEL_VALUE_CODE_BYTES, MEM_BYTES_PER_INSTANCE, MERGE_CPU_SECS, QUERY_CPU_SECS,
    QUERY_CPU_WEIGHT, QUERY_MEM_WEIGHT, SUBTRACT_CPU_SECS,
};
use super::sketch_properties::sketch_properties;
use super::solution::{OptimizerItem, QueryMethod};

/// Per-operation costs for one sketch instance. Stub defaults for v1 — real
/// values come from sketch-bench in Phase 3 (see implementation plan, 3c).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AtomicCosts {
    /// One instance; for a keyed-unbounded map, one group's entry.
    pub mem_bytes_per_instance: f64,
    pub insert_cpu_secs: f64,
    pub merge_cpu_secs: f64,
    pub subtract_cpu_secs: f64,
    pub query_cpu_secs: f64,
    /// Cost of one raw/exact query execution (the EXACT_a fallback's QueryCost).
    /// Without this, EXACT always wins since its IngestCost and QueryCost would
    /// otherwise both be zero.
    pub exact_query_cpu_secs: f64,
}

impl Default for AtomicCosts {
    fn default() -> Self {
        Self {
            mem_bytes_per_instance: MEM_BYTES_PER_INSTANCE,
            insert_cpu_secs: INSERT_CPU_SECS,
            merge_cpu_secs: MERGE_CPU_SECS,
            subtract_cpu_secs: SUBTRACT_CPU_SECS,
            query_cpu_secs: QUERY_CPU_SECS,
            exact_query_cpu_secs: EXACT_QUERY_CPU_SECS,
        }
    }
}

/// Global objective weights (w1..w4 in the design doc). Real calibration (from
/// actual cloud $/byte-sec and $/cpu-sec) is punted post-v1; defaults below
/// just reflect that RAM-held-over-time is several orders of magnitude
/// cheaper per unit than CPU-time (e.g. ~$5/GB-month vs ~$0.04/vCPU-hour is
/// roughly a 1e6 ratio), so memory weights are scaled down accordingly rather
/// than left equal to CPU weights.
#[derive(Debug, Clone, Copy)]
pub struct CostWeights {
    pub ingest_mem: f64,
    pub ingest_cpu: f64,
    pub query_mem: f64,
    pub query_cpu: f64,
}

impl Default for CostWeights {
    fn default() -> Self {
        Self {
            ingest_mem: INGEST_MEM_WEIGHT,
            ingest_cpu: INGEST_CPU_WEIGHT,
            query_mem: QUERY_MEM_WEIGHT,
            query_cpu: QUERY_CPU_WEIGHT,
        }
    }
}

/// IngestCost(g): steady-state cost rate of keeping `candidate` deployed,
/// independent of which AQEs query it (facility-location requirement).
/// `arrival_rate_hz` is the arrival rate (items/sec) for this config's metric+filter.
pub fn ingest_cost(
    candidate: &CandidateConfig,
    arrival_rate_hz: f64,
    costs: &AtomicCosts,
    weights: &CostWeights,
) -> f64 {
    let Some(agg_config) = &candidate.config else {
        return 0.0; // EXACT: no streaming config deployed.
    };

    let units = stored_units(candidate, agg_config.aggregation_type);

    // Defensive floor: slide_interval_ms is a plain u64 on a widely-shared struct;
    // guard against div-by-zero producing `inf` and poisoning cost comparisons.
    let n_concurrent = match agg_config.window_type {
        WindowType::Tumbling => 1.0,
        WindowType::Sliding => {
            (agg_config.window_size_ms as f64 / agg_config.slide_interval_ms.max(1) as f64).ceil()
        }
    };

    let mem_active = n_concurrent * units * costs.mem_bytes_per_instance;

    let cpu_ingest = match agg_config.window_type {
        WindowType::Tumbling => arrival_rate_hz * costs.insert_cpu_secs,
        WindowType::Sliding => arrival_rate_hz * n_concurrent * costs.insert_cpu_secs,
    };

    weights.ingest_mem * mem_active
        + weights.ingest_cpu * cpu_ingest
        + key_tracker_ingest_cost(candidate, arrival_rate_hz, weights)
}

/// Ingest cost of the paired key aggregation, if any: one key entry per
/// output group in its single tumbling pane, one insert per item.
// ponytail: stub insert CPU and analytical key bytes; use sketch-bench numbers once DeltaSet is measured.
fn key_tracker_ingest_cost(
    candidate: &CandidateConfig,
    arrival_rate_hz: f64,
    weights: &CostWeights,
) -> f64 {
    let Some(key_config) = &candidate.key_config else {
        return 0.0;
    };
    let n_labels = key_config.grouping_labels.len() + key_config.aggregated_labels.len();
    let entry_bytes = n_labels as f64 * LABEL_VALUE_CODE_BYTES * HASH_TABLE_SLACK;
    weights.ingest_mem * candidate.output_group_count as f64 * entry_bytes
        + weights.ingest_cpu * arrival_rate_hz * INSERT_CPU_SECS
}

/// QueryCost(a,g): cost of answering one query for `aqe` from `candidate`.
pub fn query_cost(
    item: &OptimizerItem,
    candidate: &CandidateConfig,
    costs: &AtomicCosts,
    weights: &CostWeights,
) -> f64 {
    let Some(agg_config) = &candidate.config else {
        return costs.exact_query_cpu_secs * weights.query_cpu; // EXACT: raw query at query time.
    };

    let units = stored_units(candidate, agg_config.aggregation_type);
    let props = sketch_properties(agg_config.aggregation_type);
    let read_cpu = reads_per_query(item, candidate) * costs.query_cpu_secs;

    let (cpu, mem) = match &candidate.query_method {
        QueryMethod::Direct => (read_cpu, units * costs.mem_bytes_per_instance),
        QueryMethod::Merge { num_windows } => {
            debug_assert!(props.mergeable);
            let merges = (*num_windows).saturating_sub(1) as f64;
            (
                units * merges * costs.merge_cpu_secs + read_cpu,
                *num_windows as f64 * units * costs.mem_bytes_per_instance,
            )
        }
        QueryMethod::Subtract => {
            debug_assert!(props.subtractable);
            (
                units * (costs.merge_cpu_secs + costs.subtract_cpu_secs) + read_cpu,
                2.0 * units * costs.mem_bytes_per_instance,
            )
        } // candidate_gen only ever pairs Exact with config=None, already handled above.
    };

    weights.query_cpu * cpu + weights.query_mem * mem
}

/// Units of `mem_bytes_per_instance` held per window; also scales
/// whole-structure merge/subtract work. A keyed map stores one entry per
/// output group; anything else stores fixed-size instances.
fn stored_units(candidate: &CandidateConfig, aggregation_type: AggregationType) -> f64 {
    assert!(
        candidate.instance_count > 0 && candidate.output_group_count > 0,
        "candidates require positive group counts"
    );
    if sketch_properties(aggregation_type).memory_grows_with_keys {
        candidate.output_group_count as f64
    } else {
        candidate.instance_count as f64
    }
}

/// `query_cpu_secs` operations per query: one per output group, except
/// top-k, which reads each heap once (its query cost already covers the heap).
fn reads_per_query(item: &OptimizerItem, candidate: &CandidateConfig) -> f64 {
    if item.requirements.statistics == [Statistic::Topk] {
        candidate.instance_count as f64
    } else {
        candidate.output_group_count as f64
    }
}

/// Total cost rate contributed by assigning AQE `aqe` (with frequency
/// `aqe.query_frequency_hz`) to `candidate`: IngestCost(g) + frequency * QueryCost(a,g).
/// This is the per-(a,g) term the greedy/MIP solver minimizes.
pub fn total_cost_rate(
    item: &OptimizerItem,
    candidate: &CandidateConfig,
    arrival_rate_hz: f64,
    costs: &AtomicCosts,
    weights: &CostWeights,
) -> f64 {
    ingest_cost(candidate, arrival_rate_hz, costs, weights)
        + item.query_frequency_hz * query_cost(item, candidate, costs, weights)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::candidate_gen::{enumerate_candidates, enumerate_candidates_with_facts};
    use crate::optimizer::label_set_facts::ItemFacts;
    use asap_types::query_requirements::QueryRequirements;
    use promql_utilities::data_model::KeyByLabelNames;

    fn make_aqe(stat: Statistic, range_ms: u64, min_t: u64) -> OptimizerItem {
        OptimizerItem {
            requirements: QueryRequirements {
                metric: "test_metric".into(),
                statistics: vec![stat],
                data_range_ms: range_ms,
                grouping_labels: KeyByLabelNames::empty(),
                spatial_filter_normalized: String::new(),
                topk_count_events: None,
                topk_by_labels: None,
            },
            query_strings: vec!["test_query".into()],
            query_frequency_hz: 1.0 / 60.0,
            occurrences: 1,
            t_repeat_ms: min_t,
            accuracy_sla: 0.0,
            latency_sla_ms: None,
        }
    }

    #[test]
    fn ingest_cost_independent_of_n_windows() {
        // Retained windows are a transient per-query cost (Mem_query in query_cost),
        // not a continuous allocation — so ingest_cost must not vary with n_windows.
        let a = make_aqe(Statistic::Sum, 300_000, 300_000);
        let candidates = enumerate_candidates(&a, 60_000);
        let template = candidates
            .iter()
            .find_map(|c| c.config.clone())
            .expect("expected at least one deployed candidate");

        let costs = AtomicCosts::default();
        let weights = CostWeights::default();
        let c2 = CandidateConfig {
            config: Some(template.clone()),
            query_method: QueryMethod::Merge { num_windows: 2 },
            n_windows: 2,
            instance_count: 1,
            output_group_count: 1,
            key_config: None,
        };
        let c5 = CandidateConfig {
            config: Some(template),
            query_method: QueryMethod::Merge { num_windows: 5 },
            n_windows: 5,
            instance_count: 1,
            output_group_count: 1,
            key_config: None,
        };

        assert_eq!(
            ingest_cost(&c2, 1.0, &costs, &weights),
            ingest_cost(&c5, 1.0, &costs, &weights),
        );
    }

    #[test]
    fn subtract_is_cheaper_than_merge_for_the_same_window_count() {
        // Calibration-independent: Subtract is O(1) (one subtract + one read) while
        // Merge is O(n) (n-1 merges + one read), so for the same n and same
        // underlying config, Subtract must cost less regardless of weight tuning.
        let a = make_aqe(Statistic::Sum, 300_000, 300_000);
        let candidates = enumerate_candidates(&a, 60_000);
        let template = candidates
            .iter()
            .find_map(|c| c.config.clone())
            .expect("expected at least one deployed candidate");

        let costs = AtomicCosts::default();
        let weights = CostWeights::default();
        let merge = CandidateConfig {
            config: Some(template.clone()),
            query_method: QueryMethod::Merge { num_windows: 5 },
            n_windows: 5,
            instance_count: 1,
            output_group_count: 1,
            key_config: None,
        };
        let subtract = CandidateConfig {
            config: Some(template),
            query_method: QueryMethod::Subtract,
            n_windows: 5,
            instance_count: 1,
            output_group_count: 1,
            key_config: None,
        };

        assert!(
            query_cost(&a, &subtract, &costs, &weights) < query_cost(&a, &merge, &costs, &weights)
        );
    }

    /// The Direct-method `agg_type` candidate for a `stat` AQE grouped by
    /// `svc` (and bucketed by it for top-k), with `groups` output groups.
    fn direct_candidate(
        stat: Statistic,
        agg_type: AggregationType,
        groups: u64,
    ) -> (OptimizerItem, CandidateConfig) {
        let mut a = make_aqe(stat, 300_000, 300_000);
        a.requirements.grouping_labels = KeyByLabelNames::new(vec!["svc".into()]);
        let facts = ItemFacts {
            output_group_count: groups,
            topk_by_group_count: None,
            arrival_rate_per_sec: 1.0,
        };
        let candidate = enumerate_candidates_with_facts(&a, 60_000, &facts)
            .into_iter()
            .find(|c| {
                c.query_method == QueryMethod::Direct
                    && c.config
                        .as_ref()
                        .is_some_and(|cfg| cfg.aggregation_type == agg_type)
            })
            .unwrap_or_else(|| panic!("expected a Direct {agg_type:?} candidate"));
        (a, candidate)
    }

    const MEM_ONLY: CostWeights = CostWeights {
        ingest_mem: 1.0,
        ingest_cpu: 0.0,
        query_mem: 1.0,
        query_cpu: 0.0,
    };
    const QUERY_CPU_ONLY: CostWeights = CostWeights {
        ingest_mem: 0.0,
        ingest_cpu: 0.0,
        query_mem: 0.0,
        query_cpu: 1.0,
    };

    #[test]
    fn per_group_and_keyed_map_costs_scale_with_output_groups() {
        // KLL: one instance per group. MultipleSum: one map, one entry per group.
        let costs = AtomicCosts::default();
        for (stat, agg_type) in [
            (Statistic::Quantile, AggregationType::DatasketchesKLL),
            (Statistic::Sum, AggregationType::MultipleSum),
        ] {
            let (a, one) = direct_candidate(stat, agg_type, 1);
            let (_, five) = direct_candidate(stat, agg_type, 5);
            assert_eq!(
                ingest_cost(&five, 1.0, &costs, &MEM_ONLY),
                5.0 * ingest_cost(&one, 1.0, &costs, &MEM_ONLY),
                "{agg_type:?} ingest memory"
            );
            for weights in [MEM_ONLY, QUERY_CPU_ONLY] {
                assert_eq!(
                    query_cost(&a, &five, &costs, &weights),
                    5.0 * query_cost(&a, &one, &costs, &weights),
                    "{agg_type:?} query cost"
                );
            }
        }
    }

    #[test]
    fn fixed_size_keyed_sketch_memory_ignores_groups_but_query_reads_each() {
        let costs = AtomicCosts::default();
        let (a, one) = direct_candidate(Statistic::Sum, AggregationType::CountMinSketch, 1);
        let (_, five) = direct_candidate(Statistic::Sum, AggregationType::CountMinSketch, 5);
        // Only the paired key tracker grows: one 1-label entry per extra group.
        let key_entry_bytes = LABEL_VALUE_CODE_BYTES * HASH_TABLE_SLACK;
        assert!(
            (ingest_cost(&five, 1.0, &costs, &MEM_ONLY)
                - ingest_cost(&one, 1.0, &costs, &MEM_ONLY)
                - 4.0 * key_entry_bytes)
                .abs()
                < 1e-9
        );
        assert_eq!(
            query_cost(&a, &one, &costs, &MEM_ONLY),
            query_cost(&a, &five, &costs, &MEM_ONLY)
        );
        assert_eq!(
            query_cost(&a, &five, &costs, &QUERY_CPU_ONLY),
            5.0 * query_cost(&a, &one, &costs, &QUERY_CPU_ONLY)
        );
    }

    #[test]
    fn topk_reads_each_heap_once_regardless_of_output_groups() {
        let costs = AtomicCosts::default();
        let mut a = make_aqe(Statistic::Topk, 60_000, 60_000);
        a.requirements.grouping_labels = KeyByLabelNames::new(vec!["svc".into()]);
        let heap = |groups| {
            let facts = ItemFacts {
                output_group_count: groups,
                topk_by_group_count: None,
                arrival_rate_per_sec: 1.0,
            };
            enumerate_candidates_with_facts(&a, 15_000, &facts)
                .into_iter()
                .find(|c| c.config.is_some())
                .expect("a CMS-with-heap candidate")
        };
        let (few, many) = (heap(1), heap(1000));
        assert_eq!(many.instance_count, 1);
        assert_eq!(
            query_cost(&a, &few, &costs, &QUERY_CPU_ONLY),
            query_cost(&a, &many, &costs, &QUERY_CPU_ONLY)
        );
    }
}
