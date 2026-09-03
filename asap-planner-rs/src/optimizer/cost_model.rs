use asap_types::enums::WindowType;
use promql_utilities::query_logics::enums::AggregationType;

use super::candidate_gen::CandidateConfig;
use super::constants::{
    EXACT_QUERY_CPU_SECS, INGEST_CPU_WEIGHT, INGEST_MEM_WEIGHT, INSERT_CPU_SECS,
    MEM_BYTES_PER_INSTANCE, MERGE_CPU_SECS, QUERY_CPU_SECS, QUERY_CPU_WEIGHT, QUERY_MEM_WEIGHT,
    SUBPOPULATION_COUNT, SUBTRACT_CPU_SECS,
};
use super::sketch_properties::sketch_properties;
use super::solution::{QueryMethod, AQE};

/// Per-operation costs for one sketch instance. Stub defaults for v1 — real
/// values come from sketch-bench in Phase 3 (see implementation plan, 3c).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AtomicCosts {
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

    let subpopulation_count = effective_subpopulation_count(candidate, agg_config.aggregation_type);

    // Defensive floor: slide_interval_ms is a plain u64 on a widely-shared struct;
    // guard against div-by-zero producing `inf` and poisoning cost comparisons.
    let n_concurrent = match agg_config.window_type {
        WindowType::Tumbling => 1.0,
        WindowType::Sliding => {
            (agg_config.window_size_ms as f64 / agg_config.slide_interval_ms.max(1) as f64).ceil()
        }
    };

    let mem_active = n_concurrent * subpopulation_count * costs.mem_bytes_per_instance;

    let cpu_ingest = match agg_config.window_type {
        WindowType::Tumbling => arrival_rate_hz * costs.insert_cpu_secs,
        WindowType::Sliding => arrival_rate_hz * n_concurrent * costs.insert_cpu_secs,
    };

    weights.ingest_mem * mem_active + weights.ingest_cpu * cpu_ingest
}

/// QueryCost(a,g): cost of answering one query for `aqe` from `candidate`.
pub fn query_cost(
    _aqe: &AQE,
    candidate: &CandidateConfig,
    costs: &AtomicCosts,
    weights: &CostWeights,
) -> f64 {
    let Some(agg_config) = &candidate.config else {
        return costs.exact_query_cpu_secs * weights.query_cpu; // EXACT: raw query at query time.
    };

    // Subpopulation count; see ingest_cost comment.
    let subpopulation_count = effective_subpopulation_count(candidate, agg_config.aggregation_type);
    let props = sketch_properties(agg_config.aggregation_type);

    let (cpu, mem) = match &candidate.query_method {
        QueryMethod::Direct => (
            subpopulation_count * costs.query_cpu_secs,
            subpopulation_count * costs.mem_bytes_per_instance,
        ),
        QueryMethod::Merge { num_windows } => {
            debug_assert!(props.mergeable);
            let merges = (*num_windows).saturating_sub(1) as f64;
            (
                subpopulation_count * (merges * costs.merge_cpu_secs + costs.query_cpu_secs),
                *num_windows as f64 * subpopulation_count * costs.mem_bytes_per_instance,
            )
        }
        QueryMethod::Subtract => {
            debug_assert!(props.subtractable);
            (
                subpopulation_count
                    * (costs.merge_cpu_secs + costs.subtract_cpu_secs + costs.query_cpu_secs),
                2.0 * subpopulation_count * costs.mem_bytes_per_instance,
            )
        }
        // candidate_gen only ever pairs Exact with config=None, already handled above.
        QueryMethod::Exact => {
            unreachable!("Exact query_method must not be paired with Some(config)")
        }
    };

    weights.query_cpu * cpu + weights.query_mem * mem
}

fn effective_subpopulation_count(
    candidate: &CandidateConfig,
    aggregation_type: AggregationType,
) -> f64 {
    if sketch_properties(aggregation_type).subpopulation_aware {
        SUBPOPULATION_COUNT
    } else {
        assert!(
            candidate.label_group_count > 0,
            "non-subpopulation-aware candidates require a positive label_group_count"
        );
        candidate.label_group_count as f64
    }
}

/// Total cost rate contributed by assigning AQE `aqe` (with frequency
/// `aqe.query_frequency_hz`) to `candidate`: IngestCost(g) + frequency * QueryCost(a,g).
/// This is the per-(a,g) term the greedy/MIP solver minimizes.
pub fn total_cost_rate(
    aqe: &AQE,
    candidate: &CandidateConfig,
    arrival_rate_hz: f64,
    costs: &AtomicCosts,
    weights: &CostWeights,
) -> f64 {
    ingest_cost(candidate, arrival_rate_hz, costs, weights)
        + aqe.query_frequency_hz * query_cost(aqe, candidate, costs, weights)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::candidate_gen::enumerate_candidates;
    use asap_types::query_requirements::QueryRequirements;
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::Statistic;

    fn make_aqe(stat: Statistic, range_ms: u64, min_t: u64) -> AQE {
        AQE {
            requirements: QueryRequirements {
                metric: "test_metric".into(),
                statistics: vec![stat],
                data_range_ms: range_ms,
                grouping_labels: KeyByLabelNames::empty(),
                spatial_filter_normalized: String::new(),
                topk_count_events: None,
            },
            query_strings: vec!["test_query".into()],
            query_frequency_hz: 1.0 / 60.0,
            min_t_repeat_ms: min_t,
            t_repeat_gcd_ms: min_t,
        }
    }

    #[test]
    fn exact_has_zero_ingest_cost_and_nonzero_query_cost() {
        let candidate = CandidateConfig {
            config: None,
            query_method: QueryMethod::Exact,
            n_windows: 0,
            label_group_count: 1,
        };
        let costs = AtomicCosts::default();
        let weights = CostWeights::default();
        let a = make_aqe(Statistic::Sum, 300_000, 300_000);
        assert_eq!(ingest_cost(&candidate, 1.0, &costs, &weights), 0.0);
        assert!(query_cost(&a, &candidate, &costs, &weights) > 0.0);
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
            label_group_count: 1,
        };
        let c5 = CandidateConfig {
            config: Some(template),
            query_method: QueryMethod::Merge { num_windows: 5 },
            n_windows: 5,
            label_group_count: 1,
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
            label_group_count: 1,
        };
        let subtract = CandidateConfig {
            config: Some(template),
            query_method: QueryMethod::Subtract,
            n_windows: 5,
            label_group_count: 1,
        };

        assert!(
            query_cost(&a, &subtract, &costs, &weights) < query_cost(&a, &merge, &costs, &weights)
        );
    }

    #[test]
    fn non_subpopulation_aware_cost_scales_with_label_group_count() {
        let a = make_aqe(Statistic::Sum, 300_000, 300_000);
        let candidate = enumerate_candidates(&a, 60_000)
            .into_iter()
            .find(|candidate| {
                candidate.config.as_ref().is_some_and(|config| {
                    !sketch_properties(config.aggregation_type).subpopulation_aware
                })
            })
            .expect("expected a non-subpopulation-aware candidate");
        let one_group = CandidateConfig {
            label_group_count: 1,
            ..candidate.clone()
        };
        let five_groups = CandidateConfig {
            label_group_count: 5,
            ..candidate
        };
        let costs = AtomicCosts::default();
        let weights = CostWeights {
            ingest_mem: 1.0,
            ingest_cpu: 0.0,
            query_mem: 1.0,
            query_cpu: 0.0,
        };

        assert_eq!(
            ingest_cost(&five_groups, 1.0, &costs, &weights),
            5.0 * ingest_cost(&one_group, 1.0, &costs, &weights)
        );
        assert_eq!(
            query_cost(&a, &five_groups, &costs, &weights),
            5.0 * query_cost(&a, &one_group, &costs, &weights)
        );
    }

    #[test]
    fn subpopulation_aware_cost_ignores_label_group_count() {
        let a = make_aqe(Statistic::Sum, 300_000, 300_000);
        let candidate = enumerate_candidates(&a, 60_000)
            .into_iter()
            .find(|candidate| {
                candidate.config.as_ref().is_some_and(|config| {
                    sketch_properties(config.aggregation_type).subpopulation_aware
                })
            })
            .expect("expected a subpopulation-aware candidate");
        let one_group = CandidateConfig {
            label_group_count: 1,
            ..candidate.clone()
        };
        let five_groups = CandidateConfig {
            label_group_count: 5,
            ..candidate
        };
        let costs = AtomicCosts::default();
        let weights = CostWeights::default();

        assert_eq!(
            ingest_cost(&one_group, 1.0, &costs, &weights),
            ingest_cost(&five_groups, 1.0, &costs, &weights)
        );
        assert_eq!(
            query_cost(&a, &one_group, &costs, &weights),
            query_cost(&a, &five_groups, &costs, &weights)
        );
    }
}
