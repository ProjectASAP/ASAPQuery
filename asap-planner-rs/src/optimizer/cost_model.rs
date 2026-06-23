use asap_types::enums::WindowType;

use super::candidate_gen::CandidateConfig;
use super::constants::{
    EXACT_QUERY_CPU_SECS, INGEST_CPU_WEIGHT, INGEST_MEM_WEIGHT, INSERT_CPU_SECS,
    MEM_BYTES_PER_INSTANCE, MERGE_CPU_SECS, QUERY_CPU_SECS, QUERY_CPU_WEIGHT, QUERY_MEM_WEIGHT,
    SUBTRACT_CPU_SECS,
};
use super::sketch_properties::sketch_properties;
use super::solution::{QueryMethod, AQE};

/// Per-operation costs for one sketch instance. Stub defaults for v1 — real
/// values come from sketch-bench in Phase 3 (see implementation plan, 3c).
#[derive(Debug, Clone, Copy)]
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
/// `rho_g` is the arrival rate (items/sec) for this config's metric+filter.
pub fn ingest_cost(
    candidate: &CandidateConfig,
    rho_g: f64,
    costs: &AtomicCosts,
    weights: &CostWeights,
) -> f64 {
    let Some(g) = &candidate.config else {
        return 0.0; // EXACT: no streaming config deployed.
    };

    // N(s,g) = 1 if subpopulation_aware else N_g (distinct label-group count).
    // N_g isn't profiled yet (needs Prometheus series-count data) — use 1 as a
    // placeholder; both branches collapse to the same value until that lands.
    let n = 1.0_f64;

    // Defensive floor: slide_interval is a plain u64 on a widely-shared struct;
    // guard against div-by-zero producing `inf` and poisoning cost comparisons.
    let n_concurrent = match g.window_type {
        WindowType::Tumbling => 1.0,
        WindowType::Sliding => (g.window_size as f64 / g.slide_interval.max(1) as f64).ceil(),
    };

    let mem_active = n_concurrent * n * costs.mem_bytes_per_instance;
    let mem_retain = match g.window_type {
        WindowType::Tumbling => candidate.n_windows as f64 * n * costs.mem_bytes_per_instance,
        WindowType::Sliding => 0.0, // already counted in mem_active's concurrent windows
    };

    let cpu_ingest = match g.window_type {
        WindowType::Tumbling => rho_g * costs.insert_cpu_secs,
        WindowType::Sliding => rho_g * n_concurrent * costs.insert_cpu_secs,
    };

    weights.ingest_mem * (mem_active + mem_retain) + weights.ingest_cpu * cpu_ingest
}

/// QueryCost(a,g): cost of answering one query for `a` from `candidate`.
pub fn query_cost(
    _a: &AQE,
    candidate: &CandidateConfig,
    costs: &AtomicCosts,
    weights: &CostWeights,
) -> f64 {
    let Some(g) = &candidate.config else {
        return costs.exact_query_cpu_secs * weights.query_cpu; // EXACT: raw query at query time.
    };

    let n = 1.0_f64; // N(s,g); see ingest_cost comment.
    let props = sketch_properties(g.aggregation_type);

    let (cpu, mem) = match &candidate.query_method {
        QueryMethod::Direct => (n * costs.query_cpu_secs, n * costs.mem_bytes_per_instance),
        QueryMethod::Merge { num_windows } => {
            debug_assert!(props.mergeable);
            let merges = (*num_windows).saturating_sub(1) as f64;
            (
                n * (merges * costs.merge_cpu_secs + costs.query_cpu_secs),
                *num_windows as f64 * n * costs.mem_bytes_per_instance,
            )
        }
        QueryMethod::Subtract => {
            debug_assert!(props.subtractable);
            (
                n * (costs.subtract_cpu_secs + costs.query_cpu_secs),
                2.0 * n * costs.mem_bytes_per_instance,
            )
        }
        // candidate_gen only ever pairs Exact with config=None, already handled above.
        QueryMethod::Exact => {
            unreachable!("Exact query_method must not be paired with Some(config)")
        }
    };

    weights.query_cpu * cpu + weights.query_mem * mem
}

/// Total cost rate contributed by assigning AQE `a` (with frequency `f_a` =
/// `a.query_frequency_hz`) to `candidate`: IngestCost(g) + f_a * QueryCost(a,g).
/// This is the per-(a,g) term the greedy/MIP solver minimizes.
pub fn total_cost_rate(
    a: &AQE,
    candidate: &CandidateConfig,
    rho_g: f64,
    costs: &AtomicCosts,
    weights: &CostWeights,
) -> f64 {
    ingest_cost(candidate, rho_g, costs, weights)
        + a.query_frequency_hz * query_cost(a, candidate, costs, weights)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::candidate_gen::enumerate_candidates;
    use asap_types::query_requirements::QueryRequirements;
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::Statistic;

    fn make_aqe(stat: Statistic, range_ms: Option<u64>, min_t: u64) -> AQE {
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
            min_t_repeat_secs: min_t,
            t_repeat_gcd_secs: min_t,
        }
    }

    #[test]
    fn exact_has_zero_ingest_cost_and_nonzero_query_cost() {
        let candidate = CandidateConfig {
            config: None,
            query_method: QueryMethod::Exact,
            n_windows: 0,
        };
        let costs = AtomicCosts::default();
        let weights = CostWeights::default();
        let a = make_aqe(Statistic::Sum, Some(300_000), 300);
        assert_eq!(ingest_cost(&candidate, 1.0, &costs, &weights), 0.0);
        assert!(query_cost(&a, &candidate, &costs, &weights) > 0.0);
    }

    #[test]
    fn subtract_is_cheaper_than_merge_for_the_same_window_count() {
        // Calibration-independent: Subtract is O(1) (one subtract + one read) while
        // Merge is O(n) (n-1 merges + one read), so for the same n and same
        // underlying config, Subtract must cost less regardless of weight tuning.
        let a = make_aqe(Statistic::Sum, Some(300_000), 300);
        let candidates = enumerate_candidates(&a, 60);
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
        };
        let subtract = CandidateConfig {
            config: Some(template),
            query_method: QueryMethod::Subtract,
            n_windows: 5,
        };

        assert!(
            query_cost(&a, &subtract, &costs, &weights) < query_cost(&a, &merge, &costs, &weights)
        );
    }
}
