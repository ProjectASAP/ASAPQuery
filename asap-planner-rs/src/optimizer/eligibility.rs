use std::collections::BTreeMap;

use promql_utilities::query_logics::enums::AggregationType;

use super::atomic_costs::ResolvedAtomicCosts;
use super::candidate_gen::CandidateConfig;
use super::cost_model::estimated_query_cpu_secs;
use super::solution::OptimizerItem;

const CMS_ERROR_METRIC: &str = "relative_error_mean";
const KLL_ERROR_METRIC: &str = "mean_rank_err";
const HLL_ERROR_METRIC: &str = "relative_error";
const CMS_HEAP_RECALL_METRIC: &str = "recall_at_k";

/// Whether a candidate's empirical measurements satisfy one item's SLAs.
pub fn satisfies_slas(
    item: &OptimizerItem,
    candidate: &CandidateConfig,
    resolved: &ResolvedAtomicCosts,
) -> bool {
    satisfies_accuracy_sla(item.accuracy_sla, candidate, &resolved.query_accuracy)
        && satisfies_latency_sla(item.latency_sla, item, candidate, &resolved.costs)
}

fn satisfies_accuracy_sla(
    accuracy_sla: f64,
    candidate: &CandidateConfig,
    measurements: &BTreeMap<String, f64>,
) -> bool {
    if accuracy_sla == 0.0 {
        return true;
    }

    let Some(config) = &candidate.config else {
        return true;
    };

    if exact_accumulator(config.aggregation_type) {
        return true;
    }

    let measurement = match config.aggregation_type {
        AggregationType::CountMinSketch => measurements.get(CMS_ERROR_METRIC),
        AggregationType::DatasketchesKLL => measurements.get(KLL_ERROR_METRIC),
        AggregationType::HLL => measurements.get(HLL_ERROR_METRIC),
        AggregationType::CountMinSketchWithHeap => measurements.get(CMS_HEAP_RECALL_METRIC),
        _ => return false,
    };
    let Some(&measurement) = measurement else {
        return false;
    };
    if !measurement.is_finite() || !(0.0..=1.0).contains(&measurement) {
        return false;
    }

    match config.aggregation_type {
        AggregationType::CountMinSketch
        | AggregationType::DatasketchesKLL
        | AggregationType::HLL => measurement <= 1.0 - accuracy_sla,
        AggregationType::CountMinSketchWithHeap => measurement >= accuracy_sla,
        _ => unreachable!("only measured approximate families reach this branch"),
    }
}

fn satisfies_latency_sla(
    latency_sla: f64,
    item: &OptimizerItem,
    candidate: &CandidateConfig,
    costs: &super::cost_model::AtomicCosts,
) -> bool {
    if latency_sla == 0.0 {
        return true;
    }

    let estimate = estimated_query_cpu_secs(item, candidate, costs);
    estimate.is_finite() && estimate >= 0.0 && estimate <= latency_sla
}

fn exact_accumulator(aggregation_type: AggregationType) -> bool {
    matches!(
        aggregation_type,
        AggregationType::Sum
            | AggregationType::MultipleSum
            | AggregationType::Increase
            | AggregationType::MultipleIncrease
            | AggregationType::MinMax
            | AggregationType::MultipleMinMax
    )
}
