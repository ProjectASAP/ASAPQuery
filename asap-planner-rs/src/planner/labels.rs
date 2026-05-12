use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{AggregationType, Statistic};
use promql_utilities::query_logics::logics::does_precompute_operator_support_subpopulations;

pub fn set_subpopulation_labels(
    statistic: Statistic,
    aggregation_type: AggregationType,
    subpopulation_labels: &KeyByLabelNames,
    rollup_labels: &mut KeyByLabelNames,
    grouping_labels: &mut KeyByLabelNames,
    aggregated_labels: &mut KeyByLabelNames,
) {
    // rollup is set by caller before calling this function
    let _ = rollup_labels; // not modified here
    if does_precompute_operator_support_subpopulations(statistic, aggregation_type) {
        *grouping_labels = KeyByLabelNames::empty();
        *aggregated_labels = subpopulation_labels.clone();
    } else {
        *grouping_labels = subpopulation_labels.clone();
        *aggregated_labels = KeyByLabelNames::empty();
    }
}
