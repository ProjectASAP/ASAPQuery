use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{AggregationType, Statistic};
use promql_utilities::query_logics::logics::does_precompute_operator_support_subpopulations;

pub fn set_subpopulation_labels(
    statistic: Statistic,
    aggregation_type: AggregationType,
    subpopulation_labels: &KeyByLabelNames,
    topk_by_labels: Option<&KeyByLabelNames>,
    rollup_labels: &mut KeyByLabelNames,
    grouping_labels: &mut KeyByLabelNames,
    aggregated_labels: &mut KeyByLabelNames,
) {
    // rollup is set by caller before calling this function
    let _ = rollup_labels; // not modified here

    // `topk by (job) (...)`: PromQL's `by`/`without` only *buckets* topk's
    // input for independent per-bucket ranking (unlike a reducer's `by`,
    // which collapses the output down to those labels) -- see #714. Route
    // the bucket labels to `grouping_labels` so the engine builds one
    // CountMinSketchWithHeap heap per distinct bucket value, and put the
    // remaining labels in `aggregated_labels` so each heap can still
    // recover the winning series' full identity.
    if statistic == Statistic::Topk {
        if let Some(by_labels) = topk_by_labels {
            *grouping_labels = by_labels.clone();
            *aggregated_labels = subpopulation_labels.difference(by_labels);
            return;
        }
    }

    if does_precompute_operator_support_subpopulations(statistic, aggregation_type) {
        *grouping_labels = KeyByLabelNames::empty();
        *aggregated_labels = subpopulation_labels.clone();
    } else {
        *grouping_labels = subpopulation_labels.clone();
        *aggregated_labels = KeyByLabelNames::empty();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(
        statistic: Statistic,
        aggregation_type: AggregationType,
        subpopulation_labels: &KeyByLabelNames,
        topk_by_labels: Option<&KeyByLabelNames>,
    ) -> (KeyByLabelNames, KeyByLabelNames) {
        let mut rollup = KeyByLabelNames::empty();
        let mut grouping = KeyByLabelNames::empty();
        let mut aggregated = KeyByLabelNames::empty();
        set_subpopulation_labels(
            statistic,
            aggregation_type,
            subpopulation_labels,
            topk_by_labels,
            &mut rollup,
            &mut grouping,
            &mut aggregated,
        );
        (grouping, aggregated)
    }

    /// #714: `topk by (job) (k, x)` needs one heap per `job` value, so the
    /// by-clause labels must become the engine partition key (`grouping`),
    /// not get folded into the single-heap `aggregated` key the way a bare
    /// `topk(k, x)` does.
    #[test]
    fn topk_with_by_clause_partitions_on_by_labels() {
        let all_labels = KeyByLabelNames::new(vec!["job".to_string(), "instance".to_string()]);
        let by_labels = KeyByLabelNames::new(vec!["job".to_string()]);

        let (grouping, aggregated) = run(
            Statistic::Topk,
            AggregationType::CountMinSketchWithHeap,
            &all_labels,
            Some(&by_labels),
        );

        assert_eq!(grouping, by_labels);
        assert_eq!(
            aggregated,
            KeyByLabelNames::new(vec!["instance".to_string()])
        );
    }

    /// `topk by (job, instance)` where the by-clause already covers every
    /// label: each partition is a singleton, so the per-partition heap key
    /// degenerates to empty -- still correct, since a heap with one constant
    /// key just always keeps that one item.
    #[test]
    fn topk_with_by_clause_covering_all_labels_leaves_aggregated_empty() {
        let all_labels = KeyByLabelNames::new(vec!["job".to_string(), "instance".to_string()]);

        let (grouping, aggregated) = run(
            Statistic::Topk,
            AggregationType::CountMinSketchWithHeap,
            &all_labels,
            Some(&all_labels),
        );

        assert_eq!(grouping, all_labels);
        assert_eq!(aggregated, KeyByLabelNames::empty());
    }

    /// Bare `topk(k, x)` (no by/without clause, `topk_by_labels: None`) must
    /// keep the pre-#714 behavior: a single global heap keyed on every label,
    /// not partitioned at all (see #699).
    #[test]
    fn topk_without_by_clause_uses_single_global_heap() {
        let all_labels = KeyByLabelNames::new(vec!["job".to_string(), "instance".to_string()]);

        let (grouping, aggregated) = run(
            Statistic::Topk,
            AggregationType::CountMinSketchWithHeap,
            &all_labels,
            None,
        );

        assert_eq!(grouping, KeyByLabelNames::empty());
        assert_eq!(aggregated, all_labels);
    }

    /// Non-topk statistics must ignore `topk_by_labels` entirely, even if a
    /// caller somehow passed `Some` -- it only has meaning for `Statistic::Topk`.
    #[test]
    fn non_topk_statistic_ignores_topk_by_labels() {
        let subpop = KeyByLabelNames::new(vec!["job".to_string()]);
        let irrelevant = KeyByLabelNames::new(vec!["instance".to_string()]);

        let (grouping, aggregated) = run(
            Statistic::Sum,
            AggregationType::MultipleSum,
            &subpop,
            Some(&irrelevant),
        );

        assert_eq!(grouping, KeyByLabelNames::empty());
        assert_eq!(aggregated, subpop);
    }
}
