use promql_utilities::ast_matching::collapsable_patterns::{
    build_one_temporal_one_spatial_patterns, build_topk_of_sum_patterns,
};
use promql_utilities::ast_matching::{PromQLPattern, PromQLPatternBuilder};
use promql_utilities::query_logics::enums::{AggregationOperator, PromQLFunction};

/// Build all patterns in priority order: ONLY_TEMPORAL (2), ONLY_SPATIAL (1),
/// ONE_TEMPORAL_ONE_SPATIAL (one per collapsable (function, op) pair),
/// TOPK_OF_SUM (one per function collapsable under both `sum` and `topk`).
/// Tried in order until one matches.
pub fn build_patterns() -> Vec<PromQLPattern> {
    let metric_pattern = || PromQLPatternBuilder::metric(None, None, None, Some("metric"));
    let range_vector_pattern =
        || PromQLPatternBuilder::matrix_selector(metric_pattern(), None, Some("range_vector"));

    // Temporal functions that produce a single-value result (no quantile phi arg)
    let temporal_funcs: Vec<&str> = [
        PromQLFunction::SumOverTime,
        PromQLFunction::CountOverTime,
        PromQLFunction::AvgOverTime,
        PromQLFunction::MinOverTime,
        PromQLFunction::MaxOverTime,
        PromQLFunction::Increase,
        PromQLFunction::Rate,
    ]
    .map(PromQLFunction::as_str)
    .to_vec();

    // Aggregation operators used in spatial and spatial-of-temporal patterns
    let spatial_ops: Vec<&str> = [
        AggregationOperator::Sum,
        AggregationOperator::Count,
        AggregationOperator::Avg,
        AggregationOperator::Quantile,
        AggregationOperator::Min,
        AggregationOperator::Max,
        AggregationOperator::Topk,
    ]
    .map(AggregationOperator::as_str)
    .to_vec();

    // ONLY_TEMPORAL pattern 1: quantile_over_time(phi, metric[range])
    let ot_quantile = PromQLPattern::new(PromQLPatternBuilder::function(
        vec![PromQLFunction::QuantileOverTime.as_str()],
        vec![
            PromQLPatternBuilder::number(None, None),
            range_vector_pattern(),
        ],
        Some("function"),
        Some("function_args"),
    ));

    // ONLY_TEMPORAL pattern 2: sum_over_time/count_over_time/... (metric[range])
    let ot_temporal_funcs = PromQLPattern::new(PromQLPatternBuilder::function(
        temporal_funcs.clone(),
        vec![range_vector_pattern()],
        Some("function"),
        Some("function_args"),
    ));

    // ONLY_SPATIAL pattern: agg_op(metric)
    let os_spatial = PromQLPattern::new(PromQLPatternBuilder::aggregation(
        spatial_ops,
        metric_pattern(),
        None,
        None,
        None,
        Some("aggregation"),
    ));

    let mut patterns = vec![ot_quantile, ot_temporal_funcs, os_spatial];
    patterns.extend(build_one_temporal_one_spatial_patterns());
    patterns.extend(build_topk_of_sum_patterns());
    patterns
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matches_some_pattern(query: &str) -> bool {
        let ast = promql_parser::parser::parse(query).expect("query should parse");
        build_patterns()
            .iter()
            .any(|pattern| pattern.matches(&ast).matches)
    }

    #[test]
    fn exactly_six_collapsable_one_temporal_one_spatial_patterns() {
        // 2 ONLY_TEMPORAL + 1 ONLY_SPATIAL + 6 collapsable ONE_TEMPORAL_ONE_SPATIAL
        // (sum+sum_over_time, sum+count_over_time, min+min_over_time,
        // max+max_over_time, topk+sum_over_time, topk+count_over_time)
        // + 2 TOPK_OF_SUM (sum_over_time, count_over_time).
        assert_eq!(build_patterns().len(), 11);
    }

    #[test]
    fn topk_of_sum_matches_only_linear_functions() {
        assert!(matches_some_pattern(
            "topk(32, sum by (label_0) (sum_over_time(data[15m])))"
        ));
        assert!(matches_some_pattern(
            "topk(3, sum by (job) (count_over_time(x[5m])))"
        ));
        // The counter shapes need per-series deltas before grouping.
        assert!(!matches_some_pattern(
            "topk(32, sum by (label_0) (rate(data_total[15m])))"
        ));
        assert!(!matches_some_pattern(
            "sum by (job) (rate(data_total[15m]))"
        ));
        // A CMS sums, so only `sum` can sit between topk and the function.
        assert!(!matches_some_pattern(
            "topk(3, max by (job) (sum_over_time(x[5m])))"
        ));
    }

    #[test]
    fn collapsable_combinations_match() {
        assert!(matches_some_pattern("sum(sum_over_time(x[5m]))"));
        assert!(matches_some_pattern("sum(count_over_time(x[5m]))"));
        assert!(matches_some_pattern("min(min_over_time(x[5m]))"));
        assert!(matches_some_pattern("max(max_over_time(x[5m]))"));
        assert!(matches_some_pattern("topk(1, sum_over_time(x[5m]))"));
        assert!(matches_some_pattern(
            "topk by (job) (3, count_over_time(x[5m]))"
        ));
    }

    #[test]
    fn non_collapsable_combinations_are_rejected() {
        assert!(
            !matches_some_pattern("sum(min_over_time(x[5m]))"),
            "sum+min_over_time is not collapsable"
        );
        assert!(
            !matches_some_pattern("avg(rate(x[5m]))"),
            "avg+rate is not collapsable"
        );
        assert!(
            !matches_some_pattern("sum(rate(x[5m]))"),
            "sum+rate is not collapsable"
        );
        assert!(
            !matches_some_pattern("min(max_over_time(x[5m]))"),
            "min+max_over_time is not collapsable"
        );
        assert!(
            !matches_some_pattern("topk(1, rate(x[5m]))"),
            "topk+rate is not collapsable"
        );
        assert!(
            !matches_some_pattern("topk(1, avg_over_time(x[5m]))"),
            "topk+avg_over_time is not collapsable"
        );
    }
}
