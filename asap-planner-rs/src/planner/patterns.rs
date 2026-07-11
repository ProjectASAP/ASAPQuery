use promql_utilities::ast_matching::{PromQLPattern, PromQLPatternBuilder};
use promql_utilities::query_logics::enums::{AggregationOperator, PromQLFunction};
use promql_utilities::query_logics::logics::get_is_collapsable;

/// Build all patterns in priority order: ONLY_TEMPORAL (2), ONLY_SPATIAL (1),
/// ONE_TEMPORAL_ONE_SPATIAL (one per collapsable (function, op) pair). Tried
/// in order until one matches.
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

    // ONE_TEMPORAL_ONE_SPATIAL: one narrow pattern per collapsable (function, op)
    // pair (see `get_is_collapsable`) — e.g. `sum(min_over_time(x[5m]))` cannot be
    // served by a single precomputed statistic the way `sum(sum_over_time(x[5m]))`
    // can. A broad any-op-wrapping-any-function pattern would structurally match
    // non-collapsable combinations too, silently dropping the outer aggregation
    // instead of rejecting the query (see #508).
    let one_temporal_one_spatial_collapsable: Vec<PromQLPattern> = [
        PromQLFunction::Rate,
        PromQLFunction::Increase,
        PromQLFunction::SumOverTime,
        PromQLFunction::CountOverTime,
        PromQLFunction::AvgOverTime,
        PromQLFunction::MinOverTime,
        PromQLFunction::MaxOverTime,
        PromQLFunction::QuantileOverTime,
    ]
    .into_iter()
    .flat_map(|func| {
        [
            AggregationOperator::Sum,
            AggregationOperator::Count,
            AggregationOperator::Avg,
            AggregationOperator::Quantile,
            AggregationOperator::Min,
            AggregationOperator::Max,
        ]
        .into_iter()
        .filter_map(move |op| {
            if !get_is_collapsable(func, op) {
                return None;
            }
            let pattern = PromQLPatternBuilder::aggregation(
                vec![op.as_str()],
                PromQLPatternBuilder::function(
                    vec![func.as_str()],
                    vec![range_vector_pattern()],
                    Some("function"),
                    Some("function_args"),
                ),
                None,
                None,
                None,
                Some("aggregation"),
            );
            Some(PromQLPattern::new(pattern))
        })
    })
    .collect();

    let mut patterns = vec![ot_quantile, ot_temporal_funcs, os_spatial];
    patterns.extend(one_temporal_one_spatial_collapsable);
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
    fn exactly_four_collapsable_one_temporal_one_spatial_patterns() {
        // 2 ONLY_TEMPORAL + 1 ONLY_SPATIAL + 4 collapsable ONE_TEMPORAL_ONE_SPATIAL
        // (sum+sum_over_time, sum+count_over_time, min+min_over_time, max+max_over_time).
        assert_eq!(build_patterns().len(), 7);
    }

    #[test]
    fn collapsable_combinations_match() {
        assert!(matches_some_pattern("sum(sum_over_time(x[5m]))"));
        assert!(matches_some_pattern("sum(count_over_time(x[5m]))"));
        assert!(matches_some_pattern("min(min_over_time(x[5m]))"));
        assert!(matches_some_pattern("max(max_over_time(x[5m]))"));
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
    }
}
