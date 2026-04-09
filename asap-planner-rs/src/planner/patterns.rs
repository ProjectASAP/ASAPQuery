use promql_utilities::ast_matching::{PromQLPattern, PromQLPatternBuilder};
use promql_utilities::query_logics::enums::{
    AggregationOperator, PromQLFunction, QueryPatternType,
};

/// Build all 5 patterns in priority order: ONLY_TEMPORAL (2), ONLY_SPATIAL (1), ONE_TEMPORAL_ONE_SPATIAL (2)
pub fn build_patterns() -> Vec<(QueryPatternType, PromQLPattern)> {
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

    // Spatial-of-temporal excludes topk (no topk(quantile_over_time(...)) pattern)
    let spatial_ops_no_topk: Vec<&str> = [
        AggregationOperator::Sum,
        AggregationOperator::Count,
        AggregationOperator::Avg,
        AggregationOperator::Quantile,
        AggregationOperator::Min,
        AggregationOperator::Max,
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

    // ONE_TEMPORAL_ONE_SPATIAL pattern 1: agg_op(quantile_over_time(phi, metric[range]))
    let ottos_quantile = PromQLPattern::new(PromQLPatternBuilder::aggregation(
        spatial_ops_no_topk.clone(),
        PromQLPatternBuilder::function(
            vec![PromQLFunction::QuantileOverTime.as_str()],
            vec![
                PromQLPatternBuilder::number(None, None),
                range_vector_pattern(),
            ],
            Some("function"),
            Some("function_args"),
        ),
        None,
        None,
        None,
        Some("aggregation"),
    ));

    // ONE_TEMPORAL_ONE_SPATIAL pattern 2: agg_op(temporal_func(metric[range]))
    let ottos_temporal = PromQLPattern::new(PromQLPatternBuilder::aggregation(
        spatial_ops_no_topk,
        PromQLPatternBuilder::function(
            temporal_funcs,
            vec![range_vector_pattern()],
            Some("function"),
            Some("function_args"),
        ),
        None,
        None,
        None,
        Some("aggregation"),
    ));

    vec![
        (QueryPatternType::OnlyTemporal, ot_quantile),
        (QueryPatternType::OnlyTemporal, ot_temporal_funcs),
        (QueryPatternType::OnlySpatial, os_spatial),
        (QueryPatternType::OneTemporalOneSpatial, ottos_quantile),
        (QueryPatternType::OneTemporalOneSpatial, ottos_temporal),
    ]
}
