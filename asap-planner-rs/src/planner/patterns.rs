use promql_utilities::ast_matching::{PromQLPattern, PromQLPatternBuilder};
use promql_utilities::query_logics::enums::QueryPatternType;

/// Build all 5 patterns in priority order: ONLY_TEMPORAL (2), ONLY_SPATIAL (1), ONE_TEMPORAL_ONE_SPATIAL (2)
pub fn build_patterns() -> Vec<(QueryPatternType, PromQLPattern)> {
    let metric_pattern = || PromQLPatternBuilder::metric(None, None, None, Some("metric"));
    let range_vector_pattern =
        || PromQLPatternBuilder::matrix_selector(metric_pattern(), None, Some("range_vector"));

    // ONLY_TEMPORAL pattern 1: quantile_over_time(phi, metric[range])
    let ot_quantile = PromQLPattern::new(PromQLPatternBuilder::function(
        vec!["quantile_over_time"],
        vec![
            PromQLPatternBuilder::number(None, None),
            range_vector_pattern(),
        ],
        Some("function"),
        Some("function_args"),
    ));

    // ONLY_TEMPORAL pattern 2: sum_over_time/count_over_time/... (metric[range])
    let ot_temporal_funcs = PromQLPattern::new(PromQLPatternBuilder::function(
        vec![
            "sum_over_time",
            "count_over_time",
            "avg_over_time",
            "min_over_time",
            "max_over_time",
            "increase",
            "rate",
        ],
        vec![range_vector_pattern()],
        Some("function"),
        Some("function_args"),
    ));

    // ONLY_SPATIAL pattern: agg_op(metric)
    let os_spatial = PromQLPattern::new(PromQLPatternBuilder::aggregation(
        vec!["sum", "count", "avg", "quantile", "min", "max", "topk"],
        metric_pattern(),
        None,
        None,
        None,
        Some("aggregation"),
    ));

    // ONE_TEMPORAL_ONE_SPATIAL pattern 1: agg_op(quantile_over_time(phi, metric[range]))
    let ottos_quantile = PromQLPattern::new(PromQLPatternBuilder::aggregation(
        vec!["sum", "count", "avg", "quantile", "min", "max"],
        PromQLPatternBuilder::function(
            vec!["quantile_over_time"],
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
        vec!["sum", "count", "avg", "quantile", "min", "max"],
        PromQLPatternBuilder::function(
            vec![
                "sum_over_time",
                "count_over_time",
                "avg_over_time",
                "min_over_time",
                "max_over_time",
                "increase",
                "rate",
            ],
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
