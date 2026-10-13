//! Patterns for spatial aggregations over a temporal function that collapse
//! into one precomputed statistic. The planner and the query engine both
//! build their pattern lists from these, so they accept the same shapes.

use std::collections::HashMap;

use serde_json::Value;

use crate::ast_matching::{PromQLPattern, PromQLPatternBuilder};
use crate::query_logics::enums::{AggregationOperator, PromQLFunction};
use crate::query_logics::logics::get_is_collapsable;

/// Token for the inner reducer of `topk(k, sum by (...) (f(m[w])))`.
pub const INNER_AGGREGATION_TOKEN: &str = "inner_aggregation";

const TEMPORAL_FUNCTIONS: [PromQLFunction; 8] = [
    PromQLFunction::Rate,
    PromQLFunction::Increase,
    PromQLFunction::SumOverTime,
    PromQLFunction::CountOverTime,
    PromQLFunction::AvgOverTime,
    PromQLFunction::MinOverTime,
    PromQLFunction::MaxOverTime,
    PromQLFunction::QuantileOverTime,
];

const SPATIAL_OPERATORS: [AggregationOperator; 7] = [
    AggregationOperator::Sum,
    AggregationOperator::Count,
    AggregationOperator::Avg,
    AggregationOperator::Quantile,
    AggregationOperator::Min,
    AggregationOperator::Max,
    AggregationOperator::Topk,
];

/// `func(metric[w])`, binding `metric`, `range_vector`, `function` and
/// `function_args`.
fn temporal_function(func: PromQLFunction) -> Option<HashMap<String, Value>> {
    PromQLPatternBuilder::function(
        vec![func.as_str()],
        vec![PromQLPatternBuilder::matrix_selector(
            PromQLPatternBuilder::metric(None, None, None, Some("metric")),
            None,
            Some("range_vector"),
        )],
        Some("function"),
        Some("function_args"),
    )
}

/// `op(func(metric[w]))`: one narrow pattern per collapsable (function, op)
/// pair (see `get_is_collapsable`). `sum(min_over_time(x[5m]))` can't be
/// served by one precomputed statistic the way `sum(sum_over_time(x[5m]))`
/// can, and a broad any-op-over-any-function pattern would match it and
/// silently drop the outer aggregation instead of rejecting the query.
pub fn build_one_temporal_one_spatial_patterns() -> Vec<PromQLPattern> {
    TEMPORAL_FUNCTIONS
        .into_iter()
        .flat_map(|func| {
            SPATIAL_OPERATORS
                .into_iter()
                .filter(move |&op| get_is_collapsable(func, op))
                .map(move |op| {
                    PromQLPattern::new(PromQLPatternBuilder::aggregation(
                        vec![op.as_str()],
                        temporal_function(func),
                        None,
                        None,
                        None,
                        Some("aggregation"),
                    ))
                })
        })
        .collect()
}

/// `topk(k, sum by (l) (func(metric[w])))`, one per `func` collapsable under
/// both `sum` and `topk`: such a query collapses into one
/// CountMinSketchWithHeap keyed by `l`.
pub fn build_topk_of_sum_patterns() -> Vec<PromQLPattern> {
    TEMPORAL_FUNCTIONS
        .into_iter()
        .filter(|&func| {
            get_is_collapsable(func, AggregationOperator::Sum)
                && get_is_collapsable(func, AggregationOperator::Topk)
        })
        .map(|func| {
            PromQLPattern::new(PromQLPatternBuilder::aggregation(
                vec![AggregationOperator::Topk.as_str()],
                PromQLPatternBuilder::aggregation(
                    vec![AggregationOperator::Sum.as_str()],
                    temporal_function(func),
                    None,
                    None,
                    None,
                    Some(INNER_AGGREGATION_TOKEN),
                ),
                None,
                None,
                None,
                Some("aggregation"),
            ))
        })
        .collect()
}
