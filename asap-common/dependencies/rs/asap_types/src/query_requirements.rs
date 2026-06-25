use promql_utilities::ast_matching::PromQLMatchResult;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{QueryPatternType, Statistic};
use promql_utilities::query_logics::parsing::{
    get_metric_and_spatial_filter, get_spatial_aggregation_output_labels, get_statistics_to_compute,
};
use tracing::warn;

use crate::promql_schema::PromQLSchema;
use crate::utils::normalize_spatial_filter;

/// What a query needs in order to be answered by a stored aggregation.
#[derive(Debug, Clone)]
pub struct QueryRequirements {
    /// Metric name (PromQL) or "table_name.value_column" (SQL).
    pub metric: String,
    /// One or more statistics needed.
    /// For avg this is [Sum, Count]; for everything else it is a single element.
    /// All statistics must be satisfied by aggregations sharing the same
    /// window_size and grouping_labels.
    pub statistics: Vec<Statistic>,
    /// The span of historical data the query reads, in milliseconds.
    /// None for spatial-only queries (no time range).
    pub data_range_ms: Option<u64>,
    /// GROUP BY labels expected in the query result.
    pub grouping_labels: KeyByLabelNames,
    /// Normalized label filter (produced by normalize_spatial_filter).
    pub spatial_filter_normalized: String,
    /// For `Statistic::Topk` requirements, the heavy-hitter weighting the query
    /// needs, used to disambiguate two `CountMinSketchWithHeap` configs on the
    /// same metric:
    ///   * `Some(true)`  → COUNT semantics (`count_events: true`, weight 1/event),
    ///   * `Some(false)` → SUM semantics (`count_events: false`, weight = value).
    ///
    /// `None` for non-top-k requirements (and for PromQL top-k, which does not
    /// constrain the sketch weighting); matching ignores it when `None`.
    pub topk_count_events: Option<bool>,
}

/// Build `QueryRequirements` from an already-pattern-matched PromQL query.
///
/// Shared by the query engine's capability-matching fallback and the planner's
/// AQE extractor, which independently parse and pattern-match a query before
/// calling this. `query` is used only for diagnostics on the unsupported-stats
/// warning.
pub fn build_query_requirements_promql(
    query: &str,
    match_result: &PromQLMatchResult,
    pattern_type: QueryPatternType,
    metric_schema: &PromQLSchema,
) -> Option<QueryRequirements> {
    let (metric, spatial_filter) = get_metric_and_spatial_filter(match_result);

    let statistics = get_statistics_to_compute(pattern_type, match_result)
        .map_err(|err| {
            warn!(
                query = %query,
                error = %err,
                "skipping matched query with unsupported statistics"
            );
            err
        })
        .ok()?;

    let data_range_ms = match pattern_type {
        QueryPatternType::OnlySpatial => None,
        // promql-parser supports a literal `ms` duration suffix (e.g. `[500ms]`),
        // so .num_seconds() would truncate sub-second ranges to 0.
        _ => match_result
            .get_range_duration()
            .map(|d| d.num_milliseconds() as u64),
    };

    let all_labels = metric_schema
        .get_labels(&metric)
        .cloned()
        .unwrap_or_else(KeyByLabelNames::empty);

    let grouping_labels = match pattern_type {
        // OnlyTemporal preserves all labels.
        QueryPatternType::OnlyTemporal => all_labels,
        // OnlySpatial and OneTemporalOneSpatial encode their output labels in
        // the AST's `by (...)` / `without (...)` clause.
        QueryPatternType::OnlySpatial | QueryPatternType::OneTemporalOneSpatial => {
            get_spatial_aggregation_output_labels(match_result, &all_labels)
        }
    };

    Some(QueryRequirements {
        metric,
        statistics,
        data_range_ms,
        grouping_labels,
        spatial_filter_normalized: normalize_spatial_filter(&spatial_filter),
        topk_count_events: None,
    })
}
