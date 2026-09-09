use promql_utilities::ast_matching::PromQLMatchResult;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;
use promql_utilities::query_logics::logics::promql_topk_count_events;
use promql_utilities::query_logics::parsing::{
    get_metric_and_spatial_filter, get_spatial_aggregation_output_labels,
    get_statistics_to_compute, get_topk_by_labels,
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
    /// For spatial-only queries (no time range), set to the scrape interval.
    pub data_range_ms: u64,
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
    /// PromQL top-k over a raw vector or `sum_over_time` is value-weighted
    /// (`Some(false)`); over `count_over_time` it is count-weighted
    /// (`Some(true)`). `None` is only valid for non-top-k requirements.
    pub topk_count_events: Option<bool>,
    /// For `Statistic::Topk` requirements with an explicit `by`/`without`
    /// modifier, the labels used to bucket the input for independent
    /// per-bucket ranking (see `get_topk_by_labels`) -- e.g. `Some(["job"])`
    /// for `topk by (job) (k, x)`. `None` for a bare `topk(k, x)` (single
    /// global ranking) and for every non-topk requirement. Deliberately
    /// separate from `grouping_labels`, which for topk always reports the
    /// *output* label set (all labels, unaffected by any modifier) rather
    /// than the bucketing labels (#714).
    pub topk_by_labels: Option<KeyByLabelNames>,
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
    metric_schema: &PromQLSchema,
    data_ingestion_interval_ms: u64,
) -> Option<QueryRequirements> {
    let (metric, spatial_filter) = get_metric_and_spatial_filter(match_result);

    let statistics = get_statistics_to_compute(match_result)
        .map_err(|err| {
            warn!(
                query = %query,
                error = %err,
                "skipping matched query with unsupported statistics"
            );
            err
        })
        .ok()?;

    let has_temporal_function = match_result.tokens.contains_key("function");
    let has_aggregation = match_result.tokens.contains_key("aggregation");

    let data_range_ms = if has_temporal_function {
        // promql-parser supports a literal `ms` duration suffix (e.g. `[500ms]`),
        // so .num_seconds() would truncate sub-second ranges to 0.
        match_result
            .get_range_duration()
            .map(|d| d.num_milliseconds() as u64)?
    } else {
        // OnlySpatial (no temporal component): the query has no range of its
        // own, so its data range is exactly one scrape interval.
        data_ingestion_interval_ms
    };

    let all_labels = metric_schema
        .get_labels(&metric)
        .cloned()
        .unwrap_or_else(KeyByLabelNames::empty);

    let topk_by_labels = get_topk_by_labels(match_result, &all_labels);

    let grouping_labels = if has_aggregation {
        // OnlySpatial and (collapsable, see #508) OneTemporalOneSpatial encode
        // their output labels in the AST's `by (...)` / `without (...)` clause.
        get_spatial_aggregation_output_labels(match_result, &all_labels)
    } else {
        // OnlyTemporal preserves all labels.
        all_labels
    };

    let topk_count_events = promql_topk_count_events(match_result);

    Some(QueryRequirements {
        metric,
        statistics,
        data_range_ms,
        grouping_labels,
        spatial_filter_normalized: normalize_spatial_filter(&spatial_filter),
        topk_count_events,
        topk_by_labels,
    })
}
