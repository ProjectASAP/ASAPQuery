use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;

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
}
