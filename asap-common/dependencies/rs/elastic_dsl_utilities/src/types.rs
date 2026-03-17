use serde::{Deserialize, Serialize};

/// The metric aggregation function type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricAggType {
    Avg,
    Min,
    Max,
    Sum,
    Percentiles,
}

impl MetricAggType {
    /// Returns the JSON key name for this aggregation type.
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricAggType::Avg => "avg",
            MetricAggType::Min => "min",
            MetricAggType::Max => "max",
            MetricAggType::Sum => "sum",
            MetricAggType::Percentiles => "percentiles",
        }
    }

    /// Try to parse from a string key.
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "avg" => Some(MetricAggType::Avg),
            "min" => Some(MetricAggType::Min),
            "max" => Some(MetricAggType::Max),
            "sum" => Some(MetricAggType::Sum),
            "percentiles" => Some(MetricAggType::Percentiles),
            _ => None,
        }
    }
}

/// A simple equality filter on a label (string-valued field).
/// The `.keyword` suffix is stripped from the field name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelFilter {
    pub field: String,
    pub value: String,
}

/// An optional time range applied to a timestamp field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRange {
    pub field: String,
    pub gte: Option<String>,
    pub lte: Option<String>,
}

/// A single metric aggregation extracted from an ES query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricAggregation {
    /// The top-level aggregation result key (the name given by the user).
    pub result_name: String,
    pub agg_type: MetricAggType,
    /// The document field being aggregated over.
    pub field: String,
    pub params: Option<serde_json::Value>, // Optional additional parameters (e.g. percentiles values)
}

/// One bucket in a batched-filter (multi-bucket) aggregation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketSpec {
    pub bucket_name: String,
    pub filter: LabelFilter,
}

/// The classified pattern of an ES DSL query, along with the extracted
/// structured components needed to route it to a sketch fast-path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EsDslQueryPattern {
    /// Template 1: metric aggregations over all data, with an optional time
    /// range filter.
    ///
    /// ES: `{ "size": 0, "query": { "range": {...} }, "aggs": { ... } }`
    SimpleAggregation {
        time_range: Option<TimeRange>,
        aggregations: Vec<MetricAggregation>,
    },

    /// Template 2: metric aggregations with a label equality filter plus an
    /// optional time range, expressed as a bool filter.
    ///
    /// ES: `{ "size": 0, "query": { "bool": { "filter": [...] } }, "aggs": { ... } }`
    FilteredAggregation {
        label_filters: Vec<LabelFilter>,
        time_range: Option<TimeRange>,
        aggregations: Vec<MetricAggregation>,
    },

    /// Template 3: a single top-level bucket aggregation that groups documents
    /// into named buckets via per-bucket term filters, with nested metric
    /// sub-aggregations.
    ///
    /// ES: `{ "size": 0, "aggs": { "<name>": { "filters": { "filters": {...} },
    ///         "aggs": { ... } } } }`
    FilteredAggregationBatched {
        /// The name of the outer (bucket) aggregation.
        result_name: String,
        buckets: Vec<BucketSpec>,
        time_range: Option<TimeRange>,
        aggregations: Vec<MetricAggregation>,
    },

    /// The query did not match any recognised sketch-acceleratable pattern.
    Unknown,
}
