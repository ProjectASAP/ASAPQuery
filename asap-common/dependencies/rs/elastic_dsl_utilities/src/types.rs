use serde::{Deserialize, Serialize};

/// Time range bounds resolved into epoch milliseconds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedTimeRange {
    pub field: String,
    pub gte_ms: Option<i64>,
    pub lte_ms: Option<i64>,
}

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
    pub fn as_json_str(&self) -> &'static str {
        match self {
            MetricAggType::Avg => "avg",
            MetricAggType::Min => "min",
            MetricAggType::Max => "max",
            MetricAggType::Sum => "sum",
            MetricAggType::Percentiles => "percentiles",
        }
    }

    /// Try to parse from a string key.
    pub fn from_json_str(s: &str) -> Option<Self> {
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

impl TimeRange {
    /// Parse a date-math expression into epoch milliseconds using the provided
    /// `now_ms` as reference for `now`-relative expressions.
    ///
    /// Supported forms:
    /// - `now`
    /// - `now-30s`, `now+5m`, `now-1h`, `now-2d`, `now-1w`, `now-500ms`
    /// - RFC3339 timestamps (e.g. `2026-03-22T12:34:56Z`)
    /// - Plain integer timestamps (returned as-is)
    pub fn parse_date_math(expr: &str, now_ms: i64) -> Option<i64> {
        if expr == "now" {
            return Some(now_ms);
        }

        if let Some(delta) = Self::parse_now_delta_ms(expr) {
            return now_ms.checked_add(delta);
        }

        if let Ok(v) = expr.parse::<i64>() {
            return Some(v);
        }

        chrono::DateTime::parse_from_rfc3339(expr)
            .ok()
            .map(|dt| dt.timestamp_millis())
    }

    /// Resolve `gte`/`lte` date-math strings into numeric epoch-millisecond
    /// values relative to `now_ms`.
    pub fn resolve_epoch_millis(&self, now_ms: i64) -> Option<ResolvedTimeRange> {
        let gte_ms = match &self.gte {
            Some(v) => Some(Self::parse_date_math(v, now_ms)?),
            None => None,
        };
        let lte_ms = match &self.lte {
            Some(v) => Some(Self::parse_date_math(v, now_ms)?),
            None => None,
        };

        Some(ResolvedTimeRange {
            field: self.field.clone(),
            gte_ms,
            lte_ms,
        })
    }

    fn parse_now_delta_ms(expr: &str) -> Option<i64> {
        let rest = expr.strip_prefix("now")?;
        if rest.is_empty() {
            return Some(0);
        }

        let sign_char = rest.chars().next()?;
        let sign = match sign_char {
            '+' => 1_i64,
            '-' => -1_i64,
            _ => return None,
        };

        let offset = &rest[1..];
        if offset.is_empty() {
            return None;
        }

        let digit_count = offset.chars().take_while(|c| c.is_ascii_digit()).count();
        if digit_count == 0 || digit_count == offset.len() {
            return None;
        }

        let qty = offset[..digit_count].parse::<i64>().ok()?;
        let unit = &offset[digit_count..];
        let unit_ms = match unit {
            "ms" => 1_i64,
            "s" => 1_000_i64,
            "m" => 60_000_i64,
            "h" => 3_600_000_i64,
            "d" => 86_400_000_i64,
            "w" => 604_800_000_i64,
            _ => return None,
        };

        qty.checked_mul(unit_ms)?.checked_mul(sign)
    }
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

/// Group-by shape in a grouped aggregation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupBySpec {
    /// `{"terms": {"field": "..."}}`
    Terms { field: String },
    /// `{"multi_terms": {"terms": [{"field": "..."}, ...]}}`
    MultiTerms { fields: Vec<String> },
}

/// The classified pattern of an ES DSL query, along with the extracted
/// structured components needed to route it to a sketch fast-path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EsDslQueryPattern {
    /// Template 1: metric aggregations over all data, with an optional time
    /// range and optional label filters in `bool.filter`.
    ///
    /// ES: `{ "size": 0, "query": { "range": {...} }, "aggs": { ... } }`
    SimpleAggregation {
        label_filters: Vec<LabelFilter>,
        time_range: Option<TimeRange>,
        aggregations: Vec<MetricAggregation>,
    },

    /// Template 2: grouped aggregation by one or more labels (`terms` or
    /// `multi_terms`) with nested metric aggregations, and optional
    /// `bool.filter` predicates.
    ///
    /// ES: `{ "size": 0, "aggs": { "<name>": { "terms"|"multi_terms": ...,
    ///         "aggs": { ... } } } }`
    GroupByAggregation {
        grouped_result_name: String,
        group_by: GroupBySpec,
        label_filters: Vec<LabelFilter>,
        time_range: Option<TimeRange>,
        aggregations: Vec<MetricAggregation>,
    },

    /// The query did not match any recognised sketch-acceleratable pattern.
    Unknown,
}

impl EsDslQueryPattern {
    pub fn get_time_range(&self) -> Option<&TimeRange> {
        match self {
            EsDslQueryPattern::SimpleAggregation { time_range, .. } => time_range.as_ref(),
            EsDslQueryPattern::GroupByAggregation { time_range, .. } => time_range.as_ref(),
            EsDslQueryPattern::Unknown => None,
        }
    }

    pub fn get_groupby_spec(&self) -> Option<&GroupBySpec> {
        match self {
            EsDslQueryPattern::GroupByAggregation { group_by, .. } => Some(group_by),
            _ => None,
        }
    }

    pub fn get_metric_aggs(&self) -> Option<&Vec<MetricAggregation>> {
        match self {
            EsDslQueryPattern::SimpleAggregation { aggregations, .. } => Some(aggregations),
            EsDslQueryPattern::GroupByAggregation { aggregations, .. } => Some(aggregations),
            EsDslQueryPattern::Unknown => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ResolvedTimeRange, TimeRange};

    #[test]
    fn parse_date_math_now_relative() {
        let now = 1_700_000_000_000_i64;
        assert_eq!(TimeRange::parse_date_math("now", now), Some(now));
        assert_eq!(
            TimeRange::parse_date_math("now-30s", now),
            Some(now - 30_000)
        );
        assert_eq!(
            TimeRange::parse_date_math("now+2m", now),
            Some(now + 120_000)
        );
        assert_eq!(
            TimeRange::parse_date_math("now-500ms", now),
            Some(now - 500)
        );
    }

    #[test]
    fn parse_date_math_rfc3339_and_integer() {
        let now = 0_i64;
        assert_eq!(
            TimeRange::parse_date_math("2026-03-22T00:00:00Z", now),
            Some(1_774_137_600_000)
        );
        assert_eq!(
            TimeRange::parse_date_math("1774137600000", now),
            Some(1_774_137_600_000)
        );
    }

    #[test]
    fn parse_date_math_invalid_expressions() {
        let now = 1_700_000_000_000_i64;
        assert_eq!(TimeRange::parse_date_math("now-", now), None);
        assert_eq!(TimeRange::parse_date_math("now-10q", now), None);
        assert_eq!(TimeRange::parse_date_math("yesterday", now), None);
    }

    #[test]
    fn resolve_epoch_millis_for_range() {
        let tr = TimeRange {
            field: "@timestamp".to_string(),
            gte: Some("now-30s".to_string()),
            lte: Some("now".to_string()),
        };
        let now = 1_700_000_000_000_i64;

        assert_eq!(
            tr.resolve_epoch_millis(now),
            Some(ResolvedTimeRange {
                field: "@timestamp".to_string(),
                gte_ms: Some(now - 30_000),
                lte_ms: Some(now),
            })
        );
    }
}
