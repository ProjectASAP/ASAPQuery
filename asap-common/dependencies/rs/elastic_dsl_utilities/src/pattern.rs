use serde_json::Value;

use crate::{
    parsing::{
        extract_batched_filters, extract_label_filters, extract_metric_aggs,
        extract_time_range,
    },
    types::EsDslQueryPattern,
};

/// Classify a parsed ES DSL query `Value` into one of the recognised
/// sketch-acceleratable patterns (or `Unknown` if it does not match any).
///
/// The classification logic follows the three templates documented in
/// `supported_es_queries.md`:
///
/// - **Template 1** (`SimpleAggregation`): `size=0`, top-level metric `aggs`
///   (avg/min/max/sum/percentiles), optional bare `range` query.
/// - **Template 2** (`FilteredAggregation`): `size=0`, top-level metric `aggs`
///   (avg/min/max/sum/percentiles),
///   `bool.filter` query combining a `term` label filter and an optional
///   `range` time filter.
/// - **Template 3** (`FilteredAggregationBatched`): `size=0`, single top-level
///   `filters` bucket aggregation with named buckets, nested metric sub-aggs,
///   optional bare `range` top-level query.
/// 
/// TODO: More robust parsing logic and complex pattern support (e.g. generic pattern building, structured AST, etc).
pub fn classify(value: &Value) -> EsDslQueryPattern {
    // Gate: size must be explicitly 0.
    match value.get("size") {
        Some(Value::Number(n)) => {
            if n.as_u64() != Some(0) {
                return EsDslQueryPattern::Unknown;
            }
        }
        _ => return EsDslQueryPattern::Unknown,
    }
        

    let aggs = value.get("aggs").unwrap_or(&Value::Null);
    let query = value.get("query");

    // ------------------------------------------------------------------
    // Template 3: batched filters aggregation.
    // ------------------------------------------------------------------
    if let Some((result_name, buckets, aggregations)) = extract_batched_filters(aggs) {
        // Allow an optional top-level range query alongside the batched aggs.
        let time_range = query.and_then(|q| extract_time_range(q));
        // If there *is* a query but it's not a range, reject the match.
        if query.is_some() && time_range.is_none() && query != Some(&Value::Null) {
            // Non-range query next to batched filters — not a supported pattern.
        } else {
            return EsDslQueryPattern::FilteredAggregationBatched {
                result_name,
                buckets,
                time_range,
                aggregations,
            };
        }
    }

    // ------------------------------------------------------------------
    // Templates 1 & 2 require metric-only top-level aggregations.
    // ------------------------------------------------------------------
    let aggregations = match extract_metric_aggs(aggs) {
        Some(a) => a,
        None => return EsDslQueryPattern::Unknown,
    };

    match query {
        // No query clause at all -> Template 1 without time range.
        None => EsDslQueryPattern::SimpleAggregation {
            time_range: None,
            aggregations,
        },

        Some(q) => {
            // Template 2: bool.filter with term (+ optional range).
            if let Some((label_filters, time_range)) = extract_label_filters(q) {
                return EsDslQueryPattern::FilteredAggregation {
                    label_filters,
                    time_range,
                    aggregations,
                };
            }

            // Template 1: bare range query.
            if let Some(time_range) = extract_time_range(q) {
                return EsDslQueryPattern::SimpleAggregation {
                    time_range: Some(time_range),
                    aggregations,
                };
            }

            // Query is present but doesn't match any supported form.
            EsDslQueryPattern::Unknown
        }
    }
}

/// Parse a raw JSON string as an ES DSL query and classify it into a sketch-acceleratable pattern, returning the extracted structured components if successful.
///
/// Returns a `serde_json::Error` if the input is not valid JSON.
pub fn parse_and_classify(json: &str) -> Result<EsDslQueryPattern, serde_json::Error> {
    let value: Value = serde_json::from_str(json)?;
    Ok(classify(&value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{LabelFilter, MetricAggType};
    use serde_json::json;

    // -----------------------------------------------------------------------
    // Template 1 — Simple Aggregation
    // -----------------------------------------------------------------------

    #[test]
    fn test_t1_simple_agg_with_time_range() {
        let query = json!({
            "size": 0,
            "query": {
                "range": {
                    "@timestamp": { "gte": "now-30s", "lte": "now" }
                }
            },
            "aggs": {
                "avg_latency": { "avg": { "field": "latency_ms" } },
                "max_latency": { "max": { "field": "latency_ms" } }
            }
        });

        let pattern = classify(&query);
        match pattern {
            EsDslQueryPattern::SimpleAggregation { time_range, aggregations } => {
                let tr = time_range.unwrap();
                assert_eq!(tr.field, "@timestamp");
                assert_eq!(tr.gte.as_deref(), Some("now-30s"));
                assert_eq!(tr.lte.as_deref(), Some("now"));
                assert_eq!(aggregations.len(), 2);
            }
            other => panic!("Expected SimpleAggregation, got {:?}", other),
        }
    }

    #[test]
    fn test_t1_simple_agg_no_query() {
        let query = json!({
            "size": 0,
            "aggs": {
                "total_bytes": { "sum": { "field": "bytes" } }
            }
        });

        let pattern = classify(&query);
        match pattern {
            EsDslQueryPattern::SimpleAggregation { time_range, aggregations } => {
                assert!(time_range.is_none());
                assert_eq!(aggregations.len(), 1);
                assert_eq!(aggregations[0].agg_type, MetricAggType::Sum);
                assert_eq!(aggregations[0].field, "bytes");
            }
            other => panic!("Expected SimpleAggregation, got {:?}", other),
        }
    }

    #[test]
    fn test_t1_percentiles_aggregation() {
        let query = json!({
            "size": 0,
            "aggs": {
                "p95_latency": { "percentiles": { "field": "latency_ms", "percents": [95] } }
            }
        });

        let pattern = classify(&query);
        match pattern {
            EsDslQueryPattern::SimpleAggregation { time_range, aggregations } => {
                assert!(time_range.is_none());
                assert_eq!(aggregations.len(), 1);
                assert_eq!(aggregations[0].agg_type, MetricAggType::Percentiles);
                assert_eq!(aggregations[0].field, "latency_ms");
            }
            other => panic!("Expected SimpleAggregation, got {:?}", other),
        }
    }

    #[test]
    fn test_neg_size_absent_is_unknown() {
        let query = json!({
            "aggs": {
                "min_val": { "min": { "field": "response_time" } }
            }
        });

        assert_eq!(classify(&query), EsDslQueryPattern::Unknown);
    }

    // -----------------------------------------------------------------------
    // Template 2 — Filtered Aggregation
    // -----------------------------------------------------------------------

    #[test]
    fn test_t2_filtered_agg_term_and_range() {
        let query = json!({
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        { "term": { "service.keyword": { "value": "frontend" } } },
                        { "term": { "env.keyword": { "value": "staging" } } },
                        { "range": { "@timestamp": { "gte": "now-30s", "lte": "now" } } }
                    ]
                }
            },
            "aggs": {
                "avg_latency": { "avg": { "field": "latency_ms" } }
            }
        });

        let pattern = classify(&query);
        match pattern {
            EsDslQueryPattern::FilteredAggregation { label_filters, time_range, aggregations } => {
                assert_eq!(label_filters[0].field, "service");
                assert_eq!(label_filters[0].value, "frontend");
                assert_eq!(label_filters[1].field, "env");
                assert_eq!(label_filters[1].value, "staging");
                let tr = time_range.unwrap();
                assert_eq!(tr.field, "@timestamp");
                assert_eq!(aggregations.len(), 1);
            }
            other => panic!("Expected FilteredAggregation, got {:?}", other),
        }
    }

    #[test]
    fn test_t2_filtered_agg_term_only() {
        let query = json!({
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        { "term": { "env": "staging" } }
                    ]
                }
            },
            "aggs": {
                "p99_latency": { "max": { "field": "latency_ms" } }
            }
        });

        let pattern = classify(&query);
        match pattern {
            EsDslQueryPattern::FilteredAggregation { label_filters, time_range, aggregations } => {
                assert_eq!(label_filters[0], LabelFilter { field: "env".into(), value: "staging".into() });
                assert!(time_range.is_none());
                assert_eq!(aggregations.len(), 1);
            }
            other => panic!("Expected FilteredAggregation, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Template 3 — Filtered Aggregation Batched
    // -----------------------------------------------------------------------

    #[test]
    fn test_t3_batched_filters() {
        let query = json!({
            "size": 0,
            "aggs": {
                "by_service": {
                    "filters": {
                        "filters": {
                            "frontend": { "term": { "service.keyword": { "value": "frontend" } } },
                            "backend":  { "term": { "service.keyword": { "value": "backend" } } }
                        }
                    },
                    "aggs": {
                        "avg_latency": { "avg": { "field": "latency_ms" } }
                    }
                }
            }
        });

        let pattern = classify(&query);
        match pattern {
            EsDslQueryPattern::FilteredAggregationBatched { result_name, buckets, time_range, aggregations } => {
                assert_eq!(result_name, "by_service");
                assert_eq!(buckets.len(), 2);
                assert!(time_range.is_none());
                assert_eq!(aggregations.len(), 1);
                assert_eq!(aggregations[0].agg_type, MetricAggType::Avg);
            }
            other => panic!("Expected FilteredAggregationBatched, got {:?}", other),
        }
    }

    #[test]
    fn test_t3_batched_filters_with_time_range() {
        let query = json!({
            "size": 0,
            "query": {
                "range": {
                    "@timestamp": { "gte": "now-1m", "lte": "now" }
                }
            },
            "aggs": {
                "by_region": {
                    "filters": {
                        "filters": {
                            "us-east": { "term": { "region": "us-east-1" } },
                            "us-west": { "term": { "region": "us-west-2" } }
                        }
                    },
                    "aggs": {
                        "total_requests": { "sum": { "field": "request_count" } }
                    }
                }
            }
        });

        let pattern = classify(&query);
        match pattern {
            EsDslQueryPattern::FilteredAggregationBatched { time_range, aggregations, .. } => {
                let tr = time_range.unwrap();
                assert_eq!(tr.field, "@timestamp");
                assert_eq!(tr.gte.as_deref(), Some("now-1m"));
                assert_eq!(aggregations[0].agg_type, MetricAggType::Sum);
            }
            other => panic!("Expected FilteredAggregationBatched, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Negative cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_neg_size_nonzero_is_unknown() {
        let query = json!({
            "size": 10,
            "aggs": {
                "avg_val": { "avg": { "field": "cpu" } }
            }
        });
        assert_eq!(classify(&query), EsDslQueryPattern::Unknown);
    }

    #[test]
    fn test_neg_unknown_agg_type_is_unknown() {
        let query = json!({
            "size": 0,
            "aggs": {
                "by_service": { "terms": { "field": "service" } }
            }
        });
        assert_eq!(classify(&query), EsDslQueryPattern::Unknown);
    }

    #[test]
    fn test_neg_unsupported_query_type_is_unknown() {
        // A match query in the top-level query is not a supported pattern.
        let query = json!({
            "size": 0,
            "query": {
                "match": { "message": "error" }
            },
            "aggs": {
                "count": { "sum": { "field": "bytes" } }
            }
        });
        assert_eq!(classify(&query), EsDslQueryPattern::Unknown);
    }

    #[test]
    fn test_parse_and_classify_roundtrip() {
        let json = r#"{"size":0,"aggs":{"avg_cpu":{"avg":{"field":"cpu_usage"}}}}"#;
        let result = parse_and_classify(json).unwrap();
        assert!(matches!(result, EsDslQueryPattern::SimpleAggregation { .. }));
    }

    #[test]
    fn test_parse_and_classify_invalid_json() {
        assert!(parse_and_classify("{invalid}").is_err());
    }
}
