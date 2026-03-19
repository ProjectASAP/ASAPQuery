use serde_json::Value;

use crate::{
    parsing::{
        extract_group_by_agg, extract_metric_aggs, extract_predicates_from_query,
    },
    types::EsDslQueryPattern,
};

/// Classify a parsed ES DSL query `Value` into one of the recognised
/// sketch-acceleratable patterns (or `Unknown` if it does not match any).
///
/// The classification logic follows the templates documented in
/// `supported_es_queries.md`:
///
/// - **Template 1** (`SimpleAggregation`): `size=0`, top-level metric `aggs`
///   (avg/min/max/sum/percentiles), optional bare `range` query.
/// - **Template 2** (`GroupByAggregation`): `size=0`, one top-level grouped
///   aggregation (`terms` or `multi_terms`) with nested metric sub-aggregations,
///   optional `bool.filter` predicates (term labels + optional range).
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

    let (label_filters, time_range) = match query {
        None => (Vec::new(), None),
        Some(q) => match extract_predicates_from_query(q) {
            Some(predicates) => predicates,
            None => return EsDslQueryPattern::Unknown,
        },
    };

    // ------------------------------------------------------------------
    // Template 2: grouped aggregation (`terms` or `multi_terms`).
    // ------------------------------------------------------------------
    if let Some((grouped_result_name, group_by, aggregations)) = extract_group_by_agg(aggs) {
        return EsDslQueryPattern::GroupByAggregation {
            grouped_result_name,
            group_by,
            label_filters,
            time_range,
            aggregations,
        };
    }

    // ------------------------------------------------------------------
    // Templates 1 & 2 require metric-only top-level aggregations.
    // ------------------------------------------------------------------
    let aggregations = match extract_metric_aggs(aggs) {
        Some(a) => a,
        None => return EsDslQueryPattern::Unknown,
    };

    EsDslQueryPattern::SimpleAggregation {
        label_filters,
        time_range,
        aggregations,
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
    use crate::types::{GroupBySpec, LabelFilter, MetricAggType};
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
            EsDslQueryPattern::SimpleAggregation {
                label_filters,
                time_range,
                aggregations,
            } => {
                assert!(label_filters.is_empty());
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
            EsDslQueryPattern::SimpleAggregation {
                label_filters,
                time_range,
                aggregations,
            } => {
                assert!(label_filters.is_empty());
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
            EsDslQueryPattern::SimpleAggregation {
                label_filters,
                time_range,
                aggregations,
            } => {
                assert!(label_filters.is_empty());
                assert!(time_range.is_none());
                assert_eq!(aggregations.len(), 1);
                assert_eq!(aggregations[0].agg_type, MetricAggType::Percentiles);
                assert_eq!(aggregations[0].field, "latency_ms");
            }
            other => panic!("Expected SimpleAggregation, got {:?}", other),
        }
    }

    #[test]
    fn test_simple_agg_with_bool_filter_predicates() {
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
            EsDslQueryPattern::SimpleAggregation {
                label_filters,
                time_range,
                aggregations,
            } => {
                assert_eq!(label_filters.len(), 2);
                assert_eq!(label_filters[0], LabelFilter { field: "service".into(), value: "frontend".into() });
                assert_eq!(label_filters[1], LabelFilter { field: "env".into(), value: "staging".into() });
                assert!(time_range.is_some());
                assert_eq!(aggregations.len(), 1);
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
    // Template 2 — GroupBy Aggregation
    // -----------------------------------------------------------------------

    #[test]
    fn test_t2_groupby_terms_with_filters_and_range() {
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
                "grouped_result": {
                    "terms": {
                        "field": "service.keyword"
                    },
                    "aggs": {
                        "avg_latency": { "avg": { "field": "latency_ms" } }
                    }
                }
            }
        });

        let pattern = classify(&query);
        match pattern {
            EsDslQueryPattern::GroupByAggregation {
                grouped_result_name,
                group_by,
                label_filters,
                time_range,
                aggregations,
            } => {
                assert_eq!(grouped_result_name, "grouped_result");
                assert_eq!(group_by, GroupBySpec::Terms { field: "service".into() });
                assert_eq!(label_filters[0].field, "service");
                assert_eq!(label_filters[0].value, "frontend");
                assert_eq!(label_filters[1].field, "env");
                assert_eq!(label_filters[1].value, "staging");
                let tr = time_range.unwrap();
                assert_eq!(tr.field, "@timestamp");
                assert_eq!(aggregations.len(), 1);
            }
            other => panic!("Expected GroupByAggregation, got {:?}", other),
        }
    }

    #[test]
    fn test_t2_groupby_multi_terms() {
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
                "grouped_result": {
                    "multi_terms": {
                        "terms": [
                            { "field": "service.keyword" },
                            { "field": "region.keyword" }
                        ]
                    },
                    "aggs": {
                        "p99_latency": { "max": { "field": "latency_ms" } }
                    }
                }
            }
        });

        let pattern = classify(&query);
        match pattern {
            EsDslQueryPattern::GroupByAggregation {
                grouped_result_name,
                group_by,
                label_filters,
                time_range,
                aggregations,
            } => {
                assert_eq!(grouped_result_name, "grouped_result");
                assert_eq!(
                    group_by,
                    GroupBySpec::MultiTerms {
                        fields: vec!["service".into(), "region".into()]
                    }
                );
                assert_eq!(label_filters[0], LabelFilter { field: "env".into(), value: "staging".into() });
                assert!(time_range.is_none());
                assert_eq!(aggregations.len(), 1);
            }
            other => panic!("Expected GroupByAggregation, got {:?}", other),
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
                "by_service": {
                    "terms": { "field": "service" },
                    "aggs": {
                        "foo": { "median": { "field": "latency" } }
                    }
                }
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
