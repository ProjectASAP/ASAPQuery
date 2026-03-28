use serde_json::Value;

use crate::types::{GroupBySpec, LabelFilter, MetricAggType, MetricAggregation, TimeRange};

// ---------------------------------------------------------------------------
// Metric aggregation helpers
// ---------------------------------------------------------------------------

/// Try to extract a list of metric aggregations from the top-level `"aggs"`
/// object of a query.  Returns `None` if *any* aggregation entry is not one of
/// the recognised metric types (avg / min / max / sum / percentiles).
pub fn extract_metric_aggs(aggs: &Value) -> Option<Vec<MetricAggregation>> {
    let obj = aggs.as_object()?;
    if obj.is_empty() {
        return None;
    }

    let mut result = Vec::with_capacity(obj.len());
    for (result_name, agg_body) in obj {
        // Each aggregation body is an object that should contain exactly one
        // recognised metric aggregation key.
        let body_obj = agg_body.as_object()?;
        let mut found = None;
        for (key, inner) in body_obj {
            if let Some(agg_type) = MetricAggType::from_json_str(key) {
                let field = inner.get("field")?.as_str()?.to_owned();
                let kwargs_map = inner
                    .as_object()?
                    .iter()
                    .filter(|(k, _)| *k != "field")
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let kwargs = serde_json::Value::Object(kwargs_map);
                found = Some(MetricAggregation {
                    result_name: result_name.clone(),
                    agg_type,
                    field,
                    params: if kwargs.as_object().is_some_and(|o| o.is_empty()) {
                        None
                    } else {
                        Some(kwargs)
                    },
                });
                break;
            }
        }
        result.push(found?);
    }
    Some(result)
}

// ---------------------------------------------------------------------------
// Time range helpers
// ---------------------------------------------------------------------------

/// Try to extract a `TimeRange` from a bare `{"range": {"<field>": {...}}}`
/// query value.  Accepts either string or numeric values for gte/lte.
pub fn extract_time_range(query: &Value) -> Option<TimeRange> {
    let range_obj = query.get("range")?.as_object()?;
    // There should be exactly one field entry in the range object.
    if range_obj.len() != 1 {
        return None;
    }
    let (field, bounds) = range_obj.iter().next()?;
    let gte = bounds.get("gte").and_then(value_to_string);
    let lte = bounds.get("lte").and_then(value_to_string);
    Some(TimeRange {
        field: field.clone(),
        gte,
        lte,
    })
}

fn value_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Term / label-filter helpers
// ---------------------------------------------------------------------------

/// Strip the `.keyword` suffix from a field name, if present.
fn strip_keyword_suffix(field: &str) -> &str {
    field.strip_suffix(".keyword").unwrap_or(field)
}

/// Try to extract a `LabelFilter` from a single `"term"` query object.
///
/// Handles both the opensearch-dsl long form:
/// ```json
/// { "term": { "field": { "value": "val" } } }
/// ```
/// and the ES shorthand:
/// ```json
/// { "term": { "field": "val" } }
/// ```
pub fn extract_label_filter_from_term(term_query: &Value) -> Option<LabelFilter> {
    let term_obj = term_query.get("term")?.as_object()?;
    if term_obj.len() != 1 {
        return None;
    }
    let (raw_field, field_value) = term_obj.iter().next()?;
    let field = strip_keyword_suffix(raw_field).to_owned();
    let value = if let Some(s) = field_value.as_str() {
        // Shorthand: "field": "value"
        s.to_owned()
    } else if let Some(inner) = field_value.as_object() {
        // Long form: "field": { "value": "..." }
        inner.get("value")?.as_str()?.to_owned()
    } else {
        return None;
    };
    Some(LabelFilter { field, value })
}

// ---------------------------------------------------------------------------
// Bool filter helpers
// ---------------------------------------------------------------------------

/// Try to extract a list of label filters (and optionally a time range) from a
/// `{"bool": {"filter": [...]}}` query structure.
///
/// The `filter` array must contain at least a term query, and may also contain
/// a range query.  Additional (unrecognised) entries in the array cause this
/// function to return `None`.
pub fn extract_label_filters(query: &Value) -> Option<(Vec<LabelFilter>, Option<TimeRange>)> {
    let filter_clauses = query.get("bool")?.get("filter")?;

    // The filter value may be an array (multiple clauses) or a single object.
    let clauses: Vec<&Value> = if let Some(arr) = filter_clauses.as_array() {
        arr.iter().collect()
    } else if filter_clauses.is_object() {
        vec![filter_clauses]
    } else {
        return None;
    };

    let mut label_filters: Vec<LabelFilter> = Vec::new();
    let mut time_range: Option<TimeRange> = None;

    for clause in clauses {
        if clause.get("term").is_some() {
            label_filters.push(extract_label_filter_from_term(clause)?);
        } else if clause.get("range").is_some() {
            if time_range.is_some() {
                return None;
            }
            time_range = Some(extract_time_range(clause)?);
        } else {
            // Unknown clause type in the filter.
            return None;
        }
    }

    Some((label_filters, time_range))
}

// ---------------------------------------------------------------------------
// Query predicate helpers
// ---------------------------------------------------------------------------

/// Extract optional predicates from top-level query:
/// - `{"range": ...}` -> `(label_filters=[], time_range=Some(...))`
/// - `{"bool": {"filter": ...}}` -> label filters + optional time range
/// - `None`/`null` query is represented by caller as `(vec![], None)`.
pub fn extract_predicates_from_query(
    query: &Value,
) -> Option<(Vec<LabelFilter>, Option<TimeRange>)> {
    if query.is_null() {
        return Some((Vec::new(), None));
    }

    if let Some(time_range) = extract_time_range(query) {
        return Some((Vec::new(), Some(time_range)));
    }

    if query.get("bool").is_some() {
        return extract_label_filters(query);
    }

    None
}

// ---------------------------------------------------------------------------
// Group-by helpers
// ---------------------------------------------------------------------------

/// Try to extract a grouped aggregation from top-level `"aggs"` object.
///
/// Expected shape:
/// ```json
/// {
///   "aggs": {
///     "<grouped_result>": {
///       "terms": { "field": "<label>.keyword" },
///       "aggs": { ...metric aggs... }
///     }
///   }
/// }
/// ```
/// or
/// ```json
/// {
///   "aggs": {
///     "<grouped_result>": {
///       "multi_terms": {
///         "terms": [{"field": "a.keyword"}, {"field": "b.keyword"}]
///       },
///       "aggs": { ...metric aggs... }
///     }
///   }
/// }
/// ```
pub fn extract_group_by_agg(aggs: &Value) -> Option<(String, GroupBySpec, Vec<MetricAggregation>)> {
    let obj = aggs.as_object()?;
    // There must be exactly one top-level aggregation entry.
    if obj.len() != 1 {
        return None;
    }
    let (grouped_result_name, agg_body) = obj.iter().next()?;

    let group_by = if let Some(terms_obj) = agg_body.get("terms") {
        let raw_field = terms_obj.get("field")?.as_str()?;
        GroupBySpec::Terms {
            field: strip_keyword_suffix(raw_field).to_owned(),
        }
    } else if let Some(multi_terms_obj) = agg_body.get("multi_terms") {
        let terms = multi_terms_obj.get("terms")?.as_array()?;
        if terms.is_empty() {
            return None;
        }
        let mut fields = Vec::with_capacity(terms.len());
        for term in terms {
            let raw_field = term.get("field")?.as_str()?;
            fields.push(strip_keyword_suffix(raw_field).to_owned());
        }
        GroupBySpec::MultiTerms { fields }
    } else {
        return None;
    };

    // The nested "aggs" holds the metric sub-aggregations.
    let nested_aggs = agg_body.get("aggs").unwrap_or(&Value::Null);
    let metric_aggs = extract_metric_aggs(nested_aggs)?;

    Some((grouped_result_name.clone(), group_by, metric_aggs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_metric_aggs_basic() {
        let aggs = json!({
            "avg_latency": { "avg": { "field": "latency_ms" } },
            "max_latency": { "max": { "field": "latency_ms" } },
            "p95_latency": { "percentiles": { "field": "latency_ms", "percents": [95] } }
        });
        let result = extract_metric_aggs(&aggs).unwrap();
        assert_eq!(result.len(), 3);
        let avg = result
            .iter()
            .find(|a| a.result_name == "avg_latency")
            .unwrap();
        assert_eq!(avg.agg_type, MetricAggType::Avg);
        assert_eq!(avg.field, "latency_ms");
        let p95 = result
            .iter()
            .find(|a| a.result_name == "p95_latency")
            .unwrap();
        assert_eq!(p95.agg_type, MetricAggType::Percentiles);
        assert_eq!(p95.field, "latency_ms");
        assert_eq!(
            p95.params.as_ref().unwrap().get("percents").unwrap(),
            &json!([95])
        );

        let avg = result
            .iter()
            .find(|a| a.result_name == "avg_latency")
            .unwrap();
        assert!(avg.params.is_none());
    }

    #[test]
    fn test_extract_metric_aggs_rejects_unknown_type() {
        let aggs = json!({
            "by_service": { "terms": { "field": "service" } }
        });
        assert!(extract_metric_aggs(&aggs).is_none());
    }

    #[test]
    fn test_extract_time_range() {
        let query = json!({
            "range": {
                "@timestamp": { "gte": "now-30s", "lte": "now" }
            }
        });
        let tr = extract_time_range(&query).unwrap();
        assert_eq!(tr.field, "@timestamp");
        assert_eq!(tr.gte.as_deref(), Some("now-30s"));
        assert_eq!(tr.lte.as_deref(), Some("now"));
    }

    #[test]
    fn test_extract_label_filter_long_form() {
        let term = json!({ "term": { "service.keyword": { "value": "frontend" } } });
        let f = extract_label_filter_from_term(&term).unwrap();
        assert_eq!(f.field, "service");
        assert_eq!(f.value, "frontend");
    }

    #[test]
    fn test_extract_label_filter_shorthand() {
        let term = json!({ "term": { "env": "production" } });
        let f = extract_label_filter_from_term(&term).unwrap();
        assert_eq!(f.field, "env");
        assert_eq!(f.value, "production");
    }

    #[test]
    fn test_extract_bool_filter_term_and_range() {
        let query = json!({
            "bool": {
                "filter": [
                    { "term": { "service.keyword": { "value": "frontend" } } },
                    { "term": { "env.keyword": { "value": "production" } } },
                    { "range": { "@timestamp": { "gte": "now-30s", "lte": "now" } } }
                ]
            }
        });
        let (lf, tr) = extract_label_filters(&query).unwrap();
        assert_eq!(lf[0].field, "service");
        assert_eq!(lf[0].value, "frontend");
        assert_eq!(lf[1].field, "env");
        assert_eq!(lf[1].value, "production");
        let tr = tr.unwrap();
        assert_eq!(tr.field, "@timestamp");
    }

    #[test]
    fn test_extract_bool_filter_term_only() {
        let query = json!({
            "bool": {
                "filter": [
                    { "term": { "env": "staging" } }
                ]
            }
        });
        let (lf, tr) = extract_label_filters(&query).unwrap();
        assert_eq!(lf[0].field, "env");
        assert_eq!(lf[0].value, "staging");
        assert!(tr.is_none());
    }

    #[test]
    fn test_extract_bool_filter_single_object() {
        // filter as a plain object (not array)
        let query = json!({
            "bool": {
                "filter": { "term": { "region": "us-east-1" } }
            }
        });
        let (lf, tr) = extract_label_filters(&query).unwrap();
        assert_eq!(lf[0].field, "region");
        assert_eq!(lf[0].value, "us-east-1");
        assert!(tr.is_none());
    }

    #[test]
    fn test_extract_batched_filters() {
        let aggs = json!({
            "by_service": {
                "terms": {
                    "field": "service.keyword"
                },
                "aggs": {
                    "avg_latency": { "avg": { "field": "latency_ms" } }
                }
            }
        });
        let (name, group_by, metric_aggs) = extract_group_by_agg(&aggs).unwrap();
        assert_eq!(name, "by_service");
        assert_eq!(
            group_by,
            GroupBySpec::Terms {
                field: "service".to_string()
            }
        );
        assert_eq!(metric_aggs.len(), 1);
        assert_eq!(metric_aggs[0].agg_type, MetricAggType::Avg);
    }

    #[test]
    fn test_extract_group_by_multi_terms() {
        let aggs = json!({
            "by_service_region": {
                "multi_terms": {
                    "terms": [
                        { "field": "service.keyword" },
                        { "field": "region.keyword" }
                    ]
                },
                "aggs": {
                    "p95_latency": { "percentiles": { "field": "latency_ms", "percents": [95] } }
                }
            }
        });
        let (_, group_by, metric_aggs) = extract_group_by_agg(&aggs).unwrap();
        assert_eq!(
            group_by,
            GroupBySpec::MultiTerms {
                fields: vec!["service".to_string(), "region".to_string()]
            }
        );
        assert_eq!(metric_aggs.len(), 1);
        assert_eq!(metric_aggs[0].agg_type, MetricAggType::Percentiles);
    }

    #[test]
    fn test_extract_predicates_from_range() {
        let query = json!({
            "range": {
                "@timestamp": { "gte": "now-30s", "lte": "now" }
            }
        });
        let (filters, time_range) = extract_predicates_from_query(&query).unwrap();
        assert!(filters.is_empty());
        let tr = time_range.unwrap();
        assert_eq!(tr.field, "@timestamp");
    }
}
