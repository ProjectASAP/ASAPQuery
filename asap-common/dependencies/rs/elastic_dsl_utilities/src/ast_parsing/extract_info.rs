use crate::ast_parsing::query_info::{
    AggregationType, ElasticDSLQueryInfo, FieldName, GroupBySpec, Predicate, TermValue,
};
use crate::helpers::strip_keyword_suffix;
use opensearch_dsl::{self as dsl};
use serde_json;

pub fn extract_query_info(query: &str) -> Option<ElasticDSLQueryInfo> {
    // Main entry point for extracting relevant information from the parsed query pattern. This function would contain the core logic for traversing the AST and applying rules to determine the target field, predicates, group by specifications, and aggregation type.
    let search_request = parse_query_to_ast(query)?;
    walk_ast_and_extract_info(&search_request)
 }

pub fn parse_query_to_ast(query: &str) -> Option<dsl::Search> {
    let search_request = serde_json::from_str(query).ok()?;
    search_request
}

pub fn walk_ast_and_extract_info(ast: &dsl::Search) -> Option<ElasticDSLQueryInfo> {
    // Traverses the AST and extracts relevant information for answering sketchable aggregations within ASAPQuery.
    // This would involve traversing the AST nodes and applying logic to determine query patterns, labels, statistics, etc.
    let predicates = match ast.query.clone()? {
        dsl::Query::Bool(bool_query) => {
            // Extract information from the bool query
            walk_bool_query_and_extract_info(&bool_query)
        }
        _ => {
            // Handle other query types
            Vec::new() // Return an empty vector of predicates for unsupported query types
        }
    };
    let (target_field, aggregation_type, group_by_spec) =
        walk_aggregations_and_extract_info(&ast.aggs)?;
    Some(ElasticDSLQueryInfo::new(
        target_field,
        predicates,
        group_by_spec,
        aggregation_type,
    ))
}

fn walk_bool_query_and_extract_info(bool_query: &dsl::BoolQuery) -> Vec<Predicate> {
    // Placeholder for walking the filter context of the AST and extracting relevant information
    // This would involve traversing the filter nodes and applying logic to determine label filters, time ranges, etc.
    let dsl::QueryCollection(filters) = bool_query.filter.clone();
    let mut predicates = Vec::new();
    for query in filters {
        match query {
            dsl::Query::Term(term_query) => {
                // Extract information from the term query
                let field = strip_keyword_suffix(&term_query.field).to_owned();
                let Some(value) = term_query.value else {
                    continue; // Skip if term query does not have a value
                };
                let Some(term_value) = map_term_to_json_value(&value) else {
                    continue; // Skip if term query value cannot be mapped to a JSON value
                };
                // Process the term query information as needed
                predicates.push(Predicate::Term {
                    field,
                    value: term_value,
                });
            }

            dsl::Query::Range(range_query) => {
                // Extract information from the range query
                let field = strip_keyword_suffix(&range_query.field).to_owned();
                let gte = range_query.gte.clone();
                let lte = range_query.lte.clone();
                // Process the range query information as needed
                let gte_value = gte
                    .as_ref()
                    .and_then(|gte_term| map_term_to_json_value(gte_term));
                let lte_value = lte
                    .as_ref()
                    .and_then(|lte_term| map_term_to_json_value(lte_term));
                predicates.push(Predicate::Range {
                    field,
                    gte: gte_value,
                    lte: lte_value,
                });
            }
            _ => {
                // Handle other query types
                continue; // Skip unsupported query types
            }
        }
    }
    predicates
}

fn walk_aggregations_and_extract_info(
    aggregations: &dsl::Aggregations,
) -> Option<(FieldName, AggregationType, Option<GroupBySpec>)> {
    // Traverse the aggregations in the AST and extracting relevant information. Extract the first valid aggregation type found, along with any associated group by specifications.
    for (_, agg) in aggregations {
        match agg {
            dsl::Aggregation::MultiTerms(terms_agg) => {
                // Extract information from the terms aggregation
                let field_names: Vec<String> = terms_agg
                    .multi_terms
                    .terms
                    .iter()
                    .filter_map(|multi_term| multi_term.field.clone())
                    .collect();
                let field_names: Vec<String> = field_names
                    .iter()
                    .map(|s| strip_keyword_suffix(s).to_owned())
                    .collect();
                if field_names.is_empty() {
                    return None; // Return None if no valid field names are found in the multi-terms aggregation.
                }
                let group_by_spec = Some(GroupBySpec::Fields(field_names));
                let (target_field, aggregation_type) =
                    find_aggregation_info(&terms_agg.aggs.clone())?;
                return Some((target_field, aggregation_type, group_by_spec));
            }
            dsl::Aggregation::Terms(terms_agg) => {
                // Extract information from the terms aggregation
                if let Some(field) = terms_agg.terms.field.clone() {
                    let field = strip_keyword_suffix(&field).to_owned();
                    // Process the terms aggregation information as needed
                    let group_by_spec = Some(GroupBySpec::Fields(vec![field]));
                    let (target_field, aggregation_type) =
                        find_aggregation_info(&terms_agg.aggs.clone())?;
                    return Some((target_field, aggregation_type, group_by_spec));
                }
            }
            other => {
                // Handle other aggregation types
                let (target_field, aggregation_type) = extract_aggregation_info(&other)?;
                return Some((target_field, aggregation_type, None));
            }
        }
    }
    None // Return None if no relevant aggregation information is found
}

fn find_aggregation_info(aggregations: &dsl::Aggregations) -> Option<(FieldName, AggregationType)> {
    // Placeholder for extracting specific information from an aggregation node
    for (_, agg) in aggregations {
        let (field, aggregation_type) = extract_aggregation_info(&agg)?;
        return Some((field, aggregation_type));
    }
    None // Return None if no relevant aggregation information is found
}

fn extract_aggregation_info(agg: &dsl::Aggregation) -> Option<(FieldName, AggregationType)> {
    // Extracts the specific aggregation type and target field from the given aggregation node, if it matches supported types (avg, sum, min, max, percentiles).
    match agg {
        dsl::Aggregation::Avg(avg_agg) => {
            let field = strip_keyword_suffix(&avg_agg.avg.field).to_owned();
            let aggregation_type = AggregationType::Avg;
            Some((field, aggregation_type))
        }
        dsl::Aggregation::Sum(sum_agg) => {
            let field = strip_keyword_suffix(&sum_agg.sum.field.clone()?).to_owned();
            let aggregation_type = AggregationType::Sum;
            Some((field, aggregation_type))
        }
        dsl::Aggregation::Min(min_agg) => {
            let field = strip_keyword_suffix(&min_agg.min.field.clone()?).to_owned();
            let aggregation_type = AggregationType::Min;
            Some((field, aggregation_type))
        }
        dsl::Aggregation::Max(max_agg) => {
            let field = strip_keyword_suffix(&max_agg.max.field.clone()?).to_owned();
            let aggregation_type = AggregationType::Max;
            Some((field, aggregation_type))
        }
        dsl::Aggregation::Percentiles(percentiles_agg) => {
            let field = percentiles_agg.percentiles.field.clone();
            let percents = percentiles_agg
                .percentiles
                .percents
                .clone()
                .unwrap_or_default();
            let aggregation_type = AggregationType::Percentiles(percents);
            Some((field, aggregation_type))
        }
        _ => None, // Return None for unsupported aggregation types
    }
}

fn map_term_to_json_value(term: &dsl::Term) -> Option<TermValue> {
    // Placeholder for extracting field and value from a term query
    match term {
        dsl::Term::String(value) => {
            let value_str = value.to_string(); // Convert the term value to a string representation
            Some(TermValue::String(value_str))
        }
        dsl::Term::Float32(value) => Some(TermValue::Float(value.clone() as f64)),
        dsl::Term::Float64(value) => Some(TermValue::Float(value.clone())),
        dsl::Term::PositiveNumber(value) => Some(TermValue::UnsignedInt(value.clone())),
        dsl::Term::NegativeNumber(value) => Some(TermValue::Int(value.clone())),
        dsl::Term::Boolean(value) => Some(TermValue::Boolean(value.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_query_to_ast_parses_valid_search_request() {
        let query = r#"
                {
                    "query": {
                        "bool": {
                            "filter": [
                                { "term": { "service.keyword": { "value": "frontend" } } }
                            ]
                        }
                    },
                    "aggs": {
                        "avg_latency": { "avg": { "field": "latency_ms" } }
                    }
                }
                "#;

        let ast = parse_query_to_ast(query);
        assert!(ast.is_some());
    }

    #[test]
    fn parse_query_to_ast_returns_none_for_invalid_json() {
        let query = r#"{ "query": { "bool": { "filter": [ } }"#;
        assert!(parse_query_to_ast(query).is_none());
    }

    #[test]
    fn walk_bool_query_and_extract_info_extracts_term_and_range_predicates() {
        let bool_query = dsl::Query::bool()
            .filter(dsl::Query::term("service.keyword", "frontend"))
            .filter(dsl::Query::term("is_canary", true))
            .filter(dsl::Query::range("@timestamp").gte("now-30s").lte("now"));

        let predicates = walk_bool_query_and_extract_info(&bool_query);
        assert_eq!(predicates.len(), 3);
        assert_eq!(
            predicates[0],
            Predicate::Term {
                field: "service".to_string(),
                value: TermValue::String("frontend".to_string()),
            }
        );
        assert_eq!(
            predicates[1],
            Predicate::Term {
                field: "is_canary".to_string(),
                value: TermValue::Boolean(true),
            }
        );
        assert_eq!(
            predicates[2],
            Predicate::Range {
                field: "@timestamp".to_string(),
                gte: Some(TermValue::String("now-30s".to_string())),
                lte: Some(TermValue::String("now".to_string())),
            }
        );
    }

    #[test]
    fn walk_aggregations_and_extract_info_extracts_terms_group_by_and_metric() {
        let query = r#"
                {
                    "aggs": {
                        "by_service": {
                            "terms": { "field": "service.keyword" },
                            "aggs": {
                                "avg_latency": { "avg": { "field": "latency_ms" } }
                            }
                        }
                    }
                }
                "#;
        let ast = parse_query_to_ast(query).expect("query should parse");

        let (target_field, agg_type, group_by) =
            walk_aggregations_and_extract_info(&ast.aggs).expect("aggregation info should parse");
        assert_eq!(target_field, "latency_ms");
        assert_eq!(agg_type, AggregationType::Avg);
        assert_eq!(
            group_by,
            Some(GroupBySpec::Fields(vec!["service".to_string()]))
        );
    }

    #[test]
    fn walk_aggregations_and_extract_info_extracts_multi_terms_and_percentiles() {
        let query = r#"
                {
                    "aggs": {
                        "by_labels": {
                            "multi_terms": {
                                "terms": [
                                    { "field": "service.keyword" },
                                    { "field": "env.keyword" }
                                ]
                            },
                            "aggs": {
                                "latency_percentiles": {
                                    "percentiles": {
                                        "field": "latency_ms",
                                        "percents": [50.0, 95.0]
                                    }
                                }
                            }
                        }
                    }
                }
                "#;
        let ast = parse_query_to_ast(query).expect("query should parse");

        let (target_field, agg_type, group_by) =
            walk_aggregations_and_extract_info(&ast.aggs).expect("aggregation info should parse");
        assert_eq!(target_field, "latency_ms");
        assert_eq!(agg_type, AggregationType::Percentiles(vec![50.0, 95.0]));
        assert_eq!(
            group_by,
            Some(GroupBySpec::Fields(vec![
                "service".to_string(),
                "env".to_string()
            ]))
        );
    }

    #[test]
    fn walk_ast_and_extract_info_builds_elastic_dsl_query() {
        let ast = dsl::Search::new()
            .query(
                dsl::Query::bool()
                    .filter(dsl::Query::term("service.keyword", "frontend"))
                    .filter(dsl::Query::range("@timestamp").gte("now-30s").lte("now")),
            )
            .aggregate(
                "by_service",
                dsl::Aggregation::terms("service.keyword")
                    .aggregate("max_latency", dsl::Aggregation::max("latency_ms")),
            );
        let info = walk_ast_and_extract_info(&ast).expect("info should parse");

        assert_eq!(info.target_field, "latency_ms");
        assert_eq!(info.aggregation, AggregationType::Max);
        assert_eq!(info.predicates.len(), 2);
        assert_eq!(
            info.group_by_buckets,
            Some(GroupBySpec::Fields(vec!["service".to_string()]))
        );
    }
}
