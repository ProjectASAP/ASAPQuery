use crate::test_data::*;
use promql_parser::parser as promql;
use promql_utilities::ast_matching::{PromQLPattern, PromQLPatternBuilder};
// Decoupled from QueryPatternType: use string category keys
use serde_json::Value;
use std::collections::HashMap;
use std::time::Instant;

pub struct PatternTester {
    patterns: HashMap<String, Vec<PromQLPattern>>,
}

impl PatternTester {
    pub fn new() -> Self {
        let mut patterns = HashMap::new();

        // ONLY_TEMPORAL patterns
        let temporal_patterns = vec![
            // Rate pattern
            PromQLPattern::new(
                Self::build_rate_pattern(),
                // Some("ONLY_TEMPORAL".to_string()),
            ),
            // Quantile over time pattern
            PromQLPattern::new(
                Self::build_quantile_over_time_pattern(),
                // Some("ONLY_TEMPORAL".to_string()),
            ),
        ];

        // ONLY_SPATIAL patterns
        let spatial_patterns = vec![
            // Sum aggregation pattern
            PromQLPattern::new(
                Self::build_sum_pattern(),
                // Some("ONLY_SPATIAL".to_string()),
            ),
            // Simple metric pattern
            PromQLPattern::new(
                Self::build_metric_pattern(),
                // Some("ONLY_SPATIAL".to_string()),
            ),
        ];

        // ONE_TEMPORAL_ONE_SPATIAL patterns
        let combined_patterns = vec![
            // Aggregation of single-arg temporal functions
            PromQLPattern::new(
                Self::build_one_temporal_one_spatial_pattern(),
                // Some("ONE_TEMPORAL_ONE_SPATIAL".to_string()),
            ),
            // Aggregation of quantile_over_time (2-arg)
            PromQLPattern::new(
                Self::build_combined_quantile_pattern(),
                // Some("ONE_TEMPORAL_ONE_SPATIAL".to_string()),
            ),
        ];

        // Insert in order from simple to complex to avoid panics
        // ONLY_VECTOR is derived via disambiguation in test_query, not a separate entry
        patterns.insert("ONLY_SPATIAL".to_string(), spatial_patterns);
        patterns.insert("ONLY_TEMPORAL".to_string(), temporal_patterns);
        patterns.insert("ONE_TEMPORAL_ONE_SPATIAL".to_string(), combined_patterns);

        Self { patterns }
    }

    pub fn test_query(&self, test_case: &TestCase) -> TestResult {
        let start_time = Instant::now();
        let test_id = test_case.id.clone();

        // Parse the query
        let ast = match promql::parse(&test_case.query) {
            Ok(ast) => ast,
            Err(e) => {
                return TestResult {
                    test_id,
                    success: false,
                    error_message: Some(format!("Failed to parse query: {}", e)),
                    actual_pattern_type: None,
                    actual_tokens: None,
                    execution_time_ms: start_time.elapsed().as_secs_f64() * 1000.0,
                };
            }
        };

        // Try to match against all patterns
        let mut matched_pattern_type = None;
        let mut matched_tokens = None;

        for (pattern_type, pattern_list) in &self.patterns {
            for pattern in pattern_list {
                let match_result = pattern.matches(&ast);
                if match_result.matches {
                    // If a plain vector selector matched under the spatial patterns, classify as ONLY_VECTOR
                    let final_type = if pattern_type == "ONLY_SPATIAL" {
                        if match_result.tokens.contains_key("aggregation") {
                            pattern_type.clone()
                        } else if match_result.tokens.contains_key("metric") {
                            "ONLY_VECTOR".to_string()
                        } else {
                            pattern_type.clone()
                        }
                    } else {
                        pattern_type.clone()
                    };

                    // Debug: show pattern_type and token keys for failing test
                    // debug removed
                    matched_pattern_type = Some(final_type);
                    // Extract only relevant token data to match Python format
                    let flattened_tokens = Self::flatten_token_data(&match_result.tokens);
                    matched_tokens =
                        Some(serde_json::to_value(&flattened_tokens).unwrap_or_default());
                    break;
                }
            }
            if matched_pattern_type.is_some() {
                break;
            }
        }

        let execution_time = start_time.elapsed().as_secs_f64() * 1000.0;

        // Check if results match expectations
        let expected_type = &test_case.expected_pattern_type;
        let success = matched_pattern_type.as_ref() == Some(expected_type);

        TestResult {
            test_id,
            success,
            error_message: if success {
                None
            } else {
                Some(format!(
                    "Pattern type mismatch. Expected: {}, Got: {:?}",
                    expected_type, matched_pattern_type
                ))
            },
            actual_pattern_type: matched_pattern_type,
            actual_tokens: matched_tokens,
            execution_time_ms: execution_time,
        }
    }

    // No conversion needed anymore; keys are already strings

    fn flatten_token_data(
        tokens: &HashMap<String, promql_utilities::ast_matching::TokenData>,
    ) -> HashMap<String, Value> {
        let mut result = HashMap::new();

        for (token_name, token_data) in tokens {
            // Extract only the relevant data from the token based on what's populated
            if let Some(metric) = &token_data.metric {
                let mut metric_data = serde_json::Map::new();
                metric_data.insert("name".to_string(), Value::String(metric.name.clone()));
                metric_data.insert(
                    "labels".to_string(),
                    serde_json::to_value(&metric.labels).unwrap_or(Value::Null),
                );
                metric_data.insert(
                    "at".to_string(),
                    if let Some(at) = metric.at_modifier {
                        Value::Number(serde_json::Number::from(at))
                    } else {
                        Value::Null
                    },
                );
                // Note: Skipping AST for now since it's not serializable
                result.insert(token_name.clone(), Value::Object(metric_data));
            } else if let Some(function) = &token_data.function {
                let mut function_data = serde_json::Map::new();
                function_data.insert("name".to_string(), Value::String(function.name.clone()));
                let args_values: Vec<Value> = function
                    .args
                    .iter()
                    .map(|arg| Value::String(arg.clone()))
                    .collect();
                function_data.insert("args".to_string(), Value::Array(args_values));
                // Note: Skipping AST for now since it's not serializable
                result.insert(token_name.clone(), Value::Object(function_data));
            } else if let Some(aggregation) = &token_data.aggregation {
                let mut aggregation_data = serde_json::Map::new();
                aggregation_data.insert("op".to_string(), Value::String(aggregation.op.clone()));
                aggregation_data.insert(
                    "modifier".to_string(),
                    if let Some(modifier) = &aggregation.modifier {
                        serde_json::to_value(modifier).unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    },
                );
                aggregation_data.insert(
                    "param".to_string(),
                    if let Some(param) = &aggregation.param {
                        Value::String(param.clone())
                    } else {
                        Value::Null
                    },
                );
                // Note: Skipping AST for now since it's not serializable
                result.insert(token_name.clone(), Value::Object(aggregation_data));
            } else if let Some(range_vector) = &token_data.range_vector {
                let mut range_data = serde_json::Map::new();
                // Convert chrono Duration to human-readable format like Python's "0:05:00"
                let total_seconds = range_vector.range.num_seconds() as u64;
                let hours = total_seconds / 3600;
                let minutes = (total_seconds % 3600) / 60;
                let seconds = total_seconds % 60;
                let range_str = format!("{}:{:02}:{:02}", hours, minutes, seconds);
                range_data.insert("range".to_string(), Value::String(range_str));
                // Note: Skipping AST for now since it's not serializable
                result.insert(token_name.clone(), Value::Object(range_data));
            } else if let Some(subquery) = &token_data.subquery {
                let mut subquery_data = serde_json::Map::new();
                // Convert chrono Duration to human-readable format like Python's "0:05:00"
                let total_seconds = subquery.range.num_seconds() as u64;
                let hours = total_seconds / 3600;
                let minutes = (total_seconds % 3600) / 60;
                let seconds = total_seconds % 60;
                let range_str = format!("{}:{:02}:{:02}", hours, minutes, seconds);
                subquery_data.insert("range".to_string(), Value::String(range_str));
                if let Some(offset) = &subquery.offset {
                    subquery_data.insert("offset".to_string(), Value::String(offset.clone()));
                }
                if let Some(step) = &subquery.step {
                    subquery_data.insert("step".to_string(), Value::String(step.clone()));
                }
                // Note: Skipping AST for now since it's not serializable
                result.insert(token_name.clone(), Value::Object(subquery_data));
            } else if let Some(number) = &token_data.number {
                let mut number_data = serde_json::Map::new();
                number_data.insert(
                    "value".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(number.value)
                            .unwrap_or(serde_json::Number::from(0)),
                    ),
                );
                result.insert(token_name.clone(), Value::Object(number_data));
            }

            // Handle special case for function_args (like Python does)
            if token_name == "function_args" {
                if let Some(function) = &token_data.function {
                    let args_values: Vec<Value> = function
                        .args
                        .iter()
                        .map(|arg| Value::String(arg.clone()))
                        .collect();
                    result.insert(token_name.clone(), Value::Array(args_values));
                }
            }
        }

        result
    }

    fn build_rate_pattern() -> Option<HashMap<String, Value>> {
        let ms = PromQLPatternBuilder::matrix_selector(
            PromQLPatternBuilder::metric(None, None, None, Some("metric")),
            None,
            Some("range_vector"),
        );

        let args: Vec<Option<HashMap<String, Value>>> = vec![ms];

        PromQLPatternBuilder::function(
            vec![
                "rate",
                "increase",
                "avg_over_time",
                "sum_over_time",
                "count_over_time",
                "min_over_time",
                "max_over_time",
            ],
            args,
            Some("function"),
            None,
        )
    }

    fn build_quantile_over_time_pattern() -> Option<HashMap<String, Value>> {
        let num = PromQLPatternBuilder::number(None, None);
        let ms = PromQLPatternBuilder::matrix_selector(
            PromQLPatternBuilder::metric(None, None, None, Some("metric")),
            None,
            Some("range_vector"),
        );

        let args: Vec<Option<HashMap<String, Value>>> = vec![num, ms];

        PromQLPatternBuilder::function(
            vec!["quantile_over_time"],
            args,
            Some("function"),
            Some("function_args"),
        )
    }

    fn build_sum_pattern() -> Option<HashMap<String, Value>> {
        PromQLPatternBuilder::aggregation(
            vec!["sum", "count", "avg", "min", "max"],
            PromQLPatternBuilder::metric(None, None, None, Some("metric")),
            None,
            None,
            None,
            Some("aggregation"),
        )
    }

    fn build_metric_pattern() -> Option<HashMap<String, Value>> {
        PromQLPatternBuilder::metric(None, None, None, Some("metric"))
    }

    fn build_one_temporal_one_spatial_pattern() -> Option<HashMap<String, Value>> {
        let ms = PromQLPatternBuilder::matrix_selector(
            PromQLPatternBuilder::metric(None, None, None, Some("metric")),
            None,
            Some("range_vector"),
        );

        let func_args: Vec<Option<HashMap<String, Value>>> = vec![ms];

        let func = PromQLPatternBuilder::function(
            vec![
                "sum_over_time",
                "count_over_time",
                "avg_over_time",
                "min_over_time",
                "max_over_time",
                "rate",
                "increase",
            ],
            func_args,
            Some("function"),
            None,
        );

        PromQLPatternBuilder::aggregation(
            vec!["sum", "count", "avg", "quantile", "min", "max"],
            func,
            None,
            None,
            None,
            Some("aggregation"),
        )
    }

    fn build_combined_quantile_pattern() -> Option<HashMap<String, Value>> {
        let num = PromQLPatternBuilder::number(None, None);
        let ms = PromQLPatternBuilder::matrix_selector(
            PromQLPatternBuilder::metric(None, None, None, Some("metric")),
            None,
            Some("range_vector"),
        );
        let func_args: Vec<Option<HashMap<String, Value>>> = vec![num, ms];
        let func = PromQLPatternBuilder::function(
            vec!["quantile_over_time"],
            func_args,
            Some("function"),
            None,
        );
        PromQLPatternBuilder::aggregation(
            vec!["sum", "count", "avg", "quantile", "min", "max"],
            func,
            None,
            None,
            None,
            Some("aggregation"),
        )
    }
}

impl Default for PatternTester {
    fn default() -> Self {
        Self::new()
    }
}
