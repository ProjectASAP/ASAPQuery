use chrono::Duration;
use core::panic;
use promql_parser::label::MatchOp;
use promql_parser::parser::{AtModifier, Expr, LabelModifier, SubqueryExpr, VectorSelector};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::time::UNIX_EPOCH;
use tracing::debug;

/// PromQL pattern for AST-based matching
#[derive(Debug, Clone)]
pub struct PromQLPattern {
    /// AST pattern definition (JSON-like structure). None indicates a wildcard (match any).
    pub ast_pattern: Option<HashMap<String, Value>>,
    ///// Tokens to collect during matching
    //pub collect_tokens: Vec<String>,
}

impl PromQLPattern {
    /// Create a new pattern with AST pattern definition
    //pub fn new(ast_pattern: Option<HashMap<String, Value>>, collect_tokens: Vec<String>) -> Self {
    pub fn new(ast_pattern: Option<HashMap<String, Value>>) -> Self {
        debug!("Creating new PromQLPattern");
        Self {
            ast_pattern,
            //collect_tokens,
        }
    }

    /// Convert an Expr to a clean string representation
    fn expr_to_string(expr: &Expr) -> String {
        match expr {
            Expr::NumberLiteral(num) => num.val.to_string(),
            _ => format!("{:?}", expr),
        }
    }

    /// Match this pattern against a parsed AST
    pub fn matches(&self, ast: &Expr) -> PromQLMatchResult {
        debug!("Starting pattern matching against AST");
        debug!("Pattern: {:?}", self.ast_pattern);
        debug!("AST: {:?}", ast);
        let mut tokens = HashMap::new();
        let matches = self.matches_recursive(ast, self.ast_pattern.as_ref(), &mut tokens);
        debug!(
            "Pattern matching completed: {}, collected {} tokens",
            matches,
            tokens.len()
        );
        if !matches {
            debug!("MATCH FAILED - tokens collected: {:?}", tokens);
        }
        PromQLMatchResult { matches, tokens }
    }

    /// Recursive pattern matching implementation
    fn matches_recursive(
        &self,
        node: &Expr,
        pattern: Option<&HashMap<String, Value>>,
        tokens: &mut HashMap<String, TokenData>,
    ) -> bool {
        // None pattern is treated as wildcard (matches anything) to mirror Python's None
        if pattern.is_none() {
            debug!("Wildcard pattern matched");
            return true;
        }
        let pattern = pattern.unwrap();
        if pattern.is_empty() {
            panic!("Empty pattern is not allowed");
        }

        // Get the pattern type
        let pattern_type = match pattern.get("type") {
            Some(Value::String(t)) => t.as_str(),
            _ => panic!("Pattern must have a 'type' field of string type"),
        };

        debug!("Matching pattern type: {} against node type", pattern_type);
        debug!("Full pattern: {:?}", pattern);
        debug!("Node: {:?}", node);
        match (pattern_type, node) {
            // Match metric selectors
            ("VectorSelector", Expr::VectorSelector(vs)) => {
                self.match_metric_selector(vs, pattern, tokens)
            }

            // Match function calls
            ("Call", Expr::Call(call)) => self.match_function_call(call, pattern, tokens),

            // Match aggregation operations
            ("AggregateExpr", Expr::Aggregate(agg)) => self.match_aggregation(agg, pattern, tokens),

            // Match matrix selectors (range vectors)
            ("MatrixSelector", Expr::MatrixSelector(ms)) => {
                self.match_matrix_selector(ms, pattern, tokens)
            }

            // Match binary operations
            ("BinaryExpr", Expr::Binary(bin_op)) => {
                self.match_binary_operation(bin_op, pattern, tokens)
            }

            // Match number literals
            ("NumberLiteral", Expr::NumberLiteral(num)) => {
                self.match_number_literal(num, pattern, tokens)
            }

            // Match subquery expressions
            ("SubqueryExpr", Expr::Subquery(subquery)) => {
                self.match_subquery(subquery, pattern, tokens)
            }

            _ => false, // Simply return false for non-matching types
        }
    }

    /// Match a VectorSelector node against pattern
    fn match_metric_selector(
        &self,
        vs: &VectorSelector,
        pattern: &HashMap<String, Value>,
        tokens: &mut HashMap<String, TokenData>,
    ) -> bool {
        // Check metric name if specified in pattern
        if let Some(Value::String(expected_name)) = pattern.get("name") {
            if let Some(metric_name) = &vs.name {
                if *metric_name != *expected_name {
                    return false;
                }
            } else {
                return false; // Pattern expects name but node has none
            }
        }

        // Extract and store token data if this node should be collected
        if let Some(Value::String(collect_as)) = pattern.get("_collect_as") {
            debug!("Collecting metric token as: {}", collect_as);
            let mut labels = HashMap::new();

            // Extract label matchers
            for matcher in &vs.matchers.matchers {
                if matcher.op == MatchOp::Equal {
                    labels.insert(matcher.name.clone(), matcher.value.clone());
                }
            }

            let at_modifier_opt = match &vs.at {
                Some(AtModifier::At(t)) => {
                    // Convert SystemTime to seconds since UNIX_EPOCH (u64).
                    // Panic if time is earlier than UNIX_EPOCH (pre-epoch) as requested.
                    let secs = match t.duration_since(UNIX_EPOCH) {
                        Ok(dur) => dur.as_secs(),
                        Err(_) => panic!("AtModifier::At contains a time before UNIX_EPOCH, which is not supported by the pattern matcher"),
                    };

                    Some(secs)
                }
                Some(AtModifier::Start) => {
                    panic!("AtModifier::Start is not supported by pattern matcher")
                }
                Some(AtModifier::End) => {
                    panic!("AtModifier::End is not supported by pattern matcher")
                }
                None => None,
            };

            let metric_token = MetricToken {
                name: vs.name.clone().unwrap_or_default(),
                labels,
                at_modifier: at_modifier_opt,
                ast: Some(vs.clone()),
            };

            let token_data = TokenData {
                metric: Some(metric_token),
                function: None,
                aggregation: None,
                range_vector: None,
                subquery: None,
                binary_op: None,
                number: None,
            };

            tokens.insert(collect_as.clone(), token_data);
        }

        true
    }

    /// Match a Call node (function call) against pattern
    fn match_function_call(
        &self,
        call: &promql_parser::parser::Call,
        pattern: &HashMap<String, Value>,
        tokens: &mut HashMap<String, TokenData>,
    ) -> bool {
        // Check function name
        // BUGFIX: Pattern builder creates "func" as an Object, not Array of Objects
        // Original code (incorrect - expected Array of Objects):
        // if let Some(Value::Array(expected_names)) = pattern.get("func") {
        //     if let Some(func_obj) = expected_names.first() {
        //         if let Some(func_map) = func_obj.as_object() {
        //             if let Some(Value::Array(names)) = func_map.get("name") {
        //                 let function_name = call.func.name;
        //                 let matches_name = names.iter().any(|name| {
        //                     if let Some(name_str) = name.as_str() {
        //                         name_str == function_name
        //                     } else {
        //                         false
        //                     }
        //                 });
        //
        //                 if !matches_name {
        //                     return false;
        //                 }
        //             }
        //         }
        //     }
        // }

        // Fixed code (correct - expects Object with "name" field):
        if let Some(func_pattern_value) = pattern.get("func") {
            if let Some(func_pattern) = func_pattern_value.as_object() {
                if let Some(Value::Array(names)) = func_pattern.get("name") {
                    let function_name = call.func.name;
                    let matches_name = names.iter().any(|name| {
                        if let Some(name_str) = name.as_str() {
                            name_str == function_name
                        } else {
                            false
                        }
                    });

                    if !matches_name {
                        return false;
                    }
                }
            }
        }

        // Check arguments recursively
        if let Some(Value::Array(expected_args)) = pattern.get("args") {
            if call.args.args.len() != expected_args.len() {
                return false;
            }

            for (i, arg) in call.args.args.iter().enumerate() {
                if let Some(arg_pattern) = expected_args[i].as_object() {
                    let arg_pattern_map: HashMap<String, Value> = arg_pattern
                        .clone()
                        .into_iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();

                    if !self.matches_recursive(arg.as_ref(), Some(&arg_pattern_map), tokens) {
                        return false;
                    }
                }
            }
        }

        // Extract and store token data if this node should be collected
        if let Some(Value::String(collect_as)) = pattern.get("_collect_as") {
            debug!("Collecting function token as: {}", collect_as);
            let function_token = FunctionToken {
                name: call.func.name.to_string(),
                args: call
                    .args
                    .args
                    .iter()
                    .map(|arg| Self::expr_to_string(arg))
                    .collect(), // Capture actual args
            };

            let token_data = TokenData {
                metric: None,
                function: Some(function_token),
                aggregation: None,
                range_vector: None,
                subquery: None,
                binary_op: None,
                number: None,
            };

            tokens.insert(collect_as.clone(), token_data);
        }

        // If requested, collect the raw function arguments (as strings) under a separate token
        if let Some(Value::String(collect_args_as)) = pattern.get("_collect_args_as") {
            let arg_strs: Vec<String> = call
                .args
                .args
                .iter()
                .map(|arg| Self::expr_to_string(arg))
                .collect();

            let function_args_token = FunctionToken {
                name: call.func.name.to_string(),
                args: arg_strs,
            };

            let token_data = TokenData {
                metric: None,
                function: Some(function_args_token),
                aggregation: None,
                range_vector: None,
                subquery: None,
                binary_op: None,
                number: None,
            };

            tokens.insert(collect_args_as.clone(), token_data);
        }

        true
    }

    /// Match an Aggregate node against pattern
    fn match_aggregation(
        &self,
        agg: &promql_parser::parser::AggregateExpr,
        pattern: &HashMap<String, Value>,
        tokens: &mut HashMap<String, TokenData>,
    ) -> bool {
        debug!("=== AGGREGATION MATCHING START ===");
        debug!("Aggregation pattern: {:?}", pattern);
        debug!("Aggregation AST: {:?}", agg);
        // Check aggregation operation
        if let Some(Value::Array(expected_ops)) = pattern.get("op") {
            let agg_op = agg.op.to_string();
            debug!(
                "Checking aggregation op '{}' against pattern ops: {:?}",
                agg_op, expected_ops
            );
            let matches_op = expected_ops.iter().any(|op| {
                if let Some(op_str) = op.as_str() {
                    op_str == agg_op
                } else {
                    false
                }
            });

            if !matches_op {
                debug!("Aggregation op '{}' does not match pattern ops", agg_op);
                return false;
            }
            debug!("Aggregation op '{}' matched!", agg_op);
        }

        // Check inner expression recursively
        if let Some(expr_pattern_value) = pattern.get("expr") {
            debug!("Found expr pattern value: {:?}", expr_pattern_value);
            if let Some(expr_pattern) = expr_pattern_value.as_object() {
                debug!("Expr pattern is an object, recursing...");
                let expr_pattern_map: HashMap<String, Value> = expr_pattern
                    .clone()
                    .into_iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                if !self.matches_recursive(&agg.expr, Some(&expr_pattern_map), tokens) {
                    debug!("Inner expression recursive match FAILED");
                    return false;
                }
                debug!("Inner expression recursive match SUCCESS");
            } else if expr_pattern_value.is_null() {
                debug!("Expr pattern is null, skipping validation");
            } else {
                debug!(
                    "Expr pattern is neither object nor null: {:?}",
                    expr_pattern_value
                );
            }
        } else {
            debug!("No expr pattern found, skipping inner expression check");
        }

        // Check modifier if specified in pattern
        // Original code (too strict - fails when query has modifier but pattern is null):
        // if let Some(pattern_modifier_value) = pattern.get("modifier") {
        //     let actual_modifier = match &agg.modifier {
        //         Some(LabelModifier::Include(_)) => "by",
        //         Some(LabelModifier::Exclude(_)) => "without",
        //         None => "null",
        //     };
        //
        //     match pattern_modifier_value {
        //         Value::String(expected_modifier) => {
        //             if actual_modifier != expected_modifier {
        //                 return false;
        //             }
        //         }
        //         Value::Null => {
        //             if actual_modifier != "null" {
        //                 return false;
        //             }
        //         }
        //         _ => {
        //             // Invalid pattern modifier format
        //             return false;
        //         }
        //     }
        // }

        // Fixed code - only validate modifiers if pattern explicitly specifies a non-null modifier
        if let Some(pattern_modifier_value) = pattern.get("modifier") {
            debug!("Found modifier pattern: {:?}", pattern_modifier_value);
            let actual_modifier = match &agg.modifier {
                Some(LabelModifier::Include(_)) => "by",
                Some(LabelModifier::Exclude(_)) => "without",
                None => "null",
            };
            debug!("Actual aggregation modifier: '{}'", actual_modifier);

            // Only validate if pattern explicitly requires a specific modifier (not null)
            if !pattern_modifier_value.is_null() {
                debug!("Pattern requires specific modifier, validating...");
                match pattern_modifier_value {
                    Value::String(expected_modifier) => {
                        debug!(
                            "Expected modifier: '{}', actual: '{}'",
                            expected_modifier, actual_modifier
                        );
                        if actual_modifier != expected_modifier {
                            debug!("Modifier mismatch - FAILED");
                            return false;
                        }
                        debug!("Modifier match - SUCCESS");
                    }
                    _ => {
                        debug!("Invalid pattern modifier format - FAILED");
                        return false;
                    }
                }
            } else {
                debug!("Pattern modifier is null, allowing any query modifier (wildcard)");
            }
        } else {
            debug!("No modifier pattern found, allowing any query modifier");
        }

        debug!("=== AGGREGATION MATCHING SUCCESS ===");

        // Extract and store token data if this node should be collected
        if let Some(Value::String(collect_as)) = pattern.get("_collect_as") {
            debug!("Collecting aggregation token as: {}", collect_as);
            let modifier = match &agg.modifier {
                Some(LabelModifier::Include(labels)) => Some(AggregationModifier {
                    modifier_type: "by".to_string(),
                    labels: labels.labels.clone(),
                }),
                Some(LabelModifier::Exclude(labels)) => Some(AggregationModifier {
                    modifier_type: "without".to_string(),
                    labels: labels.labels.clone(),
                }),
                None => None,
            };

            let aggregation_token = AggregationToken {
                op: agg.op.to_string(),
                modifier,
                param: agg.param.as_ref().map(|p| Self::expr_to_string(p)),
            };

            let token_data = TokenData {
                metric: None,
                function: None,
                aggregation: Some(aggregation_token),
                range_vector: None,
                subquery: None,
                binary_op: None,
                number: None,
            };

            tokens.insert(collect_as.clone(), token_data);
        }

        true
    }

    /// Match a MatrixSelector node against pattern
    fn match_matrix_selector(
        &self,
        ms: &promql_parser::parser::MatrixSelector,
        pattern: &HashMap<String, Value>,
        tokens: &mut HashMap<String, TokenData>,
    ) -> bool {
        // Check the inner vector selector
        if let Some(vs_pattern_value) = pattern.get("vector_selector") {
            if let Some(vs_pattern) = vs_pattern_value.as_object() {
                let vs_pattern_map: HashMap<String, Value> = vs_pattern
                    .clone()
                    .into_iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                if !self.match_metric_selector(&ms.vs, &vs_pattern_map, tokens) {
                    return false;
                }
            }
        }

        // Extract and store token data if this node should be collected
        if let Some(Value::String(collect_as)) = pattern.get("_collect_as") {
            // Convert std::time::Duration to chrono::Duration and store directly
            let chrono_dur = Duration::from_std(ms.range)
                .map_err(|_| Duration::zero())
                .unwrap();

            let range_token = RangeToken {
                range: chrono_dur,
                offset: ms.vs.offset.as_ref().map(|offset| format!("{:?}", offset)),
            };

            let token_data = TokenData {
                metric: None,
                function: None,
                aggregation: None,
                range_vector: Some(range_token),
                subquery: None,
                binary_op: None,
                number: None,
            };

            tokens.insert(collect_as.clone(), token_data);
        }

        true
    }

    ///// Normalize duration to standard PromQL format (prefer larger units when possible)
    // fn normalize_duration_string(duration: &std::time::Duration) -> String {
    //     let secs = duration.as_secs();

    //     // Convert to the most appropriate unit, preferring larger units when possible
    //     if secs >= 3600 && secs % 3600 == 0 {
    //         format!("{}h", secs / 3600)
    //     } else if secs >= 60 && secs % 60 == 0 {
    //         format!("{}m", secs / 60)
    //     } else if secs > 0 {
    //         format!("{secs}s")
    //     } else {
    //         // Handle sub-second durations
    //         let millis = duration.as_millis();
    //         if millis > 0 {
    //             format!("{millis}ms")
    //         } else {
    //             "0s".to_string()
    //         }
    //     }
    // }

    /// Match a Binary expression node against pattern
    fn match_binary_operation(
        &self,
        bin_op: &promql_parser::parser::BinaryExpr,
        pattern: &HashMap<String, Value>,
        tokens: &mut HashMap<String, TokenData>,
    ) -> bool {
        // Check operation type
        if let Some(Value::String(expected_op)) = pattern.get("op") {
            if bin_op.op.to_string() != *expected_op {
                return false;
            }
        }

        // Check left and right expressions recursively
        if let Some(left_pattern_value) = pattern.get("left") {
            if let Some(left_pattern) = left_pattern_value.as_object() {
                let left_pattern_map: HashMap<String, Value> = left_pattern
                    .clone()
                    .into_iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                if !self.matches_recursive(&bin_op.lhs, Some(&left_pattern_map), tokens) {
                    return false;
                }
            }
        }

        if let Some(right_pattern_value) = pattern.get("right") {
            if let Some(right_pattern) = right_pattern_value.as_object() {
                let right_pattern_map: HashMap<String, Value> = right_pattern
                    .clone()
                    .into_iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                if !self.matches_recursive(&bin_op.rhs, Some(&right_pattern_map), tokens) {
                    return false;
                }
            }
        }

        // Extract and store token data if this node should be collected
        if let Some(Value::String(collect_as)) = pattern.get("_collect_as") {
            let binary_token = BinaryOpToken {
                op: bin_op.op.to_string(),
                matching: None, // TODO: Add vector matching support
            };

            let token_data = TokenData {
                metric: None,
                function: None,
                aggregation: None,
                range_vector: None,
                subquery: None,
                binary_op: Some(binary_token),
                number: None,
            };

            tokens.insert(collect_as.clone(), token_data);
        }

        true
    }

    /// Match a NumberLiteral node against pattern
    fn match_number_literal(
        &self,
        num: &promql_parser::parser::NumberLiteral,
        pattern: &HashMap<String, Value>,
        tokens: &mut HashMap<String, TokenData>,
    ) -> bool {
        // Check value if specified in pattern
        if let Some(Value::Number(expected_value)) = pattern.get("value") {
            if let Some(expected_f64) = expected_value.as_f64() {
                if (num.val - expected_f64).abs() > f64::EPSILON {
                    return false;
                }
            }
        }

        // Extract and store token data if this node should be collected
        if let Some(Value::String(collect_as)) = pattern.get("_collect_as") {
            let number_token = NumberToken { value: num.val };

            let token_data = TokenData {
                metric: None,
                function: None,
                aggregation: None,
                range_vector: None,
                subquery: None,
                binary_op: None,
                number: Some(number_token),
            };

            tokens.insert(collect_as.clone(), token_data);
        }

        true
    }

    /// Match a SubqueryExpr node against pattern
    fn match_subquery(
        &self,
        subquery: &SubqueryExpr,
        pattern: &HashMap<String, Value>,
        tokens: &mut HashMap<String, TokenData>,
    ) -> bool {
        // Check inner expression recursively
        if let Some(expr_pattern_value) = pattern.get("expr") {
            if let Some(expr_pattern) = expr_pattern_value.as_object() {
                let expr_pattern_map: HashMap<String, Value> = expr_pattern
                    .clone()
                    .into_iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                if !self.matches_recursive(&subquery.expr, Some(&expr_pattern_map), tokens) {
                    return false;
                }
            }
        }

        // Extract and store token data if this node should be collected
        if let Some(Value::String(collect_as)) = pattern.get("_collect_as") {
            // Convert std::time::Duration to chrono::Duration and store
            let chrono_dur = Duration::from_std(subquery.range)
                .map_err(|_| Duration::zero())
                .unwrap();

            let subquery_token = SubqueryToken {
                range: chrono_dur,
                offset: subquery
                    .offset
                    .as_ref()
                    .map(|offset| format!("{:?}", offset)),
                step: subquery.step.as_ref().map(|step| format!("{:?}", step)),
            };

            let token_data = TokenData {
                metric: None,
                function: None,
                aggregation: None,
                range_vector: None,
                subquery: Some(subquery_token),
                binary_op: None,
                number: None,
            };

            tokens.insert(collect_as.clone(), token_data);
        }

        true
    }
}

/// Token data extracted from AST nodes - pattern matching system
#[derive(Debug, Clone, Serialize)]
pub struct TokenData {
    pub metric: Option<MetricToken>,
    pub function: Option<FunctionToken>,
    pub aggregation: Option<AggregationToken>,
    pub range_vector: Option<RangeToken>,
    pub subquery: Option<SubqueryToken>,
    pub binary_op: Option<BinaryOpToken>,
    pub number: Option<NumberToken>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricToken {
    pub name: String,
    pub labels: HashMap<String, String>,
    // seconds since UNIX_EPOCH
    pub at_modifier: Option<u64>,
    #[serde(skip_serializing, skip_deserializing)]
    pub ast: Option<VectorSelector>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FunctionToken {
    pub name: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AggregationToken {
    pub op: String,
    pub modifier: Option<AggregationModifier>,
    pub param: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RangeToken {
    pub range: Duration,
    pub offset: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SubqueryToken {
    pub range: Duration,
    pub offset: Option<String>,
    pub step: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BinaryOpToken {
    pub op: String,
    pub matching: Option<VectorMatching>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NumberToken {
    pub value: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct VectorMatching {
    pub card: String, // "one-to-one", "one-to-many", "many-to-one"
    pub on: Vec<String>,
    pub ignoring: Vec<String>,
    pub group_left: Vec<String>,
    pub group_right: Vec<String>,
}

/// Match result with token-based extraction
#[derive(Debug, Clone)]
pub struct PromQLMatchResult {
    pub matches: bool,
    pub tokens: HashMap<String, TokenData>,
}

impl PromQLMatchResult {
    /// Create a new empty result
    pub fn new() -> Self {
        Self {
            matches: false,
            tokens: HashMap::new(),
        }
    }

    /// Create a successful match result with tokens
    pub fn with_tokens(tokens: HashMap<String, TokenData>) -> Self {
        Self {
            matches: true,
            tokens,
        }
    }

    /// Get metric name from tokens
    pub fn get_metric_name(&self) -> Option<String> {
        self.tokens
            .get("metric")?
            .metric
            .as_ref()
            .map(|m| m.name.clone())
    }

    /// Get function name from tokens
    pub fn get_function_name(&self) -> Option<String> {
        self.tokens
            .get("function")?
            .function
            .as_ref()
            .map(|f| f.name.clone())
    }

    /// Get aggregation operation from tokens
    pub fn get_aggregation_op(&self) -> Option<String> {
        self.tokens
            .get("aggregation")?
            .aggregation
            .as_ref()
            .map(|a| a.op.clone())
    }

    /// Get range duration from tokens as chrono::Duration
    pub fn get_range_duration(&self) -> Option<Duration> {
        self.tokens
            .get("range_vector")?
            .range_vector
            .as_ref()
            .map(|r| r.range)
    }
}

impl Default for PromQLMatchResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents aggregation modifiers like "by" or "without"
#[derive(Debug, Clone, Serialize)]
pub struct AggregationModifier {
    pub modifier_type: String, // "by" or "without"
    pub labels: Vec<String>,
}

impl AggregationModifier {
    /// Create a new AggregationModifier
    pub fn new(modifier_type: String, labels: Vec<String>) -> Self {
        Self {
            modifier_type,
            labels,
        }
    }

    // /// Check if a function name represents a temporal function
    // fn is_temporal_function(&self, function_name: &str) -> bool {
    //     matches!(
    //         function_name,
    //         "rate"
    //             | "increase"
    //             | "sum_over_time"
    //             | "min_over_time"
    //             | "max_over_time"
    //             | "avg_over_time"
    //             | "count_over_time"
    //             | "quantile_over_time"
    //             | "stddev_over_time"
    //             | "stdvar_over_time"
    //             | "last_over_time"
    //             | "present_over_time"
    //     )
    // }

    // /// Extract label filters from matchers
    // fn extract_label_filters(&self, matchers: &Matchers) -> HashMap<String, String> {
    //     let mut filters = HashMap::new();

    //     for matcher in &matchers.matchers {
    //         // For now, only handle exact equality matches
    //         if matcher.op == MatchOp::Equal {
    //             filters.insert(matcher.name.clone(), matcher.value.clone());
    //         }
    //     }

    //     filters
    // }

    // /// Convert Duration to string representation in PromQL format
    // fn duration_to_string(&self, duration: &std::time::Duration) -> String {
    //     let secs = duration.as_secs();

    //     // Convert to the most appropriate unit, preferring larger units when possible
    //     if secs >= 3600 && secs % 3600 == 0 {
    //         format!("{}h", secs / 3600)
    //     } else if secs >= 60 && secs % 60 == 0 {
    //         format!("{}m", secs / 60)
    //     } else if secs > 0 {
    //         format!("{secs}s")
    //     } else {
    //         // Handle sub-second durations
    //         let millis = duration.as_millis();
    //         if millis > 0 {
    //             format!("{millis}ms")
    //         } else {
    //             "0s".to_string()
    //         }
    //     }
    // }
}
