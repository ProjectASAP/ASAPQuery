//! PromQL query language handler for SimpleEngine.
//!
//! Contains all PromQL-specific context building, pattern matching, binary arithmetic
//! dispatch, range-query handling, and query dispatch.

use super::SimpleEngine;
use super::{
    QueryExecutionContext, QueryMetadata, QueryTimestamps, RangeQueryExecutionContext,
    RangeQueryParams,
};
use crate::data_model::{AggregationIdInfo, KeyByLabelValues, QueryConfig, SchemaConfig};
use crate::engines::query_result::{QueryResult, RangeVectorElement};
use asap_types::query_requirements::QueryRequirements;
use asap_types::utils::normalize_spatial_filter;
use promql_utilities::ast_matching::PromQLMatchResult;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::get_is_collapsable;
use promql_utilities::query_logics::enums::{
    AggregationOperator, PromQLFunction, QueryPatternType, Statistic,
};
use promql_utilities::query_logics::parsing::{
    get_metric_and_spatial_filter, get_spatial_aggregation_output_labels, get_statistics_to_compute,
};
use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, warn};

impl SimpleEngine {
    /// Calculates start timestamp for PromQL queries
    fn calculate_start_timestamp_promql(
        &self,
        end_timestamp: u64,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> u64 {
        match query_pattern_type {
            QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
                let range_seconds = match_result.get_range_duration().unwrap().num_seconds() as u64;
                end_timestamp - (range_seconds * 1000)
            }
            QueryPatternType::OnlySpatial => {
                end_timestamp - (self.prometheus_scrape_interval * 1000)
            }
        }
    }

    /// Calculates and validates query timestamps for PromQL
    fn calculate_query_timestamps_promql(
        &self,
        query_time: u64,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> QueryTimestamps {
        let mut end_timestamp = if let Some(at_modifier) = match_result
            .tokens
            .get("metric")
            .and_then(|t| t.metric.as_ref())
            .and_then(|m| m.at_modifier)
        {
            at_modifier * 1000
        } else {
            query_time
        };

        end_timestamp = self.validate_and_align_end_timestamp(end_timestamp, query_pattern_type);
        let start_timestamp =
            self.calculate_start_timestamp_promql(end_timestamp, query_pattern_type, match_result);

        QueryTimestamps {
            start_timestamp,
            end_timestamp,
        }
    }

    /// Extracts quantile parameter from PromQL match result
    fn extract_quantile_param_promql(
        &self,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> Option<String> {
        let quantile_value = match query_pattern_type {
            QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
                match_result
                    .tokens
                    .get("function_args")
                    .and_then(|token| token.function.as_ref())
                    .and_then(|func| func.args.first())
            }
            QueryPatternType::OnlySpatial => match_result
                .tokens
                .get("aggregation")
                .and_then(|token| token.aggregation.as_ref())
                .and_then(|agg| agg.param.as_ref()),
        };

        quantile_value.map(|s| s.to_string())
    }

    /// Extracts topk k parameter from PromQL match result
    fn extract_topk_param(
        &self,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> Result<String, String> {
        match query_pattern_type {
            QueryPatternType::OnlySpatial => match_result
                .tokens
                .get("aggregation")
                .and_then(|token| token.aggregation.as_ref())
                .and_then(|agg| agg.param.as_ref())
                .map(|s| s.to_string())
                .ok_or_else(|| "Missing k parameter for top-k query".to_string()),
            _ => Err(format!(
                "Top-k statistic is only supported for OnlySpatial pattern, found {:?}",
                query_pattern_type
            )),
        }
    }

    /// Builds query kwargs (quantile, k, etc.) for PromQL queries
    fn build_query_kwargs_promql(
        &self,
        statistic: &Statistic,
        query_pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> Result<HashMap<String, String>, String> {
        let mut query_kwargs = HashMap::new();

        match statistic {
            Statistic::Quantile => {
                let quantile = self
                    .extract_quantile_param_promql(query_pattern_type, match_result)
                    .ok_or_else(|| "Missing quantile parameter for quantile query".to_string())?;
                debug!("Extracted quantile value: {:?}", quantile);
                query_kwargs.insert("quantile".to_string(), quantile);
            }
            Statistic::Topk => {
                let k = self.extract_topk_param(query_pattern_type, match_result)?;
                debug!("Extracted k value: {:?}", k);
                query_kwargs.insert("k".to_string(), k);
            }
            _ => {}
        }

        Ok(query_kwargs)
    }

    /// Finds a query config by structurally comparing `arm_ast` against each
    /// config's parsed query.
    ///
    /// Both the arm AST and each config's query string are first normalized to
    /// the canonical `Display` form produced by `promql_parser`. This ensures
    /// that user-written variants like `"sum(x) by (lbl)"` and the parser's
    /// canonical `"sum by (lbl) (x)"` compare equal.
    pub fn find_query_config_promql_structural(
        &self,
        arm_ast: &promql_parser::parser::Expr,
    ) -> Option<&QueryConfig> {
        let arm_canonical = format!("{}", arm_ast);
        self.inference_config.query_configs.iter().find(|config| {
            let config_canonical = promql_parser::parser::parse(&config.query)
                .map(|ast| format!("{}", ast))
                .unwrap_or_default();
            config_canonical == arm_canonical
        })
    }

    /// Variant of `build_query_execution_context_promql` that accepts a pre-parsed
    /// AST node and a pre-found `QueryConfig`, avoiding redundant parsing and lookup.
    pub fn build_query_execution_context_from_ast(
        &self,
        arm_ast: &promql_parser::parser::Expr,
        query_config: &QueryConfig,
        time: f64,
    ) -> Option<QueryExecutionContext> {
        let query_time = Self::convert_query_time_to_data_time(time);

        let mut found_match = None;
        for (pattern_type, patterns) in &self.controller_patterns {
            for pattern in patterns {
                let match_result = pattern.matches(arm_ast);
                if match_result.matches {
                    found_match = Some((*pattern_type, match_result));
                    break;
                }
            }
            if found_match.is_some() {
                break;
            }
        }

        let (query_pattern_type, match_result) = found_match?;

        let agg_info = self
            .get_aggregation_id_info(query_config)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        self.build_promql_execution_context_tail(
            &match_result,
            query_pattern_type,
            query_time,
            agg_info,
        )
    }

    /// Shared context-building tail for both PromQL context builders.
    ///
    /// Called by `build_query_execution_context_from_ast` and
    /// `build_query_execution_context_promql` after pattern matching and
    /// `agg_info` resolution are complete.  Computes labels, statistics,
    /// kwargs, metadata, query plan, and the final `QueryExecutionContext`.
    fn build_promql_execution_context_tail(
        &self,
        match_result: &PromQLMatchResult,
        query_pattern_type: QueryPatternType,
        query_time: u64,
        agg_info: AggregationIdInfo,
    ) -> Option<QueryExecutionContext> {
        let (metric, spatial_filter) = get_metric_and_spatial_filter(match_result);

        let promql_schema = match &self.inference_config.schema {
            SchemaConfig::PromQL(schema) => schema,
            _ => return None,
        };
        let all_labels = match promql_schema.get_labels(&metric).cloned() {
            Some(labels) => labels,
            None => {
                warn!("No metric configuration found for '{}'", metric);
                return None;
            }
        };

        let mut query_output_labels = match query_pattern_type {
            QueryPatternType::OnlyTemporal => all_labels.clone(),
            QueryPatternType::OnlySpatial => {
                get_spatial_aggregation_output_labels(match_result, &all_labels)
            }
            QueryPatternType::OneTemporalOneSpatial => {
                let temporal_aggregation = match_result.get_function_name().unwrap();
                let spatial_aggregation = match_result.get_aggregation_op().unwrap();
                let collapsable = temporal_aggregation
                    .parse::<PromQLFunction>()
                    .ok()
                    .zip(spatial_aggregation.parse::<AggregationOperator>().ok())
                    .is_some_and(|(f, o)| get_is_collapsable(f, o));
                if collapsable {
                    get_spatial_aggregation_output_labels(match_result, &all_labels)
                } else {
                    all_labels.clone()
                }
            }
        };

        let timestamps =
            self.calculate_query_timestamps_promql(query_time, query_pattern_type, match_result);

        let statistics_to_compute = get_statistics_to_compute(query_pattern_type, match_result);
        if statistics_to_compute.len() != 1 {
            warn!(
                "Expected exactly one statistic to compute, found {}",
                statistics_to_compute.len()
            );
            return None;
        }
        let statistic_to_compute = statistics_to_compute.first().unwrap();

        if *statistic_to_compute == Statistic::Topk {
            let mut new_labels = vec!["__name__".to_string()];
            new_labels.extend(query_output_labels.labels);
            query_output_labels = KeyByLabelNames::new(new_labels);
        }

        let query_kwargs = self
            .build_query_kwargs_promql(statistic_to_compute, query_pattern_type, match_result)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute: *statistic_to_compute,
            query_kwargs,
        };

        let query_plan = self
            .create_store_query_plan(&metric, &timestamps, &agg_info)
            .map_err(|e| {
                warn!("Failed to create store query plan: {}", e);
                e
            })
            .ok()?;

        let do_merge = query_pattern_type == QueryPatternType::OnlyTemporal
            || query_pattern_type == QueryPatternType::OneTemporalOneSpatial;

        let grouping_labels = self
            .streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .map(|config| config.grouping_labels.clone())
            .unwrap_or_else(|| query_output_labels.clone());

        let aggregated_labels = self
            .streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_key)
            .map(|config| config.aggregated_labels.clone())
            .unwrap_or_else(KeyByLabelNames::empty);

        Some(QueryExecutionContext {
            metric,
            metadata,
            store_plan: query_plan,
            agg_info,
            do_merge,
            spatial_filter,
            query_time,
            grouping_labels,
            aggregated_labels,
        })
    }

    /// Recursively builds a DataFusion logical plan for one arm of a binary
    /// arithmetic expression.
    ///
    /// - Leaf arm (supported PromQL pattern): look up config structurally, build
    ///   context, return its `to_logical_plan()` together with the output label names.
    /// - Binary arm: recursively build both sub-arms and combine with
    ///   `build_binary_vector_plan`.
    /// - Scalar literal: returns `None` (handled by the caller separately).
    fn build_arm_logical_plan(
        &self,
        arm_ast: &promql_parser::parser::Expr,
        time: f64,
    ) -> Option<(datafusion::logical_expr::LogicalPlan, Vec<String>)> {
        use crate::engines::logical::plan_builder::build_binary_vector_plan;
        use promql_parser::parser::Expr;

        match arm_ast {
            Expr::NumberLiteral(_) => None, // caller handles scalars
            Expr::Paren(paren) => self.build_arm_logical_plan(&paren.expr, time),
            Expr::Binary(binary) => {
                // Nested binary expression — recurse on both sides
                let (lhs_plan, lhs_labels) = self.build_arm_logical_plan(&binary.lhs, time)?;
                let (rhs_plan, _) = self.build_arm_logical_plan(&binary.rhs, time)?;
                let combined =
                    build_binary_vector_plan(lhs_plan, rhs_plan, &binary.op, lhs_labels.clone())
                        .ok()?;
                Some((combined, lhs_labels))
            }
            other => {
                // Leaf pattern: structural config lookup + context + plan
                let config = self.find_query_config_promql_structural(other)?;
                let ctx = self.build_query_execution_context_from_ast(other, config, time)?;
                let label_names = ctx.metadata.query_output_labels.labels.clone();
                let plan = ctx.to_logical_plan().ok()?;
                Some((plan, label_names))
            }
        }
    }

    /// Handles a binary arithmetic PromQL expression by building a combined
    /// DataFusion plan (vector–vector join or scalar projection) and executing it.
    ///
    /// Returns `None` if any arm is not acceleratable (caller falls back to Prometheus).
    fn handle_binary_expr_promql(
        &self,
        ast: &promql_parser::parser::Expr,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        use crate::engines::logical::plan_builder::{build_binary_vector_plan, build_scalar_plan};
        use promql_parser::parser::Expr;

        let query_time = Self::convert_query_time_to_data_time(time);

        let binary = match ast {
            Expr::Binary(b) => b,
            _ => return None,
        };

        let lhs = binary.lhs.as_ref();
        let rhs = binary.rhs.as_ref();
        let op = &binary.op;

        // Scalar case: either side may be a numeric literal
        let scalar_case: Option<(f64, &Expr, bool)> = match (lhs, rhs) {
            (_, Expr::NumberLiteral(nl)) => Some((nl.val, lhs, false)),
            (Expr::NumberLiteral(nl), _) => Some((nl.val, rhs, true)),
            _ => None,
        };
        if let Some((scalar, vector_arm, scalar_on_left)) = scalar_case {
            let (vector_plan, label_names) = self.build_arm_logical_plan(vector_arm, time)?;
            let combined =
                build_scalar_plan(vector_plan, scalar, op, scalar_on_left, label_names.clone())
                    .ok()?;
            let results = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(self.execute_logical_plan(
                    combined,
                    label_names.clone(),
                    "",
                    &Statistic::Sum,
                ))
            })
            .ok()?;
            return Some((
                KeyByLabelNames::new(label_names),
                QueryResult::vector(results, query_time),
            ));
        }

        // Vector–vector
        let (lhs_plan, lhs_labels) = self.build_arm_logical_plan(lhs, time)?;
        let (rhs_plan, _) = self.build_arm_logical_plan(rhs, time)?;
        let combined = build_binary_vector_plan(lhs_plan, rhs_plan, op, lhs_labels.clone()).ok()?;
        let results = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.execute_logical_plan(
                combined,
                lhs_labels.clone(),
                "",
                &Statistic::Sum,
            ))
        })
        .ok()?;
        let output_labels = KeyByLabelNames::new(lhs_labels);
        Some((output_labels, QueryResult::vector(results, query_time)))
    }

    /// Applies a PromQL binary arithmetic operator to two f64 values.
    fn apply_range_binary_op(
        op: &promql_parser::parser::token::TokenType,
        lhs: f64,
        rhs: f64,
    ) -> f64 {
        use promql_parser::parser::token::{T_ADD, T_DIV, T_MOD, T_MUL, T_POW, T_SUB};
        match op.id() {
            id if id == T_ADD => lhs + rhs,
            id if id == T_SUB => lhs - rhs,
            id if id == T_MUL => lhs * rhs,
            id if id == T_DIV => lhs / rhs,
            id if id == T_MOD => lhs % rhs,
            id if id == T_POW => lhs.powf(rhs),
            _ => f64::NAN,
        }
    }

    /// Recursively builds a range execution context for one arm of a binary arithmetic expression.
    fn build_arm_range_context(
        &self,
        arm_ast: &promql_parser::parser::Expr,
        start: f64,
        end: f64,
        step: f64,
    ) -> Option<(RangeQueryExecutionContext, Vec<String>)> {
        use promql_parser::parser::Expr;

        match arm_ast {
            Expr::NumberLiteral(_) => None, // caller handles scalars
            Expr::Paren(paren) => self.build_arm_range_context(&paren.expr, start, end, step),
            other => {
                let config = self.find_query_config_promql_structural(other)?;
                let base_context =
                    self.build_query_execution_context_from_ast(other, config, end)?;
                let label_names = base_context.metadata.query_output_labels.labels.clone();

                let start_ms = Self::convert_query_time_to_data_time(start);
                let end_ms = Self::convert_query_time_to_data_time(end);
                let step_ms = (step * 1000.0) as u64;

                let tumbling_window_ms = self
                    .streaming_config
                    .get_aggregation_config(base_context.agg_info.aggregation_id_for_value)
                    .map(|c| c.window_size * 1000)?;

                self.validate_range_query_params(start_ms, end_ms, step_ms, tumbling_window_ms)
                    .map_err(|e| {
                        warn!("Range arm query validation failed: {}", e);
                        e
                    })
                    .ok()?;

                let lookback_ms = base_context.store_plan.values_query.end_timestamp
                    - base_context.store_plan.values_query.start_timestamp;

                let buckets_per_step = (step_ms / tumbling_window_ms) as usize;
                let lookback_bucket_count = (lookback_ms / tumbling_window_ms) as usize;

                let mut extended_store_plan = base_context.store_plan.clone();
                extended_store_plan.values_query.start_timestamp =
                    start_ms.saturating_sub(lookback_ms);
                extended_store_plan.values_query.end_timestamp = end_ms;
                extended_store_plan.values_query.is_exact_query = false;

                let range_context = RangeQueryExecutionContext {
                    base: QueryExecutionContext {
                        store_plan: extended_store_plan,
                        ..base_context
                    },
                    range_params: RangeQueryParams {
                        start: start_ms,
                        end: end_ms,
                        step: step_ms,
                    },
                    buckets_per_step,
                    lookback_bucket_count,
                    tumbling_window_ms,
                };

                Some((range_context, label_names))
            }
        }
    }

    /// Handles a binary arithmetic PromQL expression for range queries.
    ///
    /// Evaluates each arm independently over the full range, then joins the
    /// resulting series by label key and applies the arithmetic operator
    /// sample-by-sample at matching timestamps.
    fn handle_binary_expr_range_promql(
        &self,
        ast: &promql_parser::parser::Expr,
        start: f64,
        end: f64,
        step: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        use promql_parser::parser::Expr;

        let binary = match ast {
            Expr::Binary(b) => b,
            _ => return None,
        };

        let lhs = binary.lhs.as_ref();
        let rhs = binary.rhs.as_ref();
        let op = &binary.op;

        // Scalar case: either side may be a numeric literal
        let scalar_case: Option<(f64, &Expr, bool)> = match (lhs, rhs) {
            (_, Expr::NumberLiteral(nl)) => Some((nl.val, lhs, false)),
            (Expr::NumberLiteral(nl), _) => Some((nl.val, rhs, true)),
            _ => None,
        };
        if let Some((scalar, vector_arm, scalar_on_left)) = scalar_case {
            let (ctx, labels) = self.build_arm_range_context(vector_arm, start, end, step)?;
            let results = self.execute_range_query_pipeline(&ctx).ok()?;
            let combined: Vec<RangeVectorElement> = results
                .into_iter()
                .map(|mut elem| {
                    for s in &mut elem.samples {
                        s.value = if scalar_on_left {
                            Self::apply_range_binary_op(op, scalar, s.value)
                        } else {
                            Self::apply_range_binary_op(op, s.value, scalar)
                        };
                    }
                    elem
                })
                .collect();
            return Some((KeyByLabelNames::new(labels), QueryResult::matrix(combined)));
        }

        // Vector-vector: evaluate both arms, join by label key, apply op per matching timestamp
        let (lhs_ctx, lhs_labels) = self.build_arm_range_context(lhs, start, end, step)?;
        let (rhs_ctx, _) = self.build_arm_range_context(rhs, start, end, step)?;
        let lhs_results = self.execute_range_query_pipeline(&lhs_ctx).ok()?;
        let rhs_results = self.execute_range_query_pipeline(&rhs_ctx).ok()?;

        // Build lookup: label_key -> {timestamp -> value} for rhs
        let mut rhs_map: HashMap<KeyByLabelValues, HashMap<u64, f64>> = HashMap::new();
        for elem in rhs_results {
            let ts_map: HashMap<u64, f64> = elem
                .samples
                .iter()
                .map(|s| (s.timestamp, s.value))
                .collect();
            rhs_map.insert(elem.labels, ts_map);
        }

        let mut combined: Vec<RangeVectorElement> = Vec::new();
        for lhs_elem in lhs_results {
            if let Some(rhs_ts_map) = rhs_map.get(&lhs_elem.labels) {
                let mut new_elem = RangeVectorElement::new(lhs_elem.labels.clone());
                for s in &lhs_elem.samples {
                    if let Some(&rhs_val) = rhs_ts_map.get(&s.timestamp) {
                        new_elem.add_sample(
                            s.timestamp,
                            Self::apply_range_binary_op(op, s.value, rhs_val),
                        );
                    }
                }
                if !new_elem.samples.is_empty() {
                    combined.push(new_elem);
                }
            }
        }

        let output_labels = KeyByLabelNames::new(lhs_labels);
        Some((output_labels, QueryResult::matrix(combined)))
    }

    /// Extract QueryRequirements from a parsed PromQL match result.
    /// Used as the fallback path when no query_configs entry is found.
    fn build_query_requirements_promql(
        &self,
        match_result: &PromQLMatchResult,
        query_pattern_type: QueryPatternType,
    ) -> QueryRequirements {
        let (metric, spatial_filter) = get_metric_and_spatial_filter(match_result);

        let statistics = get_statistics_to_compute(query_pattern_type, match_result);

        let data_range_ms = match query_pattern_type {
            QueryPatternType::OnlySpatial => None,
            _ => match_result
                .get_range_duration()
                .map(|d| d.num_seconds() as u64 * 1000),
        };

        let all_labels = match &self.inference_config.schema {
            SchemaConfig::PromQL(schema) => schema
                .get_labels(&metric)
                .cloned()
                .unwrap_or_else(KeyByLabelNames::empty),
            _ => KeyByLabelNames::empty(),
        };

        let grouping_labels = match query_pattern_type {
            QueryPatternType::OnlyTemporal => all_labels,
            QueryPatternType::OnlySpatial | QueryPatternType::OneTemporalOneSpatial => {
                get_spatial_aggregation_output_labels(match_result, &all_labels)
            }
        };

        QueryRequirements {
            metric,
            statistics,
            data_range_ms,
            grouping_labels,
            spatial_filter_normalized: normalize_spatial_filter(&spatial_filter),
        }
    }

    // /// Try to extract sketch query components from a PromQL query string.
    // ///
    // /// Attempts the standard AST parser first. If that fails (e.g. for custom
    // /// sketch-only functions), falls back to a lightweight regex extraction for
    // /// patterns like `func(metric[range])` and `func(number, metric[range])`.
    // /// Extract just the sketch function name from a query without full evaluation.
    // fn extract_sketch_func_name(&self, query: &str) -> Option<String> {
    //     self.parse_sketch_query_components(query)
    //         .map(|c| c.func_name)
    // }

    // fn parse_sketch_query_components(&self, query: &str) -> Option<SketchQueryComponents> {
    //     // --- Path A: standard PromQL parser + pattern matching ---
    //     if let Some(components) = self.parse_sketch_via_ast(query) {
    //         return Some(components);
    //     }

    //     // --- Path B: regex fallback for custom sketch functions ---
    //     self.parse_sketch_via_regex(query)
    // }

    // /// Parse sketch components using the standard PromQL AST parser.
    // fn parse_sketch_via_ast(&self, query: &str) -> Option<SketchQueryComponents> {
    //     let ast = match promql_parser::parser::parse(query) {
    //         Ok(ast) => ast,
    //         Err(_) => return None,
    //     };

    //     let mut found_match = None;
    //     for (pattern_type, patterns) in &self.controller_patterns {
    //         for pattern in patterns {
    //             let match_result = pattern.matches(&ast);
    //             if match_result.matches {
    //                 found_match = Some((*pattern_type, match_result));
    //                 break;
    //             }
    //         }
    //         if found_match.is_some() {
    //             break;
    //         }
    //     }

    //     let (query_pattern_type, match_result) = found_match?;

    //     if query_pattern_type != QueryPatternType::OnlyTemporal {
    //         debug!(
    //             "Sketch query (AST): pattern type {:?} is not OnlyTemporal, skipping for '{}'",
    //             query_pattern_type, query
    //         );
    //         return None;
    //     }

    //     let func_name = match_result.get_function_name()?;
    //     promsketch_store::promsketch_func_map(&func_name)?;

    //     let (metric, spatial_filter) = get_metric_and_spatial_filter(&match_result);
    //     let metric = if spatial_filter.is_empty() {
    //         metric
    //     } else {
    //         format!("{}{{{}}}", metric, spatial_filter)
    //     };

    //     let range_seconds = match_result.get_range_duration()?.num_seconds() as u64;

    //     let args = if func_name == "quantile_over_time" {
    //         self.extract_quantile_param_promql(query_pattern_type, &match_result)
    //             .and_then(|s| s.parse::<f64>().ok())
    //             .unwrap_or(0.5)
    //     } else {
    //         0.0
    //     };

    //     Some(SketchQueryComponents {
    //         func_name,
    //         metric,
    //         range_seconds,
    //         args,
    //     })
    // }

    // /// Regex fallback for custom sketch functions the PromQL parser doesn't know.
    // ///
    // /// Matches two forms:
    // ///   - `func_name(metric[duration])`                  (generic)
    // ///   - `func_name(number, metric[duration])`          (quantile)
    // ///   - `func_name(metric{filter}[duration])`          (with label filter)
    // fn parse_sketch_via_regex(&self, query: &str) -> Option<SketchQueryComponents> {
    //     use regex::Regex;

    //     // quantile form: quantile_over_time(0.5, metric{...}[5m])
    //     let quantile_re =
    //         Regex::new(r"^(\w+)\(\s*([0-9.]+)\s*,\s*(\w+(?:\{[^}]*\})?)\[(\d+)([smhd])\]\s*\)$")
    //             .ok()?;

    //     // generic form: func(metric{...}[5m])
    //     let generic_re =
    //         Regex::new(r"^(\w+)\(\s*(\w+(?:\{[^}]*\})?)\[(\d+)([smhd])\]\s*\)$").ok()?;

    //     if let Some(caps) = quantile_re.captures(query.trim()) {
    //         let func_name = caps[1].to_string();
    //         promsketch_store::promsketch_func_map(&func_name)?;
    //         let args: f64 = caps[2].parse().ok()?;
    //         let metric = caps[3].to_string();
    //         let range_seconds = Self::parse_duration_to_seconds(&caps[4], &caps[5])?;
    //         debug!(
    //             "Sketch query (regex/quantile): parsed {} with metric={}, range={}s, args={}",
    //             func_name, metric, range_seconds, args
    //         );
    //         return Some(SketchQueryComponents {
    //             func_name,
    //             metric,
    //             range_seconds,
    //             args,
    //         });
    //     }

    //     if let Some(caps) = generic_re.captures(query.trim()) {
    //         let func_name = caps[1].to_string();
    //         promsketch_store::promsketch_func_map(&func_name)?;
    //         let metric = caps[2].to_string();
    //         let range_seconds = Self::parse_duration_to_seconds(&caps[3], &caps[4])?;
    //         debug!(
    //             "Sketch query (regex/generic): parsed {} with metric={}, range={}s",
    //             func_name, metric, range_seconds
    //         );
    //         return Some(SketchQueryComponents {
    //             func_name,
    //             metric,
    //             range_seconds,
    //             args: 0.0,
    //         });
    //     }

    //     None
    // }

    // /// Convert a numeric value + unit suffix into seconds.
    // fn parse_duration_to_seconds(value: &str, unit: &str) -> Option<u64> {
    //     let n: u64 = value.parse().ok()?;
    //     let multiplier = match unit {
    //         "s" => 1,
    //         "m" => 60,
    //         "h" => 3600,
    //         "d" => 86400,
    //         _ => return None,
    //     };
    //     Some(n * multiplier)
    // }

    // /// Try to handle a PromQL query via the sketch shortcut path.
    // /// Returns Some if the query is sketch-backed and PromSketchStore is available.
    // /// Returns None to fall through to the precomputed pipeline.
    // fn handle_sketch_query_promql(
    //     &self,
    //     query: &str,
    //     time: f64,
    // ) -> Option<(KeyByLabelNames, QueryResult)> {
    //     let ps = self.promsketch_store.as_ref()?;

    //     let components = match self.parse_sketch_query_components(query) {
    //         Some(c) => c,
    //         None => {
    //             debug!(
    //                 "Sketch query: could not parse sketch components from '{}'",
    //                 query
    //             );
    //             return None;
    //         }
    //     };

    //     let eval_start = Instant::now();

    //     let query_time = Self::convert_query_time_to_data_time(time);
    //     let end = query_time;
    //     let start = end.saturating_sub(components.range_seconds * 1000);

    //     debug!(
    //         "Sketch query: evaluating {}({}) range=[{}, {}] args={}",
    //         components.func_name, components.metric, start, end, components.args
    //     );

    //     let results = match ps.eval_matching(
    //         &components.func_name,
    //         &components.metric,
    //         components.args,
    //         start,
    //         end,
    //     ) {
    //         Ok(r) => r,
    //         Err(e) => {
    //             warn!(
    //                 "Sketch query: eval_matching failed for {}({}): {}",
    //                 components.func_name, components.metric, e
    //             );
    //             ps_metrics::SKETCH_QUERIES_TOTAL
    //                 .with_label_values(&["miss"])
    //                 .inc();
    //             return None;
    //         }
    //     };

    //     if results.is_empty() {
    //         debug!(
    //             "Sketch query: no matching series with data for {}({}), falling through",
    //             components.func_name, components.metric
    //         );
    //         ps_metrics::SKETCH_QUERIES_TOTAL
    //             .with_label_values(&["miss"])
    //             .inc();
    //         return None;
    //     }

    //     ps_metrics::SKETCH_QUERIES_TOTAL
    //         .with_label_values(&["hit"])
    //         .inc();
    //     ps_metrics::SKETCH_QUERY_DURATION.observe(eval_start.elapsed().as_secs_f64());

    //     info!(
    //         "Sketch query: {}({}) returned {} series results",
    //         components.func_name,
    //         components.metric,
    //         results.len()
    //     );

    //     let elements: Vec<InstantVectorElement> = results
    //         .into_iter()
    //         .map(|(labels_str, value)| {
    //             let labels = KeyByLabelValues::new_with_labels(vec![labels_str]);
    //             InstantVectorElement::new(labels, value)
    //         })
    //         .collect();

    //     let output_labels = KeyByLabelNames::new(vec!["__name__".to_string()]);
    //     Some((output_labels, QueryResult::vector(elements, query_time)))
    // }

    pub fn handle_query_promql(
        &self,
        query: String,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let query_start_time = Instant::now();
        debug!("Handling query: {} at time {}", query, time);

        // Check for binary arithmetic before attempting single-query dispatch.
        // Binary expressions won't have a matching query_config, so we handle them here.
        if let Ok(ast) = promql_parser::parser::parse(&query) {
            if matches!(&ast, promql_parser::parser::Expr::Binary(_)) {
                let result = self.handle_binary_expr_promql(&ast, time);
                let total_query_duration = query_start_time.elapsed();
                debug!(
                    "Binary arithmetic query handling took: {:.2}ms",
                    total_query_duration.as_secs_f64() * 1000.0
                );
                return result;
            }
        }

        let context = self.build_query_execution_context_promql(query, time)?;

        debug!(
            "Querying store for metric: {}, aggregation_id: {}, range: [{}, {}]",
            context.metric,
            context.agg_info.aggregation_id_for_value,
            context.store_plan.values_query.start_timestamp,
            context.store_plan.values_query.end_timestamp
        );

        let result = self.execute_context(context, true);

        // Determine query routing order based on function type.
        // USampling functions prefer the precomputed path first (sketch fallback),
        // while EHUniv/EHKLL functions prefer the sketch path first.
        // let prefer_precomputed = self
        //     .extract_sketch_func_name(&query)
        //     .is_some_and(|name| is_usampling_function(&name));

        // if !prefer_precomputed {
        //     // Non-USampling sketch functions: try sketch path first
        //     if let Some(result) = self.handle_sketch_query_promql(&query, time) {
        //         let total_query_duration = query_start_time.elapsed();
        //         debug!(
        //             "Sketch query handling took: {:.2}ms",
        //             total_query_duration.as_secs_f64() * 1000.0
        //         );
        //         return Some(result);
        //     }
        // }

        // // Precomputed pipeline
        // let precomputed_result = (|| -> Option<(KeyByLabelNames, QueryResult)> {
        //     let context = self.build_query_execution_context_promql(query.clone(), time)?;

        //     debug!(
        //         "Querying store for metric: {}, aggregation_id: {}, range: [{}, {}]",
        //         context.metric,
        //         context.agg_info.aggregation_id_for_value,
        //         context.store_plan.values_query.start_timestamp,
        //         context.store_plan.values_query.end_timestamp
        //     );

        //     let results = self
        //         .execute_query_pipeline(&context, true) // PromQL: topk enabled
        //         .map_err(|e| {
        //             warn!("Query execution failed: {}", e);
        //             e
        //         })
        //         .ok()?;

        //     Some((
        //         context.metadata.query_output_labels,
        //         QueryResult::vector(results, context.query_time),
        //     ))
        // })();

        // if precomputed_result.is_some() {
        //     let total_query_duration = query_start_time.elapsed();
        //     debug!(
        //         "Total query handling took: {:.2}ms",
        //         total_query_duration.as_secs_f64() * 1000.0
        //     );
        //     return precomputed_result;
        // }

        // // Fallback: USampling functions try sketch if precomputed had no data
        // if prefer_precomputed {
        //     if let Some(result) = self.handle_sketch_query_promql(&query, time) {
        //         let total_query_duration = query_start_time.elapsed();
        //         debug!(
        //             "Sketch fallback query handling took: {:.2}ms",
        //             total_query_duration.as_secs_f64() * 1000.0
        //         );
        //         return Some(result);
        //     }
        // }

        let total_query_duration = query_start_time.elapsed();
        debug!(
            "Total query handling took: {:.2}ms (no results)",
            total_query_duration.as_secs_f64() * 1000.0
        );
        result
    }

    pub fn build_query_execution_context_promql(
        &self,
        query: String,
        time: f64,
    ) -> Option<QueryExecutionContext> {
        let query_time = Self::convert_query_time_to_data_time(time);

        // Parse PromQL AST using promql-parser crate
        let parse_start_time = Instant::now();
        let ast = match promql_parser::parser::parse(&query) {
            Ok(ast) => {
                let parse_duration = parse_start_time.elapsed();
                debug!(
                    "PromQL parsing took: {:.2}ms",
                    parse_duration.as_secs_f64() * 1000.0
                );
                ast
            }
            Err(e) => {
                warn!("Failed to parse PromQL query '{}': {}", query, e);
                return None;
            }
        };

        let pattern_match_start_time = Instant::now();

        let mut found_match = None;
        for (pattern_type, patterns) in &self.controller_patterns {
            for pattern in patterns {
                debug!(
                    "Trying pattern type: {:?} for query: {}",
                    pattern_type, query
                );
                let match_result = pattern.matches(&ast);
                debug!("Match result: {:?}", match_result);
                if match_result.matches {
                    found_match = Some((*pattern_type, match_result));
                    break;
                }
            }
            if found_match.is_some() {
                break;
            }
        }

        let (query_pattern_type, match_result) = match found_match {
            Some((pt, result)) => {
                let pattern_match_duration = pattern_match_start_time.elapsed();
                debug!(
                    "Pattern matching took: {:.2}ms",
                    pattern_match_duration.as_secs_f64() * 1000.0
                );
                (pt, result)
            }
            None => {
                warn!("No matching pattern found for query: {}", query);
                return None;
            }
        };

        debug!("Found matching query config for: {}", query);

        let query_context_start_time = Instant::now();

        // Resolve aggregation: try pre-configured query_configs first, fall back to capability matching.
        let agg_info: AggregationIdInfo = if let Some(config) = self.find_query_config(&query) {
            self.get_aggregation_id_info(config)
                .map_err(|e| {
                    warn!("{}", e);
                    e
                })
                .ok()?
        } else {
            warn!(
                "No query_config entry for PromQL query '{}'. Attempting capability-based matching.",
                query
            );
            let requirements =
                self.build_query_requirements_promql(&match_result, query_pattern_type);
            self.streaming_config
                .find_compatible_aggregation(&requirements)?
        };

        let result = self.build_promql_execution_context_tail(
            &match_result,
            query_pattern_type,
            query_time,
            agg_info,
        );

        let query_context_duration = query_context_start_time.elapsed();
        debug!(
            "[LATENCY] Query context build: {:.2}ms",
            query_context_duration.as_secs_f64() * 1000.0
        );

        result
    }

    /// Build execution context for range query
    pub fn build_range_query_execution_context_promql(
        &self,
        query: String,
        start: f64,
        end: f64,
        step: f64,
    ) -> Option<RangeQueryExecutionContext> {
        // First, build the base instant query context (reuse existing logic)
        // Use 'end' as the reference time for parsing
        let base_context = self.build_query_execution_context_promql(query, end)?;

        // Convert to milliseconds
        let start_ms = Self::convert_query_time_to_data_time(start);
        let end_ms = Self::convert_query_time_to_data_time(end);
        let step_ms = (step * 1000.0) as u64;

        // Get window size
        let tumbling_window_ms = self
            .streaming_config
            .get_aggregation_config(base_context.agg_info.aggregation_id_for_value)
            .map(|config| config.window_size * 1000)?;

        // Validate parameters
        self.validate_range_query_params(start_ms, end_ms, step_ms, tumbling_window_ms)
            .map_err(|e| {
                warn!("Range query validation failed: {}", e);
                e
            })
            .ok()?;

        // Calculate lookback from the base context's store plan
        let lookback_ms = base_context.store_plan.values_query.end_timestamp
            - base_context.store_plan.values_query.start_timestamp;

        let buckets_per_step = (step_ms / tumbling_window_ms) as usize;
        let lookback_bucket_count = (lookback_ms / tumbling_window_ms) as usize;

        // Modify the store plan to cover the entire range
        let mut extended_store_plan = base_context.store_plan.clone();
        extended_store_plan.values_query.start_timestamp = start_ms.saturating_sub(lookback_ms);
        extended_store_plan.values_query.end_timestamp = end_ms;
        // Range queries always use range fetch, not exact
        extended_store_plan.values_query.is_exact_query = false;

        Some(RangeQueryExecutionContext {
            base: QueryExecutionContext {
                store_plan: extended_store_plan,
                ..base_context
            },
            range_params: RangeQueryParams {
                start: start_ms,
                end: end_ms,
                step: step_ms,
            },
            buckets_per_step,
            lookback_bucket_count,
            tumbling_window_ms,
        })
    }

    /// Main entry point for range queries
    pub fn handle_range_query_promql(
        &self,
        query: String,
        start: f64,
        end: f64,
        step: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        let query_start_time = Instant::now();
        debug!(
            "Handling range query: {} from {} to {} step {}",
            query, start, end, step
        );

        // Check for binary arithmetic before attempting single-query dispatch.
        if let Ok(ast) = promql_parser::parser::parse(&query) {
            if matches!(&ast, promql_parser::parser::Expr::Binary(_)) {
                let result = self.handle_binary_expr_range_promql(&ast, start, end, step);
                let total_duration = query_start_time.elapsed();
                debug!(
                    "Binary arithmetic range query handling took: {:.2}ms",
                    total_duration.as_secs_f64() * 1000.0
                );
                return result;
            }
        }

        let context = self.build_range_query_execution_context_promql(query, start, end, step)?;

        // Execute range query pipeline
        let results: Vec<RangeVectorElement> = self
            .execute_range_query_pipeline(&context)
            .map_err(|e| {
                warn!("Range query execution failed: {}", e);
                e
            })
            .ok()?;

        // // Determine query routing order based on function type.
        // // USampling functions prefer the precomputed path first (sketch fallback),
        // // while EHUniv/EHKLL functions prefer the sketch path first.
        // let prefer_precomputed = self
        //     .extract_sketch_func_name(&query)
        //     .is_some_and(|name| is_usampling_function(&name));

        // if !prefer_precomputed {
        //     // Non-USampling sketch functions: try sketch path first
        //     if let Some(result) = self.handle_sketch_range_query_promql(&query, start, end, step) {
        //         let total_duration = query_start_time.elapsed();
        //         debug!(
        //             "Sketch range query handling took: {:.2}ms",
        //             total_duration.as_secs_f64() * 1000.0
        //         );
        //         return Some(result);
        //     }
        // }

        // // Precomputed pipeline
        // let precomputed_result = (|| -> Option<(KeyByLabelNames, QueryResult)> {
        //     let context =
        //         self.build_range_query_execution_context_promql(query.clone(), start, end, step)?;

        //     let results: Vec<RangeVectorElement> = self
        //         .execute_range_query_pipeline(&context)
        //         .map_err(|e| {
        //             warn!("Range query execution failed: {}", e);
        //             e
        //         })
        //         .ok()?;

        //     Some((
        //         context.base.metadata.query_output_labels,
        //         QueryResult::matrix(results),
        //     ))
        // })();

        // // Fallback: USampling functions try sketch if precomputed had no data
        // if prefer_precomputed {
        //     if let Some(result) = self.handle_sketch_range_query_promql(&query, start, end, step) {
        //         let total_duration = query_start_time.elapsed();
        //         debug!(
        //             "Sketch fallback range query handling took: {:.2}ms",
        //             total_duration.as_secs_f64() * 1000.0
        //         );
        //         return Some(result);
        //     }
        // }

        let total_duration = query_start_time.elapsed();
        debug!(
            "Total range query handling took: {:.2}ms",
            total_duration.as_secs_f64() * 1000.0
        );

        Some((
            context.base.metadata.query_output_labels,
            QueryResult::matrix(results),
        ))
    }
}
