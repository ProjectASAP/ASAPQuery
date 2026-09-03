//! PromQL query language handler for SimpleEngine.
//!
//! Contains all PromQL-specific context building, pattern matching, binary arithmetic
//! dispatch, range-query handling, and query dispatch.

use super::SimpleEngine;
use super::{QueryExecutionContext, QueryMetadata, QueryTimestamps, RangeQueryExecutionContext};
use crate::data_model::{AggregationIdInfo, KeyByLabelValues, QueryConfig, SchemaConfig};
use crate::engines::query_result::{InstantVectorElement, QueryResult, RangeVectorElement};
use asap_types::query_requirements::build_query_requirements_promql;
use asap_types::PromQLSchema;
use promql_utilities::ast_matching::PromQLMatchResult;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;
use promql_utilities::query_logics::parsing::get_metric_and_spatial_filter;
use std::collections::HashMap;
use std::time::Instant;
use tracing::{debug, warn};

const METRIC_NAME_LABEL: &str = "__name__";

/// Detects whether either side of a PromQL binary expression is a scalar
/// (numeric literal), returning the scalar value, the other (vector) arm,
/// and whether the scalar was on the left. Shared by instant and range
/// binary-expression handling.
fn detect_scalar_arm<'a>(
    lhs: &'a promql_parser::parser::Expr,
    rhs: &'a promql_parser::parser::Expr,
) -> Option<(f64, &'a promql_parser::parser::Expr, bool)> {
    use promql_parser::parser::Expr;
    match (lhs, rhs) {
        (_, Expr::NumberLiteral(nl)) => Some((nl.val, lhs, false)),
        (Expr::NumberLiteral(nl), _) => Some((nl.val, rhs, true)),
        _ => None,
    }
}

/// Vector-vector combiner for one binary-expr level (instant query): joins
/// two arms' results by label key and applies `op` per matching key,
/// dropping non-matches (inner-join semantics, matching DataFusion's
/// `build_binary_vector_plan`). Positional `KeyByLabelValues` equality is
/// safe *once `lhs_labels == rhs_labels` is confirmed*: label names are
/// canonically sorted by `KeyByLabelNames::new()`, so two arms with the same
/// label set always order their values the same way. Returns `None` if the
/// two arms don't share the same label set — mirroring DataFusion's
/// `build_binary_vector_plan`, which fails to resolve a join column that
/// only exists on one side.
fn combine_vector_vector(
    lhs_results: Vec<InstantVectorElement>,
    lhs_labels: &[String],
    rhs_results: Vec<InstantVectorElement>,
    rhs_labels: &[String],
    op: &promql_parser::parser::token::TokenType,
) -> Option<Vec<InstantVectorElement>> {
    if lhs_labels != rhs_labels {
        return None;
    }

    let rhs_map: HashMap<KeyByLabelValues, f64> = rhs_results
        .into_iter()
        .map(|elem| (elem.labels, elem.value))
        .collect();

    Some(
        lhs_results
            .into_iter()
            .filter_map(|lhs_elem| {
                rhs_map.get(&lhs_elem.labels).map(move |&rhs_val| {
                    let value = SimpleEngine::apply_range_binary_op(op, lhs_elem.value, rhs_val);
                    InstantVectorElement::new(lhs_elem.labels, value)
                })
            })
            .collect(),
    )
}

/// Scalar combiner for one binary-expr level (instant query): applies
/// `op(scalar, value)` or `op(value, scalar)` per `scalar_on_left` to every
/// element of the vector arm's results.
fn combine_scalar(
    vector_results: Vec<InstantVectorElement>,
    scalar: f64,
    op: &promql_parser::parser::token::TokenType,
    scalar_on_left: bool,
) -> Vec<InstantVectorElement> {
    vector_results
        .into_iter()
        .map(|elem| {
            let value = if scalar_on_left {
                SimpleEngine::apply_range_binary_op(op, scalar, elem.value)
            } else {
                SimpleEngine::apply_range_binary_op(op, elem.value, scalar)
            };
            InstantVectorElement::new(elem.labels, value)
        })
        .collect()
}

fn binary_matching_label_names(label_names: Vec<String>) -> Vec<String> {
    label_names
        .into_iter()
        .filter(|label| label != METRIC_NAME_LABEL)
        .collect()
}

fn is_supported_binary_arithmetic_op(op: &promql_parser::parser::token::TokenType) -> bool {
    use promql_parser::parser::token::{T_ADD, T_DIV, T_MOD, T_MUL, T_POW, T_SUB};
    matches!(op.id(), T_ADD | T_SUB | T_MUL | T_DIV | T_MOD | T_POW)
}

impl SimpleEngine {
    /// Aligns `end_timestamp` down to the nearest data-ingestion-interval
    /// boundary, unconditionally — mirroring SQL's `align_end_timestamp_sql`.
    /// A no-op in the common case (already-aligned timestamps), a safety net
    /// otherwise, for every PromQL query shape, not just a subset.
    fn align_end_timestamp_promql(&self, end_timestamp: u64) -> u64 {
        let interval_ms = self.data_ingestion_interval_ms;
        if end_timestamp.is_multiple_of(interval_ms) {
            return end_timestamp;
        }
        let aligned = (end_timestamp / interval_ms) * interval_ms;
        warn!(
            "PromQL query end timestamp {} is not aligned with data ingestion interval of {} ms; \
             aligning down to {}.",
            end_timestamp, interval_ms, aligned
        );
        aligned
    }

    /// Calculates and validates query timestamps for PromQL
    fn calculate_query_timestamps_promql(
        &self,
        query_time: u64,
        match_result: &PromQLMatchResult,
        data_range_ms: u64,
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

        end_timestamp = self.align_end_timestamp_promql(end_timestamp);
        let start_timestamp = end_timestamp - data_range_ms;

        QueryTimestamps {
            start_timestamp,
            end_timestamp,
        }
    }

    /// Extracts quantile parameter from PromQL match result. Quantile only
    /// ever appears in exactly one of these two token locations for a given
    /// match (mutually exclusive — collapsable spatial-of-temporal
    /// combinations never involve quantile, see `get_is_collapsable`), so
    /// trying one then the other needs no additional signal to pick between
    /// them — asserted below rather than silently trusted.
    fn extract_quantile_param_promql(&self, match_result: &PromQLMatchResult) -> Option<String> {
        let function_args_quantile = match_result
            .tokens
            .get("function_args")
            .and_then(|token| token.function.as_ref())
            .and_then(|func| func.args.first());
        let aggregation_quantile = match_result
            .tokens
            .get("aggregation")
            .and_then(|token| token.aggregation.as_ref())
            .and_then(|agg| agg.param.as_ref());

        debug_assert!(
            function_args_quantile.is_none() || aggregation_quantile.is_none(),
            "quantile must appear in exactly one of function_args or aggregation, never both"
        );

        function_args_quantile
            .or(aggregation_quantile)
            .map(|s| s.to_string())
    }

    /// Extracts topk k parameter from PromQL match result. Topk is only ever
    /// produced by a spatial `topk` aggregation (excluded from every
    /// collapsable spatial-of-temporal pattern), so no shape check is needed
    /// before looking at the aggregation token.
    fn extract_topk_param(&self, match_result: &PromQLMatchResult) -> Result<String, String> {
        match_result
            .tokens
            .get("aggregation")
            .and_then(|token| token.aggregation.as_ref())
            .and_then(|agg| agg.param.as_ref())
            .map(|s| s.to_string())
            .ok_or_else(|| "Missing k parameter for top-k query".to_string())
    }

    /// Builds query kwargs (quantile, k, etc.) for PromQL queries
    fn build_query_kwargs_promql(
        &self,
        statistic: &Statistic,
        match_result: &PromQLMatchResult,
    ) -> Result<HashMap<String, String>, String> {
        let mut query_kwargs = HashMap::new();

        match statistic {
            Statistic::Quantile => {
                let quantile = self
                    .extract_quantile_param_promql(match_result)
                    .ok_or_else(|| "Missing quantile parameter for quantile query".to_string())?;
                debug!("Extracted quantile value: {:?}", quantile);
                query_kwargs.insert("quantile".to_string(), quantile);
            }
            Statistic::Topk => {
                let k = self.extract_topk_param(match_result)?;
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
    ) -> Option<QueryConfig> {
        let arm_canonical = format!("{}", arm_ast);
        self.inference_config
            .read()
            .unwrap()
            .query_configs
            .iter()
            .find(|config| {
                let config_canonical = promql_parser::parser::parse(&config.query)
                    .map(|ast| format!("{}", ast))
                    .unwrap_or_default();
                config_canonical == arm_canonical
            })
            .cloned()
    }

    /// Scans `self.controller_patterns` for the first `PromQLPattern` that
    /// matches `ast`. `query` is used only for debug logging.
    fn find_matching_controller_pattern(
        &self,
        ast: &promql_parser::parser::Expr,
        query: &str,
    ) -> Option<PromQLMatchResult> {
        for pattern in &self.controller_patterns {
            debug!("Trying pattern for query: {}", query);
            let match_result = pattern.matches(ast);
            debug!("Match result: {:?}", match_result);
            if match_result.matches {
                return Some(match_result);
            }
        }
        None
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

        let match_result = self.find_matching_controller_pattern(arm_ast, &query_config.query)?;

        let agg_info = self
            .get_aggregation_id_info(query_config)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        self.build_promql_execution_context_tail(
            &query_config.query,
            &match_result,
            query_time,
            agg_info,
        )
    }

    /// Shared context-building tail for both PromQL context builders.
    ///
    /// Called by `build_query_execution_context_from_ast` and
    /// `build_query_execution_context_promql` after pattern matching and
    /// `agg_info` resolution are complete. Computes labels, statistics,
    /// kwargs, metadata, query plan, and the final `QueryExecutionContext`.
    ///
    /// Labels, statistic, and data range come from `build_query_requirements_promql`
    /// — the same canonical derivation used by the capability-matching fallback
    /// and by `asap-planner-rs` — rather than being independently re-derived
    /// here. Only quantile/topk kwargs (not part of `QueryRequirements`, since
    /// they don't affect which aggregation satisfies the query) still read
    /// `match_result` directly.
    fn build_promql_execution_context_tail(
        &self,
        query: &str,
        match_result: &PromQLMatchResult,
        query_time: u64,
        agg_info: AggregationIdInfo,
    ) -> Option<QueryExecutionContext> {
        let (metric, spatial_filter) = get_metric_and_spatial_filter(match_result);

        let ic = self.inference_config.read().unwrap();
        let promql_schema = match &ic.schema {
            SchemaConfig::PromQL(schema) => schema,
            _ => return None,
        };
        if promql_schema.get_labels(&metric).is_none() {
            warn!("No metric configuration found for '{}'", metric);
            return None;
        }

        let requirements = build_query_requirements_promql(
            query,
            match_result,
            promql_schema,
            self.data_ingestion_interval_ms,
        )?;

        let mut query_output_labels = requirements.grouping_labels;

        let timestamps = self.calculate_query_timestamps_promql(
            query_time,
            match_result,
            requirements.data_range_ms,
        );

        if requirements.statistics.len() != 1 {
            warn!(
                "Expected exactly one statistic to compute, found {}",
                requirements.statistics.len()
            );
            return None;
        }
        let statistic_to_compute = requirements.statistics[0];

        if statistic_to_compute == Statistic::Topk {
            let mut new_labels = vec![METRIC_NAME_LABEL.to_string()];
            new_labels.extend(query_output_labels.labels);
            query_output_labels = KeyByLabelNames::new(new_labels);
        }

        let query_kwargs = self
            .build_query_kwargs_promql(&statistic_to_compute, match_result)
            .map_err(|e| {
                warn!("{}", e);
                e
            })
            .ok()?;

        let metadata = QueryMetadata {
            query_output_labels: query_output_labels.clone(),
            statistic_to_compute,
            query_kwargs,
        };

        let (query_plan, do_merge, value_window_type) = self
            .create_store_query_plan(&metric, &timestamps, &agg_info)
            .map_err(|e| {
                warn!("Failed to create store query plan: {}", e);
                e
            })
            .ok()?;

        let sc = self.streaming_config.read().unwrap().clone();
        let grouping_labels = sc
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .map(|config| config.grouping_labels.clone())
            .unwrap_or_else(|| query_output_labels.clone());

        let aggregated_labels = sc
            .get_aggregation_config(agg_info.aggregation_id_for_key)
            .map(|config| config.aggregated_labels.clone())
            .unwrap_or_else(KeyByLabelNames::empty);

        Some(QueryExecutionContext {
            metric,
            metadata,
            store_plan: query_plan,
            agg_info,
            value_window_type,
            do_merge,
            spatial_filter,
            query_time,
            grouping_labels,
            aggregated_labels,
        })
    }

    /// Recursively unwraps `Paren`, then structurally resolves a leaf PromQL
    /// arm (i.e. not `Binary` or `NumberLiteral`) to its `QueryConfig` and
    /// base `QueryExecutionContext`. Shared leaf-resolution step for both
    /// `evaluate_binary_arm` (instant) and `build_arm_range_context` (range).
    ///
    /// Returns `None` for `Binary` arms (caller handles recursion) and
    /// `NumberLiteral` arms (caller handles scalars).
    fn resolve_arm_leaf_context(
        &self,
        arm_ast: &promql_parser::parser::Expr,
        time: f64,
    ) -> Option<(QueryExecutionContext, Vec<String>)> {
        use promql_parser::parser::Expr;

        match arm_ast {
            Expr::NumberLiteral(_) | Expr::Binary(_) => None,
            Expr::Paren(paren) => self.resolve_arm_leaf_context(&paren.expr, time),
            other => {
                let config = self.find_query_config_promql_structural(other)?;
                let ctx = self.build_query_execution_context_from_ast(other, &config, time)?;
                let label_names =
                    binary_matching_label_names(ctx.metadata.query_output_labels.labels.clone());
                Some((ctx, label_names))
            }
        }
    }

    /// Recursively evaluates one arm of a binary arithmetic expression via
    /// the native pipeline (`execute_query_pipeline`).
    ///
    /// - Leaf arm (supported PromQL pattern): resolved via `resolve_arm_leaf_context`,
    ///   executed through `execute_query_pipeline`.
    /// - Binary arm: recursively evaluate both sub-arms and combine with
    ///   `combine_vector_vector`. Nested `Binary` arms are combined as
    ///   vector-vector only — a scalar inside a nested arm (e.g. `(a+5)*b`)
    ///   is not supported (tracked separately, same as before this cutover).
    /// - Scalar literal: returns `None` (handled by the caller separately).
    fn evaluate_binary_arm(
        &self,
        arm_ast: &promql_parser::parser::Expr,
        time: f64,
    ) -> Option<(Vec<InstantVectorElement>, Vec<String>)> {
        use promql_parser::parser::Expr;

        match arm_ast {
            Expr::NumberLiteral(_) => None, // caller handles scalars
            Expr::Paren(paren) => self.evaluate_binary_arm(&paren.expr, time),
            Expr::Binary(binary) => {
                if binary.modifier.is_some() {
                    return None;
                }
                if !is_supported_binary_arithmetic_op(&binary.op) {
                    return None;
                }
                // Nested binary expression — recurse on both sides
                let (lhs_results, lhs_labels) = self.evaluate_binary_arm(&binary.lhs, time)?;
                let (rhs_results, rhs_labels) = self.evaluate_binary_arm(&binary.rhs, time)?;
                let combined = combine_vector_vector(
                    lhs_results,
                    &lhs_labels,
                    rhs_results,
                    &rhs_labels,
                    &binary.op,
                )?;
                Some((combined, lhs_labels))
            }
            _ => {
                let (ctx, label_names) = self.resolve_arm_leaf_context(arm_ast, time)?;
                // Unlike DataFusion's PrecomputedSummaryReadExec (which streamed
                // whatever rows existed, including zero, so a currently-empty arm
                // used to return Some(empty vector)), execute_query_pipeline errors
                // when the store has no precomputed outputs at all for this arm —
                // that propagates to None here, triggering a full Prometheus
                // fallback for the whole expression instead of an empty result
                // for just this arm. Accepted behavior change (#567); warn loudly
                // so it's visible rather than silent.
                let results = self
                    // Binary arms need Topk limiting, but must remain in the
                    // unformatted intermediate label representation until
                    // after the binary join.
                    .execute_query_pipeline(&ctx, true, false)
                    .map_err(|e| {
                        warn!(
                            "Binary-expr arm for metric '{}' failed ({}) — \
                             falls back to Prometheus for the whole expression rather than \
                             returning an empty result for just this arm",
                            ctx.metric, e
                        );
                        e
                    })
                    .ok()?;
                Some((results, label_names))
            }
        }
    }

    /// Handles a binary arithmetic PromQL expression via the native pipeline
    /// (vector–vector join or scalar combine).
    ///
    /// Returns `None` if any arm is not acceleratable (caller falls back to Prometheus).
    fn handle_binary_expr_promql(
        &self,
        ast: &promql_parser::parser::Expr,
        time: f64,
    ) -> Option<(KeyByLabelNames, QueryResult)> {
        use promql_parser::parser::Expr;

        let query_time = Self::convert_query_time_to_data_time(time);

        let binary = match ast {
            Expr::Binary(b) => b,
            _ => return None,
        };

        if !is_supported_binary_arithmetic_op(&binary.op) {
            return None;
        }
        if binary.modifier.is_some() {
            return None;
        }

        let lhs = binary.lhs.as_ref();
        let rhs = binary.rhs.as_ref();
        let op = &binary.op;

        if let Some((scalar, vector_arm, scalar_on_left)) = detect_scalar_arm(lhs, rhs) {
            let (vector_results, label_names) = self.evaluate_binary_arm(vector_arm, time)?;
            let combined = combine_scalar(vector_results, scalar, op, scalar_on_left);
            return Some((
                KeyByLabelNames::new(label_names),
                QueryResult::vector(combined, query_time),
            ));
        }

        // Vector–vector
        let (lhs_results, lhs_labels) = self.evaluate_binary_arm(lhs, time)?;
        let (rhs_results, rhs_labels) = self.evaluate_binary_arm(rhs, time)?;
        let combined =
            combine_vector_vector(lhs_results, &lhs_labels, rhs_results, &rhs_labels, op)?;
        let output_labels = KeyByLabelNames::new(lhs_labels);
        Some((output_labels, QueryResult::vector(combined, query_time)))
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

    /// Extends an instant `QueryExecutionContext` into a `RangeQueryExecutionContext`:
    /// computes the lookback window from the aggregation's tumbling window size,
    /// validates the range params, and widens the store plan to cover
    /// `[start - lookback, end]` as a range (non-exact) fetch.
    ///
    /// Shared tail for `build_arm_range_context` (one binary-expr arm) and
    /// `build_range_query_execution_context_from_parsed` (the whole query) —
    /// they differ only in how `base_context` itself is obtained.
    fn finish_range_context(
        &self,
        base_context: QueryExecutionContext,
        start: f64,
        end: f64,
        step: f64,
    ) -> Option<RangeQueryExecutionContext> {
        let start_ms = Self::convert_query_time_to_data_time(start);
        let end_ms = Self::convert_query_time_to_data_time(end);
        let step_ms = (step * 1000.0) as u64;

        let (tumbling_window_ms, window_type, window_size_ms) = {
            let sc = self.streaming_config.read().unwrap();
            let config =
                sc.get_aggregation_config(base_context.agg_info.aggregation_id_for_value)?;
            (
                Self::bucket_step_ms(config),
                config.window_type,
                config.window_size_ms,
            )
        };

        self.validate_range_query_params(start_ms, end_ms, step_ms, tumbling_window_ms)
            .map_err(|e| {
                warn!("Range query validation failed: {}", e);
                e
            })
            .ok()?;

        // Widening the fetch range to cover the whole step span (rather than
        // one window's width) is what makes this a window-grid walk instead
        // of a single exact lookup -- there's no separate flag to set for
        // that; it falls out of execute_store_query's range-driven behavior.
        let mut extended_store_plan = base_context.store_plan.clone();
        let lookback_ms =
            Self::widen_query_window(&mut extended_store_plan.values_query, start_ms, end_ms);

        let buckets_per_step = (step_ms / tumbling_window_ms) as usize;
        let lookback_bucket_count = (lookback_ms / tumbling_window_ms) as usize;

        // #583: widen keys_query the same way, using the instant window
        // create_keys_query_params already computed for it as the source of
        // truth for "how far back does this aggregation type look." This
        // needs no AggregationType branching: for SetAggregator the instant
        // window is [end-window_size, end], so keys_lookback_ms == window_size
        // and this widens to a normal sliding window; for DeltaSetAggregator
        // the instant window is [0, end], so keys_lookback_ms == end_ms,
        // which saturating_sub's to 0 for every current_time <= end_ms in
        // the per-step loop -- i.e. "replay from the beginning," for free.
        let keys_lookback_ms = extended_store_plan
            .keys_query
            .as_mut()
            .map(|keys_query| Self::widen_query_window(keys_query, start_ms, end_ms));
        let (keys_tumbling_window_ms, keys_window_type, keys_window_size_ms) =
            match keys_lookback_ms {
                Some(_) => {
                    let sc = self.streaming_config.read().unwrap();
                    let config =
                        sc.get_aggregation_config(base_context.agg_info.aggregation_id_for_key)?;
                    (
                        Some(Self::bucket_step_ms(config)),
                        Some(config.window_type),
                        Some(config.window_size_ms),
                    )
                }
                None => (None, None, None),
            };
        // A zero window_size_ms would make execute_range_query_pipeline's
        // per-step sum_window (`while t < window_end { ...; t += step_increment }`)
        // loop forever, since t would never advance. The value side is
        // accidentally protected from this by validate_range_query_params's
        // `step.is_multiple_of(tumbling_window_ms)` check (only 0 is a
        // multiple of 0); keys has no equivalent check, so guard explicitly.
        if keys_tumbling_window_ms == Some(0) {
            warn!("Range query validation failed: key aggregation window_size_ms is 0");
            return None;
        }

        Some(RangeQueryExecutionContext {
            base: QueryExecutionContext {
                store_plan: extended_store_plan,
                ..base_context
            },
            // validate_range_query_params (above) already guarantees
            // step_ms > 0 and start_ms < end_ms, so this matches the old
            // per-step loop's `current_time` sequence exactly: start_ms,
            // start_ms+step_ms, ..., the last value <= end_ms.
            output_timestamps: (start_ms..=end_ms).step_by(step_ms as usize).collect(),
            query_range_ms: lookback_ms,
            buckets_per_step,
            lookback_bucket_count,
            tumbling_window_ms,
            window_type,
            window_size_ms,
            keys_window_type,
            keys_window_size_ms,
            keys_lookback_ms,
            keys_tumbling_window_ms,
        })
    }

    /// Recursively builds a range execution context for one arm of a binary
    /// arithmetic expression.
    ///
    /// Leaf resolution (Paren-unwrap + structural config lookup) is shared
    /// with `evaluate_binary_arm` via `resolve_arm_leaf_context`. Note this
    /// does not support nested `Binary` arms (e.g. `(a+b)*c` over a range) —
    /// tracked separately in #516.
    fn build_arm_range_context(
        &self,
        arm_ast: &promql_parser::parser::Expr,
        start: f64,
        end: f64,
        step: f64,
    ) -> Option<(RangeQueryExecutionContext, Vec<String>)> {
        use promql_parser::parser::Expr;

        if matches!(arm_ast, Expr::NumberLiteral(_)) {
            return None; // caller handles scalars
        }

        let (base_context, label_names) = self.resolve_arm_leaf_context(arm_ast, end)?;
        let range_context = self.finish_range_context(base_context, start, end, step)?;

        Some((range_context, label_names))
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

        if !is_supported_binary_arithmetic_op(&binary.op) {
            return None;
        }
        if binary.modifier.is_some() {
            return None;
        }

        let lhs = binary.lhs.as_ref();
        let rhs = binary.rhs.as_ref();
        let op = &binary.op;

        if let Some((scalar, vector_arm, scalar_on_left)) = detect_scalar_arm(lhs, rhs) {
            let (ctx, labels) = self.build_arm_range_context(vector_arm, start, end, step)?;
            // Binary arms need Topk limiting, but must remain in the
            // unformatted intermediate label representation until after the
            // arithmetic operation.
            let results = self.execute_range_query_pipeline(&ctx, true, false).ok()?;
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

        // Vector-vector: evaluate both arms, join by label key, apply op per matching timestamp.
        // Reject mismatched label sets up front — same guard as the instant-query
        // combine_vector_vector, and for the same reason: positional
        // KeyByLabelValues equality below is only safe once the label *names*
        // match (they're canonically sorted by KeyByLabelNames::new(), so two
        // arms with the same label set always order their values the same way).
        let (lhs_ctx, lhs_labels) = self.build_arm_range_context(lhs, start, end, step)?;
        let (rhs_ctx, rhs_labels) = self.build_arm_range_context(rhs, start, end, step)?;
        if lhs_labels != rhs_labels {
            return None;
        }
        // Binary arms need Topk limiting, but not final presentation formatting.
        let lhs_results = self
            .execute_range_query_pipeline(&lhs_ctx, true, false)
            .ok()?;
        let rhs_results = self
            .execute_range_query_pipeline(&rhs_ctx, true, false)
            .ok()?;

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

        let ast = match promql_parser::parser::parse(&query) {
            Ok(ast) => ast,
            Err(e) => {
                warn!("Failed to parse PromQL query '{}': {}", query, e);
                return None;
            }
        };

        // Check for binary arithmetic before attempting single-query dispatch.
        // Binary expressions won't have a matching query_config, so we handle them here.
        if matches!(&ast, promql_parser::parser::Expr::Binary(_)) {
            let result = self.handle_binary_expr_promql(&ast, time);
            let total_query_duration = query_start_time.elapsed();
            debug!(
                "Binary arithmetic query handling took: {:.2}ms",
                total_query_duration.as_secs_f64() * 1000.0
            );
            return result;
        }

        let context = self.build_query_execution_context_from_parsed(&ast, &query, time)?;

        debug!(
            "Querying store for metric: {}, aggregation_id: {}, range: [{}, {}]",
            context.metric,
            context.agg_info.aggregation_id_for_value,
            context.store_plan.values_query.start_timestamp,
            context.store_plan.values_query.end_timestamp
        );

        let result = self.execute_context(context, true, true);

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

        self.build_query_execution_context_from_parsed(&ast, &query, time)
    }

    /// Variant of `build_query_execution_context_promql` that accepts an
    /// already-parsed AST, avoiding a redundant re-parse when the caller
    /// (e.g. `handle_query_promql`) has already parsed `query` once.
    fn build_query_execution_context_from_parsed(
        &self,
        ast: &promql_parser::parser::Expr,
        query: &str,
        time: f64,
    ) -> Option<QueryExecutionContext> {
        let query_time = Self::convert_query_time_to_data_time(time);

        let pattern_match_start_time = Instant::now();

        let found_match = self.find_matching_controller_pattern(ast, query);

        let match_result = match found_match {
            Some(result) => {
                let pattern_match_duration = pattern_match_start_time.elapsed();
                debug!(
                    "Pattern matching took: {:.2}ms",
                    pattern_match_duration.as_secs_f64() * 1000.0
                );
                result
            }
            None => {
                warn!("No matching pattern found for query: {}", query);
                return None;
            }
        };

        debug!("Found matching query config for: {}", query);

        let query_context_start_time = Instant::now();

        // Resolve aggregation: try pre-configured query_configs first, fall back to capability matching.
        let agg_info: AggregationIdInfo = if let Some(config) = self.find_query_config(query) {
            self.get_aggregation_id_info(&config)
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
            let inference_config = self.inference_config.read().unwrap();
            let empty_schema = PromQLSchema::new();
            let metric_schema = match &inference_config.schema {
                SchemaConfig::PromQL(schema) => schema,
                _ => &empty_schema,
            };
            let requirements = build_query_requirements_promql(
                query,
                &match_result,
                metric_schema,
                self.data_ingestion_interval_ms,
            )?;
            self.streaming_config
                .read()
                .unwrap()
                .clone()
                .find_compatible_aggregation(&requirements)
                .unwrap_or_else(|error| panic!("capability matching failed: {error}"))?
        };

        let result =
            self.build_promql_execution_context_tail(query, &match_result, query_time, agg_info);

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
        let ast = match promql_parser::parser::parse(&query) {
            Ok(ast) => ast,
            Err(e) => {
                warn!("Failed to parse PromQL query '{}': {}", query, e);
                return None;
            }
        };

        self.build_range_query_execution_context_from_parsed(&ast, &query, start, end, step)
    }

    /// Variant of `build_range_query_execution_context_promql` that accepts an
    /// already-parsed AST, avoiding a redundant re-parse when the caller
    /// (e.g. `handle_range_query_promql`) has already parsed `query` once.
    fn build_range_query_execution_context_from_parsed(
        &self,
        ast: &promql_parser::parser::Expr,
        query: &str,
        start: f64,
        end: f64,
        step: f64,
    ) -> Option<RangeQueryExecutionContext> {
        // First, build the base instant query context (reuse existing logic)
        // Use 'end' as the reference time for parsing
        let base_context = self.build_query_execution_context_from_parsed(ast, query, end)?;

        self.finish_range_context(base_context, start, end, step)
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

        let ast = match promql_parser::parser::parse(&query) {
            Ok(ast) => ast,
            Err(e) => {
                warn!("Failed to parse PromQL query '{}': {}", query, e);
                return None;
            }
        };

        // Check for binary arithmetic before attempting single-query dispatch.
        if matches!(&ast, promql_parser::parser::Expr::Binary(_)) {
            let result = self.handle_binary_expr_range_promql(&ast, start, end, step);
            let total_duration = query_start_time.elapsed();
            debug!(
                "Binary arithmetic range query handling took: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );
            return result;
        }

        let context =
            self.build_range_query_execution_context_from_parsed(&ast, &query, start, end, step)?;

        // Execute range query pipeline. (true, true): self-gated, same as
        // instant's handle_query_promql -- both flags are no-ops unless this
        // query's statistic is Topk.
        let results: Vec<RangeVectorElement> = self
            .execute_range_query_pipeline(&context, true, true)
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

/// End-to-end tests for PromQL `topk(k, …)` served by `CountMinSketchWithHeap`.
///
/// Exercises the PromQL half of the top-k flag split:
/// `execute_query_pipeline(ctx, true, true)`. Unlike SQL (`true, false`), PromQL
/// enables formatting so each result row carries the metric name as the first
/// label value (Prometheus series shape).
#[cfg(test)]
mod topk_pipeline_tests {
    use super::SimpleEngine;
    use crate::data_model::{
        AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
        PrecomputedOutput, PromQLSchema, QueryConfig, QueryLanguage, SchemaConfig, StreamingConfig,
        WindowType,
    };
    use crate::engines::QueryResult;
    use crate::precompute_operators::CountMinSketchWithHeapAccumulator;
    use crate::stores::simple_map_store::SimpleMapStore;
    use crate::stores::Store;
    use crate::utils::http::convert_query_result_to_prometheus;
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::Statistic;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    const AGG_ID: u64 = 101;
    const METRIC: &str = "transfer_events";
    // Aligned to the 1s scrape interval (multiple of 1000ms).
    const QUERY_TIME: f64 = 1_759_276_810.0;
    const TOPK_QUERY: &str = "topk(10, transfer_events)";

    #[test]
    fn promql_topk_requirements_are_explicitly_value_weighted() {
        let (engine, _store) = build_topk_engine();
        let ast = promql_parser::parser::parse(TOPK_QUERY).unwrap();
        let match_result = engine
            .find_matching_controller_pattern(&ast, TOPK_QUERY)
            .expect("topk query should match");
        let schema = PromQLSchema::new().add_metric(
            METRIC.to_string(),
            KeyByLabelNames::new(vec!["srcip".to_string()]),
        );
        let requirements =
            asap_types::build_query_requirements_promql(TOPK_QUERY, &match_result, &schema, 1000)
                .expect("topk requirements should build");

        assert_eq!(requirements.topk_count_events, Some(false));
    }

    fn build_topk_engine() -> (SimpleEngine, Arc<SimpleMapStore>) {
        let promql_schema = PromQLSchema::new().add_metric(
            METRIC.to_string(),
            KeyByLabelNames::new(vec!["srcip".to_string()]),
        );

        let query_config = QueryConfig::new(TOPK_QUERY.to_string())
            .add_aggregation(AggregationReference::new(AGG_ID, None));

        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(promql_schema),
            query_configs: vec![query_config],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };

        let agg_config = AggregationConfig {
            aggregation_id: AGG_ID,
            aggregation_type: AggregationType::CountMinSketchWithHeap,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::empty(),
            aggregated_labels: KeyByLabelNames::new(vec!["srcip".to_string()]),
            rollup_labels: KeyByLabelNames::empty(),
            original_yaml: String::new(),
            window_size_ms: 1000,
            slide_interval_ms: 1000,
            window_type: WindowType::Tumbling,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: METRIC.to_string(),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        };

        let mut agg_configs = HashMap::new();
        agg_configs.insert(AGG_ID, agg_config);
        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs: agg_configs,
        });

        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));

        let engine = SimpleEngine::new(
            store.clone(),
            inference_config,
            streaming_config,
            1000,
            QueryLanguage::promql,
        );
        (engine, store)
    }

    #[test]
    fn detects_topk_and_resolves_self_keyed_heap() {
        let (engine, _store) = build_topk_engine();
        let context = engine
            .build_query_execution_context_promql(TOPK_QUERY.to_string(), QUERY_TIME)
            .expect("topk(k, metric) should build a context via the query_config path");

        assert_eq!(
            context.metadata.statistic_to_compute,
            Statistic::Topk,
            "topk(...) must resolve to Statistic::Topk",
        );
        assert_eq!(
            context.metadata.query_kwargs.get("k").map(String::as_str),
            Some("10"),
            "the topk k argument should be threaded through as the `k` kwarg",
        );
        assert_eq!(
            context.agg_info.aggregation_id_for_key,
            context.agg_info.aggregation_id_for_value,
        );
        assert!(context.store_plan.keys_query.is_none());
        assert_eq!(
            context.metadata.query_output_labels.labels,
            vec!["__name__".to_string(), "srcip".to_string()],
            "topk PromQL rows zip to {{ __name__, srcip }} in the wire format",
        );
    }

    #[test]
    fn returns_top_k_srcips_sorted_descending_with_metric_prefix() {
        let (engine, store) = build_topk_engine();

        let context = engine
            .build_query_execution_context_promql(TOPK_QUERY.to_string(), QUERY_TIME)
            .expect("context should build");
        let window = &context.store_plan.values_query;

        let mut sketch = CountMinSketchWithHeapAccumulator::new(3, 1024, 32);
        for i in 1..=15u64 {
            let srcip = format!("10.0.0.{i}");
            sketch.inner.update(&srcip, (i * 10) as f64);
        }

        let output =
            PrecomputedOutput::new(window.start_timestamp, window.end_timestamp, None, AGG_ID);
        store
            .insert_precomputed_output(output, Box::new(sketch))
            .expect("insert should succeed");

        let results = engine
            .execute_query_pipeline(&context, true, true)
            .expect("pipeline should produce results");

        assert_eq!(results.len(), 10, "topk(10, ...) must truncate to 10 rows");

        for pair in results.windows(2) {
            assert!(
                pair[0].value >= pair[1].value,
                "results must be sorted by count descending: {} then {}",
                pair[0].value,
                pair[1].value,
            );
        }

        assert_eq!(
            results[0].labels.labels,
            vec![METRIC.to_string(), "10.0.0.15".to_string()],
        );
        assert_eq!(results[0].value, 150.0);
        for element in &results {
            assert_eq!(
                element.labels.labels.len(),
                2,
                "PromQL top-k rows carry the metric-name prefix plus the srcip",
            );
            assert_eq!(
                element.labels.labels[0], METRIC,
                "first label value must be the metric name (PromQL formatting)",
            );
        }

        let returned: HashSet<String> =
            results.iter().map(|e| e.labels.labels[1].clone()).collect();
        let expected: HashSet<String> = (6..=15u64).map(|i| format!("10.0.0.{i}")).collect();
        assert_eq!(returned, expected);

        // Wire format: zip label names with values into Prometheus instant-vector JSON.
        let output_labels = context.metadata.query_output_labels.clone();
        let query_result = QueryResult::vector(results, context.query_time);
        let prometheus_data = convert_query_result_to_prometheus(&query_result, &output_labels)
            .expect("pipeline output should convert to Prometheus instant-vector JSON");

        assert_eq!(prometheus_data["resultType"], "vector");
        let wire_rows = prometheus_data["result"]
            .as_array()
            .expect("result must be an array");
        assert_eq!(wire_rows.len(), 10);

        let top_row = &wire_rows[0];
        assert_eq!(top_row["metric"]["__name__"], METRIC);
        assert_eq!(top_row["metric"]["srcip"], "10.0.0.15");
        assert_eq!(top_row["value"][0], QUERY_TIME);
        assert_eq!(top_row["value"][1], "150");

        for row in wire_rows {
            assert_eq!(row["metric"]["__name__"], METRIC);
            assert!(row["metric"]["srcip"].is_string());
            assert!(row["value"][1].is_string());
        }

        // Descending order is preserved in the wire format.
        let wire_values: Vec<f64> = wire_rows
            .iter()
            .map(|row| row["value"][1].as_str().unwrap().parse::<f64>().unwrap())
            .collect();
        for pair in wire_values.windows(2) {
            assert!(pair[0] >= pair[1]);
        }
    }

    /// A topk leaf wrapped in an arithmetic binary expr (`topk(10, ...) + 0`)
    /// must still truncate to the top 10, while arithmetic output drops the
    /// metric name. Binary-arm evaluation must not apply standalone Topk
    /// presentation formatting before the arithmetic operation.
    #[test]
    fn topk_wrapped_in_binary_expr_truncates_without_metric_name() {
        let (engine, store) = build_topk_engine();

        let context = engine
            .build_query_execution_context_promql(TOPK_QUERY.to_string(), QUERY_TIME)
            .expect("context should build");
        let window = &context.store_plan.values_query;

        let mut sketch = CountMinSketchWithHeapAccumulator::new(3, 1024, 32);
        for i in 1..=15u64 {
            let srcip = format!("10.0.0.{i}");
            sketch.inner.update(&srcip, (i * 10) as f64);
        }

        let output =
            PrecomputedOutput::new(window.start_timestamp, window.end_timestamp, None, AGG_ID);
        store
            .insert_precomputed_output(output, Box::new(sketch))
            .expect("insert should succeed");

        let (_, query_result) = engine
            .handle_query_promql(format!("{TOPK_QUERY} + 0"), QUERY_TIME)
            .expect("binary-expr-wrapped topk should still resolve");

        let results = match query_result {
            QueryResult::Vector(iv) => iv.values,
            other => panic!("expected a vector result, got {other:?}"),
        };

        assert_eq!(
            results.len(),
            10,
            "topk(10, ...) + 0 must still truncate to 10 rows"
        );
        for pair in results.windows(2) {
            assert!(
                pair[0].value >= pair[1].value,
                "results must stay sorted by count descending"
            );
        }
        assert_eq!(results[0].labels.labels, vec!["10.0.0.15".to_string()],);
        assert_eq!(results[0].value, 150.0);
        for element in &results {
            assert_eq!(element.labels.labels.len(), 1);
        }
    }
}
