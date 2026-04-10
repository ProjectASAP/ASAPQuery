use asap_types::enums::{CleanupPolicy, WindowType};
use asap_types::PromQLSchema;
use promql_utilities::ast_matching::PromQLMatchResult;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{
    AggregationOperator, AggregationType, PromQLFunction, QueryPatternType, QueryTreatmentType,
    Statistic,
};
use promql_utilities::query_logics::logics::{
    get_is_collapsable, map_statistic_to_precompute_operator,
};
use promql_utilities::query_logics::parsing::{
    get_metric_and_spatial_filter, get_spatial_aggregation_output_labels, get_statistics_to_compute,
};
use serde_json::Value;
use std::collections::HashMap;

use crate::config::input::SketchParameterOverrides;
use crate::error::ControllerError;
use crate::planner::logics::{
    build_sketch_parameters_from_promql, get_cleanup_param, set_subpopulation_labels,
    set_window_parameters, IntermediateWindowConfig,
};
use crate::planner::patterns::build_patterns;
use crate::StreamingEngine;

/// Represents one arm of a binary arithmetic expression in the planner.
#[derive(Debug, Clone)]
pub enum BinaryArm {
    /// A PromQL query expression that may be acceleratable.
    Query(String),
    /// A scalar literal (e.g. `100` in `rate(x[5m]) * 100`).
    Scalar(f64),
}

/// Convert an AST expression to a `BinaryArm`. Scalar literals become
/// `BinaryArm::Scalar`; everything else is serialized to a query string.
/// Outer parentheses are stripped so nested binary arms can be re-parsed
/// as `Binary` expressions (not `Paren`).
fn expr_to_binary_arm(expr: &promql_parser::parser::Expr) -> BinaryArm {
    let inner = strip_parens(expr);
    if let promql_parser::parser::Expr::NumberLiteral(nl) = inner {
        BinaryArm::Scalar(nl.val)
    } else {
        BinaryArm::Query(format!("{}", inner))
    }
}

/// Recursively remove outer `Paren` wrappers from an expression.
fn strip_parens(expr: &promql_parser::parser::Expr) -> &promql_parser::parser::Expr {
    if let promql_parser::parser::Expr::Paren(paren) = expr {
        strip_parens(&paren.expr)
    } else {
        expr
    }
}

/// Internal representation of an aggregation config before IDs are assigned
#[derive(Debug, Clone)]
pub struct IntermediateAggConfig {
    pub aggregation_type: AggregationType,
    pub aggregation_sub_type: String,
    pub window_type: WindowType,
    pub window_size: u64,
    pub slide_interval: u64,
    pub spatial_filter: String,
    pub metric: String,
    pub table_name: Option<String>,
    pub value_column: Option<String>,
    pub parameters: HashMap<String, Value>,
    pub rollup_labels: KeyByLabelNames,
    pub grouping_labels: KeyByLabelNames,
    pub aggregated_labels: KeyByLabelNames,
}

impl IntermediateAggConfig {
    /// Canonical deduplication key matching Python's get_identifying_key()
    pub fn identifying_key(&self) -> String {
        // Build a canonical string representation matching Python's tuple
        let mut params_vec: Vec<(String, String)> = self
            .parameters
            .iter()
            .map(|(k, v)| (k.clone(), v.to_string()))
            .collect();
        params_vec.sort_by_key(|(k, _)| k.clone());

        let mut label_parts = String::new();
        // sorted label keys: aggregated, grouping, rollup
        let mut label_keys = vec!["aggregated", "grouping", "rollup"];
        label_keys.sort();
        for k in label_keys {
            let labels = match k {
                "aggregated" => &self.aggregated_labels,
                "grouping" => &self.grouping_labels,
                "rollup" => &self.rollup_labels,
                _ => unreachable!(),
            };
            label_parts.push_str(&format!("{}:{:?};", k, labels.labels));
        }

        format!(
            "{}|{}|{}|{}|{}|{}|{}|{:?}|{:?}|{:?}|{}",
            self.aggregation_type,
            self.aggregation_sub_type,
            self.window_type,
            self.window_size,
            self.slide_interval,
            self.spatial_filter,
            self.metric,
            self.table_name,
            self.value_column,
            params_vec,
            label_parts,
        )
    }
}

pub struct SingleQueryProcessor {
    query: String,
    t_repeat: u64,
    prometheus_scrape_interval: u64,
    metric_schema: PromQLSchema,
    #[allow(dead_code)]
    streaming_engine: StreamingEngine,
    sketch_parameters: Option<SketchParameterOverrides>,
    range_duration: u64,
    step: u64,
    cleanup_policy: CleanupPolicy,
}

impl SingleQueryProcessor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query: String,
        t_repeat: u64,
        prometheus_scrape_interval: u64,
        metric_schema: PromQLSchema,
        streaming_engine: StreamingEngine,
        sketch_parameters: Option<SketchParameterOverrides>,
        range_duration: u64,
        step: u64,
        cleanup_policy: CleanupPolicy,
    ) -> Self {
        Self {
            query,
            t_repeat,
            prometheus_scrape_interval,
            metric_schema,
            streaming_engine,
            sketch_parameters,
            range_duration,
            step,
            cleanup_policy,
        }
    }

    /// Try to match query and return (pattern_type, match_result) or None
    fn match_pattern(
        &self,
        ast: &promql_parser::parser::Expr,
    ) -> Option<(QueryPatternType, PromQLMatchResult)> {
        let patterns = build_patterns();
        for (pattern_type, pattern) in &patterns {
            let result = pattern.matches(ast);
            if result.matches {
                return Some((*pattern_type, result));
            }
        }
        None
    }

    /// Get treatment type (Exact vs Approximate) from pattern match
    fn get_treatment_type(
        pattern_type: QueryPatternType,
        match_result: &PromQLMatchResult,
    ) -> QueryTreatmentType {
        match pattern_type {
            QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
                let fn_name = match_result.get_function_name().unwrap_or_default();
                match fn_name.parse::<PromQLFunction>() {
                    Ok(f) if f.is_approximate() => QueryTreatmentType::Approximate,
                    _ => QueryTreatmentType::Exact,
                }
            }
            QueryPatternType::OnlySpatial => {
                let op = match_result.get_aggregation_op().unwrap_or_default();
                match op.parse::<AggregationOperator>() {
                    Ok(o) if o.is_approximate() => QueryTreatmentType::Approximate,
                    _ => QueryTreatmentType::Exact,
                }
            }
        }
    }

    /// Returns `Some((lhs, rhs))` if this query is a binary arithmetic expression.
    /// Each arm is either a query string (`BinaryArm::Query`) or a scalar literal
    /// (`BinaryArm::Scalar`). Returns `None` if the query is not a binary expression
    /// or cannot be parsed.
    pub fn get_binary_arm_queries(&self) -> Option<(BinaryArm, BinaryArm)> {
        let ast = promql_parser::parser::parse(&self.query).ok()?;
        if let promql_parser::parser::Expr::Binary(binary) = ast {
            let lhs = expr_to_binary_arm(binary.lhs.as_ref());
            let rhs = expr_to_binary_arm(binary.rhs.as_ref());
            // Only handle arithmetic operators (not comparison or set operators)
            if !binary.op.is_comparison_operator() && !binary.op.is_set_operator() {
                return Some((lhs, rhs));
            }
        }
        None
    }

    /// Create a new processor for an arm query, reusing all parameters from this processor.
    pub fn make_arm_processor(&self, arm_query: String) -> Self {
        SingleQueryProcessor::new(
            arm_query,
            self.t_repeat,
            self.prometheus_scrape_interval,
            self.metric_schema.clone(),
            self.streaming_engine,
            self.sketch_parameters.clone(),
            self.range_duration,
            self.step,
            self.cleanup_policy,
        )
    }

    /// Check if query should be processed (supported pattern)
    pub fn is_supported(&self) -> bool {
        if let Ok(ast) = promql_parser::parser::parse(&self.query) {
            self.match_pattern(&ast).is_some()
        } else {
            false
        }
    }

    /// Check if query should be performant (enable_punting check)
    pub fn should_be_performant(&self) -> bool {
        let ast = match promql_parser::parser::parse(&self.query) {
            Ok(a) => a,
            Err(_) => return false,
        };
        let (pattern_type, match_result) = match self.match_pattern(&ast) {
            Some(x) => x,
            None => return true,
        };

        if pattern_type == QueryPatternType::OnlyTemporal {
            let fn_name = match_result.get_function_name().unwrap_or_default();
            let parsed_fn = fn_name.parse::<PromQLFunction>();
            if matches!(
                parsed_fn,
                Ok(PromQLFunction::Rate
                    | PromQLFunction::Increase
                    | PromQLFunction::QuantileOverTime)
            ) {
                let num_data_points = self.t_repeat as f64 / self.prometheus_scrape_interval as f64;
                if num_data_points < 60.0 {
                    return false;
                }
                if parsed_fn == Ok(PromQLFunction::QuantileOverTime) {
                    if let Some(range_dur) = match_result.get_range_duration() {
                        let range_secs = range_dur.num_seconds() as f64;
                        if range_secs / self.t_repeat as f64 > 15.0 {
                            return false;
                        }
                    }
                }
            }
        }
        true
    }

    /// Generate streaming aggregation configs for this query
    pub fn get_streaming_aggregation_configs(
        &self,
    ) -> Result<(Vec<IntermediateAggConfig>, Option<u64>), ControllerError> {
        let ast = promql_parser::parser::parse(&self.query)
            .map_err(|e| ControllerError::PromQLParse(e.to_string()))?;

        let (pattern_type, match_result) = self.match_pattern(&ast).ok_or_else(|| {
            ControllerError::PlannerError(format!("Unsupported query: {}", self.query))
        })?;

        let treatment_type = Self::get_treatment_type(pattern_type, &match_result);

        let (metric, spatial_filter) = get_metric_and_spatial_filter(&match_result);

        let all_labels = self
            .metric_schema
            .get_labels(&metric)
            .ok_or_else(|| ControllerError::UnknownMetric(metric.clone()))?
            .clone();

        let statistics = get_statistics_to_compute(pattern_type, &match_result);

        let mut window_cfg = IntermediateWindowConfig::default();
        set_window_parameters(
            pattern_type,
            self.t_repeat,
            self.prometheus_scrape_interval,
            "any", // aggregation_type doesn't matter (sliding always false)
            self.step,
            &mut window_cfg,
        );

        let (rollup, subpopulation_labels) =
            get_label_routing(pattern_type, &match_result, &all_labels);

        let configs = build_agg_configs_for_statistics(
            &statistics,
            treatment_type,
            &subpopulation_labels,
            &rollup,
            &window_cfg,
            &metric,
            None,
            None,
            &spatial_filter,
            |agg_type: AggregationType, agg_sub_type: &str| {
                build_sketch_parameters_from_promql(
                    agg_type,
                    agg_sub_type,
                    &match_result,
                    self.sketch_parameters.as_ref(),
                )
            },
        )
        .map_err(ControllerError::PlannerError)?;

        // Calculate cleanup param
        let cleanup_param = if self.cleanup_policy == CleanupPolicy::NoCleanup {
            None
        } else {
            Some(
                get_cleanup_param(
                    self.cleanup_policy,
                    pattern_type,
                    &match_result,
                    self.t_repeat,
                    window_cfg.window_type,
                    self.range_duration,
                    self.step,
                )
                .map_err(ControllerError::PlannerError)?,
            )
        };

        Ok((configs, cleanup_param))
    }
}

/// Returns `(rollup, subpopulation_labels)` for a given PromQL pattern type.
/// These are constant across all statistics in a query, so they are computed
/// once before the per-statistic loop.
fn get_label_routing(
    pattern_type: QueryPatternType,
    match_result: &PromQLMatchResult,
    all_labels: &KeyByLabelNames,
) -> (KeyByLabelNames, KeyByLabelNames) {
    match pattern_type {
        QueryPatternType::OnlyTemporal => (KeyByLabelNames::empty(), all_labels.clone()),
        QueryPatternType::OnlySpatial => {
            // Match Python: if no by/without modifier, spatial_output = [] (rollup gets all labels).
            // promql_utilities::get_spatial_aggregation_output_labels has a topk patch that returns
            // all_labels when there is no modifier, but the Python planner returns [] in that case.
            let has_modifier = match_result
                .tokens
                .get("aggregation")
                .and_then(|t| t.aggregation.as_ref())
                .and_then(|a| a.modifier.as_ref())
                .is_some();
            let spatial_output = if has_modifier {
                get_spatial_aggregation_output_labels(match_result, all_labels)
            } else {
                KeyByLabelNames::empty()
            };
            (all_labels.difference(&spatial_output), spatial_output)
        }
        QueryPatternType::OneTemporalOneSpatial => {
            let fn_name = match_result.get_function_name().unwrap_or_default();
            let agg_op = match_result.get_aggregation_op().unwrap_or_default();
            let collapsable = fn_name
                .parse::<PromQLFunction>()
                .ok()
                .zip(agg_op.parse::<AggregationOperator>().ok())
                .is_some_and(|(f, o)| get_is_collapsable(f, o));
            if !collapsable {
                (KeyByLabelNames::empty(), all_labels.clone())
            } else {
                let spatial_output =
                    get_spatial_aggregation_output_labels(match_result, all_labels);
                (all_labels.difference(&spatial_output), spatial_output)
            }
        }
    }
}

/// Shared per-statistic config builder used by both PromQL and SQL paths.
///
/// `get_params(agg_type, agg_sub_type)` is a closure supplied by the caller
/// that resolves sketch parameters; it is the only thing that differs between
/// the two paths.
#[allow(clippy::too_many_arguments)]
pub fn build_agg_configs_for_statistics(
    statistics: &[Statistic],
    treatment_type: QueryTreatmentType,
    subpopulation_labels: &KeyByLabelNames,
    rollup: &KeyByLabelNames,
    window_cfg: &IntermediateWindowConfig,
    metric: &str,
    table_name: Option<&str>,
    value_column: Option<&str>,
    spatial_filter: &str,
    get_params: impl Fn(AggregationType, &str) -> Result<HashMap<String, Value>, String>,
) -> Result<Vec<IntermediateAggConfig>, String> {
    let mut configs = Vec::new();

    for statistic in statistics.iter().copied() {
        let (agg_type, agg_sub_type) =
            map_statistic_to_precompute_operator(statistic, treatment_type)?;

        let mut grouping = KeyByLabelNames::empty();
        let mut aggregated = KeyByLabelNames::empty();
        set_subpopulation_labels(
            statistic,
            agg_type,
            subpopulation_labels,
            &mut rollup.clone(),
            &mut grouping,
            &mut aggregated,
        );

        if matches!(
            agg_type,
            AggregationType::CountMinSketch | AggregationType::HydraKLL
        ) {
            let delta_params = get_params(AggregationType::DeltaSetAggregator, "")?;
            configs.push(IntermediateAggConfig {
                aggregation_type: AggregationType::DeltaSetAggregator,
                aggregation_sub_type: String::new(),
                window_type: window_cfg.window_type,
                window_size: window_cfg.window_size,
                slide_interval: window_cfg.slide_interval,
                spatial_filter: spatial_filter.to_string(),
                metric: metric.to_string(),
                table_name: table_name.map(str::to_string),
                value_column: value_column.map(str::to_string),
                parameters: delta_params,
                rollup_labels: rollup.clone(),
                grouping_labels: grouping.clone(),
                aggregated_labels: aggregated.clone(),
            });
        }

        let parameters = get_params(agg_type, &agg_sub_type)?;
        configs.push(IntermediateAggConfig {
            aggregation_type: agg_type,
            aggregation_sub_type: agg_sub_type,
            window_type: window_cfg.window_type,
            window_size: window_cfg.window_size,
            slide_interval: window_cfg.slide_interval,
            spatial_filter: spatial_filter.to_string(),
            metric: metric.to_string(),
            table_name: table_name.map(str::to_string),
            value_column: value_column.map(str::to_string),
            parameters,
            rollup_labels: rollup.clone(),
            grouping_labels: grouping,
            aggregated_labels: aggregated,
        });
    }

    Ok(configs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::HashMap;

    fn base_config() -> IntermediateAggConfig {
        IntermediateAggConfig {
            aggregation_type: AggregationType::MultipleIncrease,
            aggregation_sub_type: "rate".to_string(),
            window_type: WindowType::Tumbling,
            window_size: 300,
            slide_interval: 300,
            spatial_filter: String::new(),
            metric: "http_requests_total".to_string(),
            table_name: None,
            value_column: None,
            parameters: HashMap::new(),
            rollup_labels: KeyByLabelNames::new(vec!["instance".to_string()]),
            grouping_labels: KeyByLabelNames::empty(),
            aggregated_labels: KeyByLabelNames::empty(),
        }
    }

    #[test]
    fn identifying_key_is_stable() {
        let cfg = base_config();
        assert_eq!(cfg.identifying_key(), cfg.identifying_key());
    }

    #[test]
    fn identical_configs_have_same_key() {
        assert_eq!(
            base_config().identifying_key(),
            base_config().identifying_key()
        );
    }

    #[test]
    fn different_aggregation_type_produces_different_key() {
        let cfg1 = base_config();
        let mut cfg2 = base_config();
        cfg2.aggregation_type = AggregationType::DatasketchesKLL;
        assert_ne!(cfg1.identifying_key(), cfg2.identifying_key());
    }

    #[test]
    fn different_window_size_produces_different_key() {
        let cfg1 = base_config();
        let mut cfg2 = base_config();
        cfg2.window_size = 60;
        assert_ne!(cfg1.identifying_key(), cfg2.identifying_key());
    }

    #[test]
    fn different_rollup_labels_produce_different_key() {
        let cfg1 = base_config();
        let mut cfg2 = base_config();
        cfg2.rollup_labels = KeyByLabelNames::new(vec!["job".to_string()]);
        assert_ne!(cfg1.identifying_key(), cfg2.identifying_key());
    }

    #[test]
    fn parameter_insertion_order_does_not_affect_key() {
        let mut cfg1 = base_config();
        let mut cfg2 = base_config();
        cfg1.parameters
            .insert("depth".to_string(), Value::Number(3.into()));
        cfg1.parameters
            .insert("width".to_string(), Value::Number(1024.into()));
        cfg2.parameters
            .insert("width".to_string(), Value::Number(1024.into()));
        cfg2.parameters
            .insert("depth".to_string(), Value::Number(3.into()));
        assert_eq!(cfg1.identifying_key(), cfg2.identifying_key());
    }
}
