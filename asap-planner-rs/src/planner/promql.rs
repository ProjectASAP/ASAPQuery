use asap_types::enums::CleanupPolicy;
use asap_types::query_requirements::build_query_requirements_promql;
use asap_types::PromQLSchema;
use promql_utilities::ast_matching::PromQLMatchResult;
use promql_utilities::query_logics::enums::{
    AggregationType, PromQLFunction, QueryTreatmentType, Statistic,
};
use promql_utilities::query_logics::parsing::get_metric_and_spatial_filter;

use crate::config::input::{SketchParameterOverrides, WindowingConfig};
use crate::error::ControllerError;
use crate::planner::agg_config::{build_agg_configs_for_statistics, IntermediateAggConfig};
use crate::planner::cleanup::get_cleanup_param;
use crate::planner::patterns::build_patterns;
use crate::planner::sketch::build_sketch_parameters_from_promql;
use crate::planner::window::{set_window_parameters, IntermediateWindowConfig};
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

/// Parse `query` and, if it is a top-level binary arithmetic expression,
/// return its two arms. Returns `None` if the query fails to parse, isn't a
/// `Binary` expression, or uses a comparison/set operator (e.g. `==`, `and`).
///
/// This is the single shared entry point for binary-arm decomposition; both
/// `SingleQueryProcessor::get_binary_arm_queries` and the AQE extractor's
/// leaf decomposition build on it.
pub(crate) fn parse_binary_arms(query: &str) -> Option<(BinaryArm, BinaryArm)> {
    let ast = promql_parser::parser::parse(query).ok()?;
    if let promql_parser::parser::Expr::Binary(binary) = ast {
        if !binary.op.is_comparison_operator() && !binary.op.is_set_operator() {
            let lhs = expr_to_binary_arm(binary.lhs.as_ref());
            let rhs = expr_to_binary_arm(binary.rhs.as_ref());
            return Some((lhs, rhs));
        }
    }
    None
}

pub struct SingleQueryProcessor {
    query: String,
    t_repeat_ms: u64,
    data_ingestion_interval_ms: u64,
    metric_schema: PromQLSchema,
    #[allow(dead_code)]
    streaming_engine: StreamingEngine,
    sketch_parameters: Option<SketchParameterOverrides>,
    range_duration_ms: u64,
    step_ms: u64,
    cleanup_policy: CleanupPolicy,
    windowing: Option<WindowingConfig>,
}

impl SingleQueryProcessor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        query: String,
        t_repeat_ms: u64,
        data_ingestion_interval_ms: u64,
        metric_schema: PromQLSchema,
        streaming_engine: StreamingEngine,
        sketch_parameters: Option<SketchParameterOverrides>,
        range_duration_ms: u64,
        step_ms: u64,
        cleanup_policy: CleanupPolicy,
        windowing: Option<WindowingConfig>,
    ) -> Self {
        Self {
            query,
            t_repeat_ms,
            data_ingestion_interval_ms,
            metric_schema,
            streaming_engine,
            sketch_parameters,
            range_duration_ms,
            step_ms,
            cleanup_policy,
            windowing,
        }
    }

    /// Try to match query and return the match result, or None
    fn match_pattern(&self, ast: &promql_parser::parser::Expr) -> Option<PromQLMatchResult> {
        let patterns = build_patterns();
        for pattern in &patterns {
            let result = pattern.matches(ast);
            if result.matches {
                return Some(result);
            }
        }
        None
    }

    /// Get treatment type (Exact vs Approximate) for a query's statistics.
    /// All constituent statistics for a query (e.g. avg's `[Sum, Count]`)
    /// always agree on approximate-ness, so any one of them suffices.
    fn get_treatment_type(statistics: &[Statistic]) -> QueryTreatmentType {
        debug_assert!(
            statistics
                .iter()
                .all(|s| s.is_approximate() == statistics[0].is_approximate()),
            "all statistics for a query must agree on approximate-ness"
        );
        if statistics[0].is_approximate() {
            QueryTreatmentType::Approximate
        } else {
            QueryTreatmentType::Exact
        }
    }

    /// Returns `Some((lhs, rhs))` if this query is a binary arithmetic expression.
    /// Each arm is either a query string (`BinaryArm::Query`) or a scalar literal
    /// (`BinaryArm::Scalar`). Returns `None` if the query is not a binary expression
    /// or cannot be parsed.
    pub fn get_binary_arm_queries(&self) -> Option<(BinaryArm, BinaryArm)> {
        parse_binary_arms(&self.query)
    }

    /// Create a new processor for an arm query, reusing all parameters from this processor.
    pub fn make_arm_processor(&self, arm_query: String) -> Self {
        SingleQueryProcessor::new(
            arm_query,
            self.t_repeat_ms,
            self.data_ingestion_interval_ms,
            self.metric_schema.clone(),
            self.streaming_engine,
            self.sketch_parameters.clone(),
            self.range_duration_ms,
            self.step_ms,
            self.cleanup_policy,
            self.windowing.clone(),
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
        let match_result = match self.match_pattern(&ast) {
            Some(x) => x,
            None => return true,
        };

        // OnlyTemporal only — a temporal function with no outer spatial
        // aggregation. Deliberately not reduced to a QueryRequirements-based
        // check: doing so would also start applying this point-count-based
        // punting to a bare spatial `quantile()` aggregation, which has never
        // happened before and isn't asked for here (see #508).
        let is_only_temporal = match_result.tokens.contains_key("function")
            && !match_result.tokens.contains_key("aggregation");
        if is_only_temporal {
            let fn_name = match_result.get_function_name().unwrap_or_default();
            let parsed_fn = fn_name.parse::<PromQLFunction>();
            if matches!(
                parsed_fn,
                Ok(PromQLFunction::Rate
                    | PromQLFunction::Increase
                    | PromQLFunction::QuantileOverTime)
            ) {
                let num_data_points =
                    self.t_repeat_ms as f64 / self.data_ingestion_interval_ms as f64;
                if num_data_points < 60.0 {
                    return false;
                }
                if parsed_fn == Ok(PromQLFunction::QuantileOverTime) {
                    if let Some(range_dur) = match_result.get_range_duration() {
                        let range_ms = range_dur.num_milliseconds() as f64;
                        if range_ms / self.t_repeat_ms as f64 > 15.0 {
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

        let match_result = self.match_pattern(&ast).ok_or_else(|| {
            ControllerError::PlannerError(format!("Unsupported query: {}", self.query))
        })?;

        // Statistics, data range, and grouping labels all come from the same
        // canonical derivation query-engine uses (`build_query_requirements_promql`),
        // rather than being independently re-derived here (see #508).
        let requirements = build_query_requirements_promql(
            &self.query,
            &match_result,
            &self.metric_schema,
            self.data_ingestion_interval_ms,
        )
        .ok_or_else(|| {
            ControllerError::PlannerError(format!("Unsupported query: {}", self.query))
        })?;

        let treatment_type = Self::get_treatment_type(&requirements.statistics);

        let (metric, spatial_filter) = get_metric_and_spatial_filter(&match_result);

        let all_labels = self
            .metric_schema
            .get_labels(&metric)
            .ok_or_else(|| ControllerError::UnknownMetric(metric.clone()))?
            .clone();

        let mut window_cfg = IntermediateWindowConfig::default();
        set_window_parameters(
            requirements.data_range_ms,
            self.t_repeat_ms,
            self.data_ingestion_interval_ms,
            self.step_ms,
            &mut window_cfg,
            self.windowing.is_none(),
        )
        .map_err(ControllerError::PlannerError)?;
        crate::planner::window::apply_windowing_override(
            &mut window_cfg,
            requirements.data_range_ms,
            self.step_ms,
            self.windowing.as_ref(),
        )?;

        let subpopulation_labels = requirements.grouping_labels;
        let rollup = all_labels.difference(&subpopulation_labels);

        let configs = build_agg_configs_for_statistics(
            &requirements.statistics,
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
                    requirements.data_range_ms,
                    self.t_repeat_ms,
                    window_cfg.window_type,
                    window_cfg.window_size_ms,
                    window_cfg.slide_interval_ms,
                    self.range_duration_ms,
                    self.step_ms,
                )
                .map_err(ControllerError::PlannerError)?,
            )
        };

        Ok((configs, cleanup_param))
    }
}
