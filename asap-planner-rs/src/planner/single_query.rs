use std::collections::HashMap;
use promql_utilities::ast_matching::PromQLMatchResult;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{QueryPatternType, QueryTreatmentType, Statistic};
use promql_utilities::query_logics::logics::{get_is_collapsable, map_statistic_to_precompute_operator};
use promql_utilities::query_logics::parsing::{
    get_metric_and_spatial_filter, get_spatial_aggregation_output_labels, get_statistics_to_compute,
};
use sketch_db_common::enums::CleanupPolicy;
use sketch_db_common::PromQLSchema;
use serde_json::Value;

use crate::config::input::SketchParameterOverrides;
use crate::error::ControllerError;
use crate::planner::logics::{
    get_cleanup_param, get_precompute_operator_parameters, set_subpopulation_labels,
    set_window_parameters, IntermediateWindowConfig,
};
use crate::planner::patterns::build_patterns;
use crate::StreamingEngine;

/// Internal representation of an aggregation config before IDs are assigned
#[derive(Debug, Clone)]
pub struct IntermediateAggConfig {
    pub aggregation_type: String,
    pub aggregation_sub_type: String,
    pub window_type: String,
    pub window_size: u64,
    pub slide_interval: u64,
    pub tumbling_window_size: u64,
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
            label_parts.push_str(&format!(
                "{}:{:?};",
                k,
                labels.labels
            ));
        }

        format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{:?}|{:?}|{:?}|{}",
            self.aggregation_type,
            self.aggregation_sub_type,
            self.window_type,
            self.window_size,
            self.slide_interval,
            self.tumbling_window_size,
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
    fn match_pattern(&self, ast: &promql_parser::parser::Expr) -> Option<(QueryPatternType, PromQLMatchResult)> {
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
                let fn_name = match_result
                    .get_function_name()
                    .unwrap_or_default();
                match fn_name.as_str() {
                    "quantile_over_time" | "sum_over_time" | "count_over_time" | "avg_over_time" => {
                        QueryTreatmentType::Approximate
                    }
                    _ => QueryTreatmentType::Exact,
                }
            }
            QueryPatternType::OnlySpatial => {
                let op = match_result
                    .get_aggregation_op()
                    .unwrap_or_default();
                match op.as_str() {
                    "quantile" | "sum" | "count" | "avg" | "topk" => QueryTreatmentType::Approximate,
                    _ => QueryTreatmentType::Exact,
                }
            }
        }
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
            if matches!(fn_name.as_str(), "rate" | "increase" | "quantile_over_time") {
                let num_data_points =
                    self.t_repeat as f64 / self.prometheus_scrape_interval as f64;
                if num_data_points < 60.0 {
                    return false;
                }
                if fn_name == "quantile_over_time" {
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

        let (pattern_type, match_result) = self
            .match_pattern(&ast)
            .ok_or_else(|| ControllerError::PlannerError(format!("Unsupported query: {}", self.query)))?;

        let treatment_type = Self::get_treatment_type(pattern_type, &match_result);

        let (metric, spatial_filter) = get_metric_and_spatial_filter(&match_result);

        let all_labels = self
            .metric_schema
            .get_labels(&metric)
            .ok_or_else(|| ControllerError::UnknownMetric(metric.clone()))?
            .clone();

        let statistics = get_statistics_to_compute(pattern_type, &match_result);

        let mut configs: Vec<IntermediateAggConfig> = Vec::new();

        // Shared window config (same for all statistics in this query)
        let mut window_cfg = IntermediateWindowConfig::default();

        // We use the first aggregation_type to set window parameters
        // (window params don't depend on aggregation_type since sliding is disabled)
        set_window_parameters(
            pattern_type,
            self.t_repeat,
            self.prometheus_scrape_interval,
            "any", // aggregation_type doesn't matter (sliding always false)
            self.step,
            &mut window_cfg,
        );

        for statistic in statistics {
            let (aggregation_type, aggregation_sub_type) =
                map_statistic_to_precompute_operator(statistic, treatment_type)
                    .map_err(|e| ControllerError::PlannerError(e))?;

            // Compute labels
            let (rollup_labels, grouping_labels, aggregated_labels) = compute_labels(
                pattern_type,
                statistic,
                &aggregation_type,
                &match_result,
                &all_labels,
            );

            // Main config
            let parameters = get_precompute_operator_parameters(
                &aggregation_type,
                &aggregation_sub_type,
                &match_result,
                self.sketch_parameters.as_ref(),
            )
            .map_err(|e| ControllerError::PlannerError(e))?;

            // DeltaSetAggregator pairing (hardcoded TODO)
            if matches!(aggregation_type.as_str(), "CountMinSketch" | "HydraKLL") {
                let delta_params = get_precompute_operator_parameters(
                    "DeltaSetAggregator",
                    "",
                    &match_result,
                    self.sketch_parameters.as_ref(),
                )
                .map_err(|e| ControllerError::PlannerError(e))?;

                configs.push(IntermediateAggConfig {
                    aggregation_type: "DeltaSetAggregator".to_string(),
                    aggregation_sub_type: String::new(),
                    window_type: window_cfg.window_type.clone(),
                    window_size: window_cfg.window_size,
                    slide_interval: window_cfg.slide_interval,
                    tumbling_window_size: window_cfg.tumbling_window_size,
                    spatial_filter: spatial_filter.clone(),
                    metric: metric.clone(),
                    table_name: None,
                    value_column: None,
                    parameters: delta_params,
                    rollup_labels: rollup_labels.clone(),
                    grouping_labels: grouping_labels.clone(),
                    aggregated_labels: aggregated_labels.clone(),
                });
            }

            configs.push(IntermediateAggConfig {
                aggregation_type,
                aggregation_sub_type,
                window_type: window_cfg.window_type.clone(),
                window_size: window_cfg.window_size,
                slide_interval: window_cfg.slide_interval,
                tumbling_window_size: window_cfg.tumbling_window_size,
                spatial_filter: spatial_filter.clone(),
                metric: metric.clone(),
                table_name: None,
                value_column: None,
                parameters,
                rollup_labels,
                grouping_labels,
                aggregated_labels,
            });
        }

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
                    &window_cfg.window_type,
                    self.range_duration,
                    self.step,
                )
                .map_err(|e| ControllerError::PlannerError(e))?,
            )
        };

        Ok((configs, cleanup_param))
    }
}

fn compute_labels(
    pattern_type: QueryPatternType,
    statistic: Statistic,
    aggregation_type: &str,
    match_result: &PromQLMatchResult,
    all_labels: &KeyByLabelNames,
) -> (KeyByLabelNames, KeyByLabelNames, KeyByLabelNames) {
    let mut rollup;
    let mut grouping = KeyByLabelNames::empty();
    let mut aggregated = KeyByLabelNames::empty();

    match pattern_type {
        QueryPatternType::OnlyTemporal => {
            rollup = KeyByLabelNames::empty();
            set_subpopulation_labels(statistic, aggregation_type, all_labels, &mut rollup, &mut grouping, &mut aggregated);
        }
        QueryPatternType::OnlySpatial => {
            // Match Python: if no by/without modifier, spatial_output = [] (rollup gets all labels).
            // promql_utilities::get_spatial_aggregation_output_labels has a topk patch that returns
            // all_labels when there is no modifier, but the Python planner returns [] in that case.
            let has_modifier = match_result.tokens.get("aggregation")
                .and_then(|t| t.aggregation.as_ref())
                .and_then(|a| a.modifier.as_ref())
                .is_some();
            let spatial_output = if has_modifier {
                get_spatial_aggregation_output_labels(match_result, all_labels)
            } else {
                KeyByLabelNames::empty()
            };
            rollup = all_labels.difference(&spatial_output);
            set_subpopulation_labels(statistic, aggregation_type, &spatial_output, &mut rollup, &mut grouping, &mut aggregated);
        }
        QueryPatternType::OneTemporalOneSpatial => {
            let fn_name = match_result.get_function_name().unwrap_or_default();
            let agg_op = match_result.get_aggregation_op().unwrap_or_default();
            let collapsable = get_is_collapsable(&fn_name, &agg_op);
            if !collapsable {
                rollup = KeyByLabelNames::empty();
                set_subpopulation_labels(statistic, aggregation_type, all_labels, &mut rollup, &mut grouping, &mut aggregated);
            } else {
                let spatial_output = get_spatial_aggregation_output_labels(match_result, all_labels);
                rollup = all_labels.difference(&spatial_output);
                set_subpopulation_labels(statistic, aggregation_type, &spatial_output, &mut rollup, &mut grouping, &mut aggregated);
            }
        }
    }

    (rollup, grouping, aggregated)
}
