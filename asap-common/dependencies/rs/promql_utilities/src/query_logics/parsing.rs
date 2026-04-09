use core::panic;

use promql_parser::parser::Expr;
use tracing::debug;

use crate::ast_matching::promql_pattern::AggregationModifierType;
use crate::ast_matching::PromQLMatchResult;
use crate::data_model::KeyByLabelNames;
use crate::query_logics::enums::{QueryPatternType, Statistic};

pub fn get_metric_and_spatial_filter(match_result: &PromQLMatchResult) -> (String, String) {
    debug!("Extracting metric and spatial filter from match result");
    let mut metric_name = match_result.get_metric_name().unwrap_or_default();
    debug!("Initial metric name: {}", metric_name);

    let spatial_filter = if let Some(metric_token) = match_result
        .tokens
        .get("metric")
        .and_then(|token| token.metric.as_ref())
    {
        if let Some(ast_vs) = metric_token.ast.as_ref() {
            // Render the VectorSelector AST to string and extract inner `{...}` content
            // let ast_str = format!("{}", ast_vs);
            let ast_str = Expr::from(ast_vs.clone()).prettify();
            if let Some(inner) = ast_str.split('{').nth(1).and_then(|s| s.split('}').next()) {
                debug!("Found spatial filter content: {}", inner);
                // Ensure metric_name does not include the selector part
                metric_name = metric_name
                    .split('{')
                    .next()
                    .unwrap_or(&metric_name)
                    .to_string();
                debug!("Cleaned metric name: {}", metric_name);
                inner.to_string()
            } else {
                String::new()
            }
        } else {
            // No AST available -> return empty spatial filter (no fallback reconstruction)
            String::new()
        }
    } else {
        String::new()
    };

    debug!(
        "Final result - metric: {}, spatial_filter: {}",
        metric_name, spatial_filter
    );
    (metric_name, spatial_filter)
}

/// Get statistics to compute based on pattern type and tokens
pub fn get_statistics_to_compute(
    pattern_type: QueryPatternType,
    match_result: &PromQLMatchResult,
) -> Vec<Statistic> {
    debug!("Computing statistics for pattern type {:?}", pattern_type);
    let statistic_to_compute: Option<String> = if pattern_type == QueryPatternType::OnlyTemporal
        || pattern_type == QueryPatternType::OneTemporalOneSpatial
    {
        match_result.get_function_name().map(|function_name| {
            let name = function_name.to_lowercase();
            name.split('_').next().unwrap_or(&name).to_string()
        })
    } else if pattern_type == QueryPatternType::OnlySpatial {
        match_result
            .get_aggregation_op()
            .map(|agg| agg.to_lowercase())
    } else {
        panic!("Unsupported query pattern type");
    };

    if let Some(statistic_to_compute) = statistic_to_compute {
        debug!("Found statistic to compute: {}", statistic_to_compute);
        if statistic_to_compute == "avg" {
            vec![Statistic::Sum, Statistic::Count]
        } else if let Ok(stat) = statistic_to_compute.parse::<Statistic>() {
            vec![stat]
        } else {
            panic!("Unsupported statistic: {}", statistic_to_compute);
        }
    } else {
        panic!("No statistic found in the query");
    }
}

pub fn get_spatial_aggregation_output_labels(
    match_result: &PromQLMatchResult,
    all_labels: &KeyByLabelNames,
) -> KeyByLabelNames {
    debug!("Getting spatial aggregation output labels");
    debug!("All labels: {:?}", all_labels);
    // Match Python behaviour: assume aggregation token and modifier exist
    // and raise (panic) if missing or invalid. "by" and "without" logic
    // remain the same.
    let aggregation_token = match_result
        .tokens
        .get("aggregation")
        .and_then(|token| token.aggregation.as_ref())
        .expect("aggregation token missing");

    // Patching: When the query is topk, we should always return all labels
    if aggregation_token.op.to_lowercase() == "topk" {
        debug!("Aggregation operation is 'topk', returning all labels");
        return all_labels.clone();
    }

    // Fixing issue https://github.com/ProjectASAP/asap-internal/issues/24
    let modifier: &crate::AggregationModifier = match aggregation_token.modifier.as_ref() {
        Some(m) => m,
        None => {
            debug!("No aggregation modifier found, returning empty KeyByLabelNames");
            return KeyByLabelNames::new(vec![]);
        }
    };

    debug!(
        "Modifier type: {:?}, labels: {:?}",
        modifier.modifier_type, modifier.labels
    );
    match modifier.modifier_type {
        AggregationModifierType::By => {
            debug!("Processing 'by' modifier");
            // Return only the labels specified in "by" clause
            KeyByLabelNames::new(modifier.labels.clone())
        }
        AggregationModifierType::Without => {
            debug!("Processing 'without' modifier");
            // Return all labels except those specified in "without" clause
            let without_labels = KeyByLabelNames::new(modifier.labels.clone());
            all_labels.difference(&without_labels)
        }
    }
}
