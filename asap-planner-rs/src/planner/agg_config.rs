use asap_types::enums::WindowType;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{AggregationType, QueryTreatmentType, Statistic};
use promql_utilities::query_logics::logics::map_statistic_to_precompute_operator;
use serde_json::Value;
use std::collections::HashMap;

use crate::planner::labels::set_subpopulation_labels;
use crate::planner::window::IntermediateWindowConfig;

/// Internal representation of an aggregation config before IDs are assigned
#[derive(Debug, Clone)]
pub struct IntermediateAggConfig {
    pub aggregation_type: AggregationType,
    pub aggregation_sub_type: String,
    pub window_type: WindowType,
    pub window_size_ms: u64,
    pub slide_interval_ms: u64,
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
            self.window_size_ms,
            self.slide_interval_ms,
            self.spatial_filter,
            self.metric,
            self.table_name,
            self.value_column,
            params_vec,
            label_parts,
        )
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
                window_size_ms: window_cfg.window_size_ms,
                slide_interval_ms: window_cfg.slide_interval_ms,
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
            window_size_ms: window_cfg.window_size_ms,
            slide_interval_ms: window_cfg.slide_interval_ms,
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
            window_size_ms: 300_000,
            slide_interval_ms: 300_000,
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
        cfg2.window_size_ms = 60_000;
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
