use indexmap::IndexMap;
use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;
use std::collections::HashMap;

use asap_types::enums::CleanupPolicy;
use promql_utilities::data_model::KeyByLabelNames;

use crate::planner::agg_config::IntermediateAggConfig;

// YAML key constants — shared by both SQL and PromQL generators
pub(crate) const KEY_AGG_ID: &str = "aggregationId";
pub(crate) const KEY_AGG_SUB_TYPE: &str = "aggregationSubType";
pub(crate) const KEY_AGG_TYPE: &str = "aggregationType";
pub(crate) const KEY_AGGREGATION_ID: &str = "aggregation_id";
pub(crate) const KEY_AGGREGATIONS: &str = "aggregations";
pub(crate) const KEY_CLEANUP_POLICY: &str = "cleanup_policy";
pub(crate) const KEY_LABELS: &str = "labels";
pub(crate) const KEY_LABELS_AGGREGATED: &str = "aggregated";
pub(crate) const KEY_LABELS_GROUPING: &str = "grouping";
pub(crate) const KEY_LABELS_ROLLUP: &str = "rollup";
pub(crate) const KEY_METADATA_COLUMNS: &str = "metadata_columns";
pub(crate) const KEY_METRIC: &str = "metric";
pub(crate) const KEY_METRICS: &str = "metrics";
pub(crate) const KEY_NAME: &str = "name";
pub(crate) const KEY_NUM_AGG_TO_RETAIN: &str = "num_aggregates_to_retain";
pub(crate) const KEY_PARAMETERS: &str = "parameters";
pub(crate) const KEY_QUERIES: &str = "queries";
pub(crate) const KEY_QUERY: &str = "query";
pub(crate) const KEY_READ_COUNT_THRESHOLD: &str = "read_count_threshold";
pub(crate) const KEY_SLIDE_INTERVAL: &str = "slideIntervalMs";
pub(crate) const KEY_SPATIAL_FILTER: &str = "spatialFilter";
pub(crate) const KEY_TABLE_NAME: &str = "table_name";
pub(crate) const KEY_TABLES: &str = "tables";
pub(crate) const KEY_TIME_COLUMN: &str = "time_column";
pub(crate) const KEY_VALUE_COLUMN: &str = "value_column";
pub(crate) const KEY_VALUE_COLUMNS: &str = "value_columns";
pub(crate) const KEY_WINDOW_SIZE: &str = "windowSizeMs";
pub(crate) const KEY_WINDOW_TYPE: &str = "windowType";

pub fn key_by_labels_to_yaml(labels: &KeyByLabelNames) -> YamlValue {
    YamlValue::Sequence(
        labels
            .labels
            .iter()
            .map(|l| YamlValue::String(l.clone()))
            .collect(),
    )
}

pub fn build_aggregation_entry(id: u32, cfg: &IntermediateAggConfig) -> YamlValue {
    let mut map = serde_yaml::Mapping::new();
    map.insert(
        YamlValue::String(KEY_AGG_ID.to_string()),
        YamlValue::Number(id.into()),
    );
    map.insert(
        YamlValue::String(KEY_AGG_SUB_TYPE.to_string()),
        YamlValue::String(cfg.aggregation_sub_type.clone()),
    );
    map.insert(
        YamlValue::String(KEY_AGG_TYPE.to_string()),
        YamlValue::String(cfg.aggregation_type.to_string()),
    );

    let mut labels_map = serde_yaml::Mapping::new();
    labels_map.insert(
        YamlValue::String(KEY_LABELS_AGGREGATED.to_string()),
        key_by_labels_to_yaml(&cfg.aggregated_labels),
    );
    labels_map.insert(
        YamlValue::String(KEY_LABELS_GROUPING.to_string()),
        key_by_labels_to_yaml(&cfg.grouping_labels),
    );
    labels_map.insert(
        YamlValue::String(KEY_LABELS_ROLLUP.to_string()),
        key_by_labels_to_yaml(&cfg.rollup_labels),
    );
    map.insert(
        YamlValue::String(KEY_LABELS.to_string()),
        YamlValue::Mapping(labels_map),
    );

    map.insert(
        YamlValue::String(KEY_METRIC.to_string()),
        YamlValue::String(cfg.metric.clone()),
    );
    map.insert(
        YamlValue::String(KEY_PARAMETERS.to_string()),
        params_to_yaml(&cfg.parameters),
    );
    map.insert(
        YamlValue::String(KEY_SLIDE_INTERVAL.to_string()),
        YamlValue::Number(cfg.slide_interval_ms.into()),
    );
    map.insert(
        YamlValue::String(KEY_SPATIAL_FILTER.to_string()),
        YamlValue::String(cfg.spatial_filter.clone()),
    );
    map.insert(
        YamlValue::String(KEY_TABLE_NAME.to_string()),
        match &cfg.table_name {
            Some(t) => YamlValue::String(t.clone()),
            None => YamlValue::Null,
        },
    );
    map.insert(
        YamlValue::String(KEY_VALUE_COLUMN.to_string()),
        match &cfg.value_column {
            Some(v) => YamlValue::String(v.clone()),
            None => YamlValue::Null,
        },
    );
    map.insert(
        YamlValue::String(KEY_WINDOW_SIZE.to_string()),
        YamlValue::Number(cfg.window_size_ms.into()),
    );
    map.insert(
        YamlValue::String(KEY_WINDOW_TYPE.to_string()),
        YamlValue::String(cfg.window_type.to_string()),
    );

    YamlValue::Mapping(map)
}

pub fn build_queries_yaml(
    cleanup_policy: CleanupPolicy,
    query_keys_map: &IndexMap<String, Vec<(String, Option<u64>)>>,
    id_map: &HashMap<String, u32>,
) -> Vec<YamlValue> {
    query_keys_map
        .iter()
        .map(|(query_str, keys)| {
            let aggregations: Vec<YamlValue> = keys
                .iter()
                .map(|(key, cleanup_param)| {
                    let agg_id = id_map[key];
                    let mut agg_map = serde_yaml::Mapping::new();
                    agg_map.insert(
                        YamlValue::String(KEY_AGGREGATION_ID.to_string()),
                        YamlValue::Number(agg_id.into()),
                    );
                    if let Some(param) = cleanup_param {
                        match cleanup_policy {
                            CleanupPolicy::CircularBuffer => {
                                agg_map.insert(
                                    YamlValue::String(KEY_NUM_AGG_TO_RETAIN.to_string()),
                                    YamlValue::Number((*param).into()),
                                );
                            }
                            CleanupPolicy::ReadBased => {
                                agg_map.insert(
                                    YamlValue::String(KEY_READ_COUNT_THRESHOLD.to_string()),
                                    YamlValue::Number((*param).into()),
                                );
                            }
                            CleanupPolicy::NoCleanup => {}
                        }
                    }
                    YamlValue::Mapping(agg_map)
                })
                .collect();

            let mut q_map = serde_yaml::Mapping::new();
            q_map.insert(
                YamlValue::String(KEY_AGGREGATIONS.to_string()),
                YamlValue::Sequence(aggregations),
            );
            q_map.insert(
                YamlValue::String(KEY_QUERY.to_string()),
                YamlValue::String(query_str.clone()),
            );
            YamlValue::Mapping(q_map)
        })
        .collect()
}

pub fn params_to_yaml(params: &HashMap<String, JsonValue>) -> YamlValue {
    if params.is_empty() {
        return YamlValue::Mapping(serde_yaml::Mapping::new());
    }
    let mut map = serde_yaml::Mapping::new();
    // Sort for determinism
    let mut sorted: Vec<_> = params.iter().collect();
    sorted.sort_by_key(|(k, _)| k.as_str());
    for (k, v) in sorted {
        let yaml_val = match v {
            JsonValue::Number(n) => {
                if let Some(i) = n.as_u64() {
                    YamlValue::Number(serde_yaml::Number::from(i))
                } else if let Some(f) = n.as_f64() {
                    YamlValue::Number(serde_yaml::Number::from(f))
                } else {
                    YamlValue::String(n.to_string())
                }
            }
            JsonValue::String(s) => YamlValue::String(s.clone()),
            JsonValue::Bool(b) => YamlValue::Bool(*b),
            other => YamlValue::String(other.to_string()),
        };
        map.insert(YamlValue::String(k.clone()), yaml_val);
    }
    YamlValue::Mapping(map)
}

#[derive(Debug, Clone)]
pub struct PuntedQuery {
    pub query: String,
}

pub struct GeneratorOutput {
    pub punted_queries: Vec<PuntedQuery>,
    pub streaming_yaml: YamlValue,
    pub inference_yaml: YamlValue,
    pub aggregation_count: usize,
    pub query_count: usize,
}
