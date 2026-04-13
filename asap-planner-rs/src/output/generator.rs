use indexmap::IndexMap;
use serde_json::Value as JsonValue;
use serde_yaml::Value as YamlValue;
use std::collections::HashMap;

use asap_types::enums::CleanupPolicy;
use asap_types::PromQLSchema;
use promql_utilities::data_model::KeyByLabelNames;

use crate::config::input::ControllerConfig;
use crate::error::ControllerError;
use crate::planner::single_query::{BinaryArm, IntermediateAggConfig, SingleQueryProcessor};
use crate::RuntimeOptions;

// YAML key constants — shared with sql_generator.rs and lib.rs via pub(crate)
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
pub(crate) const KEY_SLIDE_INTERVAL: &str = "slideInterval";
pub(crate) const KEY_SPATIAL_FILTER: &str = "spatialFilter";
pub(crate) const KEY_TABLE_NAME: &str = "table_name";
pub(crate) const KEY_TABLES: &str = "tables";
pub(crate) const KEY_TIME_COLUMN: &str = "time_column";
pub(crate) const KEY_VALUE_COLUMN: &str = "value_column";
pub(crate) const KEY_VALUE_COLUMNS: &str = "value_columns";
pub(crate) const KEY_WINDOW_SIZE: &str = "windowSize";
pub(crate) const KEY_WINDOW_TYPE: &str = "windowType";

/// `(query_string, Vec<(identifying_key, cleanup_param)>)` pairs produced by binary leaf decomposition.
type LeafEntries = Vec<(String, Vec<(String, Option<u64>)>)>;

/// Run the full planning pipeline and produce YAML outputs
pub fn generate_plan(
    controller_config: &ControllerConfig,
    schema: &PromQLSchema,
    opts: &RuntimeOptions,
) -> Result<GeneratorOutput, ControllerError> {
    let metric_schema = schema.clone();

    // Determine cleanup policy
    let cleanup_policy = controller_config
        .aggregate_cleanup
        .as_ref()
        .and_then(|c| c.policy)
        .unwrap_or(CleanupPolicy::ReadBased);

    // Validate no duplicate queries
    let mut seen_queries = std::collections::HashSet::new();
    for qg in &controller_config.query_groups {
        for q in &qg.queries {
            if !seen_queries.insert(q.clone()) {
                return Err(ControllerError::DuplicateQuery(q.clone()));
            }
        }
    }

    // Deduplication map: identifying_key -> (agg_config, assigned_id_placeholder)
    let mut dedup_map: IndexMap<String, IntermediateAggConfig> = IndexMap::new();
    // query_string -> Vec<(key, cleanup_param)>
    let mut query_keys_map: IndexMap<String, Vec<(String, Option<u64>)>> = IndexMap::new();

    let mut punted_queries: Vec<PuntedQuery> = Vec::new();

    for qg in &controller_config.query_groups {
        for query_string in &qg.queries {
            let processor = SingleQueryProcessor::new(
                query_string.clone(),
                qg.repetition_delay,
                opts.prometheus_scrape_interval,
                metric_schema.clone(),
                opts.streaming_engine,
                controller_config.sketch_parameters.clone(),
                qg.range_duration.unwrap_or(opts.range_duration),
                qg.step.unwrap_or(opts.step),
                cleanup_policy,
            );

            let mut should_process = processor.is_supported();
            if opts.enable_punting && should_process {
                should_process = should_process && processor.should_be_performant();
                if !should_process {
                    punted_queries.push(PuntedQuery {
                        query: query_string.clone(),
                    });
                }
            }

            if should_process {
                match processor.get_streaming_aggregation_configs() {
                    Ok((configs, cleanup_param)) => {
                        let mut keys_for_query = Vec::new();
                        for config in configs {
                            let key = config.identifying_key();
                            keys_for_query.push((key.clone(), cleanup_param));
                            dedup_map.entry(key).or_insert(config);
                        }
                        query_keys_map.insert(query_string.clone(), keys_for_query);
                    }
                    Err(ControllerError::UnknownMetric(ref metric)) => {
                        tracing::warn!(
                            query = %query_string,
                            metric = %metric,
                            "skipping query referencing unknown metric"
                        );
                    }
                    Err(e) => return Err(e),
                }
            } else if let Some(arm_entries) =
                collect_binary_leaf_entries(&processor, &mut dedup_map)?
            {
                // Binary arithmetic: register each leaf arm in dedup_map and query_keys_map
                for (arm_query, keys_for_arm) in arm_entries {
                    // Use `entry` so a standalone query that duplicates an arm wins
                    query_keys_map.entry(arm_query).or_insert(keys_for_arm);
                }
            }
        }
    }

    // Assign sequential IDs (1-indexed, insertion order)
    let mut id_map: HashMap<String, u32> = HashMap::new();
    for (idx, key) in dedup_map.keys().enumerate() {
        id_map.insert(key.clone(), idx as u32 + 1);
    }

    // Build streaming_config YAML
    let streaming_yaml = build_streaming_yaml(&dedup_map, &id_map, &metric_schema)?;

    // Build inference_config YAML
    let inference_yaml =
        build_inference_yaml(cleanup_policy, &query_keys_map, &id_map, &metric_schema)?;

    Ok(GeneratorOutput {
        punted_queries,
        streaming_yaml,
        inference_yaml,
        aggregation_count: dedup_map.len(),
        query_count: query_keys_map.len(),
    })
}

/// Recursively collect (arm_query_string, Vec<(dedup_key, cleanup_param)>) pairs
/// from a binary arithmetic expression, registering new configs in `dedup_map`.
///
/// Returns `Some(Vec<...>)` when every leaf arm is acceleratable.
/// Returns `None` if any arm is unsupported (caller should skip the query).
/// Returns `Err` only on internal planner errors.
fn collect_binary_leaf_entries(
    processor: &SingleQueryProcessor,
    dedup_map: &mut IndexMap<String, IntermediateAggConfig>,
) -> Result<Option<LeafEntries>, ControllerError> {
    let arms = match processor.get_binary_arm_queries() {
        Some(arms) => arms,
        None => return Ok(None), // not a binary expression
    };

    let mut all_entries: LeafEntries = Vec::new();

    for arm in [arms.0, arms.1] {
        match arm {
            BinaryArm::Scalar(_) => {
                // Scalar literals need no aggregation config — skip silently.
            }
            BinaryArm::Query(arm_query) => {
                let arm_processor = processor.make_arm_processor(arm_query.clone());

                if arm_processor.is_supported() {
                    // Leaf arm: gather its streaming aggregation configs.
                    let (configs, cleanup_param) =
                        arm_processor.get_streaming_aggregation_configs()?;
                    let mut keys_for_arm = Vec::new();
                    for config in configs {
                        let key = config.identifying_key();
                        keys_for_arm.push((key.clone(), cleanup_param));
                        dedup_map.entry(key).or_insert(config);
                    }
                    all_entries.push((arm_query, keys_for_arm));
                } else {
                    // The arm might itself be a binary expression — recurse.
                    match collect_binary_leaf_entries(&arm_processor, dedup_map)? {
                        Some(sub_entries) => {
                            all_entries.extend(sub_entries);
                        }
                        None => {
                            // Arm is neither a supported leaf nor a binary expression.
                            // This entire query cannot be accelerated.
                            return Ok(None);
                        }
                    }
                }
            }
        }
    }

    Ok(Some(all_entries))
}

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
        YamlValue::Number(cfg.slide_interval.into()),
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
        YamlValue::Number(cfg.window_size.into()),
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

fn build_streaming_yaml(
    dedup_map: &IndexMap<String, IntermediateAggConfig>,
    id_map: &HashMap<String, u32>,
    metric_schema: &asap_types::PromQLSchema,
) -> Result<YamlValue, ControllerError> {
    let aggregations: Vec<YamlValue> = dedup_map
        .iter()
        .map(|(key, cfg)| build_aggregation_entry(id_map[key], cfg))
        .collect();

    // Build metrics section
    let mut metrics_map = serde_yaml::Mapping::new();
    for (metric_name, labels) in &metric_schema.config {
        metrics_map.insert(
            YamlValue::String(metric_name.clone()),
            key_by_labels_to_yaml(labels),
        );
    }

    let mut root = serde_yaml::Mapping::new();
    root.insert(
        YamlValue::String(KEY_AGGREGATIONS.to_string()),
        YamlValue::Sequence(aggregations),
    );
    root.insert(
        YamlValue::String(KEY_METRICS.to_string()),
        YamlValue::Mapping(metrics_map),
    );

    Ok(YamlValue::Mapping(root))
}

fn build_inference_yaml(
    cleanup_policy: CleanupPolicy,
    query_keys_map: &IndexMap<String, Vec<(String, Option<u64>)>>,
    id_map: &HashMap<String, u32>,
    metric_schema: &asap_types::PromQLSchema,
) -> Result<YamlValue, ControllerError> {
    let mut cleanup_map = serde_yaml::Mapping::new();
    cleanup_map.insert(
        YamlValue::String(KEY_NAME.to_string()),
        YamlValue::String(cleanup_policy.to_string()),
    );

    let queries = build_queries_yaml(cleanup_policy, query_keys_map, id_map);

    // Build metrics section
    let mut metrics_map = serde_yaml::Mapping::new();
    for (metric_name, labels) in &metric_schema.config {
        metrics_map.insert(
            YamlValue::String(metric_name.clone()),
            key_by_labels_to_yaml(labels),
        );
    }

    let mut root = serde_yaml::Mapping::new();
    root.insert(
        YamlValue::String(KEY_CLEANUP_POLICY.to_string()),
        YamlValue::Mapping(cleanup_map),
    );
    root.insert(
        YamlValue::String(KEY_METRICS.to_string()),
        YamlValue::Mapping(metrics_map),
    );
    root.insert(
        YamlValue::String(KEY_QUERIES.to_string()),
        YamlValue::Sequence(queries),
    );

    Ok(YamlValue::Mapping(root))
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
