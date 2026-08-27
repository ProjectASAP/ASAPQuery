use indexmap::IndexMap;
use serde_yaml::Value as YamlValue;
use std::collections::HashMap;

use asap_types::enums::CleanupPolicy;
use asap_types::PromQLSchema;

use crate::config::input::ControllerConfig;
use crate::error::ControllerError;
use crate::generator::{
    build_aggregation_entry, build_queries_yaml, key_by_labels_to_yaml, GeneratorOutput,
    PuntedQuery, KEY_AGGREGATIONS, KEY_CLEANUP_POLICY, KEY_METRICS, KEY_NAME, KEY_QUERIES,
};
use crate::planner::agg_config::IntermediateAggConfig;
use crate::planner::promql::{BinaryArm, SingleQueryProcessor};
use crate::planner::window::WindowingError;
use crate::RuntimeOptions;

/// `(query_string, Vec<(identifying_key, cleanup_param)>)` pairs produced by binary leaf decomposition.
type LeafEntries = Vec<(String, Vec<(String, Option<u64>)>)>;

/// Run the full planning pipeline and produce YAML outputs.
///
/// This is the hardcoded sketch/window selection path that `Controller::generate()`
/// currently calls. `crate::optimizer` implements an optimization-based replacement
/// (issue #405) but is not wired in here yet — see `bin/optimizer_cli.rs` for an
/// offline runner and `.design_docs/optimizer-v1-implementation-plan.md` for status.
pub fn generate_plan(
    controller_config: &ControllerConfig,
    schema: &PromQLSchema,
    opts: &RuntimeOptions,
) -> Result<GeneratorOutput, ControllerError> {
    let metric_schema = schema.clone();

    if let Some(windowing) = &controller_config.windowing {
        windowing
            .validate()
            .map_err(|error| ControllerError::Windowing(WindowingError::InvalidConfig(error)))?;
    }

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
    let mut windowing_errors: Vec<String> = Vec::new();

    for qg in &controller_config.query_groups {
        for query_string in &qg.queries {
            let processor = SingleQueryProcessor::new(
                query_string.clone(),
                qg.repetition_delay_ms,
                opts.data_ingestion_interval_ms,
                metric_schema.clone(),
                opts.streaming_engine,
                controller_config.sketch_parameters.clone(),
                qg.range_duration_ms.unwrap_or(opts.range_duration_ms),
                qg.step_ms.unwrap_or(opts.step_ms),
                cleanup_policy,
                controller_config.windowing.clone(),
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
                    Err(ControllerError::Windowing(error)) => {
                        windowing_errors.push(format!("query '{query_string}': {error}"));
                    }
                    Err(e) => return Err(e),
                }
            } else if let Some(arm_entries) = collect_binary_leaf_entries(
                &processor,
                &mut dedup_map,
                &mut windowing_errors,
                query_string,
            )? {
                // Binary arithmetic: register each leaf arm in dedup_map and query_keys_map
                for (arm_query, keys_for_arm) in arm_entries {
                    // Use `entry` so a standalone query that duplicates an arm wins
                    query_keys_map.entry(arm_query).or_insert(keys_for_arm);
                }
            }
        }
    }

    if !windowing_errors.is_empty() {
        return Err(ControllerError::PlannerError(format!(
            "sliding window validation failed:\n{}",
            windowing_errors.join("\n")
        )));
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
    windowing_errors: &mut Vec<String>,
    query_context: &str,
) -> Result<Option<LeafEntries>, ControllerError> {
    let arms = match processor.get_binary_arm_queries() {
        Some(arms) => arms,
        None => return Ok(None), // not a binary expression
    };

    let mut all_entries: LeafEntries = Vec::new();
    let mut found_windowing_error = false;
    let mut found_unsupported_arm = false;

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
                        match arm_processor.get_streaming_aggregation_configs() {
                            Ok(result) => result,
                            Err(ControllerError::Windowing(error)) => {
                                windowing_errors.push(format!(
                                    "query '{query_context}' (leaf '{arm_query}'): {error}"
                                ));
                                found_windowing_error = true;
                                continue;
                            }
                            Err(error) => return Err(error),
                        };
                    let mut keys_for_arm = Vec::new();
                    for config in configs {
                        let key = config.identifying_key();
                        keys_for_arm.push((key.clone(), cleanup_param));
                        dedup_map.entry(key).or_insert(config);
                    }
                    all_entries.push((arm_query, keys_for_arm));
                } else {
                    // The arm might itself be a binary expression — recurse.
                    let error_count = windowing_errors.len();
                    match collect_binary_leaf_entries(
                        &arm_processor,
                        dedup_map,
                        windowing_errors,
                        query_context,
                    )? {
                        Some(sub_entries) => {
                            all_entries.extend(sub_entries);
                        }
                        None => {
                            if windowing_errors.len() > error_count {
                                found_windowing_error = true;
                                continue;
                            }
                            // Arm is neither a supported leaf nor a binary expression.
                            // This entire query cannot be accelerated.
                            found_unsupported_arm = true;
                            continue;
                        }
                    }
                }
            }
        }
    }

    if found_windowing_error || found_unsupported_arm {
        Ok(None)
    } else {
        Ok(Some(all_entries))
    }
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
