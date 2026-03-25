use indexmap::IndexMap;
use serde_yaml::Value as YamlValue;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::input::SQLControllerConfig;
use crate::error::ControllerError;
use crate::output::generator::{
    key_by_labels_to_yaml, params_to_yaml, parse_cleanup_policy, GeneratorOutput,
};
use crate::planner::single_query::IntermediateAggConfig;
use crate::planner::sql_single_query::SQLSingleQueryProcessor;
use crate::StreamingEngine;

pub struct SQLRuntimeOptions {
    pub streaming_engine: StreamingEngine,
    pub query_evaluation_time: Option<f64>,
    pub data_ingestion_interval: u64,
}

pub fn generate_sql_plan(
    config: &SQLControllerConfig,
    opts: &SQLRuntimeOptions,
) -> Result<GeneratorOutput, ControllerError> {
    let eval_time: f64 = opts.query_evaluation_time.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
    });

    let cleanup_policy_str = config
        .aggregate_cleanup
        .as_ref()
        .and_then(|c| c.policy.as_deref())
        .unwrap_or("read_based");
    let cleanup_policy = parse_cleanup_policy(cleanup_policy_str)?;

    // Validate T % data_ingestion_interval == 0
    for qg in &config.query_groups {
        if qg.repetition_delay % opts.data_ingestion_interval != 0 {
            return Err(ControllerError::PlannerError(format!(
                "repetition_delay {} is not a multiple of data_ingestion_interval {}",
                qg.repetition_delay, opts.data_ingestion_interval
            )));
        }
    }

    // Check for duplicate queries
    let mut seen_queries = std::collections::HashSet::new();
    for qg in &config.query_groups {
        for q in &qg.queries {
            if !seen_queries.insert(q.clone()) {
                return Err(ControllerError::DuplicateQuery(q.clone()));
            }
        }
    }

    // Dedup map: identifying_key -> IntermediateAggConfig
    let mut dedup_map: IndexMap<String, IntermediateAggConfig> = IndexMap::new();
    // query_string -> Vec<(key, cleanup_param)>
    let mut query_keys_map: IndexMap<String, Vec<(String, Option<u64>)>> = IndexMap::new();

    for qg in &config.query_groups {
        for query_string in &qg.queries {
            let processor = SQLSingleQueryProcessor::new(
                query_string.clone(),
                qg.repetition_delay,
                opts.data_ingestion_interval,
                config.tables.clone(),
                opts.streaming_engine,
                config.sketch_parameters.clone(),
                cleanup_policy,
            );

            let (configs, cleanup_param) =
                processor.get_streaming_aggregation_configs(eval_time)?;

            let mut keys_for_query = Vec::new();
            for config_item in configs {
                let key = config_item.identifying_key();
                keys_for_query.push((key.clone(), cleanup_param));
                dedup_map.entry(key).or_insert(config_item);
            }
            query_keys_map.insert(query_string.clone(), keys_for_query);
        }
    }

    // Assign sequential IDs
    let mut id_map: HashMap<String, u32> = HashMap::new();
    for (idx, key) in dedup_map.keys().enumerate() {
        id_map.insert(key.clone(), idx as u32 + 1);
    }

    let streaming_yaml = build_sql_streaming_yaml(config, &dedup_map, &id_map)?;
    let inference_yaml = build_sql_inference_yaml(
        config,
        cleanup_policy,
        cleanup_policy_str,
        &query_keys_map,
        &id_map,
    )?;

    Ok(GeneratorOutput {
        punted_queries: Vec::new(),
        streaming_yaml,
        inference_yaml,
        aggregation_count: dedup_map.len(),
        query_count: query_keys_map.len(),
    })
}

fn build_sql_streaming_yaml(
    config: &SQLControllerConfig,
    dedup_map: &IndexMap<String, IntermediateAggConfig>,
    id_map: &HashMap<String, u32>,
) -> Result<YamlValue, ControllerError> {
    let aggregations: Vec<YamlValue> = dedup_map
        .iter()
        .map(|(key, cfg)| {
            let id = id_map[key];
            let mut map = serde_yaml::Mapping::new();
            map.insert(
                YamlValue::String("aggregationId".to_string()),
                YamlValue::Number(id.into()),
            );
            map.insert(
                YamlValue::String("aggregationSubType".to_string()),
                YamlValue::String(cfg.aggregation_sub_type.clone()),
            );
            map.insert(
                YamlValue::String("aggregationType".to_string()),
                YamlValue::String(cfg.aggregation_type.clone()),
            );

            // labels
            let mut labels_map = serde_yaml::Mapping::new();
            labels_map.insert(
                YamlValue::String("aggregated".to_string()),
                key_by_labels_to_yaml(&cfg.aggregated_labels),
            );
            labels_map.insert(
                YamlValue::String("grouping".to_string()),
                key_by_labels_to_yaml(&cfg.grouping_labels),
            );
            labels_map.insert(
                YamlValue::String("rollup".to_string()),
                key_by_labels_to_yaml(&cfg.rollup_labels),
            );
            map.insert(
                YamlValue::String("labels".to_string()),
                YamlValue::Mapping(labels_map),
            );

            map.insert(
                YamlValue::String("metric".to_string()),
                YamlValue::String(cfg.metric.clone()),
            );
            map.insert(
                YamlValue::String("parameters".to_string()),
                params_to_yaml(&cfg.parameters),
            );
            map.insert(
                YamlValue::String("slideInterval".to_string()),
                YamlValue::Number(cfg.slide_interval.into()),
            );
            map.insert(
                YamlValue::String("spatialFilter".to_string()),
                YamlValue::String(cfg.spatial_filter.clone()),
            );
            map.insert(
                YamlValue::String("table_name".to_string()),
                match &cfg.table_name {
                    Some(t) => YamlValue::String(t.clone()),
                    None => YamlValue::Null,
                },
            );
            map.insert(
                YamlValue::String("value_column".to_string()),
                match &cfg.value_column {
                    Some(v) => YamlValue::String(v.clone()),
                    None => YamlValue::Null,
                },
            );
            map.insert(
                YamlValue::String("windowSize".to_string()),
                YamlValue::Number(cfg.window_size.into()),
            );
            map.insert(
                YamlValue::String("windowType".to_string()),
                YamlValue::String(cfg.window_type.clone()),
            );

            YamlValue::Mapping(map)
        })
        .collect();

    // Build tables section
    let tables_seq: Vec<YamlValue> = config
        .tables
        .iter()
        .map(|t| {
            let mut map = serde_yaml::Mapping::new();
            map.insert(
                YamlValue::String("name".to_string()),
                YamlValue::String(t.name.clone()),
            );
            map.insert(
                YamlValue::String("time_column".to_string()),
                YamlValue::String(t.time_column.clone()),
            );
            map.insert(
                YamlValue::String("value_columns".to_string()),
                YamlValue::Sequence(
                    t.value_columns
                        .iter()
                        .map(|c| YamlValue::String(c.clone()))
                        .collect(),
                ),
            );
            map.insert(
                YamlValue::String("metadata_columns".to_string()),
                YamlValue::Sequence(
                    t.metadata_columns
                        .iter()
                        .map(|c| YamlValue::String(c.clone()))
                        .collect(),
                ),
            );
            YamlValue::Mapping(map)
        })
        .collect();

    let mut root = serde_yaml::Mapping::new();
    root.insert(
        YamlValue::String("aggregations".to_string()),
        YamlValue::Sequence(aggregations),
    );
    root.insert(
        YamlValue::String("tables".to_string()),
        YamlValue::Sequence(tables_seq),
    );

    Ok(YamlValue::Mapping(root))
}

fn build_sql_inference_yaml(
    config: &SQLControllerConfig,
    cleanup_policy: sketch_db_common::enums::CleanupPolicy,
    cleanup_policy_str: &str,
    query_keys_map: &IndexMap<String, Vec<(String, Option<u64>)>>,
    id_map: &HashMap<String, u32>,
) -> Result<YamlValue, ControllerError> {
    let mut cleanup_map = serde_yaml::Mapping::new();
    cleanup_map.insert(
        YamlValue::String("name".to_string()),
        YamlValue::String(cleanup_policy_str.to_string()),
    );

    let queries: Vec<YamlValue> = query_keys_map
        .iter()
        .map(|(query_str, keys)| {
            let aggregations: Vec<YamlValue> = keys
                .iter()
                .map(|(key, cleanup_param)| {
                    let agg_id = id_map[key];
                    let mut agg_map = serde_yaml::Mapping::new();
                    agg_map.insert(
                        YamlValue::String("aggregation_id".to_string()),
                        YamlValue::Number(agg_id.into()),
                    );
                    if let Some(param) = cleanup_param {
                        match cleanup_policy {
                            sketch_db_common::enums::CleanupPolicy::CircularBuffer => {
                                agg_map.insert(
                                    YamlValue::String("num_aggregates_to_retain".to_string()),
                                    YamlValue::Number((*param).into()),
                                );
                            }
                            sketch_db_common::enums::CleanupPolicy::ReadBased => {
                                agg_map.insert(
                                    YamlValue::String("read_count_threshold".to_string()),
                                    YamlValue::Number((*param).into()),
                                );
                            }
                            sketch_db_common::enums::CleanupPolicy::NoCleanup => {}
                        }
                    }
                    YamlValue::Mapping(agg_map)
                })
                .collect();

            let mut q_map = serde_yaml::Mapping::new();
            q_map.insert(
                YamlValue::String("aggregations".to_string()),
                YamlValue::Sequence(aggregations),
            );
            q_map.insert(
                YamlValue::String("query".to_string()),
                YamlValue::String(query_str.clone()),
            );
            YamlValue::Mapping(q_map)
        })
        .collect();

    // Build tables section
    let tables_seq: Vec<YamlValue> = config
        .tables
        .iter()
        .map(|t| {
            let mut map = serde_yaml::Mapping::new();
            map.insert(
                YamlValue::String("name".to_string()),
                YamlValue::String(t.name.clone()),
            );
            map.insert(
                YamlValue::String("time_column".to_string()),
                YamlValue::String(t.time_column.clone()),
            );
            map.insert(
                YamlValue::String("value_columns".to_string()),
                YamlValue::Sequence(
                    t.value_columns
                        .iter()
                        .map(|c| YamlValue::String(c.clone()))
                        .collect(),
                ),
            );
            map.insert(
                YamlValue::String("metadata_columns".to_string()),
                YamlValue::Sequence(
                    t.metadata_columns
                        .iter()
                        .map(|c| YamlValue::String(c.clone()))
                        .collect(),
                ),
            );
            YamlValue::Mapping(map)
        })
        .collect();

    let mut root = serde_yaml::Mapping::new();
    root.insert(
        YamlValue::String("cleanup_policy".to_string()),
        YamlValue::Mapping(cleanup_map),
    );
    root.insert(
        YamlValue::String("queries".to_string()),
        YamlValue::Sequence(queries),
    );
    root.insert(
        YamlValue::String("tables".to_string()),
        YamlValue::Sequence(tables_seq),
    );

    Ok(YamlValue::Mapping(root))
}
