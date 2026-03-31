use indexmap::IndexMap;
use serde_yaml::Value as YamlValue;
use sketch_db_common::enums::CleanupPolicy;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::input::SQLControllerConfig;
use crate::error::ControllerError;
use crate::output::generator::{
    build_aggregation_entry, build_queries_yaml, parse_cleanup_policy, GeneratorOutput,
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

fn build_tables_yaml(config: &SQLControllerConfig) -> Vec<YamlValue> {
    config
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
        .collect()
}

fn build_sql_streaming_yaml(
    config: &SQLControllerConfig,
    dedup_map: &IndexMap<String, IntermediateAggConfig>,
    id_map: &HashMap<String, u32>,
) -> Result<YamlValue, ControllerError> {
    let aggregations: Vec<YamlValue> = dedup_map
        .iter()
        .map(|(key, cfg)| build_aggregation_entry(id_map[key], cfg))
        .collect();

    let mut root = serde_yaml::Mapping::new();
    root.insert(
        YamlValue::String("aggregations".to_string()),
        YamlValue::Sequence(aggregations),
    );
    root.insert(
        YamlValue::String("tables".to_string()),
        YamlValue::Sequence(build_tables_yaml(config)),
    );

    Ok(YamlValue::Mapping(root))
}

fn build_sql_inference_yaml(
    config: &SQLControllerConfig,
    cleanup_policy: CleanupPolicy,
    cleanup_policy_str: &str,
    query_keys_map: &IndexMap<String, Vec<(String, Option<u64>)>>,
    id_map: &HashMap<String, u32>,
) -> Result<YamlValue, ControllerError> {
    let mut cleanup_map = serde_yaml::Mapping::new();
    cleanup_map.insert(
        YamlValue::String("name".to_string()),
        YamlValue::String(cleanup_policy_str.to_string()),
    );

    let mut root = serde_yaml::Mapping::new();
    root.insert(
        YamlValue::String("cleanup_policy".to_string()),
        YamlValue::Mapping(cleanup_map),
    );
    root.insert(
        YamlValue::String("queries".to_string()),
        YamlValue::Sequence(build_queries_yaml(cleanup_policy, query_keys_map, id_map)),
    );
    root.insert(
        YamlValue::String("tables".to_string()),
        YamlValue::Sequence(build_tables_yaml(config)),
    );

    Ok(YamlValue::Mapping(root))
}
