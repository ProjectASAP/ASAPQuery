use asap_types::enums::CleanupPolicy;
use indexmap::IndexMap;
use indexmap::IndexSet;
use serde_yaml::Value as YamlValue;
use std::collections::HashMap;

use crate::config::input::ElasticDSLControllerConfig;
use crate::error::ControllerError;
use crate::generator::{
    build_aggregation_entry, build_queries_yaml, GeneratorOutput, KEY_AGGREGATIONS,
    KEY_CLEANUP_POLICY, KEY_NAME, KEY_QUERIES,
};
use crate::planner::agg_config::IntermediateAggConfig;
use crate::planner::elastic_dsl::ElasticSingleQueryProcessor;
use crate::StreamingEngine;
use elastic_dsl_utilities::ast_parsing::{extract_query_info, GroupBySpec, Predicate};

#[derive(Default, Clone)]
pub struct ElasticIndexSchemaBuilder {
    pub index: String,
    pub time_field: String,
    pub metric_columns: IndexSet<String>,
    pub metadata_columns: IndexSet<String>,
}

impl ElasticIndexSchemaBuilder {
    fn new(index: String, time_field: String) -> Self {
        Self {
            index,
            time_field,
            metric_columns: IndexSet::new(),
            metadata_columns: IndexSet::new(),
        }
    }

    fn update_from_query_info(
        &mut self,
        query_info: &elastic_dsl_utilities::ast_parsing::ElasticDSLQueryInfo,
    ) -> Result<(), ControllerError> {
        self.metric_columns.insert(query_info.target_field.clone());
        for field in collect_elastic_metadata_fields(query_info, &self.time_field) {
            self.metadata_columns.insert(field);
        }
        Ok(())
    }
}

pub struct ElasticRuntimeOptions {
    pub streaming_engine: StreamingEngine,
    pub data_ingestion_interval_ms: u64,
}

pub fn generate_elastic_plan(
    config: &ElasticDSLControllerConfig,
    opts: &ElasticRuntimeOptions,
) -> Result<GeneratorOutput, ControllerError> {
    let cleanup_policy = config
        .aggregate_cleanup
        .as_ref()
        .and_then(|c| c.policy)
        .unwrap_or(CleanupPolicy::ReadBased);

    // Validate T % data_ingestion_interval_ms == 0
    for qg in &config.query_groups {
        if qg.repetition_delay_ms % opts.data_ingestion_interval_ms != 0 {
            return Err(ControllerError::PlannerError(format!(
                "repetition_delay_ms {} is not a multiple of data_ingestion_interval_ms {}",
                qg.repetition_delay_ms, opts.data_ingestion_interval_ms
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
    // index -> schema builder derived from the queries targeting that index
    let mut index_schema_builders: IndexMap<String, ElasticIndexSchemaBuilder> = IndexMap::new();

    // First pass to build index schema builders from query info.
    for qg in &config.query_groups {
        for query_string in &qg.queries {
            let query_info = extract_query_info(query_string).ok_or_else(|| {
                ControllerError::ElasticDSLParse(format!(
                    "Failed to parse Elasticsearch DSL query: {}",
                    query_string
                ))
            })?;

            index_schema_builders
                .entry(qg.index.clone())
                .or_insert_with(|| {
                    ElasticIndexSchemaBuilder::new(qg.index.clone(), qg.time_field.clone())
                })
                .update_from_query_info(&query_info)?;
        }
    }

    // Second pass to build aggregation configs and query mappings.
    for qg in &config.query_groups {
        for query_string in &qg.queries {
            let processor = ElasticSingleQueryProcessor::new(
                query_string.clone(),
                qg.repetition_delay_ms,
                opts.data_ingestion_interval_ms,
                index_schema_builders[&qg.index].clone(),
                opts.streaming_engine,
                config.sketch_parameters.clone(),
                cleanup_policy,
            );

            let (configs, cleanup_param) = processor.get_streaming_aggregation_configs()?;

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

    let streaming_yaml = build_elastic_streaming_yaml(&dedup_map, &id_map)?;
    let inference_yaml = build_elastic_inference_yaml(
        cleanup_policy,
        &query_keys_map,
        &id_map,
        &index_schema_builders,
    )?;

    Ok(GeneratorOutput {
        punted_queries: Vec::new(),
        streaming_yaml,
        inference_yaml,
        aggregation_count: dedup_map.len(),
        query_count: query_keys_map.len(),
    })
}

fn build_elastic_streaming_yaml(
    dedup_map: &IndexMap<String, IntermediateAggConfig>,
    id_map: &HashMap<String, u32>,
) -> Result<YamlValue, ControllerError> {
    let aggregations: Vec<YamlValue> = dedup_map
        .iter()
        .map(|(key, cfg)| build_aggregation_entry(id_map[key], cfg))
        .collect();

    let mut root = serde_yaml::Mapping::new();
    root.insert(
        YamlValue::String(KEY_AGGREGATIONS.to_string()),
        YamlValue::Sequence(aggregations),
    );

    Ok(YamlValue::Mapping(root))
}

fn build_elastic_inference_yaml(
    cleanup_policy: CleanupPolicy,
    query_keys_map: &IndexMap<String, Vec<(String, Option<u64>)>>,
    id_map: &HashMap<String, u32>,
    index_schema_builders: &IndexMap<String, ElasticIndexSchemaBuilder>,
) -> Result<YamlValue, ControllerError> {
    let mut cleanup_map = serde_yaml::Mapping::new();
    cleanup_map.insert(
        YamlValue::String(KEY_NAME.to_string()),
        YamlValue::String(cleanup_policy.to_string()),
    );

    let mut root = serde_yaml::Mapping::new();
    root.insert(
        YamlValue::String(KEY_CLEANUP_POLICY.to_string()),
        YamlValue::Mapping(cleanup_map),
    );
    root.insert(
        YamlValue::String(KEY_QUERIES.to_string()),
        YamlValue::Sequence(build_queries_yaml(cleanup_policy, query_keys_map, id_map)),
    );
    root.insert(
        YamlValue::String("indices".to_string()),
        YamlValue::Sequence(
            index_schema_builders
                .iter()
                .map(|(index_name, builder)| build_elastic_index_yaml(index_name, builder))
                .collect(),
        ),
    );

    Ok(YamlValue::Mapping(root))
}

fn build_elastic_index_yaml(index_name: &str, builder: &ElasticIndexSchemaBuilder) -> YamlValue {
    let mut map = serde_yaml::Mapping::new();
    map.insert(
        YamlValue::String("name".to_string()),
        YamlValue::String(index_name.to_string()),
    );
    map.insert(
        YamlValue::String("time_field".to_string()),
        YamlValue::String(builder.time_field.clone()),
    );
    map.insert(
        YamlValue::String("metric_columns".to_string()),
        YamlValue::Sequence(
            builder
                .metric_columns
                .iter()
                .cloned()
                .map(YamlValue::String)
                .collect(),
        ),
    );
    map.insert(
        YamlValue::String("metadata_columns".to_string()),
        YamlValue::Sequence(
            builder
                .metadata_columns
                .iter()
                .cloned()
                .map(YamlValue::String)
                .collect(),
        ),
    );

    YamlValue::Mapping(map)
}

fn collect_elastic_metadata_fields(
    query_info: &elastic_dsl_utilities::ast_parsing::ElasticDSLQueryInfo,
    time_field: &str,
) -> IndexSet<String> {
    let mut fields = IndexSet::new();

    for predicate in &query_info.predicates {
        match predicate {
            Predicate::Term { field, .. } => {
                if field != time_field {
                    fields.insert(field.clone());
                }
            }
            Predicate::Range { field, .. } => {
                if field != time_field {
                    fields.insert(field.clone());
                }
            }
        }
    }

    if let Some(group_by_buckets) = &query_info.group_by_buckets {
        match group_by_buckets {
            GroupBySpec::Fields(group_fields) => {
                for field in group_fields {
                    if field != time_field {
                        fields.insert(field.clone());
                    }
                }
            }
            GroupBySpec::Filters(predicates) => {
                for predicate in predicates {
                    match predicate {
                        Predicate::Term { field, .. } | Predicate::Range { field, .. } => {
                            if field != time_field {
                                fields.insert(field.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    fields
}
