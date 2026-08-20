use asap_types::computed_label::ComputedLabelConfig;
use asap_types::enums::CleanupPolicy;
use asap_types::stateful_transition::StatefulTransitionConfig;
use indexmap::IndexMap;
use serde_yaml::Value as YamlValue;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::input::SQLControllerConfig;
use crate::error::ControllerError;
use crate::generator::{
    build_aggregation_entry, build_queries_yaml, GeneratorOutput, PuntedQuery, KEY_AGGREGATIONS,
    KEY_CLEANUP_POLICY, KEY_METADATA_COLUMNS, KEY_NAME, KEY_QUERIES, KEY_TABLES, KEY_TIME_COLUMN,
    KEY_VALUE_COLUMNS,
};
use crate::planner::agg_config::IntermediateAggConfig;
use crate::planner::sql::SQLSingleQueryProcessor;
use crate::StreamingEngine;
use sql_utilities::ast_matching::pattern_rewrites::{
    build_multi_aggregate_surrogates, parse_multi_aggregate_query,
};

pub struct SQLRuntimeOptions {
    pub streaming_engine: StreamingEngine,
    pub query_evaluation_time: Option<f64>,
    pub data_ingestion_interval_ms: u64,
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

    // Validate that all tables have metadata_columns populated (either from config
    // or filled in by from_file_with_discovery before reaching here).
    for t in &config.tables {
        if t.metadata_columns.is_empty() {
            return Err(ControllerError::PlannerError(format!(
                "Table '{}' has no metadata_columns. List them in the config file \
                 or pass --clickhouse-url for auto-discovery.",
                t.name
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
    // Stateful transitions the planner auto-detected (e.g. lagInFrame queries),
    // deduped by derived metric_name - multiple queries referencing the same
    // derived stream only need one operator maintaining it.
    let mut stateful_transitions: IndexMap<String, StatefulTransitionConfig> = IndexMap::new();
    // Computed labels the planner auto-detected (e.g. origin-ASN extraction),
    // deduped by label name.
    let mut computed_label_cols: IndexMap<String, ComputedLabelConfig> = IndexMap::new();
    // Queries with no GROUP BY and no aggregate function - raw/DISTINCT row
    // scans no precomputed summary can ever answer. Left out of both
    // dedup_map and query_keys_map entirely, so they never appear in
    // inference_config.yaml; at query time the local engine simply won't
    // recognize them and (if forward_unsupported_queries is enabled) they
    // fall through to the ClickHouse fallback for an exact answer.
    let mut punted_queries: Vec<PuntedQuery> = Vec::new();

    for qg in &config.query_groups {
        for query_string in &qg.queries {
            // Multi-aggregate queries (2+ aggregate expressions over one
            // GROUP BY, e.g. `count()` and `uniqExact(...)` side by side)
            // can't be represented as a single SQLQueryData - split into N
            // independent single-aggregate surrogates and plan + register
            // each one separately, so the query engine's ordinary
            // structural matcher (find_query_config_sql) can find each
            // surrogate directly at serve time. The engine reconstructs the
            // identical split from the raw incoming query
            // (handle_multi_aggregate_sql) and merges the N results back
            // into one row per group.
            if let Some(mm) = parse_multi_aggregate_query(query_string) {
                for surrogate in build_multi_aggregate_surrogates(&mm) {
                    let sub_processor = SQLSingleQueryProcessor::new(
                        surrogate.clone(),
                        qg.repetition_delay_ms,
                        opts.data_ingestion_interval_ms,
                        config.tables.clone(),
                        opts.streaming_engine,
                        config.sketch_parameters.clone(),
                        cleanup_policy,
                    );

                    let (configs, cleanup_param, stateful_transition, template_override, labels) =
                        sub_processor.get_streaming_aggregation_configs(eval_time)?;

                    if let Some(st) = stateful_transition {
                        stateful_transitions
                            .entry(st.metric_name.clone())
                            .or_insert(st);
                    }
                    for (label_name, cfg) in labels {
                        computed_label_cols.entry(label_name).or_insert(cfg);
                    }

                    let mut keys_for_query = Vec::new();
                    for config_item in configs {
                        let key = config_item.identifying_key();
                        keys_for_query.push((key.clone(), cleanup_param));
                        dedup_map.entry(key).or_insert(config_item);
                    }
                    let registered_query = template_override.unwrap_or(surrogate);
                    query_keys_map.insert(registered_query, keys_for_query);
                }
                continue;
            }

            let processor = SQLSingleQueryProcessor::new(
                query_string.clone(),
                qg.repetition_delay_ms,
                opts.data_ingestion_interval_ms,
                config.tables.clone(),
                opts.streaming_engine,
                config.sketch_parameters.clone(),
                cleanup_policy,
            );

            if processor.is_exact_only() {
                tracing::warn!(
                    query = %query_string,
                    "punting query: no aggregate function and no GROUP BY, so no \
                     precomputed summary can answer it; relying on the ClickHouse \
                     fallback (forward_unsupported_queries) for an exact answer"
                );
                punted_queries.push(PuntedQuery {
                    query: query_string.clone(),
                });
                continue;
            }

            let (configs, cleanup_param, stateful_transition, template_override, labels) =
                processor.get_streaming_aggregation_configs(eval_time)?;

            if let Some(st) = stateful_transition {
                stateful_transitions
                    .entry(st.metric_name.clone())
                    .or_insert(st);
            }
            for (label_name, cfg) in labels {
                computed_label_cols.entry(label_name).or_insert(cfg);
            }

            let mut keys_for_query = Vec::new();
            for config_item in configs {
                let key = config_item.identifying_key();
                keys_for_query.push((key.clone(), cleanup_param));
                dedup_map.entry(key).or_insert(config_item);
            }
            // Some query shapes (lagInFrame CTEs, MOAS's DISTINCT_SET) aren't
            // parseable by SQLPatternParser at all, so the raw query can never
            // be matched against at query time either - the surrogate that was
            // actually planned against must be what's registered here, or the
            // query-time matcher will never find this aggregation no matter
            // how correctly the runtime rewrites the incoming query.
            let registered_query = template_override.unwrap_or_else(|| query_string.clone());
            query_keys_map.insert(registered_query, keys_for_query);
        }
    }

    // Assign sequential IDs
    let mut id_map: HashMap<String, u32> = HashMap::new();
    for (idx, key) in dedup_map.keys().enumerate() {
        id_map.insert(key.clone(), idx as u32 + 1);
    }

    let streaming_yaml =
        build_sql_streaming_yaml(
            config,
            &dedup_map,
            &id_map,
            &stateful_transitions,
            &computed_label_cols,
        )?;
    let extra_metadata_columns: Vec<String> = computed_label_cols.keys().cloned().collect();
    let inference_yaml = build_sql_inference_yaml(
        config,
        cleanup_policy,
        &query_keys_map,
        &id_map,
        &extra_metadata_columns,
    )?;

    Ok(GeneratorOutput {
        punted_queries,
        streaming_yaml,
        inference_yaml,
        aggregation_count: dedup_map.len(),
        query_count: query_keys_map.len(),
    })
}

/// `extra_metadata_columns` is every computed-label name the planner
/// auto-detected (e.g. "origin_asn") across the whole workload. It has to
/// land in the *emitted* tables list, not just be used locally while
/// building one query's aggregation config: this is the schema the engine
/// itself rebuilds from streaming_config.yaml/inference_config.yaml to
/// parse and match incoming queries at serve time. A computed label that's
/// valid enough to plan against but never makes it into this list is
/// invisible to the engine's own schema validation, so even a perfectly
/// rewritten runtime query fails the same "not present for metric" check
/// the planner would have hit without its own local augmentation.
fn build_tables_yaml(
    config: &SQLControllerConfig,
    extra_metadata_columns: &[String],
) -> Vec<YamlValue> {
    config
        .tables
        .iter()
        .map(|t| {
            let mut map = serde_yaml::Mapping::new();
            map.insert(
                YamlValue::String(KEY_NAME.to_string()),
                YamlValue::String(t.name.clone()),
            );
            map.insert(
                YamlValue::String(KEY_TIME_COLUMN.to_string()),
                YamlValue::String(t.time_column.clone()),
            );
            map.insert(
                YamlValue::String(KEY_VALUE_COLUMNS.to_string()),
                YamlValue::Sequence(
                    t.value_columns
                        .iter()
                        .map(|c| YamlValue::String(c.clone()))
                        .collect(),
                ),
            );
            let mut metadata_columns: Vec<String> = t.metadata_columns.clone();
            for extra in extra_metadata_columns {
                if !metadata_columns.iter().any(|c| c == extra) {
                    metadata_columns.push(extra.clone());
                }
            }
            map.insert(
                YamlValue::String(KEY_METADATA_COLUMNS.to_string()),
                YamlValue::Sequence(
                    metadata_columns
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
    stateful_transitions: &IndexMap<String, StatefulTransitionConfig>,
    computed_label_cols: &IndexMap<String, ComputedLabelConfig>,
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
    let extra_metadata_columns: Vec<String> = computed_label_cols.keys().cloned().collect();
    root.insert(
        YamlValue::String(KEY_TABLES.to_string()),
        YamlValue::Sequence(build_tables_yaml(config, &extra_metadata_columns)),
    );

    if !stateful_transitions.is_empty() {
        let entries: Result<Vec<YamlValue>, ControllerError> = stateful_transitions
            .values()
            .map(|st| {
                serde_yaml::to_value(st)
                    .map_err(|e| ControllerError::PlannerError(e.to_string()))
            })
            .collect();
        root.insert(
            YamlValue::String("stateful_transitions".to_string()),
            YamlValue::Sequence(entries?),
        );
    }

    if !computed_label_cols.is_empty() {
        let mut labels_map = serde_yaml::Mapping::new();
        for (label_name, cfg) in computed_label_cols.iter() {
            let value = serde_yaml::to_value(cfg)
                .map_err(|e| ControllerError::PlannerError(e.to_string()))?;
            labels_map.insert(YamlValue::String(label_name.clone()), value);
        }
        root.insert(
            YamlValue::String("computed_label_cols".to_string()),
            YamlValue::Mapping(labels_map),
        );
    }

    Ok(YamlValue::Mapping(root))
}

fn build_sql_inference_yaml(
    config: &SQLControllerConfig,
    cleanup_policy: CleanupPolicy,
    query_keys_map: &IndexMap<String, Vec<(String, Option<u64>)>>,
    id_map: &HashMap<String, u32>,
    extra_metadata_columns: &[String],
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
        YamlValue::String(KEY_TABLES.to_string()),
        YamlValue::Sequence(build_tables_yaml(config, extra_metadata_columns)),
    );

    Ok(YamlValue::Mapping(root))
}
