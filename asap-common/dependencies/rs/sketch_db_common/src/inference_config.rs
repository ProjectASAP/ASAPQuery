use anyhow::Result;
use serde_yaml::Value;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;

use crate::aggregation_reference::AggregationReference;
use crate::enums::{CleanupPolicy, QueryLanguage};
use crate::promql_schema::PromQLSchema;
use crate::query_config::QueryConfig;
use promql_utilities::data_model::KeyByLabelNames;
use sql_utilities::sqlhelper::{SQLSchema, Table};

/// Schema configuration that can be either PromQL or SQL format
#[derive(Debug, Clone)]
pub enum SchemaConfig {
    PromQL(PromQLSchema),
    SQL(SQLSchema),
    ElasticQueryDSL,
    ElasticSQL,
}

#[derive(Debug, Clone)]
pub struct InferenceConfig {
    pub schema: SchemaConfig,
    pub query_configs: Vec<QueryConfig>,
    pub cleanup_policy: CleanupPolicy,
}

impl InferenceConfig {
    pub fn new(query_language: QueryLanguage, cleanup_policy: CleanupPolicy) -> Self {
        let schema = match query_language {
            QueryLanguage::promql => SchemaConfig::PromQL(PromQLSchema::new()),
            QueryLanguage::sql => SchemaConfig::SQL(SQLSchema::new(Vec::new())),
            QueryLanguage::elastic_querydsl => SchemaConfig::ElasticQueryDSL,
            QueryLanguage::elastic_sql => SchemaConfig::ElasticSQL,
        };
        Self {
            schema,
            query_configs: Vec::new(),
            cleanup_policy,
        }
    }

    pub fn from_yaml_file(yaml_file: &str, query_language: QueryLanguage) -> Result<Self> {
        let file = File::open(yaml_file)?;
        let reader = BufReader::new(file);
        let data: Value = serde_yaml::from_reader(reader)?;

        Self::from_yaml_data(&data, query_language)
    }

    pub fn from_yaml_data(data: &Value, query_language: QueryLanguage) -> Result<Self> {
        let schema = match query_language {
            QueryLanguage::promql => {
                let promql_schema = Self::parse_promql_schema(data)?;
                SchemaConfig::PromQL(promql_schema)
            }
            QueryLanguage::sql => {
                let sql_schema = Self::parse_sql_schema(data)?;
                SchemaConfig::SQL(sql_schema)
            }
            QueryLanguage::elastic_querydsl => SchemaConfig::ElasticQueryDSL,
            QueryLanguage::elastic_sql => SchemaConfig::ElasticSQL,
        };

        let cleanup_policy = Self::parse_cleanup_policy(data)?;
        let query_configs = Self::parse_query_configs(data, cleanup_policy)?;

        Ok(Self {
            schema,
            query_configs,
            cleanup_policy,
        })
    }

    /// Parse PromQL schema from YAML data (metrics: key)
    fn parse_promql_schema(data: &Value) -> Result<PromQLSchema> {
        let mut promql_schema = PromQLSchema::new();
        if let Some(metrics) = data.get("metrics") {
            if let Some(metrics_map) = metrics.as_mapping() {
                for (metric_name_val, labels_val) in metrics_map {
                    if let (Some(metric_name), Some(labels_seq)) =
                        (metric_name_val.as_str(), labels_val.as_sequence())
                    {
                        let labels: Vec<String> = labels_seq
                            .iter()
                            .filter_map(|v| v.as_str())
                            .map(|s| s.to_string())
                            .collect();
                        let key_by_label_names = KeyByLabelNames::new(labels);
                        promql_schema =
                            promql_schema.add_metric(metric_name.to_string(), key_by_label_names);
                    }
                }
            }
        }
        Ok(promql_schema)
    }

    /// Parse SQL schema from YAML data (tables: key at top level, matching ArroyoSketch format)
    fn parse_sql_schema(data: &Value) -> Result<SQLSchema> {
        let tables_data = data
            .get("tables")
            .and_then(|v| v.as_sequence())
            .ok_or_else(|| {
                anyhow::anyhow!("Missing or invalid tables field for SQL query language")
            })?;

        let mut tables = Vec::new();
        for table_data in tables_data {
            let name = table_data
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing name field in table"))?
                .to_string();

            let time_column = table_data
                .get("time_column")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing time_column field in table {}", name))?
                .to_string();

            let value_columns: HashSet<String> = table_data
                .get("value_columns")
                .and_then(|v| v.as_sequence())
                .ok_or_else(|| anyhow::anyhow!("Missing value_columns field in table {}", name))?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect();

            let metadata_columns: HashSet<String> = table_data
                .get("metadata_columns")
                .and_then(|v| v.as_sequence())
                .ok_or_else(|| anyhow::anyhow!("Missing metadata_columns field in table {}", name))?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect();

            tables.push(Table::new(
                name,
                time_column,
                value_columns,
                metadata_columns,
            ));
        }

        Ok(SQLSchema::new(tables))
    }

    /// Parse cleanup policy from YAML data. Errors if not specified.
    fn parse_cleanup_policy(data: &Value) -> Result<CleanupPolicy> {
        let cleanup_policy_data = data.get("cleanup_policy").ok_or_else(|| {
            anyhow::anyhow!(
                "Missing cleanup_policy section in inference_config.yaml. \
                 Must specify cleanup_policy.name as one of: circular_buffer, read_based, no_cleanup"
            )
        })?;

        let name = cleanup_policy_data
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Missing cleanup_policy.name in inference_config.yaml. \
                     Must be one of: circular_buffer, read_based, no_cleanup"
                )
            })?;

        match name {
            "circular_buffer" => Ok(CleanupPolicy::CircularBuffer),
            "read_based" => Ok(CleanupPolicy::ReadBased),
            "no_cleanup" => Ok(CleanupPolicy::NoCleanup),
            _ => Err(anyhow::anyhow!(
                "Invalid cleanup policy: '{}'. Valid options: circular_buffer, read_based, no_cleanup",
                name
            )),
        }
    }

    fn parse_query_configs(
        data: &Value,
        cleanup_policy: CleanupPolicy,
    ) -> Result<Vec<QueryConfig>> {
        let query_configs = if let Some(queries) = data.get("queries").and_then(|v| v.as_sequence())
        {
            let mut configs = Vec::new();
            for query_data in queries {
                let query = query_data
                    .get("query")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("Missing query field"))?
                    .to_string();

                let aggregations = if let Some(aggregations_data) =
                    query_data.get("aggregations").and_then(|v| v.as_sequence())
                {
                    let mut agg_refs = Vec::new();
                    for agg_data in aggregations_data {
                        let aggregation_id = agg_data
                            .get("aggregation_id")
                            .and_then(|v| v.as_u64())
                            .ok_or_else(|| {
                                anyhow::anyhow!("Missing aggregation_id in aggregation")
                            })?;

                        let agg_ref = match cleanup_policy {
                            CleanupPolicy::CircularBuffer => {
                                let num_aggregates_to_retain = agg_data
                                    .get("num_aggregates_to_retain")
                                    .and_then(|v| v.as_u64());
                                AggregationReference::new(aggregation_id, num_aggregates_to_retain)
                            }
                            CleanupPolicy::ReadBased => {
                                let read_count_threshold = agg_data
                                    .get("read_count_threshold")
                                    .and_then(|v| v.as_u64());
                                AggregationReference::with_read_count_threshold(
                                    aggregation_id,
                                    read_count_threshold,
                                )
                            }
                            CleanupPolicy::NoCleanup => {
                                AggregationReference::new(aggregation_id, None)
                            }
                        };
                        agg_refs.push(agg_ref);
                    }
                    agg_refs
                } else {
                    Vec::new()
                };

                let config = QueryConfig::new(query).with_aggregations(aggregations);
                configs.push(config);
            }
            configs
        } else {
            Vec::new()
        };
        Ok(query_configs)
    }
}
