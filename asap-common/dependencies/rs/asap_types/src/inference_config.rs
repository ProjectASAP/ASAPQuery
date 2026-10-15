use anyhow::Result;
use serde_yaml::Value;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;

use crate::aggregation_reference::AggregationReference;
use crate::enums::{CleanupPolicy, QueryLanguage};
use crate::promql_schema::PromQLSchema;
use crate::query_config::{QueryConfig, QueryTimeAggregation};
use elastic_dsl_utilities::{ElasticIndexSchema, ElasticMappingSchema};
use promql_utilities::data_model::KeyByLabelNames;
use sql_utilities::sqlhelper::{SQLSchema, Table};

/// Schema configuration that can be either PromQL or SQL format
#[derive(Debug, Clone)]
pub enum SchemaConfig {
    PromQL(PromQLSchema),
    SQL(SQLSchema),
    ElasticQueryDSL(ElasticMappingSchema),
    ElasticSQL(SQLSchema),
}

/// How the engine answers one RQE leaf directly: the aggregations it reads
/// and the query-time stages that regroup them to the RQE's grouping.
#[derive(Debug, Clone)]
pub struct RqeConfig {
    pub aggregations: Vec<AggregationReference>,
    pub query_time_aggregations: Vec<QueryTimeAggregation>,
}

#[derive(Debug, Clone)]
pub struct InferenceConfig {
    pub schema: SchemaConfig,
    pub query_configs: Vec<QueryConfig>,
    pub cleanup_policy: CleanupPolicy,
    /// RQE id -> how to answer that RQE leaf; empty unless planned from RQE trees.
    pub rqe_configs: HashMap<String, RqeConfig>,
}

impl InferenceConfig {
    pub fn new(query_language: QueryLanguage, cleanup_policy: CleanupPolicy) -> Self {
        let schema = match query_language {
            QueryLanguage::promql => SchemaConfig::PromQL(PromQLSchema::new()),
            QueryLanguage::sql => SchemaConfig::SQL(SQLSchema::new(Vec::new())),
            QueryLanguage::elastic_querydsl => {
                SchemaConfig::ElasticQueryDSL(ElasticMappingSchema::new(Vec::new()))
            }
            QueryLanguage::elastic_sql => SchemaConfig::ElasticSQL(SQLSchema::new(Vec::new())),
        };
        Self {
            schema,
            query_configs: Vec::new(),
            cleanup_policy,
            rqe_configs: HashMap::new(),
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
            QueryLanguage::elastic_querydsl => {
                let elastic_schema = Self::parse_elastic_querydsl_schema(data)?;
                SchemaConfig::ElasticQueryDSL(elastic_schema)
            }
            QueryLanguage::elastic_sql => {
                let sql_schema = Self::parse_sql_schema(data)?;
                SchemaConfig::SQL(sql_schema)
            }
        };

        let cleanup_policy = Self::parse_cleanup_policy(data)?;
        let query_configs = Self::parse_query_configs(data, cleanup_policy)?;
        let rqe_configs = Self::parse_rqe_configs(data, cleanup_policy)?;

        Ok(Self {
            schema,
            query_configs,
            cleanup_policy,
            rqe_configs,
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

    /// Parse Elasticsearch mapping schema from YAML data (indices: key at top level).
    fn parse_elastic_querydsl_schema(data: &Value) -> Result<ElasticMappingSchema> {
        let Some(indices_data) = data.get("indices").and_then(|v| v.as_sequence()) else {
            return Ok(ElasticMappingSchema::new(Vec::new()));
        };

        let mut indices = Vec::new();
        for index_data in indices_data {
            let name = index_data
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing name field in elastic index"))?
                .to_string();

            let time_field = index_data
                .get("time_field")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("Missing time_field field in elastic index {name}"))?
                .to_string();

            let metric_columns: HashSet<String> = index_data
                .get("metric_columns")
                .and_then(|v| v.as_sequence())
                .ok_or_else(|| {
                    anyhow::anyhow!("Missing metric_columns field in elastic index {name}")
                })?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect();

            let metadata_columns: HashSet<String> = index_data
                .get("metadata_columns")
                .and_then(|v| v.as_sequence())
                .ok_or_else(|| {
                    anyhow::anyhow!("Missing metadata_columns field in elastic index {name}")
                })?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect();

            indices.push((
                name,
                ElasticIndexSchema::new(time_field, metric_columns, metadata_columns),
            ));
        }

        Ok(ElasticMappingSchema::new(indices))
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

        name.parse::<CleanupPolicy>().map_err(|_| {
            anyhow::anyhow!(
                "Invalid cleanup policy: '{}'. Valid options: circular_buffer, read_based, no_cleanup",
                name
            )
        })
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
                let planned_subquery = query_data
                    .get("planned_subquery")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("Missing planned_subquery field"))?
                    .to_string();
                let query_time_aggregations = query_data
                    .get("query_time_aggregations")
                    .ok_or_else(|| anyhow::anyhow!("Missing query_time_aggregations field"))
                    .and_then(|value| {
                        serde_yaml::from_value::<Vec<QueryTimeAggregation>>(value.clone())
                            .map_err(anyhow::Error::from)
                    })?;

                let aggregations = match query_data.get("aggregations") {
                    Some(aggregations) => {
                        Self::parse_aggregation_refs(aggregations, cleanup_policy)?
                    }
                    None => Vec::new(),
                };

                let config =
                    QueryConfig::with_plan(query, planned_subquery, query_time_aggregations)
                        .with_aggregations(aggregations);
                config
                    .validate_execution_plan()
                    .map_err(|error| anyhow::anyhow!("Invalid query execution plan: {error}"))?;
                configs.push(config);
            }
            configs
        } else {
            Vec::new()
        };
        Ok(query_configs)
    }

    /// A sequence of `{aggregation_id, num_aggregates_to_retain | read_count_threshold}`.
    fn parse_aggregation_refs(
        aggregations_data: &Value,
        cleanup_policy: CleanupPolicy,
    ) -> Result<Vec<AggregationReference>> {
        let Some(aggregations_data) = aggregations_data.as_sequence() else {
            return Ok(Vec::new());
        };
        let mut agg_refs = Vec::new();
        for agg_data in aggregations_data {
            let aggregation_id = agg_data
                .get("aggregation_id")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| anyhow::anyhow!("Missing aggregation_id in aggregation"))?;

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
                CleanupPolicy::NoCleanup => AggregationReference::new(aggregation_id, None),
            };
            agg_refs.push(agg_ref);
        }
        Ok(agg_refs)
    }

    /// The optional `rqe_configs` mapping: RQE id -> `{aggregations,
    /// query_time_aggregations}`, both required.
    fn parse_rqe_configs(
        data: &Value,
        cleanup_policy: CleanupPolicy,
    ) -> Result<HashMap<String, RqeConfig>> {
        let Some(rqe_configs) = data.get("rqe_configs") else {
            return Ok(HashMap::new());
        };
        let rqe_configs = rqe_configs
            .as_mapping()
            .ok_or_else(|| anyhow::anyhow!("rqe_configs must map RQE ids to configs"))?;
        let mut configs = HashMap::new();
        for (id, config) in rqe_configs {
            let id = id
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("rqe_configs key {id:?} is not a string"))?;
            let aggregations = config
                .get("aggregations")
                .ok_or_else(|| anyhow::anyhow!("RQE {id:?}: missing aggregations"))
                .and_then(|value| Self::parse_aggregation_refs(value, cleanup_policy))?;
            if aggregations.is_empty() {
                anyhow::bail!("RQE {id:?}: aggregations is empty");
            }
            let query_time_aggregations = config
                .get("query_time_aggregations")
                .ok_or_else(|| anyhow::anyhow!("RQE {id:?}: missing query_time_aggregations"))
                .and_then(|value| {
                    serde_yaml::from_value::<Vec<QueryTimeAggregation>>(value.clone())
                        .map_err(anyhow::Error::from)
                })?;
            for stage in &query_time_aggregations {
                stage
                    .validate()
                    .map_err(|error| anyhow::anyhow!("RQE {id:?}: {error}"))?;
            }
            configs.insert(
                id.to_string(),
                RqeConfig {
                    aggregations,
                    query_time_aggregations,
                },
            );
        }
        Ok(configs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_config::QueryTimeAggregationOperator;

    #[test]
    fn rejects_a_query_without_a_planned_subquery() {
        let data: Value = serde_yaml::from_str(
            r#"
cleanup_policy:
  name: no_cleanup
metrics: {}
queries:
  - query: "sum(metric)"
    query_time_aggregations: []
    aggregations: []
"#,
        )
        .unwrap();

        let error = InferenceConfig::from_yaml_data(&data, QueryLanguage::promql)
            .expect_err("query plans must name their planned subquery");

        assert!(error.to_string().contains("planned_subquery"));
    }

    #[test]
    fn rejects_a_query_without_a_query_time_pipeline() {
        let data: Value = serde_yaml::from_str(
            r#"
cleanup_policy:
  name: no_cleanup
metrics: {}
queries:
  - query: "sum(metric)"
    planned_subquery: "sum(metric)"
    aggregations: []
"#,
        )
        .unwrap();

        let error = InferenceConfig::from_yaml_data(&data, QueryLanguage::promql)
            .expect_err("query plans must declare their query-time pipeline");

        assert!(error.to_string().contains("query_time_aggregations"));
    }

    #[test]
    fn rejects_an_invalid_query_time_aggregation() {
        let data: Value = serde_yaml::from_str(
            r#"
cleanup_policy:
  name: no_cleanup
metrics: {}
queries:
  - query: "quantile(1.5, sum(metric))"
    planned_subquery: "sum(metric)"
    query_time_aggregations:
      - operator: quantile
        grouping:
          mode: all
          labels: []
        parameter: 1.5
    aggregations: []
"#,
        )
        .unwrap();

        let error = InferenceConfig::from_yaml_data(&data, QueryLanguage::promql)
            .expect_err("invalid pipeline parameters must fail during config loading");

        assert!(error.to_string().contains("quantile"));
    }

    #[test]
    fn loads_rqe_configs_with_their_roll_up() {
        let data: Value = serde_yaml::from_str(
            r#"
cleanup_policy:
  name: circular_buffer
metrics: {}
queries: []
rqe_configs:
  sum_by_job:
    aggregations:
      - aggregation_id: 2
        num_aggregates_to_retain: 5
    query_time_aggregations:
      - operator: sum
        grouping:
          mode: by
          labels: [job]
        parameter: null
"#,
        )
        .unwrap();

        let config = InferenceConfig::from_yaml_data(&data, QueryLanguage::promql).unwrap();

        let rqe = &config.rqe_configs["sum_by_job"];
        let [aggregation] = rqe.aggregations.as_slice() else {
            panic!("{:?}", rqe.aggregations);
        };
        assert_eq!(aggregation.aggregation_id, 2);
        assert_eq!(aggregation.num_aggregates_to_retain, Some(5));
        let [stage] = rqe.query_time_aggregations.as_slice() else {
            panic!("{:?}", rqe.query_time_aggregations);
        };
        assert!(matches!(stage.operator, QueryTimeAggregationOperator::Sum));
        assert_eq!(stage.grouping.labels, ["job"]);
    }

    #[test]
    fn a_config_without_rqe_configs_has_none() {
        let data: Value =
            serde_yaml::from_str("cleanup_policy: {name: no_cleanup}\nmetrics: {}\n").unwrap();
        let config = InferenceConfig::from_yaml_data(&data, QueryLanguage::promql).unwrap();
        assert!(config.rqe_configs.is_empty());
    }

    #[test]
    fn rejects_an_rqe_config_missing_a_field_or_with_an_invalid_stage() {
        for (rqe, reason) in [
            ("{query_time_aggregations: []}", "missing aggregations"),
            (
                "{aggregations: [], query_time_aggregations: []}",
                "aggregations is empty",
            ),
            (
                "{aggregations: [{aggregation_id: 1}]}",
                "missing query_time_aggregations",
            ),
            (
                "{aggregations: [{aggregation_id: 1}], query_time_aggregations: \
                 [{operator: sum, grouping: {mode: by, labels: []}, parameter: null}]}",
                "by grouping must name at least one label",
            ),
        ] {
            let yaml = format!(
                "cleanup_policy: {{name: no_cleanup}}\nmetrics: {{}}\nrqe_configs: {{r: {rqe}}}\n"
            );
            let data: Value = serde_yaml::from_str(&yaml).unwrap();
            let error =
                InferenceConfig::from_yaml_data(&data, QueryLanguage::promql).expect_err("refused");
            assert!(error.to_string().contains(reason), "{error}");
            assert!(error.to_string().contains("\"r\""), "{error}");
        }
    }
}
