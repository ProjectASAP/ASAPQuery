use anyhow::Result;
use core::panic;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::ops::Index;

use crate::aggregation_config::{AggregationConfig, AggregationIdInfo};
use crate::capability_matching::find_compatible_aggregation as common_find_compatible;
use crate::enums::QueryLanguage;
use crate::inference_config::{InferenceConfig, SchemaConfig};
use crate::query_requirements::QueryRequirements;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingConfig {
    pub aggregation_configs: HashMap<u64, AggregationConfig>,
}

impl StreamingConfig {
    pub fn new(aggregation_configs: HashMap<u64, AggregationConfig>) -> Self {
        Self {
            aggregation_configs,
        }
    }

    pub fn get_aggregation_config(&self, aggregation_id: u64) -> Option<&AggregationConfig> {
        self.aggregation_configs.get(&aggregation_id)
    }

    pub fn get_all_aggregation_configs(&self) -> &HashMap<u64, AggregationConfig> {
        &self.aggregation_configs
    }

    pub fn contains(&self, aggregation_id: u64) -> bool {
        self.aggregation_configs.contains_key(&aggregation_id)
    }

    pub fn from_yaml_file(yaml_file: &str) -> Result<Self> {
        let file = File::open(yaml_file)?;
        let reader = BufReader::new(file);
        let data: Value = serde_yaml::from_reader(reader)?;

        Self::from_yaml_data(&data, None)
    }

    pub fn from_yaml_data(
        data: &Value,
        inference_config: Option<&InferenceConfig>,
    ) -> Result<Self> {
        let mut retention_map: HashMap<u64, u64> = HashMap::new();
        let mut read_count_threshold_map: HashMap<u64, u64> = HashMap::new();

        if let Some(inference_config) = inference_config {
            for query_config in &inference_config.query_configs {
                for aggregation in &query_config.aggregations {
                    let aggregation_id = aggregation.aggregation_id;
                    if let Some(num_aggregates) = aggregation.num_aggregates_to_retain {
                        // OLD: Keep last value only (for backwards compatibility)
                        retention_map.insert(aggregation_id, num_aggregates);

                        // NEW: Sum up num_aggregates_to_retain across all queries
                        *read_count_threshold_map.entry(aggregation_id).or_insert(0) +=
                            num_aggregates;
                    }
                }
            }
        }

        // Derive query_language from inference_config schema
        let query_language = inference_config
            .map(|ic| match &ic.schema {
                SchemaConfig::PromQL(_) => QueryLanguage::promql,
                SchemaConfig::SQL(_) => QueryLanguage::sql,
                SchemaConfig::ElasticQueryDSL => QueryLanguage::elastic_querydsl,
                SchemaConfig::ElasticSQL(_) => QueryLanguage::elastic_sql,
            })
            .unwrap_or(QueryLanguage::promql); // Default to promql if no inference_config

        let mut aggregation_configs: HashMap<u64, AggregationConfig> = HashMap::new();

        if let Some(aggregations) = data.get("aggregations").and_then(|v| v.as_sequence()) {
            for aggregation_data in aggregations {
                if let Some(aggregation_id) = aggregation_data.get("aggregationId") {
                    let aggregation_id_u64 = aggregation_id.as_u64().or_else(|| panic!()).unwrap();
                    let num_aggregates_to_retain = retention_map.get(&aggregation_id_u64);
                    let read_count_threshold = read_count_threshold_map.get(&aggregation_id_u64);
                    let config = AggregationConfig::from_yaml_data(
                        aggregation_data,
                        num_aggregates_to_retain.copied(),
                        read_count_threshold.copied(),
                        query_language,
                    )?;
                    aggregation_configs.insert(aggregation_id_u64, config);
                }
            }
        }

        Ok(Self::new(aggregation_configs))
    }
}

impl StreamingConfig {
    /// Find a compatible aggregation for the given requirements using capability-based matching.
    /// Delegates to `sketch_db_common::find_compatible_aggregation`.
    pub fn find_compatible_aggregation(
        &self,
        requirements: &QueryRequirements,
    ) -> Option<AggregationIdInfo> {
        common_find_compatible(&self.aggregation_configs, requirements)
    }
}

impl Index<u64> for StreamingConfig {
    type Output = AggregationConfig;

    fn index(&self, aggregation_id: u64) -> &Self::Output {
        &self.aggregation_configs[&aggregation_id]
    }
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self::new(HashMap::new())
    }
}
