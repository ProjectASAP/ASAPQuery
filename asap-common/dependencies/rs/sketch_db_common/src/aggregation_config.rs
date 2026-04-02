use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_yaml;
use std::collections::HashMap;

use crate::enums::QueryLanguage;
use crate::traits::SerializableToSink;
use crate::utils::normalize_spatial_filter;
use promql_utilities::data_model::KeyByLabelNames;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationConfig {
    pub aggregation_id: u64,
    pub aggregation_type: String,
    pub aggregation_sub_type: String,
    pub parameters: HashMap<String, Value>,
    pub grouping_labels: KeyByLabelNames,
    pub aggregated_labels: KeyByLabelNames,
    pub rollup_labels: KeyByLabelNames,
    pub original_yaml: String,

    pub window_size: u64,    // Window size in seconds (e.g., 900s for 15m)
    pub slide_interval: u64, // Slide/hop interval in seconds (e.g., 30s)
    pub window_type: String, // "tumbling" or "sliding"

    pub spatial_filter: String,
    pub spatial_filter_normalized: String,
    pub metric: String, // PromQL mode: metric name; SQL mode: derived from table_name.value_column
    pub num_aggregates_to_retain: Option<u64>,
    pub read_count_threshold: Option<u64>,

    // SQL-specific fields (optional, used when query_language=sql)
    pub table_name: Option<String>,   // SQL mode: table name
    pub value_column: Option<String>, // SQL mode: which value column to aggregate
}

/// Aggregation IDs and types for both the key and value dimensions of a query.
/// For single-population queries, key and value share the same ID and type.
/// For multi-population queries (e.g. Topk), they differ.
#[derive(Debug, Clone)]
pub struct AggregationIdInfo {
    pub aggregation_id_for_key: u64,
    pub aggregation_id_for_value: u64,
    pub aggregation_type_for_key: String,
    pub aggregation_type_for_value: String,
}

// TODO: need to implement deserialization methods

impl AggregationConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        aggregation_id: u64,
        aggregation_type: String,
        aggregation_sub_type: String,
        parameters: HashMap<String, Value>,
        grouping_labels: KeyByLabelNames,
        aggregated_labels: KeyByLabelNames,
        rollup_labels: KeyByLabelNames,
        original_yaml: String,
        window_size: u64,
        slide_interval: u64,
        window_type: String,
        spatial_filter: String,
        metric: String,
        num_aggregates_to_retain: Option<u64>,
        read_count_threshold: Option<u64>,
        // SQL-specific fields
        table_name: Option<String>,
        value_column: Option<String>,
    ) -> Self {
        // Generate normalized spatial filter (placeholder implementation)
        let spatial_filter_normalized = normalize_spatial_filter(&spatial_filter);

        Self {
            aggregation_id,
            aggregation_type,
            aggregation_sub_type,
            parameters,
            grouping_labels,
            aggregated_labels,
            rollup_labels,
            original_yaml,
            window_size,
            slide_interval,
            window_type,
            spatial_filter,
            spatial_filter_normalized,
            metric,
            num_aggregates_to_retain,
            read_count_threshold,
            table_name,
            value_column,
        }
    }

    // pub fn with_sub_type(mut self, sub_type: String) -> Self {
    //     self.aggregation_sub_type = Some(sub_type);
    //     self
    // }

    // pub fn with_parameters(mut self, parameters: HashMap<String, String>) -> Self {
    //     self.parameters = parameters;
    //     self
    // }

    pub fn with_original_yaml(mut self, yaml: String) -> Self {
        self.original_yaml = yaml;
        self
    }

    pub fn deserialize_from_json(
        data: &Value,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let aggregation_id = data["aggregationId"]
            .as_u64()
            .ok_or("Missing aggregationId")?;

        let aggregation_type = data["aggregationType"]
            .as_str()
            .ok_or("Missing aggregationType")?
            .to_string();

        let aggregation_sub_type = data["aggregationSubType"]
            .as_str()
            .ok_or("Missing aggregationSubType")?
            .to_string();

        let parameters = data["parameters"]
            .as_object()
            .ok_or("Missing parameters")?
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Note: In Python, eval(data["originalYaml"]) is used, but this is unsafe
        // Using the string value directly instead
        let original_yaml = data["originalYaml"].as_str().unwrap_or("").to_string();

        // Deserialize KeyByLabelNames - assuming they have deserialize_from_json methods
        let grouping_labels = KeyByLabelNames::deserialize_from_json(&data["groupingLabels"])?;
        let aggregated_labels = KeyByLabelNames::deserialize_from_json(&data["aggregatedLabels"])?;
        let rollup_labels = KeyByLabelNames::deserialize_from_json(&data["rollupLabels"])?;

        let window_size = data["windowSize"].as_u64().ok_or("Missing windowSize")?;

        let window_type = data
            .get("windowType")
            .and_then(|v| v.as_str())
            .unwrap_or("tumbling")
            .to_string();

        let slide_interval = data
            .get("slideInterval")
            .and_then(|v| v.as_u64())
            .unwrap_or(window_size);

        let spatial_filter = data["spatialFilter"].as_str().unwrap_or("").to_string();

        let metric = data["metric"].as_str().ok_or("Missing metric")?.to_string();

        let num_aggregates_to_retain = data.get("numAggregatesToRetain").and_then(|v| v.as_u64());
        let read_count_threshold = data.get("readCountThreshold").and_then(|v| v.as_u64());

        // SQL-specific fields (optional)
        let table_name = data
            .get("tableName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let value_column = data
            .get("valueColumn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Ok(Self::new(
            aggregation_id,
            aggregation_type,
            aggregation_sub_type,
            parameters,
            grouping_labels,
            aggregated_labels,
            rollup_labels,
            original_yaml,
            window_size,
            slide_interval,
            window_type,
            spatial_filter,
            metric,
            num_aggregates_to_retain,
            read_count_threshold,
            table_name,
            value_column,
        ))
    }

    pub fn deserialize_from_bytes(
        bytes: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let data_str = std::str::from_utf8(bytes)?.trim();
        let data: Value = serde_json::from_str(data_str)?;
        Self::deserialize_from_json(&data)
    }

    pub fn from_yaml_data(
        aggregation_data: &serde_yaml::Value,
        num_aggregates_to_retain: Option<u64>,
        read_count_threshold: Option<u64>,
        query_language: QueryLanguage,
    ) -> Result<Self, anyhow::Error> {
        let aggregation_id = aggregation_data["aggregationId"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("Missing aggregationId"))?;

        let labels = &aggregation_data["labels"];
        let grouping_labels = KeyByLabelNames::new(
            labels["grouping"]
                .as_sequence()
                .ok_or_else(|| anyhow::anyhow!("Missing grouping labels"))?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect(),
        );
        let aggregated_labels = KeyByLabelNames::new(
            labels["aggregated"]
                .as_sequence()
                .ok_or_else(|| anyhow::anyhow!("Missing aggregated labels"))?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect(),
        );
        let rollup_labels = KeyByLabelNames::new(
            labels["rollup"]
                .as_sequence()
                .ok_or_else(|| anyhow::anyhow!("Missing rollup labels"))?
                .iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect(),
        );

        let aggregation_type = aggregation_data["aggregationType"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing aggregationType"))?
            .to_string();

        let aggregation_sub_type = aggregation_data["aggregationSubType"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing aggregationSubType"))?
            .to_string();

        // Convert serde_yaml::Value to serde_json::Value for parameters
        let parameters: HashMap<String, Value> = aggregation_data["parameters"]
            .as_mapping()
            .ok_or_else(|| anyhow::anyhow!("Missing parameters"))?
            .iter()
            .map(|(k, v)| {
                let key = k.as_str().unwrap_or("").to_string();
                let value = serde_json::to_value(v).unwrap_or(Value::Null);
                (key, value)
            })
            .collect();

        let window_size = aggregation_data["windowSize"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("Missing windowSize"))?;

        let window_type = aggregation_data
            .get("windowType")
            .and_then(|v| v.as_str())
            .unwrap_or("tumbling")
            .to_string();

        let slide_interval = aggregation_data
            .get("slideInterval")
            .and_then(|v| v.as_u64())
            .unwrap_or(window_size);

        let spatial_filter = aggregation_data["spatialFilter"]
            .as_str()
            .unwrap_or("")
            .to_string();

        // Handle PromQL (metric) vs SQL (table_name/value_column) based on query_language
        let (metric, table_name, value_column) = match query_language {
            QueryLanguage::promql => {
                let metric = aggregation_data["metric"]
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("Missing metric for PromQL query language"))?
                    .to_string();
                (metric, None, None)
            }
            QueryLanguage::sql => {
                let table_name = aggregation_data
                    .get("table_name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow::anyhow!("Missing table_name for SQL query language"))?
                    .to_string();
                let value_column = aggregation_data
                    .get("value_column")
                    .and_then(|v| v.as_str())
                    .unwrap_or("value")
                    .to_string();
                // Derive metric from table_name.value_column for internal use
                let metric = format!("{}.{}", table_name, value_column);
                (metric, Some(table_name), Some(value_column))
            }
            QueryLanguage::elastic_querydsl => {
                // Elastic doesn't use metric/table_name in aggregations
                (String::new(), None, None)
            }
            QueryLanguage::elastic_sql => {
                // Elastic doesn't use metric/table_name in aggregations
                (String::new(), None, None)
            }
        };

        Ok(Self::new(
            aggregation_id,
            aggregation_type,
            aggregation_sub_type,
            parameters,
            grouping_labels,
            aggregated_labels,
            rollup_labels,
            String::new(), // original_yaml - empty as in Python
            window_size,
            slide_interval,
            window_type,
            spatial_filter,
            metric,
            num_aggregates_to_retain,
            read_count_threshold,
            table_name,
            value_column,
        ))
    }
}

impl SerializableToSink for AggregationConfig {
    fn serialize_to_json(&self) -> Value {
        let mut json = serde_json::json!({
            "aggregationId": self.aggregation_id,
            "aggregationType": self.aggregation_type,
            "aggregationSubType": self.aggregation_sub_type,
            "parameters": self.parameters,
            "originalYaml": self.original_yaml,
            "windowSize": self.window_size,
            "slideInterval": self.slide_interval,
            "windowType": self.window_type,
            "spatialFilter": self.spatial_filter,
            "metric": self.metric,
        });

        // Only include numAggregatesToRetain if it's Some
        if let Some(num_aggregates) = self.num_aggregates_to_retain {
            json["numAggregatesToRetain"] = serde_json::json!(num_aggregates);
        }

        // Only include readCountThreshold if it's Some
        if let Some(threshold) = self.read_count_threshold {
            json["readCountThreshold"] = serde_json::json!(threshold);
        }

        // SQL-specific fields (only include if present)
        if let Some(ref table_name) = self.table_name {
            json["tableName"] = serde_json::json!(table_name);
        }
        if let Some(ref value_column) = self.value_column {
            json["valueColumn"] = serde_json::json!(value_column);
        }

        json
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.original_yaml.as_bytes().to_vec()
    }
}
