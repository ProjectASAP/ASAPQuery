//! Configuration builders for query equivalence tests
//!
//! Provides utilities to easily construct InferenceConfig, StreamingConfig,
//! and SQLSchema objects for testing.

use crate::data_model::{
    AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
    PromQLSchema, QueryConfig, SchemaConfig, StreamingConfig, WindowType,
};
use promql_utilities::data_model::KeyByLabelNames;
use sql_utilities::sqlhelper::{SQLSchema, Table};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Builder for creating test configurations
pub struct TestConfigBuilder {
    metric: String,
    time_col: String,
    value_col: String,
    grouping_labels: Vec<String>,
    rollup_labels: Vec<String>,
    scrape_interval: u64,
    query_configs: Vec<QueryConfig>,
    streaming_configs: HashMap<u64, AggregationConfig>,
}

impl TestConfigBuilder {
    /// Create a new builder for a given metric
    pub fn new(metric: &str) -> Self {
        Self {
            metric: metric.to_string(),
            time_col: "time".to_string(),
            value_col: "value".to_string(),
            grouping_labels: Vec::new(),
            rollup_labels: Vec::new(),
            scrape_interval: 1,
            query_configs: Vec::new(),
            streaming_configs: HashMap::new(),
        }
    }

    pub fn with_grouping_labels(mut self, labels: Vec<&str>) -> Self {
        self.grouping_labels = labels.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn with_rollup_labels(mut self, labels: Vec<&str>) -> Self {
        self.rollup_labels = labels.iter().map(|s| s.to_string()).collect();
        self
    }

    // /// Set the labels for this metric
    // pub fn with_labels(mut self, labels: Vec<&str>) -> Self {
    //     self.labels = labels.iter().map(|s| s.to_string()).collect();
    //     self
    // }

    /// Set custom column names (default: "time" and "value")
    pub fn with_columns(mut self, time_col: &str, value_col: &str) -> Self {
        self.time_col = time_col.to_string();
        self.value_col = value_col.to_string();
        self
    }

    /// Set the scrape interval in seconds (default: 1)
    pub fn with_scrape_interval(mut self, interval: u64) -> Self {
        self.scrape_interval = interval;
        self
    }

    /// Add a temporal query configuration
    pub fn add_temporal_query(
        mut self,
        promql: &str,
        sql: &str,
        agg_id: u64,
        window_seconds: u64,
        window_type: WindowType,
    ) -> Self {
        // Add PromQL query config
        let promql_config = QueryConfig::new(promql.to_string())
            .add_aggregation(AggregationReference::new(agg_id, None));
        self.query_configs.push(promql_config);

        // Add SQL query config
        let sql_config = QueryConfig::new(sql.to_string())
            .add_aggregation(AggregationReference::new(agg_id, None));
        self.query_configs.push(sql_config);

        // Create streaming config for this aggregation
        let agg_config = AggregationConfig {
            aggregation_id: agg_id,
            aggregation_type: AggregationType::Sum,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::new(self.grouping_labels.clone()),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::new(self.rollup_labels.clone()),
            original_yaml: String::new(),
            window_size: window_seconds,
            slide_interval: window_seconds,
            window_type,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: self.metric.clone(),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        };
        self.streaming_configs.insert(agg_id, agg_config);

        self
    }

    /// Add a spatial query configuration
    pub fn add_spatial_query(mut self, promql: &str, sql: &str, agg_id: u64) -> Self {
        // Add PromQL query config
        let promql_config = QueryConfig::new(promql.to_string())
            .add_aggregation(AggregationReference::new(agg_id, None));
        self.query_configs.push(promql_config);

        // Add SQL query config
        let sql_config = QueryConfig::new(sql.to_string())
            .add_aggregation(AggregationReference::new(agg_id, None));
        self.query_configs.push(sql_config);

        let agg_config = AggregationConfig {
            aggregation_id: agg_id,
            aggregation_type: AggregationType::Sum,
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::new(self.grouping_labels.clone()),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::new(self.rollup_labels.clone()),
            original_yaml: String::new(),
            window_size: self.scrape_interval,
            slide_interval: self.scrape_interval,
            window_type: WindowType::Tumbling,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: self.metric.clone(),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        };
        self.streaming_configs.insert(agg_id, agg_config);

        self
    }

    /// Add a spatial-of-temporal query configuration
    pub fn add_spatial_of_temporal_query(
        mut self,
        promql: &str,
        sql: &str,
        agg_id: u64,
        window_seconds: u64,
    ) -> Self {
        // Add PromQL query config
        let promql_config = QueryConfig::new(promql.to_string())
            .add_aggregation(AggregationReference::new(agg_id, None));
        self.query_configs.push(promql_config);

        // Add SQL query config
        let sql_config = QueryConfig::new(sql.to_string())
            .add_aggregation(AggregationReference::new(agg_id, None));
        self.query_configs.push(sql_config);

        let agg_config = AggregationConfig {
            aggregation_id: agg_id,
            aggregation_type: AggregationType::Sum, // For collapsable sum queries
            aggregation_sub_type: String::new(),
            parameters: HashMap::new(),
            grouping_labels: KeyByLabelNames::new(self.grouping_labels.clone()),
            aggregated_labels: KeyByLabelNames::empty(),
            rollup_labels: KeyByLabelNames::new(self.rollup_labels.clone()),
            original_yaml: String::new(),
            window_size: window_seconds,
            slide_interval: window_seconds,
            window_type: WindowType::Tumbling,
            spatial_filter: String::new(),
            spatial_filter_normalized: String::new(),
            metric: self.metric.clone(),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        };
        self.streaming_configs.insert(agg_id, agg_config);

        self
    }

    /// Build the InferenceConfig (with PromQL schema) and StreamingConfig
    pub fn build(self) -> (InferenceConfig, Arc<StreamingConfig>) {
        // Create PromQLSchema
        let promql_schema = PromQLSchema::new().add_metric(
            self.metric.clone(),
            KeyByLabelNames::new([&self.grouping_labels[..], &self.rollup_labels[..]].concat()),
        );

        // Create InferenceConfig
        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(promql_schema),
            query_configs: self.query_configs,
            cleanup_policy: CleanupPolicy::NoCleanup,
        };

        // Create StreamingConfig
        let streaming_config = StreamingConfig {
            aggregation_configs: self.streaming_configs,
        };

        (inference_config, Arc::new(streaming_config))
    }

    /// Build separate InferenceConfigs for PromQL and SQL, plus StreamingConfig
    ///
    /// Returns (promql_config, sql_config, streaming_config)
    pub fn build_both(self) -> (InferenceConfig, InferenceConfig, Arc<StreamingConfig>) {
        let all_labels = [&self.grouping_labels[..], &self.rollup_labels[..]].concat();

        // Create PromQL InferenceConfig
        let promql_schema = PromQLSchema::new().add_metric(
            self.metric.clone(),
            KeyByLabelNames::new(all_labels.clone()),
        );
        let promql_inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(promql_schema),
            query_configs: self.query_configs.clone(),
            cleanup_policy: CleanupPolicy::NoCleanup,
        };

        // Create SQL InferenceConfig
        let metadata_columns: HashSet<String> = all_labels.into_iter().collect();
        let value_columns: HashSet<String> = [self.value_col.clone()].into_iter().collect();
        let table = Table::new(
            self.metric.clone(),
            self.time_col.clone(),
            value_columns,
            metadata_columns,
        );
        let sql_schema = SQLSchema::new(vec![table]);
        let sql_inference_config = InferenceConfig {
            schema: SchemaConfig::SQL(sql_schema),
            query_configs: self.query_configs,
            cleanup_policy: CleanupPolicy::NoCleanup,
        };

        // Create StreamingConfig
        let streaming_config = StreamingConfig {
            aggregation_configs: self.streaming_configs,
        };

        (
            promql_inference_config,
            sql_inference_config,
            Arc::new(streaming_config),
        )
    }
}

/// Helper to create a SQLSchema for SQL parsing
///
/// Creates a schema with a single table matching the metric configuration
pub fn create_test_schema(
    metric: &str,
    time_col: &str,
    value_col: &str,
    labels: Vec<&str>,
) -> SQLSchema {
    let metadata_columns: HashSet<String> = labels.iter().map(|s| s.to_string()).collect();
    let value_columns: HashSet<String> = [value_col.to_string()].into_iter().collect();
    let table = Table::new(
        metric.to_string(),
        time_col.to_string(),
        value_columns,
        metadata_columns,
    );
    SQLSchema::new(vec![table])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_creates_valid_configs() {
        let (inference_config, streaming_config) = TestConfigBuilder::new("cpu_usage")
            .with_grouping_labels(vec!["L1", "L2", "L3", "L4"])
            .with_scrape_interval(1)
            .add_temporal_query(
                "sum_over_time(cpu_usage[10s])",
                "SELECT SUM(value) FROM cpu_usage WHERE time BETWEEN NOW() AND DATEADD(s, -10, NOW()) GROUP BY L1, L2, L3, L4",
                1,
                10,
                WindowType::Tumbling,
            )
            .build();

        // Verify schema has PromQL type
        match &inference_config.schema {
            SchemaConfig::PromQL(promql_schema) => {
                assert!(promql_schema.get_labels("cpu_usage").is_some());
            }
            SchemaConfig::SQL(_) => panic!("Expected PromQL schema"),
            SchemaConfig::ElasticQueryDSL => panic!("Expected PromQL schema"),
            SchemaConfig::ElasticSQL(_) => panic!("Expected PromQL schema"),
        }

        // Verify query configs (2 queries: PromQL + SQL)
        assert_eq!(inference_config.query_configs.len(), 2);

        // Verify streaming config
        assert!(streaming_config.get_aggregation_config(1).is_some());
        let agg_config = streaming_config.get_aggregation_config(1).unwrap();
        assert_eq!(agg_config.window_size, 10);
        assert_eq!(agg_config.window_type, WindowType::Tumbling);
    }

    #[test]
    fn test_schema_creation() {
        let schema = create_test_schema("cpu_usage", "time", "value", vec!["L1", "L2", "L3"]);

        // Verify schema has the table and correct columns
        assert_eq!(
            schema.get_time_column("cpu_usage"),
            Some(&"time".to_string())
        );

        // Verify value columns
        let value_cols = schema.get_value_columns("cpu_usage").unwrap();
        assert!(value_cols.contains("value"));

        // Verify metadata columns
        let metadata_cols = schema.get_metadata_columns("cpu_usage").unwrap();
        assert_eq!(metadata_cols.len(), 3);
        assert!(metadata_cols.contains("L1"));
        assert!(metadata_cols.contains("L2"));
        assert!(metadata_cols.contains("L3"));
    }
}
