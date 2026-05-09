pub mod ast_parsing;
pub mod datemath;
pub mod helpers;

pub use ast_parsing::*;
pub use datemath::*;
pub use helpers::*;

use std::collections::{HashMap, HashSet};


#[derive(Debug, Clone)]
pub struct ElasticIndexSchema {
    pub time_field: String,
    pub metric_columns: HashSet<String>,
    pub metadata_columns: HashSet<String>,
}

impl ElasticIndexSchema {
    pub fn new(
        time_field: String,
        metric_columns: HashSet<String>,
        metadata_columns: HashSet<String>,
    ) -> Self {
        Self {
            time_field,
            metric_columns,
            metadata_columns,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ElasticMappingSchema {
    pub config: HashMap<String, ElasticIndexSchema>,
}

impl ElasticMappingSchema {
    pub fn new(indexes: Vec<(String, ElasticIndexSchema)>) -> Self {
        let mut config = HashMap::new();
        for (index_name, index_schema) in indexes {
            config.insert(index_name, index_schema);
        }
        Self { config }
    }

    pub fn add_index(mut self, index: String, schema: ElasticIndexSchema) -> Self {
        self.config.insert(index, schema);
        self
    }

    pub fn get_time_field(&self, index: &str) -> Option<&String> {
        self.config.get(index).map(|schema| &schema.time_field)
    }

    pub fn get_metric_columns(&self, index: &str) -> Option<&HashSet<String>> {
        self.config.get(index).map(|schema| &schema.metric_columns)
    }

    pub fn get_metadata_columns(&self, index: &str) -> Option<&HashSet<String>> {
        self.config.get(index).map(|schema| &schema.metadata_columns)
    }

    pub fn is_valid_metric_column(&self, index: &str, metric_column: &str) -> bool {
        self.get_metric_columns(index)
            .map(|columns| columns.contains(metric_column))
            .unwrap_or(false)
    }

    pub fn are_valid_metadata_columns(&self, index: &str, columns: &HashSet<String>) -> bool {
        self.get_metadata_columns(index)
            .map(|schema_columns| columns.iter().all(|c| schema_columns.contains(c)))
            .unwrap_or(false)
    }
}