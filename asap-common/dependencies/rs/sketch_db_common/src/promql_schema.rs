use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use promql_utilities::data_model::KeyByLabelNames;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromQLSchema {
    pub config: HashMap<String, KeyByLabelNames>,
}

impl PromQLSchema {
    pub fn new() -> Self {
        Self {
            config: HashMap::new(),
        }
    }

    pub fn add_metric(mut self, metric: String, labels: KeyByLabelNames) -> Self {
        self.config.insert(metric, labels);
        self
    }

    pub fn get_labels(&self, metric: &str) -> Option<&KeyByLabelNames> {
        self.config.get(metric)
    }
}

impl Default for PromQLSchema {
    fn default() -> Self {
        Self::new()
    }
}
