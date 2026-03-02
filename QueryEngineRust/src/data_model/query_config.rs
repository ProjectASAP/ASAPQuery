use serde::{Deserialize, Serialize};

use crate::data_model::AggregationReference;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    pub query: String,
    pub aggregations: Vec<AggregationReference>,
}

impl QueryConfig {
    pub fn new(query: String) -> Self {
        Self {
            query,
            aggregations: Vec::new(),
        }
    }

    pub fn add_aggregation(mut self, aggregation: AggregationReference) -> Self {
        self.aggregations.push(aggregation);
        self
    }

    pub fn with_aggregations(mut self, aggregations: Vec<AggregationReference>) -> Self {
        self.aggregations = aggregations;
        self
    }
}
