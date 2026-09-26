use serde::{Deserialize, Serialize};

use crate::aggregation_reference::AggregationReference;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    pub query: String,
    pub planned_subquery: String,
    pub query_time_aggregations: Vec<QueryTimeAggregation>,
    pub aggregations: Vec<AggregationReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryTimeAggregationOperator {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    Quantile,
    Topk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryTimeGroupingMode {
    All,
    By,
    Without,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTimeGrouping {
    pub mode: QueryTimeGroupingMode,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryTimeAggregationParameter {
    Integer(u64),
    Float(f64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTimeAggregation {
    pub operator: QueryTimeAggregationOperator,
    pub grouping: QueryTimeGrouping,
    pub parameter: Option<QueryTimeAggregationParameter>,
}

impl QueryConfig {
    pub fn new(query: String) -> Self {
        Self::with_plan(query.clone(), query, Vec::new())
    }

    pub fn with_plan(
        query: String,
        planned_subquery: String,
        query_time_aggregations: Vec<QueryTimeAggregation>,
    ) -> Self {
        Self {
            query,
            planned_subquery,
            query_time_aggregations,
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
