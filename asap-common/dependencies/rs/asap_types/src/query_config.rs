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

impl QueryTimeAggregation {
    pub fn validate(&self) -> Result<(), String> {
        match self.grouping.mode {
            QueryTimeGroupingMode::All if !self.grouping.labels.is_empty() => {
                return Err("all grouping cannot name labels".to_string());
            }
            QueryTimeGroupingMode::By | QueryTimeGroupingMode::Without
                if self.grouping.labels.is_empty() =>
            {
                return Err("by and without grouping must name at least one label".to_string());
            }
            _ => {}
        }

        if self.grouping.labels.iter().any(|label| label.is_empty()) {
            return Err("grouping labels cannot be empty".to_string());
        }
        let unique_label_count = self
            .grouping
            .labels
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();
        if unique_label_count != self.grouping.labels.len() {
            return Err("grouping labels must be unique".to_string());
        }

        match (&self.operator, &self.parameter) {
            (
                QueryTimeAggregationOperator::Topk,
                Some(QueryTimeAggregationParameter::Integer(_)),
            ) => {}
            (QueryTimeAggregationOperator::Topk, _) => {
                return Err("topk requires an integer parameter".to_string());
            }
            (
                QueryTimeAggregationOperator::Quantile,
                Some(QueryTimeAggregationParameter::Float(phi)),
            ) if phi.is_finite() && (0.0..=1.0).contains(phi) => {}
            (QueryTimeAggregationOperator::Quantile, _) => {
                return Err("quantile requires a finite parameter between 0 and 1".to_string());
            }
            (_, None) => {}
            _ => return Err("this aggregation does not accept a parameter".to_string()),
        }

        Ok(())
    }
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

    pub fn validate_execution_plan(&self) -> Result<(), String> {
        if self.planned_subquery.trim().is_empty() {
            return Err("planned_subquery cannot be empty".to_string());
        }
        for aggregation in &self.query_time_aggregations {
            aggregation.validate()?;
        }
        Ok(())
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
