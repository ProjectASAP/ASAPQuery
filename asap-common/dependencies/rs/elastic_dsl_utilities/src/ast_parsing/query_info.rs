use serde::{Deserialize, Serialize};

pub type FieldName = String;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ElasticDSLQueryInfo {
    // A distilled representation of an ElasticSearch DSL query, capturing the essential logic and structure.
    pub target_field: FieldName,    // List of metrics being queried
    pub predicates: Vec<Predicate>, // Predicates applied to the query (e.g. filters in bool.filter)
    pub group_by_buckets: Option<GroupBySpec>, // Grouping specification if the query includes a group by clause
    pub aggregation: AggregationType, // The statistic being computed (e.g. avg, sum, percentiles)
}

impl ElasticDSLQueryInfo {
    // Additional methods for processing or analyzing the query can be added here

    pub fn new(
        target_field: FieldName,
        predicates: Vec<Predicate>,
        group_by_buckets: Option<GroupBySpec>,
        aggregation: AggregationType,
    ) -> Self {
        Self {
            target_field,
            predicates,
            group_by_buckets,
            aggregation,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregationType {
    Avg,
    Sum,
    Min,
    Max,
    Percentiles(Vec<f64>), // List of percentiles being computed
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Predicate {
    Term {
        field: FieldName,
        value: TermValue,
    },
    Range {
        field: FieldName,
        gte: Option<TermValue>,
        lte: Option<TermValue>,
    },
    // Other predicate types can be added here (e.g. exists, wildcard, etc.)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TermValue {
    String(String),
    Float(f64),
    Int(i64),
    UnsignedInt(u64),
    Boolean(bool),
    // Other term value types can be added here (e.g. boolean, date, etc.)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GroupBySpec {
    Fields(Vec<FieldName>),
    Filters(Vec<Predicate>), // Grouping by filters (e.g. group by whether a field matches a certain value)
}
