use crate::data_model::KeyByLabelValues;
use serde::{Deserialize, Serialize};

use promql_utilities::query_logics::enums::QueryResultType;

/// Represents the result of a PromQL query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryResult {
    Vector(InstantVector),
    Matrix(RangeVector),
}

impl QueryResult {
    pub fn result_type(&self) -> QueryResultType {
        match self {
            QueryResult::Vector(_) => QueryResultType::InstantVector,
            QueryResult::Matrix(_) => QueryResultType::RangeVector,
        }
    }

    pub fn vector(values: Vec<InstantVectorElement>, timestamp: u64) -> Self {
        QueryResult::Vector(InstantVector { values, timestamp })
    }

    pub fn matrix(values: Vec<RangeVectorElement>) -> Self {
        QueryResult::Matrix(RangeVector { values })
    }
}

/// Instant vector - a set of time series containing a single sample for each time series, all sharing the same timestamp
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstantVector {
    pub values: Vec<InstantVectorElement>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstantVectorElement {
    pub labels: KeyByLabelValues,
    pub value: f64,
}

impl InstantVectorElement {
    pub fn new(labels: KeyByLabelValues, value: f64) -> Self {
        Self { labels, value }
    }
}

/// Range vector - a set of time series containing multiple samples over a time range
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeVector {
    pub values: Vec<RangeVectorElement>,
}

/// Individual element in a range vector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeVectorElement {
    pub labels: KeyByLabelValues,
    pub samples: Vec<Sample>,
}

/// A single sample (timestamp, value) pair
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    pub timestamp: u64,
    pub value: f64,
}

impl Sample {
    pub fn new(timestamp: u64, value: f64) -> Self {
        Self { timestamp, value }
    }
}

impl RangeVectorElement {
    pub fn new(labels: KeyByLabelValues) -> Self {
        Self {
            labels,
            samples: Vec::new(),
        }
    }

    pub fn add_sample(&mut self, timestamp: u64, value: f64) {
        self.samples.push(Sample::new(timestamp, value));
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    fn create_test_labels() -> KeyByLabelValues {
        KeyByLabelValues::new_with_labels(vec![
            "localhost:9090".to_string(),
            "prometheus".to_string(),
        ])
    }

    #[test]
    fn test_instant_vector_creation() {
        let labels = create_test_labels();
        let element = InstantVectorElement::new(labels.clone(), 42.0);
        let vector = QueryResult::vector(vec![element], 1000);

        assert_eq!(vector.result_type(), QueryResultType::InstantVector);

        if let QueryResult::Vector(iv) = vector {
            assert_eq!(iv.values.len(), 1);
            assert_eq!(iv.values[0].value, 42.0);
            assert_eq!(iv.values[0].labels, labels);
        } else {
            panic!("Expected Vector result");
        }
    }

    #[test]
    fn test_range_vector_creation() {
        let labels = create_test_labels();
        let mut element = RangeVectorElement::new(labels.clone());
        element.add_sample(1000, 42.0);
        element.add_sample(2000, 43.0);

        let result = QueryResult::matrix(vec![element]);

        assert_eq!(result.result_type(), QueryResultType::RangeVector);

        if let QueryResult::Matrix(matrix) = result {
            assert_eq!(matrix.values.len(), 1);
            assert_eq!(matrix.values[0].samples.len(), 2);
            assert_eq!(matrix.values[0].samples[0].timestamp, 1000);
            assert_eq!(matrix.values[0].samples[0].value, 42.0);
            assert_eq!(matrix.values[0].samples[1].timestamp, 2000);
            assert_eq!(matrix.values[0].samples[1].value, 43.0);
        } else {
            panic!("Expected Matrix result");
        }
    }

    #[test]
    fn test_serialization() {
        let labels = create_test_labels();
        let element = InstantVectorElement::new(labels, 42.0);
        let vector = QueryResult::vector(vec![element], 1000);

        let json = serde_json::to_string(&vector).unwrap();
        let deserialized: QueryResult = serde_json::from_str(&json).unwrap();

        assert_eq!(vector.result_type(), deserialized.result_type());
    }

    #[test]
    fn test_range_vector_serialization() {
        let labels = create_test_labels();
        let mut element = RangeVectorElement::new(labels);
        element.add_sample(1000, 42.0);
        element.add_sample(2000, 43.0);

        let result = QueryResult::matrix(vec![element]);

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: QueryResult = serde_json::from_str(&json).unwrap();

        assert_eq!(result.result_type(), deserialized.result_type());
    }

    #[test]
    fn test_range_vector_empty_samples() {
        let labels = create_test_labels();
        let element = RangeVectorElement::new(labels.clone());

        assert!(element.samples.is_empty());
        assert_eq!(element.labels, labels);

        let result = QueryResult::matrix(vec![element]);
        if let QueryResult::Matrix(matrix) = result {
            assert_eq!(matrix.values.len(), 1);
            assert!(matrix.values[0].samples.is_empty());
        } else {
            panic!("Expected Matrix result");
        }
    }

    #[test]
    fn test_range_vector_multiple_elements() {
        let labels1 = KeyByLabelValues::new_with_labels(vec!["host1".to_string()]);
        let labels2 = KeyByLabelValues::new_with_labels(vec!["host2".to_string()]);

        let mut element1 = RangeVectorElement::new(labels1.clone());
        element1.add_sample(1000, 10.0);
        element1.add_sample(2000, 20.0);

        let mut element2 = RangeVectorElement::new(labels2.clone());
        element2.add_sample(1000, 100.0);
        element2.add_sample(2000, 200.0);

        let result = QueryResult::matrix(vec![element1, element2]);

        if let QueryResult::Matrix(matrix) = result {
            assert_eq!(matrix.values.len(), 2);

            // First element
            assert_eq!(matrix.values[0].labels, labels1);
            assert_eq!(matrix.values[0].samples.len(), 2);

            // Second element
            assert_eq!(matrix.values[1].labels, labels2);
            assert_eq!(matrix.values[1].samples.len(), 2);
            assert_eq!(matrix.values[1].samples[0].value, 100.0);
        } else {
            panic!("Expected Matrix result");
        }
    }

    #[test]
    fn test_sample_ordering_preserved() {
        let labels = create_test_labels();
        let mut element = RangeVectorElement::new(labels);

        // Add samples in specific order
        element.add_sample(3000, 30.0);
        element.add_sample(1000, 10.0);
        element.add_sample(2000, 20.0);

        // Order should be preserved (not sorted)
        assert_eq!(element.samples[0].timestamp, 3000);
        assert_eq!(element.samples[1].timestamp, 1000);
        assert_eq!(element.samples[2].timestamp, 2000);
    }

    #[test]
    fn test_sample_new() {
        let sample = Sample::new(12345, 99.9);
        assert_eq!(sample.timestamp, 12345);
        assert_eq!(sample.value, 99.9);
    }
}
