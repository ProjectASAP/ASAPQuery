use promql_utilities::KeyByLabelNames;
use serde_json::{json, Value};
use std::collections::HashMap;

use crate::engines::QueryResult;

// /// Prometheus-compatible response structure
// #[derive(Debug, serde::Serialize, serde::Deserialize)]
// pub struct PrometheusResponse {
//     pub status: String,
//     pub data: PrometheusData,
// }

// #[derive(Debug, serde::Serialize, serde::Deserialize)]
// pub struct PrometheusData {
//     #[serde(rename = "resultType")]
//     pub result_type: String,
//     pub result: Vec<PrometheusResult>,
// }

// #[derive(Debug, serde::Serialize, serde::Deserialize)]
// pub struct PrometheusResult {
//     pub metric: HashMap<String, String>,
//     pub value: (f64, String), // [timestamp, value]
// }

// /// Format results as Prometheus-compatible HTTP response
// pub fn format_results_as_http_response(
//     results: &[PrecomputedOutput],
//     timestamp: f64,
// ) -> Result<PrometheusResponse> {
//     let mut prometheus_results = Vec::new();

//     for result in results {
//         if let Some(ref key) = result.key {
//             let prometheus_result = PrometheusResult {
//                 metric: key.labels.clone(),
//                 value: (timestamp, "0.0".to_string()), // TODO: Extract actual value from accumulator
//             };
//             prometheus_results.push(prometheus_result);
//         }
//     }

//     let response = PrometheusResponse {
//         status: "success".to_string(),
//         data: PrometheusData {
//             result_type: "vector".to_string(),
//             result: prometheus_results,
//         },
//     };

//     Ok(response)
// }

// /// Format error response in Prometheus format
// pub fn format_error_response(error_msg: &str) -> PrometheusResponse {
//     tracing::error!("Error: {}", error_msg);
//     PrometheusResponse {
//         status: "error".to_string(),
//         data: PrometheusData {
//             result_type: "vector".to_string(),
//             result: vec![],
//         },
//     }
// }

// /// Parse query parameters from HTTP request
// pub fn parse_query_params(query_string: &str) -> HashMap<String, Vec<String>> {
//     let mut params = HashMap::new();

//     for pair in query_string.split('&') {
//         if let Some((key, value)) = pair.split_once('=') {
//             let decoded_key = urlencoding::decode(key).unwrap_or_default().into_owned();
//             let decoded_value = urlencoding::decode(value).unwrap_or_default().into_owned();

//             params
//                 .entry(decoded_key)
//                 .or_insert_with(Vec::new)
//                 .push(decoded_value);
//         }
//     }

//     params
// }

// /// Format results as Prometheus-compatible HTTP response
// pub fn format_results_as_http_response(
//     result_type: QueryResultType,
//     results: &HashMap<String, f64>, // Simplified - key as string, value as f64
//     grouping_labels: &KeyByLabelNames,
//     time: u64,
// ) -> Value {
//     match result_type {
//         QueryResultType::InstantVector => {
//             let mut result = Vec::new();
//             for (k, v) in results.iter() {
//                 // Parse the key string back to values - this is a simplification
//                 // In the Python version, k is a Key object with values attribute
//                 let key_values: Vec<&str> = k.split(',').collect();

//                 let metric: HashMap<String, String> = grouping_labels
//                     .keys
//                     .iter()
//                     .zip(key_values.iter())
//                     .map(|(label, value)| (label.clone(), value.to_string()))
//                     .collect();

//                 result.push(json!({
//                     "metric": metric,
//                     "value": [time as f64 / 1000.0, v.to_string()]
//                 }));
//             }

//             json!({
//                 "status": "success",
//                 "data": {
//                     "resultType": "vector",
//                     "result": result
//                 }
//             })
//         }
//     }
// }

/// Convert QueryResult to Prometheus-compatible format (for instant queries only)
///
/// Returns an error if passed a Matrix result - use `convert_range_result_to_prometheus` for that.
pub fn convert_query_result_to_prometheus(
    result: &QueryResult,
    query_output_labels: &KeyByLabelNames,
) -> Result<Value, &'static str> {
    match result {
        QueryResult::Vector(instant_vector) => {
            let mut prometheus_results = Vec::new();
            let timestamp = instant_vector.timestamp as f64 / 1000.0;

            for element in &instant_vector.values {
                // zip over query_output_labels.keys and element.labels.labels and collect into metric_map
                let mut metric_map = HashMap::new();
                for (key, label) in query_output_labels
                    .labels
                    .iter()
                    .zip(element.labels.labels.iter())
                {
                    metric_map.insert(key, label);
                }

                let prometheus_result = json!({
                    "metric": metric_map,
                    "value": [timestamp, element.value.to_string()]
                });
                prometheus_results.push(prometheus_result);
            }
            Ok(json!({
                "resultType": "vector",
                "result": prometheus_results
            }))
        }
        QueryResult::Matrix(_) => {
            Err("convert_query_result_to_prometheus called with Matrix result; use convert_range_result_to_prometheus instead")
        }
    }
}

/// Convert range query result to Prometheus matrix format (for range queries only)
///
/// Returns an error if passed a Vector result - use `convert_query_result_to_prometheus` for that.
pub fn convert_range_result_to_prometheus(
    result: &QueryResult,
    label_names: &KeyByLabelNames,
) -> Result<Value, &'static str> {
    match result {
        QueryResult::Matrix(matrix) => {
            let results: Vec<Value> = matrix
                .values
                .iter()
                .map(|element| {
                    // Build metric labels object
                    let mut metric = serde_json::Map::new();
                    for (i, label_name) in label_names.labels.iter().enumerate() {
                        if i < element.labels.labels.len() {
                            metric.insert(
                                label_name.clone(),
                                Value::String(element.labels.labels[i].clone()),
                            );
                        }
                    }

                    // Build values array: [[timestamp, "value"], ...]
                    let values: Vec<Value> = element
                        .samples
                        .iter()
                        .map(|sample| {
                            json!([
                                sample.timestamp as f64 / 1000.0, // Convert ms to seconds
                                sample.value.to_string()
                            ])
                        })
                        .collect();

                    json!({
                        "metric": metric,
                        "values": values
                    })
                })
                .collect();

            Ok(json!({
                "resultType": "matrix",
                "result": results
            }))
        }
        QueryResult::Vector(_) => {
            Err("convert_range_result_to_prometheus called with Vector result; use convert_query_result_to_prometheus instead")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::KeyByLabelValues;
    use crate::engines::query_result::{InstantVectorElement, RangeVectorElement};

    fn create_test_labels() -> KeyByLabelValues {
        KeyByLabelValues::new_with_labels(vec!["host1".to_string(), "job1".to_string()])
    }

    fn create_test_label_names() -> KeyByLabelNames {
        KeyByLabelNames::new(vec!["instance".to_string(), "job".to_string()])
    }

    // Tests for convert_query_result_to_prometheus

    #[test]
    fn test_convert_instant_vector_to_prometheus() {
        let labels = create_test_labels();
        let label_names = create_test_label_names();
        let element = InstantVectorElement::new(labels, 42.0);
        let result = QueryResult::vector(vec![element], 1000);

        let prometheus_data = convert_query_result_to_prometheus(&result, &label_names);

        assert!(prometheus_data.is_ok());
        let data = prometheus_data.unwrap();

        assert_eq!(data["resultType"], "vector");
        assert_eq!(data["result"].as_array().unwrap().len(), 1);

        let first_result = &data["result"][0];
        assert_eq!(first_result["metric"]["instance"], "host1");
        assert_eq!(first_result["metric"]["job"], "job1");
        // Timestamp is converted from ms to seconds: 1000ms -> 1.0s
        assert_eq!(first_result["value"][0], 1.0);
        assert_eq!(first_result["value"][1], "42");
    }

    #[test]
    fn test_convert_instant_vector_empty() {
        let label_names = create_test_label_names();
        let result = QueryResult::vector(vec![], 1000);

        let prometheus_data = convert_query_result_to_prometheus(&result, &label_names);

        assert!(prometheus_data.is_ok());
        let data = prometheus_data.unwrap();
        assert_eq!(data["resultType"], "vector");
        assert!(data["result"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_convert_instant_vector_rejects_matrix() {
        let label_names = create_test_label_names();
        let labels = create_test_labels();
        let element = RangeVectorElement::new(labels);
        let result = QueryResult::matrix(vec![element]);

        let prometheus_data = convert_query_result_to_prometheus(&result, &label_names);

        assert!(prometheus_data.is_err());
        assert!(prometheus_data.err().unwrap().contains("Matrix"));
    }

    // Tests for convert_range_result_to_prometheus

    #[test]
    fn test_convert_range_vector_to_prometheus() {
        let labels = create_test_labels();
        let label_names = create_test_label_names();

        let mut element = RangeVectorElement::new(labels);
        element.add_sample(1000, 10.0);
        element.add_sample(2000, 20.0);
        element.add_sample(3000, 30.0);

        let result = QueryResult::matrix(vec![element]);

        let prometheus_data = convert_range_result_to_prometheus(&result, &label_names);

        assert!(prometheus_data.is_ok());
        let data = prometheus_data.unwrap();

        assert_eq!(data["resultType"], "matrix");
        assert_eq!(data["result"].as_array().unwrap().len(), 1);

        let first_result = &data["result"][0];
        assert_eq!(first_result["metric"]["instance"], "host1");
        assert_eq!(first_result["metric"]["job"], "job1");

        let values = first_result["values"].as_array().unwrap();
        assert_eq!(values.len(), 3);

        // Check timestamps converted from ms to seconds
        assert_eq!(values[0][0], 1.0); // 1000ms -> 1.0s
        assert_eq!(values[0][1], "10");
        assert_eq!(values[1][0], 2.0); // 2000ms -> 2.0s
        assert_eq!(values[1][1], "20");
        assert_eq!(values[2][0], 3.0); // 3000ms -> 3.0s
        assert_eq!(values[2][1], "30");
    }

    #[test]
    fn test_convert_range_vector_empty_samples() {
        let labels = create_test_labels();
        let label_names = create_test_label_names();
        let element = RangeVectorElement::new(labels);
        let result = QueryResult::matrix(vec![element]);

        let prometheus_data = convert_range_result_to_prometheus(&result, &label_names);

        assert!(prometheus_data.is_ok());
        let data = prometheus_data.unwrap();

        assert_eq!(data["resultType"], "matrix");
        let first_result = &data["result"][0];
        assert!(first_result["values"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_convert_range_vector_multiple_series() {
        let labels1 = KeyByLabelValues::new_with_labels(vec!["host1".to_string()]);
        let labels2 = KeyByLabelValues::new_with_labels(vec!["host2".to_string()]);
        let label_names = KeyByLabelNames::new(vec!["instance".to_string()]);

        let mut element1 = RangeVectorElement::new(labels1);
        element1.add_sample(1000, 10.0);

        let mut element2 = RangeVectorElement::new(labels2);
        element2.add_sample(1000, 100.0);

        let result = QueryResult::matrix(vec![element1, element2]);

        let prometheus_data = convert_range_result_to_prometheus(&result, &label_names);

        assert!(prometheus_data.is_ok());
        let data = prometheus_data.unwrap();

        assert_eq!(data["result"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_convert_range_vector_rejects_vector() {
        let label_names = create_test_label_names();
        let labels = create_test_labels();
        let element = InstantVectorElement::new(labels, 42.0);
        let result = QueryResult::vector(vec![element], 1000);

        let prometheus_data = convert_range_result_to_prometheus(&result, &label_names);

        assert!(prometheus_data.is_err());
        assert!(prometheus_data.err().unwrap().contains("Vector"));
    }
}
