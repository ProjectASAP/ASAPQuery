use serde::{Deserialize, Serialize};
use tracing::error;

use crate::data_model::traits::SerializableToSink;
use crate::data_model::KeyByLabelValues;
use asap_types::traits::SerializationError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecomputedOutput {
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub key: Option<KeyByLabelValues>,
    pub aggregation_id: u64,
    // pub config: AggregationConfig,
    // Note: precompute will be handled separately as it's a trait object
}

impl PrecomputedOutput {
    pub fn new(
        start_timestamp: u64,
        end_timestamp: u64,
        key: Option<KeyByLabelValues>,
        aggregation_id: u64,
        // TODO: we should remove AggregationConfig from here. Configs should only be accessed from the StreamingConfig read in main.rs
        // config: AggregationConfig,
    ) -> Self {
        Self {
            start_timestamp,
            end_timestamp,
            key,
            aggregation_id,
            // config,
        }
    }

    pub fn get_freshness_debug_string(&self) -> String {
        // Match Python implementation more closely
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let freshness = current_time.saturating_sub(self.end_timestamp);
        format!(
            "end_timestamp: {}, current_time: {}, freshness: {}",
            self.end_timestamp, current_time, freshness
        )
    }

    // /// Serialize PrecomputedOutput with precompute data to match Python JSON format
    // pub fn serialize_to_json_with_precompute(
    //     &self,
    //     precompute: &dyn crate::data_model::AggregateCore,
    // ) -> serde_json::Value {
    //     serde_json::json!({
    //         // "config": self.config.serialize_to_json(),
    //         "start_timestamp": self.start_timestamp,
    //         "end_timestamp": self.end_timestamp,
    //         "key": self.key.as_ref().map(|k| k.serialize_to_json()),
    //         "precompute": precompute.serialize_to_json()
    //     })
    // }

    /// Deserialize from bytes using Python-compatible format
    pub fn deserialize_from_bytes_with_precompute(
        _data: &[u8],
    ) -> Result<(Self, Vec<u8>), Box<dyn std::error::Error>> {
        error!("Not implemented: deserialize_from_bytes_with_precompute");
        Err(("Not implemented: deserialize_from_bytes_with_precompute").into())
    }

    // /// Simple deserialization from bytes (compatibility method for Kafka consumer)
    // /// This doesn't include precompute data and is primarily for compatibility
    // pub fn deserialize_from_bytes(
    //     data: &[u8],
    // ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
    //     // Try to deserialize as JSON first (common case)
    //     if let Ok(json_str) = String::from_utf8(data.to_vec()) {
    //         if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(&json_str) {
    //             return Self::deserialize_from_json(&json_value);
    //         }
    //     }

    //     // If JSON fails, try binary format
    //     let (output, _precompute_bytes) = Self::deserialize_from_bytes_with_precompute(data)
    //         .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
    //             format!("Failed to deserialize from bytes: {e}").into()
    //         })?;
    //     Ok(output)
    // }

    // /// Legacy deserialization method for backward compatibility
    // pub fn deserialize_from_json(
    //     data: &serde_json::Value,
    // ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
    //     // Extract required fields
    //     let config_data = data.get("config").ok_or("Missing 'config' field in JSON")?;
    //     // Use custom deserialization for the config
    //     let config = AggregationConfig::deserialize_from_json(config_data).map_err(
    //         |e| -> Box<dyn std::error::Error + Send + Sync> {
    //             format!("Failed to deserialize config: {e}").into()
    //         },
    //     )?;

    //     let start_timestamp = data
    //         .get("start_timestamp")
    //         .and_then(|v| v.as_u64())
    //         .ok_or("Missing or invalid 'start_timestamp' field")?;

    //     let end_timestamp = data
    //         .get("end_timestamp")
    //         .and_then(|v| v.as_u64())
    //         .ok_or("Missing or invalid 'end_timestamp' field")?;

    //     let key = if let Some(key_data) = data.get("key") {
    //         if key_data.is_null() {
    //             None
    //         } else {
    //             // Use the custom deserialize_from_json method which expects the direct HashMap format
    //             Some(KeyByLabelValues::deserialize_from_json(key_data).map_err(
    //                 |e| -> Box<dyn std::error::Error + Send + Sync> {
    //                     format!("Failed to deserialize key: {e}").into()
    //                 },
    //             )?)
    //         }
    //     } else {
    //         None
    //     };

    //     // For now, we create a PrecomputedOutput without precompute data
    //     // In a full implementation, we would deserialize the precompute field as well
    //     Ok(Self {
    //         start_timestamp,
    //         end_timestamp,
    //         key,
    //         config,
    //     })
    // }

    // /// Deserialize from JSON and extract precompute data following Python implementation
    // /// This is the public method that should be used by Kafka consumer
    // pub fn deserialize_from_json_with_precompute(
    //     data: &serde_json::Value,
    // ) -> Result<
    //     (Self, Box<dyn crate::data_model::AggregateCore>),
    //     Box<dyn std::error::Error + Send + Sync>,
    // > {
    //     debug!("Deserializing PrecomputedOutput with precompute from JSON: {data}");
    //     // First get the metadata
    //     let precomputed_output = Self::deserialize_from_json(data)?;
    //     debug!(
    //         "Deserialized PrecomputedOutput metadata: {:?}",
    //         precomputed_output
    //     );

    //     // Then deserialize the precompute data based on aggregation type
    //     let precompute_data = data
    //         .get("precompute")
    //         .ok_or("Missing 'precompute' field in JSON")?;
    //     let precompute = Self::create_precompute_from_json(
    //         &precomputed_output.config.aggregation_type,
    //         precompute_data,
    //     )?;

    //     Ok((precomputed_output, precompute))
    // }

    // /// Deserialize from bytes and extract precompute data following Python implementation
    // /// This is the public method that should be used by Kafka consumer
    // pub fn deserialize_from_bytes_with_precompute_and_type(
    //     data: &[u8],
    //     aggregation_type: &str,
    // ) -> Result<
    //     (Self, Box<dyn crate::data_model::AggregateCore>),
    //     Box<dyn std::error::Error + Send + Sync>,
    // > {
    //     // First get the metadata and precompute bytes
    //     let (precomputed_output, precompute_bytes) = Self::deserialize_from_bytes_with_precompute(
    //         data,
    //     )
    //     .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
    //         format!("Failed to deserialize from bytes: {e}").into()
    //     })?;

    //     // Then create the accumulator from the precompute bytes
    //     let precompute =
    //         Self::create_precompute_from_bytes(aggregation_type, &precompute_bytes, "flink")?;

    //     Ok((precomputed_output, precompute))
    // }

    // /// Factory method to create precompute accumulator from JSON data
    // fn create_precompute_from_json(
    //     precompute_type: &str,
    //     data: &serde_json::Value,
    // ) -> Result<Box<dyn crate::data_model::AggregateCore>, Box<dyn std::error::Error + Send + Sync>>
    // {
    //     use crate::precompute_operators::*;

    //     match precompute_type {
    //         "Sum" | "sum" => {
    //             let accumulator = SumAccumulator::deserialize_from_json(data)
    //                 .map_err(|e| format!("Failed to deserialize SumAccumulator: {e}"))?;
    //             Ok(Box::new(accumulator))
    //         }
    //         "MinMax" => {
    //             let accumulator = MinMaxAccumulator::deserialize_from_json(data)
    //                 .map_err(|e| format!("Failed to deserialize MinMaxAccumulator: {e}"))?;
    //             Ok(Box::new(accumulator))
    //         }
    //         "Increase" => {
    //             let accumulator = IncreaseAccumulator::deserialize_from_json(data)
    //                 .map_err(|e| format!("Failed to deserialize IncreaseAccumulator: {e}"))?;
    //             Ok(Box::new(accumulator))
    //         }
    //         "MultipleSum" => {
    //             let accumulator = MultipleSumAccumulator::deserialize_from_json(data)
    //                 .map_err(|e| format!("Failed to deserialize MultipleSumAccumulator: {e}"))?;
    //             Ok(Box::new(accumulator))
    //         }
    //         "MultipleMinMax" => {
    //             // Extract sub_type from data
    //             let _sub_type = data
    //                 .get("sub_type")
    //                 .and_then(|v| v.as_str())
    //                 .unwrap_or("min")
    //                 .to_string();
    //             let accumulator = MultipleMinMaxAccumulator::deserialize_from_json(data)
    //                 .map_err(|e| format!("Failed to deserialize MultipleMinMaxAccumulator: {e}"))?;
    //             Ok(Box::new(accumulator))
    //         }
    //         "MultipleIncrease" => {
    //             let accumulator = MultipleIncreaseAccumulator::deserialize_from_json(data)
    //                 .map_err(|e| {
    //                     format!("Failed to deserialize MultipleIncreaseAccumulator: {e}")
    //                 })?;
    //             Ok(Box::new(accumulator))
    //         }
    //         "CountMinSketch" => {
    //             let accumulator = CountMinSketchAccumulator::deserialize_from_json(data)
    //                 .map_err(|e| format!("Failed to deserialize CountMinSketchAccumulator: {e}"))?;
    //             Ok(Box::new(accumulator))
    //         }
    //         "DatasketchesKLL" => {
    //             let accumulator =
    //                 DatasketchesKLLAccumulator::deserialize_from_json(data).map_err(|e| {
    //                     format!("Failed to deserialize DatasketchesKLLAccumulator: {e}")
    //                 })?;
    //             Ok(Box::new(accumulator))
    //         }
    //         "DeltaSetAggregator" => {
    //             let accumulator = DeltaSetAggregatorAccumulator::deserialize_from_json(data)
    //                 .map_err(|e| {
    //                     format!("Failed to deserialize DeltaSetAggregatorAccumulator: {e}")
    //                 })?;
    //             Ok(Box::new(accumulator))
    //         }
    //         _ => Err(format!("Unknown precompute type: {precompute_type}").into()),
    //     }
    // }
}

impl SerializableToSink for PrecomputedOutput {
    fn serialize_to_json(&self) -> Result<serde_json::Value, SerializationError> {
        // Default implementation without precompute data for backward compatibility
        let key_json = self
            .key
            .as_ref()
            .map(|k| k.serialize_to_json())
            .transpose()?;
        Ok(serde_json::json!({
            // "config": self.config.serialize_to_json(),
            "start_timestamp": self.start_timestamp,
            "end_timestamp": self.end_timestamp,
            "key": key_json
        }))
    }

    fn serialize_to_bytes(&self) -> Result<Vec<u8>, SerializationError> {
        // Default implementation without precompute data for backward compatibility
        serde_json::to_vec(self).map_err(|e| SerializationError::Bytes {
            type_name: "PrecomputedOutput",
            source: e.into(),
        })
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_aggregation_config_creation() {
//         let labels = KeyByLabelNames::from_names(vec!["instance".to_string(), "job".to_string()]);
//         let empty_labels = KeyByLabelNames::new(vec![]);
//         let config = AggregationConfig::new(
//             1,
//             "cpu_usage".to_string(),
//             labels,
//             empty_labels.clone(),
//             empty_labels,
//             "".to_string(),
//             "sum".to_string(),
//             10,
//         );

//         assert_eq!(config.aggregation_id, 1);
//         assert_eq!(config.metric, "cpu_usage");
//         assert_eq!(config.aggregation_type, "sum");
//         assert_eq!(config.window_size, 10);
//     }

//     #[test]
//     fn test_query_config_builder() {
//         let labels = KeyByLabelNames::from_names(vec!["instance".to_string()]);
//         let empty_labels = KeyByLabelNames::new(vec![]);
//         let aggregation = AggregationConfig::new(
//             1,
//             "cpu_usage".to_string(),
//             labels,
//             empty_labels.clone(),
//             empty_labels,
//             "".to_string(),
//             "sum".to_string(),
//             10,
//         );

//         let query_config = QueryConfig::new("sum_over_time(cpu_usage[5m])".to_string())
//             .add_aggregation(aggregation);

//         assert_eq!(query_config.query, "sum_over_time(cpu_usage[5m])");
//         assert_eq!(query_config.aggregations.len(), 1);
//     }

//     #[test]
//     fn test_precomputed_output_json_serialization_with_precompute() {
//         // Test Issue 9: PrecomputedOutput JSON serialization alignment with Python behavior
//         use crate::precompute_operators::SumAccumulator;
//         use std::collections::BTreeMap;

//         let labels = KeyByLabelNames::from_names(vec!["instance".to_string()]);
//         let empty_labels = KeyByLabelNames::new(vec![]);
//         let config = AggregationConfig::new(
//             1,
//             "cpu_usage".to_string(),
//             labels,
//             empty_labels.clone(),
//             empty_labels,
//             "".to_string(),
//             "sum".to_string(),
//             10,
//         );

//         let mut key_labels = BTreeMap::new();
//         key_labels.insert("instance".to_string(), "server1".to_string());
//         let key = Some(KeyByLabelValues::new_with_labels(key_labels));

//         let precomputed_output = PrecomputedOutput::new(
//             1000, // start_timestamp
//             2000, // end_timestamp
//             key.clone(),
//             config.clone(),
//         );

//         let accumulator = SumAccumulator::with_sum(42.5);

//         // Test JSON serialization with precompute data (matching Python format)
//         let json_with_precompute =
//             precomputed_output.serialize_to_json_with_precompute(&accumulator);

//         // Verify the JSON structure matches Python implementation
//         assert!(json_with_precompute["config"].is_object());
//         assert_eq!(json_with_precompute["start_timestamp"], 1000);
//         assert_eq!(json_with_precompute["end_timestamp"], 2000);
//         assert!(json_with_precompute["key"].is_object());
//         assert!(json_with_precompute["precompute"].is_object());

//         // Verify precompute data is included (this is the key difference from default serialization)
//         assert_eq!(json_with_precompute["precompute"]["sum"], 42.5);

//         // Test default JSON serialization without precompute data
//         let json_default = precomputed_output.serialize_to_json();

//         // Verify default serialization does NOT include precompute data
//         assert!(
//             json_default["precompute"].is_null()
//                 || !json_default.as_object().unwrap().contains_key("precompute")
//         );
//         assert_eq!(json_default["start_timestamp"], 1000);
//         assert_eq!(json_default["end_timestamp"], 2000);
//     }

//     #[test]
//     fn test_precomputed_output_byte_serialization_with_precompute() {
//         // Test Issue 9: PrecomputedOutput byte serialization alignment with Python behavior
//         use crate::precompute_operators::SumAccumulator;

//         let labels = KeyByLabelNames::from_names(vec!["instance".to_string()]);
//         let empty_labels = KeyByLabelNames::new(vec![]);
//         let config = AggregationConfig::new(
//             1,
//             "cpu_usage".to_string(),
//             labels,
//             empty_labels.clone(),
//             empty_labels,
//             "".to_string(),
//             "sum".to_string(),
//             10,
//         );

//         let precomputed_output = PrecomputedOutput::new(
//             1000, // start_timestamp
//             2000, // end_timestamp
//             None, // key
//             config,
//         );

//         let accumulator = SumAccumulator::with_sum(42.5);

//         // Test byte serialization with precompute data (matching Python format)
//         let bytes_with_precompute =
//             precomputed_output.serialize_to_bytes_with_precompute(&accumulator);

//         // Test round-trip: serialize then deserialize
//         let (deserialized_output, precompute_bytes) =
//             PrecomputedOutput::deserialize_from_bytes_with_precompute(&bytes_with_precompute)
//                 .unwrap();

//         // Verify round-trip works correctly
//         assert_eq!(deserialized_output.start_timestamp, 1000);
//         assert_eq!(deserialized_output.end_timestamp, 2000);
//         assert!(deserialized_output.key.is_none());
//         assert_eq!(deserialized_output.config.aggregation_id, 1);
//         assert_eq!(deserialized_output.config.metric, "cpu_usage");

//         // Verify precompute data can be deserialized back to SumAccumulator
//         let deserialized_accumulator =
//             SumAccumulator::deserialize_from_bytes(&precompute_bytes).unwrap();
//         assert_eq!(deserialized_accumulator.sum, 42.5);
//     }
// }
