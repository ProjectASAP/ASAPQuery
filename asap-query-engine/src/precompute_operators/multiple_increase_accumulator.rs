use crate::data_model::{
    AggregateCore, AggregationType, KeyByLabelValues, MergeableAccumulator,
    MultipleSubpopulationAggregate, SerializableToSink, SingleSubpopulationAggregate,
};
use crate::precompute_operators::IncreaseAccumulator;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::data_model::Measurement;
use promql_utilities::query_logics::enums::Statistic;

/// Accumulator that maintains separate increase accumulators for multiple keys
/// Allows tracking rate/increase for different label combinations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipleIncreaseAccumulator {
    pub increases: HashMap<KeyByLabelValues, IncreaseAccumulator>,
}

#[derive(Serialize, Deserialize)]
struct MeasurementData {
    starting_measurement: f64,
    starting_timestamp: i64,
    last_seen_measurement: f64,
    last_seen_timestamp: i64,
}

impl MultipleIncreaseAccumulator {
    pub fn new() -> Self {
        Self {
            increases: HashMap::new(),
        }
    }

    pub fn new_with_increases(increases: HashMap<KeyByLabelValues, IncreaseAccumulator>) -> Self {
        Self { increases }
    }

    pub fn update(&mut self, key: KeyByLabelValues, accumulator: IncreaseAccumulator) {
        self.increases.insert(key, accumulator);
    }

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let mut accumulator = Self::new();

        if let Some(entries) = data["entries"].as_array() {
            for entry in entries {
                let key = KeyByLabelValues::deserialize_from_json(&entry["key"])?;
                let increase_data =
                    IncreaseAccumulator::deserialize_from_json(&entry["increase_data"])?;
                accumulator.increases.insert(key, increase_data);
            }
        }

        Ok(accumulator)
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut accumulator = Self::new();
        let mut offset = 0;

        // Read number of entries
        if buffer.len() < 4 {
            return Err("Buffer too short for entry count".into());
        }
        let num_entries = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        offset += 4;

        for _ in 0..num_entries {
            // Read key length and key
            if offset + 4 > buffer.len() {
                return Err("Buffer too short for key length".into());
            }
            let key_length = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + key_length > buffer.len() {
                return Err("Buffer too short for key data".into());
            }
            let key =
                KeyByLabelValues::deserialize_from_bytes(&buffer[offset..offset + key_length])?;
            offset += key_length;

            // Read IncreaseAccumulator data
            if offset >= buffer.len() {
                return Err("Buffer too short for increase accumulator data".into());
            }
            let increase_data = IncreaseAccumulator::deserialize_from_bytes(&buffer[offset..])?;

            // Calculate consumed bytes for IncreaseAccumulator
            // Structure: starting_measurement_len(4) + starting_measurement + starting_timestamp(8) +
            //           last_seen_measurement_len(4) + last_seen_measurement + last_seen_timestamp(8)
            let starting_measurement_len = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            let last_seen_measurement_len = u32::from_le_bytes([
                buffer[offset + 4 + starting_measurement_len + 8],
                buffer[offset + 4 + starting_measurement_len + 8 + 1],
                buffer[offset + 4 + starting_measurement_len + 8 + 2],
                buffer[offset + 4 + starting_measurement_len + 8 + 3],
            ]) as usize;
            let consumed_bytes =
                4 + starting_measurement_len + 8 + 4 + last_seen_measurement_len + 8;
            offset += consumed_bytes;

            accumulator.increases.insert(key, increase_data);
        }

        Ok(accumulator)
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let precompute: HashMap<String, MeasurementData> =
            rmp_serde::from_slice(buffer).map_err(|e| {
                format!("Failed to deserialize MultipleIncreaseAccumulator from MessagePack: {e}")
            })?;

        let mut accumulator = Self::new();
        for (key_str, values) in precompute {
            // Parse semicolon-separated key values
            let key_values: Vec<String> = key_str.split(';').map(|s| s.to_string()).collect();
            // let mut labels = std::collections::BTreeMap::new();
            // for (i, value) in key_values.into_iter().enumerate() {
            //     labels.insert(format!("label_{i}"), value);
            // }
            let key_obj = KeyByLabelValues::new_with_labels(key_values);

            let starting_measurement = Measurement::new(values.starting_measurement);
            let starting_timestamp = values.starting_timestamp;
            let last_seen_measurement = Measurement::new(values.last_seen_measurement);
            let last_seen_timestamp = values.last_seen_timestamp;

            let increase_accumulator = IncreaseAccumulator::new(
                starting_measurement,
                starting_timestamp,
                last_seen_measurement,
                last_seen_timestamp,
            );

            accumulator.increases.insert(key_obj, increase_accumulator);
        }

        Ok(accumulator)
    }

    /// Serialize to Arroyo-compatible format (MessagePack HashMap<String, MeasurementData>)
    /// Matches the Arroyo multipleincrease_ UDF format
    pub fn serialize_to_bytes_arroyo(&self) -> Vec<u8> {
        use serde::Serialize;
        let mut per_key_storage: HashMap<String, MeasurementData> = HashMap::new();

        for (key, increase_acc) in &self.increases {
            // Keys are semicolon-separated label values
            let key_str = key.labels.join(";");
            per_key_storage.insert(
                key_str,
                MeasurementData {
                    starting_measurement: increase_acc.starting_measurement.value,
                    starting_timestamp: increase_acc.starting_timestamp,
                    last_seen_measurement: increase_acc.last_seen_measurement.value,
                    last_seen_timestamp: increase_acc.last_seen_timestamp,
                },
            );
        }

        let mut buf = Vec::new();
        per_key_storage
            .serialize(&mut rmp_serde::Serializer::new(&mut buf))
            .expect("Failed to serialize MultipleIncreaseAccumulator to MessagePack");
        buf
    }
}

impl Default for MultipleIncreaseAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SerializableToSink for MultipleIncreaseAccumulator {
    fn serialize_to_json(&self) -> Value {
        let entries: Vec<Value> = self
            .increases
            .iter()
            .map(|(key, data)| {
                serde_json::json!({
                    "key": key.serialize_to_json(),
                    "increase_data": data.serialize_to_json()
                })
            })
            .collect();

        serde_json::json!({
            "entries": entries
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        // Write number of entries
        buffer.extend_from_slice(&(self.increases.len() as u32).to_le_bytes());

        // Write each key-value pair
        for (key, data) in &self.increases {
            let key_bytes = key.serialize_to_bytes();
            buffer.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(&key_bytes);

            let data_bytes = data.serialize_to_bytes();
            buffer.extend_from_slice(&data_bytes);
        }

        buffer
    }
}

impl AggregateCore for MultipleIncreaseAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "MultipleIncreaseAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        // Check if other is also a MultipleIncreaseAccumulator
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge MultipleIncreaseAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        // Downcast to MultipleIncreaseAccumulator
        let other_multiple_increase = other
            .as_any()
            .downcast_ref::<MultipleIncreaseAccumulator>()
            .ok_or("Failed to downcast to MultipleIncreaseAccumulator")?;

        // Clone self once, then merge other's data in-place
        let mut merged = self.clone();
        for (key, data) in &other_multiple_increase.increases {
            if let Some(existing_data) = merged.increases.get_mut(key) {
                // Merge in-place: take earliest start, latest end
                if data.starting_timestamp < existing_data.starting_timestamp {
                    existing_data.starting_measurement = data.starting_measurement.clone();
                    existing_data.starting_timestamp = data.starting_timestamp;
                }
                if data.last_seen_timestamp > existing_data.last_seen_timestamp {
                    existing_data.last_seen_measurement = data.last_seen_measurement.clone();
                    existing_data.last_seen_timestamp = data.last_seen_timestamp;
                }
            } else {
                merged.increases.insert(key.clone(), data.clone());
            }
        }

        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::MultipleIncrease
    }

    fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
        Some(self.increases.keys().cloned().collect())
    }

    fn query_statistic(
        &self,
        statistic: promql_utilities::query_logics::enums::Statistic,
        key: &Option<KeyByLabelValues>,
        query_kwargs: &std::collections::HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        use crate::data_model::MultipleSubpopulationAggregate;
        let key_val = key
            .as_ref()
            .ok_or("Key required for MultipleIncreaseAccumulator")?;
        self.query(statistic, key_val, Some(query_kwargs))
    }
}

impl MultipleSubpopulationAggregate for MultipleIncreaseAccumulator {
    fn query(
        &self,
        statistic: Statistic,
        key: &KeyByLabelValues,
        _query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        let data = self
            .increases
            .get(key)
            .ok_or_else(|| format!("Key {key} not found in MultipleIncreaseAccumulator"))?;

        data.query(statistic, None)
    }

    fn clone_boxed(&self) -> Box<dyn MultipleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

impl MergeableAccumulator<MultipleIncreaseAccumulator> for MultipleIncreaseAccumulator {
    fn merge_accumulators(
        accumulators: Vec<MultipleIncreaseAccumulator>,
    ) -> Result<MultipleIncreaseAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let mut result = MultipleIncreaseAccumulator::new();

        for accumulator in accumulators {
            for (key, data) in accumulator.increases {
                if let Some(existing_data) = result.increases.get_mut(&key) {
                    // Merge in-place without cloning existing_data
                    // Take the earliest start time and latest end time
                    if data.starting_timestamp < existing_data.starting_timestamp {
                        existing_data.starting_measurement = data.starting_measurement;
                        existing_data.starting_timestamp = data.starting_timestamp;
                    }
                    if data.last_seen_timestamp > existing_data.last_seen_timestamp {
                        existing_data.last_seen_measurement = data.last_seen_measurement;
                        existing_data.last_seen_timestamp = data.last_seen_timestamp;
                    }
                } else {
                    result.increases.insert(key, data);
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::Measurement;

    fn create_test_increase_accumulator(start_val: f64, end_val: f64) -> IncreaseAccumulator {
        IncreaseAccumulator::new(
            Measurement::new(start_val),
            1000,
            Measurement::new(end_val),
            2000,
        )
    }

    fn create_test_increase_accumulator_with_time(
        start_val: f64,
        start_time: i64,
        end_val: f64,
        end_time: i64,
    ) -> IncreaseAccumulator {
        IncreaseAccumulator::new(
            Measurement::new(start_val),
            start_time,
            Measurement::new(end_val),
            end_time,
        )
    }

    #[test]
    fn test_multiple_increase_accumulator_creation() {
        let acc = MultipleIncreaseAccumulator::new();
        assert!(acc.increases.is_empty());
    }

    #[test]
    fn test_multiple_increase_accumulator_update() {
        let mut acc = MultipleIncreaseAccumulator::new();

        let key1 = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);

        let key2 = KeyByLabelValues::new_with_labels(vec!["api".to_string()]);

        let increase1 = create_test_increase_accumulator(10.0, 25.0);
        let increase2 = create_test_increase_accumulator(5.0, 15.0);

        acc.update(key1.clone(), increase1);
        acc.update(key2.clone(), increase2);

        assert_eq!(acc.increases.len(), 2);
        assert!(acc.increases.contains_key(&key1));
        assert!(acc.increases.contains_key(&key2));
    }

    #[test]
    fn test_multiple_increase_accumulator_query() {
        let mut acc = MultipleIncreaseAccumulator::new();

        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);

        let increase_acc = create_test_increase_accumulator(10.0, 25.0);
        acc.update(key.clone(), increase_acc);

        // Test increase query
        assert_eq!(acc.query(Statistic::Increase, &key, None).unwrap(), 15.0);

        // Test rate query (15.0 increase over 1 second = 15.0 per second)
        assert_eq!(acc.query(Statistic::Rate, &key, None).unwrap(), 15.0);

        // Test error cases
        assert!(acc.query(Statistic::Sum, &key, None).is_err());

        let unknown_key = KeyByLabelValues::new();
        assert!(acc.query(Statistic::Increase, &unknown_key, None).is_err());
    }

    #[test]
    fn test_multiple_increase_accumulator_merge() {
        let mut acc1 = MultipleIncreaseAccumulator::new();
        let mut acc2 = MultipleIncreaseAccumulator::new();

        let key1 = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);

        let key2 = KeyByLabelValues::new_with_labels(vec!["api".to_string()]);

        // Add different keys to each accumulator
        acc1.update(key1.clone(), create_test_increase_accumulator(10.0, 20.0));
        acc2.update(key2.clone(), create_test_increase_accumulator(5.0, 15.0));

        // Also add overlapping key with different time ranges (later timestamps)
        acc2.update(
            key1.clone(),
            create_test_increase_accumulator_with_time(15.0, 2000, 30.0, 3000),
        ); // Later time range

        let merged = MultipleIncreaseAccumulator::merge_accumulators(vec![acc1, acc2]).unwrap();

        assert_eq!(merged.increases.len(), 2);
        assert!(merged.increases.contains_key(&key1));
        assert!(merged.increases.contains_key(&key2));

        // The merged key1 should have the full range (earliest start to latest end)
        let merged_key1 = merged.increases.get(&key1).unwrap();
        assert_eq!(merged_key1.starting_measurement.value, 10.0); // Earlier start
        assert_eq!(merged_key1.last_seen_measurement.value, 30.0); // Later end
    }

    #[test]
    fn test_multiple_increase_accumulator_serialization() {
        let mut acc = MultipleIncreaseAccumulator::new();

        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);

        acc.update(key.clone(), create_test_increase_accumulator(10.0, 25.0));

        // Test JSON serialization
        let json_value = acc.serialize_to_json();
        let deserialized = MultipleIncreaseAccumulator::deserialize_from_json(&json_value).unwrap();

        assert_eq!(deserialized.increases.len(), 1);
        let deserialized_acc = deserialized.increases.get(&key).unwrap();
        assert_eq!(deserialized_acc.starting_measurement.value, 10.0);
        assert_eq!(deserialized_acc.last_seen_measurement.value, 25.0);

        // Test binary serialization
        let bytes = acc.serialize_to_bytes();
        let deserialized_bytes =
            MultipleIncreaseAccumulator::deserialize_from_bytes(&bytes).unwrap();

        assert_eq!(deserialized_bytes.increases.len(), 1);
        let deserialized_acc_bytes = deserialized_bytes.increases.get(&key).unwrap();
        assert_eq!(deserialized_acc_bytes.starting_measurement.value, 10.0);
        assert_eq!(deserialized_acc_bytes.last_seen_measurement.value, 25.0);
    }

    #[test]
    fn test_multiple_increase_accumulator_get_keys() {
        let mut acc = MultipleIncreaseAccumulator::new();

        let key1 = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);
        let key2 = KeyByLabelValues::new_with_labels(vec!["api".to_string()]);

        acc.update(key1.clone(), create_test_increase_accumulator(10.0, 20.0));
        acc.update(key2.clone(), create_test_increase_accumulator(5.0, 15.0));

        let keys = acc.get_keys().unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&key1));
        assert!(keys.contains(&key2));
    }

    #[test]
    fn test_trait_object() {
        let mut acc = MultipleIncreaseAccumulator::new();
        let key = KeyByLabelValues::new();
        acc.update(key.clone(), create_test_increase_accumulator(10.0, 25.0));

        let trait_obj: Box<dyn MultipleSubpopulationAggregate> = Box::new(acc);
        assert_eq!(
            trait_obj.query(Statistic::Increase, &key, None).unwrap(),
            15.0
        );

        let keys = trait_obj.get_keys().unwrap();
        assert_eq!(keys.len(), 1);
    }

    // #[test]
    // fn test_multiple_increase_accumulator_arroyo_deserialization() {
    //     // Create test data in Arroyo MessagePack format
    //     // Format: {key: [starting_value, starting_timestamp, last_seen_value, last_seen_timestamp]}
    //     let mut test_data = std::collections::HashMap::new();
    //     test_data.insert("web;service".to_string(), vec![10.0, 1000.0, 25.0, 2000.0]);
    //     test_data.insert("api;service".to_string(), vec![5.0, 1500.0, 15.0, 2500.0]);

    //     // Serialize to MessagePack
    //     let arroyo_buffer = rmp_serde::to_vec(&test_data).unwrap();

    //     // Test Arroyo deserialization
    //     let deserialized_acc =
    //         MultipleIncreaseAccumulator::deserialize_from_bytes_arroyo(&arroyo_buffer).unwrap();

    //     // Verify the deserialized accumulator has the correct data
    //     assert_eq!(deserialized_acc.increases.len(), 2);

    //     // Check first key (web;service)
    //     let keys: Vec<_> = deserialized_acc.increases.keys().collect();
    //     let key1 = keys
    //         .iter()
    //         .find(|k| k.labels.get("label_0").is_some_and(|v| v == "web"))
    //         .unwrap();

    //     let increase1 = deserialized_acc.increases.get(key1).unwrap();
    //     assert_eq!(increase1.starting_measurement.value, 10.0);
    //     assert_eq!(increase1.starting_timestamp, 1000);
    //     assert_eq!(increase1.last_seen_measurement.value, 25.0);
    //     assert_eq!(increase1.last_seen_timestamp, 2000);

    //     // Check second key (api;service)
    //     let key2 = keys
    //         .iter()
    //         .find(|k| k.labels.get("label_0").is_some_and(|v| v == "api"))
    //         .unwrap();

    //     let increase2 = deserialized_acc.increases.get(key2).unwrap();
    //     assert_eq!(increase2.starting_measurement.value, 5.0);
    //     assert_eq!(increase2.starting_timestamp, 1500);
    //     assert_eq!(increase2.last_seen_measurement.value, 15.0);
    //     assert_eq!(increase2.last_seen_timestamp, 2500);

    //     // Test querying
    //     assert_eq!(
    //         deserialized_acc.query(Statistic::Increase, key1).unwrap(),
    //         15.0
    //     ); // 25.0 - 10.0
    //     assert_eq!(
    //         deserialized_acc.query(Statistic::Increase, key2).unwrap(),
    //         10.0
    //     ); // 15.0 - 5.0
    // }
}
