use crate::data_model::{
    AggregateCore, AggregationType, KeyByLabelValues, MergeableAccumulator,
    MultipleSubpopulationAggregate, QueryBounds, SerializableToSink, SingleSubpopulationAggregate,
};
use crate::precompute_operators::IncreaseAccumulator;
use asap_types::traits::SerializationError;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

/// Accumulator that maintains separate increase accumulators for multiple keys
/// Allows tracking rate/increase for different label combinations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipleIncreaseAccumulator {
    pub increases: HashMap<KeyByLabelValues, IncreaseAccumulator>,
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

            // IncreaseAccumulator owns its binary framing and reports its length.
            let (increase_data, consumed_bytes) =
                IncreaseAccumulator::deserialize_from_bytes_with_consumed(&buffer[offset..])?;
            offset += consumed_bytes;

            accumulator.increases.insert(key, increase_data);
        }

        Ok(accumulator)
    }

    fn merge_increase(
        increases: &mut HashMap<KeyByLabelValues, IncreaseAccumulator>,
        key: KeyByLabelValues,
        data: IncreaseAccumulator,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let Some(existing_data) = increases.remove(&key) else {
            increases.insert(key, data);
            return Ok(());
        };

        match IncreaseAccumulator::merge_accumulators(vec![existing_data, data]) {
            Ok(merged_data) => {
                increases.insert(key, merged_data);
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
}

impl Default for MultipleIncreaseAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SerializableToSink for MultipleIncreaseAccumulator {
    fn serialize_to_json(&self) -> Result<Value, SerializationError> {
        let entries: Vec<Value> = self
            .increases
            .iter()
            .map(|(key, data)| {
                Ok(serde_json::json!({
                    "key": key.serialize_to_json()?,
                    "increase_data": data.serialize_to_json()?
                }))
            })
            .collect::<Result<_, SerializationError>>()?;

        Ok(serde_json::json!({
            "entries": entries
        }))
    }

    fn serialize_to_bytes(&self) -> Result<Vec<u8>, SerializationError> {
        let mut buffer = Vec::new();

        // Write number of entries
        buffer.extend_from_slice(&(self.increases.len() as u32).to_le_bytes());

        // Write each key-value pair
        for (key, data) in &self.increases {
            let key_bytes = key.serialize_to_bytes()?;
            buffer.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(&key_bytes);

            let data_bytes = data.serialize_to_bytes()?;
            buffer.extend_from_slice(&data_bytes);
        }

        Ok(buffer)
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

        // Clone self once, then merge other's data in-place.
        let mut merged = self.clone();
        for (key, data) in &other_multiple_increase.increases {
            Self::merge_increase(&mut merged.increases, key.clone(), data.clone())?;
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

    fn query_statistic_with_bounds(
        &self,
        statistic: Statistic,
        key: &Option<KeyByLabelValues>,
        _query_kwargs: &std::collections::HashMap<String, String>,
        bounds: &QueryBounds,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        let key_val = key
            .as_ref()
            .ok_or("Key required for MultipleIncreaseAccumulator")?;
        let data = self
            .increases
            .get(key_val)
            .ok_or_else(|| format!("Key {key_val} not found in MultipleIncreaseAccumulator"))?;
        data.query_with_bounds(statistic, bounds)
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
                Self::merge_increase(&mut result.increases, key, data)?;
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
    fn test_multiple_increase_accumulator_corrects_counter_reset() {
        let mut acc = MultipleIncreaseAccumulator::new();
        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);
        let mut increase_acc = create_test_increase_accumulator_with_time(100.0, 0, 100.0, 0);

        increase_acc.update(Measurement::new(150.0), 1_000);
        increase_acc.update(Measurement::new(10.0), 2_000);
        increase_acc.update(Measurement::new(60.0), 3_000);
        acc.update(key.clone(), increase_acc);

        assert_eq!(acc.query(Statistic::Increase, &key, None).unwrap(), 110.0);
        let rate = acc.query(Statistic::Rate, &key, None).unwrap();
        assert!((rate - (110.0 / 3.0)).abs() < f64::EPSILON);
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
        assert_eq!(merged_key1.sample_count, 2);
    }

    #[test]
    fn test_multiple_increase_accumulator_serialization() {
        let mut acc = MultipleIncreaseAccumulator::new();

        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);

        acc.update(
            key.clone(),
            IncreaseAccumulator::new_with_sample_count(
                Measurement::new(10.0),
                1_000,
                Measurement::new(25.0),
                2_000,
                2,
            ),
        );

        // Test JSON serialization
        let json_value = acc.serialize_to_json().unwrap();
        let deserialized = MultipleIncreaseAccumulator::deserialize_from_json(&json_value).unwrap();

        assert_eq!(deserialized.increases.len(), 1);
        let deserialized_acc = deserialized.increases.get(&key).unwrap();
        assert_eq!(deserialized_acc.starting_measurement.value, 10.0);
        assert_eq!(deserialized_acc.last_seen_measurement.value, 25.0);
        assert_eq!(deserialized_acc.sample_count, 2);

        // Test binary serialization
        let bytes = acc.serialize_to_bytes().unwrap();
        let deserialized_bytes =
            MultipleIncreaseAccumulator::deserialize_from_bytes(&bytes).unwrap();

        assert_eq!(deserialized_bytes.increases.len(), 1);
        let deserialized_acc_bytes = deserialized_bytes.increases.get(&key).unwrap();
        assert_eq!(deserialized_acc_bytes.starting_measurement.value, 10.0);
        assert_eq!(deserialized_acc_bytes.last_seen_measurement.value, 25.0);
        assert_eq!(deserialized_acc_bytes.sample_count, 2);
    }

    #[test]
    fn keyed_queries_use_each_key_sample_count() {
        let key_with_enough_samples = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);
        let key_with_one_sample = KeyByLabelValues::new_with_labels(vec!["api".to_string()]);
        let mut acc = MultipleIncreaseAccumulator::new();
        acc.update(
            key_with_enough_samples.clone(),
            IncreaseAccumulator::new_with_sample_count(
                Measurement::new(100.0),
                1_000,
                Measurement::new(110.0),
                2_000,
                2,
            ),
        );
        acc.update(
            key_with_one_sample.clone(),
            IncreaseAccumulator::new(
                Measurement::new(100.0),
                1_000,
                Measurement::new(100.0),
                1_000,
            ),
        );

        let bounds = crate::data_model::QueryBounds::new(1_000, 2_000);
        let query_kwargs = HashMap::new();
        assert!(acc
            .query_statistic_with_bounds(
                Statistic::Rate,
                &Some(key_with_enough_samples),
                &query_kwargs,
                &bounds,
            )
            .is_ok());
        assert!(acc
            .query_statistic_with_bounds(
                Statistic::Rate,
                &Some(key_with_one_sample),
                &query_kwargs,
                &bounds,
            )
            .is_err());
    }

    #[test]
    fn test_multiple_increase_counter_reset_correction_survives_serialization() {
        let mut acc = MultipleIncreaseAccumulator::new();
        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);
        let mut increase_acc = create_test_increase_accumulator_with_time(100.0, 0, 100.0, 0);

        increase_acc.update(Measurement::new(150.0), 1_000);
        increase_acc.update(Measurement::new(10.0), 2_000);
        increase_acc.update(Measurement::new(60.0), 3_000);
        acc.update(key.clone(), increase_acc);

        let json_round_trip =
            MultipleIncreaseAccumulator::deserialize_from_json(&acc.serialize_to_json().unwrap())
                .unwrap();
        assert_eq!(
            json_round_trip
                .query(Statistic::Increase, &key, None)
                .unwrap(),
            110.0
        );

        let bytes_round_trip =
            MultipleIncreaseAccumulator::deserialize_from_bytes(&acc.serialize_to_bytes().unwrap())
                .unwrap();
        assert_eq!(
            bytes_round_trip
                .query(Statistic::Increase, &key, None)
                .unwrap(),
            110.0
        );
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

    #[test]
    fn test_trait_object_merge_corrects_counter_reset() {
        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);
        let mut first = MultipleIncreaseAccumulator::new();
        let mut first_data = create_test_increase_accumulator_with_time(100.0, 0, 100.0, 0);
        first_data.update(Measurement::new(150.0), 1_000);
        first.update(key.clone(), first_data);

        let mut second = MultipleIncreaseAccumulator::new();
        second.update(
            key.clone(),
            create_test_increase_accumulator_with_time(10.0, 2_000, 60.0, 3_000),
        );

        let merged = first.merge_with(&second).unwrap();
        let merged = merged
            .as_any()
            .downcast_ref::<MultipleIncreaseAccumulator>()
            .unwrap();
        assert_eq!(
            merged.query(Statistic::Increase, &key, None).unwrap(),
            110.0
        );
    }
}
