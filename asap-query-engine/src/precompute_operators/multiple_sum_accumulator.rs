use crate::data_model::{
    AggregateCore, AggregationType, KeyByLabelValues, MergeableAccumulator,
    MultipleSubpopulationAggregate, MultipleSubpopulationAggregateFactory, SerializableToSink,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

/// Accumulator that maintains separate sum values for multiple keys
/// Allows querying sums for specific label combinations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipleSumAccumulator {
    pub sums: HashMap<KeyByLabelValues, f64>,
}

impl MultipleSumAccumulator {
    pub fn new() -> Self {
        Self {
            sums: HashMap::new(),
        }
    }

    pub fn new_with_sums(sums: HashMap<KeyByLabelValues, f64>) -> Self {
        Self { sums }
    }

    pub fn update(&mut self, key: KeyByLabelValues, value: f64) {
        *self.sums.entry(key).or_insert(0.0) += value;
    }

    pub fn add_sum(&mut self, key: KeyByLabelValues, sum: f64) {
        self.sums.insert(key, sum);
    }

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let sums_data = data["sums"]
            .as_object()
            .ok_or("Missing or invalid 'sums' field")?;

        let mut sums = HashMap::new();
        for (key_str, value) in sums_data {
            let key_json: Value = serde_json::from_str(key_str)?;
            let key = KeyByLabelValues::deserialize_from_json(&key_json)?;
            let sum = value.as_f64().ok_or("Invalid sum value")?;
            sums.insert(key, sum);
        }

        Ok(Self { sums })
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let precompute: HashMap<String, f64> = rmp_serde::from_slice(buffer).map_err(|e| {
            format!("Failed to deserialize MultipleSumAccumulator from MessagePack: {e}")
        })?;

        let mut sums = HashMap::new();
        for (key_str, sum) in precompute {
            let key_values: Vec<String> = key_str.split(';').map(|s| s.to_string()).collect();
            let key = KeyByLabelValues::new_with_labels(key_values);
            sums.insert(key, sum);
        }

        Ok(Self { sums })
    }

    /// Serialize to Arroyo-compatible format (MessagePack HashMap<String, f64>)
    pub fn serialize_to_bytes_arroyo(&self) -> Vec<u8> {
        use serde::Serialize;
        let per_key_storage: HashMap<String, f64> = self
            .sums
            .iter()
            .map(|(key, &sum)| (key.labels.join(";"), sum))
            .collect();

        let mut buf = Vec::new();
        per_key_storage
            .serialize(&mut rmp_serde::Serializer::new(&mut buf))
            .expect("Failed to serialize MultipleSumAccumulator to MessagePack");
        buf
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut offset = 0;

        // Read number of entries
        if buffer.len() < 4 {
            return Err("Buffer too short for entry count".into());
        }
        let num_entries = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]) as usize;
        offset += 4;

        let mut sums = HashMap::new();

        for _ in 0..num_entries {
            // Read key length and data
            if buffer.len() < offset + 4 {
                return Err("Buffer too short for key length".into());
            }
            let key_length = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            offset += 4;

            if buffer.len() < offset + key_length {
                return Err("Buffer too short for key data".into());
            }
            let key =
                KeyByLabelValues::deserialize_from_bytes(&buffer[offset..offset + key_length])?;
            offset += key_length;

            // Read sum value
            if buffer.len() < offset + 8 {
                return Err("Buffer too short for sum value".into());
            }
            let sum = f64::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
                buffer[offset + 4],
                buffer[offset + 5],
                buffer[offset + 6],
                buffer[offset + 7],
            ]);
            offset += 8;

            sums.insert(key, sum);
        }

        Ok(Self { sums })
    }
}

impl Default for MultipleSumAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SerializableToSink for MultipleSumAccumulator {
    fn serialize_to_json(&self) -> Value {
        let mut sums_obj = serde_json::Map::new();
        for (key, sum) in &self.sums {
            let key_json = key.serialize_to_json();
            let key_str = serde_json::to_string(&key_json).unwrap();
            sums_obj.insert(
                key_str,
                Value::Number(serde_json::Number::from_f64(*sum).unwrap()),
            );
        }

        serde_json::json!({
            "sums": sums_obj
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        // Write number of entries
        buffer.extend_from_slice(&(self.sums.len() as u32).to_le_bytes());

        // Write each key-value pair
        for (key, sum) in &self.sums {
            let key_bytes = key.serialize_to_bytes();

            // Write key length and data
            buffer.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(&key_bytes);

            // Write sum value
            buffer.extend_from_slice(&sum.to_le_bytes());
        }

        buffer
    }
}

impl AggregateCore for MultipleSumAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "MultipleSumAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        // Check if other is also a MultipleSumAccumulator
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge MultipleSumAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        // Downcast to MultipleSumAccumulator
        let other_multiple_sum = other
            .as_any()
            .downcast_ref::<MultipleSumAccumulator>()
            .ok_or("Failed to downcast to MultipleSumAccumulator")?;

        // Use the existing merge_accumulators method
        let merged = Self::merge_accumulators(vec![self.clone(), other_multiple_sum.clone()])?;

        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::MultipleSum
    }

    fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
        Some(self.sums.keys().cloned().collect())
    }
}

impl MultipleSubpopulationAggregate for MultipleSumAccumulator {
    fn query(
        &self,
        statistic: Statistic,
        key: &KeyByLabelValues,
        _query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        match statistic {
            Statistic::Sum | Statistic::Count => self
                .sums
                .get(key)
                .copied()
                .ok_or_else(|| "Key not found in MultipleSumAccumulator".to_string().into()),
            _ => Err(
                format!("Unsupported statistic in MultipleSumAccumulator: {statistic:?}").into(),
            ),
        }
    }

    fn clone_boxed(&self) -> Box<dyn MultipleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

// Factory implementation for merging
pub struct MultipleSumAccumulatorFactory;

impl MultipleSubpopulationAggregateFactory for MultipleSumAccumulatorFactory {
    fn merge_accumulators(
        &self,
        accumulators: Vec<Box<dyn MultipleSubpopulationAggregate>>,
    ) -> Result<Box<dyn MultipleSubpopulationAggregate>, Box<dyn std::error::Error + Send + Sync>>
    {
        let mut merged_sums = HashMap::new();

        for acc in accumulators {
            if acc.type_name() != "MultipleSumAccumulator" {
                return Err("Cannot merge different accumulator types".into());
            }

            // Get keys and merge values
            let keys = acc.get_keys().unwrap();
            for key in keys {
                let value = acc.query(Statistic::Sum, &key, None)?;
                *merged_sums.entry(key).or_insert(0.0) += value;
            }
        }

        Ok(Box::new(MultipleSumAccumulator::new_with_sums(merged_sums)))
    }

    fn create_default(&self) -> Box<dyn MultipleSubpopulationAggregate> {
        Box::new(MultipleSumAccumulator::new())
    }
}

impl MergeableAccumulator<MultipleSumAccumulator> for MultipleSumAccumulator {
    fn merge_accumulators(
        accumulators: Vec<MultipleSumAccumulator>,
    ) -> Result<MultipleSumAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let mut result = MultipleSumAccumulator::new();

        for acc in accumulators {
            for (key, sum) in acc.sums {
                *result.sums.entry(key).or_insert(0.0) += sum;
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    #[test]
    fn test_multiple_sum_accumulator_creation() {
        let acc = MultipleSumAccumulator::new();
        assert!(acc.sums.is_empty());
    }

    #[test]
    fn test_multiple_sum_accumulator_update() {
        let mut acc = MultipleSumAccumulator::new();

        let key1 = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);

        let key2 = KeyByLabelValues::new_with_labels(vec!["api".to_string()]);

        acc.update(key1.clone(), 10.0);
        acc.update(key2.clone(), 20.0);
        acc.update(key1.clone(), 5.0); // Should add to existing

        assert_eq!(acc.sums.get(&key1), Some(&15.0));
        assert_eq!(acc.sums.get(&key2), Some(&20.0));
    }

    #[test]
    fn test_multiple_sum_accumulator_query() {
        let mut acc = MultipleSumAccumulator::new();

        let key = KeyByLabelValues::new_with_labels(vec!["service".to_string()]);

        acc.add_sum(key.clone(), 42.0);

        // Test total queries (querying with the specific key)
        assert_eq!(
            crate::MultipleSubpopulationAggregate::query(&acc, Statistic::Sum, &key, None).unwrap(),
            42.0
        );

        // Test error cases
        assert!(
            crate::MultipleSubpopulationAggregate::query(&acc, Statistic::Min, &key, None).is_err()
        );
    }

    #[test]
    fn test_multiple_sum_accumulator_get_keys() {
        let mut acc = MultipleSumAccumulator::new();

        let key1 = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);

        let key2 = KeyByLabelValues::new_with_labels(vec!["api".to_string()]);

        acc.add_sum(key1.clone(), 10.0);
        acc.add_sum(key2.clone(), 20.0);

        let keys = crate::AggregateCore::get_keys(&acc).unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&key1));
        assert!(keys.contains(&key2));
    }

    #[test]
    fn test_multiple_sum_accumulator_merge() {
        let mut acc1 = MultipleSumAccumulator::new();
        let mut acc2 = MultipleSumAccumulator::new();

        let key1 = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);

        let key2 = KeyByLabelValues::new_with_labels(vec!["api".to_string()]);

        acc1.add_sum(key1.clone(), 10.0);
        acc1.add_sum(key2.clone(), 20.0);

        acc2.add_sum(key1.clone(), 5.0); // Same key, different accumulator

        let merged = <MultipleSumAccumulator as MergeableAccumulator<MultipleSumAccumulator>>::merge_accumulators(vec![acc1, acc2]).unwrap();

        assert_eq!(merged.sums.get(&key1), Some(&15.0)); // Should be merged
        assert_eq!(merged.sums.get(&key2), Some(&20.0)); // Should be preserved
    }

    #[test]
    fn test_multiple_sum_accumulator_serialization() {
        let mut acc = MultipleSumAccumulator::new();

        let key = KeyByLabelValues::new_with_labels(vec!["service".to_string()]);

        acc.add_sum(key.clone(), 42.5);

        // Test JSON serialization
        let json = acc.serialize_to_json();
        let deserialized = MultipleSumAccumulator::deserialize_from_json(&json).unwrap();
        assert_eq!(deserialized.sums.get(&key), Some(&42.5));

        // Test byte serialization
        let bytes = acc.serialize_to_bytes();
        let deserialized_bytes = MultipleSumAccumulator::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(deserialized_bytes.sums.get(&key), Some(&42.5));
    }

    #[test]
    fn test_trait_object() {
        let mut acc = MultipleSumAccumulator::new();

        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);

        acc.add_sum(key.clone(), 42.0);

        let trait_obj: Box<dyn AggregateCore> = Box::new(acc);

        // Test type name through trait object
        assert_eq!(trait_obj.type_name(), "MultipleSumAccumulator");
    }
}
