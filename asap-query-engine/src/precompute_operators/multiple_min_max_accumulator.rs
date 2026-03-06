use crate::data_model::{
    AggregateCore, KeyByLabelValues, MergeableAccumulator, MultipleSubpopulationAggregate,
    SerializableToSink,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

/// Accumulator that maintains separate min/max values for multiple keys
/// Allows querying min/max for specific label combinations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipleMinMaxAccumulator {
    pub values: HashMap<KeyByLabelValues, f64>,
    pub sub_type: String, // "min" or "max"
}

impl MultipleMinMaxAccumulator {
    pub fn new(sub_type: String) -> Self {
        if sub_type != "min" && sub_type != "max" {
            panic!("sub_type must be 'min' or 'max'");
        }

        Self {
            values: HashMap::new(),
            sub_type,
        }
    }

    pub fn new_min() -> Self {
        Self::new("min".to_string())
    }

    pub fn new_max() -> Self {
        Self::new("max".to_string())
    }

    pub fn new_with_values(values: HashMap<KeyByLabelValues, f64>, sub_type: String) -> Self {
        if sub_type != "min" && sub_type != "max" {
            panic!("sub_type must be 'min' or 'max'");
        }

        Self { values, sub_type }
    }

    pub fn update(&mut self, key: KeyByLabelValues, value: f64) {
        match self.sub_type.as_str() {
            "min" => {
                let current = self.values.entry(key).or_insert(f64::INFINITY);
                if value < *current {
                    *current = value;
                }
            }
            "max" => {
                let current = self.values.entry(key).or_insert(f64::NEG_INFINITY);
                if value > *current {
                    *current = value;
                }
            }
            _ => panic!("Invalid sub_type"),
        }
    }

    pub fn add_value(&mut self, key: KeyByLabelValues, value: f64) {
        self.values.insert(key, value);
    }

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let sub_type = data["sub_type"]
            .as_str()
            .ok_or("Missing or invalid 'sub_type' field")?
            .to_string();

        if sub_type != "min" && sub_type != "max" {
            return Err("sub_type must be 'min' or 'max'".into());
        }

        let values_data = data["values"]
            .as_object()
            .ok_or("Missing or invalid 'values' field")?;

        let mut values = HashMap::new();
        for (key_str, value) in values_data {
            let key_json: Value = serde_json::from_str(key_str)?;
            let key = KeyByLabelValues::deserialize_from_json(&key_json)?;
            let val = value.as_f64().ok_or("Invalid value")?;
            values.insert(key, val);
        }

        Ok(Self { values, sub_type })
    }

    pub fn deserialize_from_bytes(
        buffer: &[u8],
        sub_type: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if sub_type != "min" && sub_type != "max" {
            return Err("sub_type must be 'min' or 'max'".into());
        }

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

        let mut values = HashMap::new();

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

            // Read value
            if buffer.len() < offset + 8 {
                return Err("Buffer too short for value".into());
            }
            let value = f64::from_le_bytes([
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

            values.insert(key, value);
        }

        Ok(Self { values, sub_type })
    }
}

impl SerializableToSink for MultipleMinMaxAccumulator {
    fn serialize_to_json(&self) -> Value {
        let mut values_obj = serde_json::Map::new();
        for (key, value) in &self.values {
            let key_json = key.serialize_to_json();
            let key_str = serde_json::to_string(&key_json).unwrap();
            values_obj.insert(
                key_str,
                Value::Number(serde_json::Number::from_f64(*value).unwrap()),
            );
        }

        serde_json::json!({
            "values": values_obj,
            "sub_type": self.sub_type
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();

        // Write number of entries
        buffer.extend_from_slice(&(self.values.len() as u32).to_le_bytes());

        // Write each key-value pair
        for (key, value) in &self.values {
            let key_bytes = key.serialize_to_bytes();

            // Write key length and data
            buffer.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(&key_bytes);

            // Write value
            buffer.extend_from_slice(&value.to_le_bytes());
        }

        buffer
    }
}

impl AggregateCore for MultipleMinMaxAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "MultipleMinMaxAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        // Check if other is also a MultipleMinMaxAccumulator
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge MultipleMinMaxAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        // Downcast to MultipleMinMaxAccumulator
        let other_multiple_minmax = other
            .as_any()
            .downcast_ref::<MultipleMinMaxAccumulator>()
            .ok_or("Failed to downcast to MultipleMinMaxAccumulator")?;

        // Use the existing merge_accumulators method
        let merged = Self::merge_accumulators(vec![self.clone(), other_multiple_minmax.clone()])?;

        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> &'static str {
        "MultipleMinMaxAccumulator"
    }

    fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
        Some(self.values.keys().cloned().collect())
    }
}

impl MultipleSubpopulationAggregate for MultipleMinMaxAccumulator {
    fn query(
        &self,
        statistic: Statistic,
        key: &KeyByLabelValues,
        _query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        // Query specific key
        match statistic {
            Statistic::Min => {
                if self.sub_type == "min" {
                    self.values.get(key).copied().ok_or_else(|| {
                        format!("Key {key} not found in MultipleMinMaxAccumulator").into()
                    })
                } else {
                    Err("Cannot query Min statistic from Max accumulator".into())
                }
            }
            Statistic::Max => {
                if self.sub_type == "max" {
                    self.values.get(key).copied().ok_or_else(|| {
                        format!("Key {key} not found in MultipleMinMaxAccumulator").into()
                    })
                } else {
                    Err("Cannot query Max statistic from Min accumulator".into())
                }
            }
            _ => Err(
                format!("Unsupported statistic in MultipleMinMaxAccumulator: {statistic:?}").into(),
            ),
        }
    }

    fn clone_boxed(&self) -> Box<dyn MultipleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

impl MergeableAccumulator<MultipleMinMaxAccumulator> for MultipleMinMaxAccumulator {
    fn merge_accumulators(
        accumulators: Vec<MultipleMinMaxAccumulator>,
    ) -> Result<MultipleMinMaxAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let sub_type = accumulators[0].sub_type.clone();

        // Verify all accumulators have the same sub_type
        for acc in &accumulators {
            if acc.sub_type != sub_type {
                return Err("Cannot merge accumulators with different sub_types".into());
            }
        }

        let mut result = MultipleMinMaxAccumulator::new(sub_type.clone());

        for acc in accumulators {
            for (key, value) in acc.values {
                match result.values.get(&key) {
                    Some(existing_value) => match sub_type.as_str() {
                        "min" => {
                            if value < *existing_value {
                                result.values.insert(key, value);
                            }
                        }
                        "max" => {
                            if value > *existing_value {
                                result.values.insert(key, value);
                            }
                        }
                        _ => unreachable!(),
                    },
                    None => {
                        result.values.insert(key, value);
                    }
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multiple_min_max_accumulator_creation() {
        let min_acc = MultipleMinMaxAccumulator::new_min();
        assert_eq!(min_acc.sub_type, "min");
        assert!(min_acc.values.is_empty());

        let max_acc = MultipleMinMaxAccumulator::new_max();
        assert_eq!(max_acc.sub_type, "max");
        assert!(max_acc.values.is_empty());
    }

    #[test]
    fn test_multiple_min_accumulator_update() {
        let mut acc = MultipleMinMaxAccumulator::new_min();

        let key1 = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);
        let key2 = KeyByLabelValues::new_with_labels(vec!["api".to_string()]);

        acc.update(key1.clone(), 10.0);
        acc.update(key1.clone(), 5.0); // Should update to smaller value
        acc.update(key1.clone(), 15.0); // Should not update (larger)
        acc.update(key2.clone(), 20.0);

        assert_eq!(acc.values.get(&key1), Some(&5.0));
        assert_eq!(acc.values.get(&key2), Some(&20.0));
    }

    #[test]
    fn test_multiple_max_accumulator_update() {
        let mut acc = MultipleMinMaxAccumulator::new_max();

        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);

        acc.update(key.clone(), 10.0);
        acc.update(key.clone(), 5.0); // Should not update (smaller)
        acc.update(key.clone(), 15.0); // Should update to larger value

        assert_eq!(acc.values.get(&key), Some(&15.0));
    }

    #[test]
    fn test_multiple_min_max_accumulator_query() {
        let mut min_acc = MultipleMinMaxAccumulator::new_min();
        let mut max_acc = MultipleMinMaxAccumulator::new_max();

        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);

        min_acc.add_value(key.clone(), 5.0);
        max_acc.add_value(key.clone(), 15.0);

        // Test queries with the specific key
        assert_eq!(
            crate::MultipleSubpopulationAggregate::query(&min_acc, Statistic::Min, &key, None)
                .unwrap(),
            5.0
        );
        assert_eq!(
            crate::MultipleSubpopulationAggregate::query(&max_acc, Statistic::Max, &key, None)
                .unwrap(),
            15.0
        );

        // Test error cases
        assert!(
            crate::MultipleSubpopulationAggregate::query(&min_acc, Statistic::Max, &key, None)
                .is_err()
        );
        assert!(
            crate::MultipleSubpopulationAggregate::query(&max_acc, Statistic::Min, &key, None)
                .is_err()
        );
        assert!(
            crate::MultipleSubpopulationAggregate::query(&min_acc, Statistic::Sum, &key, None)
                .is_err()
        );
    }

    #[test]
    fn test_multiple_min_max_accumulator_merge() {
        let mut acc1 = MultipleMinMaxAccumulator::new_min();
        let mut acc2 = MultipleMinMaxAccumulator::new_min();

        let key1 = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);

        let key2 = KeyByLabelValues::new_with_labels(vec!["api".to_string()]);

        acc1.add_value(key1.clone(), 10.0);
        acc1.add_value(key2.clone(), 20.0);

        acc2.add_value(key1.clone(), 5.0); // Smaller value, should be used

        let merged = <MultipleMinMaxAccumulator as MergeableAccumulator<
            MultipleMinMaxAccumulator,
        >>::merge_accumulators(vec![acc1, acc2])
        .unwrap();

        assert_eq!(merged.values.get(&key1), Some(&5.0)); // Should use smaller value
        assert_eq!(merged.values.get(&key2), Some(&20.0)); // Should be preserved
    }

    #[test]
    fn test_multiple_min_max_accumulator_serialization() {
        let mut acc = MultipleMinMaxAccumulator::new_min();

        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);

        acc.add_value(key.clone(), 42.5);

        // Test JSON serialization
        let json = acc.serialize_to_json();
        let deserialized = MultipleMinMaxAccumulator::deserialize_from_json(&json).unwrap();
        assert_eq!(deserialized.values.get(&key), Some(&42.5));
        assert_eq!(deserialized.sub_type, "min");

        // Test byte serialization
        let bytes = acc.serialize_to_bytes();
        let deserialized_bytes =
            MultipleMinMaxAccumulator::deserialize_from_bytes(&bytes, "min".to_string()).unwrap();
        assert_eq!(deserialized_bytes.values.get(&key), Some(&42.5));
        assert_eq!(deserialized_bytes.sub_type, "min");
    }

    #[test]
    fn test_trait_object() {
        let mut acc = MultipleMinMaxAccumulator::new_min();

        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string()]);

        acc.add_value(key.clone(), 42.0);

        let trait_obj: Box<dyn AggregateCore> = Box::new(acc);

        // Test type name through trait object
        assert_eq!(trait_obj.type_name(), "MultipleMinMaxAccumulator");
    }
}
