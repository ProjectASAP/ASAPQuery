use crate::data_model::{
    AggregateCore, KeyByLabelValues, MergeableAccumulator, MultipleSubpopulationAggregate,
    SerializableToSink,
};
use serde_json::Value;
use sketch_core::set_aggregator::SetAggregator;
use std::collections::{HashMap, HashSet};

use promql_utilities::query_logics::enums::Statistic;

/// Set aggregator accumulator — wraps sketch_core::SetAggregator.
/// Core struct, merge/serde logic live in sketch-core.
/// This file retains QE-specific trait impls, KeyByLabelValues conversion,
/// and legacy deserializers.
#[derive(Debug, Clone)]
pub struct SetAggregatorAccumulator {
    pub added: HashSet<KeyByLabelValues>,
}

impl SetAggregatorAccumulator {
    pub fn new() -> Self {
        Self {
            added: HashSet::new(),
        }
    }

    pub fn with_added(added: HashSet<KeyByLabelValues>) -> Self {
        Self { added }
    }

    pub fn add_key(&mut self, key: KeyByLabelValues) {
        self.added.insert(key);
    }

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let mut added = HashSet::new();

        if let Some(added_array) = data["added"].as_array() {
            for item in added_array {
                let key_data = if let Some(values) = item.get("values") {
                    values
                } else {
                    item
                };
                let key = KeyByLabelValues::deserialize_from_json(key_data)?;
                added.insert(key);
            }
        }

        Ok(Self { added })
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut offset = 0;
        let mut added = HashSet::new();

        if offset + 4 > buffer.len() {
            return Err("Buffer too short for added set size".into());
        }
        let added_size = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]) as usize;
        offset += 4;

        for _ in 0..added_size {
            if offset + 4 > buffer.len() {
                return Err("Buffer too short for added item size".into());
            }
            let item_size = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + item_size > buffer.len() {
                return Err("Buffer too short for added item data".into());
            }
            let key =
                KeyByLabelValues::deserialize_from_bytes(&buffer[offset..offset + item_size])?;
            offset += item_size;
            added.insert(key);
        }

        Ok(Self { added })
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let sa = SetAggregator::deserialize_msgpack(buffer)?;
        let added = sa
            .values
            .into_iter()
            .map(|s| KeyByLabelValues::from_semicolon_str(&s))
            .collect();
        Ok(Self { added })
    }

    /// Serialize to Arroyo-compatible format (MessagePack StringSet).
    /// Delegates to sketch-core's canonical wire format.
    pub fn serialize_to_bytes_arroyo(&self) -> Vec<u8> {
        let mut sa = SetAggregator::new();
        for key in &self.added {
            sa.insert(&key.to_semicolon_str());
        }
        sa.serialize_msgpack()
    }
}

impl Default for SetAggregatorAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SerializableToSink for SetAggregatorAccumulator {
    fn serialize_to_json(&self) -> Value {
        let added_json: Vec<Value> = self
            .added
            .iter()
            .map(|key| key.serialize_to_json())
            .collect();
        serde_json::json!({ "added": added_json })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        // Legacy binary format (Flink); matches deserialize_from_bytes().
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&(self.added.len() as u32).to_le_bytes());
        for key in &self.added {
            let key_bytes = key.serialize_to_bytes();
            buffer.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(&key_bytes);
        }
        buffer
    }
}

impl AggregateCore for SetAggregatorAccumulator {
    fn type_name(&self) -> &'static str {
        "SetAggregatorAccumulator"
    }

    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge SetAggregatorAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        let other_set = other
            .as_any()
            .downcast_ref::<SetAggregatorAccumulator>()
            .ok_or("Failed to downcast to SetAggregatorAccumulator")?;

        let merged = Self::merge_accumulators(vec![self.clone(), other_set.clone()])?;
        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> &'static str {
        "SetAggregatorAccumulator"
    }

    fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
        Some(self.added.iter().cloned().collect())
    }
}

impl MultipleSubpopulationAggregate for SetAggregatorAccumulator {
    fn query(
        &self,
        _statistic: Statistic,
        _key: &KeyByLabelValues,
        _query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        Err("SetAggregatorAccumulator does not support query operation".into())
    }

    fn clone_boxed(&self) -> Box<dyn MultipleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

impl MergeableAccumulator<SetAggregatorAccumulator> for SetAggregatorAccumulator {
    fn merge_accumulators(
        accumulators: Vec<SetAggregatorAccumulator>,
    ) -> Result<SetAggregatorAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let mut merged = SetAggregatorAccumulator::new();
        for accumulator in accumulators {
            merged.added.extend(accumulator.added);
        }

        Ok(merged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_key(service: &str) -> KeyByLabelValues {
        KeyByLabelValues::new_with_labels(vec![service.to_string()])
    }

    #[test]
    fn test_set_aggregator_creation() {
        let acc = SetAggregatorAccumulator::new();
        assert!(acc.added.is_empty());
    }

    #[test]
    fn test_set_aggregator_add() {
        let mut acc = SetAggregatorAccumulator::new();
        let key1 = create_test_key("web");
        let key2 = create_test_key("api");
        acc.add_key(key1.clone());
        acc.add_key(key2.clone());
        assert_eq!(acc.added.len(), 2);
        assert!(acc.added.contains(&key1));
        assert!(acc.added.contains(&key2));
    }

    #[test]
    fn test_set_aggregator_get_keys() {
        let mut acc = SetAggregatorAccumulator::new();
        let key1 = create_test_key("web");
        let key2 = create_test_key("api");
        acc.add_key(key1.clone());
        acc.add_key(key2.clone());
        let keys = acc.get_keys().unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&key1));
        assert!(keys.contains(&key2));
    }

    #[test]
    fn test_set_aggregator_merge() {
        let mut acc1 = SetAggregatorAccumulator::new();
        let mut acc2 = SetAggregatorAccumulator::new();
        let key1 = create_test_key("web");
        let key2 = create_test_key("api");
        let key3 = create_test_key("db");
        acc1.add_key(key1.clone());
        acc1.add_key(key2.clone());
        acc2.add_key(key2.clone());
        acc2.add_key(key3.clone());
        let merged = SetAggregatorAccumulator::merge_accumulators(vec![acc1, acc2]).unwrap();
        assert_eq!(merged.added.len(), 3);
        assert!(merged.added.contains(&key1));
        assert!(merged.added.contains(&key2));
        assert!(merged.added.contains(&key3));
    }

    #[test]
    fn test_set_aggregator_query() {
        let acc = SetAggregatorAccumulator::new();
        let key = create_test_key("test");
        assert!(acc.query(Statistic::Sum, &key, None).is_err());
    }

    #[test]
    fn test_set_aggregator_serialization() {
        let mut acc = SetAggregatorAccumulator::new();
        let key1 = create_test_key("web");
        let key2 = create_test_key("api");
        acc.add_key(key1.clone());
        acc.add_key(key2.clone());

        // Test JSON serialization
        let json_value = acc.serialize_to_json();
        let deserialized = SetAggregatorAccumulator::deserialize_from_json(&json_value).unwrap();
        assert_eq!(deserialized.added.len(), 2);
        assert!(deserialized.added.contains(&key1));
        assert!(deserialized.added.contains(&key2));

        // Test binary serialization
        let bytes = acc.serialize_to_bytes();
        let deserialized_bytes = SetAggregatorAccumulator::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(deserialized_bytes.added.len(), 2);
        assert!(deserialized_bytes.added.contains(&key1));
        assert!(deserialized_bytes.added.contains(&key2));
    }

    #[test]
    fn test_trait_object() {
        let mut acc = SetAggregatorAccumulator::new();
        let key = create_test_key("web");
        acc.add_key(key.clone());
        let trait_obj: Box<dyn AggregateCore> = Box::new(acc);
        assert_eq!(trait_obj.type_name(), "SetAggregatorAccumulator");

        let multi_trait_obj: Box<dyn MultipleSubpopulationAggregate> =
            Box::new(SetAggregatorAccumulator::new());
        let keys = multi_trait_obj.get_keys().unwrap();
        assert_eq!(keys.len(), 0);
    }

    #[test]
    fn test_arroyo_roundtrip() {
        // Verify serialize_to_bytes_arroyo / deserialize_from_bytes_arroyo round-trip.
        // Both now delegate to sketch-core's SetAggregator which uses the same
        // StringSet { values: HashSet<String> } format as Arroyo's setaggregator_ UDF.
        let mut acc = SetAggregatorAccumulator::new();
        acc.add_key(KeyByLabelValues::new_with_labels(vec![
            "web".to_string(),
            "prod".to_string(),
        ]));
        acc.add_key(KeyByLabelValues::new_with_labels(vec!["api".to_string()]));

        let bytes = acc.serialize_to_bytes_arroyo();
        let deserialized = SetAggregatorAccumulator::deserialize_from_bytes_arroyo(&bytes).expect(
            "deserialize_from_bytes_arroyo failed — format mismatch with serialize_to_bytes_arroyo",
        );

        assert_eq!(
            deserialized.added.len(),
            acc.added.len(),
            "roundtrip changed the number of keys"
        );
        for key in &acc.added {
            assert!(
                deserialized.added.contains(key),
                "key {key:?} missing after arroyo roundtrip"
            );
        }
    }
}
