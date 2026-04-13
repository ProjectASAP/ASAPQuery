use crate::data_model::{
    AggregateCore, AggregationType, KeyByLabelValues, MergeableAccumulator,
    MultipleSubpopulationAggregate, SerializableToSink,
};
use serde_json::Value;
use sketch_core::delta_set_aggregator::{deserialize_msgpack, serialize_msgpack};
use std::collections::{HashMap, HashSet};

use promql_utilities::query_logics::enums::Statistic;

/// Accumulator that tracks sets of added and removed keys.
/// Used for delta aggregation to track changes in cardinality.
/// Wire format (DeltaResult) and msgpack serde live in sketch-core.
#[derive(Debug, Clone)]
pub struct DeltaSetAggregatorAccumulator {
    pub added: HashSet<KeyByLabelValues>,
    pub removed: HashSet<KeyByLabelValues>,
}

impl DeltaSetAggregatorAccumulator {
    pub fn new() -> Self {
        Self {
            added: HashSet::new(),
            removed: HashSet::new(),
        }
    }

    pub fn new_with_sets(
        added: HashSet<KeyByLabelValues>,
        removed: HashSet<KeyByLabelValues>,
    ) -> Self {
        Self { added, removed }
    }

    pub fn add_key(&mut self, key: KeyByLabelValues) {
        self.added.insert(key);
    }

    pub fn remove_key(&mut self, key: KeyByLabelValues) {
        self.removed.insert(key);
    }

    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let mut added = HashSet::new();
        let mut removed = HashSet::new();

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

        if let Some(removed_array) = data["removed"].as_array() {
            for item in removed_array {
                let key_data = if let Some(values) = item.get("values") {
                    values
                } else {
                    item
                };
                let key = KeyByLabelValues::deserialize_from_json(key_data)?;
                removed.insert(key);
            }
        }

        Ok(Self { added, removed })
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut offset = 0;
        let mut added = HashSet::new();
        let mut removed = HashSet::new();

        // Read added set
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

        // Read removed set
        if offset + 4 > buffer.len() {
            return Err("Buffer too short for removed set size".into());
        }
        let removed_size = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]) as usize;
        offset += 4;

        for _ in 0..removed_size {
            if offset + 4 > buffer.len() {
                return Err("Buffer too short for removed item size".into());
            }
            let item_size = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + item_size > buffer.len() {
                return Err("Buffer too short for removed item data".into());
            }
            let key =
                KeyByLabelValues::deserialize_from_bytes(&buffer[offset..offset + item_size])?;
            offset += item_size;
            removed.insert(key);
        }

        Ok(Self { added, removed })
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Delegate to sketch-core canonical DeltaResult msgpack format
        let delta = deserialize_msgpack(buffer)?;

        let mut added = HashSet::new();
        for item in &delta.added {
            added.insert(KeyByLabelValues::from_semicolon_str(item));
        }

        let mut removed = HashSet::new();
        for item in &delta.removed {
            removed.insert(KeyByLabelValues::from_semicolon_str(item));
        }

        Ok(Self { added, removed })
    }
}

impl Default for DeltaSetAggregatorAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SerializableToSink for DeltaSetAggregatorAccumulator {
    fn serialize_to_json(&self) -> Value {
        let added_json: Vec<Value> = self
            .added
            .iter()
            .map(|key| key.serialize_to_json())
            .collect();
        let removed_json: Vec<Value> = self
            .removed
            .iter()
            .map(|key| key.serialize_to_json())
            .collect();
        serde_json::json!({ "added": added_json, "removed": removed_json })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        // Delegate to sketch-core canonical DeltaResult msgpack format
        let added: HashSet<String> = self
            .added
            .iter()
            .map(|key| key.to_semicolon_str())
            .collect();
        let removed: HashSet<String> = self
            .removed
            .iter()
            .map(|key| key.to_semicolon_str())
            .collect();
        serialize_msgpack(&added, &removed)
    }
}

impl AggregateCore for DeltaSetAggregatorAccumulator {
    fn type_name(&self) -> &'static str {
        "DeltaSetAggregatorAccumulator"
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
                "Cannot merge DeltaSetAggregatorAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        let other_delta = other
            .as_any()
            .downcast_ref::<DeltaSetAggregatorAccumulator>()
            .ok_or("Failed to downcast to DeltaSetAggregatorAccumulator")?;

        let merged = Self::merge_accumulators(vec![self.clone(), other_delta.clone()])?;
        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::DeltaSetAggregator
    }

    fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
        if !self.removed.is_empty() {
            panic!("DeltaSetAggregatorAccumulator does not support get_keys when removed items are present");
        }
        Some(self.added.iter().cloned().collect())
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
            .ok_or("Key required for DeltaSetAggregatorAccumulator")?;
        self.query(statistic, key_val, Some(query_kwargs))
    }
}

impl MultipleSubpopulationAggregate for DeltaSetAggregatorAccumulator {
    fn query(
        &self,
        _statistic: Statistic,
        _key: &KeyByLabelValues,
        _query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        Err("DeltaSetAggregatorAccumulator does not support query operation".into())
    }

    fn clone_boxed(&self) -> Box<dyn MultipleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

impl MergeableAccumulator<DeltaSetAggregatorAccumulator> for DeltaSetAggregatorAccumulator {
    fn merge_accumulators(
        accumulators: Vec<DeltaSetAggregatorAccumulator>,
    ) -> Result<DeltaSetAggregatorAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let mut all_added = HashSet::new();
        let mut all_removed = HashSet::new();

        for accumulator in accumulators {
            all_added.extend(accumulator.added);
            all_removed.extend(accumulator.removed);
        }

        let conflicts: HashSet<KeyByLabelValues> =
            all_added.intersection(&all_removed).cloned().collect();
        for key in &conflicts {
            all_added.remove(key);
            all_removed.remove(key);
        }

        Ok(DeltaSetAggregatorAccumulator {
            added: all_added,
            removed: all_removed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_key(service: &str) -> KeyByLabelValues {
        KeyByLabelValues::new_with_labels(vec![service.to_string()])
    }

    #[test]
    fn test_delta_set_aggregator_creation() {
        let acc = DeltaSetAggregatorAccumulator::new();
        assert!(acc.added.is_empty());
        assert!(acc.removed.is_empty());
    }

    #[test]
    fn test_delta_set_aggregator_add_remove() {
        let mut acc = DeltaSetAggregatorAccumulator::new();
        let key1 = create_test_key("web");
        let key2 = create_test_key("api");
        acc.add_key(key1.clone());
        acc.remove_key(key2.clone());
        assert!(acc.added.contains(&key1));
        assert!(acc.removed.contains(&key2));
        assert_eq!(acc.added.len(), 1);
        assert_eq!(acc.removed.len(), 1);
    }

    #[test]
    fn test_delta_set_aggregator_merge() {
        let mut acc1 = DeltaSetAggregatorAccumulator::new();
        let mut acc2 = DeltaSetAggregatorAccumulator::new();
        let mut acc3 = DeltaSetAggregatorAccumulator::new();

        let key1 = create_test_key("web");
        let key2 = create_test_key("api");
        let key3 = create_test_key("db");
        let key4 = create_test_key("cache");

        acc1.add_key(key1.clone());
        acc1.remove_key(key2.clone());
        acc2.add_key(key2.clone());
        acc2.remove_key(key3.clone());
        acc3.add_key(key4.clone());

        let merged =
            DeltaSetAggregatorAccumulator::merge_accumulators(vec![acc1, acc2, acc3]).unwrap();

        assert!(merged.added.contains(&key1));
        assert!(merged.added.contains(&key4));
        assert!(!merged.added.contains(&key2));
        assert!(merged.removed.contains(&key3));
        assert!(!merged.removed.contains(&key2));
        assert_eq!(merged.added.len(), 2);
        assert_eq!(merged.removed.len(), 1);
    }

    #[test]
    fn test_delta_set_aggregator_serialization() {
        let mut acc = DeltaSetAggregatorAccumulator::new();
        let key1 = create_test_key("web");
        let key2 = create_test_key("api");
        acc.add_key(key1.clone());
        acc.remove_key(key2.clone());

        // Test binary (msgpack) serialization roundtrip
        let bytes = acc.serialize_to_bytes();
        let deserialized_bytes =
            DeltaSetAggregatorAccumulator::deserialize_from_bytes_arroyo(&bytes).unwrap();

        assert_eq!(deserialized_bytes.added.len(), 1);
        assert_eq!(deserialized_bytes.removed.len(), 1);
        assert!(deserialized_bytes.added.contains(&key1));
        assert!(deserialized_bytes.removed.contains(&key2));
    }

    #[test]
    fn test_delta_set_aggregator_query() {
        let acc = DeltaSetAggregatorAccumulator::new();
        let key = create_test_key("test");
        assert!(acc.query(Statistic::Sum, &key, None).is_err());
    }

    #[test]
    fn test_trait_object() {
        let mut acc = DeltaSetAggregatorAccumulator::new();
        let key = create_test_key("web");
        acc.add_key(key.clone());

        let trait_obj: Box<dyn AggregateCore> = Box::new(acc);
        assert_eq!(trait_obj.type_name(), "DeltaSetAggregatorAccumulator");

        let multi_trait_obj: Box<dyn MultipleSubpopulationAggregate> =
            Box::new(DeltaSetAggregatorAccumulator::new());
        let keys = multi_trait_obj.get_keys().unwrap();
        assert_eq!(keys.len(), 0);
    }
}
