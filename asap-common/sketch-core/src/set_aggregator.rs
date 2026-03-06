// Adapted from QueryEngineRust/src/precompute_operators/set_aggregator_accumulator.rs
// Changes:
//   - Renamed SetAggregatorAccumulator -> SetAggregator
//   - values field is now HashSet<String> instead of HashSet<KeyByLabelValues>
//   - add_key(&str) instead of add_key(KeyByLabelValues)
//   - serialize_msgpack / deserialize_msgpack use StringSet { values: HashSet<String> }
//     wire format matching the Arroyo setaggregator_ UDF exactly (same as DeltaResult pattern)
//   - merge_accumulators -> merge
//   - Removed: deserialize_from_json, deserialize_from_bytes, deserialize_from_bytes_arroyo
//     (QE-specific / buggy legacy formats stay in QE)
//   - Removed: AggregateCore, SerializableToSink, MergeableAccumulator, MultipleSubpopulationAggregate impls
//   - Removed: with_added (QE-specific constructor)

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Set aggregator for tracking a set of unique string keys.
/// Wire format: StringSet { values: HashSet<String> } in MessagePack — matches Arroyo setaggregator_ UDF.
#[derive(Debug, Clone)]
pub struct SetAggregator {
    pub values: HashSet<String>,
}

impl SetAggregator {
    pub fn new() -> Self {
        Self {
            values: HashSet::new(),
        }
    }

    pub fn insert(&mut self, key: &str) {
        self.values.insert(key.to_string());
    }

    pub fn merge(
        accumulators: Vec<Self>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let mut merged = SetAggregator::new();
        for accumulator in accumulators {
            merged.values.extend(accumulator.values);
        }

        Ok(merged)
    }

    /// Serialize to MessagePack — matches the Arroyo setaggregator_ UDF wire format exactly:
    /// StringSet { values: HashSet<String> } as a msgpack map.
    pub fn serialize_msgpack(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct StringSet<'a> {
            values: &'a HashSet<String>,
        }
        let wrapper = StringSet {
            values: &self.values,
        };
        let mut buf = Vec::new();
        rmp_serde::encode::write(&mut buf, &wrapper).unwrap();
        buf
    }

    /// Deserialize from MessagePack produced by the Arroyo setaggregator_ UDF.
    pub fn deserialize_msgpack(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        #[derive(Deserialize)]
        struct StringSet {
            values: HashSet<String>,
        }
        let wrapper: StringSet = rmp_serde::from_slice(buffer)
            .map_err(|e| format!("Failed to deserialize SetAggregator from MessagePack: {e}"))?;
        Ok(Self {
            values: wrapper.values,
        })
    }
}

impl Default for SetAggregator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creation() {
        let sa = SetAggregator::new();
        assert!(sa.values.is_empty());
    }

    #[test]
    fn test_insert() {
        let mut sa = SetAggregator::new();
        sa.insert("web");
        sa.insert("api");
        sa.insert("web"); // duplicate
        assert_eq!(sa.values.len(), 2);
        assert!(sa.values.contains("web"));
        assert!(sa.values.contains("api"));
    }

    #[test]
    fn test_merge() {
        let mut sa1 = SetAggregator::new();
        let mut sa2 = SetAggregator::new();

        sa1.insert("web");
        sa1.insert("api");
        sa2.insert("api"); // duplicate
        sa2.insert("db");

        let merged = SetAggregator::merge(vec![sa1, sa2]).unwrap();
        assert_eq!(merged.values.len(), 3);
        assert!(merged.values.contains("web"));
        assert!(merged.values.contains("api"));
        assert!(merged.values.contains("db"));
    }

    #[test]
    fn test_msgpack_round_trip() {
        let mut sa = SetAggregator::new();
        sa.insert("web");
        sa.insert("api");

        let bytes = sa.serialize_msgpack();
        let deserialized = SetAggregator::deserialize_msgpack(&bytes).unwrap();

        assert_eq!(deserialized.values.len(), 2);
        assert!(deserialized.values.contains("web"));
        assert!(deserialized.values.contains("api"));
    }

    #[test]
    fn test_msgpack_matches_arroyo_format() {
        // Verify wire format is StringSet { values: [...] } not a plain array.
        // Arroyo's setaggregator_.rs serializes StringSet { values: HashSet<String> }.
        #[derive(Deserialize)]
        struct StringSet {
            values: HashSet<String>,
        }
        let mut sa = SetAggregator::new();
        sa.insert("a");
        let bytes = sa.serialize_msgpack();
        let decoded: StringSet =
            rmp_serde::from_slice(&bytes).expect("should decode as StringSet { values: ... }");
        assert!(decoded.values.contains("a"));
    }
}
