// Adapted from QueryEngineRust/src/precompute_operators/count_min_sketch_accumulator.rs
// Changes:
//   - Renamed CountMinSketchAccumulator -> CountMinSketch
//   - _update(&KeyByLabelValues) -> pub update(&str)  (caller does key-to-string conversion)
//   - query_key(&KeyByLabelValues) -> query_key(&str)
//   - serialize_to_bytes (trait) -> serialize_msgpack (inherent method)
//   - deserialize_from_bytes_arroyo -> deserialize_msgpack
//   - merge_accumulators -> merge
//   - Removed: deserialize_from_json, deserialize_from_bytes (legacy QE formats, stay in QE)
//   - Removed: merge_multiple (QE trait-object helper, stays in QE)
//   - Removed: AggregateCore, SerializableToSink, MergeableAccumulator, MultipleSubpopulationAggregate impls
//   - Added: aggregate_count() / aggregate_sum() one-shot helpers for Arroyo call pattern

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh32::xxh32;

/// Count-Min Sketch probabilistic data structure for frequency counting.
/// Provides approximate frequency counts with error bounds.
/// This is the canonical shared implementation; the msgpack wire format is the
/// contract between Arroyo UDAFs (producers) and QueryEngineRust (consumer).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountMinSketch {
    pub sketch: Vec<Vec<f64>>,
    pub row_num: usize,
    pub col_num: usize,
}

impl CountMinSketch {
    pub fn new(row_num: usize, col_num: usize) -> Self {
        let sketch = vec![vec![0.0; col_num]; row_num];
        Self {
            sketch,
            row_num,
            col_num,
        }
    }

    pub fn update(&mut self, key: &str, value: f64) {
        let key_bytes = key.as_bytes();
        // Update each row using different hash functions
        for i in 0..self.row_num {
            let hash_value = xxh32(key_bytes, i as u32);
            let col_index = (hash_value as usize) % self.col_num;
            self.sketch[i][col_index] += value;
        }
    }

    pub fn query_key(&self, key: &str) -> f64 {
        let key_bytes = key.as_bytes();
        let mut min_value = f64::MAX;
        // Query each row and take the minimum
        for i in 0..self.row_num {
            let hash_value = xxh32(key_bytes, i as u32);
            let col_index = (hash_value as usize) % self.col_num;
            min_value = min_value.min(self.sketch[i][col_index]);
        }
        min_value
    }

    pub fn merge(
        accumulators: Vec<Self>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        if accumulators.len() == 1 {
            return Ok(accumulators.into_iter().next().unwrap());
        }

        // Check that all accumulators have the same dimensions
        let row_num = accumulators[0].row_num;
        let col_num = accumulators[0].col_num;

        for acc in &accumulators {
            if acc.row_num != row_num || acc.col_num != col_num {
                return Err(
                    "Cannot merge CountMinSketch accumulators with different dimensions".into(),
                );
            }
        }

        let mut merged = accumulators[0].clone();
        // Add all sketches element-wise
        for acc in &accumulators[1..] {
            for (merged_row, acc_row) in merged.sketch.iter_mut().zip(&acc.sketch) {
                for (m_cell, a_cell) in merged_row.iter_mut().zip(acc_row.iter()) {
                    *m_cell += *a_cell;
                }
            }
        }

        Ok(merged)
    }

    /// Merge from references, allocating only the output — no input clones.
    pub fn merge_refs(
        accumulators: &[&Self],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let row_num = accumulators[0].row_num;
        let col_num = accumulators[0].col_num;

        for acc in accumulators {
            if acc.row_num != row_num || acc.col_num != col_num {
                return Err(
                    "Cannot merge CountMinSketch accumulators with different dimensions".into(),
                );
            }
        }

        let mut merged = Self::new(row_num, col_num);
        for acc in accumulators {
            for (merged_row, acc_row) in merged.sketch.iter_mut().zip(&acc.sketch) {
                for (m_cell, a_cell) in merged_row.iter_mut().zip(acc_row.iter()) {
                    *m_cell += *a_cell;
                }
            }
        }

        Ok(merged)
    }

    /// Serialize to MessagePack — matches the Arroyo UDF wire format exactly.
    pub fn serialize_msgpack(&self) -> Vec<u8> {
        // Match Arroyo UDF: countminsketch.serialize(&mut Serializer::new(&mut buf))
        let mut buf = Vec::new();
        self.serialize(&mut rmp_serde::Serializer::new(&mut buf))
            .unwrap();
        buf
    }

    /// Deserialize from MessagePack produced by the Arroyo UDF.
    pub fn deserialize_msgpack(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        rmp_serde::from_slice(buffer).map_err(|e| {
            format!("Failed to deserialize CountMinSketch from MessagePack: {e}").into()
        })
    }

    /// One-shot aggregation for the Arroyo UDAF call pattern: build a sketch from
    /// parallel key/value slices and return the msgpack bytes.
    pub fn aggregate_count(
        depth: usize,
        width: usize,
        keys: &[&str],
        values: &[f64],
    ) -> Option<Vec<u8>> {
        if keys.is_empty() {
            return None;
        }
        let mut sketch = Self::new(depth, width);
        for (key, &value) in keys.iter().zip(values.iter()) {
            sketch.update(key, value);
        }
        Some(sketch.serialize_msgpack())
    }

    /// Same as aggregate_count — CMS accumulates sums by construction.
    pub fn aggregate_sum(
        depth: usize,
        width: usize,
        keys: &[&str],
        values: &[f64],
    ) -> Option<Vec<u8>> {
        Self::aggregate_count(depth, width, keys, values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_min_sketch_creation() {
        let cms = CountMinSketch::new(4, 1000);
        assert_eq!(cms.row_num, 4);
        assert_eq!(cms.col_num, 1000);
        assert_eq!(cms.sketch.len(), 4);
        assert_eq!(cms.sketch[0].len(), 1000);

        // Check all values are initialized to 0
        for row in &cms.sketch {
            for &value in row {
                assert_eq!(value, 0.0);
            }
        }
    }

    #[test]
    fn test_count_min_sketch_update() {
        let mut cms = CountMinSketch::new(2, 10);
        cms.update("key1", 1.0);
        // Query should return at least the updated value
        let result = cms.query_key("key1");
        assert!(result >= 1.0);
    }

    #[test]
    fn test_count_min_sketch_query_empty() {
        let cms = CountMinSketch::new(2, 10);
        assert_eq!(cms.query_key("anything"), 0.0);
    }

    #[test]
    fn test_count_min_sketch_merge() {
        let mut cms1 = CountMinSketch::new(2, 3);
        let mut cms2 = CountMinSketch::new(2, 3);

        cms1.sketch[0][0] = 5.0;
        cms1.sketch[1][2] = 10.0;

        cms2.sketch[0][0] = 3.0;
        cms2.sketch[0][1] = 7.0;

        let merged = CountMinSketch::merge(vec![cms1, cms2]).unwrap();

        assert_eq!(merged.sketch[0][0], 8.0); // 5 + 3
        assert_eq!(merged.sketch[0][1], 7.0); // 0 + 7
        assert_eq!(merged.sketch[1][2], 10.0); // 10 + 0
    }

    #[test]
    fn test_count_min_sketch_merge_dimension_mismatch() {
        let cms1 = CountMinSketch::new(2, 3);
        let cms2 = CountMinSketch::new(3, 3);
        assert!(CountMinSketch::merge(vec![cms1, cms2]).is_err());
    }

    #[test]
    fn test_count_min_sketch_msgpack_round_trip() {
        let mut cms = CountMinSketch::new(2, 3);
        cms.sketch[0][1] = 42.0;
        cms.sketch[1][2] = 100.0;

        let bytes = cms.serialize_msgpack();
        let deserialized = CountMinSketch::deserialize_msgpack(&bytes).unwrap();

        assert_eq!(deserialized.row_num, 2);
        assert_eq!(deserialized.col_num, 3);
        assert_eq!(deserialized.sketch[0][1], 42.0);
        assert_eq!(deserialized.sketch[1][2], 100.0);
    }

    #[test]
    fn test_aggregate_count() {
        let keys = ["a", "b", "a"];
        let values = [1.0, 2.0, 3.0];
        let bytes = CountMinSketch::aggregate_count(4, 100, &keys, &values).unwrap();
        let cms = CountMinSketch::deserialize_msgpack(&bytes).unwrap();
        // "a" was updated twice (1.0 + 3.0 = 4.0), "b" once (2.0)
        assert!(cms.query_key("a") >= 4.0);
        assert!(cms.query_key("b") >= 2.0);
    }

    #[test]
    fn test_aggregate_count_empty() {
        assert!(CountMinSketch::aggregate_count(4, 100, &[], &[]).is_none());
    }
}
