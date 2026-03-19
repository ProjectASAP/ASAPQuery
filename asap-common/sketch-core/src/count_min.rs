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

use crate::config::use_sketchlib_for_count_min;
use crate::count_min_sketchlib::{
    matrix_from_sketchlib_cms, new_sketchlib_cms, sketchlib_cms_from_matrix, sketchlib_cms_query,
    sketchlib_cms_update, SketchlibCms,
};

#[derive(Serialize, Deserialize)]
struct WireFormat {
    sketch: Vec<Vec<f64>>,
    row_num: usize,
    col_num: usize,
}

/// Backend implementation for Count-Min Sketch. Only one is active at a time.
#[derive(Debug, Clone)]
pub enum CountMinBackend {
    /// Original hand-written matrix implementation.
    Legacy(Vec<Vec<f64>>),
    /// sketchlib-rust backed implementation.
    Sketchlib(SketchlibCms),
}

/// Count-Min Sketch probabilistic data structure for frequency counting.
/// Provides approximate frequency counts with error bounds.
/// This is the canonical shared implementation; the msgpack wire format is the
/// contract between Arroyo UDAFs (producers) and QueryEngineRust (consumer).
#[derive(Debug, Clone)]
pub struct CountMinSketch {
    pub row_num: usize,
    pub col_num: usize,
    pub backend: CountMinBackend,
}

impl CountMinSketch {
    pub fn new(row_num: usize, col_num: usize) -> Self {
        let backend = if use_sketchlib_for_count_min() {
            CountMinBackend::Sketchlib(new_sketchlib_cms(row_num, col_num))
        } else {
            CountMinBackend::Legacy(vec![vec![0.0; col_num]; row_num])
        };
        Self {
            row_num,
            col_num,
            backend,
        }
    }

    /// Returns the sketch matrix (for wire format, serialization, tests).
    pub fn sketch(&self) -> Vec<Vec<f64>> {
        match &self.backend {
            CountMinBackend::Legacy(m) => m.clone(),
            CountMinBackend::Sketchlib(s) => matrix_from_sketchlib_cms(s),
        }
    }

    /// Mutable access to the matrix. Only `Some` for Legacy backend.
    pub fn sketch_mut(&mut self) -> Option<&mut Vec<Vec<f64>>> {
        match &mut self.backend {
            CountMinBackend::Legacy(m) => Some(m),
            CountMinBackend::Sketchlib(_) => None,
        }
    }

    /// Construct from a legacy matrix (used by deserialization and query engine).
    pub fn from_legacy_matrix(sketch: Vec<Vec<f64>>, row_num: usize, col_num: usize) -> Self {
        let backend = if use_sketchlib_for_count_min() {
            CountMinBackend::Sketchlib(sketchlib_cms_from_matrix(row_num, col_num, &sketch))
        } else {
            CountMinBackend::Legacy(sketch)
        };
        Self {
            row_num,
            col_num,
            backend,
        }
    }

    pub fn update(&mut self, key: &str, value: f64) {
        match &mut self.backend {
            CountMinBackend::Legacy(sketch) => {
                let key_bytes = key.as_bytes();
                for (i, row) in sketch.iter_mut().enumerate().take(self.row_num) {
                    let hash_value = xxh32(key_bytes, i as u32);
                    let col_index = (hash_value as usize) % self.col_num;
                    row[col_index] += value;
                }
            }
            CountMinBackend::Sketchlib(s) => {
                sketchlib_cms_update(s, key, value);
            }
        }
    }

    pub fn query_key(&self, key: &str) -> f64 {
        match &self.backend {
            CountMinBackend::Legacy(sketch) => {
                let key_bytes = key.as_bytes();
                let mut min_value = f64::MAX;
                for (i, row) in sketch.iter().enumerate().take(self.row_num) {
                    let hash_value = xxh32(key_bytes, i as u32);
                    let col_index = (hash_value as usize) % self.col_num;
                    min_value = min_value.min(row[col_index]);
                }
                min_value
            }
            CountMinBackend::Sketchlib(s) => sketchlib_cms_query(s, key),
        }
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

        if use_sketchlib_for_count_min() {
            let mut sketchlib_inners: Vec<SketchlibCms> = Vec::with_capacity(accumulators.len());
            for acc in accumulators {
                let matrix = acc.sketch();
                let inner = sketchlib_cms_from_matrix(acc.row_num, acc.col_num, &matrix);
                sketchlib_inners.push(inner);
            }
            let merged_sketchlib = sketchlib_inners
                .into_iter()
                .reduce(|mut lhs: SketchlibCms, rhs: SketchlibCms| {
                    lhs.merge(&rhs);
                    lhs
                })
                .ok_or("No accumulators to merge")?;

            let sketch = matrix_from_sketchlib_cms(&merged_sketchlib);
            let row_num = sketch.len();
            let col_num = sketch.first().map(|r| r.len()).unwrap_or(0);

            Ok(Self {
                row_num,
                col_num,
                backend: CountMinBackend::Sketchlib(merged_sketchlib),
            })
        } else {
            let mut merged = accumulators[0].clone();
            for acc in &accumulators[1..] {
                let acc_matrix = acc.sketch();
                if let CountMinBackend::Legacy(merged_matrix) = &mut merged.backend {
                    for (merged_row, acc_row) in merged_matrix.iter_mut().zip(acc_matrix.iter()) {
                        for (m_cell, a_cell) in merged_row.iter_mut().zip(acc_row.iter()) {
                            *m_cell += *a_cell;
                        }
                    }
                }
            }
            Ok(merged)
        }
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

        if use_sketchlib_for_count_min() {
            let mut sketchlib_inners: Vec<SketchlibCms> = Vec::with_capacity(accumulators.len());
            for acc in accumulators {
                let acc_matrix = acc.sketch();
                let matrix_has_values = acc_matrix
                    .iter()
                    .any(|row: &Vec<f64>| row.iter().any(|&v| v != 0.0));

                let inner = if matrix_has_values {
                    sketchlib_cms_from_matrix(acc.row_num, acc.col_num, &acc_matrix)
                } else if let CountMinBackend::Sketchlib(s) = &acc.backend {
                    s.clone()
                } else {
                    sketchlib_cms_from_matrix(acc.row_num, acc.col_num, &acc_matrix)
                };

                sketchlib_inners.push(inner);
            }

            let merged_sketchlib = sketchlib_inners
                .into_iter()
                .reduce(|mut lhs: SketchlibCms, rhs: SketchlibCms| {
                    lhs.merge(&rhs);
                    lhs
                })
                .ok_or("No accumulators to merge")?;

            let sketch = matrix_from_sketchlib_cms(&merged_sketchlib);
            let r = sketch.len();
            let c = sketch.first().map(|row| row.len()).unwrap_or(0);

            Ok(Self {
                row_num: r,
                col_num: c,
                backend: CountMinBackend::Sketchlib(merged_sketchlib),
            })
        } else {
            let mut merged = Self::new(row_num, col_num);
            if let CountMinBackend::Legacy(ref mut merged_sketch) = merged.backend {
                for acc in accumulators {
                    let acc_matrix = acc.sketch();
                    for (merged_row, acc_row) in merged_sketch.iter_mut().zip(acc_matrix.iter()) {
                        for (m_cell, a_cell) in merged_row.iter_mut().zip(acc_row.iter()) {
                            *m_cell += *a_cell;
                        }
                    }
                }
            }
            Ok(merged)
        }
    }

    /// Serialize to MessagePack — matches the Arroyo UDF wire format exactly.
    pub fn serialize_msgpack(&self) -> Vec<u8> {
        let sketch = self.sketch();
        let wire = WireFormat {
            sketch,
            row_num: self.row_num,
            col_num: self.col_num,
        };

        let mut buf = Vec::new();
        wire.serialize(&mut rmp_serde::Serializer::new(&mut buf))
            .unwrap();
        buf
    }

    /// Deserialize from MessagePack produced by the Arroyo UDF.
    pub fn deserialize_msgpack(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let wire: WireFormat =
            rmp_serde::from_slice(buffer).map_err(|e| -> Box<dyn std::error::Error> {
                format!("Failed to deserialize CountMinSketch from MessagePack: {e}").into()
            })?;

        let backend = if use_sketchlib_for_count_min() {
            CountMinBackend::Sketchlib(sketchlib_cms_from_matrix(
                wire.row_num,
                wire.col_num,
                &wire.sketch,
            ))
        } else {
            CountMinBackend::Legacy(wire.sketch)
        };

        Ok(Self {
            row_num: wire.row_num,
            col_num: wire.col_num,
            backend,
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
        let sketch = cms.sketch();
        assert_eq!(sketch.len(), 4);
        assert_eq!(sketch[0].len(), 1000);

        // Check all values are initialized to 0
        for row in &sketch {
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
        // Use from_legacy_matrix so the test works regardless of sketchlib/legacy config
        let mut sketch1 = vec![vec![0.0; 3]; 2];
        sketch1[0][0] = 5.0;
        sketch1[1][2] = 10.0;
        let cms1 = CountMinSketch::from_legacy_matrix(sketch1, 2, 3);

        let mut sketch2 = vec![vec![0.0; 3]; 2];
        sketch2[0][0] = 3.0;
        sketch2[0][1] = 7.0;
        let cms2 = CountMinSketch::from_legacy_matrix(sketch2, 2, 3);

        let merged = CountMinSketch::merge(vec![cms1, cms2]).unwrap();
        let merged_sketch = merged.sketch();

        assert_eq!(merged_sketch[0][0], 8.0); // 5 + 3
        assert_eq!(merged_sketch[0][1], 7.0); // 0 + 7
        assert_eq!(merged_sketch[1][2], 10.0); // 10 + 0
    }

    #[test]
    fn test_count_min_sketch_merge_dimension_mismatch() {
        let cms1 = CountMinSketch::new(2, 3);
        let cms2 = CountMinSketch::new(3, 3);
        assert!(CountMinSketch::merge(vec![cms1, cms2]).is_err());
    }

    #[test]
    fn test_count_min_sketch_msgpack_round_trip() {
        let mut cms = CountMinSketch::new(4, 256);
        cms.update("apple", 5.0);
        cms.update("banana", 3.0);
        cms.update("apple", 2.0); // total "apple" = 7

        let bytes = cms.serialize_msgpack();
        let deserialized = CountMinSketch::deserialize_msgpack(&bytes).unwrap();

        assert_eq!(deserialized.row_num, 4);
        assert_eq!(deserialized.col_num, 256);
        assert!(deserialized.query_key("apple") >= 7.0);
        assert!(deserialized.query_key("banana") >= 3.0);
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
