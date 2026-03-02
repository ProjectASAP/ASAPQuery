// Adapted from QueryEngineRust/src/precompute_operators/hydra_kll_accumulator.rs
// Changes:
//   - Renamed HydraKllSketchAccumulator -> HydraKllSketch
//   - KllSketchData import replaced by crate::kll::{KllSketch, KllSketchData}
//   - Inner cells are KllSketch instead of DatasketchesKLLAccumulator
//   - update() takes &str instead of &KeyByLabelValues
//   - query_key() takes &str; renamed to query()
//   - serialize_to_bytes (trait) -> serialize_msgpack (inherent method)
//   - deserialize_from_bytes_arroyo -> deserialize_msgpack
//   - merge_accumulators -> merge
//   - Removed: deserialize_from_bytes (stub, stays in QE)
//   - Removed: AggregateCore, SerializableToSink, MergeableAccumulator, MultipleSubpopulationAggregate impls
//   - Removed: base64, serde_json imports (QE-specific)
//   - Added: aggregate_hydrakll() one-shot helper

use crate::kll::{KllSketch, KllSketchData};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use xxhash_rust::xxh32::xxh32;

#[derive(Serialize, Deserialize)]
struct HydraKllSketchData {
    row_num: usize,
    col_num: usize,
    sketches: Vec<Vec<KllSketchData>>,
}

#[derive(Debug, Clone)]
pub struct HydraKllSketch {
    pub sketch: Vec<Vec<KllSketch>>,
    pub row_num: usize,
    pub col_num: usize,
}

impl HydraKllSketch {
    pub fn new(row_num: usize, col_num: usize, k: u16) -> Self {
        let sketch = vec![vec![KllSketch::new(k); col_num]; row_num];
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
            self.sketch[i][col_index].update(value);
        }
    }

    pub fn query(&self, key: &str, quantile: f64) -> f64 {
        let key_bytes = key.as_bytes();
        let mut quantiles = Vec::with_capacity(self.row_num);

        for i in 0..self.row_num {
            let hash_value = xxh32(key_bytes, i as u32);
            let col_index = (hash_value as usize) % self.col_num;
            quantiles.push(self.sketch[i][col_index].get_quantile(quantile));
        }

        if quantiles.is_empty() {
            return 0.0;
        }

        quantiles.sort_by(|a, b| match a.partial_cmp(b) {
            Some(ordering) => ordering,
            None => Ordering::Equal,
        });

        let mid = quantiles.len() / 2;
        if quantiles.len() % 2 == 0 {
            (quantiles[mid - 1] + quantiles[mid]) / 2.0
        } else {
            quantiles[mid]
        }
    }

    pub fn merge(
        accumulators: Vec<Self>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        // Check dimensions match
        let row_num = accumulators[0].row_num;
        let col_num = accumulators[0].col_num;
        for acc in &accumulators {
            if acc.row_num != row_num || acc.col_num != col_num {
                return Err(
                    "Cannot merge HydraKllSketch accumulators with different dimensions".into(),
                );
            }
        }

        // Transpose Vec<HydraKllSketch> into Vec<Vec<Vec<KllSketch>>> indexed [row][col][acc],
        // consuming the owned accumulators so no per-cell clones are needed.
        let mut by_cell: Vec<Vec<Vec<KllSketch>>> = (0..row_num)
            .map(|_| (0..col_num).map(|_| Vec::new()).collect())
            .collect();
        for acc in accumulators {
            for (i, row) in acc.sketch.into_iter().enumerate() {
                for (j, cell) in row.into_iter().enumerate() {
                    by_cell[i][j].push(cell);
                }
            }
        }

        // Merge each cell independently
        let mut merged_sketch = Vec::with_capacity(row_num);
        for row in by_cell {
            let mut merged_row = Vec::with_capacity(col_num);
            for cells in row {
                merged_row.push(KllSketch::merge(cells)?);
            }
            merged_sketch.push(merged_row);
        }

        Ok(HydraKllSketch {
            sketch: merged_sketch,
            row_num,
            col_num,
        })
    }

    /// Serialize to MessagePack — matches the Arroyo UDF wire format exactly.
    pub fn serialize_msgpack(&self) -> Vec<u8> {
        let mut sketches = Vec::with_capacity(self.row_num);
        for row in &self.sketch {
            let mut row_data = Vec::with_capacity(self.col_num);
            for cell in row {
                // Serialize each KllSketch to KllSketchData
                let cell_bytes = cell.serialize_msgpack();
                let kll_data: KllSketchData = rmp_serde::from_slice(&cell_bytes)
                    .expect("Failed to deserialize KllSketchData from cell");
                row_data.push(kll_data);
            }
            sketches.push(row_data);
        }

        let serialized = HydraKllSketchData {
            row_num: self.row_num,
            col_num: self.col_num,
            sketches,
        };

        let mut buf = Vec::new();
        rmp_serde::encode::write(&mut buf, &serialized).unwrap();
        buf
    }

    /// Deserialize from MessagePack produced by the Arroyo UDF.
    pub fn deserialize_msgpack(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let deserialized_sketch_data: HydraKllSketchData = rmp_serde::from_slice(buffer)
            .map_err(|e| format!("Failed to deserialize HydraKLL from MessagePack: {e}"))?;

        if deserialized_sketch_data.sketches.len() != deserialized_sketch_data.row_num {
            return Err(format!(
                "HydraKLL row count mismatch: expected {}, got {}",
                deserialized_sketch_data.row_num,
                deserialized_sketch_data.sketches.len()
            )
            .into());
        }

        let mut sketch: Vec<Vec<KllSketch>> = Vec::with_capacity(deserialized_sketch_data.row_num);

        for (row_idx, row) in deserialized_sketch_data.sketches.into_iter().enumerate() {
            if row.len() != deserialized_sketch_data.col_num {
                return Err(format!(
                    "HydraKLL column count mismatch in row {}: expected {}, got {}",
                    row_idx,
                    deserialized_sketch_data.col_num,
                    row.len()
                )
                .into());
            }

            let mut accum_row: Vec<KllSketch> =
                Vec::with_capacity(deserialized_sketch_data.col_num);
            for cell in row {
                let cell_bytes = rmp_serde::to_vec(&cell)
                    .map_err(|e| format!("Failed to serialize nested KLL sketch: {e}"))?;
                let kll = KllSketch::deserialize_msgpack(&cell_bytes)?;
                accum_row.push(kll);
            }

            sketch.push(accum_row);
        }

        Ok(Self {
            sketch,
            row_num: deserialized_sketch_data.row_num,
            col_num: deserialized_sketch_data.col_num,
        })
    }

    /// One-shot aggregation for the Arroyo UDAF call pattern.
    pub fn aggregate_hydrakll(
        row_num: usize,
        col_num: usize,
        k: u16,
        keys: &[&str],
        values: &[f64],
    ) -> Option<Vec<u8>> {
        if keys.is_empty() {
            return None;
        }
        let mut sketch = Self::new(row_num, col_num, k);
        for (key, &value) in keys.iter().zip(values.iter()) {
            sketch.update(key, value);
        }
        Some(sketch.serialize_msgpack())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_creation() {
        let h = HydraKllSketch::new(2, 3, 200);
        assert_eq!(h.row_num, 2);
        assert_eq!(h.col_num, 3);
        assert_eq!(h.sketch.len(), 2);
        assert_eq!(h.sketch[0].len(), 3);
    }

    #[test]
    fn test_update_and_query() {
        let mut h = HydraKllSketch::new(2, 10, 200);
        h.update("key1", 5.0);
        h.update("key1", 10.0);
        // With 2 values, median quantile should be between them
        let q = h.query("key1", 0.5);
        assert!(q >= 0.0);
    }

    #[test]
    fn test_merge() {
        let mut h1 = HydraKllSketch::new(2, 5, 200);
        let mut h2 = HydraKllSketch::new(2, 5, 200);

        for i in 1..=5 {
            h1.update("key1", i as f64);
        }
        for i in 6..=10 {
            h2.update("key1", i as f64);
        }

        let merged = HydraKllSketch::merge(vec![h1, h2]).unwrap();
        assert_eq!(merged.row_num, 2);
        assert_eq!(merged.col_num, 5);
    }

    #[test]
    fn test_merge_dimension_mismatch() {
        let h1 = HydraKllSketch::new(2, 5, 200);
        let h2 = HydraKllSketch::new(3, 5, 200);
        assert!(HydraKllSketch::merge(vec![h1, h2]).is_err());
    }

    #[test]
    fn test_msgpack_round_trip() {
        let mut h = HydraKllSketch::new(2, 3, 200);
        h.update("key1", 5.0);
        h.update("key2", 10.0);

        let bytes = h.serialize_msgpack();
        let deserialized = HydraKllSketch::deserialize_msgpack(&bytes).unwrap();

        assert_eq!(deserialized.row_num, 2);
        assert_eq!(deserialized.col_num, 3);
    }

    #[test]
    fn test_aggregate_hydrakll() {
        let keys = ["a", "b", "a"];
        let values = [1.0, 2.0, 3.0];
        let bytes = HydraKllSketch::aggregate_hydrakll(2, 5, 200, &keys, &values).unwrap();
        let h = HydraKllSketch::deserialize_msgpack(&bytes).unwrap();
        assert_eq!(h.row_num, 2);
        assert_eq!(h.col_num, 5);
    }

    #[test]
    fn test_aggregate_hydrakll_empty() {
        assert!(HydraKllSketch::aggregate_hydrakll(2, 5, 200, &[], &[]).is_none());
    }
}
