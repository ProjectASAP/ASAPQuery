// Adapted from QueryEngineRust/src/precompute_operators/count_min_sketch_with_heap_accumulator.rs
// Changes:
//   - Renamed CountMinSketchWithHeapAccumulator -> CountMinSketchWithHeap
//   - Inner CmsData helper renamed to avoid name collision with count_min::CountMinSketch
//   - update() takes &str instead of &KeyByLabelValues
//   - query_key() takes &str
//   - serialize_to_bytes (trait) -> serialize_msgpack (inherent method)
//   - deserialize_from_bytes_arroyo -> deserialize_msgpack
//   - merge_accumulators -> merge
//   - Removed: deserialize_from_json, deserialize_from_bytes (legacy QE formats, stay in QE)
//   - Removed: AggregateCore, SerializableToSink, MergeableAccumulator, MultipleSubpopulationAggregate impls
//   - Removed: get_topk_keys (returns KeyByLabelValues — QE-specific)
//   - Added: insert_or_update_heap helper, aggregate_topk() one-shot helper
//
// NOTE (bug, do not fix): QueryEngineRust uses xxhash-rust::xxh32; the Arroyo template uses
// twox-hash::XxHash32. Bucket assignments differ, so query results will be wrong until the
// hash crate mismatch is resolved. Tracked separately.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use xxhash_rust::xxh32::xxh32;

/// Item in the top-k heap representing a key-value pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeapItem {
    pub key: String,
    pub value: f64,
}

/// Helper struct matching Arroyo's nested serialization format (inner CMS).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CmsData {
    sketch: Vec<Vec<f64>>,
    row_num: usize,
    col_num: usize,
}

/// Helper struct matching Arroyo's serialization format (outer wrapper).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CountMinSketchWithHeapSerialized {
    sketch: CmsData,
    topk_heap: Vec<HeapItem>,
    heap_size: usize,
}

/// Count-Min Sketch with Heap for top-k tracking.
/// Combines probabilistic frequency counting with efficient top-k maintenance.
#[derive(Debug, Clone)]
pub struct CountMinSketchWithHeap {
    pub sketch: Vec<Vec<f64>>,
    pub row_num: usize,
    pub col_num: usize,
    pub topk_heap: Vec<HeapItem>,
    pub heap_size: usize,
}

impl CountMinSketchWithHeap {
    pub fn new(row_num: usize, col_num: usize, heap_size: usize) -> Self {
        let sketch = vec![vec![0.0; col_num]; row_num];
        Self {
            sketch,
            row_num,
            col_num,
            topk_heap: Vec::new(),
            heap_size,
        }
    }

    pub fn update(&mut self, key: &str, value: f64) {
        let key_bytes = key.as_bytes();
        for i in 0..self.row_num {
            let hash_value = xxh32(key_bytes, i as u32);
            let col_index = (hash_value as usize) % self.col_num;
            self.sketch[i][col_index] += value;
        }
        self.insert_or_update_heap(key, value);
    }

    fn insert_or_update_heap(&mut self, key: &str, value: f64) {
        if let Some(item) = self.topk_heap.iter_mut().find(|i| i.key == key) {
            item.value += value;
        } else if self.topk_heap.len() < self.heap_size {
            self.topk_heap.push(HeapItem {
                key: key.to_string(),
                value,
            });
        } else if let Some(min_item) = self
            .topk_heap
            .iter_mut()
            .min_by(|a, b| a.value.partial_cmp(&b.value).unwrap())
        {
            if value > min_item.value {
                *min_item = HeapItem {
                    key: key.to_string(),
                    value,
                };
            }
        }
    }

    pub fn query_key(&self, key: &str) -> f64 {
        let key_bytes = key.as_bytes();
        let mut min_value = f64::MAX;
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
                    "Cannot merge CountMinSketchWithHeap accumulators with different dimensions"
                        .into(),
                );
            }
        }

        // Merge the Count-Min Sketch tables element-wise
        let mut merged_sketch = vec![vec![0.0; col_num]; row_num];
        for acc in &accumulators {
            for (i, row) in merged_sketch.iter_mut().enumerate() {
                for (j, cell) in row.iter_mut().enumerate() {
                    *cell += acc.sketch[i][j];
                }
            }
        }

        // Find the minimum heap size across all accumulators
        let min_heap_size = accumulators
            .iter()
            .map(|acc| acc.heap_size)
            .min()
            .unwrap_or(0);

        // Enumerate all unique keys from all heaps
        let mut all_keys: HashSet<String> = HashSet::new();
        for acc in &accumulators {
            for item in &acc.topk_heap {
                all_keys.insert(item.key.clone());
            }
        }

        // Create a temporary merged accumulator to query frequencies
        let temp_merged = CountMinSketchWithHeap {
            sketch: merged_sketch.clone(),
            row_num,
            col_num,
            topk_heap: Vec::new(),
            heap_size: min_heap_size,
        };

        // Query the merged CMS for each key and build heap items
        let mut heap_items: Vec<HeapItem> = all_keys
            .into_iter()
            .map(|key_str| {
                let frequency = temp_merged.query_key(&key_str);
                HeapItem {
                    key: key_str,
                    value: frequency,
                }
            })
            .collect();

        // Sort by frequency (descending) and take top min_heap_size items
        heap_items.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap());
        heap_items.truncate(min_heap_size);

        Ok(CountMinSketchWithHeap {
            sketch: merged_sketch,
            row_num,
            col_num,
            topk_heap: heap_items,
            heap_size: min_heap_size,
        })
    }

    /// Serialize to MessagePack — matches the Arroyo UDF wire format exactly.
    pub fn serialize_msgpack(&self) -> Vec<u8> {
        // Match Arroyo UDF: serialize with nested MessagePack format
        let serialized = CountMinSketchWithHeapSerialized {
            sketch: CmsData {
                sketch: self.sketch.clone(),
                row_num: self.row_num,
                col_num: self.col_num,
            },
            topk_heap: self.topk_heap.clone(),
            heap_size: self.heap_size,
        };

        let mut buf = Vec::new();
        serialized
            .serialize(&mut rmp_serde::Serializer::new(&mut buf))
            .unwrap();
        buf
    }

    /// Deserialize from MessagePack produced by the Arroyo UDF.
    pub fn deserialize_msgpack(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let serialized: CountMinSketchWithHeapSerialized =
            rmp_serde::from_slice(buffer).map_err(|e| {
                format!("Failed to deserialize CountMinSketchWithHeap from MessagePack: {e}")
            })?;

        // Sort the topk_heap by value from largest to smallest
        let mut sorted_topk_heap = serialized.topk_heap;
        // We must sort here since the vectorized heap does not guarantee order.
        sorted_topk_heap.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap());

        Ok(Self {
            sketch: serialized.sketch.sketch,
            row_num: serialized.sketch.row_num,
            col_num: serialized.sketch.col_num,
            topk_heap: sorted_topk_heap,
            heap_size: serialized.heap_size,
        })
    }

    /// One-shot aggregation for the Arroyo UDAF call pattern.
    pub fn aggregate_topk(
        row_num: usize,
        col_num: usize,
        heap_size: usize,
        keys: &[&str],
        values: &[f64],
    ) -> Option<Vec<u8>> {
        if keys.is_empty() {
            return None;
        }
        let mut sketch = Self::new(row_num, col_num, heap_size);
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
        let cms = CountMinSketchWithHeap::new(4, 1000, 20);
        assert_eq!(cms.row_num, 4);
        assert_eq!(cms.col_num, 1000);
        assert_eq!(cms.heap_size, 20);
        assert_eq!(cms.sketch.len(), 4);
        assert_eq!(cms.sketch[0].len(), 1000);
        assert_eq!(cms.topk_heap.len(), 0);
    }

    #[test]
    fn test_query_empty() {
        let cms = CountMinSketchWithHeap::new(2, 10, 5);
        assert_eq!(cms.query_key("anything"), 0.0);
    }

    #[test]
    fn test_merge() {
        let mut cms1 = CountMinSketchWithHeap::new(2, 10, 5);
        let mut cms2 = CountMinSketchWithHeap::new(2, 10, 3);

        cms1.sketch[0][0] = 10.0;
        cms1.sketch[1][1] = 20.0;
        cms2.sketch[0][0] = 5.0;
        cms2.sketch[1][1] = 15.0;

        cms1.topk_heap.push(HeapItem {
            key: "key1".to_string(),
            value: 100.0,
        });
        cms1.topk_heap.push(HeapItem {
            key: "key2".to_string(),
            value: 50.0,
        });
        cms2.topk_heap.push(HeapItem {
            key: "key3".to_string(),
            value: 75.0,
        });
        cms2.topk_heap.push(HeapItem {
            key: "key1".to_string(),
            value: 80.0,
        });

        let merged = CountMinSketchWithHeap::merge(vec![cms1, cms2]).unwrap();

        assert_eq!(merged.sketch[0][0], 15.0); // 10 + 5
        assert_eq!(merged.sketch[1][1], 35.0); // 20 + 15
        assert_eq!(merged.heap_size, 3); // min(5, 3)
        assert!(merged.topk_heap.len() <= 3);
    }

    #[test]
    fn test_merge_dimension_mismatch() {
        let cms1 = CountMinSketchWithHeap::new(2, 10, 5);
        let cms2 = CountMinSketchWithHeap::new(3, 10, 5);
        assert!(CountMinSketchWithHeap::merge(vec![cms1, cms2]).is_err());
    }

    #[test]
    fn test_msgpack_round_trip() {
        let mut cms = CountMinSketchWithHeap::new(2, 3, 5);
        cms.sketch[0][1] = 42.0;
        cms.sketch[1][2] = 100.0;
        cms.topk_heap.push(HeapItem {
            key: "test_key".to_string(),
            value: 99.0,
        });

        let bytes = cms.serialize_msgpack();
        let deserialized = CountMinSketchWithHeap::deserialize_msgpack(&bytes).unwrap();

        assert_eq!(deserialized.row_num, 2);
        assert_eq!(deserialized.col_num, 3);
        assert_eq!(deserialized.heap_size, 5);
        assert_eq!(deserialized.sketch[0][1], 42.0);
        assert_eq!(deserialized.sketch[1][2], 100.0);
        assert_eq!(deserialized.topk_heap.len(), 1);
        assert_eq!(deserialized.topk_heap[0].key, "test_key");
        assert_eq!(deserialized.topk_heap[0].value, 99.0);
    }

    #[test]
    fn test_aggregate_topk() {
        let keys = ["a", "b", "a", "c"];
        let values = [1.0, 2.0, 3.0, 0.5];
        let bytes = CountMinSketchWithHeap::aggregate_topk(4, 100, 2, &keys, &values).unwrap();
        let cms = CountMinSketchWithHeap::deserialize_msgpack(&bytes).unwrap();
        assert_eq!(cms.heap_size, 2);
        assert!(cms.topk_heap.len() <= 2);
    }

    #[test]
    fn test_aggregate_topk_empty() {
        assert!(CountMinSketchWithHeap::aggregate_topk(4, 100, 10, &[], &[]).is_none());
    }
}
