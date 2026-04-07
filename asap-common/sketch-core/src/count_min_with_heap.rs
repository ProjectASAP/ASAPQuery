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
//   - Refactored to enum-based backend (Legacy vs Sketchlib)
//
// NOTE (bug, do not fix): QueryEngineRust uses xxhash-rust::xxh32; the Arroyo template uses
// twox-hash::XxHash32. Bucket assignments differ, so query results will be wrong until the
// hash crate mismatch is resolved. Tracked separately.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use xxhash_rust::xxh32::xxh32;

use crate::config::use_sketchlib_for_count_min_with_heap;
use crate::count_min_with_heap_sketchlib::{
    heap_to_wire, matrix_from_sketchlib_cms_heap, new_sketchlib_cms_heap,
    sketchlib_cms_heap_from_matrix_and_heap, sketchlib_cms_heap_query, sketchlib_cms_heap_update,
    SketchlibCMSHeap, WireHeapItem,
};

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

/// Backend implementation for Count-Min Sketch with Heap. Only one is active at a time.
pub enum CountMinWithHeapBackend {
    /// Legacy implementation: matrix + local heap.
    Legacy {
        sketch: Vec<Vec<f64>>,
        heap: Vec<HeapItem>,
    },
    /// asap_sketchlib CMSHeap implementation.
    Sketchlib(SketchlibCMSHeap),
}

impl std::fmt::Debug for CountMinWithHeapBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CountMinWithHeapBackend::Legacy { sketch, heap } => f
                .debug_struct("Legacy")
                .field("sketch", sketch)
                .field("heap", heap)
                .finish(),
            CountMinWithHeapBackend::Sketchlib(_) => write!(f, "Sketchlib(..)"),
        }
    }
}

/// Count-Min Sketch with Heap for top-k tracking.
/// Combines probabilistic frequency counting with efficient top-k maintenance.
pub struct CountMinSketchWithHeap {
    pub row_num: usize,
    pub col_num: usize,
    pub heap_size: usize,
    pub backend: CountMinWithHeapBackend,
}

impl std::fmt::Debug for CountMinSketchWithHeap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountMinSketchWithHeap")
            .field("row_num", &self.row_num)
            .field("col_num", &self.col_num)
            .field("heap_size", &self.heap_size)
            .field("backend", &self.backend)
            .finish()
    }
}

impl Clone for CountMinSketchWithHeap {
    fn clone(&self) -> Self {
        let backend = match &self.backend {
            CountMinWithHeapBackend::Legacy { sketch, heap } => CountMinWithHeapBackend::Legacy {
                sketch: sketch.clone(),
                heap: heap.clone(),
            },
            CountMinWithHeapBackend::Sketchlib(cms_heap) => {
                let sketch = matrix_from_sketchlib_cms_heap(cms_heap);
                let heap_items: Vec<HeapItem> = heap_to_wire(cms_heap)
                    .into_iter()
                    .map(|w| HeapItem {
                        key: w.key,
                        value: w.value,
                    })
                    .collect();
                let wire_ref: Vec<WireHeapItem> = heap_items
                    .iter()
                    .map(|h| WireHeapItem {
                        key: h.key.clone(),
                        value: h.value,
                    })
                    .collect();
                CountMinWithHeapBackend::Sketchlib(sketchlib_cms_heap_from_matrix_and_heap(
                    self.row_num,
                    self.col_num,
                    self.heap_size,
                    &sketch,
                    &wire_ref,
                ))
            }
        };
        Self {
            row_num: self.row_num,
            col_num: self.col_num,
            heap_size: self.heap_size,
            backend,
        }
    }
}

impl CountMinSketchWithHeap {
    pub fn new(row_num: usize, col_num: usize, heap_size: usize) -> Self {
        let backend = if use_sketchlib_for_count_min_with_heap() {
            CountMinWithHeapBackend::Sketchlib(new_sketchlib_cms_heap(row_num, col_num, heap_size))
        } else {
            CountMinWithHeapBackend::Legacy {
                sketch: vec![vec![0.0; col_num]; row_num],
                heap: Vec::new(),
            }
        };
        Self {
            row_num,
            col_num,
            heap_size,
            backend,
        }
    }

    /// Create from legacy matrix and heap (e.g. from JSON deserialization).
    pub fn from_legacy_matrix(
        sketch: Vec<Vec<f64>>,
        topk_heap: Vec<HeapItem>,
        row_num: usize,
        col_num: usize,
        heap_size: usize,
    ) -> Self {
        Self {
            row_num,
            col_num,
            heap_size,
            backend: CountMinWithHeapBackend::Legacy {
                sketch,
                heap: topk_heap,
            },
        }
    }

    /// Mutable reference to the sketch matrix. Only valid for Legacy backend.
    pub fn sketch_mut(&mut self) -> Option<&mut Vec<Vec<f64>>> {
        match &mut self.backend {
            CountMinWithHeapBackend::Legacy { sketch, .. } => Some(sketch),
            CountMinWithHeapBackend::Sketchlib(_) => None,
        }
    }

    /// Get the top-k heap items (works for both backends).
    pub fn topk_heap_items(&self) -> Vec<HeapItem> {
        match &self.backend {
            CountMinWithHeapBackend::Legacy { heap, .. } => heap.clone(),
            CountMinWithHeapBackend::Sketchlib(cms_heap) => heap_to_wire(cms_heap)
                .into_iter()
                .map(|w| HeapItem {
                    key: w.key,
                    value: w.value,
                })
                .collect(),
        }
    }

    /// Get the sketch matrix (works for both backends).
    pub fn sketch_matrix(&self) -> Vec<Vec<f64>> {
        match &self.backend {
            CountMinWithHeapBackend::Legacy { sketch, .. } => sketch.clone(),
            CountMinWithHeapBackend::Sketchlib(cms_heap) => {
                matrix_from_sketchlib_cms_heap(cms_heap)
            }
        }
    }

    pub fn update(&mut self, key: &str, value: f64) {
        match &mut self.backend {
            CountMinWithHeapBackend::Legacy { sketch, heap } => {
                let key_bytes = key.as_bytes();
                for (i, row) in sketch.iter_mut().enumerate().take(self.row_num) {
                    let hash_value = xxh32(key_bytes, i as u32);
                    let col_index = (hash_value as usize) % self.col_num;
                    row[col_index] += value;
                }
                Self::insert_or_update_heap_inline(heap, key, value, self.heap_size);
            }
            CountMinWithHeapBackend::Sketchlib(cms_heap) => {
                sketchlib_cms_heap_update(cms_heap, key, value);
            }
        }
    }

    fn insert_or_update_heap_inline(
        heap: &mut Vec<HeapItem>,
        key: &str,
        value: f64,
        heap_size: usize,
    ) {
        if let Some(item) = heap.iter_mut().find(|i| i.key == key) {
            item.value += value;
        } else if heap.len() < heap_size {
            heap.push(HeapItem {
                key: key.to_string(),
                value,
            });
        } else if let Some(min_item) = heap.iter_mut().min_by(|a, b| {
            a.value
                .partial_cmp(&b.value)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            if value > min_item.value {
                *min_item = HeapItem {
                    key: key.to_string(),
                    value,
                };
            }
        }
    }

    pub fn query_key(&self, key: &str) -> f64 {
        match &self.backend {
            CountMinWithHeapBackend::Legacy { sketch, .. } => {
                let key_bytes = key.as_bytes();
                let mut min_value = f64::MAX;
                for (i, row) in sketch.iter().enumerate().take(self.row_num) {
                    let hash_value = xxh32(key_bytes, i as u32);
                    let col_index = (hash_value as usize) % self.col_num;
                    min_value = min_value.min(row[col_index]);
                }
                min_value
            }
            CountMinWithHeapBackend::Sketchlib(cms_heap) => sketchlib_cms_heap_query(cms_heap, key),
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

        let min_heap_size = accumulators
            .iter()
            .map(|acc| acc.heap_size)
            .min()
            .unwrap_or(0);

        let mut all_keys: HashSet<String> = HashSet::new();
        for acc in &accumulators {
            for item in acc.topk_heap_items() {
                all_keys.insert(item.key);
            }
        }

        match &accumulators[0].backend {
            CountMinWithHeapBackend::Sketchlib(_) => {
                let mut sketchlib_cms_heaps: Vec<SketchlibCMSHeap> =
                    Vec::with_capacity(accumulators.len());
                for acc in accumulators {
                    let (sketch, heap) = match &acc.backend {
                        CountMinWithHeapBackend::Legacy { sketch, heap } => {
                            (sketch.clone(), heap.clone())
                        }
                        CountMinWithHeapBackend::Sketchlib(cms_heap) => (
                            matrix_from_sketchlib_cms_heap(cms_heap),
                            heap_to_wire(cms_heap)
                                .into_iter()
                                .map(|w| HeapItem {
                                    key: w.key,
                                    value: w.value,
                                })
                                .collect(),
                        ),
                    };
                    let wire_heap: Vec<WireHeapItem> = heap
                        .iter()
                        .map(|h| WireHeapItem {
                            key: h.key.clone(),
                            value: h.value,
                        })
                        .collect();
                    sketchlib_cms_heaps.push(sketchlib_cms_heap_from_matrix_and_heap(
                        acc.row_num,
                        acc.col_num,
                        acc.heap_size,
                        &sketch,
                        &wire_heap,
                    ));
                }

                let merged_sketchlib = sketchlib_cms_heaps
                    .into_iter()
                    .reduce(|mut lhs, rhs| {
                        lhs.merge(&rhs);
                        lhs
                    })
                    .ok_or("No accumulators to merge")?;

                let _merged_sketch = matrix_from_sketchlib_cms_heap(&merged_sketchlib);
                let _heap_items: Vec<HeapItem> = heap_to_wire(&merged_sketchlib)
                    .into_iter()
                    .map(|w| HeapItem {
                        key: w.key,
                        value: w.value,
                    })
                    .collect();

                Ok(CountMinSketchWithHeap {
                    row_num,
                    col_num,
                    heap_size: min_heap_size,
                    backend: CountMinWithHeapBackend::Sketchlib(merged_sketchlib),
                })
            }
            CountMinWithHeapBackend::Legacy { .. } => {
                let mut merged_sketch = vec![vec![0.0; col_num]; row_num];
                for acc in &accumulators {
                    let sketch = match &acc.backend {
                        CountMinWithHeapBackend::Legacy { sketch, .. } => sketch,
                        CountMinWithHeapBackend::Sketchlib(_) => {
                            return Err(
                                "Cannot mix Legacy and Sketchlib backends when merging".into()
                            );
                        }
                    };
                    for (i, row) in merged_sketch.iter_mut().enumerate() {
                        for (j, cell) in row.iter_mut().enumerate() {
                            *cell += sketch[i][j];
                        }
                    }
                }

                let temp_merged = Self::from_legacy_matrix(
                    merged_sketch.clone(),
                    Vec::new(),
                    row_num,
                    col_num,
                    min_heap_size,
                );

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

                heap_items.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap());
                heap_items.truncate(min_heap_size);

                Ok(CountMinSketchWithHeap {
                    row_num,
                    col_num,
                    heap_size: min_heap_size,
                    backend: CountMinWithHeapBackend::Legacy {
                        sketch: merged_sketch,
                        heap: heap_items,
                    },
                })
            }
        }
    }

    pub fn serialize_msgpack(&self) -> Vec<u8> {
        let (sketch, topk_heap) = (self.sketch_matrix(), self.topk_heap_items());

        let serialized = CountMinSketchWithHeapSerialized {
            sketch: CmsData {
                sketch,
                row_num: self.row_num,
                col_num: self.col_num,
            },
            topk_heap,
            heap_size: self.heap_size,
        };

        let mut buf = Vec::new();
        serialized
            .serialize(&mut rmp_serde::Serializer::new(&mut buf))
            .unwrap();
        buf
    }

    pub fn deserialize_msgpack(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let serialized: CountMinSketchWithHeapSerialized =
            rmp_serde::from_slice(buffer).map_err(|e| {
                format!("Failed to deserialize CountMinSketchWithHeap from MessagePack: {e}")
            })?;

        let mut sorted_topk_heap = serialized.topk_heap;
        sorted_topk_heap.sort_by(|a, b| b.value.partial_cmp(&a.value).unwrap());

        let backend = if use_sketchlib_for_count_min_with_heap() {
            let wire_heap: Vec<WireHeapItem> = sorted_topk_heap
                .iter()
                .map(|h| WireHeapItem {
                    key: h.key.clone(),
                    value: h.value,
                })
                .collect();
            CountMinWithHeapBackend::Sketchlib(sketchlib_cms_heap_from_matrix_and_heap(
                serialized.sketch.row_num,
                serialized.sketch.col_num,
                serialized.heap_size,
                &serialized.sketch.sketch,
                &wire_heap,
            ))
        } else {
            CountMinWithHeapBackend::Legacy {
                sketch: serialized.sketch.sketch,
                heap: sorted_topk_heap,
            }
        };

        Ok(Self {
            row_num: serialized.sketch.row_num,
            col_num: serialized.sketch.col_num,
            heap_size: serialized.heap_size,
            backend,
        })
    }

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
        assert_eq!(cms.sketch_matrix().len(), 4);
        assert_eq!(cms.sketch_matrix()[0].len(), 1000);
        assert_eq!(cms.topk_heap_items().len(), 0);
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

        if let Some(sketch) = cms1.sketch_mut() {
            sketch[0][0] = 10.0;
            sketch[1][1] = 20.0;
        }
        if let Some(sketch) = cms2.sketch_mut() {
            sketch[0][0] = 5.0;
            sketch[1][1] = 15.0;
        }
        if let CountMinWithHeapBackend::Legacy { heap, .. } = &mut cms1.backend {
            heap.push(HeapItem {
                key: "key1".to_string(),
                value: 100.0,
            });
            heap.push(HeapItem {
                key: "key2".to_string(),
                value: 50.0,
            });
        }
        if let CountMinWithHeapBackend::Legacy { heap, .. } = &mut cms2.backend {
            heap.push(HeapItem {
                key: "key3".to_string(),
                value: 75.0,
            });
            heap.push(HeapItem {
                key: "key1".to_string(),
                value: 80.0,
            });
        }

        let merged = CountMinSketchWithHeap::merge(vec![cms1, cms2]).unwrap();

        assert_eq!(merged.sketch_matrix()[0][0], 15.0);
        assert_eq!(merged.sketch_matrix()[1][1], 35.0);
        assert_eq!(merged.heap_size, 3);
        assert!(merged.topk_heap_items().len() <= 3);
    }

    #[test]
    fn test_merge_dimension_mismatch() {
        let cms1 = CountMinSketchWithHeap::new(2, 10, 5);
        let cms2 = CountMinSketchWithHeap::new(3, 10, 5);
        assert!(CountMinSketchWithHeap::merge(vec![cms1, cms2]).is_err());
    }

    #[test]
    fn test_msgpack_round_trip() {
        let mut cms = CountMinSketchWithHeap::new(4, 128, 3);
        cms.update("hot", 100.0);
        cms.update("cold", 1.0);

        let bytes = cms.serialize_msgpack();
        let deserialized = CountMinSketchWithHeap::deserialize_msgpack(&bytes).unwrap();

        assert_eq!(deserialized.row_num, 4);
        assert_eq!(deserialized.col_num, 128);
        assert_eq!(deserialized.heap_size, 3);
        assert!(!deserialized.topk_heap_items().is_empty());
        assert_eq!(deserialized.topk_heap_items()[0].key, "hot");
        assert!(deserialized.topk_heap_items()[0].value >= 100.0);
        assert!(deserialized.query_key("hot") >= 100.0);
        assert!(deserialized.query_key("cold") >= 1.0);
    }

    #[test]
    fn test_aggregate_topk() {
        let keys = ["a", "b", "a", "c"];
        let values = [1.0, 2.0, 3.0, 0.5];
        let bytes = CountMinSketchWithHeap::aggregate_topk(4, 100, 2, &keys, &values).unwrap();
        let cms = CountMinSketchWithHeap::deserialize_msgpack(&bytes).unwrap();
        assert_eq!(cms.heap_size, 2);
        assert!(cms.topk_heap_items().len() <= 2);
    }

    #[test]
    fn test_aggregate_topk_empty() {
        assert!(CountMinSketchWithHeap::aggregate_topk(4, 100, 10, &[], &[]).is_none());
    }
}
