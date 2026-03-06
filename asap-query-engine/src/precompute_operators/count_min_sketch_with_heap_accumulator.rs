use crate::data_model::{
    AggregateCore, KeyByLabelValues, MergeableAccumulator, MultipleSubpopulationAggregate,
    SerializableToSink,
};
use serde_json::Value;
use sketch_core::count_min_with_heap::{CountMinSketchWithHeap, HeapItem};
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

/// Count-Min Sketch with Heap accumulator — wraps sketch_core::CountMinSketchWithHeap.
/// Core struct, update/merge/serde logic live in sketch-core.
/// This file retains QE-specific trait impls, legacy deserializers, and JSON output.
///
/// NOTE (bug, do not fix): QueryEngineRust uses xxhash-rust::xxh32; the Arroyo template uses
/// twox-hash::XxHash32. Bucket assignments differ. Tracked separately.
#[derive(Debug, Clone)]
pub struct CountMinSketchWithHeapAccumulator {
    pub inner: CountMinSketchWithHeap,
}

// Re-export HeapItem so existing code using CountMinSketchWithHeapAccumulator::HeapItem still works.
pub use sketch_core::count_min_with_heap::HeapItem as HeapItemReexport;

impl CountMinSketchWithHeapAccumulator {
    pub fn new(row_num: usize, col_num: usize, heap_size: usize) -> Self {
        Self {
            inner: CountMinSketchWithHeap::new(row_num, col_num, heap_size),
        }
    }

    pub fn query_key(&self, key: &KeyByLabelValues) -> f64 {
        let key_string = key.labels.join(";");
        self.inner.query_key(&key_string)
    }

    /// This function seems will never be used anymore. Keep it for possible future use.
    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let row_num = data["row_num"]
            .as_f64()
            .ok_or("Missing or invalid 'row_num' field")? as usize;
        let col_num = data["col_num"]
            .as_f64()
            .ok_or("Missing or invalid 'col_num' field")? as usize;
        let heap_size = data["heap_size"]
            .as_f64()
            .ok_or("Missing or invalid 'heap_size' field")? as usize;

        let sketch_data = data["sketch"]
            .as_array()
            .ok_or("Missing or invalid 'sketch' field")?;

        let mut sketch = Vec::new();
        for row in sketch_data {
            let row_array = row.as_array().ok_or("Invalid row in sketch data")?;
            let mut sketch_row = Vec::new();
            for cell in row_array {
                let value = cell.as_f64().ok_or("Invalid cell value in sketch data")?;
                sketch_row.push(value);
            }
            sketch.push(sketch_row);
        }

        let topk_heap_data = data["topk_heap"]
            .as_array()
            .ok_or("Missing or invalid 'topk_heap' field")?;

        let mut topk_heap = Vec::new();
        for item in topk_heap_data {
            let key = item["key"]
                .as_str()
                .ok_or("Missing or invalid 'key' in heap item")?
                .to_string();
            let value = item["value"]
                .as_f64()
                .ok_or("Missing or invalid 'value' in heap item")?;
            topk_heap.push(HeapItem { key, value });
        }

        Ok(Self {
            inner: CountMinSketchWithHeap::from_legacy_matrix(
                sketch, topk_heap, row_num, col_num, heap_size,
            ),
        })
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            inner: CountMinSketchWithHeap::deserialize_msgpack(buffer)?,
        })
    }

    pub fn deserialize_from_bytes(_buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        Err("deserialize_from_bytes for CountMinSketchWithHeapAccumulator not implemented".into())
    }

    /// Get all keys from the top-k heap.
    pub fn get_topk_keys(&self) -> Vec<KeyByLabelValues> {
        self.inner
            .topk_heap_items()
            .iter()
            .map(|item| {
                let labels: Vec<String> = item.key.split(';').map(|s| s.to_string()).collect();
                KeyByLabelValues { labels }
            })
            .collect()
    }
}

impl SerializableToSink for CountMinSketchWithHeapAccumulator {
    fn serialize_to_json(&self) -> Value {
        let heap_items: Vec<Value> = self
            .inner
            .topk_heap_items()
            .iter()
            .map(|item| {
                serde_json::json!({
                    "key": item.key,
                    "value": item.value
                })
            })
            .collect();

        serde_json::json!({
            "row_num": self.inner.row_num,
            "col_num": self.inner.col_num,
            "heap_size": self.inner.heap_size,
            "sketch": self.inner.sketch_matrix(),
            "topk_heap": heap_items
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.inner.serialize_msgpack()
    }
}

impl AggregateCore for CountMinSketchWithHeapAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "CountMinSketchWithHeapAccumulator"
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
                "Cannot merge CountMinSketchWithHeapAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        let other_cms = other
            .as_any()
            .downcast_ref::<CountMinSketchWithHeapAccumulator>()
            .ok_or("Failed to downcast to CountMinSketchWithHeapAccumulator")?;

        let merged = Self::merge_accumulators(vec![self.clone(), other_cms.clone()])?;
        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> &'static str {
        "CountMinSketchWithHeapAccumulator"
    }

    fn get_keys(&self) -> Option<Vec<crate::KeyByLabelValues>> {
        Some(self.get_topk_keys())
    }
}

impl MultipleSubpopulationAggregate for CountMinSketchWithHeapAccumulator {
    fn query(
        &self,
        _statistic: Statistic,
        key: &KeyByLabelValues,
        _query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.query_key(key))
    }

    fn clone_boxed(&self) -> Box<dyn MultipleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

impl MergeableAccumulator<CountMinSketchWithHeapAccumulator> for CountMinSketchWithHeapAccumulator {
    fn merge_accumulators(
        accumulators: Vec<CountMinSketchWithHeapAccumulator>,
    ) -> Result<CountMinSketchWithHeapAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }
        let inners: Vec<CountMinSketchWithHeap> =
            accumulators.into_iter().map(|acc| acc.inner).collect();
        let merged_inner = CountMinSketchWithHeap::merge(inners)?;
        Ok(Self {
            inner: merged_inner,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_min_sketch_with_heap_creation() {
        let cms = CountMinSketchWithHeapAccumulator::new(4, 1000, 20);
        assert_eq!(cms.inner.row_num, 4);
        assert_eq!(cms.inner.col_num, 1000);
        assert_eq!(cms.inner.heap_size, 20);
        assert_eq!(cms.inner.topk_heap_items().len(), 0);
    }

    #[test]
    fn test_count_min_sketch_with_heap_query() {
        let cms = CountMinSketchWithHeapAccumulator::new(2, 10, 5);
        let key = KeyByLabelValues::new();
        assert_eq!(cms.query_key(&key), 0.0);

        let multi_trait: &dyn MultipleSubpopulationAggregate = &cms;
        assert_eq!(multi_trait.query(Statistic::Sum, &key, None).unwrap(), 0.0);
    }

    #[test]
    fn test_count_min_sketch_with_heap_merge() {
        let mut cms1 = CountMinSketchWithHeapAccumulator::new(2, 10, 5);
        let mut cms2 = CountMinSketchWithHeapAccumulator::new(2, 10, 3);

        if let Some(sketch) = cms1.inner.sketch_mut() {
            sketch[0][0] = 10.0;
            sketch[1][1] = 20.0;
        }
        if let Some(sketch) = cms2.inner.sketch_mut() {
            sketch[0][0] = 5.0;
            sketch[1][1] = 15.0;
        }
        for item in [
            HeapItem { key: "key1".to_string(), value: 100.0 },
            HeapItem { key: "key2".to_string(), value: 50.0 },
        ] {
            cms1.inner.update(&item.key, item.value);
        }
        for item in [
            HeapItem { key: "key3".to_string(), value: 75.0 },
            HeapItem { key: "key1".to_string(), value: 80.0 },
        ] {
            cms2.inner.update(&item.key, item.value);
        }

        let result = CountMinSketchWithHeapAccumulator::merge_accumulators(vec![cms1, cms2]);
        assert!(result.is_ok());
        let merged = result.unwrap();
        assert_eq!(merged.inner.sketch_matrix()[0][0], 15.0);
        assert_eq!(merged.inner.sketch_matrix()[1][1], 35.0);
        assert_eq!(merged.inner.heap_size, 3);
        assert!(merged.inner.topk_heap_items().len() <= 3);
    }

    #[test]
    fn test_count_min_sketch_with_heap_merge_single() {
        let cms = CountMinSketchWithHeapAccumulator::new(2, 3, 5);
        let result = CountMinSketchWithHeapAccumulator::merge_accumulators(vec![cms.clone()]);
        assert!(result.is_ok());
        let merged = result.unwrap();
        assert_eq!(merged.inner.row_num, cms.inner.row_num);
        assert_eq!(merged.inner.col_num, cms.inner.col_num);
        assert_eq!(merged.inner.heap_size, cms.inner.heap_size);
    }

    #[test]
    fn test_count_min_sketch_with_heap_merge_dimension_mismatch() {
        let cms1 = CountMinSketchWithHeapAccumulator::new(2, 10, 5);
        let cms2 = CountMinSketchWithHeapAccumulator::new(3, 10, 5);
        let result = CountMinSketchWithHeapAccumulator::merge_accumulators(vec![cms1, cms2]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("different dimensions"));
    }

    #[test]
    fn test_count_min_sketch_with_heap_serialization() {
        // Use from_legacy_matrix for a controlled state that round-trips correctly with both backends.
        let sketch = vec![vec![0.0, 42.0, 0.0], vec![0.0, 0.0, 100.0]];
        let topk_heap = vec![HeapItem {
            key: "test_key".to_string(),
            value: 99.0,
        }];
        let cms = CountMinSketchWithHeapAccumulator {
            inner: CountMinSketchWithHeap::from_legacy_matrix(
                sketch, topk_heap, 2, 3, 5,
            ),
        };

        let bytes = cms.serialize_to_bytes();
        let deserialized =
            CountMinSketchWithHeapAccumulator::deserialize_from_bytes_arroyo(&bytes).unwrap();

        assert_eq!(deserialized.inner.row_num, 2);
        assert_eq!(deserialized.inner.col_num, 3);
        assert_eq!(deserialized.inner.heap_size, 5);
        assert_eq!(deserialized.inner.sketch_matrix()[0][1], 42.0);
        // [1][2] may be 100 (legacy, no hash collision) or 199 (100+99 when test_key hashes there)
        assert!(
            deserialized.inner.sketch_matrix()[1][2] >= 100.0,
            "expected >= 100, got {}",
            deserialized.inner.sketch_matrix()[1][2]
        );
        assert_eq!(deserialized.inner.topk_heap_items().len(), 1);
        assert_eq!(deserialized.inner.topk_heap_items()[0].key, "test_key");
        // With sketchlib backend, heap stores CMS estimate (min over buckets for key).
        // "test_key" may hash to (0,1) and (1,2) giving min(42,100)=42, or other values.
        assert!(
            deserialized.inner.topk_heap_items()[0].value >= 42.0,
            "expected >= 42, got {}",
            deserialized.inner.topk_heap_items()[0].value
        );
    }

    #[test]
    fn test_count_min_sketch_with_heap_as_aggregate_core() {
        let cms = CountMinSketchWithHeapAccumulator::new(2, 3, 5);
        assert_eq!(cms.type_name(), "CountMinSketchWithHeapAccumulator");
    }

    #[test]
    fn test_get_topk_keys() {
        let mut cms = CountMinSketchWithHeapAccumulator::new(2, 3, 5);
        cms.inner.update("label1;label2", 100.0);
        cms.inner.update("label3;label4", 50.0);

        let keys = cms.get_topk_keys();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].labels, vec!["label1", "label2"]);
        assert_eq!(keys[1].labels, vec!["label3", "label4"]);
    }

    #[test]
    fn test_multiple_subpopulation_aggregate() {
        let cms = CountMinSketchWithHeapAccumulator::new(3, 50, 10);
        let key = KeyByLabelValues::new();

        let multi_trait: &dyn MultipleSubpopulationAggregate = &cms;
        let result = multi_trait.query(Statistic::Sum, &key, None).unwrap();
        assert_eq!(result, 0.0);

        let keys = multi_trait.get_keys();
        assert!(keys.is_some());
        assert_eq!(keys.unwrap().len(), 0);
    }
}
