use crate::data_model::{
    AggregateCore, AggregationType, KeyByLabelValues, MergeableAccumulator,
    MultipleSubpopulationAggregate, SerializableToSink,
};
use asap_sketchlib::{CMSHeap, DataInput, FastPath, HeapItem, Vector2D};
use asap_types::traits::SerializationError;
use serde_json::Value;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

/// Count-Min Sketch with Heap accumulator backed by the benchmarked FastPath implementation.
#[derive(Clone)]
pub struct CountMinSketchWithHeapAccumulator {
    inner: CMSHeap<Vector2D<i32>, FastPath>,
}

impl std::fmt::Debug for CountMinSketchWithHeapAccumulator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CountMinSketchWithHeapAccumulator")
            .field("rows", &self.inner.rows())
            .field("cols", &self.inner.cols())
            .field("heap_size", &self.inner.heap().capacity())
            .finish()
    }
}

impl CountMinSketchWithHeapAccumulator {
    pub fn new(row_num: usize, col_num: usize, heap_size: usize) -> Self {
        Self {
            inner: CMSHeap::new(row_num, col_num, heap_size),
        }
    }

    pub fn update(&mut self, key: &str, count: i32) {
        if count > 0 {
            self.inner.insert_many(&DataInput::Str(key), count);
        }
    }

    pub fn query_key(&self, key: &KeyByLabelValues) -> f64 {
        let key_string = key.labels.join(";");
        self.inner.estimate(&DataInput::Str(&key_string)) as f64
    }

    /// Get all keys from the top-k heap.
    pub fn get_topk_keys(&self) -> Vec<KeyByLabelValues> {
        self.inner
            .heap()
            .heap()
            .iter()
            .map(|item| {
                let key = match &item.key {
                    HeapItem::String(key) => key,
                    other => panic!("expected string heap key, got {other:?}"),
                };
                let labels: Vec<String> = key.split(';').map(str::to_owned).collect();
                KeyByLabelValues { labels }
            })
            .collect()
    }

    fn sketch_matrix(&self) -> Vec<Vec<i32>> {
        let storage = self.inner.cms().as_storage();
        (0..storage.rows())
            .map(|row| {
                (0..storage.cols())
                    .map(|col| *storage.get(row, col).expect("matrix index is in bounds"))
                    .collect()
            })
            .collect()
    }
}

impl SerializableToSink for CountMinSketchWithHeapAccumulator {
    fn serialize_to_json(&self) -> Result<Value, SerializationError> {
        let heap_items: Vec<Value> = self
            .inner
            .heap()
            .heap()
            .iter()
            .map(|item| {
                let key = match &item.key {
                    HeapItem::String(key) => key,
                    other => panic!("expected string heap key, got {other:?}"),
                };
                serde_json::json!({
                    "key": key,
                    "value": item.count
                })
            })
            .collect();

        Ok(serde_json::json!({
            "row_num": self.inner.rows(),
            "col_num": self.inner.cols(),
            "heap_size": self.inner.heap().capacity(),
            "sketch": self.sketch_matrix(),
            "topk_heap": heap_items
        }))
    }

    fn serialize_to_bytes(&self) -> Result<Vec<u8>, SerializationError> {
        Err(SerializationError::Bytes {
            type_name: "CountMinSketchWithHeapAccumulator",
            source: "byte serialization is not supported for FastPath CMSHeap".into(),
        })
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

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::CountMinSketchWithHeap
    }

    fn get_keys(&self) -> Option<Vec<crate::KeyByLabelValues>> {
        Some(self.get_topk_keys())
    }

    fn query_statistic(
        &self,
        statistic: promql_utilities::query_logics::enums::Statistic,
        key: &Option<crate::KeyByLabelValues>,
        query_kwargs: &std::collections::HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        use crate::data_model::MultipleSubpopulationAggregate;
        let key_val = key
            .as_ref()
            .ok_or("Key required for CountMinSketchWithHeapAccumulator")?;
        self.query(statistic, key_val, Some(query_kwargs))
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
        let mut iter = accumulators.into_iter();
        let mut merged = iter.next().unwrap();
        for acc in iter {
            if merged.inner.rows() != acc.inner.rows() || merged.inner.cols() != acc.inner.cols() {
                return Err("CountMinSketchWithHeap dimension mismatch".into());
            }
            merged.inner.merge(&acc.inner);
        }
        Ok(merged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_min_sketch_with_heap_creation() {
        let cms = CountMinSketchWithHeapAccumulator::new(4, 1000, 20);
        assert_eq!(cms.inner.rows(), 4);
        assert_eq!(cms.inner.cols(), 1000);
        assert_eq!(cms.inner.heap().capacity(), 20);
        assert_eq!(cms.inner.heap().len(), 0);
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
    fn fastpath_fixed_stream_matches_expected_estimates_and_topk() {
        let mut cms = CountMinSketchWithHeapAccumulator::new(3, 1024, 2);
        cms.update("api", 7);
        cms.update("worker", 3);
        cms.update("api", 5);
        cms.update("batch", 1);

        let api = KeyByLabelValues::new_with_labels(vec!["api".to_owned()]);
        let worker = KeyByLabelValues::new_with_labels(vec!["worker".to_owned()]);
        assert_eq!(cms.query_key(&api), 12.0);
        assert_eq!(cms.query_key(&worker), 3.0);

        let keys: std::collections::HashSet<_> = cms
            .get_topk_keys()
            .into_iter()
            .map(|key| key.labels)
            .collect();
        assert_eq!(
            keys,
            std::collections::HashSet::from([vec!["api".to_owned()], vec!["worker".to_owned()]])
        );
    }

    #[test]
    fn test_count_min_sketch_with_heap_merge() {
        let mut left = CountMinSketchWithHeapAccumulator::new(3, 1024, 5);
        let mut right = CountMinSketchWithHeapAccumulator::new(3, 1024, 3);
        left.update("key1", 100);
        right.update("key1", 80);
        right.update("key2", 75);

        let merged = CountMinSketchWithHeapAccumulator::merge_accumulators(vec![left, right])
            .expect("matching sketches merge");
        let key = KeyByLabelValues::new_with_labels(vec!["key1".to_owned()]);
        assert_eq!(merged.query_key(&key), 180.0);
        assert_eq!(merged.inner.heap().capacity(), 5);
    }

    #[test]
    fn test_count_min_sketch_with_heap_merge_single() {
        let cms = CountMinSketchWithHeapAccumulator::new(2, 3, 5);
        let result = CountMinSketchWithHeapAccumulator::merge_accumulators(vec![cms.clone()]);
        assert!(result.is_ok());
        let merged = result.unwrap();
        assert_eq!(merged.inner.rows(), cms.inner.rows());
        assert_eq!(merged.inner.cols(), cms.inner.cols());
        assert_eq!(merged.inner.heap().capacity(), cms.inner.heap().capacity());
    }

    #[test]
    fn test_count_min_sketch_with_heap_merge_dimension_mismatch() {
        let cms1 = CountMinSketchWithHeapAccumulator::new(2, 10, 5);
        let cms2 = CountMinSketchWithHeapAccumulator::new(3, 10, 5);
        let result = CountMinSketchWithHeapAccumulator::merge_accumulators(vec![cms1, cms2]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("dimension"));
    }

    #[test]
    fn test_count_min_sketch_with_heap_as_aggregate_core() {
        let cms = CountMinSketchWithHeapAccumulator::new(2, 3, 5);
        assert_eq!(cms.type_name(), "CountMinSketchWithHeapAccumulator");
    }

    #[test]
    fn test_get_topk_keys() {
        let mut cms = CountMinSketchWithHeapAccumulator::new(2, 3, 5);
        cms.update("label1;label2", 100);
        cms.update("label3;label4", 50);

        let keys = cms.get_topk_keys();
        assert_eq!(keys.len(), 2);
        // Heap ordering is not part of the top-k interface.
        let label_sets: std::collections::HashSet<_> =
            keys.iter().map(|k| k.labels.clone()).collect();
        assert!(label_sets.contains(&vec!["label1".to_string(), "label2".to_string()]));
        assert!(label_sets.contains(&vec!["label3".to_string(), "label4".to_string()]));
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

    #[test]
    fn byte_serialization_is_unsupported() {
        let cms = CountMinSketchWithHeapAccumulator::new(3, 50, 10);

        let error = cms.serialize_to_bytes().unwrap_err();

        assert!(error.to_string().contains("not supported"));
    }
}
