use crate::data_model::{
    AggregateCore, KeyByLabelValues, MergeableAccumulator, MultipleSubpopulationAggregate,
    SerializableToSink,
};
use serde_json::Value;
use sketch_core::count_min::CountMinSketch;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

/// Count-Min Sketch accumulator — wraps sketch_core::CountMinSketch.
/// Core struct, update/merge/serde logic live in sketch-core.
/// This file retains QE-specific trait impls, legacy deserializers, and JSON output.
#[derive(Debug, Clone)]
pub struct CountMinSketchAccumulator {
    pub inner: CountMinSketch,
}

impl CountMinSketchAccumulator {
    pub fn new(row_num: usize, col_num: usize) -> Self {
        Self {
            inner: CountMinSketch::new(row_num, col_num),
        }
    }

    // Marked as _update and kept private; only called internally.
    fn _update(&mut self, key: &KeyByLabelValues, value: f64) {
        self.inner.update(&key.to_semicolon_str(), value);
    }

    pub fn query_key(&self, key: &KeyByLabelValues) -> f64 {
        self.inner.query_key(&key.to_semicolon_str())
    }

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let row_num = data["row_num"]
            .as_f64()
            .ok_or("Missing or invalid 'row_num' field")? as usize;
        let col_num = data["col_num"]
            .as_f64()
            .ok_or("Missing or invalid 'col_num' field")? as usize;

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

        Ok(Self {
            inner: CountMinSketch {
                sketch,
                row_num,
                col_num,
            },
        })
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            inner: CountMinSketch::deserialize_msgpack(buffer)?,
        })
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        if buffer.len() < 8 {
            return Err("Buffer too short for row_num and col_num".into());
        }

        // TODO: this logic will need to be checked for i32 -> f64
        // Github Issue #11

        let row_num = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        let col_num = u32::from_le_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]) as usize;

        let expected_size = 8 + (row_num * col_num * 4);
        if buffer.len() < expected_size {
            return Err("Buffer too short for sketch data".into());
        }

        let mut sketch = Vec::new();
        let mut offset = 8;

        for _ in 0..row_num {
            let mut row = Vec::new();
            for _ in 0..col_num {
                let value = f64::from_le_bytes([
                    buffer[offset],
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                    buffer[offset + 4],
                    buffer[offset + 5],
                    buffer[offset + 6],
                    buffer[offset + 7],
                ]);
                row.push(value);
                offset += 8;
            }
            sketch.push(row);
        }

        Ok(Self {
            inner: CountMinSketch {
                row_num,
                col_num,
                sketch,
            },
        })
    }

    /// Merge multiple accumulators efficiently without cloning all of them.
    pub fn merge_multiple(
        accumulators: &[Box<dyn crate::data_model::AggregateCore>],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let mut cms_accumulators = Vec::with_capacity(accumulators.len());
        for acc in accumulators {
            if acc.get_accumulator_type() != "CountMinSketchAccumulator" {
                return Err(format!(
                    "Cannot merge CountMinSketchAccumulator with {}",
                    acc.get_accumulator_type()
                )
                .into());
            }
            let cms_acc = acc
                .as_any()
                .downcast_ref::<CountMinSketchAccumulator>()
                .ok_or("Failed to downcast to CountMinSketchAccumulator")?;
            cms_accumulators.push(cms_acc);
        }

        // Check dimensions are consistent
        let row_num = cms_accumulators[0].inner.row_num;
        let col_num = cms_accumulators[0].inner.col_num;
        for acc in &cms_accumulators {
            if acc.inner.row_num != row_num || acc.inner.col_num != col_num {
                return Err(
                    "Cannot merge CountMinSketch accumulators with different dimensions".into(),
                );
            }
        }

        let inner_refs: Vec<&CountMinSketch> =
            cms_accumulators.iter().map(|acc| &acc.inner).collect();
        let merged_inner = CountMinSketch::merge_refs(&inner_refs)?;
        Ok(Self {
            inner: merged_inner,
        })
    }
}

impl SerializableToSink for CountMinSketchAccumulator {
    fn serialize_to_json(&self) -> Value {
        serde_json::json!({
            "row_num": self.inner.row_num,
            "col_num": self.inner.col_num,
            "sketch": self.inner.sketch
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.inner.serialize_msgpack()
    }
}

impl AggregateCore for CountMinSketchAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "CountMinSketchAccumulator"
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
                "Cannot merge CountMinSketchAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        let other_cms = other
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .ok_or("Failed to downcast to CountMinSketchAccumulator")?;

        let merged_inner = CountMinSketch::merge_refs(&[&self.inner, &other_cms.inner])?;
        Ok(Box::new(Self {
            inner: merged_inner,
        }))
    }

    fn get_accumulator_type(&self) -> &'static str {
        "CountMinSketchAccumulator"
    }

    fn get_keys(&self) -> Option<Vec<crate::KeyByLabelValues>> {
        None
    }
}

impl MultipleSubpopulationAggregate for CountMinSketchAccumulator {
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

impl MergeableAccumulator<CountMinSketchAccumulator> for CountMinSketchAccumulator {
    fn merge_accumulators(
        accumulators: Vec<CountMinSketchAccumulator>,
    ) -> Result<CountMinSketchAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }
        let inners: Vec<CountMinSketch> = accumulators.into_iter().map(|acc| acc.inner).collect();
        let merged_inner = CountMinSketch::merge(inners)?;
        Ok(Self {
            inner: merged_inner,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_min_sketch_creation() {
        let cms = CountMinSketchAccumulator::new(4, 1000);
        assert_eq!(cms.inner.row_num, 4);
        assert_eq!(cms.inner.col_num, 1000);
        assert_eq!(cms.inner.sketch.len(), 4);
        assert_eq!(cms.inner.sketch[0].len(), 1000);

        for row in &cms.inner.sketch {
            for &value in row {
                assert_eq!(value, 0.0);
            }
        }
    }

    #[test]
    fn test_count_min_sketch_update() {
        let mut cms = CountMinSketchAccumulator::new(2, 10);
        let key = KeyByLabelValues::new();
        cms._update(&key, 1.0);
        let result = cms.query_key(&key);
        assert!(result >= 1.0);
    }

    #[test]
    fn test_count_min_sketch_query() {
        let cms = CountMinSketchAccumulator::new(2, 10);
        let key = KeyByLabelValues::new();
        assert_eq!(cms.query_key(&key), 0.0);

        let multi_trait: &dyn MultipleSubpopulationAggregate = &cms;
        assert_eq!(multi_trait.query(Statistic::Sum, &key, None).unwrap(), 0.0);
    }

    #[test]
    fn test_count_min_sketch_merge() {
        let mut cms1 = CountMinSketchAccumulator::new(2, 3);
        let mut cms2 = CountMinSketchAccumulator::new(2, 3);

        cms1.inner.sketch[0][0] = 5.0;
        cms1.inner.sketch[1][2] = 10.0;
        cms2.inner.sketch[0][0] = 3.0;
        cms2.inner.sketch[0][1] = 7.0;

        let merged = CountMinSketchAccumulator::merge_accumulators(vec![cms1, cms2]).unwrap();

        assert_eq!(merged.inner.sketch[0][0], 8.0);
        assert_eq!(merged.inner.sketch[0][1], 7.0);
        assert_eq!(merged.inner.sketch[1][2], 10.0);
    }

    #[test]
    fn test_count_min_sketch_merge_dimension_mismatch() {
        let cms1 = CountMinSketchAccumulator::new(2, 3);
        let cms2 = CountMinSketchAccumulator::new(3, 3);
        let result = CountMinSketchAccumulator::merge_accumulators(vec![cms1, cms2]);
        assert!(result.is_err());
    }

    #[test]
    fn test_count_min_sketch_serialization() {
        let mut cms = CountMinSketchAccumulator::new(2, 3);
        cms.inner.sketch[0][1] = 42.0;
        cms.inner.sketch[1][2] = 100.0;

        let bytes = cms.serialize_to_bytes();
        let deserialized =
            CountMinSketchAccumulator::deserialize_from_bytes_arroyo(&bytes).unwrap();

        assert_eq!(deserialized.inner.row_num, 2);
        assert_eq!(deserialized.inner.col_num, 3);
        assert_eq!(deserialized.inner.sketch[0][1], 42.0);
        assert_eq!(deserialized.inner.sketch[1][2], 100.0);
    }

    #[test]
    fn test_count_min_sketch_as_aggregate_core() {
        let cms = CountMinSketchAccumulator::new(2, 3);
        assert_eq!(cms.type_name(), "CountMinSketchAccumulator");
    }

    #[test]
    fn test_trait_object() {
        let cms = CountMinSketchAccumulator::new(2, 3);
        let trait_obj: Box<dyn AggregateCore> = Box::new(cms);
        assert_eq!(trait_obj.type_name(), "CountMinSketchAccumulator");
    }

    #[test]
    fn test_count_min_sketch_key_query() {
        let mut cms = CountMinSketchAccumulator::new(4, 100);
        let key = KeyByLabelValues::new();
        assert_eq!(cms.query_key(&key), 0.0);
        cms._update(&key, 5.0);
        let result = cms.query_key(&key);
        assert!(result >= 5.0);
    }

    #[test]
    fn test_update_and_query_use_same_key_encoding() {
        // Regression test: _update and query_key must hash the same key string.
        // Previously _update went through serialize_to_json (which returns a JSON
        // array, so as_object() is always None) and always stored under key "".
        // query_key correctly used key.labels.join(";"), so they never matched.
        let mut cms = CountMinSketchAccumulator::new(4, 1000);
        let key = KeyByLabelValues::new_with_labels(vec!["web".to_string(), "prod".to_string()]);
        cms._update(&key, 5.0);
        let result = cms.query_key(&key);
        assert!(
            result >= 5.0,
            "_update and query_key used different key encodings: got {result}"
        );

        // Also verify a different key does not interfere.
        let other_key = KeyByLabelValues::new_with_labels(vec!["api".to_string()]);
        // other_key was never updated; its estimate should be lower than key's.
        let other_result = cms.query_key(&other_key);
        // In a sketch this large there should be no collision, so other_result == 0.
        assert_eq!(
            other_result, 0.0,
            "unrelated key returned non-zero: {other_result}"
        );
    }

    #[test]
    fn test_multiple_subpopulation_aggregate() {
        let mut cms = CountMinSketchAccumulator::new(3, 50);
        let key = KeyByLabelValues::new();
        cms._update(&key, 10.0);

        let multi_trait: &dyn MultipleSubpopulationAggregate = &cms;
        let result = multi_trait.query(Statistic::Sum, &key, None).unwrap();
        assert!(result >= 10.0);

        let keys = multi_trait.get_keys();
        assert!(keys.is_none());
    }

    #[test]
    fn test_count_min_sketch_merge_multiple() {
        let mut cms1 = CountMinSketchAccumulator::new(2, 3);
        let mut cms2 = CountMinSketchAccumulator::new(2, 3);
        let mut cms3 = CountMinSketchAccumulator::new(2, 3);

        cms1.inner.sketch[0][0] = 5.0;
        cms1.inner.sketch[1][2] = 10.0;
        cms2.inner.sketch[0][0] = 3.0;
        cms2.inner.sketch[0][1] = 7.0;
        cms3.inner.sketch[0][0] = 2.0;
        cms3.inner.sketch[1][2] = 5.0;

        let boxed_accs: Vec<Box<dyn AggregateCore>> =
            vec![Box::new(cms1), Box::new(cms2), Box::new(cms3)];

        let merged = CountMinSketchAccumulator::merge_multiple(&boxed_accs).unwrap();

        assert_eq!(merged.inner.sketch[0][0], 10.0);
        assert_eq!(merged.inner.sketch[0][1], 7.0);
        assert_eq!(merged.inner.sketch[1][2], 15.0);
    }

    #[test]
    fn test_count_min_sketch_merge_multiple_error_cases() {
        let empty: Vec<Box<dyn AggregateCore>> = vec![];
        assert!(CountMinSketchAccumulator::merge_multiple(&empty).is_err());

        let cms1 = CountMinSketchAccumulator::new(2, 3);
        let cms2 = CountMinSketchAccumulator::new(3, 3);
        let boxed_accs: Vec<Box<dyn AggregateCore>> = vec![Box::new(cms1), Box::new(cms2)];
        assert!(CountMinSketchAccumulator::merge_multiple(&boxed_accs).is_err());

        use crate::precompute_operators::sum_accumulator::SumAccumulator;
        let cms = CountMinSketchAccumulator::new(2, 3);
        let sum = SumAccumulator::new();
        let mixed_accs: Vec<Box<dyn AggregateCore>> = vec![Box::new(cms), Box::new(sum)];
        assert!(CountMinSketchAccumulator::merge_multiple(&mixed_accs).is_err());
    }
}
