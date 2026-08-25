//! Shared fixtures for testing accumulator-batch merge behavior
//!
//! Used by both the range-query (`NaiveMerger`) and instant-query
//! (`SimpleEngine::merge_accumulators`) merge regression tests, so the two
//! test suites can assert the two paths agree without duplicating fixtures.

use crate::data_model::{AggregateCore, AggregationType, KeyByLabelValues, SerializableToSink};
use crate::precompute_operators::CountMinSketchAccumulator;
use asap_sketchlib::CountMinSketch;
use serde_json::Value;
use std::any::Any;

pub fn cms_from_matrix(
    matrix: Vec<Vec<f64>>,
    rows: usize,
    cols: usize,
) -> CountMinSketchAccumulator {
    CountMinSketchAccumulator {
        inner: CountMinSketch::from_legacy_matrix(matrix, rows, cols),
    }
}

/// Independent oracle: a plain sequential `merge_with` fold, computed
/// without going through either `NaiveMerger` or `merge_accumulators`.
pub fn oracle_sequential_fold(buckets: &[Box<dyn AggregateCore>]) -> Box<dyn AggregateCore> {
    let mut iter = buckets.iter();
    let mut acc = iter
        .next()
        .expect("oracle needs at least one bucket")
        .clone();
    for b in iter {
        acc = acc
            .merge_with(b.as_ref())
            .expect("oracle sequential fold's merge_with failed");
    }
    acc
}

/// Mock accumulator whose `merge_with` fails under a condition the test
/// fully controls (a `poisoned` flag), independent of any real
/// accumulator's library-specific error conditions.
#[derive(Clone, Debug)]
pub struct PoisonableAccumulator {
    pub id: u32,
    pub poisoned: bool,
}

impl SerializableToSink for PoisonableAccumulator {
    fn serialize_to_json(&self) -> Value {
        serde_json::json!({"id": self.id})
    }
    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.id.to_le_bytes().to_vec()
    }
}

impl AggregateCore for PoisonableAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }
    fn type_name(&self) -> &'static str {
        "PoisonableAccumulator"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        let other_p = other
            .as_any()
            .downcast_ref::<PoisonableAccumulator>()
            .ok_or("Cannot merge with different accumulator type")?;
        if self.poisoned || other_p.poisoned {
            return Err(format!("poisoned merge involving id {} / {}", self.id, other_p.id).into());
        }
        Ok(Box::new(PoisonableAccumulator {
            id: self.id.max(other_p.id),
            poisoned: false,
        }))
    }
    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::Sum
    }
    fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
        None
    }
    fn query_statistic(
        &self,
        _statistic: promql_utilities::query_logics::enums::Statistic,
        _key: &Option<KeyByLabelValues>,
        _query_kwargs: &std::collections::HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        Err("PoisonableAccumulator does not support query_statistic".into())
    }
}
