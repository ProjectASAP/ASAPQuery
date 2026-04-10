use crate::{
    data_model::{
        AggregateCore, AggregationType, MergeableAccumulator, MultipleSubpopulationAggregate,
        SerializableToSink,
    },
    KeyByLabelValues,
};
use base64::{engine::general_purpose, Engine as _};
use sketch_core::hydra_kll::HydraKllSketch;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

/// HydraKLL sketch accumulator — wraps sketch_core::HydraKllSketch.
/// Core struct, update/merge/serde logic live in sketch-core.
/// This file retains QE-specific trait impls and JSON output.
#[derive(Debug, Clone)]
pub struct HydraKllSketchAccumulator {
    pub inner: HydraKllSketch,
}

impl HydraKllSketchAccumulator {
    pub fn new(row_num: usize, col_num: usize, k: u16) -> Self {
        Self {
            inner: HydraKllSketch::new(row_num, col_num, k),
        }
    }

    pub fn update(&mut self, key: &KeyByLabelValues, value: f64) {
        self.inner.update(&key.to_semicolon_str(), value);
    }

    pub fn deserialize_from_bytes(_buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        Err("deserialize_from_bytes for HydraKllSketchAccumulator not implemented".into())
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            inner: HydraKllSketch::deserialize_msgpack(buffer)?,
        })
    }

    pub fn query_key(&self, key: &KeyByLabelValues, quantile: f64) -> f64 {
        self.inner.query(&key.to_semicolon_str(), quantile)
    }
}

impl SerializableToSink for HydraKllSketchAccumulator {
    fn serialize_to_json(&self) -> serde_json::Value {
        // Mirror Python implementation: {"sketch": base64_encoded_string}
        let sketch_bytes = self.inner.serialize_msgpack();
        let sketch_b64 = general_purpose::STANDARD.encode(&sketch_bytes);
        serde_json::json!({ "sketch": sketch_b64 })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.inner.serialize_msgpack()
    }
}

impl MergeableAccumulator<HydraKllSketchAccumulator> for HydraKllSketchAccumulator {
    fn merge_accumulators(
        accumulators: Vec<HydraKllSketchAccumulator>,
    ) -> Result<HydraKllSketchAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }
        let inners: Vec<HydraKllSketch> = accumulators.into_iter().map(|acc| acc.inner).collect();
        let merged_inner = HydraKllSketch::merge(inners)?;
        Ok(Self {
            inner: merged_inner,
        })
    }
}

impl AggregateCore for HydraKllSketchAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "HydraKllSketchAccumulator"
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
                "Cannot merge HydraKllSketchAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        let hk = other
            .as_any()
            .downcast_ref::<HydraKllSketchAccumulator>()
            .ok_or("Failed to downcast to HydraKllSketchAccumulator")?;

        let merged = Self::merge_accumulators(vec![self.clone(), hk.clone()])?;
        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::HydraKLL
    }

    fn get_keys(&self) -> Option<Vec<crate::KeyByLabelValues>> {
        None
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
            .ok_or("Key required for HydraKllSketchAccumulator")?;
        self.query(statistic, key_val, Some(query_kwargs))
    }
}

impl MultipleSubpopulationAggregate for HydraKllSketchAccumulator {
    fn query(
        &self,
        statistic: Statistic,
        key: &KeyByLabelValues,
        query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        match statistic {
            Statistic::Quantile => {
                let quantile = query_kwargs
                    .and_then(|kwargs| kwargs.get("quantile"))
                    .ok_or("Missing quantile parameter for quantile query")?
                    .parse::<f64>()
                    .map_err(|_| "Invalid quantile parameter format")?;

                if !(0.0..=1.0).contains(&quantile) {
                    return Err("Quantile must be between 0.0 and 1.0".into());
                }

                Ok(self.query_key(key, quantile))
            }
            _ => Err(
                format!("Unsupported statistic in HydraKllSketchAccumulator: {statistic:?}").into(),
            ),
        }
    }

    fn clone_boxed(&self) -> Box<dyn MultipleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}
