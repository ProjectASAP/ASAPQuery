use crate::data_model::{
    AggregateCore, AggregationType, MergeableAccumulator, SerializableToSink,
    SingleSubpopulationAggregate,
};
use asap_sketchlib::DDSketch;
use asap_types::traits::SerializationError;
use base64::{engine::general_purpose, Engine as _};
use serde_json::Value;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

/// DDSketch accumulator — wraps `asap_sketchlib::DDSketch`, the same
/// implementation sketch-bench measures for the planner's cost table.
///
/// `alpha` is the relative-accuracy bound: a returned quantile is within a
/// factor of `(1 + alpha) / (1 - alpha)` of the true value. Negative values go
/// to a mirrored store and zeros to a zero bucket; non-finite inputs are dropped.
#[derive(Clone, Debug)]
pub struct DDSketchAccumulator {
    pub inner: DDSketch,
}

impl DDSketchAccumulator {
    /// Panics unless `alpha` is in `(0, 1)`; configs are validated before this
    /// is reached (`AggregationConfig::validate`).
    pub fn new(alpha: f64) -> Self {
        Self {
            inner: DDSketch::new(alpha),
        }
    }

    pub fn update(&mut self, value: f64) {
        self.inner.add(&value);
    }

    pub fn count(&self) -> u64 {
        self.inner.get_count()
    }

    pub fn alpha(&self) -> f64 {
        self.inner.alpha()
    }

    /// `NaN` for an empty sketch, matching an empty window having no quantile.
    pub fn get_quantile(&self, quantile: f64) -> f64 {
        self.inner
            .get_value_at_quantile(quantile)
            .unwrap_or(f64::NAN)
    }

    fn merge_inner(&mut self, other: &DDSketchAccumulator) -> Result<(), String> {
        self.inner.merge(&other.inner)
    }
}

impl SerializableToSink for DDSketchAccumulator {
    fn serialize_to_json(&self) -> Result<Value, SerializationError> {
        let sketch_bytes = self.serialize_to_bytes()?;
        let sketch_b64 = general_purpose::STANDARD.encode(&sketch_bytes);
        Ok(serde_json::json!({ "sketch": sketch_b64 }))
    }

    fn serialize_to_bytes(&self) -> Result<Vec<u8>, SerializationError> {
        self.inner
            .serialize_to_bytes()
            .map_err(|e| SerializationError::Bytes {
                type_name: "DDSketchAccumulator",
                source: e.to_string().into(),
            })
    }
}

impl AggregateCore for DDSketchAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "DDSketchAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        let other_dd = other
            .as_any()
            .downcast_ref::<DDSketchAccumulator>()
            .ok_or_else(|| {
                format!(
                    "Cannot merge DDSketchAccumulator with {}",
                    other.get_accumulator_type()
                )
            })?;
        let mut merged = self.clone();
        merged.merge_inner(other_dd)?;
        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::DDSketch
    }

    fn get_keys(&self) -> Option<Vec<crate::KeyByLabelValues>> {
        None
    }

    fn query_statistic(
        &self,
        statistic: Statistic,
        _key: &Option<crate::KeyByLabelValues>,
        query_kwargs: &HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        self.query(statistic, Some(query_kwargs))
    }
}

impl SingleSubpopulationAggregate for DDSketchAccumulator {
    fn query(
        &self,
        statistic: Statistic,
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

                Ok(self.get_quantile(quantile))
            }
            _ => Err(format!("Unsupported statistic in DDSketchAccumulator: {statistic:?}").into()),
        }
    }

    fn clone_boxed(&self) -> Box<dyn SingleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

impl MergeableAccumulator<DDSketchAccumulator> for DDSketchAccumulator {
    fn merge_accumulators(
        accumulators: Vec<DDSketchAccumulator>,
    ) -> Result<DDSketchAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        let mut iter = accumulators.into_iter();
        let mut merged = iter.next().ok_or("No accumulators to merge")?;
        for acc in iter {
            merged.merge_inner(&acc)?;
        }
        Ok(merged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALPHA: f64 = 0.01;

    /// Relative error of `estimate` against `truth`, the bound DDSketch guarantees.
    fn rel_err(estimate: f64, truth: f64) -> f64 {
        (estimate - truth).abs() / truth
    }

    #[test]
    fn quantiles_are_within_relative_accuracy() {
        let mut dd = DDSketchAccumulator::new(ALPHA);
        for i in 1..=10_000 {
            dd.update(i as f64);
        }
        assert_eq!(dd.count(), 10_000);
        // Rank q·n of 1..=n is q·n itself, so the true quantile is known exactly.
        for q in [0.5, 0.75, 0.9, 0.95, 0.99] {
            let truth = (q * 10_000.0_f64).ceil();
            let estimate = dd.get_quantile(q);
            assert!(
                rel_err(estimate, truth) <= ALPHA,
                "q={q}: estimate {estimate} vs truth {truth}"
            );
        }
    }

    #[test]
    fn zero_and_negative_values_are_counted() {
        let mut dd = DDSketchAccumulator::new(ALPHA);
        dd.update(-5.0);
        dd.update(0.0);
        dd.update(f64::NAN);
        dd.update(42.0);
        assert_eq!(dd.count(), 3);
        assert_eq!(dd.get_quantile(0.0), -5.0);
        assert_eq!(dd.get_quantile(0.5), 0.0);
        assert_eq!(dd.get_quantile(1.0), 42.0);
    }

    #[test]
    fn empty_sketch_quantile_is_nan() {
        assert!(DDSketchAccumulator::new(ALPHA).get_quantile(0.5).is_nan());
    }

    #[test]
    fn query_reads_quantile_kwarg_and_rejects_other_statistics() {
        let mut dd = DDSketchAccumulator::new(ALPHA);
        for i in 1..=100 {
            dd.update(i as f64);
        }
        let mut kwargs = HashMap::new();
        kwargs.insert("quantile".to_string(), "0.5".to_string());
        let median = dd.query(Statistic::Quantile, Some(&kwargs)).unwrap();
        assert!(rel_err(median, 50.0) <= ALPHA, "median {median}");

        kwargs.insert("quantile".to_string(), "1.5".to_string());
        assert!(dd.query(Statistic::Quantile, Some(&kwargs)).is_err());
        assert!(dd.query(Statistic::Quantile, None).is_err());
        assert!(dd.query(Statistic::Sum, Some(&kwargs)).is_err());
    }

    #[test]
    fn merge_equals_single_sketch_over_all_values() {
        let mut whole = DDSketchAccumulator::new(ALPHA);
        let mut parts = vec![
            DDSketchAccumulator::new(ALPHA),
            DDSketchAccumulator::new(ALPHA),
            DDSketchAccumulator::new(ALPHA),
        ];
        for i in 1..=3_000 {
            whole.update(i as f64);
            parts[i % 3].update(i as f64);
        }

        let merged = DDSketchAccumulator::merge_accumulators(parts.clone()).unwrap();
        assert_eq!(merged.count(), whole.count());
        for q in [0.0, 0.5, 0.99, 1.0] {
            assert_eq!(merged.get_quantile(q), whole.get_quantile(q), "q={q}");
        }

        // The trait-object path the window merger uses agrees with the direct merge.
        let boxed = parts[0].merge_with(&parts[1]).unwrap();
        let boxed = boxed.merge_with(&parts[2]).unwrap();
        let via_trait = boxed
            .as_any()
            .downcast_ref::<DDSketchAccumulator>()
            .unwrap();
        assert_eq!(via_trait.get_quantile(0.5), whole.get_quantile(0.5));
    }

    #[test]
    fn merge_rejects_mismatched_alpha_and_type() {
        let mut a = DDSketchAccumulator::new(0.01);
        let mut b = DDSketchAccumulator::new(0.02);
        a.update(1.0);
        b.update(2.0);
        assert!(DDSketchAccumulator::merge_accumulators(vec![a.clone(), b.clone()]).is_err());
        assert!(a.merge_with(&b).is_err());

        let other = crate::precompute_operators::SumAccumulator::new();
        assert!(a.merge_with(&other).is_err());
    }

    #[test]
    fn serializes_to_bytes_and_json() {
        let mut dd = DDSketchAccumulator::new(ALPHA);
        dd.update(3.0);
        let bytes = dd.serialize_to_bytes().unwrap();
        let restored = DDSketch::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(restored.get_count(), 1);

        let json = dd.serialize_to_json().unwrap();
        let b64 = json["sketch"].as_str().unwrap();
        assert_eq!(general_purpose::STANDARD.decode(b64).unwrap(), bytes);
    }

    #[test]
    fn reports_type() {
        let dd = DDSketchAccumulator::new(ALPHA);
        assert_eq!(dd.type_name(), "DDSketchAccumulator");
        assert_eq!(dd.get_accumulator_type(), AggregationType::DDSketch);
        assert!(dd.get_keys().is_none());
    }
}
