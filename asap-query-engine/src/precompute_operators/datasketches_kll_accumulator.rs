use crate::data_model::{
    AggregateCore, AggregationType, MergeableAccumulator, SerializableToSink,
    SingleSubpopulationAggregate,
};
use base64::{engine::general_purpose, Engine as _};
use serde_json::Value;
use sketch_core::kll::KllSketch;
use std::collections::HashMap;
#[cfg(feature = "extra_debugging")]
use std::time::Instant;
use tracing::debug;

use promql_utilities::query_logics::enums::Statistic;

/// KLL sketch accumulator — wraps sketch_core::KllSketch.
/// Core struct, update/merge/serde logic live in sketch-core.
/// This file retains QE-specific trait impls, legacy deserializers, and JSON output.
pub struct DatasketchesKLLAccumulator {
    pub inner: KllSketch,
}

impl DatasketchesKLLAccumulator {
    pub fn new(k: u16) -> Self {
        Self {
            inner: KllSketch::new(k),
        }
    }

    pub fn update(&mut self, value: f64) {
        self.inner.update(value);
    }

    pub fn get_quantile(&self, quantile: f64) -> f64 {
        self.inner.get_quantile(quantile)
    }

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        // Mirror Python implementation: expects {"sketch": base64_encoded_string}
        let sketch_b64 = data["sketch"]
            .as_str()
            .ok_or("Missing or invalid 'sketch' field")?;

        let sketch_bytes = general_purpose::STANDARD
            .decode(sketch_b64)
            .map_err(|e| format!("Failed to decode base64 sketch data: {e}"))?;

        // TODO: remove this hardcoding once FlinkSketch serializes k in its output
        Ok(Self {
            inner: KllSketch::from_dsrs_bytes(&sketch_bytes, 200)?,
        })
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        // Mirror Python implementation: deserialize sketch directly from bytes
        // TODO: remove this hardcoding once FlinkSketch serializes k in its output
        Ok(Self {
            inner: KllSketch::from_dsrs_bytes(buffer, 200)?,
        })
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        debug!(
            "Deserializing DatasketchesKLLAccumulator from Arroyo MessagePack buffer of size {}",
            buffer.len()
        );
        Ok(Self {
            inner: KllSketch::deserialize_msgpack(buffer)?,
        })
    }

    /// Merge multiple accumulators efficiently without cloning all of them.
    pub fn merge_multiple(
        accumulators: &[Box<dyn crate::data_model::AggregateCore>],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let mut kll_accumulators = Vec::with_capacity(accumulators.len());
        for acc in accumulators {
            if acc.get_accumulator_type() != AggregationType::DatasketchesKLL {
                return Err(format!(
                    "Cannot merge DatasketchesKLLAccumulator with {:?}",
                    acc.get_accumulator_type()
                )
                .into());
            }
            let kll_acc = acc
                .as_any()
                .downcast_ref::<DatasketchesKLLAccumulator>()
                .ok_or("Failed to downcast to DatasketchesKLLAccumulator")?;
            kll_accumulators.push(kll_acc);
        }

        let inner_refs: Vec<&KllSketch> = kll_accumulators.iter().map(|acc| &acc.inner).collect();
        let merged_inner = KllSketch::merge_refs(&inner_refs)?;
        Ok(Self {
            inner: merged_inner,
        })
    }
}

// Manual trait implementations since the C++ library doesn't provide them
impl Clone for DatasketchesKLLAccumulator {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl std::fmt::Debug for DatasketchesKLLAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasketchesKLLAccumulator")
            .field("k", &self.inner.k)
            .field("sketch_n", &self.inner.count())
            .finish()
    }
}

// TODO: verify this
// Thread safety: The C++ library is not thread-safe by default, but since we're using it
// in a single-threaded context per accumulator instance and only sharing read-only operations,
// this should be safe.
unsafe impl Send for DatasketchesKLLAccumulator {}
unsafe impl Sync for DatasketchesKLLAccumulator {}

impl SerializableToSink for DatasketchesKLLAccumulator {
    fn serialize_to_json(&self) -> Value {
        // Mirror Python implementation: {"sketch": base64_encoded_string}
        let sketch_bytes = self.inner.sketch_bytes();
        let sketch_b64 = general_purpose::STANDARD.encode(&sketch_bytes);
        serde_json::json!({ "sketch": sketch_b64 })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.inner.serialize_msgpack()
    }
}

impl AggregateCore for DatasketchesKLLAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "DatasketchesKLLAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        #[cfg(feature = "extra_debugging")]
        let merge_with_start = Instant::now();
        #[cfg(feature = "extra_debugging")]
        debug!(
            "[PERF] DatasketchesKLLAccumulator::merge_with() started - self.k={}, self.n={}",
            self.inner.k,
            self.inner.count()
        );

        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge DatasketchesKLLAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        let other_kll = other
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .ok_or("Failed to downcast to DatasketchesKLLAccumulator")?;

        let merged_inner = KllSketch::merge_refs(&[&self.inner, &other_kll.inner])?;
        let merged = Self {
            inner: merged_inner,
        };

        #[cfg(feature = "extra_debugging")]
        debug!(
            "[PERF] DatasketchesKLLAccumulator::merge_with() TOTAL TIME: {:?}",
            merge_with_start.elapsed()
        );

        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::DatasketchesKLL
    }

    fn get_keys(&self) -> Option<Vec<crate::KeyByLabelValues>> {
        None
    }

    fn query_statistic(
        &self,
        statistic: promql_utilities::query_logics::enums::Statistic,
        _key: &Option<crate::KeyByLabelValues>,
        query_kwargs: &std::collections::HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        use crate::data_model::SingleSubpopulationAggregate;
        self.query(statistic, Some(query_kwargs))
    }
}

impl SingleSubpopulationAggregate for DatasketchesKLLAccumulator {
    fn query(
        &self,
        statistic: Statistic,
        query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        match statistic {
            Statistic::Quantile => {
                debug!(
                    "Querying DatasketchesKLLAccumulator for quantile with kwargs: {:?}",
                    query_kwargs
                );
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
            _ => Err(
                format!("Unsupported statistic in DatasketchesKLLAccumulator: {statistic:?}")
                    .into(),
            ),
        }
    }

    fn clone_boxed(&self) -> Box<dyn SingleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

impl MergeableAccumulator<DatasketchesKLLAccumulator> for DatasketchesKLLAccumulator {
    fn merge_accumulators(
        accumulators: Vec<DatasketchesKLLAccumulator>,
    ) -> Result<DatasketchesKLLAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }
        let inners: Vec<KllSketch> = accumulators.into_iter().map(|acc| acc.inner).collect();
        let merged_inner = KllSketch::merge(inners)?;
        Ok(Self {
            inner: merged_inner,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datasketches_kll_creation() {
        let kll = DatasketchesKLLAccumulator::new(200);
        assert!(kll.inner.count() == 0);
        assert_eq!(kll.inner.k, 200);
    }

    #[test]
    fn test_datasketches_kll_update() {
        let mut kll = DatasketchesKLLAccumulator::new(200);
        kll.update(10.0);
        kll.update(20.0);
        kll.update(15.0);
        assert_eq!(kll.inner.count(), 3);
    }

    #[test]
    fn test_datasketches_kll_quantile() {
        let mut kll = DatasketchesKLLAccumulator::new(200);
        for i in 1..=10 {
            kll.update(i as f64);
        }
        assert_eq!(kll.get_quantile(0.0), 1.0);
        assert_eq!(kll.get_quantile(1.0), 10.0);
        // Sketchlib KLL is approximate; 0.5 quantile of 1..10 may be 5, 6, or 7.
        let q50 = kll.get_quantile(0.5);
        assert!((q50 - 6.0).abs() <= 1.0, "expected median ~6, got {q50}");
    }

    #[test]
    fn test_datasketches_kll_query() {
        let mut kll = DatasketchesKLLAccumulator::new(200);
        for i in 1..=10 {
            kll.update(i as f64);
        }

        let mut query_kwargs = HashMap::new();
        query_kwargs.insert("quantile".to_string(), "0.5".to_string());
        let result = kll.query(Statistic::Quantile, Some(&query_kwargs)).unwrap();
        // Sketchlib KLL is approximate; 0.5 quantile of 1..10 may be 5, 6, or 7.
        assert!(
            (result - 6.0).abs() <= 1.0,
            "expected median ~6, got {result}"
        );

        assert!(kll.query(Statistic::Sum, Some(&query_kwargs)).is_err());
    }

    #[test]
    fn test_datasketches_kll_merge() {
        let mut kll1 = DatasketchesKLLAccumulator::new(200);
        let mut kll2 = DatasketchesKLLAccumulator::new(200);

        for i in 1..=5 {
            kll1.update(i as f64);
        }
        for i in 6..=10 {
            kll2.update(i as f64);
        }

        let merged = DatasketchesKLLAccumulator::merge_accumulators(vec![kll1, kll2]).unwrap();
        assert_eq!(merged.inner.count(), 10);
        assert_eq!(merged.get_quantile(0.0), 1.0);
        assert_eq!(merged.get_quantile(1.0), 10.0);
    }

    #[test]
    fn test_datasketches_kll_serialization() {
        let mut kll = DatasketchesKLLAccumulator::new(200);
        for i in 1..=5 {
            kll.update(i as f64);
        }

        let bytes = kll.serialize_to_bytes();
        let deserialized =
            DatasketchesKLLAccumulator::deserialize_from_bytes_arroyo(&bytes).unwrap();

        assert_eq!(deserialized.inner.k, 200);
        assert_eq!(deserialized.inner.count(), 5);
        assert_eq!(deserialized.get_quantile(0.0), 1.0);
        assert_eq!(deserialized.get_quantile(1.0), 5.0);
    }

    #[test]
    fn test_datasketches_kll_get_keys() {
        let kll = DatasketchesKLLAccumulator::new(200);
        assert_eq!(kll.type_name(), "DatasketchesKLLAccumulator");
    }

    #[test]
    fn test_trait_object() {
        let mut kll = DatasketchesKLLAccumulator::new(200);
        kll.update(5.0);
        let trait_obj: Box<dyn AggregateCore> = Box::new(kll);
        assert_eq!(trait_obj.type_name(), "DatasketchesKLLAccumulator");
    }

    #[test]
    fn test_datasketches_kll_query_with_kwargs() {
        let mut kll = DatasketchesKLLAccumulator::new(200);
        for i in 1..=10 {
            kll.update(i as f64);
        }

        let mut query_kwargs = HashMap::new();
        query_kwargs.insert("quantile".to_string(), "0.5".to_string());
        let result = kll.query(Statistic::Quantile, Some(&query_kwargs)).unwrap();
        // Sketchlib KLL is approximate; 0.5 quantile of 1..10 may be 5, 6, or 7.
        assert!(
            (result - 6.0).abs() <= 1.0,
            "expected median ~6, got {result}"
        );

        query_kwargs.insert("quantile".to_string(), "0.9".to_string());
        let result = kll.query(Statistic::Quantile, Some(&query_kwargs)).unwrap();
        // Sketchlib KLL is approximate; 0.9 quantile of 1..10 may be 9 or 10.
        assert!(
            (9.0..=10.0).contains(&result),
            "expected 0.9 quantile in [9,10], got {result}"
        );

        query_kwargs.insert("quantile".to_string(), "0.0".to_string());
        assert_eq!(
            kll.query(Statistic::Quantile, Some(&query_kwargs)).unwrap(),
            1.0
        );

        query_kwargs.insert("quantile".to_string(), "1.0".to_string());
        assert_eq!(
            kll.query(Statistic::Quantile, Some(&query_kwargs)).unwrap(),
            10.0
        );

        assert!(kll.query(Statistic::Quantile, None).is_err());

        query_kwargs.insert("quantile".to_string(), "invalid".to_string());
        assert!(kll.query(Statistic::Quantile, Some(&query_kwargs)).is_err());

        query_kwargs.insert("quantile".to_string(), "1.5".to_string());
        assert!(kll.query(Statistic::Quantile, Some(&query_kwargs)).is_err());

        query_kwargs.insert("quantile".to_string(), "-0.1".to_string());
        assert!(kll.query(Statistic::Quantile, Some(&query_kwargs)).is_err());

        query_kwargs.insert("quantile".to_string(), "0.5".to_string());
        assert!(kll.query(Statistic::Sum, Some(&query_kwargs)).is_err());
    }

    #[test]
    fn test_datasketches_kll_merge_multiple() {
        let mut kll1 = DatasketchesKLLAccumulator::new(200);
        let mut kll2 = DatasketchesKLLAccumulator::new(200);
        let mut kll3 = DatasketchesKLLAccumulator::new(200);

        for i in 1..=5 {
            kll1.update(i as f64);
        }
        for i in 6..=10 {
            kll2.update(i as f64);
        }
        for i in 11..=15 {
            kll3.update(i as f64);
        }

        let boxed_accs: Vec<Box<dyn AggregateCore>> =
            vec![Box::new(kll1), Box::new(kll2), Box::new(kll3)];

        let merged = DatasketchesKLLAccumulator::merge_multiple(&boxed_accs).unwrap();
        assert_eq!(merged.inner.count(), 15);
        assert_eq!(merged.get_quantile(0.0), 1.0);
        assert_eq!(merged.get_quantile(1.0), 15.0);
        assert_eq!(merged.get_quantile(0.5), 8.0);
    }

    #[test]
    fn test_datasketches_kll_merge_multiple_error_cases() {
        let empty: Vec<Box<dyn AggregateCore>> = vec![];
        assert!(DatasketchesKLLAccumulator::merge_multiple(&empty).is_err());

        let kll1 = DatasketchesKLLAccumulator::new(200);
        let kll2 = DatasketchesKLLAccumulator::new(100);
        let boxed_accs: Vec<Box<dyn AggregateCore>> = vec![Box::new(kll1), Box::new(kll2)];
        assert!(DatasketchesKLLAccumulator::merge_multiple(&boxed_accs).is_err());

        use crate::precompute_operators::sum_accumulator::SumAccumulator;
        let kll = DatasketchesKLLAccumulator::new(200);
        let sum = SumAccumulator::new();
        let mixed_accs: Vec<Box<dyn AggregateCore>> = vec![Box::new(kll), Box::new(sum)];
        assert!(DatasketchesKLLAccumulator::merge_multiple(&mixed_accs).is_err());
    }
}
