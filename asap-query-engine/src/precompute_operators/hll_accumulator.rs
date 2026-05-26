use std::collections::HashMap;

use asap_sketchlib::sketches::hll::{HllSketch, HllVariant};
use base64::{engine::general_purpose, Engine as _};
use serde_json::Value;

use promql_utilities::query_logics::enums::Statistic;

use crate::data_model::{
    AggregateCore, AggregationType, MergeableAccumulator, SerializableToSink,
    SingleSubpopulationAggregate,
};

/// Default HLL precision when streaming config omits `parameters.precision`.
pub const DEFAULT_HLL_PRECISION: u32 = 14;

/// HLL sketch accumulator — wraps `asap_sketchlib::HllSketch`.
/// Core insert/merge/serde logic lives in `asap_sketchlib`; this file retains
/// QE-specific trait impls.
#[derive(Debug, Clone)]
pub struct HllAccumulator {
    pub inner: HllSketch,
}

impl HllAccumulator {
    pub fn new(precision: u32) -> Self {
        Self {
            inner: HllSketch::new(HllVariant::Regular, precision),
        }
    }

    pub fn with_default_precision() -> Self {
        Self::new(DEFAULT_HLL_PRECISION)
    }

    pub fn update(&mut self, value: f64) {
        self.inner.update(&value.to_le_bytes());
    }

    pub fn estimate(&self) -> f64 {
        self.inner.estimate()
    }

    pub fn precision(&self) -> u32 {
        self.inner.precision
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let inner = HllSketch::deserialize_msgpack(buffer)
            .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;
        Ok(Self { inner })
    }
}

impl Default for HllAccumulator {
    fn default() -> Self {
        Self::with_default_precision()
    }
}

impl SerializableToSink for HllAccumulator {
    fn serialize_to_json(&self) -> Value {
        let bytes = self.inner.serialize_msgpack().unwrap_or_default();
        let b64 = general_purpose::STANDARD.encode(&bytes);
        serde_json::json!({
            "sketch": b64,
            "precision": self.inner.precision,
            "variant": format!("{:?}", self.inner.variant),
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        self.inner.serialize_msgpack().unwrap_or_default()
    }
}

impl AggregateCore for HllAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "HllAccumulator"
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
                "Cannot merge HllAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }
        let other_hll = other
            .as_any()
            .downcast_ref::<HllAccumulator>()
            .ok_or("Failed to downcast to HllAccumulator")?;

        let mut merged = self.inner.clone();
        merged.merge(&other_hll.inner)?;
        Ok(Box::new(Self { inner: merged }))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::HLL
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
        SingleSubpopulationAggregate::query(self, statistic, Some(query_kwargs))
    }
}

impl SingleSubpopulationAggregate for HllAccumulator {
    fn query(
        &self,
        statistic: Statistic,
        _query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        match statistic {
            Statistic::Cardinality => Ok(self.estimate()),
            other => Err(format!("Unsupported statistic in HllAccumulator: {other:?}").into()),
        }
    }

    fn clone_boxed(&self) -> Box<dyn SingleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

impl MergeableAccumulator<HllAccumulator> for HllAccumulator {
    fn merge_accumulators(
        accumulators: Vec<HllAccumulator>,
    ) -> Result<HllAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }
        let mut iter = accumulators.into_iter();
        let mut merged = iter.next().unwrap();
        for acc in iter {
            merged.inner.merge(&acc.inner)?;
        }
        Ok(merged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: insert n unique f64 values and return the accumulator.
    fn build_with_n_unique(n: usize, precision: u32) -> HllAccumulator {
        let mut acc = HllAccumulator::new(precision);
        for i in 0..n {
            acc.update(i as f64);
        }
        acc
    }

    #[test]
    fn new_with_precision_has_zero_estimate() {
        let acc = HllAccumulator::new(14);
        assert_eq!(acc.precision(), 14);
        // Empty register array → estimate exactly 0.0 by the HLL small-range correction.
        assert_eq!(acc.estimate(), 0.0);
    }

    #[test]
    fn default_uses_documented_precision() {
        let acc = HllAccumulator::default();
        assert_eq!(acc.precision(), DEFAULT_HLL_PRECISION);
    }

    #[test]
    fn update_distinct_values_grows_estimate_within_tolerance() {
        // 1000 distinct values at precision 14 should land within the documented
        // ~0.8% standard error (allow 5% to keep this test deterministic; the
        // accuracy-tightness is upstream's `assert_accuracy` test).
        let acc = build_with_n_unique(1000, 14);
        let est = acc.estimate();
        assert!(
            est > 900.0 && est < 1100.0,
            "expected estimate near 1000, got {est}"
        );
    }

    #[test]
    fn duplicates_do_not_increase_estimate() {
        let mut acc = HllAccumulator::new(14);
        for _ in 0..10_000 {
            acc.update(42.0);
        }
        let est = acc.estimate();
        assert!(
            est <= 5.0,
            "estimate after 10k duplicates of one value should be ≈ 1, got {est}"
        );
    }

    #[test]
    fn query_cardinality_returns_estimate() {
        let acc = build_with_n_unique(500, 14);
        let via_trait = SingleSubpopulationAggregate::query(&acc, Statistic::Cardinality, None)
            .expect("Cardinality should be supported");
        assert_eq!(via_trait, acc.estimate());
    }

    #[test]
    fn query_other_statistic_errors() {
        let acc = HllAccumulator::new(14);
        for stat in [
            Statistic::Sum,
            Statistic::Count,
            Statistic::Min,
            Statistic::Max,
            Statistic::Quantile,
            Statistic::Topk,
            Statistic::Rate,
            Statistic::Increase,
        ] {
            assert!(
                SingleSubpopulationAggregate::query(&acc, stat, None).is_err(),
                "HLL should reject {stat:?}",
            );
        }
    }

    #[test]
    fn merge_two_disjoint_sketches_approximates_union_size() {
        // 500 evens + 500 odds = 1000 distinct values total.
        let mut left = HllAccumulator::new(14);
        let mut right = HllAccumulator::new(14);
        for i in 0..500 {
            left.update((i * 2) as f64);
            right.update((i * 2 + 1) as f64);
        }
        let merged = HllAccumulator::merge_accumulators(vec![left.clone(), right])
            .expect("merge of two same-precision sketches should succeed");
        let est = merged.estimate();
        assert!(
            est > 900.0 && est < 1100.0,
            "merged estimate should be ≈1000, got {est}"
        );
        // Left should be untouched (merge_accumulators consumed clones).
        let left_est = left.estimate();
        assert!(
            left_est > 450.0 && left_est < 550.0,
            "left sketch should still report ≈500, got {left_est}"
        );
    }

    #[test]
    fn merge_via_aggregate_core_trait_returns_same_result() {
        // Exercises the AggregateCore::merge_with path used by the query engine
        // when collapsing multiple per-pane sketches at infer time.
        let left = build_with_n_unique(300, 14);
        let mut right = HllAccumulator::new(14);
        for i in 300..700 {
            right.update(i as f64);
        }
        let merged_box = left.merge_with(&right).expect("merge_with should succeed");
        assert_eq!(
            merged_box.get_accumulator_type(),
            AggregationType::HLL,
            "merged accumulator must report HLL type",
        );
        let merged = merged_box
            .as_any()
            .downcast_ref::<HllAccumulator>()
            .expect("downcast HllAccumulator");
        let est = merged.estimate();
        // 700 distinct values; allow 5% tolerance.
        assert!(
            est > 650.0 && est < 750.0,
            "merged estimate should be ≈700, got {est}"
        );
    }

    #[test]
    fn merge_with_wrong_type_errors() {
        // Cross-type merges must fail rather than silently produce garbage.
        // Use any other AggregateCore impl — SetAggregatorAccumulator is the
        // sibling distinct-tracking type and the most likely accidental swap.
        use crate::precompute_operators::SetAggregatorAccumulator;
        let acc = HllAccumulator::new(14);
        let other = SetAggregatorAccumulator::new();
        let result = acc.merge_with(&other);
        assert!(
            result.is_err(),
            "merging HLL with non-HLL accumulator must error",
        );
        let msg = result.err().unwrap().to_string();
        assert!(
            msg.contains("HllAccumulator") || msg.contains("Cannot merge"),
            "error message should mention HllAccumulator, got: {msg}",
        );
    }

    #[test]
    fn msgpack_round_trip_preserves_estimate() {
        // Real serialisation: the register array survives encode→decode and the
        // estimate is identical (modulo the lossless f64 estimator math).
        // This is the property that the existing
        // `datafusion_summary_library::physical::hll::HllSketch` lacks — its
        // to_bytes/from_bytes drop the register state and only persist the count,
        // which would corrupt any merge that happens after a store round-trip.
        let original = build_with_n_unique(2000, 14);
        let bytes = original.serialize_to_bytes();
        assert!(!bytes.is_empty(), "serialize_to_bytes must produce data");
        let restored =
            HllAccumulator::deserialize_from_bytes_arroyo(&bytes).expect("msgpack round trip");
        assert_eq!(restored.precision(), original.precision());
        assert_eq!(restored.estimate(), original.estimate());
        // Bytes must be stable across re-encode (canonical form).
        assert_eq!(restored.serialize_to_bytes(), bytes);
    }

    #[test]
    fn round_trip_then_merge_recovers_full_state() {
        // Regression guard for the lossy-serialisation footgun: serialize, deserialize,
        // then merge new data — if the restored sketch had dropped its register
        // state, the post-merge estimate would underflow.
        let acc_a = build_with_n_unique(1000, 14);
        let bytes = acc_a.serialize_to_bytes();
        let restored =
            HllAccumulator::deserialize_from_bytes_arroyo(&bytes).expect("msgpack round trip");

        let mut acc_b = HllAccumulator::new(14);
        for i in 1000..2000 {
            acc_b.update(i as f64);
        }
        let merged = restored
            .merge_with(&acc_b)
            .expect("merge restored + new must succeed");
        let est = merged
            .as_any()
            .downcast_ref::<HllAccumulator>()
            .unwrap()
            .estimate();
        // 2000 distinct values; allow 5% tolerance.
        assert!(
            est > 1800.0 && est < 2200.0,
            "post-round-trip merge estimate should be ≈2000, got {est}",
        );
    }

    #[test]
    fn json_serialisation_includes_sketch_blob_and_precision() {
        let acc = build_with_n_unique(100, 14);
        let json = acc.serialize_to_json();
        assert!(json.get("sketch").and_then(|v| v.as_str()).is_some());
        assert_eq!(json["precision"], 14);
    }

    #[test]
    fn aggregate_core_query_statistic_dispatches_to_estimate() {
        // The engine path goes through AggregateCore::query_statistic; verify the
        // dispatch reaches the same code as the trait method.
        let acc = build_with_n_unique(500, 14);
        let kwargs = HashMap::new();
        let via_core = acc
            .query_statistic(Statistic::Cardinality, &None, &kwargs)
            .expect("query_statistic should dispatch Cardinality");
        assert_eq!(via_core, acc.estimate());
    }
}
