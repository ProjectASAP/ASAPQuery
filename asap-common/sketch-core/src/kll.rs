// Adapted from QueryEngineRust/src/precompute_operators/datasketches_kll_accumulator.rs
// Changes:
//   - Renamed DatasketchesKLLAccumulator -> KllSketch
//   - KllSketchData made pub (used by hydra_kll)
//   - _update -> pub update
//   - serialize_to_bytes (trait) -> serialize_msgpack (inherent method)
//   - deserialize_from_bytes_arroyo -> deserialize_msgpack
//   - merge_accumulators -> merge
//   - Removed: deserialize_from_json, deserialize_from_bytes (legacy QE formats, stay in QE)
//   - Removed: merge_multiple (QE trait-object helper, stays in QE)
//   - Removed: AggregateCore, SerializableToSink, MergeableAccumulator, SingleSubpopulationAggregate impls
//   - Removed: base64, serde_json, tracing imports (QE-specific)
//   - Added: aggregate_kll() one-shot helper

use core::panic;
use dsrs::KllDoubleSketch;
use serde::{Deserialize, Serialize};

/// Wire format used in MessagePack serialization (matches Arroyo UDF output).
#[derive(Deserialize, Serialize)]
pub struct KllSketchData {
    pub k: u16,
    pub sketch_bytes: Vec<u8>,
}

pub struct KllSketch {
    pub k: u16,
    pub sketch: KllDoubleSketch,
}

impl KllSketch {
    pub fn new(k: u16) -> Self {
        Self {
            k,
            sketch: KllDoubleSketch::with_k(k),
        }
    }

    pub fn update(&mut self, value: f64) {
        self.sketch.update(value);
    }

    pub fn get_quantile(&self, quantile: f64) -> f64 {
        if self.sketch.get_n() == 0 {
            return 0.0;
        }
        self.sketch.get_quantile(quantile)
    }

    pub fn merge(
        accumulators: Vec<Self>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        // check K values for all and merge
        let k = accumulators[0].k;
        for acc in &accumulators {
            if acc.k != k {
                return Err("Cannot merge KllSketch with different k values".into());
            }
        }

        let mut merged = KllSketch::new(k);
        for accumulator in accumulators {
            merged.sketch.merge(&accumulator.sketch);
        }

        Ok(merged)
    }

    /// Serialize to MessagePack — matches the Arroyo UDF wire format exactly.
    pub fn serialize_msgpack(&self) -> Vec<u8> {
        // Create KllSketchData compatible with deserialize_msgpack()
        // This matches exactly what the Arroyo UDF does
        let sketch_data = self.sketch.serialize();
        let serialized = KllSketchData {
            k: self.k,
            sketch_bytes: sketch_data.as_ref().to_vec(),
        };

        let mut buf = Vec::new();
        match rmp_serde::encode::write(&mut buf, &serialized) {
            Ok(_) => buf,
            Err(_) => {
                panic!("Failed to serialize KllSketchData to MessagePack");
            }
        }
    }

    /// Deserialize from MessagePack produced by the Arroyo UDF.
    pub fn deserialize_msgpack(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let deserialized_sketch_data: KllSketchData = rmp_serde::from_slice(buffer)
            .map_err(|e| format!("Failed to deserialize KllSketchData from MessagePack: {e}"))?;

        let sketch: KllDoubleSketch =
            KllDoubleSketch::deserialize(&deserialized_sketch_data.sketch_bytes)
                .map_err(|e| format!("Failed to deserialize KLL sketch: {e}"))?;

        Ok(Self {
            k: deserialized_sketch_data.k,
            sketch,
        })
    }

    /// Merge from references without cloning — possible because KllDoubleSketch::merge
    /// takes &other (the underlying C++ merge API is borrow-based).
    pub fn merge_refs(
        sketches: &[&Self],
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if sketches.is_empty() {
            return Err("No sketches to merge".into());
        }
        let k = sketches[0].k;
        for s in sketches {
            if s.k != k {
                return Err("Cannot merge KllSketch with different k values".into());
            }
        }
        let mut merged = Self::new(k);
        for s in sketches {
            merged.sketch.merge(&s.sketch);
        }
        Ok(merged)
    }

    /// Deserialize from a raw datasketches byte buffer (legacy Flink/FlinkSketch format).
    /// Used by QE's legacy deserializers to avoid a direct dsrs dependency there.
    pub fn from_dsrs_bytes(bytes: &[u8], k: u16) -> Result<Self, Box<dyn std::error::Error>> {
        let sketch = KllDoubleSketch::deserialize(bytes)
            .map_err(|e| format!("Failed to deserialize KLL sketch from dsrs bytes: {e}"))?;
        Ok(Self { k, sketch })
    }

    /// One-shot aggregation for the Arroyo UDAF call pattern.
    pub fn aggregate_kll(k: u16, values: &[f64]) -> Option<Vec<u8>> {
        if values.is_empty() {
            return None;
        }
        let mut sketch = Self::new(k);
        for &value in values {
            sketch.update(value);
        }
        Some(sketch.serialize_msgpack())
    }
}

// Manual trait implementations since the C++ library doesn't provide them
impl Clone for KllSketch {
    fn clone(&self) -> Self {
        let bytes = self.sketch.serialize();
        let new_sketch = KllDoubleSketch::deserialize(bytes.as_ref()).unwrap();
        Self {
            k: self.k,
            sketch: new_sketch,
        }
    }
}

impl std::fmt::Debug for KllSketch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KllSketch")
            .field("k", &self.k)
            .field("sketch_n", &self.sketch.get_n())
            .finish()
    }
}

// TODO: verify this
// Thread safety: The C++ library is not thread-safe by default, but since we're using it
// in a single-threaded context per accumulator instance and only sharing read-only operations,
// this should be safe. The actual sketch data is immutable once created.
unsafe impl Send for KllSketch {}
unsafe impl Sync for KllSketch {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kll_creation() {
        let kll = KllSketch::new(200);
        assert!(kll.sketch.get_n() == 0);
        assert_eq!(kll.k, 200);
    }

    #[test]
    fn test_kll_update() {
        let mut kll = KllSketch::new(200);
        kll.update(10.0);
        kll.update(20.0);
        kll.update(15.0);
        assert_eq!(kll.sketch.get_n(), 3);
    }

    #[test]
    fn test_kll_quantile() {
        let mut kll = KllSketch::new(200);
        for i in 1..=10 {
            kll.update(i as f64);
        }
        assert_eq!(kll.get_quantile(0.0), 1.0);
        assert_eq!(kll.get_quantile(1.0), 10.0);
        assert_eq!(kll.get_quantile(0.5), 6.0);
    }

    #[test]
    fn test_kll_merge() {
        let mut kll1 = KllSketch::new(200);
        let mut kll2 = KllSketch::new(200);

        for i in 1..=5 {
            kll1.update(i as f64);
        }
        for i in 6..=10 {
            kll2.update(i as f64);
        }

        let merged = KllSketch::merge(vec![kll1, kll2]).unwrap();
        assert_eq!(merged.sketch.get_n(), 10);
        assert_eq!(merged.get_quantile(0.0), 1.0);
        assert_eq!(merged.get_quantile(1.0), 10.0);
    }

    #[test]
    fn test_msgpack_round_trip() {
        let mut kll = KllSketch::new(200);
        for i in 1..=5 {
            kll.update(i as f64);
        }

        let bytes = kll.serialize_msgpack();
        let deserialized = KllSketch::deserialize_msgpack(&bytes).unwrap();

        assert_eq!(deserialized.k, 200);
        assert_eq!(deserialized.sketch.get_n(), 5);
        assert_eq!(deserialized.get_quantile(0.0), 1.0);
        assert_eq!(deserialized.get_quantile(1.0), 5.0);
    }

    #[test]
    fn test_aggregate_kll() {
        let values = [1.0, 2.0, 3.0, 4.0, 5.0];
        let bytes = KllSketch::aggregate_kll(200, &values).unwrap();
        let kll = KllSketch::deserialize_msgpack(&bytes).unwrap();
        assert_eq!(kll.sketch.get_n(), 5);
        assert_eq!(kll.get_quantile(0.0), 1.0);
        assert_eq!(kll.get_quantile(1.0), 5.0);
    }

    #[test]
    fn test_aggregate_kll_empty() {
        assert!(KllSketch::aggregate_kll(200, &[]).is_none());
    }
}
