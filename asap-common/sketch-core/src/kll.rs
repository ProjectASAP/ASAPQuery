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

use crate::config::use_sketchlib_for_kll;
use crate::kll_sketchlib::{
    bytes_from_sketchlib_kll, sketchlib_kll_from_bytes, sketchlib_kll_merge,
    sketchlib_kll_quantile, sketchlib_kll_update, new_sketchlib_kll, SketchlibKll,
};

/// Wire format used in MessagePack serialization (matches Arroyo UDF output).
#[derive(Deserialize, Serialize)]
pub struct KllSketchData {
    pub k: u16,
    pub sketch_bytes: Vec<u8>,
}

/// Backend implementation for KLL Sketch. Only one is active at a time.
pub enum KllBackend {
    /// dsrs (DataSketches) implementation.
    Legacy(KllDoubleSketch),
    /// sketchlib-rust backed implementation.
    Sketchlib(SketchlibKll),
}

impl std::fmt::Debug for KllBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KllBackend::Legacy(_) => write!(f, "Legacy(..)"),
            KllBackend::Sketchlib(_) => write!(f, "Sketchlib(..)"),
        }
    }
}

impl Clone for KllBackend {
    fn clone(&self) -> Self {
        match self {
            KllBackend::Legacy(s) => {
                if s.get_n() == 0 {
                    KllBackend::Legacy(KllDoubleSketch::with_k(200)) // k will be overwritten by KllSketch
                } else {
                    let bytes = s.serialize();
                    KllBackend::Legacy(KllDoubleSketch::deserialize(bytes.as_ref()).unwrap())
                }
            }
            KllBackend::Sketchlib(s) => KllBackend::Sketchlib(s.clone()),
        }
    }
}

pub struct KllSketch {
    pub k: u16,
    pub backend: KllBackend,
}

impl KllSketch {
    pub fn new(k: u16) -> Self {
        let backend = if use_sketchlib_for_kll() {
            KllBackend::Sketchlib(new_sketchlib_kll(k))
        } else {
            KllBackend::Legacy(KllDoubleSketch::with_k(k))
        };
        Self { k, backend }
    }

    /// Returns the raw sketch bytes (for JSON serialization, etc.).
    pub fn sketch_bytes(&self) -> Vec<u8> {
        match &self.backend {
            KllBackend::Legacy(s) => s.serialize().as_ref().to_vec(),
            KllBackend::Sketchlib(s) => bytes_from_sketchlib_kll(s),
        }
    }

    pub fn update(&mut self, value: f64) {
        match &mut self.backend {
            KllBackend::Legacy(s) => s.update(value),
            KllBackend::Sketchlib(s) => sketchlib_kll_update(s, value),
        }
    }

    pub fn count(&self) -> u64 {
        match &self.backend {
            KllBackend::Legacy(s) => s.get_n(),
            KllBackend::Sketchlib(s) => s.count() as u64,
        }
    }

    pub fn get_quantile(&self, quantile: f64) -> f64 {
        if self.count() == 0 {
            return 0.0;
        }
        match &self.backend {
            KllBackend::Legacy(s) => s.get_quantile(quantile),
            KllBackend::Sketchlib(s) => sketchlib_kll_quantile(s, quantile),
        }
    }

    pub fn merge(
        accumulators: Vec<Self>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let k = accumulators[0].k;
        for acc in &accumulators {
            if acc.k != k {
                return Err("Cannot merge KllSketch with different k values".into());
            }
        }

        let mut merged = KllSketch::new(k);
        match &mut merged.backend {
            KllBackend::Legacy(merged_legacy) => {
                for acc in accumulators {
                    if let KllBackend::Legacy(acc_legacy) = acc.backend {
                        merged_legacy.merge(&acc_legacy);
                    } else {
                        return Err("Cannot merge Legacy with Sketchlib KLL".into());
                    }
                }
            }
            KllBackend::Sketchlib(merged_sketchlib) => {
                for acc in accumulators {
                    if let KllBackend::Sketchlib(acc_sketchlib) = &acc.backend {
                        sketchlib_kll_merge(merged_sketchlib, acc_sketchlib);
                    } else {
                        return Err("Cannot merge Sketchlib with Legacy KLL".into());
                    }
                }
            }
        }

        Ok(merged)
    }

    /// Serialize to MessagePack — matches the Arroyo UDF wire format exactly.
    pub fn serialize_msgpack(&self) -> Vec<u8> {
        let sketch_bytes = self.sketch_bytes();
        let serialized = KllSketchData {
            k: self.k,
            sketch_bytes,
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
        let wire: KllSketchData = rmp_serde::from_slice(buffer)
            .map_err(|e| format!("Failed to deserialize KllSketchData from MessagePack: {e}"))?;

        let backend = if use_sketchlib_for_kll() {
            KllBackend::Sketchlib(sketchlib_kll_from_bytes(&wire.sketch_bytes)?)
        } else {
            KllBackend::Legacy(
                KllDoubleSketch::deserialize(&wire.sketch_bytes)
                    .map_err(|e| format!("Failed to deserialize KLL sketch: {e}"))?,
            )
        };

        Ok(Self {
            k: wire.k,
            backend,
        })
    }

    /// Merge from references without cloning.
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
        match &mut merged.backend {
            KllBackend::Legacy(merged_legacy) => {
                for s in sketches {
                    if let KllBackend::Legacy(s_legacy) = &s.backend {
                        merged_legacy.merge(s_legacy);
                    } else {
                        return Err("Cannot merge Legacy with Sketchlib KLL".into());
                    }
                }
            }
            KllBackend::Sketchlib(merged_sketchlib) => {
                for s in sketches {
                    if let KllBackend::Sketchlib(s_sketchlib) = &s.backend {
                        sketchlib_kll_merge(merged_sketchlib, s_sketchlib);
                    } else {
                        return Err("Cannot merge Sketchlib with Legacy KLL".into());
                    }
                }
            }
        }
        Ok(merged)
    }

    /// Deserialize from a raw datasketches byte buffer (legacy Flink/FlinkSketch format).
    pub fn from_dsrs_bytes(bytes: &[u8], k: u16) -> Result<Self, Box<dyn std::error::Error>> {
        let sketch = KllDoubleSketch::deserialize(bytes)
            .map_err(|e| format!("Failed to deserialize KLL sketch from dsrs bytes: {e}"))?;
        Ok(Self {
            k,
            backend: KllBackend::Legacy(sketch),
        })
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

// Manual trait implementations since the C++ and sketchlib types don't provide Clone
impl Clone for KllSketch {
    fn clone(&self) -> Self {
        let backend = match &self.backend {
            KllBackend::Legacy(sketch) => {
                let new_sketch = if sketch.get_n() == 0 {
                    KllDoubleSketch::with_k(self.k)
                } else {
                    let bytes = sketch.serialize();
                    KllDoubleSketch::deserialize(bytes.as_ref()).unwrap()
                };
                KllBackend::Legacy(new_sketch)
            }
            KllBackend::Sketchlib(s) => {
                let bytes = bytes_from_sketchlib_kll(s);
                KllBackend::Sketchlib(sketchlib_kll_from_bytes(&bytes).unwrap())
            }
        };
        Self {
            k: self.k,
            backend,
        }
    }
}

impl std::fmt::Debug for KllSketch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KllSketch")
            .field("k", &self.k)
            .field("sketch_n", &self.count())
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
        assert_eq!(kll.count(), 0);
        assert_eq!(kll.k, 200);
    }

    #[test]
    fn test_kll_update() {
        let mut kll = KllSketch::new(200);
        kll.update(10.0);
        kll.update(20.0);
        kll.update(15.0);
        assert_eq!(kll.count(), 3);
    }

    #[test]
    fn test_kll_quantile() {
        let mut kll = KllSketch::new(200);
        for i in 1..=10 {
            kll.update(i as f64);
        }
        assert_eq!(kll.get_quantile(0.0), 1.0);
        assert_eq!(kll.get_quantile(1.0), 10.0);
        let median = kll.get_quantile(0.5);
        assert!(
            (5.0..=6.0).contains(&median),
            "median should be between 5 and 6; got {median}"
        );
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
        assert_eq!(merged.count(), 10);
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
        assert_eq!(deserialized.count(), 5);
        assert_eq!(deserialized.get_quantile(0.0), 1.0);
        assert_eq!(deserialized.get_quantile(1.0), 5.0);
    }

    #[test]
    fn test_aggregate_kll() {
        let values = [1.0, 2.0, 3.0, 4.0, 5.0];
        let bytes = KllSketch::aggregate_kll(200, &values).unwrap();
        let kll = KllSketch::deserialize_msgpack(&bytes).unwrap();
        assert_eq!(kll.count(), 5);
        assert_eq!(kll.get_quantile(0.0), 1.0);
        assert_eq!(kll.get_quantile(1.0), 5.0);
    }

    #[test]
    fn test_aggregate_kll_empty() {
        assert!(KllSketch::aggregate_kll(200, &[]).is_none());
    }
}
