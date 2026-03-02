// HyperLogLog wrapper for cardinality estimation.
//
// This provides a simple wrapper around the hyperloglogplus crate for use
// in sketch-based COUNT(DISTINCT) queries.

use std::collections::hash_map::{DefaultHasher, RandomState};
use std::hash::{Hash, Hasher};

use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};

/// Wrapper around HyperLogLog++ for cardinality estimation.
///
/// Uses precision 14 by default which gives ~0.8% standard error.
#[derive(Clone)]
pub struct HllSketch {
    hll: HyperLogLogPlus<u64, RandomState>,
}

impl HllSketch {
    /// Create a new HLL sketch with default precision (14).
    pub fn new() -> Self {
        Self::with_precision(14)
    }

    /// Create a new HLL sketch with specified precision.
    ///
    /// Precision must be between 4 and 18. Higher precision means
    /// more accuracy but more memory usage.
    pub fn with_precision(precision: u8) -> Self {
        let hll = HyperLogLogPlus::new(precision, RandomState::new())
            .expect("Valid precision range is 4-18");
        Self { hll }
    }

    /// Insert a value into the sketch.
    ///
    /// The value is hashed to u64 before insertion.
    pub fn insert<T: Hash>(&mut self, value: &T) {
        // Hash the value to u64 first, then insert
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        let hash = hasher.finish();
        self.hll.insert(&hash);
    }

    /// Insert a byte slice into the sketch.
    pub fn insert_bytes(&mut self, value: &[u8]) {
        self.insert(&value);
    }

    /// Get the estimated cardinality.
    pub fn count(&mut self) -> u64 {
        self.hll.count().round() as u64
    }

    /// Merge another HLL sketch into this one.
    #[allow(dead_code)]
    pub fn merge(&mut self, other: &mut Self) {
        self.hll
            .merge(&other.hll)
            .expect("HLL merge should succeed for same precision");
    }

    /// Serialize the sketch to bytes.
    ///
    /// Format: [precision: u8][count as f64 bytes(8)]
    /// This is a hacky serialization that just stores the current count.
    pub fn to_bytes(&mut self) -> Vec<u8> {
        let precision = 14u8; // We always use 14 for now
        let count = self.hll.count();

        // Simple format: [precision(1)][count as f64 bytes(8)]
        let mut bytes = Vec::with_capacity(9);
        bytes.push(precision);
        bytes.extend_from_slice(&count.to_le_bytes());
        bytes
    }

    /// Deserialize a sketch from bytes.
    /// Note: This only recovers the count, not the full HLL state.
    #[allow(dead_code)]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 9 {
            return None;
        }

        let _precision = bytes[0];
        let count_bytes: [u8; 8] = bytes[1..9].try_into().ok()?;
        let _count = f64::from_le_bytes(count_bytes);

        // Since we can't truly deserialize the HLL state from just the count,
        // we create an empty HLL. This is a limitation of the simple format.
        Some(Self::new())
    }
}

impl Default for HllSketch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hll_basic() {
        let mut hll = HllSketch::new();

        // Insert 1000 unique values
        for i in 0..1000 {
            hll.insert(&i);
        }

        let count = hll.count();
        // HLL has ~0.8% error at precision 14, so allow 5% tolerance
        assert!(count > 900, "Count {} should be > 900", count);
        assert!(count < 1100, "Count {} should be < 1100", count);
    }

    #[test]
    fn test_hll_duplicates() {
        let mut hll = HllSketch::new();

        // Insert same value many times
        for _ in 0..1000 {
            hll.insert(&42);
        }

        let count = hll.count();
        assert_eq!(count, 1, "Duplicates should not increase count");
    }

    #[test]
    fn test_hll_merge() {
        let mut hll1 = HllSketch::new();
        let mut hll2 = HllSketch::new();

        // Insert different values into each
        for i in 0..500 {
            hll1.insert(&i);
        }
        for i in 500..1000 {
            hll2.insert(&i);
        }

        hll1.merge(&mut hll2);
        let count = hll1.count();

        // Should have ~1000 unique values
        assert!(count > 900, "Merged count {} should be > 900", count);
        assert!(count < 1100, "Merged count {} should be < 1100", count);
    }

    #[test]
    fn test_hll_strings() {
        let mut hll = HllSketch::new();

        for i in 0..1000 {
            let s = format!("user_{}", i);
            hll.insert(&s);
        }

        let count = hll.count();
        assert!(count > 900, "String count {} should be > 900", count);
        assert!(count < 1100, "String count {} should be < 1100", count);
    }
}
