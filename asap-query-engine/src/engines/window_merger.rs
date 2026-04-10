//! Window merging strategies for range queries
//!
//! This module provides the `WindowMerger` trait and implementations for
//! merging buckets in a sliding window. The abstraction allows swapping
//! merge strategies:
//!
//! - `NaiveMerger`: Re-merge all buckets each step (current implementation)
//! - `IncrementalMerger`: Add/subtract for subtractable accumulators (future)
//! - `SwagMerger`: Two-stack queue for non-subtractable accumulators (future)

use crate::data_model::{AggregateCore, AggregationType};

/// Trait for merging buckets in a sliding window
///
/// This abstraction allows swapping merge strategies:
/// - NaiveMerger: Re-merge all buckets each step (current implementation)
/// - IncrementalMerger: Add/subtract for subtractable accumulators (future)
/// - SwagMerger: Two-stack queue for non-subtractable accumulators (future)
pub trait WindowMerger: Send + Sync {
    /// Initialize with the first window's buckets
    fn initialize(&mut self, buckets: Vec<Box<dyn AggregateCore>>);

    /// Slide the window: remove `remove_count` old buckets, add new buckets
    fn slide(&mut self, remove_count: usize, new_buckets: Vec<Box<dyn AggregateCore>>);

    /// Get the current merged result
    fn get_merged(&self) -> Result<Box<dyn AggregateCore>, String>;

    /// Check if the window has been initialized
    fn is_initialized(&self) -> bool;
}

/// Naive implementation that re-merges all buckets each time
///
/// This is the simplest implementation with O(n) merge per step.
/// It's suitable for small windows or when optimization isn't critical.
pub struct NaiveMerger {
    buckets: Vec<Box<dyn AggregateCore>>,
}

impl NaiveMerger {
    pub fn new() -> Self {
        Self {
            buckets: Vec::new(),
        }
    }

    fn merge_all(&self) -> Result<Box<dyn AggregateCore>, String> {
        if self.buckets.is_empty() {
            return Err("No buckets to merge".to_string());
        }

        let mut result = self.buckets[0].clone_boxed_core();
        for bucket in &self.buckets[1..] {
            result = result
                .merge_with(bucket.as_ref())
                .map_err(|e| format!("Merge failed: {}", e))?;
        }
        Ok(result)
    }
}

impl Default for NaiveMerger {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowMerger for NaiveMerger {
    fn initialize(&mut self, buckets: Vec<Box<dyn AggregateCore>>) {
        self.buckets = buckets;
    }

    fn slide(&mut self, remove_count: usize, new_buckets: Vec<Box<dyn AggregateCore>>) {
        // Remove old buckets from front
        self.buckets.drain(0..remove_count.min(self.buckets.len()));
        // Add new buckets to back
        self.buckets.extend(new_buckets);
    }

    fn get_merged(&self) -> Result<Box<dyn AggregateCore>, String> {
        self.merge_all()
    }

    fn is_initialized(&self) -> bool {
        !self.buckets.is_empty()
    }
}

/// Factory function to create appropriate merger based on accumulator type
///
/// For now, always returns NaiveMerger. Future implementations could return
/// optimized mergers based on the accumulator type:
/// - IncrementalMerger for subtractable accumulators (Sum, CountMinSketch)
/// - SwagMerger for non-subtractable accumulators (KLL, MinMax)
#[allow(dead_code)]
pub fn create_window_merger(_accumulator_type: AggregationType) -> Box<dyn WindowMerger> {
    // Future implementation:
    // match accumulator_type {
    //     "SumAccumulator" | "CountMinSketchAccumulator" => Box::new(IncrementalMerger::new()),
    //     "DatasketchesKLLAccumulator" | "MinMaxAccumulator" => Box::new(SwagMerger::new()),
    //     _ => Box::new(NaiveMerger::new()),
    // }
    Box::new(NaiveMerger::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::{KeyByLabelValues, SerializableToSink};
    use serde_json::Value;
    use std::any::Any;

    /// Mock accumulator for testing - simply sums values
    #[derive(Clone, Debug)]
    struct MockSumAccumulator {
        value: f64,
    }

    impl MockSumAccumulator {
        fn new(value: f64) -> Self {
            Self { value }
        }
    }

    impl SerializableToSink for MockSumAccumulator {
        fn serialize_to_json(&self) -> Value {
            serde_json::json!({"value": self.value})
        }

        fn serialize_to_bytes(&self) -> Vec<u8> {
            self.value.to_le_bytes().to_vec()
        }
    }

    impl AggregateCore for MockSumAccumulator {
        fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
            Box::new(self.clone())
        }

        fn type_name(&self) -> &'static str {
            "MockSumAccumulator"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn merge_with(
            &self,
            other: &dyn AggregateCore,
        ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
            if let Some(other_mock) = other.as_any().downcast_ref::<MockSumAccumulator>() {
                Ok(Box::new(MockSumAccumulator::new(
                    self.value + other_mock.value,
                )))
            } else {
                Err("Cannot merge with different accumulator type".into())
            }
        }

        fn get_accumulator_type(&self) -> AggregationType {
            AggregationType::Sum
        }

        fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
            None
        }
    }

    // Basic structure tests

    #[test]
    fn test_naive_merger_creation() {
        let merger = NaiveMerger::new();
        assert!(!merger.is_initialized());
    }

    #[test]
    fn test_naive_merger_empty_merge_error() {
        let merger = NaiveMerger::new();
        let result = merger.get_merged();
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), "No buckets to merge");
    }

    #[test]
    fn test_create_window_merger() {
        let merger = create_window_merger(AggregationType::Sum);
        assert!(!merger.is_initialized());
    }

    // Tests with MockSumAccumulator

    #[test]
    fn test_naive_merger_initialize() {
        let mut merger = NaiveMerger::new();
        assert!(!merger.is_initialized());

        let buckets: Vec<Box<dyn AggregateCore>> = vec![
            Box::new(MockSumAccumulator::new(10.0)),
            Box::new(MockSumAccumulator::new(20.0)),
        ];
        merger.initialize(buckets);

        assert!(merger.is_initialized());
    }

    #[test]
    fn test_naive_merger_get_merged_single_bucket() {
        let mut merger = NaiveMerger::new();
        let buckets: Vec<Box<dyn AggregateCore>> = vec![Box::new(MockSumAccumulator::new(42.0))];
        merger.initialize(buckets);

        let result = merger.get_merged();
        assert!(result.is_ok());

        let merged = result.unwrap();
        let mock = merged
            .as_any()
            .downcast_ref::<MockSumAccumulator>()
            .unwrap();
        assert_eq!(mock.value, 42.0);
    }

    #[test]
    fn test_naive_merger_get_merged_multiple_buckets() {
        let mut merger = NaiveMerger::new();
        let buckets: Vec<Box<dyn AggregateCore>> = vec![
            Box::new(MockSumAccumulator::new(10.0)),
            Box::new(MockSumAccumulator::new(20.0)),
            Box::new(MockSumAccumulator::new(30.0)),
        ];
        merger.initialize(buckets);

        let result = merger.get_merged();
        assert!(result.is_ok());

        let merged = result.unwrap();
        let mock = merged
            .as_any()
            .downcast_ref::<MockSumAccumulator>()
            .unwrap();
        assert_eq!(mock.value, 60.0); // 10 + 20 + 30
    }

    #[test]
    fn test_naive_merger_slide_removes_old_buckets() {
        let mut merger = NaiveMerger::new();
        // Initialize with [10, 20, 30]
        let buckets: Vec<Box<dyn AggregateCore>> = vec![
            Box::new(MockSumAccumulator::new(10.0)),
            Box::new(MockSumAccumulator::new(20.0)),
            Box::new(MockSumAccumulator::new(30.0)),
        ];
        merger.initialize(buckets);

        // Slide: remove 1 old, add [40]
        // Result should be [20, 30, 40]
        let new_buckets: Vec<Box<dyn AggregateCore>> =
            vec![Box::new(MockSumAccumulator::new(40.0))];
        merger.slide(1, new_buckets);

        let result = merger.get_merged();
        assert!(result.is_ok());

        let merged = result.unwrap();
        let mock = merged
            .as_any()
            .downcast_ref::<MockSumAccumulator>()
            .unwrap();
        assert_eq!(mock.value, 90.0); // 20 + 30 + 40
    }

    #[test]
    fn test_naive_merger_slide_removes_multiple_buckets() {
        let mut merger = NaiveMerger::new();
        // Initialize with [10, 20, 30, 40]
        let buckets: Vec<Box<dyn AggregateCore>> = vec![
            Box::new(MockSumAccumulator::new(10.0)),
            Box::new(MockSumAccumulator::new(20.0)),
            Box::new(MockSumAccumulator::new(30.0)),
            Box::new(MockSumAccumulator::new(40.0)),
        ];
        merger.initialize(buckets);

        // Slide: remove 2 old, add [50, 60]
        // Result should be [30, 40, 50, 60]
        let new_buckets: Vec<Box<dyn AggregateCore>> = vec![
            Box::new(MockSumAccumulator::new(50.0)),
            Box::new(MockSumAccumulator::new(60.0)),
        ];
        merger.slide(2, new_buckets);

        let result = merger.get_merged();
        assert!(result.is_ok());

        let merged = result.unwrap();
        let mock = merged
            .as_any()
            .downcast_ref::<MockSumAccumulator>()
            .unwrap();
        assert_eq!(mock.value, 180.0); // 30 + 40 + 50 + 60
    }

    #[test]
    fn test_naive_merger_slide_with_empty_new_buckets() {
        let mut merger = NaiveMerger::new();
        let buckets: Vec<Box<dyn AggregateCore>> = vec![
            Box::new(MockSumAccumulator::new(10.0)),
            Box::new(MockSumAccumulator::new(20.0)),
            Box::new(MockSumAccumulator::new(30.0)),
        ];
        merger.initialize(buckets);

        // Slide: remove 1 old, add nothing
        // Result should be [20, 30]
        merger.slide(1, vec![]);

        let result = merger.get_merged();
        assert!(result.is_ok());

        let merged = result.unwrap();
        let mock = merged
            .as_any()
            .downcast_ref::<MockSumAccumulator>()
            .unwrap();
        assert_eq!(mock.value, 50.0); // 20 + 30
    }

    #[test]
    fn test_naive_merger_slide_remove_more_than_exists() {
        let mut merger = NaiveMerger::new();
        let buckets: Vec<Box<dyn AggregateCore>> = vec![
            Box::new(MockSumAccumulator::new(10.0)),
            Box::new(MockSumAccumulator::new(20.0)),
        ];
        merger.initialize(buckets);

        // Slide: try to remove 5 (more than exists), add [30]
        // Should only remove what exists, result should be [30]
        let new_buckets: Vec<Box<dyn AggregateCore>> =
            vec![Box::new(MockSumAccumulator::new(30.0))];
        merger.slide(5, new_buckets);

        let result = merger.get_merged();
        assert!(result.is_ok());

        let merged = result.unwrap();
        let mock = merged
            .as_any()
            .downcast_ref::<MockSumAccumulator>()
            .unwrap();
        assert_eq!(mock.value, 30.0);
    }

    #[test]
    fn test_naive_merger_simulates_sliding_window() {
        let mut merger = NaiveMerger::new();

        // Simulate a sliding window of size 3 with step 1
        // Window 1: [10, 20, 30] = 60
        let buckets: Vec<Box<dyn AggregateCore>> = vec![
            Box::new(MockSumAccumulator::new(10.0)),
            Box::new(MockSumAccumulator::new(20.0)),
            Box::new(MockSumAccumulator::new(30.0)),
        ];
        merger.initialize(buckets);

        let result1 = merger.get_merged().unwrap();
        let mock1 = result1
            .as_any()
            .downcast_ref::<MockSumAccumulator>()
            .unwrap();
        assert_eq!(mock1.value, 60.0);

        // Window 2: [20, 30, 40] = 90
        merger.slide(1, vec![Box::new(MockSumAccumulator::new(40.0))]);
        let result2 = merger.get_merged().unwrap();
        let mock2 = result2
            .as_any()
            .downcast_ref::<MockSumAccumulator>()
            .unwrap();
        assert_eq!(mock2.value, 90.0);

        // Window 3: [30, 40, 50] = 120
        merger.slide(1, vec![Box::new(MockSumAccumulator::new(50.0))]);
        let result3 = merger.get_merged().unwrap();
        let mock3 = result3
            .as_any()
            .downcast_ref::<MockSumAccumulator>()
            .unwrap();
        assert_eq!(mock3.value, 120.0);
    }
}
