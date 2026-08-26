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

        crate::engines::merge_utils::merge_accumulators_batch(&self.buckets)
            .map_err(|e| format!("Merge failed: {}", e))
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
    use crate::precompute_operators::{
        CountMinSketchAccumulator, DatasketchesKLLAccumulator, SumAccumulator,
    };
    use crate::tests::test_utilities::{
        cms_from_matrix, oracle_sequential_fold, PoisonableAccumulator,
    };
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

        fn query_statistic(
            &self,
            _statistic: promql_utilities::query_logics::enums::Statistic,
            _key: &Option<KeyByLabelValues>,
            _query_kwargs: &std::collections::HashMap<String, String>,
        ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
            Err("MockSumAccumulator does not support query_statistic".into())
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

    /// Pins a subtle, easy-to-miss requirement that `execute_range_query_pipeline`'s
    /// per-step keys merge (#583) depends on: `NaiveMerger` folds buckets
    /// *pairwise, left-to-right* (each `get_merged` walks `buckets[0].merge_with(buckets[1])`,
    /// then that result `.merge_with(buckets[2])`, and so on) rather than
    /// passing the whole slice to a single flat N-way merge. For most
    /// accumulators the distinction is invisible (merging is associative and
    /// commutative). For `DeltaSetAggregatorAccumulator` it used to matter:
    /// before #586, a flat N-way `merge_accumulators` call unioned every
    /// bucket's added/removed sets before resolving conflicts, so a key that
    /// toggled more than once lost its final state, while the pairwise
    /// sequential fold folded each toggle in order and got it right. After
    /// #586's fix, `merge_accumulators` itself folds its input in order
    /// (treating the first element as the starting state and each
    /// subsequent one as the next chronological bucket), so a flat call over
    /// a chronologically-ordered `Vec` and a pairwise-sequential fold over
    /// the same buckets are now equivalent -- both are just a left-fold in
    /// the same order, expressed two different ways.
    ///
    /// Grows the window one bucket at a time (add, remove, add, remove, add)
    /// via `slide(0, ..)` -- mirroring how the range pipeline's per-step
    /// window for `DeltaSetAggregator` only ever grows, never expires -- and
    /// checks `get_merged()` after *every* addition, not just the final one,
    /// so a fold that's only correct at the boundary (e.g. an implementation
    /// that happens to get the last step right by luck) can't hide.
    ///
    /// If a future change ever makes `merge_accumulators` order-insensitive
    /// again (e.g. reverting to a union-based approach), the final assertion
    /// below -- that a flat call agrees with the pairwise sequential fold --
    /// is the tripwire that catches it.
    #[test]
    fn naive_merger_sequential_fold_replays_delta_set_toggles_at_every_window() {
        use crate::data_model::traits::MergeableAccumulator;
        use crate::precompute_operators::DeltaSetAggregatorAccumulator;

        let key = KeyByLabelValues::new_with_labels(vec!["host-a".to_string()]);

        let mut add = DeltaSetAggregatorAccumulator::new();
        add.add_key(key.clone());
        let mut remove = DeltaSetAggregatorAccumulator::new();
        remove.remove_key(key.clone());

        // add, remove, add, remove, add -> present, absent, present, absent, present
        let deltas = [
            add.clone(),
            remove.clone(),
            add.clone(),
            remove.clone(),
            add.clone(),
        ];
        let expected_present = [true, false, true, false, true];

        let mut merger = NaiveMerger::new();
        merger.initialize(vec![Box::new(deltas[0].clone())]);
        let mismatches: Vec<String> = expected_present
            .iter()
            .enumerate()
            .filter_map(|(i, &expected)| {
                if i > 0 {
                    merger.slide(0, vec![Box::new(deltas[i].clone())]);
                }
                let merged = merger.get_merged().unwrap();
                let actual = merged
                    .get_keys()
                    .expect("no unresolved removals after a sequential fold")
                    .contains(&key);
                (actual != expected)
                    .then(|| format!("window {}: expected {expected}, got {actual}", i + 1))
            })
            .collect();
        assert!(
            mismatches.is_empty(),
            "NaiveMerger's sequential left-fold must replay each toggle chronologically \
             at every window, not just the final one -- diverged at: {mismatches:?}"
        );

        // Contrast: the same 5 buckets merged in one flat call now agree
        // with the pairwise sequential fold -- both are a left-fold over the
        // same chronologically-ordered buckets, just expressed differently.
        let flat_result = DeltaSetAggregatorAccumulator::merge_accumulators(deltas.to_vec())
            .expect("flat merge should succeed");
        assert!(
            flat_result.get_keys().unwrap().contains(&key),
            "a flat merge_accumulators call over chronologically-ordered buckets must \
             agree with NaiveMerger's pairwise sequential fold over the same buckets"
        );
    }

    // ---- Issue #596 regression tests ----
    //
    // NaiveMerger::merge_all is supposed to gain the CMS/KLL batch-merge fast
    // path (merge_multiple) that SimpleEngine::merge_accumulators already had,
    // falling back to a sequential fold that ABORTS on the first merge_with
    // error (rather than warning-and-continuing or silently dropping a bucket).
    //
    // These tests only observe behavior through the public WindowMerger API
    // (initialize/slide/get_merged/is_initialized) and a manually-computed
    // oracle fold, so they don't assume which code path (fast or fallback)
    // NaiveMerger actually takes for a given input.

    #[test]
    fn naive_merger_cms_batch_matches_manual_sequential_fold() {
        let cms_boxes: Vec<Box<dyn AggregateCore>> = vec![
            Box::new(cms_from_matrix(
                vec![vec![5.0, 0.0, 0.0], vec![0.0, 1.0, 0.0]],
                2,
                3,
            )),
            Box::new(cms_from_matrix(
                vec![vec![2.0, 3.0, 0.0], vec![0.0, 0.0, 4.0]],
                2,
                3,
            )),
            Box::new(cms_from_matrix(
                vec![vec![0.0, 0.0, 7.0], vec![1.0, 0.0, 0.0]],
                2,
                3,
            )),
        ];
        let for_merger: Vec<Box<dyn AggregateCore>> = cms_boxes.to_vec();

        let oracle = oracle_sequential_fold(&cms_boxes);
        let oracle_cms = oracle
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .unwrap();

        let mut merger = NaiveMerger::new();
        merger.initialize(for_merger);
        let merged = merger
            .get_merged()
            .expect("NaiveMerger should merge a same-typed CMS batch");
        let merged_cms = merged
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .unwrap();

        assert_eq!(
            merged_cms.inner.sketch(),
            oracle_cms.inner.sketch(),
            "NaiveMerger's CMS batch merge must match a manual sequential merge_with fold"
        );
    }

    #[test]
    fn naive_merger_kll_batch_matches_manual_sequential_fold() {
        let mut k1 = DatasketchesKLLAccumulator::new(200);
        for i in 1..=5 {
            k1.update(i as f64);
        }
        let mut k2 = DatasketchesKLLAccumulator::new(200);
        for i in 6..=10 {
            k2.update(i as f64);
        }
        let mut k3 = DatasketchesKLLAccumulator::new(200);
        for i in 11..=15 {
            k3.update(i as f64);
        }

        let boxes: Vec<Box<dyn AggregateCore>> = vec![Box::new(k1), Box::new(k2), Box::new(k3)];
        let for_merger: Vec<Box<dyn AggregateCore>> = boxes.to_vec();

        let oracle = oracle_sequential_fold(&boxes);
        let oracle_kll = oracle
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .unwrap();

        let mut merger = NaiveMerger::new();
        merger.initialize(for_merger);
        let merged = merger
            .get_merged()
            .expect("NaiveMerger should merge a same-typed KLL batch");
        let merged_kll = merged
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .unwrap();

        assert_eq!(merged_kll.inner.count(), oracle_kll.inner.count());
        assert_eq!(merged_kll.get_quantile(0.0), oracle_kll.get_quantile(0.0));
        assert_eq!(merged_kll.get_quantile(1.0), oracle_kll.get_quantile(1.0));
    }

    #[test]
    fn naive_merger_single_cms_accumulator_passes_through_unchanged() {
        let matrix = vec![vec![3.0, 0.0, 5.0], vec![0.0, 7.0, 0.0]];
        let cms = cms_from_matrix(matrix.clone(), 2, 3);
        let boxes: Vec<Box<dyn AggregateCore>> = vec![Box::new(cms)];

        let mut merger = NaiveMerger::new();
        merger.initialize(boxes);
        let merged = merger
            .get_merged()
            .expect("single-bucket CMS merge should succeed");
        let merged_cms = merged
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .unwrap();

        assert_eq!(merged_cms.inner.sketch(), matrix);
    }

    #[test]
    fn naive_merger_single_kll_accumulator_passes_through_unchanged() {
        let mut kll = DatasketchesKLLAccumulator::new(200);
        for i in 1..=7 {
            kll.update(i as f64);
        }
        let expected_count = kll.inner.count();
        let expected_min = kll.get_quantile(0.0);
        let expected_max = kll.get_quantile(1.0);

        let boxes: Vec<Box<dyn AggregateCore>> = vec![Box::new(kll)];
        let mut merger = NaiveMerger::new();
        merger.initialize(boxes);
        let merged = merger
            .get_merged()
            .expect("single-bucket KLL merge should succeed");
        let merged_kll = merged
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .unwrap();

        assert_eq!(merged_kll.inner.count() as usize, expected_count as usize);
        assert_eq!(merged_kll.get_quantile(0.0), expected_min);
        assert_eq!(merged_kll.get_quantile(1.0), expected_max);
    }

    #[test]
    fn naive_merger_large_cms_batch_merges_every_bucket_not_just_a_prefix() {
        const N: usize = 80;
        let mut boxes: Vec<Box<dyn AggregateCore>> = Vec::with_capacity(N);
        for i in 0..N {
            // Each bucket sets exactly one cell to 1.0. The merged row-0 total
            // can only equal N if every single bucket actually contributed --
            // a fast path that silently truncates to a prefix (or skips
            // entirely and falls through to some default) would under-count.
            let col = i % 3;
            let mut row0 = vec![0.0, 0.0, 0.0];
            row0[col] = 1.0;
            let matrix = vec![row0, vec![0.0, 0.0, 0.0]];
            boxes.push(Box::new(cms_from_matrix(matrix, 2, 3)));
        }

        let mut merger = NaiveMerger::new();
        merger.initialize(boxes);
        let merged = merger.get_merged().expect("large CMS batch should merge");
        let merged_cms = merged
            .as_any()
            .downcast_ref::<CountMinSketchAccumulator>()
            .unwrap();
        let total: f64 = merged_cms.inner.sketch()[0].iter().sum();

        assert_eq!(
            total, N as f64,
            "merged CMS row-0 total mass ({total}) must equal the number of buckets ({N}) -- \
             a truncated or skipped fast path would under-count"
        );
    }

    #[test]
    fn naive_merger_large_kll_batch_merges_every_bucket_not_just_a_prefix() {
        const N: usize = 70;
        const PER_BUCKET: usize = 4;
        let mut boxes: Vec<Box<dyn AggregateCore>> = Vec::with_capacity(N);
        for i in 0..N {
            let mut kll = DatasketchesKLLAccumulator::new(200);
            for j in 0..PER_BUCKET {
                kll.update((i * PER_BUCKET + j) as f64);
            }
            boxes.push(Box::new(kll));
        }

        let mut merger = NaiveMerger::new();
        merger.initialize(boxes);
        let merged = merger.get_merged().expect("large KLL batch should merge");
        let merged_kll = merged
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .unwrap();

        assert_eq!(
            merged_kll.inner.count() as usize,
            N * PER_BUCKET,
            "merged KLL total count must reflect every bucket's updates -- \
             a truncated or skipped fast path would under-count"
        );
    }

    #[test]
    fn naive_merger_cms_batch_with_wrong_type_mixed_in_errors_consistently() {
        let cms1 = cms_from_matrix(vec![vec![1.0, 0.0], vec![0.0, 1.0]], 2, 2);
        let cms2 = cms_from_matrix(vec![vec![2.0, 0.0], vec![0.0, 2.0]], 2, 2);
        let wrong_type: Box<dyn AggregateCore> = Box::new(SumAccumulator::new());

        let buckets: Vec<Box<dyn AggregateCore>> = vec![Box::new(cms1), Box::new(cms2), wrong_type];

        let mut merger = NaiveMerger::new();
        merger.initialize(buckets);
        let result = merger.get_merged();

        assert!(
            result.is_err(),
            "a batch mixing CMS accumulators with an incompatible accumulator type must \
             fail -- whether the CMS batch-merge fast path's type-guard rejects it up \
             front, or the sequential fallback fold's merge_with rejects the type \
             mismatch -- it must never silently produce an Ok result over only the \
             CMS-typed subset"
        );
    }

    #[test]
    fn naive_merger_kll_batch_with_wrong_type_mixed_in_errors_consistently() {
        let mut kll1 = DatasketchesKLLAccumulator::new(200);
        kll1.update(1.0);
        let mut kll2 = DatasketchesKLLAccumulator::new(200);
        kll2.update(2.0);
        let wrong_type: Box<dyn AggregateCore> = Box::new(SumAccumulator::new());

        let buckets: Vec<Box<dyn AggregateCore>> = vec![Box::new(kll1), Box::new(kll2), wrong_type];

        let mut merger = NaiveMerger::new();
        merger.initialize(buckets);
        let result = merger.get_merged();

        assert!(
            result.is_err(),
            "a batch mixing KLL accumulators with an incompatible accumulator type must \
             fail -- whether the KLL batch-merge fast path's type-guard rejects it up \
             front, or the sequential fallback fold's merge_with rejects the type \
             mismatch -- it must never silently produce an Ok result over only the \
             KLL-typed subset"
        );
    }

    #[test]
    fn naive_merger_aborts_whole_merge_on_mid_batch_error_not_silent_drop() {
        // Buckets 1 and 2 merge fine; bucket 3 poisons the fold partway
        // through; bucket 4 would also merge fine if reached. A correct
        // implementation aborts the ENTIRE merge (Err), not just drops
        // bucket 3 and returns Ok(merge(1, 2, 4)) or Ok(merge(1, 2)).
        let buckets: Vec<Box<dyn AggregateCore>> = vec![
            Box::new(PoisonableAccumulator {
                id: 1,
                poisoned: false,
            }),
            Box::new(PoisonableAccumulator {
                id: 2,
                poisoned: false,
            }),
            Box::new(PoisonableAccumulator {
                id: 3,
                poisoned: true,
            }),
            Box::new(PoisonableAccumulator {
                id: 4,
                poisoned: false,
            }),
        ];

        let mut merger = NaiveMerger::new();
        merger.initialize(buckets);
        let result = merger.get_merged();

        assert!(
            result.is_err(),
            "a merge_with failure partway through the batch must abort the whole merge \
             (propagate Err), not silently drop the failed bucket and return a partial Ok"
        );
    }
}
