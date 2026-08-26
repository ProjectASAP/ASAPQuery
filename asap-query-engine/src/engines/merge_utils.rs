//! Shared accumulator-batch merge fold, used by both the instant-query path
//! (`SimpleEngine::merge_accumulators`) and the range-query path
//! (`NaiveMerger::merge_all`), so the two stay behaviorally identical.
//!
//! Tries a batch merge for accumulator types that support one (currently
//! `DatasketchesKLL` and `CountMinSketch`), falling back to a sequential
//! pairwise fold otherwise or if the batch merge itself fails. The fold
//! aborts on the first `merge_with` error instead of skipping it, so a
//! caller can't get a silently-partial merge back as `Ok`.

use crate::data_model::{AggregateCore, AggregationType};
use crate::precompute_operators::count_min_sketch_accumulator::CountMinSketchAccumulator;
use crate::precompute_operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator;
use tracing::warn;

/// Precondition: `accumulators` is non-empty. Callers already special-case
/// the empty slice to report their own error (`AccumulatorError::EmptySlice`,
/// `"No buckets to merge"`), so this doesn't repeat that check.
pub(crate) fn merge_accumulators_batch(
    accumulators: &[Box<dyn AggregateCore>],
) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
    debug_assert!(
        !accumulators.is_empty(),
        "merge_accumulators_batch requires at least one accumulator"
    );

    if accumulators.len() == 1 {
        return Ok(accumulators[0].clone_boxed_core());
    }

    let accumulator_type = accumulators[0].get_accumulator_type();

    if accumulator_type == AggregationType::DatasketchesKLL {
        match DatasketchesKLLAccumulator::merge_multiple(accumulators) {
            Ok(merged) => return Ok(Box::new(merged)),
            Err(e) => warn!(
                "Batch merge failed: {}. Falling back to sequential merge.",
                e
            ),
        }
    } else if accumulator_type == AggregationType::CountMinSketch {
        match CountMinSketchAccumulator::merge_multiple(accumulators) {
            Ok(merged) => return Ok(Box::new(merged)),
            Err(e) => warn!(
                "Batch merge failed: {}. Falling back to sequential merge.",
                e
            ),
        }
    }

    let mut result = accumulators[0].clone_boxed_core();
    for accumulator in &accumulators[1..] {
        result = result.merge_with(accumulator.as_ref())?;
    }
    Ok(result)
}
