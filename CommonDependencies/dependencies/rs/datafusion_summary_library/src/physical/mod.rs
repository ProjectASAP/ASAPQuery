// Physical execution module for sketch-based query plans.
//
// This module provides physical execution plan nodes for sketch operators:
// - SummaryInsertExec: Computes sketches from raw data
// - SummaryInferExec: Extracts results from sketches
//
// Currently only HLL (HyperLogLog) sketches are supported for COUNT(DISTINCT).

mod hll;
mod planner;
mod summary_infer_exec;
mod summary_insert_exec;

pub use hll::HllSketch;
pub use planner::SketchExtensionPlanner;
pub use summary_infer_exec::SummaryInferExec;
pub use summary_insert_exec::SummaryInsertExec;
