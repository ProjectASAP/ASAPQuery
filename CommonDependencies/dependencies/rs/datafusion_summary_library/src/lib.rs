// DataFusion Summary Library
//
// This library provides logical and physical operators for sketch-based
// query optimization in DataFusion. It supports approximate query processing
// using data structures like HyperLogLog for COUNT(DISTINCT) operations.

pub mod physical;
pub mod sketch_operators;

pub use physical::{HllSketch, SketchExtensionPlanner, SummaryInferExec, SummaryInsertExec};
pub use sketch_operators::{
    GroupingStrategy, InferOperation, PrecomputedSummaryRead, SketchMetadata, SketchSpec,
    SketchType, SummaryInfer, SummaryInsert, SummaryMerge, SummaryMergeMultiple, SummaryRead,
    SummarySubtract, SummaryType, TypedExpr,
};
