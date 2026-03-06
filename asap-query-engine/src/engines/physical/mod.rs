//! Physical Execution Operators for DataFusion
//!
//! This module provides physical execution plan operators that implement
//! DataFusion's ExecutionPlan trait for precomputed summary operations.

pub mod accumulator_serde;
pub mod conversion;
pub mod planner;
pub mod precomputed_summary_read_exec;
pub mod summary_infer_exec;
pub mod summary_merge_multiple_exec;

pub use planner::{CustomQueryPlanner, QueryEngineExtensionPlanner};
pub use precomputed_summary_read_exec::PrecomputedSummaryReadExec;
pub use summary_infer_exec::SummaryInferExec;
pub use summary_merge_multiple_exec::SummaryMergeMultipleExec;

use arrow::datatypes::SchemaRef;

/// Format an Arrow schema as a compact string for debug logging.
/// Example: `{host: Utf8, region: Utf8, sketch: Binary}`
pub(crate) fn format_schema(schema: &SchemaRef) -> String {
    let fields: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
        .collect();
    format!("{{{}}}", fields.join(", "))
}
