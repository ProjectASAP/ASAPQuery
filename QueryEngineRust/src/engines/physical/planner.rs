//! Extension Planner for QueryEngineRust
//!
//! This module provides an ExtensionPlanner implementation that converts
//! custom logical operators (PrecomputedSummaryRead, SummaryMergeMultiple)
//! into their physical execution counterparts.

use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_summary_library::{
    PrecomputedSummaryRead, SketchType, SummaryInfer, SummaryMergeMultiple,
};
use std::fmt;
use std::sync::Arc;

use super::{PrecomputedSummaryReadExec, SummaryInferExec, SummaryMergeMultipleExec};
use crate::stores::Store;

/// Extension planner that handles custom logical operators for QueryEngineRust.
///
/// This planner knows how to convert:
/// - PrecomputedSummaryRead -> PrecomputedSummaryReadExec
/// - SummaryMergeMultiple -> SummaryMergeMultipleExec
///
/// Note: SummaryInfer is handled by datafusion_summary_library's planner
pub struct QueryEngineExtensionPlanner {
    /// Reference to the store for reading precomputed outputs
    store: Arc<dyn Store>,
}

impl QueryEngineExtensionPlanner {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ExtensionPlanner for QueryEngineExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        // Try to downcast to PrecomputedSummaryRead
        if let Some(read) = node.as_any().downcast_ref::<PrecomputedSummaryRead>() {
            return Ok(Some(Arc::new(PrecomputedSummaryReadExec::new(
                read.clone(),
                self.store.clone(),
            ))));
        }

        // Try to downcast to SummaryMergeMultiple
        if let Some(merge) = node.as_any().downcast_ref::<SummaryMergeMultiple>() {
            if physical_inputs.len() != 1 {
                return Err(DataFusionError::Internal(
                    "SummaryMergeMultiple expects exactly one input".to_string(),
                ));
            }
            return Ok(Some(Arc::new(SummaryMergeMultipleExec::new(
                merge.clone(),
                physical_inputs[0].clone(),
            ))));
        }

        // Try to downcast to SummaryInfer
        if let Some(infer) = node.as_any().downcast_ref::<SummaryInfer>() {
            // Extract summary_type and sketch_column from the first logical input (values SummaryMergeMultiple)
            let values_input_plan = _logical_inputs.first().ok_or_else(|| {
                DataFusionError::Internal("SummaryInfer has no logical inputs".to_string())
            })?;

            let (summary_type, sketch_column) = extract_merge_info(values_input_plan, "values")?;

            if _logical_inputs.len() == 2 && physical_inputs.len() == 2 {
                // Dual-input: extract keys summary type from second logical input
                let keys_input_plan = _logical_inputs[1];
                let (keys_summary_type, _) = extract_merge_info(keys_input_plan, "keys")?;

                return Ok(Some(Arc::new(SummaryInferExec::new_dual_input(
                    infer.clone(),
                    physical_inputs[0].clone(),
                    physical_inputs[1].clone(),
                    summary_type,
                    keys_summary_type,
                    sketch_column,
                ))));
            } else if physical_inputs.len() == 1 {
                // Single-input (original path)
                return Ok(Some(Arc::new(SummaryInferExec::new(
                    infer.clone(),
                    physical_inputs[0].clone(),
                    summary_type,
                    sketch_column,
                ))));
            } else {
                return Err(DataFusionError::Internal(format!(
                    "SummaryInfer: unexpected number of inputs: logical={}, physical={}",
                    _logical_inputs.len(),
                    physical_inputs.len()
                )));
            }
        }

        // Not a node we handle - let other planners try
        Ok(None)
    }
}

/// Extract summary_type and sketch_column from a SummaryMergeMultiple logical plan node.
fn extract_merge_info(
    plan: &LogicalPlan,
    label: &str,
) -> Result<(SketchType, String), DataFusionError> {
    match plan {
        LogicalPlan::Extension(ext) => ext
            .node
            .as_any()
            .downcast_ref::<SummaryMergeMultiple>()
            .map(|merge| {
                (
                    merge.summary_type().clone(),
                    merge.sketch_column().to_string(),
                )
            })
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "SummaryInfer {} input is not SummaryMergeMultiple",
                    label
                ))
            }),
        _ => Err(DataFusionError::Internal(format!(
            "SummaryInfer {} input must be an Extension node",
            label
        ))),
    }
}

/// Custom query planner that combines the default DataFusion planner with
/// our QueryEngineExtensionPlanner for custom operators.
pub struct CustomQueryPlanner {
    store: Arc<dyn Store>,
}

impl CustomQueryPlanner {
    pub fn new(store: Arc<dyn Store>) -> Self {
        Self { store }
    }
}

impl fmt::Debug for CustomQueryPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CustomQueryPlanner")
            .field("store", &"<Store>")
            .finish()
    }
}

#[async_trait]
impl datafusion::execution::context::QueryPlanner for CustomQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Create default planner with our extension planner
        let extension_planner = QueryEngineExtensionPlanner::new(self.store.clone());
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(extension_planner)]);

        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
