// ExtensionPlanner for sketch-based logical plan nodes.
//
// This planner converts SummaryInsert and SummaryInfer logical nodes
// into their physical execution plan counterparts.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use crate::sketch_operators::{InferOperation, SketchType, SummaryInfer, SummaryInsert};

use super::{SummaryInferExec, SummaryInsertExec};

/// ExtensionPlanner that handles SummaryInsert and SummaryInfer logical nodes.
#[derive(Debug, Default)]
pub struct SketchExtensionPlanner;

impl SketchExtensionPlanner {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ExtensionPlanner for SketchExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        // Try to downcast to SummaryInsert
        if let Some(summary_insert) = node.as_any().downcast_ref::<SummaryInsert>() {
            return self.plan_summary_insert(summary_insert, physical_inputs);
        }

        // Try to downcast to SummaryInfer
        if let Some(summary_infer) = node.as_any().downcast_ref::<SummaryInfer>() {
            return self.plan_summary_infer(summary_infer, physical_inputs);
        }

        // Unknown node type, let other planners handle it
        Ok(None)
    }
}

impl SketchExtensionPlanner {
    fn plan_summary_insert(
        &self,
        node: &SummaryInsert,
        physical_inputs: &[Arc<dyn ExecutionPlan>],
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if physical_inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "SummaryInsert expects exactly one input".to_string(),
            ));
        }

        let input = physical_inputs[0].clone();
        let input_schema = input.schema();

        // Only support HLL for now
        if node.sketches.len() != 1 {
            return Err(DataFusionError::NotImplemented(
                "SummaryInsert with multiple sketches not yet supported".to_string(),
            ));
        }

        let sketch_spec = &node.sketches[0];
        if sketch_spec.sketch_type != SketchType::HLL {
            return Err(DataFusionError::NotImplemented(format!(
                "Sketch type {:?} not yet supported, only HLL is implemented",
                sketch_spec.sketch_type
            )));
        }

        // Find value column index
        let value_col_idx = match &sketch_spec.value_column {
            Some(col_name) => input_schema
                .fields()
                .iter()
                .position(|f| f.name() == col_name)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Value column '{}' not found in input schema",
                        col_name
                    ))
                })?,
            None => {
                return Err(DataFusionError::Plan(
                    "SummaryInsert requires a value column for HLL".to_string(),
                ));
            }
        };

        // Find group-by column indices
        let group_by_indices: Vec<usize> = if !node.group_by_exprs.is_empty() {
            // Use group_by_exprs: find columns by expression name
            node.group_by_exprs
                .iter()
                .map(|typed_expr| {
                    // For simple column expressions, extract the column name
                    let col_name =
                        if let datafusion::logical_expr::Expr::Column(col) = &typed_expr.expr {
                            col.name.clone()
                        } else {
                            typed_expr.expr.schema_name().to_string()
                        };

                    input_schema
                        .fields()
                        .iter()
                        .position(|f| f.name() == &col_name)
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "Group-by column '{}' not found in input schema",
                                col_name
                            ))
                        })
                })
                .collect::<DFResult<Vec<_>>>()?
        } else {
            // Use legacy group_by strings
            node.group_by
                .iter()
                .map(|col_name| {
                    input_schema
                        .fields()
                        .iter()
                        .position(|f| f.name() == col_name)
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "Group-by column '{}' not found in input schema",
                                col_name
                            ))
                        })
                })
                .collect::<DFResult<Vec<_>>>()?
        };

        let exec = SummaryInsertExec::new(
            input,
            value_col_idx,
            group_by_indices,
            sketch_spec.output_column_name.clone(),
        );

        Ok(Some(Arc::new(exec)))
    }

    fn plan_summary_infer(
        &self,
        node: &SummaryInfer,
        physical_inputs: &[Arc<dyn ExecutionPlan>],
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if physical_inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "SummaryInfer expects exactly one input".to_string(),
            ));
        }

        let input = physical_inputs[0].clone();
        let input_schema = input.schema();

        // Only support single operation for now
        if node.operations.len() != 1 {
            return Err(DataFusionError::NotImplemented(
                "SummaryInfer with multiple operations not yet supported".to_string(),
            ));
        }

        let operation = node.operations[0].clone();
        let output_name = node.output_names[0].clone();

        // Only support CountDistinct for now
        if !matches!(operation, InferOperation::CountDistinct) {
            return Err(DataFusionError::NotImplemented(format!(
                "Infer operation {:?} not yet supported, only CountDistinct is implemented",
                operation
            )));
        }

        // Find sketch column index (last column with "sketch" in name, or Binary type)
        let sketch_col_idx = input_schema
            .fields()
            .iter()
            .rposition(|f| {
                f.name().contains("sketch") || f.data_type() == &arrow::datatypes::DataType::Binary
            })
            .ok_or_else(|| {
                DataFusionError::Plan("No sketch column found in input schema".to_string())
            })?;

        let exec = SummaryInferExec::new(input, sketch_col_idx, operation, output_name);

        Ok(Some(Arc::new(exec)))
    }
}
