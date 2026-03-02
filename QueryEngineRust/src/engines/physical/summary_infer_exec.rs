//! SummaryInferExec - Physical execution operator for extracting values from summaries
//!
//! This operator extracts values from serialized accumulators (summaries).
//!
//! **Single-population path** (no keys_input):
//!   Input: rows with [label columns, sketch column]
//!   Output: rows with [label columns, value column]
//!
//! **Multi-population path** (keys_input present):
//!   Input 0 (values): rows with [spatial_label columns, sketch column]
//!   Input 1 (keys):   rows with [spatial_label columns, sketch column]
//!   Output: rows with [spatial_label columns, sub_key columns, value column]

use arrow::array::{ArrayRef, BinaryArray, Float64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_summary_library::{InferOperation, SketchType, SummaryInfer};
use futures::stream;
use promql_utilities::query_logics::enums::Statistic;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

use super::format_schema;
use crate::engines::physical::accumulator_serde::{
    deserialize_accumulator, deserialize_keys_accumulator, deserialize_multiple_subpopulation,
    deserialize_single_subpopulation,
};

/// Physical execution plan for extracting values from serialized summaries.
pub struct SummaryInferExec {
    /// The logical operator this was created from
    logical_node: SummaryInfer,
    /// Input execution plan (values branch)
    input: Arc<dyn ExecutionPlan>,
    /// Optional second input (keys branch) for multi-population accumulators
    keys_input: Option<Arc<dyn ExecutionPlan>>,
    /// Type of summary being inferred (determines deserialization)
    summary_type: SketchType,
    /// Type of the keys summary (only set for dual-input)
    keys_summary_type: Option<SketchType>,
    /// Name of the sketch column in the input schema
    sketch_column: String,
    /// Output schema (labels + value)
    schema: SchemaRef,
    /// Plan properties (cached)
    properties: PlanProperties,
}

impl SummaryInferExec {
    pub fn new(
        logical_node: SummaryInfer,
        input: Arc<dyn ExecutionPlan>,
        summary_type: SketchType,
        sketch_column: String,
    ) -> Self {
        let schema = Self::build_schema(&logical_node, &input, None, &sketch_column);

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            logical_node,
            input,
            keys_input: None,
            summary_type,
            keys_summary_type: None,
            sketch_column,
            schema,
            properties,
        }
    }

    /// Create a dual-input SummaryInferExec for multi-population accumulators.
    pub fn new_dual_input(
        logical_node: SummaryInfer,
        input: Arc<dyn ExecutionPlan>,
        keys_input: Arc<dyn ExecutionPlan>,
        summary_type: SketchType,
        keys_summary_type: SketchType,
        sketch_column: String,
    ) -> Self {
        let schema = Self::build_schema(&logical_node, &input, Some(&keys_input), &sketch_column);

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            logical_node,
            input,
            keys_input: Some(keys_input),
            summary_type,
            keys_summary_type: Some(keys_summary_type),
            sketch_column,
            schema,
            properties,
        }
    }

    fn build_schema(
        logical_node: &SummaryInfer,
        input: &Arc<dyn ExecutionPlan>,
        _keys_input: Option<&Arc<dyn ExecutionPlan>>,
        sketch_column: &str,
    ) -> SchemaRef {
        let input_schema = input.schema();

        // Build output schema: label columns from input (minus sketch) ...
        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .filter(|f| f.name() != sketch_column)
            .map(|f| f.as_ref().clone())
            .collect();

        // ... plus sub-key columns for dual-input (group_key_columns)
        for key_col in &logical_node.group_key_columns {
            fields.push(Field::new(key_col, DataType::Utf8, true));
        }

        // ... plus output value columns (one per operation)
        for output_name in &logical_node.output_names {
            fields.push(Field::new(output_name, DataType::Float64, false));
        }

        Arc::new(Schema::new(fields))
    }

    /// Map InferOperation to Statistic for accumulator query
    fn infer_op_to_statistic(op: &InferOperation) -> Statistic {
        match op {
            InferOperation::ExtractSum => Statistic::Sum,
            InferOperation::ExtractCount => Statistic::Count,
            InferOperation::ExtractMin => Statistic::Min,
            InferOperation::ExtractMax => Statistic::Max,
            InferOperation::ExtractIncrease => Statistic::Increase,
            InferOperation::ExtractRate => Statistic::Rate,
            InferOperation::CountDistinct => Statistic::Cardinality,
            InferOperation::Quantile(_) | InferOperation::Median => Statistic::Quantile,
            InferOperation::TopK(_) => Statistic::Topk,
            // All other operations - use Count as fallback
            _ => Statistic::Count,
        }
    }

    /// Extract query kwargs from an InferOperation.
    ///
    /// Some operations embed parameters (e.g. Quantile embeds the quantile value,
    /// TopK embeds k) that accumulators need via the kwargs HashMap.
    fn infer_op_to_kwargs(op: &InferOperation) -> Option<HashMap<String, String>> {
        match op {
            InferOperation::Quantile(q_u16) => {
                let q = *q_u16 as f64 / 10000.0;
                let mut kwargs = HashMap::new();
                kwargs.insert("quantile".to_string(), q.to_string());
                Some(kwargs)
            }
            InferOperation::Median => {
                let mut kwargs = HashMap::new();
                kwargs.insert("quantile".to_string(), "0.5".to_string());
                Some(kwargs)
            }
            InferOperation::TopK(k) => {
                let mut kwargs = HashMap::new();
                kwargs.insert("k".to_string(), k.to_string());
                Some(kwargs)
            }
            _ => None,
        }
    }
}

impl fmt::Debug for SummaryInferExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SummaryInferExec")
            .field("operations", &self.logical_node.operations)
            .field("has_keys_input", &self.keys_input.is_some())
            .finish()
    }
}

impl DisplayAs for SummaryInferExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SummaryInferExec: operations={:?}, dual_input={}",
            self.logical_node.operations,
            self.keys_input.is_some()
        )
    }
}

impl ExecutionPlan for SummaryInferExec {
    fn name(&self) -> &str {
        "SummaryInferExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        let mut children = vec![&self.input];
        if let Some(ref keys_input) = self.keys_input {
            children.push(keys_input);
        }
        children
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        match (children.len(), &self.keys_input) {
            (1, None) => Ok(Arc::new(Self::new(
                self.logical_node.clone(),
                children[0].clone(),
                self.summary_type.clone(),
                self.sketch_column.clone(),
            ))),
            (2, Some(_)) => Ok(Arc::new(Self::new_dual_input(
                self.logical_node.clone(),
                children[0].clone(),
                children[1].clone(),
                self.summary_type.clone(),
                self.keys_summary_type.clone().unwrap(),
                self.sketch_column.clone(),
            ))),
            _ => Err(DataFusionError::Internal(format!(
                "SummaryInferExec: expected {} children, got {}",
                if self.keys_input.is_some() { 2 } else { 1 },
                children.len()
            ))),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let schema = self.schema.clone();
        let schema_for_stream = schema.clone();
        let logical_node = self.logical_node.clone();
        let summary_type = self.summary_type.clone();
        let sketch_column = self.sketch_column.clone();

        debug!(
            input_schema = %format_schema(&self.input.schema()),
            output_schema = %format_schema(&self.schema),
            operations = ?self.logical_node.operations,
            sketch_column = %self.sketch_column,
            has_keys_input = self.keys_input.is_some(),
            "SummaryInferExec::execute"
        );

        if let Some(ref keys_input) = self.keys_input {
            // Multi-population path
            let values_stream = self.input.execute(partition, context.clone())?;
            let keys_stream = keys_input.execute(partition, context)?;
            let keys_summary_type = self.keys_summary_type.clone().unwrap();

            let output_stream = async move {
                let collect_start = Instant::now();
                let values_batches = collect(values_stream).await?;
                let keys_batches = collect(keys_stream).await?;
                let values_rows: usize = values_batches.iter().map(|b| b.num_rows()).sum();
                let keys_rows: usize = keys_batches.iter().map(|b| b.num_rows()).sum();
                debug!(
                    collect_ms = format!("{:.2}", collect_start.elapsed().as_secs_f64() * 1000.0),
                    values_rows, keys_rows, "SummaryInferExec collected dual-input batches"
                );

                let infer_start = Instant::now();
                let result = process_dual_input(
                    &values_batches,
                    &keys_batches,
                    &logical_node,
                    &summary_type,
                    &keys_summary_type,
                    &sketch_column,
                    &schema,
                )?;
                debug!(
                    infer_ms = format!("{:.2}", infer_start.elapsed().as_secs_f64() * 1000.0),
                    output_rows = result.num_rows(),
                    "SummaryInferExec dual-input infer complete"
                );
                Ok(result)
            };

            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema_for_stream,
                stream::once(output_stream),
            )))
        } else {
            // Single-population path (original behavior)
            let input_stream = self.input.execute(partition, context)?;

            let output_stream = async move {
                let collect_start = Instant::now();
                let batches = collect(input_stream).await?;
                let total_input_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                debug!(
                    collect_ms = format!("{:.2}", collect_start.elapsed().as_secs_f64() * 1000.0),
                    total_input_rows,
                    num_batches = batches.len(),
                    "SummaryInferExec collected input (single-pop)"
                );

                let mut all_label_values: Vec<Vec<String>> = Vec::new();
                let mut all_result_values: Vec<f64> = Vec::new();

                let self_keyed = is_self_keyed_multi_pop(&summary_type);

                let infer_start = Instant::now();
                for batch in &batches {
                    if self_keyed {
                        process_self_keyed_multi_pop_batch(
                            &mut all_label_values,
                            &mut all_result_values,
                            batch,
                            &logical_node,
                            &summary_type,
                            &sketch_column,
                        )?;
                    } else {
                        process_single_pop_batch(
                            &mut all_label_values,
                            &mut all_result_values,
                            batch,
                            &logical_node,
                            &summary_type,
                            &sketch_column,
                        )?;
                    }
                }
                debug!(
                    infer_ms = format!("{:.2}", infer_start.elapsed().as_secs_f64() * 1000.0),
                    output_rows = all_result_values.len(),
                    self_keyed,
                    "SummaryInferExec infer complete (single-pop)"
                );

                build_output_batch(&all_label_values, &all_result_values, &schema)
            };

            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema_for_stream,
                stream::once(output_stream),
            )))
        }
    }
}

// ============================================================================
// Single-population processing (unchanged logic)
// ============================================================================

/// Process a batch and extract values from single-population accumulators
fn process_single_pop_batch(
    all_label_values: &mut Vec<Vec<String>>,
    all_result_values: &mut Vec<f64>,
    batch: &RecordBatch,
    logical_node: &SummaryInfer,
    summary_type: &SketchType,
    sketch_column: &str,
) -> Result<(), DataFusionError> {
    let sketch_idx = find_sketch_column_index(batch, sketch_column)?;

    let sketch_array = batch
        .column(sketch_idx)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| DataFusionError::Internal("Sketch column is not Binary".to_string()))?;

    let label_indices: Vec<usize> = (0..batch.num_columns())
        .filter(|&i| i != sketch_idx)
        .collect();

    let operation = logical_node
        .operations
        .first()
        .ok_or_else(|| DataFusionError::Internal("SummaryInfer has no operations".to_string()))?;

    let statistic = SummaryInferExec::infer_op_to_statistic(operation);
    let query_kwargs = SummaryInferExec::infer_op_to_kwargs(operation);

    for row in 0..batch.num_rows() {
        let label_values = extract_label_values(batch, &label_indices, row);
        let sketch_bytes = sketch_array.value(row);

        let accumulator =
            deserialize_single_subpopulation(sketch_bytes, summary_type).map_err(|e| {
                DataFusionError::Internal(format!("Failed to deserialize accumulator: {}", e))
            })?;

        let value = accumulator
            .query(statistic, query_kwargs.as_ref())
            .map_err(|e| {
                DataFusionError::Internal(format!("Failed to query accumulator: {}", e))
            })?;

        all_label_values.push(label_values);
        all_result_values.push(value);
    }

    Ok(())
}

// ============================================================================
// Self-keyed multi-population processing (single-input, keys from accumulator)
// ============================================================================

/// Returns true if the SketchType is a multi-population accumulator that
/// carries its own keys (via `get_keys()`), so it can be processed in
/// single-input mode without a separate keys stream.
fn is_self_keyed_multi_pop(summary_type: &SketchType) -> bool {
    matches!(
        summary_type,
        SketchType::MultipleIncrease | SketchType::MultipleSum | SketchType::MultipleMinMax
    )
}

/// Process a batch of self-keyed multi-population accumulators.
///
/// For each row (spatial group):
///   1. Deserialize as AggregateCore to call get_keys()
///   2. Deserialize as MultipleSubpopulationAggregate to call query(stat, key)
///   3. Emit one output row per sub-key
fn process_self_keyed_multi_pop_batch(
    all_label_values: &mut Vec<Vec<String>>,
    all_result_values: &mut Vec<f64>,
    batch: &RecordBatch,
    logical_node: &SummaryInfer,
    summary_type: &SketchType,
    sketch_column: &str,
) -> Result<(), DataFusionError> {
    let sketch_idx = find_sketch_column_index(batch, sketch_column)?;

    let sketch_array = batch
        .column(sketch_idx)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| DataFusionError::Internal("Sketch column is not Binary".to_string()))?;

    let label_indices: Vec<usize> = (0..batch.num_columns())
        .filter(|&i| i != sketch_idx)
        .collect();

    let operation = logical_node
        .operations
        .first()
        .ok_or_else(|| DataFusionError::Internal("SummaryInfer has no operations".to_string()))?;

    let statistic = SummaryInferExec::infer_op_to_statistic(operation);
    let query_kwargs = SummaryInferExec::infer_op_to_kwargs(operation);

    let num_sub_key_cols = logical_node.group_key_columns.len();

    for row in 0..batch.num_rows() {
        let spatial_labels = extract_label_values(batch, &label_indices, row);
        let sketch_bytes = sketch_array.value(row);

        // Get keys from the accumulator itself
        let acc = deserialize_accumulator(sketch_bytes, summary_type).map_err(|e| {
            DataFusionError::Internal(format!("Failed to deserialize accumulator: {}", e))
        })?;
        let sub_keys = acc.get_keys().unwrap_or_default();

        // Deserialize as multi-pop for querying
        let multi_acc =
            deserialize_multiple_subpopulation(sketch_bytes, summary_type).map_err(|e| {
                DataFusionError::Internal(format!(
                    "Failed to deserialize multi-pop accumulator: {}",
                    e
                ))
            })?;

        for sub_key in &sub_keys {
            let value = multi_acc
                .query(statistic, sub_key, query_kwargs.as_ref())
                .map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to query multi-pop accumulator for key {:?}: {}",
                        sub_key, e
                    ))
                })?;

            let mut row_labels = spatial_labels.clone();
            for i in 0..num_sub_key_cols {
                if i < sub_key.labels.len() {
                    row_labels.push(sub_key.labels[i].clone());
                } else {
                    row_labels.push(String::new());
                }
            }

            all_label_values.push(row_labels);
            all_result_values.push(value);
        }
    }

    Ok(())
}

// ============================================================================
// Multi-population (dual-input) processing
// ============================================================================

/// Process dual-input: values + keys batches for multi-population accumulators.
///
/// For each spatial group (row) in the values stream:
///   1. Deserialize the value sketch as MultipleSubpopulationAggregate
///   2. Find the matching spatial group in the keys stream
///   3. Deserialize the keys accumulator, call get_keys() to enumerate sub-keys
///   4. For each sub-key, call value_acc.query(statistic, sub_key) -> one output row
fn process_dual_input(
    values_batches: &[RecordBatch],
    keys_batches: &[RecordBatch],
    logical_node: &SummaryInfer,
    values_summary_type: &SketchType,
    keys_summary_type: &SketchType,
    sketch_column: &str,
    schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let operation = logical_node
        .operations
        .first()
        .ok_or_else(|| DataFusionError::Internal("SummaryInfer has no operations".to_string()))?;
    let statistic = SummaryInferExec::infer_op_to_statistic(operation);
    let query_kwargs = SummaryInferExec::infer_op_to_kwargs(operation);

    // Build a lookup from spatial labels -> keys sketch bytes
    // Key: spatial label values (as Vec<String>), Value: serialized keys accumulator bytes
    let keys_lookup = build_keys_lookup(keys_batches, sketch_column)?;

    let num_sub_key_cols = logical_node.group_key_columns.len();

    let mut all_label_values: Vec<Vec<String>> = Vec::new();
    let mut all_result_values: Vec<f64> = Vec::new();

    for batch in values_batches {
        let sketch_idx = find_sketch_column_index(batch, sketch_column)?;

        let sketch_array = batch
            .column(sketch_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Values sketch column is not Binary".to_string())
            })?;

        let label_indices: Vec<usize> = (0..batch.num_columns())
            .filter(|&i| i != sketch_idx)
            .collect();

        for row in 0..batch.num_rows() {
            let spatial_labels = extract_label_values(batch, &label_indices, row);
            let value_sketch_bytes = sketch_array.value(row);

            // Deserialize the value sketch as MultipleSubpopulationAggregate
            let value_acc =
                deserialize_multiple_subpopulation(value_sketch_bytes, values_summary_type)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Failed to deserialize multi-pop value accumulator: {}",
                            e
                        ))
                    })?;

            // Find matching keys accumulator
            let keys_bytes = keys_lookup.get(&spatial_labels).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "No keys accumulator found for spatial group: {:?}",
                    spatial_labels
                ))
            })?;

            let keys_acc =
                deserialize_keys_accumulator(keys_bytes, keys_summary_type).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to deserialize keys accumulator: {}",
                        e
                    ))
                })?;

            let sub_keys = keys_acc.get_keys().unwrap_or_default();

            debug!(
                spatial_labels = ?spatial_labels,
                num_sub_keys = sub_keys.len(),
                "Processing multi-pop spatial group"
            );

            // For each sub-key, query the value accumulator
            for sub_key in &sub_keys {
                let value = value_acc
                    .query(statistic, sub_key, query_kwargs.as_ref())
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Failed to query multi-pop accumulator for key {:?}: {}",
                            sub_key, e
                        ))
                    })?;

                // Output row: [spatial_labels..., sub_key_labels..., value]
                let mut row_labels = spatial_labels.clone();
                // Append sub-key label values
                // sub_key.labels is Vec<String> of values
                for i in 0..num_sub_key_cols {
                    if i < sub_key.labels.len() {
                        row_labels.push(sub_key.labels[i].clone());
                    } else {
                        row_labels.push(String::new());
                    }
                }

                all_label_values.push(row_labels);
                all_result_values.push(value);
            }
        }
    }

    debug!(
        output_rows = all_result_values.len(),
        "SummaryInferExec building output (multi-pop)"
    );

    build_output_batch(&all_label_values, &all_result_values, schema)
}

/// Build a lookup map from spatial label values to keys sketch bytes.
fn build_keys_lookup(
    keys_batches: &[RecordBatch],
    sketch_column: &str,
) -> Result<HashMap<Vec<String>, Vec<u8>>, DataFusionError> {
    let mut lookup: HashMap<Vec<String>, Vec<u8>> = HashMap::new();

    for batch in keys_batches {
        let sketch_idx = find_sketch_column_index(batch, sketch_column)?;

        let sketch_array = batch
            .column(sketch_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Keys sketch column is not Binary".to_string())
            })?;

        let label_indices: Vec<usize> = (0..batch.num_columns())
            .filter(|&i| i != sketch_idx)
            .collect();

        for row in 0..batch.num_rows() {
            let label_values = extract_label_values(batch, &label_indices, row);
            let sketch_bytes = sketch_array.value(row).to_vec();
            lookup.insert(label_values, sketch_bytes);
        }
    }

    Ok(lookup)
}

// ============================================================================
// Common helpers
// ============================================================================

/// Find the index of the sketch column in a batch
fn find_sketch_column_index(
    batch: &RecordBatch,
    sketch_column: &str,
) -> Result<usize, DataFusionError> {
    batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == sketch_column)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Sketch column '{}' not found in batch schema: {:?}",
                sketch_column,
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            ))
        })
}

/// Extract label values from a batch row
fn extract_label_values(batch: &RecordBatch, label_indices: &[usize], row: usize) -> Vec<String> {
    label_indices
        .iter()
        .map(|&idx| {
            let col = batch.column(idx);
            if let Some(str_array) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                str_array.value(row).to_string()
            } else {
                String::new()
            }
        })
        .collect()
}

/// Build output batch from extracted values
fn build_output_batch(
    all_label_values: &[Vec<String>],
    all_result_values: &[f64],
    schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    // Get number of label columns (schema fields minus the value column)
    let num_label_cols = schema.fields().len() - 1;

    // Build label column builders
    let mut label_builders: Vec<StringBuilder> =
        (0..num_label_cols).map(|_| StringBuilder::new()).collect();

    // Build value column
    let mut value_builder = Float64Builder::new();

    for (label_values, value) in all_label_values.iter().zip(all_result_values.iter()) {
        // Add label values
        for (i, label_value) in label_values.iter().enumerate() {
            if i < label_builders.len() {
                label_builders[i].append_value(label_value);
            }
        }
        // Add value
        value_builder.append_value(*value);
    }

    // Build columns
    let mut columns: Vec<ArrayRef> = label_builders
        .iter_mut()
        .map(|b| Arc::new(b.finish()) as ArrayRef)
        .collect();
    columns.push(Arc::new(value_builder.finish()));

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::Internal(format!("Failed to build output batch: {}", e)))
}
