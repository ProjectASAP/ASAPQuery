// Physical execution plan for SummaryInsert (sketch building).
//
// This ExecutionPlan consumes input batches and builds HLL sketches
// for each group. Currently only supports HLL sketches.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryBuilder, RecordBatch, StringBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};

use super::hll::HllSketch;

/// Physical execution plan for building HLL sketches.
///
/// Takes input batches and produces one row per group with:
/// - Group key columns
/// - Binary column containing serialized HLL sketch
#[derive(Debug)]
pub struct SummaryInsertExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,

    /// Index of the value column to sketch (in input schema)
    value_col_idx: usize,

    /// Indices of group-by columns (in input schema)
    group_by_indices: Vec<usize>,

    /// Name of the output sketch column
    sketch_col_name: String,

    /// Output schema
    schema: SchemaRef,

    /// Plan properties (cached)
    properties: PlanProperties,
}

impl SummaryInsertExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        value_col_idx: usize,
        group_by_indices: Vec<usize>,
        sketch_col_name: String,
    ) -> Self {
        let input_schema = input.schema();

        // Build output schema: group columns + sketch column
        let mut fields: Vec<Field> = group_by_indices
            .iter()
            .map(|&idx| input_schema.field(idx).clone())
            .collect();

        // Add sketch column with the specified name
        fields.push(Field::new(&sketch_col_name, DataType::Binary, false));

        let schema = Arc::new(Schema::new(fields));

        // Plan properties: single partition output, no ordering guarantees
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            input,
            value_col_idx,
            group_by_indices,
            sketch_col_name,
            schema,
            properties,
        }
    }

    /// Extracts a group key from a row as a vector of ScalarValues.
    fn extract_group_key(
        batch: &RecordBatch,
        row_idx: usize,
        group_by_indices: &[usize],
    ) -> Vec<ScalarValue> {
        group_by_indices
            .iter()
            .map(|&col_idx| {
                ScalarValue::try_from_array(batch.column(col_idx), row_idx)
                    .unwrap_or(ScalarValue::Null)
            })
            .collect()
    }

    /// Extracts a value as bytes for hashing.
    fn extract_value_bytes(array: &ArrayRef, row_idx: usize) -> Vec<u8> {
        // Convert any value to string representation for hashing
        // This is a hacky but universal approach
        if array.is_null(row_idx) {
            return b"__NULL__".to_vec();
        }

        // Use Arrow's display formatter to get string representation
        let value = ScalarValue::try_from_array(array.as_ref(), row_idx)
            .map(|v| v.to_string())
            .unwrap_or_else(|_| "__ERROR__".to_string());

        value.into_bytes()
    }
}

impl DisplayAs for SummaryInsertExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "SummaryInsertExec: value_col={}, group_by={:?}",
                    self.value_col_idx, self.group_by_indices
                )
            }
        }
    }
}

impl ExecutionPlan for SummaryInsertExec {
    fn name(&self) -> &str {
        "SummaryInsertExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "SummaryInsertExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(SummaryInsertExec::new(
            children[0].clone(),
            self.value_col_idx,
            self.group_by_indices.clone(),
            self.sketch_col_name.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "SummaryInsertExec only supports partition 0, got {}",
                partition
            )));
        }

        // Get input stream
        let input_stream = self.input.execute(0, context)?;

        // Create the output stream
        let schema = self.schema.clone();
        let value_col_idx = self.value_col_idx;
        let group_by_indices = self.group_by_indices.clone();

        let stream =
            SummaryInsertStream::new(input_stream, schema, value_col_idx, group_by_indices);

        Ok(Box::pin(stream))
    }
}

/// Stream that consumes input batches and produces aggregated sketch results.
struct SummaryInsertStream {
    /// Input stream
    input: SendableRecordBatchStream,

    /// Output schema
    schema: SchemaRef,

    /// Value column index
    value_col_idx: usize,

    /// Group-by column indices
    group_by_indices: Vec<usize>,

    /// Accumulated sketches per group
    groups: HashMap<Vec<ScalarValue>, HllSketch>,

    /// Whether we've finished consuming input
    finished_input: bool,

    /// Whether we've emitted the final result
    emitted_result: bool,
}

impl SummaryInsertStream {
    fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        value_col_idx: usize,
        group_by_indices: Vec<usize>,
    ) -> Self {
        Self {
            input,
            schema,
            value_col_idx,
            group_by_indices,
            groups: HashMap::new(),
            finished_input: false,
            emitted_result: false,
        }
    }

    /// Process a batch of input data.
    fn process_batch(&mut self, batch: &RecordBatch) {
        let value_array = batch.column(self.value_col_idx);
        let num_rows = batch.num_rows();

        for row_idx in 0..num_rows {
            // Extract group key
            let group_key =
                SummaryInsertExec::extract_group_key(batch, row_idx, &self.group_by_indices);

            // Get or create HLL for this group
            let hll = self.groups.entry(group_key).or_default();

            // Extract value and insert into HLL
            let value_bytes = SummaryInsertExec::extract_value_bytes(value_array, row_idx);
            hll.insert_bytes(&value_bytes);
        }
    }

    /// Build the final output batch from accumulated sketches.
    fn build_output(&mut self) -> DFResult<RecordBatch> {
        let num_groups = self.groups.len();

        // Build group key columns
        let mut group_builders: Vec<ScalarArrayBuilder> = self
            .schema
            .fields()
            .iter()
            .take(self.group_by_indices.len())
            .map(|field| ScalarArrayBuilder::new(field.data_type(), num_groups))
            .collect();

        // Build sketch column
        let mut sketch_builder = BinaryBuilder::with_capacity(num_groups, num_groups * 16);

        // Populate arrays
        for (group_key, hll) in &mut self.groups {
            // Add group key values
            for (idx, scalar) in group_key.iter().enumerate() {
                group_builders[idx].append(scalar);
            }

            // Add serialized sketch
            let sketch_bytes = hll.to_bytes();
            sketch_builder.append_value(&sketch_bytes);
        }

        // Finish building arrays
        let mut columns: Vec<ArrayRef> = group_builders.iter_mut().map(|b| b.finish()).collect();
        columns.push(Arc::new(sketch_builder.finish()));

        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| DataFusionError::ArrowError(e, None))
    }
}

use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

impl Stream for SummaryInsertStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // If we've emitted the result, we're done
        if self.emitted_result {
            return Poll::Ready(None);
        }

        // Consume all input batches first
        if !self.finished_input {
            loop {
                match Pin::new(&mut self.input).poll_next(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        self.process_batch(&batch);
                    }
                    Poll::Ready(Some(Err(e))) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        self.finished_input = true;
                        break;
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                }
            }
        }

        // Build and emit the final result
        self.emitted_result = true;

        // Handle case with no groups
        if self.groups.is_empty() {
            return Poll::Ready(None);
        }

        let batch = self.build_output();
        Poll::Ready(Some(batch))
    }
}

impl datafusion::physical_plan::RecordBatchStream for SummaryInsertStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// Helper enum for building arrays dynamically
enum ScalarArrayBuilder {
    Utf8(StringBuilder),
    UInt64(UInt64Builder),
    Int64(arrow::array::Int64Builder),
    Float64(arrow::array::Float64Builder),
    Int32(arrow::array::Int32Builder),
    UInt32(arrow::array::UInt32Builder),
}

impl ScalarArrayBuilder {
    fn new(data_type: &DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Utf8 => {
                ScalarArrayBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 32))
            }
            DataType::UInt64 => ScalarArrayBuilder::UInt64(UInt64Builder::with_capacity(capacity)),
            DataType::Int64 => {
                ScalarArrayBuilder::Int64(arrow::array::Int64Builder::with_capacity(capacity))
            }
            DataType::Float64 => {
                ScalarArrayBuilder::Float64(arrow::array::Float64Builder::with_capacity(capacity))
            }
            DataType::Int32 => {
                ScalarArrayBuilder::Int32(arrow::array::Int32Builder::with_capacity(capacity))
            }
            DataType::UInt32 => {
                ScalarArrayBuilder::UInt32(arrow::array::UInt32Builder::with_capacity(capacity))
            }
            // For unsupported types, fall back to string representation
            _ => ScalarArrayBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 32)),
        }
    }

    fn append(&mut self, scalar: &ScalarValue) {
        match (self, scalar) {
            (ScalarArrayBuilder::Utf8(b), ScalarValue::Utf8(v)) => match v {
                Some(s) => b.append_value(s),
                None => b.append_null(),
            },
            (ScalarArrayBuilder::UInt64(b), ScalarValue::UInt64(v)) => match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            },
            (ScalarArrayBuilder::Int64(b), ScalarValue::Int64(v)) => match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            },
            (ScalarArrayBuilder::Float64(b), ScalarValue::Float64(v)) => match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            },
            (ScalarArrayBuilder::Int32(b), ScalarValue::Int32(v)) => match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            },
            (ScalarArrayBuilder::UInt32(b), ScalarValue::UInt32(v)) => match v {
                Some(val) => b.append_value(*val),
                None => b.append_null(),
            },
            // Fallback: convert to string for Utf8 builder
            (ScalarArrayBuilder::Utf8(b), scalar) => {
                if scalar.is_null() {
                    b.append_null();
                } else {
                    b.append_value(scalar.to_string());
                }
            }
            // For type mismatches with non-Utf8 builders, append null
            (ScalarArrayBuilder::UInt64(b), _) => b.append_null(),
            (ScalarArrayBuilder::Int64(b), _) => b.append_null(),
            (ScalarArrayBuilder::Float64(b), _) => b.append_null(),
            (ScalarArrayBuilder::Int32(b), _) => b.append_null(),
            (ScalarArrayBuilder::UInt32(b), _) => b.append_null(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ScalarArrayBuilder::Utf8(b) => Arc::new(b.finish()),
            ScalarArrayBuilder::UInt64(b) => Arc::new(b.finish()),
            ScalarArrayBuilder::Int64(b) => Arc::new(b.finish()),
            ScalarArrayBuilder::Float64(b) => Arc::new(b.finish()),
            ScalarArrayBuilder::Int32(b) => Arc::new(b.finish()),
            ScalarArrayBuilder::UInt32(b) => Arc::new(b.finish()),
        }
    }
}
