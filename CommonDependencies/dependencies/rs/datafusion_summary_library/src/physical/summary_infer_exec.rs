// Physical execution plan for SummaryInfer (sketch querying).
//
// This ExecutionPlan reads sketch data and extracts results.
// Currently only supports CountDistinct operation on HLL sketches.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, ArrayRef, BinaryArray, RecordBatch, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use futures::Stream;

use crate::sketch_operators::InferOperation;

/// Physical execution plan for extracting results from HLL sketches.
///
/// Takes input batches with sketch columns and produces one row per input row with:
/// - Group key columns (passed through)
/// - Result column (e.g., UInt64 for CountDistinct)
#[derive(Debug)]
pub struct SummaryInferExec {
    /// Input execution plan (typically SummaryInsertExec)
    input: Arc<dyn ExecutionPlan>,

    /// Index of the sketch column in input schema
    sketch_col_idx: usize,

    /// Infer operation to perform
    operation: InferOperation,

    /// Output column name
    output_name: String,

    /// Output schema
    schema: SchemaRef,

    /// Plan properties (cached)
    properties: PlanProperties,
}

impl SummaryInferExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        sketch_col_idx: usize,
        operation: InferOperation,
        output_name: String,
    ) -> Self {
        let input_schema = input.schema();

        // Build output schema: all columns except sketch column, plus result column
        let mut fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(idx, _)| *idx != sketch_col_idx)
            .map(|(_, f)| f.as_ref().clone())
            .collect();

        // Add result column based on operation type
        let result_type = match &operation {
            InferOperation::CountDistinct => DataType::UInt64,
            InferOperation::Quantile(_) | InferOperation::Median => DataType::Float64,
            _ => DataType::UInt64, // Default for unsupported ops
        };

        fields.push(Field::new(&output_name, result_type, false));

        let schema = Arc::new(Schema::new(fields));

        // Plan properties: same partitioning as input
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            input,
            sketch_col_idx,
            operation,
            output_name,
            schema,
            properties,
        }
    }
}

impl DisplayAs for SummaryInferExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "SummaryInferExec: sketch_col={}, op={}, output={}",
                    self.sketch_col_idx, self.operation, self.output_name
                )
            }
        }
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
                "SummaryInferExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(SummaryInferExec::new(
            children[0].clone(),
            self.sketch_col_idx,
            self.operation.clone(),
            self.output_name.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "SummaryInferExec only supports partition 0, got {}",
                partition
            )));
        }

        let input_stream = self.input.execute(0, context)?;

        let stream = SummaryInferStream::new(
            input_stream,
            self.schema.clone(),
            self.sketch_col_idx,
            self.operation.clone(),
        );

        Ok(Box::pin(stream))
    }
}

/// Stream that transforms sketch batches into result batches.
struct SummaryInferStream {
    /// Input stream
    input: SendableRecordBatchStream,

    /// Output schema
    schema: SchemaRef,

    /// Sketch column index
    sketch_col_idx: usize,

    /// Operation to perform
    operation: InferOperation,
}

impl SummaryInferStream {
    fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        sketch_col_idx: usize,
        operation: InferOperation,
    ) -> Self {
        Self {
            input,
            schema,
            sketch_col_idx,
            operation,
        }
    }

    /// Transform a batch by extracting results from sketches.
    fn transform_batch(&self, batch: &RecordBatch) -> DFResult<RecordBatch> {
        let num_rows = batch.num_rows();

        // Get the sketch column
        let sketch_col = batch.column(self.sketch_col_idx);
        let sketch_array = sketch_col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Sketch column is not Binary type".to_string())
            })?;

        // Build result column based on operation
        let result_col: ArrayRef = match &self.operation {
            InferOperation::CountDistinct => {
                let mut builder = UInt64Builder::with_capacity(num_rows);
                for i in 0..num_rows {
                    if sketch_array.is_null(i) {
                        builder.append_null();
                    } else {
                        let sketch_bytes = sketch_array.value(i);
                        // Deserialize and get count
                        // Note: Our simple serialization format stores the count directly
                        let count = if sketch_bytes.len() >= 9 {
                            let count_bytes: [u8; 8] = sketch_bytes[1..9].try_into().unwrap();
                            f64::from_le_bytes(count_bytes).round() as u64
                        } else {
                            0
                        };
                        builder.append_value(count);
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Infer operation {:?} not yet implemented",
                    self.operation
                )));
            }
        };

        // Build output columns: all input columns except sketch, plus result
        let mut columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .enumerate()
            .filter(|(idx, _)| *idx != self.sketch_col_idx)
            .map(|(_, col)| col.clone())
            .collect();
        columns.push(result_col);

        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| DataFusionError::ArrowError(e, None))
    }
}

impl Stream for SummaryInferStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let result = self.transform_batch(&batch);
                Poll::Ready(Some(result))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl datafusion::physical_plan::RecordBatchStream for SummaryInferStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
