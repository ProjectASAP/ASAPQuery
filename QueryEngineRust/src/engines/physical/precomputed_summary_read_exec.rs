//! PrecomputedSummaryReadExec - Physical execution operator for reading precomputed summaries
//!
//! This operator reads precomputed aggregates from a Store and produces
//! RecordBatches with label columns and a serialized sketch column.

use arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::UserDefinedLogicalNodeCore;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_summary_library::PrecomputedSummaryRead;
use futures::stream;
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

use super::format_schema;
use crate::engines::physical::conversion::store_result_to_record_batch;
use crate::stores::Store;

/// Physical execution plan for reading precomputed summaries from a store.
pub struct PrecomputedSummaryReadExec {
    /// The logical operator this was created from
    logical_node: PrecomputedSummaryRead,
    /// Reference to the store
    store: Arc<dyn Store>,
    /// Output schema
    schema: SchemaRef,
    /// Plan properties (cached)
    properties: PlanProperties,
}

impl PrecomputedSummaryReadExec {
    pub fn new(logical_node: PrecomputedSummaryRead, store: Arc<dyn Store>) -> Self {
        // Convert DFSchema to Schema
        let schema = Arc::new(logical_node.schema().as_ref().into());

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            logical_node,
            store,
            schema,
            properties,
        }
    }
}

impl fmt::Debug for PrecomputedSummaryReadExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrecomputedSummaryReadExec")
            .field("metric", &self.logical_node.metric())
            .field("aggregation_id", &self.logical_node.aggregation_id())
            .finish()
    }
}

impl DisplayAs for PrecomputedSummaryReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PrecomputedSummaryReadExec: metric={}, agg_id={}, range=[{}, {}]",
            self.logical_node.metric(),
            self.logical_node.aggregation_id(),
            self.logical_node.start_timestamp(),
            self.logical_node.end_timestamp()
        )
    }
}

impl ExecutionPlan for PrecomputedSummaryReadExec {
    fn name(&self) -> &str {
        "PrecomputedSummaryReadExec"
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
        vec![] // Leaf node
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // No children to replace
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        debug!(
            metric = %self.logical_node.metric(),
            aggregation_id = self.logical_node.aggregation_id(),
            start_timestamp = self.logical_node.start_timestamp(),
            end_timestamp = self.logical_node.end_timestamp(),
            is_exact_query = self.logical_node.is_exact_query(),
            output_schema = %format_schema(&self.schema),
            output_labels = ?self.logical_node.output_labels(),
            "PrecomputedSummaryReadExec::execute"
        );

        // Query the store
        let store_query_start = Instant::now();
        let store_result = if self.logical_node.is_exact_query() {
            self.store.query_precomputed_output_exact(
                self.logical_node.metric(),
                self.logical_node.aggregation_id(),
                self.logical_node.start_timestamp(),
                self.logical_node.end_timestamp(),
            )
        } else {
            self.store.query_precomputed_output(
                self.logical_node.metric(),
                self.logical_node.aggregation_id(),
                self.logical_node.start_timestamp(),
                self.logical_node.end_timestamp(),
            )
        }
        .map_err(DataFusionError::External)?;
        debug!(
            store_query_ms = format!("{:.2}", store_query_start.elapsed().as_secs_f64() * 1000.0),
            unique_keys = store_result.len(),
            "PrecomputedSummaryReadExec store query complete"
        );

        // Convert to RecordBatch
        let convert_start = Instant::now();
        let label_names: Vec<String> = self.logical_node.output_labels().to_vec();
        let batch = store_result_to_record_batch(&store_result, &label_names)?;

        debug!(
            convert_ms = format!("{:.2}", convert_start.elapsed().as_secs_f64() * 1000.0),
            output_rows = batch.num_rows(),
            output_cols = batch.num_columns(),
            "PrecomputedSummaryReadExec produced batch"
        );

        // Create a stream that yields this single batch
        let schema = self.schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(async move { Ok(batch) }),
        )))
    }
}
