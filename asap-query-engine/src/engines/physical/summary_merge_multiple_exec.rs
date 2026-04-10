//! SummaryMergeMultipleExec - Physical execution operator for merging summaries
//!
//! This operator merges multiple summaries with the same group key into one.
//! Input: multiple rows per group key with serialized accumulators
//! Output: one row per group key with merged accumulator

use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder, StringBuilder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_summary_library::SummaryMergeMultiple;
use futures::stream;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

use super::format_schema;
use crate::engines::physical::accumulator_serde::{
    deserialize_accumulator, serialize_accumulator_arroyo,
};

/// Physical execution plan for merging multiple summaries by group key.
pub struct SummaryMergeMultipleExec {
    /// The logical operator this was created from
    logical_node: SummaryMergeMultiple,
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Output schema (same as input)
    schema: SchemaRef,
    /// Plan properties (cached)
    properties: PlanProperties,
}

impl SummaryMergeMultipleExec {
    pub fn new(logical_node: SummaryMergeMultiple, input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = input.schema();

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            logical_node,
            input,
            schema,
            properties,
        }
    }
}

impl fmt::Debug for SummaryMergeMultipleExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SummaryMergeMultipleExec")
            .field("group_by", &self.logical_node.group_by())
            .field("summary_type", &self.logical_node.summary_type())
            .finish()
    }
}

impl DisplayAs for SummaryMergeMultipleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SummaryMergeMultipleExec: group_by=[{}], type={}",
            self.logical_node.group_by().join(", "),
            self.logical_node.summary_type()
        )
    }
}

impl ExecutionPlan for SummaryMergeMultipleExec {
    fn name(&self) -> &str {
        "SummaryMergeMultipleExec"
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
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "SummaryMergeMultipleExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            self.logical_node.clone(),
            children[0].clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let input_stream = self.input.execute(partition, context)?;
        let schema = self.schema.clone();
        let schema_for_stream = schema.clone();
        let logical_node = self.logical_node.clone();

        debug!(
            input_schema = %format_schema(&self.input.schema()),
            output_schema = %format_schema(&self.schema),
            group_by = ?self.logical_node.group_by(),
            sketch_column = %self.logical_node.sketch_column(),
            summary_type = ?self.logical_node.summary_type(),
            "SummaryMergeMultipleExec::execute"
        );

        // Use an async block to process all batches and merge
        let output_stream = async move {
            let mut groups: HashMap<Vec<String>, (Vec<String>, Vec<u8>)> = HashMap::new();

            // Collect all batches from input using datafusion's collect helper
            let collect_start = Instant::now();
            let batches = collect(input_stream).await?;
            let total_input_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            debug!(
                collect_ms = format!("{:.2}", collect_start.elapsed().as_secs_f64() * 1000.0),
                total_input_rows,
                num_batches = batches.len(),
                "SummaryMergeMultipleExec collected input"
            );

            // Process each batch
            let merge_start = Instant::now();
            for batch in &batches {
                process_batch(&mut groups, batch, &logical_node)?;
            }
            debug!(
                merge_ms = format!("{:.2}", merge_start.elapsed().as_secs_f64() * 1000.0),
                output_groups = groups.len(),
                "SummaryMergeMultipleExec merged into groups"
            );

            // Build output batch
            build_output_batch(&groups, &logical_node, &schema)
        };

        // Convert to stream
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_stream,
            stream::once(output_stream),
        )))
    }
}

/// Process a batch and accumulate into groups
fn process_batch(
    groups: &mut HashMap<Vec<String>, (Vec<String>, Vec<u8>)>,
    batch: &RecordBatch,
    logical_node: &SummaryMergeMultiple,
) -> Result<(), DataFusionError> {
    let group_by_cols = logical_node.group_by();
    let sketch_col_name = logical_node.sketch_column();

    // Find column indices
    let group_indices: Vec<usize> = group_by_cols
        .iter()
        .filter_map(|name| {
            batch
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == name)
        })
        .collect();

    let sketch_idx = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == sketch_col_name)
        .ok_or_else(|| {
            DataFusionError::Internal(format!("Sketch column '{}' not found", sketch_col_name))
        })?;

    let sketch_array = batch
        .column(sketch_idx)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| DataFusionError::Internal("Sketch column is not Binary".to_string()))?;

    for row in 0..batch.num_rows() {
        // Extract group key
        let group_key: Vec<String> = group_indices
            .iter()
            .map(|&idx| {
                let col = batch.column(idx);
                if let Some(str_array) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                    str_array.value(row).to_string()
                } else {
                    String::new()
                }
            })
            .collect();

        // Get sketch bytes
        let sketch_bytes = sketch_array.value(row);

        // Merge with existing group or insert new
        if let Some((_, existing_bytes)) = groups.get_mut(&group_key) {
            // Deserialize both accumulators and merge
            let existing_acc =
                deserialize_accumulator(existing_bytes, logical_node.summary_type())?;
            let new_acc = deserialize_accumulator(sketch_bytes, logical_node.summary_type())?;

            // Merge accumulators
            let merged = existing_acc.merge_with(new_acc.as_ref()).map_err(|e| {
                DataFusionError::Internal(format!("Failed to merge accumulators: {}", e))
            })?;

            // Serialize merged accumulator in arroyo format for downstream deserialization
            *existing_bytes = serialize_accumulator_arroyo(merged.as_ref());
        } else {
            // First time seeing this group
            groups.insert(group_key.clone(), (group_key, sketch_bytes.to_vec()));
        }
    }

    Ok(())
}

/// Build output batch from merged groups (public for testing)
pub(crate) fn build_output_batch(
    groups: &HashMap<Vec<String>, (Vec<String>, Vec<u8>)>,
    logical_node: &SummaryMergeMultiple,
    schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let group_by_cols = logical_node.group_by();

    // Build arrays for each column
    let mut label_builders: Vec<StringBuilder> =
        group_by_cols.iter().map(|_| StringBuilder::new()).collect();
    let mut sketch_builder = BinaryBuilder::new();

    for (label_values, bytes) in groups.values() {
        // Add label values
        for (i, value) in label_values.iter().enumerate() {
            if i < label_builders.len() {
                label_builders[i].append_value(value);
            }
        }
        // Add sketch bytes
        sketch_builder.append_value(bytes);
    }

    // Build columns
    let mut columns: Vec<ArrayRef> = label_builders
        .iter_mut()
        .map(|b| Arc::new(b.finish()) as ArrayRef)
        .collect();
    columns.push(Arc::new(sketch_builder.finish()));

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| DataFusionError::Internal(format!("Failed to build output batch: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::{AggregationType, KeyByLabelValues};
    use crate::engines::physical::accumulator_serde::serialize_accumulator_arroyo;
    use crate::precompute_operators::{
        DatasketchesKLLAccumulator, SetAggregatorAccumulator, SumAccumulator,
    };
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_summary_library::SketchType;

    /// Helper to create a RecordBatch with [host (Utf8), sketch (Binary)]
    fn make_batch(rows: Vec<(&str, Vec<u8>)>) -> RecordBatch {
        let mut host_builder = StringBuilder::new();
        let mut sketch_builder = BinaryBuilder::new();
        for (host, sketch_bytes) in &rows {
            host_builder.append_value(host);
            sketch_builder.append_value(sketch_bytes);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("sketch", DataType::Binary, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(host_builder.finish()) as ArrayRef,
                Arc::new(sketch_builder.finish()) as ArrayRef,
            ],
        )
        .unwrap()
    }

    /// Helper to create a SummaryMergeMultiple logical node for testing
    fn make_logical_node(summary_type: SketchType) -> SummaryMergeMultiple {
        use arrow::datatypes::DataType as DT;
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::{Extension, LogicalPlan};
        use datafusion_summary_library::PrecomputedSummaryRead;

        let fields = vec![
            (None, Arc::new(Field::new("host", DT::Utf8, true))),
            (None, Arc::new(Field::new("sketch", DT::Binary, false))),
        ];
        let schema = Arc::new(DFSchema::new_with_metadata(fields, Default::default()).unwrap());
        let read = PrecomputedSummaryRead::new(
            "test".to_string(),
            1,
            0,
            1000,
            true,
            vec!["host".to_string()],
            summary_type.clone(),
            schema,
        );
        let read_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(read),
        });
        SummaryMergeMultiple::new(
            Arc::new(read_plan),
            vec!["host".to_string()],
            "sketch".to_string(),
            summary_type,
        )
    }

    #[test]
    fn test_merge_single_row_passthrough() {
        let acc = SumAccumulator::with_sum(42.0);
        let bytes = serialize_accumulator_arroyo(&acc);
        let batch = make_batch(vec![("host-a", bytes.clone())]);

        let logical = make_logical_node(SketchType::Sum);
        let mut groups: HashMap<Vec<String>, (Vec<String>, Vec<u8>)> = HashMap::new();
        process_batch(&mut groups, &batch, &logical).unwrap();

        assert_eq!(groups.len(), 1);
        // The single row should pass through unchanged
        let (_, merged_bytes) = groups.values().next().unwrap();
        let restored = deserialize_accumulator(merged_bytes, &SketchType::Sum).unwrap();
        assert_eq!(restored.get_accumulator_type(), AggregationType::Sum);
    }

    #[test]
    fn test_merge_two_sums_same_group() {
        let acc1 = SumAccumulator::with_sum(50.0);
        let acc2 = SumAccumulator::with_sum(50.0);
        let bytes1 = serialize_accumulator_arroyo(&acc1);
        let bytes2 = serialize_accumulator_arroyo(&acc2);
        let batch = make_batch(vec![("host-a", bytes1), ("host-a", bytes2)]);

        let logical = make_logical_node(SketchType::Sum);
        let mut groups: HashMap<Vec<String>, (Vec<String>, Vec<u8>)> = HashMap::new();
        process_batch(&mut groups, &batch, &logical).unwrap();

        assert_eq!(groups.len(), 1);
        let (_, merged_bytes) = groups.values().next().unwrap();
        let restored =
            crate::engines::physical::accumulator_serde::deserialize_single_subpopulation(
                merged_bytes,
                &SketchType::Sum,
            )
            .unwrap();
        let value = restored
            .query(promql_utilities::query_logics::enums::Statistic::Sum, None)
            .unwrap();
        assert!(
            (value - 100.0).abs() < 1e-10,
            "Merged sum should be 100.0, got {}",
            value
        );
    }

    #[test]
    fn test_merge_three_sums_associativity() {
        let bytes: Vec<Vec<u8>> = [30.0, 40.0, 30.0]
            .iter()
            .map(|v| serialize_accumulator_arroyo(&SumAccumulator::with_sum(*v)))
            .collect();
        let batch = make_batch(vec![
            ("host-a", bytes[0].clone()),
            ("host-a", bytes[1].clone()),
            ("host-a", bytes[2].clone()),
        ]);

        let logical = make_logical_node(SketchType::Sum);
        let mut groups: HashMap<Vec<String>, (Vec<String>, Vec<u8>)> = HashMap::new();
        process_batch(&mut groups, &batch, &logical).unwrap();

        assert_eq!(groups.len(), 1);
        let (_, merged_bytes) = groups.values().next().unwrap();
        let restored =
            crate::engines::physical::accumulator_serde::deserialize_single_subpopulation(
                merged_bytes,
                &SketchType::Sum,
            )
            .unwrap();
        let value = restored
            .query(promql_utilities::query_logics::enums::Statistic::Sum, None)
            .unwrap();
        assert!(
            (value - 100.0).abs() < 1e-10,
            "30+40+30 should be 100.0, got {}",
            value
        );
    }

    #[test]
    fn test_merge_separate_groups_no_contamination() {
        let bytes_a = serialize_accumulator_arroyo(&SumAccumulator::with_sum(100.0));
        let bytes_b = serialize_accumulator_arroyo(&SumAccumulator::with_sum(200.0));
        let batch = make_batch(vec![("host-a", bytes_a), ("host-b", bytes_b)]);

        let logical = make_logical_node(SketchType::Sum);
        let mut groups: HashMap<Vec<String>, (Vec<String>, Vec<u8>)> = HashMap::new();
        process_batch(&mut groups, &batch, &logical).unwrap();

        assert_eq!(groups.len(), 2, "Two different hosts should stay separate");

        // Verify values
        for (key, (_, bytes)) in &groups {
            let restored =
                crate::engines::physical::accumulator_serde::deserialize_single_subpopulation(
                    bytes,
                    &SketchType::Sum,
                )
                .unwrap();
            let value = restored
                .query(promql_utilities::query_logics::enums::Statistic::Sum, None)
                .unwrap();
            if key[0] == "host-a" {
                assert!((value - 100.0).abs() < 1e-10);
            } else {
                assert!((value - 200.0).abs() < 1e-10);
            }
        }
    }

    #[test]
    fn test_merge_kll_sketches() {
        let mut kll1 = DatasketchesKLLAccumulator::new(200);
        kll1._update(1.0);
        kll1._update(2.0);
        let mut kll2 = DatasketchesKLLAccumulator::new(200);
        kll2._update(3.0);
        kll2._update(4.0);

        let bytes1 = serialize_accumulator_arroyo(&kll1);
        let bytes2 = serialize_accumulator_arroyo(&kll2);
        let batch = make_batch(vec![("host-a", bytes1), ("host-a", bytes2)]);

        let logical = make_logical_node(SketchType::KLL);
        let mut groups: HashMap<Vec<String>, (Vec<String>, Vec<u8>)> = HashMap::new();
        process_batch(&mut groups, &batch, &logical).unwrap();

        assert_eq!(groups.len(), 1);
        // Verify merged KLL has data from both
        let (_, merged_bytes) = groups.values().next().unwrap();
        let restored =
            crate::engines::physical::accumulator_serde::deserialize_single_subpopulation(
                merged_bytes,
                &SketchType::KLL,
            )
            .unwrap();
        let mut kwargs = std::collections::HashMap::new();
        kwargs.insert("quantile".to_string(), "1.0".to_string());
        let max = restored
            .query(
                promql_utilities::query_logics::enums::Statistic::Quantile,
                Some(&kwargs),
            )
            .unwrap();
        assert!(
            (max - 4.0).abs() < 1e-10,
            "Max quantile should be 4.0, got {}",
            max
        );
    }

    #[test]
    fn test_merge_set_aggregators() {
        let mut set1 = SetAggregatorAccumulator::new();
        set1.add_key(KeyByLabelValues {
            labels: vec!["a".to_string()],
        });
        let mut set2 = SetAggregatorAccumulator::new();
        set2.add_key(KeyByLabelValues {
            labels: vec!["b".to_string()],
        });

        let bytes1 = serialize_accumulator_arroyo(&set1);
        let bytes2 = serialize_accumulator_arroyo(&set2);
        let batch = make_batch(vec![("host-a", bytes1), ("host-a", bytes2)]);

        let logical = make_logical_node(SketchType::SetAggregator);
        let mut groups: HashMap<Vec<String>, (Vec<String>, Vec<u8>)> = HashMap::new();
        process_batch(&mut groups, &batch, &logical).unwrap();

        assert_eq!(groups.len(), 1);
        let (_, merged_bytes) = groups.values().next().unwrap();
        let restored = deserialize_accumulator(merged_bytes, &SketchType::SetAggregator).unwrap();
        let keys = restored.get_keys().unwrap();
        assert_eq!(keys.len(), 2, "Union of two sets should have 2 keys");
    }

    #[test]
    fn test_merge_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("sketch", DataType::Binary, false),
        ]));
        let host_array = StringArray::from(Vec::<&str>::new());
        let sketch_array = arrow::array::BinaryArray::from(Vec::<&[u8]>::new());
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(host_array) as ArrayRef,
                Arc::new(sketch_array) as ArrayRef,
            ],
        )
        .unwrap();

        let logical = make_logical_node(SketchType::Sum);
        let mut groups: HashMap<Vec<String>, (Vec<String>, Vec<u8>)> = HashMap::new();
        process_batch(&mut groups, &batch, &logical).unwrap();

        assert_eq!(groups.len(), 0, "Empty batch should produce 0 groups");
    }
}
