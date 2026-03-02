//! Conversion Utilities Module
//!
//! Provides functions to convert between store results and Arrow RecordBatches.
//! This enables DataFusion physical operators to work with precomputed outputs.

use arrow::array::{ArrayRef, BinaryBuilder, Float64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use std::collections::HashMap;
use std::sync::Arc;

use crate::data_model::KeyByLabelValues;
use crate::engines::physical::accumulator_serde::serialize_accumulator_arroyo;
use crate::stores::traits::TimestampedBucketsMap;

/// Convert store query results to an Arrow RecordBatch.
///
/// The output schema is: [label_columns..., sketch (Binary)]
/// Each row represents one group key with its serialized accumulator.
///
/// # Arguments
/// * `store_result` - HashMap from group key to accumulators
/// * `label_names` - Names of the label columns (in order)
///
/// # Returns
/// A RecordBatch with label columns (Utf8) and a sketch column (Binary)
pub fn store_result_to_record_batch(
    store_result: &TimestampedBucketsMap,
    label_names: &[String],
) -> Result<RecordBatch, DataFusionError> {
    // Build arrays for each label column
    let mut label_builders: Vec<StringBuilder> =
        label_names.iter().map(|_| StringBuilder::new()).collect();

    // Build array for sketch column
    let mut sketch_builder = BinaryBuilder::new();

    for (key_opt, timestamped_buckets) in store_result {
        // Handle each accumulator for this key
        for (_timestamps, acc) in timestamped_buckets {
            // Add label values
            if let Some(key) = key_opt {
                for (i, label_value) in key.labels.iter().enumerate() {
                    if i < label_builders.len() {
                        label_builders[i].append_value(label_value);
                    }
                }
                // Pad with empty strings if key has fewer labels than expected
                for item in label_builders.iter_mut().skip(key.labels.len()) {
                    item.append_value("");
                }
            } else {
                // No key - use empty strings for all labels
                for builder in &mut label_builders {
                    builder.append_value("");
                }
            }

            // Serialize accumulator to Arroyo-compatible bytes (MessagePack)
            // so downstream operators can deserialize with deserialize_from_bytes_arroyo
            let bytes = serialize_accumulator_arroyo(acc.as_ref());
            sketch_builder.append_value(&bytes);
        }
    }

    // Build schema
    let mut fields: Vec<Field> = label_names
        .iter()
        .map(|name| Field::new(name, DataType::Utf8, true))
        .collect();
    fields.push(Field::new("sketch", DataType::Binary, false));
    let schema = Arc::new(Schema::new(fields));

    // Build columns
    let mut columns: Vec<ArrayRef> = label_builders
        .iter_mut()
        .map(|b| Arc::new(b.finish()) as ArrayRef)
        .collect();
    columns.push(Arc::new(sketch_builder.finish()));

    RecordBatch::try_new(schema, columns)
        .map_err(|e| DataFusionError::Internal(format!("Failed to create RecordBatch: {}", e)))
}

/// Convert a RecordBatch with inferred values back to a result map.
///
/// The input schema is expected to be: [label_columns..., value_column (Float64)]
///
/// # Arguments
/// * `batch` - RecordBatch with label columns and a value column
/// * `label_names` - Names of the label columns (to identify which columns are labels)
/// * `value_column` - Name of the column containing the inferred values
///
/// # Returns
/// A HashMap from group key to the extracted value
pub fn record_batch_to_result_map(
    batch: &RecordBatch,
    label_names: &[&str],
    value_column: &str,
) -> Result<HashMap<Option<KeyByLabelValues>, f64>, DataFusionError> {
    let mut result: HashMap<Option<KeyByLabelValues>, f64> = HashMap::new();

    // Find the value column
    let value_col_idx = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == value_column)
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "No '{}' column found in batch schema",
                value_column
            ))
        })?;

    let value_array = batch
        .column(value_col_idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!("'{}' column is not Float64", value_column))
        })?;

    // Find label column indices
    let label_indices: Vec<usize> = label_names
        .iter()
        .filter_map(|name| {
            batch
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == *name)
        })
        .collect();

    for row_idx in 0..batch.num_rows() {
        // Extract label values for this row
        let labels: Vec<String> = label_indices
            .iter()
            .map(|&col_idx| {
                let col = batch.column(col_idx);
                // Try to extract string value
                if let Some(str_array) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                    str_array.value(row_idx).to_string()
                } else {
                    String::new()
                }
            })
            .collect();

        let key = if labels.is_empty() || labels.iter().all(|l| l.is_empty()) {
            None
        } else {
            Some(KeyByLabelValues { labels })
        };

        let value = value_array.value(row_idx);
        result.insert(key, value);
    }

    Ok(result)
}

/// Helper function to count total rows in store result
pub fn count_store_result_rows(store_result: &TimestampedBucketsMap) -> usize {
    store_result.values().map(|v| v.len()).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::precompute_operators::SumAccumulator;
    use crate::stores::traits::TimestampedBucket;

    fn make_bucket(acc: Box<dyn crate::AggregateCore>) -> TimestampedBucket {
        ((0, 0), acc)
    }

    #[test]
    fn test_store_result_to_record_batch_basic() {
        let mut store_result: TimestampedBucketsMap = HashMap::new();

        let key1 = KeyByLabelValues {
            labels: vec!["host-a".to_string()],
        };
        let acc1 = Box::new(SumAccumulator::with_sum(100.0));
        store_result.insert(Some(key1), vec![make_bucket(acc1)]);

        let key2 = KeyByLabelValues {
            labels: vec!["host-b".to_string()],
        };
        let acc2 = Box::new(SumAccumulator::with_sum(200.0));
        store_result.insert(Some(key2), vec![make_bucket(acc2)]);

        let label_names = vec!["host".to_string()];
        let batch = store_result_to_record_batch(&store_result, &label_names).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2); // host, sketch
    }

    #[test]
    fn test_store_result_to_record_batch_multiple_labels() {
        let mut store_result: TimestampedBucketsMap = HashMap::new();

        let key1 = KeyByLabelValues {
            labels: vec!["host-a".to_string(), "region-1".to_string()],
        };
        let acc1 = Box::new(SumAccumulator::with_sum(100.0));
        store_result.insert(Some(key1), vec![make_bucket(acc1)]);

        let label_names = vec!["host".to_string(), "region".to_string()];
        let batch = store_result_to_record_batch(&store_result, &label_names).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3); // host, region, sketch
    }

    #[test]
    fn test_store_result_to_record_batch_no_key() {
        let mut store_result: TimestampedBucketsMap = HashMap::new();

        let acc = Box::new(SumAccumulator::with_sum(500.0));
        store_result.insert(None, vec![make_bucket(acc)]);

        let label_names: Vec<String> = vec![];
        let batch = store_result_to_record_batch(&store_result, &label_names).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1); // just sketch
    }

    #[test]
    fn test_record_batch_to_result_map() {
        // Create a test batch with [host, value]
        let host_array = arrow::array::StringArray::from(vec!["host-a", "host-b"]);
        let value_array = Float64Array::from(vec![100.0, 200.0]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(host_array) as ArrayRef,
                Arc::new(value_array) as ArrayRef,
            ],
        )
        .unwrap();

        let result = record_batch_to_result_map(&batch, &["host"], "value").unwrap();

        assert_eq!(result.len(), 2);

        let key_a = KeyByLabelValues {
            labels: vec!["host-a".to_string()],
        };
        assert_eq!(result.get(&Some(key_a)), Some(&100.0));

        let key_b = KeyByLabelValues {
            labels: vec!["host-b".to_string()],
        };
        assert_eq!(result.get(&Some(key_b)), Some(&200.0));
    }

    // ========================================================================
    // Edge case tests
    // ========================================================================

    #[test]
    fn test_store_result_to_record_batch_empty() {
        let store_result: TimestampedBucketsMap = HashMap::new();
        let label_names = vec!["host".to_string()];
        let batch = store_result_to_record_batch(&store_result, &label_names).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2); // host + sketch
    }

    #[test]
    fn test_store_result_to_record_batch_five_labels() {
        let mut store_result: TimestampedBucketsMap = HashMap::new();
        let key = KeyByLabelValues {
            labels: vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string(),
            ],
        };
        store_result.insert(
            Some(key),
            vec![make_bucket(Box::new(SumAccumulator::with_sum(1.0)))],
        );
        let label_names: Vec<String> = vec!["l1", "l2", "l3", "l4", "l5"]
            .into_iter()
            .map(String::from)
            .collect();
        let batch = store_result_to_record_batch(&store_result, &label_names).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 6); // 5 labels + sketch
    }

    #[test]
    fn test_store_result_to_record_batch_special_chars() {
        let mut store_result: TimestampedBucketsMap = HashMap::new();
        let key = KeyByLabelValues {
            labels: vec!["host,with,commas".to_string(), "région-1".to_string()],
        };
        store_result.insert(
            Some(key),
            vec![make_bucket(Box::new(SumAccumulator::with_sum(42.0)))],
        );
        let label_names = vec!["host".to_string(), "region".to_string()];
        let batch = store_result_to_record_batch(&store_result, &label_names).unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Verify the special characters survived
        let host_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(host_col.value(0), "host,with,commas");
        let region_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(region_col.value(0), "région-1");
    }

    #[test]
    fn test_store_result_to_record_batch_multiple_timestamps_per_key() {
        let mut store_result: TimestampedBucketsMap = HashMap::new();
        let key = KeyByLabelValues {
            labels: vec!["host-a".to_string()],
        };
        // 3 buckets for the same key
        store_result.insert(
            Some(key),
            vec![
                (
                    (100, 200),
                    Box::new(SumAccumulator::with_sum(10.0)) as Box<dyn crate::AggregateCore>,
                ),
                (
                    (200, 300),
                    Box::new(SumAccumulator::with_sum(20.0)) as Box<dyn crate::AggregateCore>,
                ),
                (
                    (300, 400),
                    Box::new(SumAccumulator::with_sum(30.0)) as Box<dyn crate::AggregateCore>,
                ),
            ],
        );
        let label_names = vec!["host".to_string()];
        let batch = store_result_to_record_batch(&store_result, &label_names).unwrap();
        assert_eq!(batch.num_rows(), 3, "3 buckets should produce 3 rows");
    }

    #[test]
    fn test_record_batch_to_result_map_no_labels() {
        let value_array = Float64Array::from(vec![42.0]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(value_array) as ArrayRef]).unwrap();

        let result = record_batch_to_result_map(&batch, &[], "value").unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get(&None), Some(&42.0));
    }

    #[test]
    fn test_record_batch_to_result_map_missing_value_column() {
        let host_array = arrow::array::StringArray::from(vec!["host-a"]);
        let schema = Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(host_array) as ArrayRef]).unwrap();

        let result = record_batch_to_result_map(&batch, &["host"], "value");
        assert!(result.is_err(), "Missing value column should produce error");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("value"),
            "Error should mention 'value' column"
        );
    }

    #[test]
    fn test_record_batch_to_result_map_wrong_type() {
        // value column is Utf8 instead of Float64
        let host_array = arrow::array::StringArray::from(vec!["host-a"]);
        let value_array = arrow::array::StringArray::from(vec!["not_a_number"]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(host_array) as ArrayRef,
                Arc::new(value_array) as ArrayRef,
            ],
        )
        .unwrap();

        let result = record_batch_to_result_map(&batch, &["host"], "value");
        assert!(result.is_err(), "Wrong type for value column should error");
    }

    #[test]
    fn test_count_store_result_rows() {
        let mut store_result: TimestampedBucketsMap = HashMap::new();
        let key1 = KeyByLabelValues {
            labels: vec!["a".to_string()],
        };
        let key2 = KeyByLabelValues {
            labels: vec!["b".to_string()],
        };
        store_result.insert(
            Some(key1),
            vec![
                make_bucket(Box::new(SumAccumulator::with_sum(1.0))),
                make_bucket(Box::new(SumAccumulator::with_sum(2.0))),
            ],
        );
        store_result.insert(
            Some(key2),
            vec![make_bucket(Box::new(SumAccumulator::with_sum(3.0)))],
        );
        assert_eq!(count_store_result_rows(&store_result), 3);

        let empty: TimestampedBucketsMap = HashMap::new();
        assert_eq!(count_store_result_rows(&empty), 0);
    }
}
