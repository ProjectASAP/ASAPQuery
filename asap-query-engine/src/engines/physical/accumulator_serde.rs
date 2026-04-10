//! Accumulator Serialization/Deserialization Registry
//!
//! Provides functions to deserialize bytes back to accumulator objects
//! based on the SummaryType (SketchType).
//!
//! Note: This module assumes accumulators are serialized using the Arroyo format
//! (MessagePack serialization via rmp-serde).

use datafusion::error::DataFusionError;
use datafusion_summary_library::SketchType;

use crate::data_model::{MultipleSubpopulationAggregate, SingleSubpopulationAggregate};
use crate::precompute_operators::{
    CountMinSketchAccumulator, DatasketchesKLLAccumulator, DeltaSetAggregatorAccumulator,
    HydraKllSketchAccumulator, MultipleIncreaseAccumulator, MultipleSumAccumulator,
    SetAggregatorAccumulator, SumAccumulator,
};
use crate::AggregateCore;

/// Deserialize bytes to an accumulator based on the summary type.
///
/// This function dispatches to the appropriate accumulator deserializer
/// based on the SummaryType enum. Expects Arroyo format (MessagePack).
///
/// # Arguments
/// * `bytes` - Serialized accumulator data (Arroyo/MessagePack format)
/// * `summary_type` - Type of the summary (determines which deserializer to use)
///
/// # Returns
/// A boxed AggregateCore trait object
pub fn deserialize_accumulator(
    bytes: &[u8],
    summary_type: &SketchType,
) -> Result<Box<dyn AggregateCore>, DataFusionError> {
    match summary_type {
        // Single-population exact aggregators
        SketchType::Sum => {
            let acc = SumAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(|e| {
                DataFusionError::Internal(format!("Failed to deserialize Sum: {}", e))
            })?;
            Ok(Box::new(acc))
        }
        SketchType::Increase => Err(DataFusionError::NotImplemented(
            "Increase Arroyo deserialization not implemented".to_string(),
        )),
        SketchType::MinMax => Err(DataFusionError::NotImplemented(
            "MinMax Arroyo deserialization not implemented".to_string(),
        )),

        // Quantile sketches
        SketchType::KLL => {
            let acc =
                DatasketchesKLLAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to deserialize KLL: {}", e))
                })?;
            Ok(Box::new(acc))
        }
        SketchType::HydraKLL => {
            let acc =
                HydraKllSketchAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to deserialize HydraKLL: {}", e))
                })?;
            Ok(Box::new(acc))
        }

        // Set aggregators
        SketchType::SetAggregator => {
            let acc =
                SetAggregatorAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to deserialize SetAggregator: {}", e))
                })?;
            Ok(Box::new(acc))
        }
        SketchType::DeltaSetAggregator => {
            let acc = DeltaSetAggregatorAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(
                |e| {
                    DataFusionError::Internal(format!(
                        "Failed to deserialize DeltaSetAggregator: {}",
                        e
                    ))
                },
            )?;
            Ok(Box::new(acc))
        }

        // Multi-population exact aggregators
        SketchType::MultipleIncrease => {
            let acc =
                MultipleIncreaseAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to deserialize MultipleIncrease: {}",
                        e
                    ))
                })?;
            Ok(Box::new(acc))
        }
        SketchType::MultipleSum => {
            let acc =
                MultipleSumAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to deserialize MultipleSum: {}", e))
                })?;
            Ok(Box::new(acc))
        }

        // Frequency sketches
        SketchType::CountMinSketch => {
            let acc =
                CountMinSketchAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to deserialize CountMinSketch: {}",
                        e
                    ))
                })?;
            Ok(Box::new(acc))
        }

        // Sketches that aren't implemented yet
        _ => Err(DataFusionError::NotImplemented(format!(
            "Accumulator deserialization not implemented for: {:?}",
            summary_type
        ))),
    }
}

/// Serialize an accumulator to bytes (native format).
///
/// This is a convenience wrapper that calls serialize_to_bytes on the accumulator.
pub fn serialize_accumulator(acc: &dyn AggregateCore) -> Vec<u8> {
    acc.serialize_to_bytes()
}

/// Serialize an accumulator to Arroyo-compatible bytes (MessagePack format).
///
/// For accumulators whose native serialize_to_bytes already uses MessagePack,
/// this delegates to serialize_to_bytes. For others (SumAccumulator,
/// SetAggregatorAccumulator, MultipleIncreaseAccumulator), this uses
/// their serialize_to_bytes_arroyo method.
pub fn serialize_accumulator_arroyo(acc: &dyn AggregateCore) -> Vec<u8> {
    // Try to downcast to types that have a separate arroyo format
    if let Some(sum_acc) = acc.as_any().downcast_ref::<SumAccumulator>() {
        return sum_acc.serialize_to_bytes_arroyo();
    }
    if let Some(set_acc) = acc.as_any().downcast_ref::<SetAggregatorAccumulator>() {
        return set_acc.serialize_to_bytes_arroyo();
    }
    if let Some(inc_acc) = acc.as_any().downcast_ref::<MultipleIncreaseAccumulator>() {
        return inc_acc.serialize_to_bytes_arroyo();
    }
    if let Some(ms_acc) = acc.as_any().downcast_ref::<MultipleSumAccumulator>() {
        return ms_acc.serialize_to_bytes_arroyo();
    }
    // All other accumulators already use MessagePack in serialize_to_bytes
    acc.serialize_to_bytes()
}

/// Deserialize bytes to a SingleSubpopulationAggregate for querying.
///
/// This function returns a trait object that supports the query method.
/// Only works for single-subpopulation accumulators (Sum, Increase, MinMax, etc.).
///
/// Note: Uses Arroyo/MessagePack format.
pub fn deserialize_single_subpopulation(
    bytes: &[u8],
    summary_type: &SketchType,
) -> Result<Box<dyn SingleSubpopulationAggregate>, DataFusionError> {
    match summary_type {
        SketchType::Sum => {
            let acc = SumAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(|e| {
                DataFusionError::Internal(format!("Failed to deserialize Sum: {}", e))
            })?;
            Ok(Box::new(acc))
        }
        SketchType::Increase => Err(DataFusionError::NotImplemented(
            "Increase Arroyo deserialization not implemented".to_string(),
        )),
        SketchType::MinMax => Err(DataFusionError::NotImplemented(
            "MinMax Arroyo deserialization not implemented".to_string(),
        )),
        SketchType::KLL => {
            let acc =
                DatasketchesKLLAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to deserialize KLL: {}", e))
                })?;
            Ok(Box::new(acc))
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "SingleSubpopulationAggregate deserialization not implemented for: {:?}",
            summary_type
        ))),
    }
}

/// Deserialize bytes to a MultipleSubpopulationAggregate for querying.
///
/// This function returns a trait object that supports querying by sub-key.
/// Works for multi-population accumulators (Hydra types, CountMinSketch, etc.).
///
/// Note: Uses Arroyo/MessagePack format.
pub fn deserialize_multiple_subpopulation(
    bytes: &[u8],
    summary_type: &SketchType,
) -> Result<Box<dyn MultipleSubpopulationAggregate>, DataFusionError> {
    match summary_type {
        SketchType::MultipleIncrease => {
            let acc =
                MultipleIncreaseAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to deserialize MultipleIncrease: {}",
                        e
                    ))
                })?;
            Ok(Box::new(acc))
        }
        SketchType::MultipleSum => {
            let acc =
                MultipleSumAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to deserialize MultipleSum: {}", e))
                })?;
            Ok(Box::new(acc))
        }
        SketchType::HydraKLL => {
            let acc =
                HydraKllSketchAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to deserialize HydraKLL: {}", e))
                })?;
            Ok(Box::new(acc))
        }
        SketchType::CountMinSketch => {
            let acc =
                CountMinSketchAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Failed to deserialize CountMinSketch: {}",
                        e
                    ))
                })?;
            Ok(Box::new(acc))
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "MultipleSubpopulationAggregate deserialization not implemented for: {:?}",
            summary_type
        ))),
    }
}

/// Deserialize bytes to a keys accumulator (DeltaSetAggregator/SetAggregator).
///
/// Returns a boxed AggregateCore whose `get_keys()` method enumerates the sub-keys
/// stored in the accumulator.
pub fn deserialize_keys_accumulator(
    bytes: &[u8],
    summary_type: &SketchType,
) -> Result<Box<dyn AggregateCore>, DataFusionError> {
    match summary_type {
        SketchType::DeltaSetAggregator => {
            let acc = DeltaSetAggregatorAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(
                |e| {
                    DataFusionError::Internal(format!(
                        "Failed to deserialize DeltaSetAggregator: {}",
                        e
                    ))
                },
            )?;
            Ok(Box::new(acc))
        }
        SketchType::SetAggregator => {
            let acc =
                SetAggregatorAccumulator::deserialize_from_bytes_arroyo(bytes).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to deserialize SetAggregator: {}", e))
                })?;
            Ok(Box::new(acc))
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Keys accumulator deserialization not supported for: {:?}",
            summary_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::AggregationType;

    // Helper to serialize f64 as MessagePack (Arroyo format)
    fn serialize_f64_arroyo(value: f64) -> Vec<u8> {
        rmp_serde::to_vec(&value).unwrap()
    }

    #[test]
    fn test_deserialize_sum_accumulator() {
        // SumAccumulator::deserialize_from_bytes_arroyo expects MessagePack f64
        let bytes = serialize_f64_arroyo(42.0);

        let restored = deserialize_accumulator(&bytes, &SketchType::Sum).unwrap();

        assert_eq!(restored.get_accumulator_type(), AggregationType::Sum);
    }

    #[test]
    fn test_deserialize_minmax_returns_not_implemented() {
        let bytes = vec![1, 2, 3, 4];
        let result = deserialize_accumulator(&bytes, &SketchType::MinMax);

        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_unsupported_type() {
        let bytes = vec![1, 2, 3, 4];
        let result = deserialize_accumulator(&bytes, &SketchType::HLL);

        assert!(result.is_err());
    }
}
