use crate::data_model::KeyByLabelValues;
use serde_json::Value;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

pub use asap_types::traits::SerializableToSink;

/// Core trait for all aggregates containing shared functionality
/// This trait provides common operations like serialization, cloning, and type identification
pub trait AggregateCore: SerializableToSink + Send + Sync {
    /// Clone this accumulator into a boxed trait object
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore>;

    /// Get the type name of this accumulator
    fn type_name(&self) -> &'static str;

    /// Downcast to Any for type checking
    fn as_any(&self) -> &dyn std::any::Any;

    /// Merge this accumulator with another accumulator of the same type
    /// Returns a new merged accumulator, leaving the original unchanged
    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>>;

    /// Get the accumulator type identifier for merge compatibility checking
    fn get_accumulator_type(&self) -> &'static str;

    /// Get all keys stored in this accumulator
    fn get_keys(&self) -> Option<Vec<KeyByLabelValues>>;
}

/// Trait for accumulators that support a single subpopulation
/// These accumulators store a single aggregate value (e.g., Sum, Increase)
pub trait SingleSubpopulationAggregate: AggregateCore {
    /// Query the accumulator for a specific statistic
    fn query(
        &self,
        statistic: Statistic,
        query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>>;

    /// Clone this accumulator into a boxed trait object
    fn clone_boxed(&self) -> Box<dyn SingleSubpopulationAggregate>;
}

/// Trait for accumulators that support multiple subpopulations identified by keys
/// These accumulators store separate values for different label combinations
pub trait MultipleSubpopulationAggregate: AggregateCore {
    /// Query the accumulator for a specific statistic and key
    fn query(
        &self,
        statistic: Statistic,
        key: &KeyByLabelValues,
        query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>>;

    /// Clone this accumulator into a boxed trait object
    fn clone_boxed(&self) -> Box<dyn MultipleSubpopulationAggregate>;
}

/// Factory traits for creating and merging accumulators (object-safe)
pub trait SingleSubpopulationAggregateFactory {
    fn merge_accumulators(
        &self,
        accumulators: Vec<Box<dyn SingleSubpopulationAggregate>>,
    ) -> Result<Box<dyn SingleSubpopulationAggregate>, Box<dyn std::error::Error + Send + Sync>>;
    fn create_default(&self) -> Box<dyn SingleSubpopulationAggregate>;
}

pub trait MultipleSubpopulationAggregateFactory {
    fn merge_accumulators(
        &self,
        accumulators: Vec<Box<dyn MultipleSubpopulationAggregate>>,
    ) -> Result<Box<dyn MultipleSubpopulationAggregate>, Box<dyn std::error::Error + Send + Sync>>;
    fn create_default(&self) -> Box<dyn MultipleSubpopulationAggregate>;
}

/// Trait for merging multiple accumulators of the same type
pub trait MergeableAccumulator<T> {
    fn merge_accumulators(
        accumulators: Vec<T>,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        T: Sized;
}

// Implement Clone for the new trait objects
impl Clone for Box<dyn AggregateCore> {
    fn clone(&self) -> Self {
        self.clone_boxed_core()
    }
}

impl Clone for Box<dyn SingleSubpopulationAggregate> {
    fn clone(&self) -> Self {
        self.clone_boxed()
    }
}

impl Clone for Box<dyn MultipleSubpopulationAggregate> {
    fn clone(&self) -> Self {
        self.clone_boxed()
    }
}

/// Factory trait for creating accumulators from serialized data
pub trait AccumulatorFactory {
    fn create_from_json(
        accumulator_type: &str,
        data: &Value,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error>>;
    fn create_from_bytes(
        accumulator_type: &str,
        buffer: &[u8],
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error>>;
}

#[cfg(test)]
mod tests {}
