use crate::data_model::{
    AggregateCore, AggregationType, Measurement, MergeableAccumulator, SerializableToSink,
    SingleSubpopulationAggregate, SingleSubpopulationAggregateFactory,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

/// Accumulator for tracking increases in counter metrics
/// Stores the starting and last seen measurements with timestamps
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncreaseAccumulator {
    pub starting_measurement: Measurement,
    pub starting_timestamp: i64,
    pub last_seen_measurement: Measurement,
    pub last_seen_timestamp: i64,
}

impl IncreaseAccumulator {
    pub fn new(
        starting_measurement: Measurement,
        starting_timestamp: i64,
        last_seen_measurement: Measurement,
        last_seen_timestamp: i64,
    ) -> Self {
        Self {
            starting_measurement,
            starting_timestamp,
            last_seen_measurement,
            last_seen_timestamp,
        }
    }

    pub fn update(&mut self, measurement: Measurement, timestamp: i64) {
        self.last_seen_measurement = measurement;
        self.last_seen_timestamp = timestamp;
    }

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let starting_measurement =
            Measurement::deserialize_from_json(&data["starting_measurement"])?;
        let starting_timestamp = data["starting_timestamp"]
            .as_i64()
            .ok_or("Missing or invalid 'starting_timestamp' field")?;
        let last_seen_measurement =
            Measurement::deserialize_from_json(&data["last_seen_measurement"])?;
        let last_seen_timestamp = data["last_seen_timestamp"]
            .as_i64()
            .ok_or("Missing or invalid 'last_seen_timestamp' field")?;

        Ok(Self::new(
            starting_measurement,
            starting_timestamp,
            last_seen_measurement,
            last_seen_timestamp,
        ))
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut offset = 0;

        // Read starting measurement length and data
        if buffer.len() < offset + 4 {
            return Err("Buffer too short for starting measurement length".into());
        }
        let starting_measurement_length = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]) as usize;
        offset += 4;

        if buffer.len() < offset + starting_measurement_length {
            return Err("Buffer too short for starting measurement".into());
        }
        let starting_measurement = Measurement::deserialize_from_bytes(
            &buffer[offset..offset + starting_measurement_length],
        )?;
        offset += starting_measurement_length;

        // Read starting timestamp
        if buffer.len() < offset + 8 {
            return Err("Buffer too short for starting timestamp".into());
        }
        let starting_timestamp = i64::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
            buffer[offset + 4],
            buffer[offset + 5],
            buffer[offset + 6],
            buffer[offset + 7],
        ]);
        offset += 8;

        // Read last seen measurement length and data
        if buffer.len() < offset + 4 {
            return Err("Buffer too short for last seen measurement length".into());
        }
        let last_seen_measurement_length = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]) as usize;
        offset += 4;

        if buffer.len() < offset + last_seen_measurement_length {
            return Err("Buffer too short for last seen measurement".into());
        }
        let last_seen_measurement = Measurement::deserialize_from_bytes(
            &buffer[offset..offset + last_seen_measurement_length],
        )?;
        offset += last_seen_measurement_length;

        // Read last seen timestamp
        if buffer.len() < offset + 8 {
            return Err("Buffer too short for last seen timestamp".into());
        }
        let last_seen_timestamp = i64::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
            buffer[offset + 4],
            buffer[offset + 5],
            buffer[offset + 6],
            buffer[offset + 7],
        ]);

        Ok(Self::new(
            starting_measurement,
            starting_timestamp,
            last_seen_measurement,
            last_seen_timestamp,
        ))
    }
}

impl SerializableToSink for IncreaseAccumulator {
    fn serialize_to_json(&self) -> Value {
        serde_json::json!({
            "starting_measurement": self.starting_measurement.serialize_to_json(),
            "starting_timestamp": self.starting_timestamp,
            "last_seen_measurement": self.last_seen_measurement.serialize_to_json(),
            "last_seen_timestamp": self.last_seen_timestamp,
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        let starting_measurement_bytes = self.starting_measurement.serialize_to_bytes();
        let last_seen_measurement_bytes = self.last_seen_measurement.serialize_to_bytes();

        let mut buffer = Vec::new();

        // Starting measurement length and data
        buffer.extend_from_slice(&(starting_measurement_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&starting_measurement_bytes);

        // Starting timestamp
        buffer.extend_from_slice(&self.starting_timestamp.to_le_bytes());

        // Last seen measurement length and data
        buffer.extend_from_slice(&(last_seen_measurement_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&last_seen_measurement_bytes);

        // Last seen timestamp
        buffer.extend_from_slice(&self.last_seen_timestamp.to_le_bytes());

        buffer
    }
}

impl MergeableAccumulator<IncreaseAccumulator> for IncreaseAccumulator {
    fn merge_accumulators(
        accumulators: Vec<IncreaseAccumulator>,
    ) -> Result<IncreaseAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let mut result = accumulators[0].clone();

        for acc in &accumulators[1..] {
            // Use the earlier starting point
            if acc.starting_timestamp < result.starting_timestamp {
                result.starting_measurement = acc.starting_measurement.clone();
                result.starting_timestamp = acc.starting_timestamp;
            }

            // Use the later last seen point
            if acc.last_seen_timestamp > result.last_seen_timestamp {
                result.last_seen_measurement = acc.last_seen_measurement.clone();
                result.last_seen_timestamp = acc.last_seen_timestamp;
            }
        }

        Ok(result)
    }
}

impl AggregateCore for IncreaseAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "IncreaseAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        // Check if other is also an IncreaseAccumulator
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge IncreaseAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        // Downcast to IncreaseAccumulator
        let other_increase = other
            .as_any()
            .downcast_ref::<IncreaseAccumulator>()
            .ok_or("Failed to downcast to IncreaseAccumulator")?;

        // Use the existing merge_accumulators method
        let merged = Self::merge_accumulators(vec![self.clone(), other_increase.clone()])?;

        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::Increase
    }

    fn get_keys(&self) -> Option<Vec<crate::KeyByLabelValues>> {
        None
    }

    fn query_statistic(
        &self,
        statistic: promql_utilities::query_logics::enums::Statistic,
        _key: &Option<crate::KeyByLabelValues>,
        _query_kwargs: &std::collections::HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        use crate::data_model::SingleSubpopulationAggregate;
        self.query(statistic, None)
    }
}

impl SingleSubpopulationAggregate for IncreaseAccumulator {
    fn query(
        &self,
        statistic: Statistic,
        query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        // IncreaseAccumulator doesn't use query_kwargs, assert it's None
        if query_kwargs.is_some() {
            return Err("IncreaseAccumulator does not support query parameters".into());
        }

        match statistic {
            Statistic::Increase => {
                Ok(self.last_seen_measurement.value - self.starting_measurement.value)
            }
            Statistic::Rate => {
                // Convert to per second; timestamps are in milliseconds
                let time_diff = (self.last_seen_timestamp - self.starting_timestamp) as f64;
                if time_diff <= 0.0 {
                    return Err("Invalid time difference for rate calculation".into());
                }
                let value_diff = self.last_seen_measurement.value - self.starting_measurement.value;
                Ok(value_diff / time_diff * 1000.0)
            }
            _ => Err(format!("Unsupported statistic in IncreaseAccumulator: {statistic:?}").into()),
        }
    }

    fn clone_boxed(&self) -> Box<dyn SingleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

pub struct IncreaseAccumulatorFactory;

impl SingleSubpopulationAggregateFactory for IncreaseAccumulatorFactory {
    fn merge_accumulators(
        &self,
        accumulators: Vec<Box<dyn SingleSubpopulationAggregate>>,
    ) -> Result<Box<dyn SingleSubpopulationAggregate>, Box<dyn std::error::Error + Send + Sync>>
    {
        let mut concrete_accumulators = Vec::new();

        for acc in accumulators {
            if let Some(concrete) = acc.as_any().downcast_ref::<IncreaseAccumulator>() {
                concrete_accumulators.push(concrete.clone());
            } else {
                return Err("Type mismatch in merge operation".into());
            }
        }

        if concrete_accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let merged =
            <IncreaseAccumulator as MergeableAccumulator<IncreaseAccumulator>>::merge_accumulators(
                concrete_accumulators,
            )
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { format!("{e}").into() })?;
        Ok(Box::new(merged))
    }

    fn create_default(&self) -> Box<dyn SingleSubpopulationAggregate> {
        Box::new(IncreaseAccumulator::new(
            Measurement::new(0.0),
            0,
            Measurement::new(0.0),
            0,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increase_accumulator_creation() {
        let starting_measurement = Measurement::new(10.0);
        let last_seen_measurement = Measurement::new(25.0);
        let acc = IncreaseAccumulator::new(
            starting_measurement.clone(),
            1000,
            last_seen_measurement.clone(),
            2000,
        );

        assert_eq!(acc.starting_measurement.value, 10.0);
        assert_eq!(acc.starting_timestamp, 1000);
        assert_eq!(acc.last_seen_measurement.value, 25.0);
        assert_eq!(acc.last_seen_timestamp, 2000);
    }

    #[test]
    fn test_increase_accumulator_update() {
        let starting_measurement = Measurement::new(10.0);
        let mut acc = IncreaseAccumulator::new(
            starting_measurement.clone(),
            1000,
            starting_measurement.clone(),
            1000,
        );

        let new_measurement = Measurement::new(25.0);
        acc.update(new_measurement.clone(), 2000);

        assert_eq!(acc.last_seen_measurement.value, 25.0);
        assert_eq!(acc.last_seen_timestamp, 2000);
        assert_eq!(acc.starting_measurement.value, 10.0); // Should remain unchanged
    }

    #[test]
    fn test_increase_accumulator_query() {
        let starting_measurement = Measurement::new(10.0);
        let last_seen_measurement = Measurement::new(25.0);
        let acc = IncreaseAccumulator::new(
            starting_measurement,
            1000,
            last_seen_measurement,
            3000, // 2 second difference
        );

        // Test increase calculation
        assert_eq!(
            crate::SingleSubpopulationAggregate::query(&acc, Statistic::Increase, None).unwrap(),
            15.0
        );

        // Test rate calculation (per second)
        assert_eq!(
            crate::SingleSubpopulationAggregate::query(&acc, Statistic::Rate, None).unwrap(),
            7.5
        ); // 15.0 / 2.0

        assert!(crate::SingleSubpopulationAggregate::query(&acc, Statistic::Sum, None).is_err());
    }

    #[test]
    fn test_increase_accumulator_merge() {
        let acc1 =
            IncreaseAccumulator::new(Measurement::new(10.0), 1000, Measurement::new(20.0), 2000);
        let acc2 = IncreaseAccumulator::new(
            Measurement::new(5.0),
            500, // Earlier start
            Measurement::new(15.0),
            1500,
        );
        let acc3 = IncreaseAccumulator::new(
            Measurement::new(20.0),
            2000,
            Measurement::new(30.0),
            3000, // Later end
        );

        let merged =
            <IncreaseAccumulator as MergeableAccumulator<IncreaseAccumulator>>::merge_accumulators(
                vec![acc1, acc2, acc3],
            )
            .unwrap();

        // Should use earliest start and latest end
        assert_eq!(merged.starting_measurement.value, 5.0);
        assert_eq!(merged.starting_timestamp, 500);
        assert_eq!(merged.last_seen_measurement.value, 30.0);
        assert_eq!(merged.last_seen_timestamp, 3000);
    }

    #[test]
    fn test_increase_accumulator_serialization() {
        let acc =
            IncreaseAccumulator::new(Measurement::new(10.0), 1000, Measurement::new(25.0), 2000);

        // Test JSON serialization
        let json = acc.serialize_to_json();
        let deserialized = IncreaseAccumulator::deserialize_from_json(&json).unwrap();
        assert_eq!(
            acc.starting_measurement.value,
            deserialized.starting_measurement.value
        );
        assert_eq!(acc.starting_timestamp, deserialized.starting_timestamp);
        assert_eq!(
            acc.last_seen_measurement.value,
            deserialized.last_seen_measurement.value
        );
        assert_eq!(acc.last_seen_timestamp, deserialized.last_seen_timestamp);

        // Test byte serialization
        let bytes = acc.serialize_to_bytes();
        let deserialized_bytes = IncreaseAccumulator::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(
            acc.starting_measurement.value,
            deserialized_bytes.starting_measurement.value
        );
        assert_eq!(
            acc.starting_timestamp,
            deserialized_bytes.starting_timestamp
        );
        assert_eq!(
            acc.last_seen_measurement.value,
            deserialized_bytes.last_seen_measurement.value
        );
        assert_eq!(
            acc.last_seen_timestamp,
            deserialized_bytes.last_seen_timestamp
        );
    }

    #[test]
    fn test_trait_object() {
        let acc: Box<dyn AggregateCore> = Box::new(IncreaseAccumulator::new(
            Measurement::new(10.0),
            1000,
            Measurement::new(25.0),
            2000,
        ));

        assert_eq!(acc.type_name(), "IncreaseAccumulator");
    }
}
