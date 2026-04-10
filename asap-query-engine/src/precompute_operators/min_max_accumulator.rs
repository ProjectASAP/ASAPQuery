use crate::data_model::{
    AggregateCore, AggregationType, MergeableAccumulator, SerializableToSink,
    SingleSubpopulationAggregate, SingleSubpopulationAggregateFactory,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinMaxAccumulator {
    pub value: f64,
    pub sub_type: String, // "min" or "max"
}

impl MinMaxAccumulator {
    pub fn new_min() -> Self {
        Self {
            value: f64::INFINITY,
            sub_type: "min".to_string(),
        }
    }

    pub fn new_max() -> Self {
        Self {
            value: f64::NEG_INFINITY,
            sub_type: "max".to_string(),
        }
    }

    pub fn new(sub_type: String) -> Self {
        match sub_type.as_str() {
            "min" => Self::new_min(),
            "max" => Self::new_max(),
            _ => panic!("sub_type must be 'min' or 'max'"),
        }
    }

    pub fn with_value(value: f64, sub_type: String) -> Self {
        if sub_type != "min" && sub_type != "max" {
            panic!("sub_type must be 'min' or 'max'");
        }
        Self { value, sub_type }
    }

    pub fn update(&mut self, value: f64) {
        match self.sub_type.as_str() {
            "min" => {
                if value < self.value {
                    self.value = value;
                }
            }
            "max" => {
                if value > self.value {
                    self.value = value;
                }
            }
            _ => panic!("Invalid sub_type"),
        }
    }

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let value = data["value"]
            .as_f64()
            .ok_or("Missing or invalid 'value' field")?;
        let sub_type = data["sub_type"]
            .as_str()
            .ok_or("Missing or invalid 'sub_type' field")?
            .to_string();

        if sub_type != "min" && sub_type != "max" {
            return Err("sub_type must be 'min' or 'max'".into());
        }

        Ok(Self::with_value(value, sub_type))
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        if buffer.len() < 9 {
            return Err("Buffer too short".into());
        }

        let value = f64::from_le_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ]);

        let sub_type = match buffer[8] {
            0 => "min".to_string(),
            1 => "max".to_string(),
            _ => return Err("Invalid sub_type byte".into()),
        };

        Ok(Self::with_value(value, sub_type))
    }
}

impl SerializableToSink for MinMaxAccumulator {
    fn serialize_to_json(&self) -> Value {
        serde_json::json!({
            "value": self.value,
            "sub_type": self.sub_type
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.value.to_le_bytes().to_vec();
        let sub_type_byte = match self.sub_type.as_str() {
            "min" => 0u8,
            "max" => 1u8,
            _ => panic!("Invalid sub_type"),
        };
        bytes.push(sub_type_byte);
        bytes
    }
}

impl MergeableAccumulator<MinMaxAccumulator> for MinMaxAccumulator {
    fn merge_accumulators(
        accumulators: Vec<MinMaxAccumulator>,
    ) -> Result<MinMaxAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let sub_type = &accumulators[0].sub_type;

        // Verify all accumulators have the same sub_type
        for acc in &accumulators {
            if acc.sub_type != *sub_type {
                return Err("Cannot merge accumulators with different sub_types".into());
            }
        }

        let mut result = MinMaxAccumulator::new(sub_type.clone());

        for acc in accumulators {
            result.update(acc.value);
        }

        Ok(result)
    }
}

impl AggregateCore for MinMaxAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "MinMaxAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        // Check if other is also a MinMaxAccumulator
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge MinMaxAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        // Downcast to MinMaxAccumulator
        let other_minmax = other
            .as_any()
            .downcast_ref::<MinMaxAccumulator>()
            .ok_or("Failed to downcast to MinMaxAccumulator")?;

        // Use the existing merge_accumulators method
        let merged = Self::merge_accumulators(vec![self.clone(), other_minmax.clone()])?;

        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::MinMax
    }

    fn get_keys(&self) -> Option<Vec<crate::KeyByLabelValues>> {
        None
    }
}

impl SingleSubpopulationAggregate for MinMaxAccumulator {
    fn query(
        &self,
        statistic: Statistic,
        query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        // MinMaxAccumulator doesn't use query_kwargs, assert it's None
        if query_kwargs.is_some() {
            return Err("MinMaxAccumulator does not support query parameters".into());
        }

        match (statistic, self.sub_type.as_str()) {
            (Statistic::Min, "min") => Ok(self.value),
            (Statistic::Max, "max") => Ok(self.value),
            _ => Err(format!(
                "Unsupported statistic in MinMaxAccumulator: {:?} for sub_type: {}",
                statistic, self.sub_type
            )
            .into()),
        }
    }

    fn clone_boxed(&self) -> Box<dyn SingleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

// Factory implementation for merging
pub struct MinMaxAccumulatorFactory {
    pub sub_type: String,
}

impl MinMaxAccumulatorFactory {
    pub fn new_min() -> Self {
        Self {
            sub_type: "min".to_string(),
        }
    }

    pub fn new_max() -> Self {
        Self {
            sub_type: "max".to_string(),
        }
    }
}

impl SingleSubpopulationAggregateFactory for MinMaxAccumulatorFactory {
    fn merge_accumulators(
        &self,
        accumulators: Vec<Box<dyn SingleSubpopulationAggregate>>,
    ) -> Result<Box<dyn SingleSubpopulationAggregate>, Box<dyn std::error::Error + Send + Sync>>
    {
        if accumulators.is_empty() {
            return match self.sub_type.as_str() {
                "min" => Ok(Box::new(MinMaxAccumulator::new_min())),
                "max" => Ok(Box::new(MinMaxAccumulator::new_max())),
                _ => Err(format!("Unsupported sub_type: {}", self.sub_type).into()),
            };
        }

        let mut result_value = match self.sub_type.as_str() {
            "min" => f64::INFINITY,
            "max" => f64::NEG_INFINITY,
            _ => return Err(format!("Unsupported sub_type: {}", self.sub_type).into()),
        };

        for acc in accumulators {
            let value = match self.sub_type.as_str() {
                "min" => acc.query(Statistic::Min, None)?,
                "max" => acc.query(Statistic::Max, None)?,
                _ => return Err(format!("Unsupported sub_type: {}", self.sub_type).into()),
            };

            result_value = match self.sub_type.as_str() {
                "min" => result_value.min(value),
                "max" => result_value.max(value),
                _ => return Err(format!("Unsupported sub_type: {}", self.sub_type).into()),
            };
        }

        Ok(Box::new(MinMaxAccumulator::with_value(
            result_value,
            self.sub_type.clone(),
        )))
    }

    fn create_default(&self) -> Box<dyn SingleSubpopulationAggregate> {
        match self.sub_type.as_str() {
            "min" => Box::new(MinMaxAccumulator::new_min()),
            "max" => Box::new(MinMaxAccumulator::new_max()),
            _ => Box::new(MinMaxAccumulator::new_min()), // Default fallback
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_accumulator() {
        let mut acc = MinMaxAccumulator::new_min();
        acc.update(10.0);
        acc.update(5.0);
        acc.update(15.0);

        assert_eq!(acc.value, 5.0);
        assert_eq!(
            crate::SingleSubpopulationAggregate::query(&acc, Statistic::Min, None).unwrap(),
            5.0
        );
        assert!(crate::SingleSubpopulationAggregate::query(&acc, Statistic::Max, None).is_err());
    }

    #[test]
    fn test_max_accumulator() {
        let mut acc = MinMaxAccumulator::new_max();
        acc.update(10.0);
        acc.update(5.0);
        acc.update(15.0);

        assert_eq!(acc.value, 15.0);
        assert_eq!(
            crate::SingleSubpopulationAggregate::query(&acc, Statistic::Max, None).unwrap(),
            15.0
        );
        assert!(crate::SingleSubpopulationAggregate::query(&acc, Statistic::Min, None).is_err());
    }

    #[test]
    fn test_merge_min_accumulators() {
        let acc1 = MinMaxAccumulator::with_value(10.0, "min".to_string());
        let acc2 = MinMaxAccumulator::with_value(5.0, "min".to_string());
        let acc3 = MinMaxAccumulator::with_value(15.0, "min".to_string());

        let merged =
            <MinMaxAccumulator as MergeableAccumulator<MinMaxAccumulator>>::merge_accumulators(
                vec![acc1, acc2, acc3],
            )
            .unwrap();
        assert_eq!(merged.value, 5.0);
        assert_eq!(merged.sub_type, "min");
    }

    #[test]
    fn test_merge_max_accumulators() {
        let acc1 = MinMaxAccumulator::with_value(10.0, "max".to_string());
        let acc2 = MinMaxAccumulator::with_value(5.0, "max".to_string());
        let acc3 = MinMaxAccumulator::with_value(15.0, "max".to_string());

        let merged =
            <MinMaxAccumulator as MergeableAccumulator<MinMaxAccumulator>>::merge_accumulators(
                vec![acc1, acc2, acc3],
            )
            .unwrap();
        assert_eq!(merged.value, 15.0);
        assert_eq!(merged.sub_type, "max");
    }

    #[test]
    fn test_merge_different_types_error() {
        let acc1 = MinMaxAccumulator::with_value(10.0, "min".to_string());
        let acc2 = MinMaxAccumulator::with_value(5.0, "max".to_string());

        assert!(
            <MinMaxAccumulator as MergeableAccumulator<MinMaxAccumulator>>::merge_accumulators(
                vec![acc1, acc2]
            )
            .is_err()
        );
    }

    #[test]
    fn test_serialization() {
        let acc = MinMaxAccumulator::with_value(42.5, "min".to_string());

        // Test JSON serialization
        let json = acc.serialize_to_json();
        let deserialized = MinMaxAccumulator::deserialize_from_json(&json).unwrap();
        assert_eq!(acc.value, deserialized.value);
        assert_eq!(acc.sub_type, deserialized.sub_type);

        // Test byte serialization
        let bytes = acc.serialize_to_bytes();
        let deserialized_bytes = MinMaxAccumulator::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(acc.value, deserialized_bytes.value);
        assert_eq!(acc.sub_type, deserialized_bytes.sub_type);
    }

    #[test]
    fn test_single_subpopulation_aggregate_trait() {
        let acc: Box<dyn SingleSubpopulationAggregate> =
            Box::new(MinMaxAccumulator::with_value(42.0, "max".to_string()));

        assert_eq!(acc.query(Statistic::Max, None).unwrap(), 42.0);
        assert!(acc.query(Statistic::Min, None).is_err());
        assert_eq!(acc.type_name(), "MinMaxAccumulator");
    }
}
