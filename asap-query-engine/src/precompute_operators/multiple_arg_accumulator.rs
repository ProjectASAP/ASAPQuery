use super::error::AccumulatorError;
use crate::data_model::{
    AggregateCore, AggregationType, KeyByLabelValues, MergeableAccumulator,
    MultipleSubpopulationAggregate, SerializableToSink,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

/// Backs `argMax(x, timestamp)` / `argMin(x, timestamp)`: for each key,
/// remembers the string value of `x` from whichever row had the largest
/// (argmax) or smallest (argmin) timestamp seen so far. Structurally the
/// keyed analogue of `MinMaxAccumulator` - same "extremal timestamp per
/// key" tracking `MultipleMinMaxAccumulator` already does - plus one string
/// riding along for the row that set each key's extremum.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipleArgAccumulator {
    /// key -> (best timestamp seen, the arg value from that row).
    pub values: HashMap<KeyByLabelValues, (f64, String)>,
    pub sub_type: String, // "argmax" or "argmin"
}

impl MultipleArgAccumulator {
    pub fn new(sub_type: String) -> Result<Self, AccumulatorError> {
        if sub_type != "argmax" && sub_type != "argmin" {
            return Err(AccumulatorError::InvalidSubType(sub_type));
        }
        Ok(Self {
            values: HashMap::new(),
            sub_type,
        })
    }

    pub fn new_argmax() -> Self {
        Self {
            values: HashMap::new(),
            sub_type: "argmax".to_string(),
        }
    }

    pub fn new_argmin() -> Self {
        Self {
            values: HashMap::new(),
            sub_type: "argmin".to_string(),
        }
    }

    /// `key`'s new (comparison_key, arg_value) observation. Replaces the
    /// stored pair only when `comparison_key` is a new extremum for this
    /// key - a strict comparison, so the FIRST row observed at a tied
    /// timestamp wins (matches `MinMaxAccumulator`'s own tie-breaking,
    /// which never overwrites on `==`).
    pub fn update(&mut self, key: KeyByLabelValues, comparison_key: f64, arg_value: String) {
        let is_better = match self.sub_type.as_str() {
            "argmax" => |new: f64, cur: f64| new > cur,
            "argmin" => |new: f64, cur: f64| new < cur,
            _ => unreachable!("MultipleArgAccumulator sub_type is always 'argmax' or 'argmin'"),
        };
        match self.values.get(&key) {
            Some((cur, _)) if !is_better(comparison_key, *cur) => {}
            _ => {
                self.values.insert(key, (comparison_key, arg_value));
            }
        }
    }

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let sub_type = data["sub_type"]
            .as_str()
            .ok_or("Missing or invalid 'sub_type' field")?
            .to_string();
        if sub_type != "argmax" && sub_type != "argmin" {
            return Err("sub_type must be 'argmax' or 'argmin'".into());
        }

        let values_data = data["values"]
            .as_object()
            .ok_or("Missing or invalid 'values' field")?;

        let mut values = HashMap::new();
        for (key_str, entry) in values_data {
            let key_json: Value = serde_json::from_str(key_str)?;
            let key = KeyByLabelValues::deserialize_from_json(&key_json)?;
            let comparison_key = entry["comparison_key"]
                .as_f64()
                .ok_or("Missing or invalid 'comparison_key' field")?;
            let arg_value = entry["arg_value"]
                .as_str()
                .ok_or("Missing or invalid 'arg_value' field")?
                .to_string();
            values.insert(key, (comparison_key, arg_value));
        }

        Ok(Self { values, sub_type })
    }

    pub fn deserialize_from_bytes(
        buffer: &[u8],
        sub_type: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if sub_type != "argmax" && sub_type != "argmin" {
            return Err("sub_type must be 'argmax' or 'argmin'".into());
        }

        let mut offset = 0;
        if buffer.len() < 4 {
            return Err("Buffer too short for entry count".into());
        }
        let num_entries = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]) as usize;
        offset += 4;

        let mut values = HashMap::new();

        for _ in 0..num_entries {
            if buffer.len() < offset + 4 {
                return Err("Buffer too short for key length".into());
            }
            let key_length = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            offset += 4;

            if buffer.len() < offset + key_length {
                return Err("Buffer too short for key data".into());
            }
            let key =
                KeyByLabelValues::deserialize_from_bytes(&buffer[offset..offset + key_length])?;
            offset += key_length;

            if buffer.len() < offset + 8 {
                return Err("Buffer too short for comparison_key".into());
            }
            let comparison_key = f64::from_le_bytes([
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

            if buffer.len() < offset + 4 {
                return Err("Buffer too short for arg_value length".into());
            }
            let arg_len = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            offset += 4;

            if buffer.len() < offset + arg_len {
                return Err("Buffer too short for arg_value data".into());
            }
            let arg_value = String::from_utf8(buffer[offset..offset + arg_len].to_vec())?;
            offset += arg_len;

            values.insert(key, (comparison_key, arg_value));
        }

        Ok(Self { values, sub_type })
    }
}

impl SerializableToSink for MultipleArgAccumulator {
    fn serialize_to_json(&self) -> Value {
        let mut values_obj = serde_json::Map::new();
        for (key, (comparison_key, arg_value)) in &self.values {
            let key_json = key.serialize_to_json();
            let key_str = serde_json::to_string(&key_json).unwrap();
            values_obj.insert(
                key_str,
                serde_json::json!({
                    "comparison_key": comparison_key,
                    "arg_value": arg_value,
                }),
            );
        }

        serde_json::json!({
            "values": values_obj,
            "sub_type": self.sub_type
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&(self.values.len() as u32).to_le_bytes());

        for (key, (comparison_key, arg_value)) in &self.values {
            let key_bytes = key.serialize_to_bytes();
            buffer.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(&key_bytes);
            buffer.extend_from_slice(&comparison_key.to_le_bytes());
            let arg_bytes = arg_value.as_bytes();
            buffer.extend_from_slice(&(arg_bytes.len() as u32).to_le_bytes());
            buffer.extend_from_slice(arg_bytes);
        }

        buffer
    }
}

impl AggregateCore for MultipleArgAccumulator {
    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn type_name(&self) -> &'static str {
        "MultipleArgAccumulator"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge MultipleArgAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        let other_arg = other
            .as_any()
            .downcast_ref::<MultipleArgAccumulator>()
            .ok_or("Failed to downcast to MultipleArgAccumulator")?;

        let merged = Self::merge_accumulators(vec![self.clone(), other_arg.clone()])?;
        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::MultipleArg
    }

    fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
        Some(self.values.keys().cloned().collect())
    }

    fn query_statistic(
        &self,
        statistic: Statistic,
        key: &Option<KeyByLabelValues>,
        query_kwargs: &std::collections::HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        use crate::data_model::MultipleSubpopulationAggregate;
        let key_val = key
            .as_ref()
            .ok_or("Key required for MultipleArgAccumulator")?;
        self.query(statistic, key_val, Some(query_kwargs))
    }

    fn query_statistic_string(
        &self,
        statistic: Statistic,
        key: &Option<KeyByLabelValues>,
        _query_kwargs: &std::collections::HashMap<String, String>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let key_val = key
            .as_ref()
            .ok_or("Key required for MultipleArgAccumulator")?;
        let expected = match statistic {
            Statistic::ArgMax => "argmax",
            Statistic::ArgMin => "argmin",
            _ => {
                return Err(
                    format!("Unsupported statistic in MultipleArgAccumulator: {statistic:?}")
                        .into(),
                )
            }
        };
        if self.sub_type != expected {
            return Err(format!(
                "Cannot query {expected} statistic from {} accumulator",
                self.sub_type
            )
            .into());
        }
        self.values
            .get(key_val)
            .map(|(_, arg_value)| arg_value.clone())
            .ok_or_else(|| format!("Key {key_val} not found in MultipleArgAccumulator").into())
    }
}

impl MultipleSubpopulationAggregate for MultipleArgAccumulator {
    fn query(
        &self,
        _statistic: Statistic,
        _key: &KeyByLabelValues,
        _query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        // MultipleArgAccumulator's real answer is a string - see
        // query_statistic_string on the AggregateCore impl above. The
        // numeric query() this trait requires has nothing meaningful to
        // return.
        Err("MultipleArgAccumulator does not support numeric queries; use query_statistic_string"
            .into())
    }

    fn clone_boxed(&self) -> Box<dyn MultipleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

impl MergeableAccumulator<MultipleArgAccumulator> for MultipleArgAccumulator {
    fn merge_accumulators(
        accumulators: Vec<MultipleArgAccumulator>,
    ) -> Result<MultipleArgAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err(AccumulatorError::EmptySlice.into());
        }

        let sub_type = accumulators[0].sub_type.clone();
        for acc in &accumulators {
            if acc.sub_type != sub_type {
                return Err(AccumulatorError::MergeTypeMismatch {
                    expected: sub_type.clone(),
                    got: acc.sub_type.clone(),
                }
                .into());
            }
        }

        let mut result = MultipleArgAccumulator::new(sub_type.clone())?;

        for acc in accumulators {
            for (key, (comparison_key, arg_value)) in acc.values {
                result.update(key, comparison_key, arg_value);
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_argmax_accumulator_update() {
        let mut acc = MultipleArgAccumulator::new_argmax();
        let key = KeyByLabelValues::new_with_labels(vec!["8.8.8.0/24".to_string()]);

        acc.update(key.clone(), 100.0, "A".to_string());
        acc.update(key.clone(), 50.0, "W".to_string()); // older, should not replace
        acc.update(key.clone(), 200.0, "A".to_string()); // newer, should replace

        assert_eq!(
            acc.values.get(&key),
            Some(&(200.0, "A".to_string()))
        );
    }

    #[test]
    fn test_argmin_accumulator_update() {
        let mut acc = MultipleArgAccumulator::new_argmin();
        let key = KeyByLabelValues::new_with_labels(vec!["8.8.8.0/24".to_string()]);

        acc.update(key.clone(), 100.0, "A".to_string());
        acc.update(key.clone(), 200.0, "W".to_string()); // later, should not replace
        acc.update(key.clone(), 50.0, "W".to_string()); // earlier, should replace

        assert_eq!(
            acc.values.get(&key),
            Some(&(50.0, "W".to_string()))
        );
    }

    #[test]
    fn test_tie_keeps_first_observed() {
        let mut acc = MultipleArgAccumulator::new_argmax();
        let key = KeyByLabelValues::new_with_labels(vec!["k".to_string()]);

        acc.update(key.clone(), 100.0, "first".to_string());
        acc.update(key.clone(), 100.0, "second".to_string()); // tie - strict > means no replace

        assert_eq!(acc.values.get(&key), Some(&(100.0, "first".to_string())));
    }

    #[test]
    fn test_different_keys_independent() {
        let mut acc = MultipleArgAccumulator::new_argmax();
        let key1 = KeyByLabelValues::new_with_labels(vec!["a".to_string()]);
        let key2 = KeyByLabelValues::new_with_labels(vec!["b".to_string()]);

        acc.update(key1.clone(), 10.0, "x".to_string());
        acc.update(key2.clone(), 20.0, "y".to_string());

        assert_eq!(acc.values.get(&key1), Some(&(10.0, "x".to_string())));
        assert_eq!(acc.values.get(&key2), Some(&(20.0, "y".to_string())));
    }

    #[test]
    fn test_query_statistic_string() {
        let mut acc = MultipleArgAccumulator::new_argmax();
        let key = KeyByLabelValues::new_with_labels(vec!["8.8.8.0/24".to_string()]);
        acc.update(key.clone(), 100.0, "3356 174".to_string());

        let result = acc
            .query_statistic_string(Statistic::ArgMax, &Some(key.clone()), &HashMap::new())
            .unwrap();
        assert_eq!(result, "3356 174");

        // Wrong direction is rejected.
        assert!(acc
            .query_statistic_string(Statistic::ArgMin, &Some(key.clone()), &HashMap::new())
            .is_err());

        // Missing key is rejected.
        let missing = KeyByLabelValues::new_with_labels(vec!["missing".to_string()]);
        assert!(acc
            .query_statistic_string(Statistic::ArgMax, &Some(missing), &HashMap::new())
            .is_err());

        // No key at all is rejected.
        assert!(acc
            .query_statistic_string(Statistic::ArgMax, &None, &HashMap::new())
            .is_err());
    }

    #[test]
    fn test_merge_argmax_accumulators() {
        let mut acc1 = MultipleArgAccumulator::new_argmax();
        let mut acc2 = MultipleArgAccumulator::new_argmax();
        let key = KeyByLabelValues::new_with_labels(vec!["k".to_string()]);

        acc1.update(key.clone(), 10.0, "from_acc1".to_string());
        acc2.update(key.clone(), 20.0, "from_acc2".to_string());

        let merged = <MultipleArgAccumulator as MergeableAccumulator<
            MultipleArgAccumulator,
        >>::merge_accumulators(vec![acc1, acc2])
        .unwrap();

        assert_eq!(
            merged.values.get(&key),
            Some(&(20.0, "from_acc2".to_string()))
        );
    }

    #[test]
    fn test_merge_different_sub_types_error() {
        let acc1 = MultipleArgAccumulator::new_argmax();
        let acc2 = MultipleArgAccumulator::new_argmin();

        assert!(<MultipleArgAccumulator as MergeableAccumulator<
            MultipleArgAccumulator,
        >>::merge_accumulators(vec![acc1, acc2])
        .is_err());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut acc = MultipleArgAccumulator::new_argmax();
        let key = KeyByLabelValues::new_with_labels(vec!["8.8.8.0/24".to_string()]);
        acc.update(key.clone(), 42.5, "some as_path here".to_string());

        let json = acc.serialize_to_json();
        let deserialized = MultipleArgAccumulator::deserialize_from_json(&json).unwrap();
        assert_eq!(
            deserialized.values.get(&key),
            Some(&(42.5, "some as_path here".to_string()))
        );
        assert_eq!(deserialized.sub_type, "argmax");

        let bytes = acc.serialize_to_bytes();
        let deserialized_bytes =
            MultipleArgAccumulator::deserialize_from_bytes(&bytes, "argmax".to_string()).unwrap();
        assert_eq!(
            deserialized_bytes.values.get(&key),
            Some(&(42.5, "some as_path here".to_string()))
        );
    }

    #[test]
    fn test_trait_object_type_name() {
        let acc = MultipleArgAccumulator::new_argmax();
        let trait_obj: Box<dyn AggregateCore> = Box::new(acc);
        assert_eq!(trait_obj.type_name(), "MultipleArgAccumulator");
    }

    #[test]
    fn test_new_invalid_sub_type() {
        let err = MultipleArgAccumulator::new("mean".to_string()).unwrap_err();
        assert!(matches!(err, AccumulatorError::InvalidSubType(s) if s == "mean"));
    }
}
