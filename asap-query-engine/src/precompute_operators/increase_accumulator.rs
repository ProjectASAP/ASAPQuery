use crate::data_model::{
    AggregateCore, AggregationType, Measurement, MergeableAccumulator, SerializableToSink,
    SingleSubpopulationAggregate, SingleSubpopulationAggregateFactory,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

pub(crate) const INCREASE_BINARY_FORMAT_MAGIC_V2: [u8; 4] = *b"INC2";
pub(crate) const INCREASE_BINARY_FORMAT_MAGIC_V3: [u8; 4] = *b"INC3";
pub(crate) const INCREASE_BINARY_FORMAT_MAGIC_V4: [u8; 4] = *b"INC4";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CounterResetEvent {
    pub timestamp: i64,
    pub adjustment: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OpaqueResetRange {
    pub starting_timestamp: i64,
    pub last_seen_timestamp: i64,
}

/// Accumulator for tracking increases in counter metrics
/// Stores the starting and last seen measurements with timestamps, plus
/// correction for counter resets observed between them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncreaseAccumulator {
    pub starting_measurement: Measurement,
    pub starting_timestamp: i64,
    pub last_seen_measurement: Measurement,
    pub last_seen_timestamp: i64,
    #[serde(default)]
    pub counter_reset_adjustment: f64,
    #[serde(default)]
    pub counter_reset_events: Vec<CounterResetEvent>,
    #[serde(default)]
    pub opaque_reset_ranges: Vec<OpaqueResetRange>,
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
            counter_reset_adjustment: 0.0,
            counter_reset_events: Vec::new(),
            opaque_reset_ranges: Vec::new(),
        }
    }

    pub fn update(&mut self, measurement: Measurement, timestamp: i64) {
        if measurement.value < self.last_seen_measurement.value {
            let adjustment = self.last_seen_measurement.value;
            self.counter_reset_adjustment += adjustment;
            self.counter_reset_events.push(CounterResetEvent {
                timestamp,
                adjustment,
            });
        }
        self.last_seen_measurement = measurement;
        self.last_seen_timestamp = timestamp;
    }

    fn corrected_increase(&self) -> f64 {
        self.last_seen_measurement.value - self.starting_measurement.value
            + self.counter_reset_adjustment
    }

    fn add_reset_event(&mut self, event: CounterResetEvent) {
        if self
            .counter_reset_events
            .iter()
            .any(|existing| existing.timestamp == event.timestamp)
        {
            return;
        }
        self.counter_reset_adjustment += event.adjustment;
        self.counter_reset_events.push(event);
    }

    fn has_opaque_reset_adjustment(&self) -> bool {
        let event_adjustment: f64 = self
            .counter_reset_events
            .iter()
            .map(|event| event.adjustment)
            .sum();
        !self.opaque_reset_ranges.is_empty() || self.counter_reset_adjustment != event_adjustment
    }

    fn opaque_reset_ranges(&self) -> Vec<OpaqueResetRange> {
        if !self.opaque_reset_ranges.is_empty() {
            return self.opaque_reset_ranges.clone();
        }

        if self.has_opaque_reset_adjustment() {
            vec![OpaqueResetRange {
                starting_timestamp: self.starting_timestamp,
                last_seen_timestamp: self.last_seen_timestamp,
            }]
        } else {
            Vec::new()
        }
    }

    fn add_opaque_reset_range(&mut self, range: OpaqueResetRange) {
        if !self.opaque_reset_ranges.iter().any(|existing| {
            existing.starting_timestamp == range.starting_timestamp
                && existing.last_seen_timestamp == range.last_seen_timestamp
        }) {
            self.opaque_reset_ranges.push(range);
        }
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
        let counter_reset_adjustment = match data.get("counter_reset_adjustment") {
            None | Some(Value::Null) => 0.0,
            Some(value) => value
                .as_f64()
                .ok_or("Missing or invalid 'counter_reset_adjustment' field")?,
        };
        let counter_reset_events = match data.get("counter_reset_events") {
            None | Some(Value::Null) => Vec::new(),
            Some(value) => serde_json::from_value(value.clone())
                .map_err(|e| format!("Invalid 'counter_reset_events' field: {e}"))?,
        };
        let opaque_reset_ranges = match data.get("opaque_reset_ranges") {
            None | Some(Value::Null) => Vec::new(),
            Some(value) => serde_json::from_value(value.clone())
                .map_err(|e| format!("Invalid 'opaque_reset_ranges' field: {e}"))?,
        };

        let mut accumulator = Self::new(
            starting_measurement,
            starting_timestamp,
            last_seen_measurement,
            last_seen_timestamp,
        );
        accumulator.counter_reset_adjustment = counter_reset_adjustment;
        accumulator.counter_reset_events = counter_reset_events;
        accumulator.opaque_reset_ranges = opaque_reset_ranges;
        if accumulator.opaque_reset_ranges.is_empty() && accumulator.has_opaque_reset_adjustment() {
            accumulator.add_opaque_reset_range(OpaqueResetRange {
                starting_timestamp,
                last_seen_timestamp,
            });
        }
        Ok(accumulator)
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let has_v2_format = buffer.starts_with(&INCREASE_BINARY_FORMAT_MAGIC_V2);
        let has_v3_format = buffer.starts_with(&INCREASE_BINARY_FORMAT_MAGIC_V3);
        let has_v4_format = buffer.starts_with(&INCREASE_BINARY_FORMAT_MAGIC_V4);
        let has_reset_adjustment = has_v2_format || has_v3_format || has_v4_format;
        let mut offset = if has_reset_adjustment {
            INCREASE_BINARY_FORMAT_MAGIC_V2.len()
        } else {
            0
        };

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
        offset += 8;

        let counter_reset_adjustment = if has_reset_adjustment {
            if buffer.len() < offset + 8 {
                return Err("Buffer too short for counter reset adjustment".into());
            }
            f64::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
                buffer[offset + 4],
                buffer[offset + 5],
                buffer[offset + 6],
                buffer[offset + 7],
            ])
        } else {
            0.0
        };

        if has_reset_adjustment {
            offset += 8;
        }

        let counter_reset_events = if has_v3_format || has_v4_format {
            if buffer.len() < offset + 4 {
                return Err("Buffer too short for counter reset event count".into());
            }
            let event_count = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            offset += 4;
            let event_bytes = event_count
                .checked_mul(16)
                .ok_or("Counter reset event data length overflow")?;
            if buffer.len() < offset + event_bytes {
                return Err("Buffer too short for counter reset events".into());
            }
            (0..event_count)
                .map(|_| {
                    let timestamp = i64::from_le_bytes([
                        buffer[offset],
                        buffer[offset + 1],
                        buffer[offset + 2],
                        buffer[offset + 3],
                        buffer[offset + 4],
                        buffer[offset + 5],
                        buffer[offset + 6],
                        buffer[offset + 7],
                    ]);
                    let adjustment = f64::from_le_bytes([
                        buffer[offset + 8],
                        buffer[offset + 9],
                        buffer[offset + 10],
                        buffer[offset + 11],
                        buffer[offset + 12],
                        buffer[offset + 13],
                        buffer[offset + 14],
                        buffer[offset + 15],
                    ]);
                    offset += 16;
                    CounterResetEvent {
                        timestamp,
                        adjustment,
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        let opaque_reset_ranges = if has_v4_format {
            if buffer.len() < offset + 4 {
                return Err("Buffer too short for opaque reset range count".into());
            }
            let range_count = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            offset += 4;
            let range_bytes = range_count
                .checked_mul(16)
                .ok_or("Opaque reset range data length overflow")?;
            if buffer.len() < offset + range_bytes {
                return Err("Buffer too short for opaque reset ranges".into());
            }
            (0..range_count)
                .map(|_| {
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
                    let last_seen_timestamp = i64::from_le_bytes([
                        buffer[offset + 8],
                        buffer[offset + 9],
                        buffer[offset + 10],
                        buffer[offset + 11],
                        buffer[offset + 12],
                        buffer[offset + 13],
                        buffer[offset + 14],
                        buffer[offset + 15],
                    ]);
                    offset += 16;
                    OpaqueResetRange {
                        starting_timestamp,
                        last_seen_timestamp,
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        let mut accumulator = Self::new(
            starting_measurement,
            starting_timestamp,
            last_seen_measurement,
            last_seen_timestamp,
        );
        accumulator.counter_reset_adjustment = counter_reset_adjustment;
        accumulator.counter_reset_events = counter_reset_events;
        accumulator.opaque_reset_ranges = opaque_reset_ranges;
        if accumulator.opaque_reset_ranges.is_empty() && accumulator.has_opaque_reset_adjustment() {
            accumulator.add_opaque_reset_range(OpaqueResetRange {
                starting_timestamp,
                last_seen_timestamp,
            });
        }
        Ok(accumulator)
    }
}

impl SerializableToSink for IncreaseAccumulator {
    fn serialize_to_json(&self) -> Value {
        serde_json::json!({
            "starting_measurement": self.starting_measurement.serialize_to_json(),
            "starting_timestamp": self.starting_timestamp,
            "last_seen_measurement": self.last_seen_measurement.serialize_to_json(),
            "last_seen_timestamp": self.last_seen_timestamp,
            "counter_reset_adjustment": self.counter_reset_adjustment,
            "counter_reset_events": self.counter_reset_events,
            "opaque_reset_ranges": self.opaque_reset_ranges,
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        let starting_measurement_bytes = self.starting_measurement.serialize_to_bytes();
        let last_seen_measurement_bytes = self.last_seen_measurement.serialize_to_bytes();

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&INCREASE_BINARY_FORMAT_MAGIC_V4);

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
        buffer.extend_from_slice(&self.counter_reset_adjustment.to_le_bytes());
        buffer.extend_from_slice(&(self.counter_reset_events.len() as u32).to_le_bytes());
        for event in &self.counter_reset_events {
            buffer.extend_from_slice(&event.timestamp.to_le_bytes());
            buffer.extend_from_slice(&event.adjustment.to_le_bytes());
        }
        buffer.extend_from_slice(&(self.opaque_reset_ranges.len() as u32).to_le_bytes());
        for range in &self.opaque_reset_ranges {
            buffer.extend_from_slice(&range.starting_timestamp.to_le_bytes());
            buffer.extend_from_slice(&range.last_seen_timestamp.to_le_bytes());
        }

        buffer
    }
}

fn ranges_overlap(first_start: i64, first_end: i64, second_start: i64, second_end: i64) -> bool {
    first_start < second_end && second_start < first_end
}

impl MergeableAccumulator<IncreaseAccumulator> for IncreaseAccumulator {
    fn merge_accumulators(
        accumulators: Vec<IncreaseAccumulator>,
    ) -> Result<IncreaseAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        let mut accumulators = accumulators;
        accumulators.sort_by_key(|acc| acc.starting_timestamp);
        let original_ranges: Vec<_> = accumulators
            .iter()
            .enumerate()
            .map(|(index, acc)| {
                (
                    index,
                    acc.starting_timestamp,
                    acc.last_seen_timestamp,
                    acc.opaque_reset_ranges(),
                )
            })
            .collect();
        let mut result = accumulators.remove(0);
        for range in result.opaque_reset_ranges() {
            result.add_opaque_reset_range(range);
        }

        for (index, acc) in accumulators.into_iter().enumerate() {
            let original_index = index + 1;
            let acc_opaque_ranges = acc.opaque_reset_ranges();
            let overlaps_ambiguous_range = original_ranges.iter().any(
                |(other_index, other_start, other_end, other_opaque_ranges)| {
                    if *other_index == original_index {
                        return false;
                    }

                    acc_opaque_ranges.iter().any(|range| {
                        ranges_overlap(
                            range.starting_timestamp,
                            range.last_seen_timestamp,
                            *other_start,
                            *other_end,
                        )
                    }) || other_opaque_ranges.iter().any(|range| {
                        ranges_overlap(
                            range.starting_timestamp,
                            range.last_seen_timestamp,
                            acc.starting_timestamp,
                            acc.last_seen_timestamp,
                        )
                    })
                },
            );
            if overlaps_ambiguous_range {
                return Err(
                    "Cannot merge overlapping accumulators with opaque counter reset adjustments"
                        .into(),
                );
            }

            for range in acc_opaque_ranges {
                result.add_opaque_reset_range(range);
            }

            // Adjacent accumulators represent consecutive portions of the same
            // counter. A decrease at their boundary is also a reset.
            if acc.starting_timestamp >= result.last_seen_timestamp
                && acc.starting_measurement.value < result.last_seen_measurement.value
            {
                result.add_reset_event(CounterResetEvent {
                    timestamp: acc.starting_timestamp,
                    adjustment: result.last_seen_measurement.value,
                });
            }

            let event_adjustment: f64 = acc
                .counter_reset_events
                .iter()
                .map(|event| event.adjustment)
                .sum();
            for event in acc.counter_reset_events {
                result.add_reset_event(event);
            }
            result.counter_reset_adjustment += acc.counter_reset_adjustment - event_adjustment;

            // Use the later last seen point.
            if acc.last_seen_timestamp > result.last_seen_timestamp {
                result.last_seen_measurement = acc.last_seen_measurement;
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
            Statistic::Increase => Ok(self.corrected_increase()),
            Statistic::Rate => {
                // Convert to per second; timestamps are in milliseconds
                let time_diff = (self.last_seen_timestamp - self.starting_timestamp) as f64;
                if time_diff <= 0.0 {
                    return Err("Invalid time difference for rate calculation".into());
                }
                Ok(self.corrected_increase() / time_diff * 1000.0)
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
    fn counter_reset_increase_is_corrected() {
        // Counter resets must contribute the post-reset value instead of making
        // the total increase negative.
        let mut acc =
            IncreaseAccumulator::new(Measurement::new(100.0), 0, Measurement::new(100.0), 0);
        acc.update(Measurement::new(150.0), 1_000);
        acc.update(Measurement::new(10.0), 2_000);
        acc.update(Measurement::new(60.0), 3_000);

        assert_eq!(
            crate::SingleSubpopulationAggregate::query(&acc, Statistic::Increase, None).unwrap(),
            110.0
        );
    }

    #[test]
    fn counter_reset_rate_is_corrected() {
        let mut acc =
            IncreaseAccumulator::new(Measurement::new(100.0), 0, Measurement::new(100.0), 0);
        acc.update(Measurement::new(150.0), 1_000);
        acc.update(Measurement::new(10.0), 2_000);
        acc.update(Measurement::new(60.0), 3_000);

        let rate = crate::SingleSubpopulationAggregate::query(&acc, Statistic::Rate, None).unwrap();
        assert!((rate - (110.0 / 3.0)).abs() < f64::EPSILON);
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
    fn test_increase_accumulator_merge_corrects_reset_at_boundary() {
        let first =
            IncreaseAccumulator::new(Measurement::new(100.0), 0, Measurement::new(150.0), 1_000);
        let second =
            IncreaseAccumulator::new(Measurement::new(10.0), 2_000, Measurement::new(60.0), 3_000);

        let merged =
            <IncreaseAccumulator as MergeableAccumulator<IncreaseAccumulator>>::merge_accumulators(
                vec![first, second],
            )
            .unwrap();

        assert_eq!(merged.query(Statistic::Increase, None).unwrap(), 110.0);
    }

    #[test]
    fn test_increase_accumulator_merge_keeps_reset_from_overlapping_prefix() {
        let mut first =
            IncreaseAccumulator::new(Measurement::new(100.0), 0, Measurement::new(100.0), 0);
        first.update(Measurement::new(150.0), 1_000);
        first.update(Measurement::new(10.0), 1_100);
        first.update(Measurement::new(20.0), 1_200);

        let mut second = IncreaseAccumulator::new(
            Measurement::new(150.0),
            1_000,
            Measurement::new(150.0),
            1_000,
        );
        second.update(Measurement::new(10.0), 1_100);
        second.update(Measurement::new(30.0), 2_000);
        let merged =
            <IncreaseAccumulator as MergeableAccumulator<IncreaseAccumulator>>::merge_accumulators(
                vec![first, second],
            )
            .unwrap();

        assert_eq!(merged.query(Statistic::Increase, None).unwrap(), 80.0);
    }

    #[test]
    fn test_increase_accumulator_rejects_opaque_legacy_reset_overlap() {
        let mut legacy =
            IncreaseAccumulator::new(Measurement::new(100.0), 0, Measurement::new(20.0), 1_200);
        legacy.counter_reset_adjustment = 150.0;

        let mut event_aware = IncreaseAccumulator::new(
            Measurement::new(150.0),
            1_000,
            Measurement::new(150.0),
            1_000,
        );
        event_aware.update(Measurement::new(10.0), 1_100);
        event_aware.update(Measurement::new(30.0), 2_000);

        let result =
            <IncreaseAccumulator as MergeableAccumulator<IncreaseAccumulator>>::merge_accumulators(
                vec![legacy, event_aware],
            );
        assert!(result.is_err());
    }

    #[test]
    fn test_increase_accumulator_allows_event_overlap_outside_opaque_range() {
        let mut legacy =
            IncreaseAccumulator::new(Measurement::new(100.0), 0, Measurement::new(150.0), 1_000);
        legacy.counter_reset_adjustment = 100.0;

        let mut event_aware = IncreaseAccumulator::new(
            Measurement::new(150.0),
            1_000,
            Measurement::new(150.0),
            1_000,
        );
        event_aware.update(Measurement::new(10.0), 1_500);
        event_aware.update(Measurement::new(30.0), 2_000);

        let later_overlap =
            IncreaseAccumulator::new(Measurement::new(20.0), 1_500, Measurement::new(50.0), 2_500);

        let result =
            <IncreaseAccumulator as MergeableAccumulator<IncreaseAccumulator>>::merge_accumulators(
                vec![legacy, event_aware, later_overlap],
            );
        assert!(result.is_ok());
    }

    #[test]
    fn test_increase_accumulator_rejects_event_then_opaque_overlap() {
        let mut event_aware =
            IncreaseAccumulator::new(Measurement::new(100.0), 0, Measurement::new(100.0), 0);
        event_aware.update(Measurement::new(10.0), 500);
        event_aware.update(Measurement::new(30.0), 1_000);

        let mut legacy =
            IncreaseAccumulator::new(Measurement::new(10.0), 500, Measurement::new(50.0), 1_500);
        legacy.counter_reset_adjustment = 100.0;

        let result =
            <IncreaseAccumulator as MergeableAccumulator<IncreaseAccumulator>>::merge_accumulators(
                vec![event_aware, legacy],
            );
        assert!(result.is_err());
    }

    #[test]
    fn test_increase_accumulator_preserves_opaque_range_across_sequential_merges() {
        let mut legacy =
            IncreaseAccumulator::new(Measurement::new(100.0), 0, Measurement::new(150.0), 1_000);
        legacy.counter_reset_adjustment = 100.0;

        let mut event_aware = IncreaseAccumulator::new(
            Measurement::new(150.0),
            1_000,
            Measurement::new(150.0),
            1_000,
        );
        event_aware.update(Measurement::new(10.0), 1_500);
        event_aware.update(Measurement::new(30.0), 2_000);

        let later_overlap =
            IncreaseAccumulator::new(Measurement::new(20.0), 1_500, Measurement::new(50.0), 2_500);

        let first_merge = legacy.merge_with(&event_aware).unwrap();
        let first_merge = first_merge
            .as_any()
            .downcast_ref::<IncreaseAccumulator>()
            .unwrap();
        let result = first_merge.merge_with(&later_overlap);

        assert!(result.is_ok());
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
    fn test_counter_reset_correction_survives_serialization() {
        let mut acc =
            IncreaseAccumulator::new(Measurement::new(100.0), 0, Measurement::new(100.0), 0);
        acc.update(Measurement::new(150.0), 1_000);
        acc.update(Measurement::new(10.0), 2_000);
        acc.update(Measurement::new(60.0), 3_000);

        let json_round_trip =
            IncreaseAccumulator::deserialize_from_json(&acc.serialize_to_json()).unwrap();
        assert_eq!(
            json_round_trip.query(Statistic::Increase, None).unwrap(),
            110.0
        );

        let bytes_round_trip =
            IncreaseAccumulator::deserialize_from_bytes(&acc.serialize_to_bytes()).unwrap();
        assert_eq!(
            bytes_round_trip.query(Statistic::Increase, None).unwrap(),
            110.0
        );
    }

    #[test]
    fn test_opaque_reset_range_survives_serialization() {
        let mut legacy =
            IncreaseAccumulator::new(Measurement::new(100.0), 0, Measurement::new(150.0), 1_000);
        legacy.counter_reset_adjustment = 100.0;
        let event_aware = IncreaseAccumulator::new(
            Measurement::new(150.0),
            1_000,
            Measurement::new(200.0),
            2_000,
        );
        let merged = legacy.merge_with(&event_aware).unwrap();
        let merged = merged
            .as_any()
            .downcast_ref::<IncreaseAccumulator>()
            .unwrap();

        let later_overlap = IncreaseAccumulator::new(
            Measurement::new(200.0),
            1_500,
            Measurement::new(250.0),
            2_500,
        );
        let json_round_trip =
            IncreaseAccumulator::deserialize_from_json(&merged.serialize_to_json()).unwrap();
        assert!(json_round_trip.merge_with(&later_overlap).is_ok());

        let bytes_round_trip =
            IncreaseAccumulator::deserialize_from_bytes(&merged.serialize_to_bytes()).unwrap();
        assert!(bytes_round_trip.merge_with(&later_overlap).is_ok());
    }

    #[test]
    fn test_increase_accumulator_keeps_opaque_residual_when_merged_as_later_range() {
        let mut legacy =
            IncreaseAccumulator::new(Measurement::new(100.0), 0, Measurement::new(150.0), 1_000);
        legacy.counter_reset_adjustment = 100.0;

        let mut event_aware = IncreaseAccumulator::new(
            Measurement::new(150.0),
            1_000,
            Measurement::new(150.0),
            1_000,
        );
        event_aware.update(Measurement::new(10.0), 1_500);
        event_aware.update(Measurement::new(30.0), 2_000);

        let later_result =
            <IncreaseAccumulator as MergeableAccumulator<IncreaseAccumulator>>::merge_accumulators(
                vec![legacy, event_aware],
            )
            .unwrap();
        let earlier =
            IncreaseAccumulator::new(Measurement::new(50.0), -1_000, Measurement::new(100.0), 0);

        let merged =
            <IncreaseAccumulator as MergeableAccumulator<IncreaseAccumulator>>::merge_accumulators(
                vec![later_result, earlier],
            )
            .unwrap();

        assert_eq!(merged.query(Statistic::Increase, None).unwrap(), 230.0);
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
