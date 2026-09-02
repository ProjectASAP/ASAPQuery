use crate::data_model::{
    AggregateCore, AggregationType, Measurement, MergeableAccumulator, QueryBounds,
    SerializableToSink, SingleSubpopulationAggregate, SingleSubpopulationAggregateFactory,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::Statistic;

pub(crate) const INCREASE_BINARY_FORMAT_MAGIC: [u8; 4] = *b"INC7";
pub(crate) const RESET_RECORD_BYTES: usize =
    std::mem::size_of::<i64>() + std::mem::size_of::<f64>();

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CounterResetEvent {
    pub timestamp: i64,
    pub adjustment: f64,
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
    pub sample_count: u64,
    pub counter_reset_adjustment: f64,
    pub counter_reset_events: Vec<CounterResetEvent>,
}

impl IncreaseAccumulator {
    pub fn new(
        starting_measurement: Measurement,
        starting_timestamp: i64,
        last_seen_measurement: Measurement,
        last_seen_timestamp: i64,
    ) -> Self {
        Self::new_with_sample_count(
            starting_measurement,
            starting_timestamp,
            last_seen_measurement,
            last_seen_timestamp,
            1,
        )
    }

    pub fn new_with_sample_count(
        starting_measurement: Measurement,
        starting_timestamp: i64,
        last_seen_measurement: Measurement,
        last_seen_timestamp: i64,
        sample_count: u64,
    ) -> Self {
        assert!(
            sample_count > 0,
            "IncreaseAccumulator sample count must be positive"
        );
        Self {
            starting_measurement,
            starting_timestamp,
            last_seen_measurement,
            last_seen_timestamp,
            sample_count,
            counter_reset_adjustment: 0.0,
            counter_reset_events: Vec::new(),
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
        self.sample_count = self
            .sample_count
            .checked_add(1)
            .expect("IncreaseAccumulator sample count overflow");
    }

    fn increase(&self) -> f64 {
        self.last_seen_measurement.value - self.starting_measurement.value
            + self.counter_reset_adjustment
    }

    pub fn query_with_bounds(
        &self,
        statistic: Statistic,
        bounds: &QueryBounds,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        if bounds.start_timestamp >= bounds.end_timestamp {
            return Err("Query range must have a positive duration".into());
        }
        if self.sample_count < 2 {
            return Err("At least two samples are required".into());
        }

        let sampled_interval = self
            .last_seen_timestamp
            .checked_sub(self.starting_timestamp)
            .ok_or("Observed sample timestamps overflowed")? as f64;
        if sampled_interval <= 0.0 {
            return Err("Observed samples must span a positive duration".into());
        }

        let average_sample_interval = sampled_interval / (self.sample_count - 1) as f64;
        let extrapolation_threshold = average_sample_interval * 1.1;
        let mut duration_to_start =
            self.starting_timestamp
                .checked_sub(bounds.start_timestamp)
                .ok_or("Start boundary duration overflowed")? as f64;
        let mut duration_to_end = bounds
            .end_timestamp
            .checked_sub(self.last_seen_timestamp)
            .ok_or("End boundary duration overflowed")? as f64;

        if duration_to_start >= extrapolation_threshold {
            duration_to_start = average_sample_interval / 2.0;
        }
        if duration_to_end >= extrapolation_threshold {
            duration_to_end = average_sample_interval / 2.0;
        }

        let increase = self.increase();
        if increase > 0.0 && self.starting_measurement.value >= 0.0 {
            let duration_to_zero = sampled_interval * (self.starting_measurement.value / increase);
            if duration_to_zero < duration_to_start {
                duration_to_start = duration_to_zero;
            }
        }

        let factor = (sampled_interval + duration_to_start + duration_to_end) / sampled_interval;
        let query_interval = bounds
            .end_timestamp
            .checked_sub(bounds.start_timestamp)
            .ok_or("Query range duration overflowed")? as f64;
        let result = match statistic {
            Statistic::Increase => increase * factor,
            Statistic::Rate => increase * factor / query_interval * 1000.0,
            _ => {
                return Err(
                    format!("Unsupported statistic in IncreaseAccumulator: {statistic:?}").into(),
                )
            }
        };

        Ok(result)
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

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        if data.get("opaque_reset_adjustment").is_some()
            || data.get("opaque_reset_ranges").is_some()
        {
            return Err("Opaque reset metadata is not supported by the current format".into());
        }

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
        let sample_count = data["sample_count"]
            .as_u64()
            .ok_or("Missing or invalid 'sample_count' field")?;
        if sample_count == 0 {
            return Err("Sample count must be positive".into());
        }
        let counter_reset_adjustment = data["counter_reset_adjustment"]
            .as_f64()
            .ok_or("Missing or invalid 'counter_reset_adjustment' field")?;
        let counter_reset_events = serde_json::from_value(data["counter_reset_events"].clone())
            .map_err(|e| format!("Invalid 'counter_reset_events' field: {e}"))?;
        let mut accumulator = Self::new(
            starting_measurement,
            starting_timestamp,
            last_seen_measurement,
            last_seen_timestamp,
        );
        accumulator.sample_count = sample_count;
        accumulator.counter_reset_adjustment = counter_reset_adjustment;
        accumulator.counter_reset_events = counter_reset_events;
        Ok(accumulator)
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        Self::deserialize_from_bytes_with_consumed(buffer).map(|(accumulator, _)| accumulator)
    }

    pub(crate) fn deserialize_from_bytes_with_consumed(
        buffer: &[u8],
    ) -> Result<(Self, usize), Box<dyn std::error::Error>> {
        if !buffer.starts_with(&INCREASE_BINARY_FORMAT_MAGIC) {
            return Err("Unsupported IncreaseAccumulator binary format".into());
        }
        let mut offset = INCREASE_BINARY_FORMAT_MAGIC.len();

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

        if buffer.len() < offset + 8 {
            return Err("Buffer too short for sample count".into());
        }
        let sample_count = u64::from_le_bytes([
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
        if sample_count == 0 {
            return Err("Sample count must be positive".into());
        }

        if buffer.len() < offset + 8 {
            return Err("Buffer too short for counter reset adjustment".into());
        }
        let counter_reset_adjustment = f64::from_le_bytes([
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
            .checked_mul(RESET_RECORD_BYTES)
            .ok_or("Counter reset event data length overflow")?;
        if buffer.len() < offset + event_bytes {
            return Err("Buffer too short for counter reset events".into());
        }
        let counter_reset_events = (0..event_count)
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
                offset += RESET_RECORD_BYTES;
                CounterResetEvent {
                    timestamp,
                    adjustment,
                }
            })
            .collect();

        let mut accumulator = Self::new(
            starting_measurement,
            starting_timestamp,
            last_seen_measurement,
            last_seen_timestamp,
        );
        accumulator.sample_count = sample_count;
        accumulator.counter_reset_adjustment = counter_reset_adjustment;
        accumulator.counter_reset_events = counter_reset_events;
        Ok((accumulator, offset))
    }
}

impl SerializableToSink for IncreaseAccumulator {
    fn serialize_to_json(&self) -> Value {
        serde_json::json!({
            "starting_measurement": self.starting_measurement.serialize_to_json(),
            "starting_timestamp": self.starting_timestamp,
            "last_seen_measurement": self.last_seen_measurement.serialize_to_json(),
            "last_seen_timestamp": self.last_seen_timestamp,
            "sample_count": self.sample_count,
            "counter_reset_adjustment": self.counter_reset_adjustment,
            "counter_reset_events": self.counter_reset_events,
        })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        let starting_measurement_bytes = self.starting_measurement.serialize_to_bytes();
        let last_seen_measurement_bytes = self.last_seen_measurement.serialize_to_bytes();

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&INCREASE_BINARY_FORMAT_MAGIC);

        // Starting measurement length and data
        buffer.extend_from_slice(&(starting_measurement_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&starting_measurement_bytes);

        // Starting timestamp
        buffer.extend_from_slice(&self.starting_timestamp.to_le_bytes());

        // Last seen measurement length and data
        buffer.extend_from_slice(&(last_seen_measurement_bytes.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&last_seen_measurement_bytes);

        // Last seen timestamp and total reset adjustment
        buffer.extend_from_slice(&self.last_seen_timestamp.to_le_bytes());
        buffer.extend_from_slice(&self.sample_count.to_le_bytes());
        buffer.extend_from_slice(&self.counter_reset_adjustment.to_le_bytes());
        buffer.extend_from_slice(&(self.counter_reset_events.len() as u32).to_le_bytes());
        for event in &self.counter_reset_events {
            buffer.extend_from_slice(&event.timestamp.to_le_bytes());
            buffer.extend_from_slice(&event.adjustment.to_le_bytes());
        }

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

        let mut accumulators = accumulators;
        accumulators.sort_by_key(|acc| acc.starting_timestamp);
        let mut result = accumulators.remove(0);

        for acc in accumulators {
            // Query-time merges receive disjoint pane/window observations.
            // Reset events are deduplicated for defensive overlap handling,
            // but arbitrary duplicate samples are not identifiable from this
            // summary shape, so sample counts remain additive by contract.
            result.sample_count = result
                .sample_count
                .checked_add(acc.sample_count)
                .ok_or("Sample count overflow while merging IncreaseAccumulator")?;

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

            for event in acc.counter_reset_events {
                result.add_reset_event(event);
            }

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

    fn query_statistic_with_bounds(
        &self,
        statistic: Statistic,
        _key: &Option<crate::KeyByLabelValues>,
        _query_kwargs: &HashMap<String, String>,
        bounds: &QueryBounds,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        self.query_with_bounds(statistic, bounds)
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
            Statistic::Increase => Ok(self.increase()),
            Statistic::Rate => {
                // Convert to per second; timestamps are in milliseconds
                let time_diff = (self.last_seen_timestamp - self.starting_timestamp) as f64;
                if time_diff <= 0.0 {
                    return Err("Invalid time difference for rate calculation".into());
                }
                Ok(self.increase() / time_diff * 1000.0)
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
    fn no_boundary_gap_keeps_increase_and_scales_rate_to_requested_range() {
        let mut acc = IncreaseAccumulator::new(
            Measurement::new(100.0),
            1_000,
            Measurement::new(100.0),
            1_000,
        );
        acc.update(Measurement::new(110.0), 2_000);
        acc.update(Measurement::new(120.0), 3_000);

        let bounds = crate::data_model::QueryBounds::new(1_000, 3_000);

        assert_eq!(
            acc.query_with_bounds(Statistic::Increase, &bounds).unwrap(),
            20.0
        );
        assert_eq!(
            acc.query_with_bounds(Statistic::Rate, &bounds).unwrap(),
            10.0
        );
    }

    #[test]
    fn large_boundary_gaps_are_limited_to_half_average_sample_interval() {
        let mut acc = IncreaseAccumulator::new(
            Measurement::new(100.0),
            1_000,
            Measurement::new(100.0),
            1_000,
        );
        acc.update(Measurement::new(110.0), 2_000);
        acc.update(Measurement::new(120.0), 3_000);

        let bounds = crate::data_model::QueryBounds::new(-1_000, 5_000);

        assert_eq!(
            acc.query_with_bounds(Statistic::Increase, &bounds).unwrap(),
            30.0
        );
        assert_eq!(
            acc.query_with_bounds(Statistic::Rate, &bounds).unwrap(),
            5.0
        );
    }

    #[test]
    fn exact_threshold_uses_half_an_average_sample_interval() {
        let acc = IncreaseAccumulator::new_with_sample_count(
            Measurement::new(100.0),
            1_000,
            Measurement::new(110.0),
            2_000,
            2,
        );
        let bounds = crate::data_model::QueryBounds::new(-100, 2_000);

        // The left gap is exactly 1.1 times the average interval, so the
        // inclusive threshold must choose half an interval (500ms).
        assert_eq!(
            acc.query_with_bounds(Statistic::Increase, &bounds).unwrap(),
            15.0
        );
    }

    #[test]
    fn irregular_sample_intervals_use_the_average_interval() {
        let acc = IncreaseAccumulator::new_with_sample_count(
            Measurement::new(100.0),
            0,
            Measurement::new(125.0),
            1_700,
            3,
        );
        let bounds = crate::data_model::QueryBounds::new(-700, 2_400);

        let expected_increase = 25.0 * 3_100.0 / 1_700.0;
        let expected_rate = expected_increase / 3_100.0 * 1_000.0;
        assert!(
            (acc.query_with_bounds(Statistic::Increase, &bounds).unwrap() - expected_increase)
                .abs()
                < 1e-12
        );
        assert!(
            (acc.query_with_bounds(Statistic::Rate, &bounds).unwrap() - expected_rate).abs()
                < 1e-12
        );
    }

    #[test]
    fn fewer_than_two_samples_and_degenerate_ranges_are_rejected() {
        let single_sample = IncreaseAccumulator::new(
            Measurement::new(100.0),
            1_000,
            Measurement::new(100.0),
            1_000,
        );
        assert!(single_sample
            .query_with_bounds(
                Statistic::Increase,
                &crate::data_model::QueryBounds::new(0, 2_000)
            )
            .is_err());

        let two_samples = IncreaseAccumulator::new_with_sample_count(
            Measurement::new(100.0),
            1_000,
            Measurement::new(110.0),
            2_000,
            2,
        );
        assert!(two_samples
            .query_with_bounds(
                Statistic::Rate,
                &crate::data_model::QueryBounds::new(2_000, 2_000)
            )
            .is_err());
    }

    #[test]
    fn counter_resets_are_corrected_before_boundary_extrapolation() {
        let mut acc = IncreaseAccumulator::new(
            Measurement::new(100.0),
            1_000,
            Measurement::new(100.0),
            1_000,
        );
        acc.update(Measurement::new(150.0), 2_000);
        acc.update(Measurement::new(10.0), 3_000);
        acc.update(Measurement::new(60.0), 4_000);

        let bounds = crate::data_model::QueryBounds::new(0, 5_000);

        let increase = acc.query_with_bounds(Statistic::Increase, &bounds).unwrap();
        let rate = acc.query_with_bounds(Statistic::Rate, &bounds).unwrap();
        assert!((increase - (110.0 * 5.0 / 3.0)).abs() < f64::EPSILON);
        assert!((rate - (110.0 / 3.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn counter_extrapolation_is_clamped_to_duration_to_zero() {
        let acc = IncreaseAccumulator::new_with_sample_count(
            Measurement::new(1.0),
            1_000,
            Measurement::new(101.0),
            3_000,
            2,
        );
        let bounds = crate::data_model::QueryBounds::new(-500, 3_000);

        // Without the counter-to-zero clamp, the 1.5s left gap would be
        // extrapolated as if the counter had already been increasing before
        // the first observed sample. Prometheus limits it to 20ms here.
        assert_eq!(
            acc.query_with_bounds(Statistic::Increase, &bounds).unwrap(),
            101.0
        );
    }

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
        assert_eq!(acc.increase(), 15.0);

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
        assert_eq!(merged.sample_count, 3);
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
    fn test_increase_accumulator_serialization() {
        let mut acc =
            IncreaseAccumulator::new(Measurement::new(10.0), 1000, Measurement::new(10.0), 1000);
        acc.update(Measurement::new(15.0), 1500);
        acc.update(Measurement::new(25.0), 2000);

        // Test JSON serialization
        let json = acc.serialize_to_json();
        assert_eq!(json["sample_count"], 3);
        assert!(json.get("opaque_reset_adjustment").is_none());
        assert!(json.get("opaque_reset_ranges").is_none());
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
        assert_eq!(acc.sample_count, deserialized.sample_count);

        // Test byte serialization
        let bytes = acc.serialize_to_bytes();
        assert_eq!(&bytes[..4], b"INC7");
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
        assert_eq!(acc.sample_count, deserialized_bytes.sample_count);
    }

    #[test]
    fn test_deserialize_from_bytes_rejects_previous_formats() {
        assert!(IncreaseAccumulator::deserialize_from_bytes(b"INC2").is_err());
        assert!(IncreaseAccumulator::deserialize_from_bytes(b"INC6").is_err());
        assert!(IncreaseAccumulator::deserialize_from_bytes(b"INC5").is_err());
    }

    #[test]
    fn test_deserialize_from_json_rejects_legacy_payloads() {
        let legacy = serde_json::json!({
            "starting_measurement": {"value": 10.0},
            "starting_timestamp": 1000,
            "last_seen_measurement": {"value": 25.0},
            "last_seen_timestamp": 2000
        });

        assert!(IncreaseAccumulator::deserialize_from_json(&legacy).is_err());
    }

    #[test]
    fn test_deserialize_from_bytes_reports_consumed_bytes() {
        let first =
            IncreaseAccumulator::new(Measurement::new(10.0), 1000, Measurement::new(25.0), 2000);
        let second =
            IncreaseAccumulator::new(Measurement::new(30.0), 3000, Measurement::new(45.0), 4000);
        let first_bytes = first.serialize_to_bytes();
        let second_bytes = second.serialize_to_bytes();
        let mut combined = first_bytes.clone();
        combined.extend_from_slice(&second_bytes);

        let (deserialized, consumed) =
            IncreaseAccumulator::deserialize_from_bytes_with_consumed(&combined).unwrap();

        assert_eq!(deserialized.starting_timestamp, first.starting_timestamp);
        assert_eq!(deserialized.last_seen_timestamp, first.last_seen_timestamp);
        assert_eq!(consumed, first_bytes.len());
        assert_eq!(
            IncreaseAccumulator::deserialize_from_bytes(&combined[consumed..])
                .unwrap()
                .starting_timestamp,
            second.starting_timestamp
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
    fn test_increase_accumulator_keeps_infinite_event_adjustment() {
        let earlier =
            IncreaseAccumulator::new(Measurement::new(0.0), -1_000, Measurement::new(0.0), 0);
        let mut event_aware =
            IncreaseAccumulator::new(Measurement::new(0.0), 0, Measurement::new(0.0), 0);
        event_aware.update(Measurement::new(f64::INFINITY), 1_000);
        event_aware.update(Measurement::new(0.0), 2_000);

        let merged =
            <IncreaseAccumulator as MergeableAccumulator<IncreaseAccumulator>>::merge_accumulators(
                vec![earlier, event_aware],
            )
            .unwrap();

        assert_eq!(
            merged.query(Statistic::Increase, None).unwrap(),
            f64::INFINITY
        );
    }

    #[test]
    fn test_increase_accumulator_keeps_mixed_infinite_reset_adjustment() {
        let mut earlier_with_adjustment =
            IncreaseAccumulator::new(Measurement::new(100.0), 0, Measurement::new(150.0), 1_000);
        earlier_with_adjustment.counter_reset_adjustment = 100.0;

        let mut event_aware = IncreaseAccumulator::new(
            Measurement::new(150.0),
            1_000,
            Measurement::new(150.0),
            1_000,
        );
        event_aware.update(Measurement::new(f64::INFINITY), 1_500);
        event_aware.update(Measurement::new(0.0), 2_000);

        let later_result =
            <IncreaseAccumulator as MergeableAccumulator<IncreaseAccumulator>>::merge_accumulators(
                vec![earlier_with_adjustment, event_aware],
            )
            .unwrap();
        let earlier =
            IncreaseAccumulator::new(Measurement::new(50.0), -1_000, Measurement::new(100.0), 0);

        let merged =
            <IncreaseAccumulator as MergeableAccumulator<IncreaseAccumulator>>::merge_accumulators(
                vec![later_result, earlier],
            )
            .unwrap();

        assert_eq!(
            merged.query(Statistic::Increase, None).unwrap(),
            f64::INFINITY
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
