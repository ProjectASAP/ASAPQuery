use crate::data_model::{
    AggregateCore, AggregationType, Measurement, MergeableAccumulator, SerializableToSink,
    SingleSubpopulationAggregate, SingleSubpopulationAggregateFactory,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use promql_utilities::query_logics::enums::{Statistic, RANGE_END_MS_KWARG, RANGE_START_MS_KWARG};

/// Sample count assumed when deserializing a blob that predates the
/// `sample_count` field. Two (the minimum for a defined increase) keeps
/// extrapolation well-defined instead of dividing by `sample_count - 1 == 0`.
const DEFAULT_LEGACY_SAMPLE_COUNT: u64 = 2;

/// MessagePack ("Arroyo") wire form of an [`IncreaseAccumulator`]: the window
/// endpoints plus the reset correction and sample count, with the measurements
/// flattened to `f64`.
///
/// The field order/types intentionally mirror the per-key `MeasurementData`
/// struct inside `MultipleIncreaseAccumulator`, so a single-population Increase
/// blob is byte-identical to one MultipleIncrease entry (rmp-serde encodes
/// structs as positional arrays). `#[serde(default)]` on the trailing fields
/// lets legacy 4-field blobs (from before reset support) still decode.
#[derive(Serialize, Deserialize)]
struct ArroyoIncrease {
    starting_measurement: f64,
    starting_timestamp: i64,
    last_seen_measurement: f64,
    last_seen_timestamp: i64,
    #[serde(default)]
    counter_reset_correction: f64,
    #[serde(default)]
    sample_count: u64,
}

/// Accumulator for tracking increases in counter metrics.
///
/// Stores the starting and last-seen measurements with timestamps, plus the
/// running counter-reset correction and the number of samples observed. The
/// extra state is what lets `query` reproduce Prometheus `rate()`/`increase()`
/// semantics (counter-reset correction + extrapolation) even though only the
/// window endpoints are retained — see [`IncreaseAccumulator::update`] and
/// [`IncreaseAccumulator::extrapolated`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncreaseAccumulator {
    pub starting_measurement: Measurement,
    pub starting_timestamp: i64,
    pub last_seen_measurement: Measurement,
    pub last_seen_timestamp: i64,
    /// Sum of the values observed just before each counter reset within this
    /// window (Prometheus `counterCorrection`). Added back to `last - start` so
    /// the increase stays monotonic across restarts.
    pub counter_reset_correction: f64,
    /// Number of samples folded into this accumulator. Needed to estimate the
    /// average inter-sample interval during extrapolation.
    pub sample_count: u64,
}

impl IncreaseAccumulator {
    /// Construct from the first observed sample (`starting == last_seen`), with a
    /// zero reset correction and a sample count of 1. The per-sample updaters in
    /// `accumulator_factory` create via this constructor and then call
    /// [`update`](Self::update) for every subsequent sample.
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
            counter_reset_correction: 0.0,
            sample_count: 1,
        }
    }

    /// Construct with every field explicit. Used only by deserializers, which
    /// must restore the persisted correction and sample count rather than the
    /// first-sample defaults of [`new`](Self::new).
    pub fn new_full(
        starting_measurement: Measurement,
        starting_timestamp: i64,
        last_seen_measurement: Measurement,
        last_seen_timestamp: i64,
        counter_reset_correction: f64,
        sample_count: u64,
    ) -> Self {
        Self {
            starting_measurement,
            starting_timestamp,
            last_seen_measurement,
            last_seen_timestamp,
            counter_reset_correction,
            sample_count,
        }
    }

    pub fn update(&mut self, measurement: Measurement, timestamp: i64) {
        // Counter-reset detection: a sample lower than its predecessor means the
        // counter restarted, so add the pre-reset value back (Prometheus
        // `counterCorrection`). This must happen here, per sample — only the
        // window endpoints survive, so a reset between them is unrecoverable at
        // query time.
        if measurement.value < self.last_seen_measurement.value {
            self.counter_reset_correction += self.last_seen_measurement.value;
        }
        self.last_seen_measurement = measurement;
        self.last_seen_timestamp = timestamp;
        self.sample_count += 1;
    }

    /// Reset-corrected raw increase over the observed samples: `last - start`
    /// plus the accumulated counter-reset correction.
    pub fn corrected_increase(&self) -> f64 {
        self.last_seen_measurement.value - self.starting_measurement.value
            + self.counter_reset_correction
    }

    /// Prometheus `extrapolatedRate`: extrapolate the reset-corrected increase to
    /// the range-vector boundaries `[range_start_ms, range_end_ms]`. When
    /// `is_rate` is true the result is divided by the range duration (per-second
    /// rate); otherwise it is the extrapolated increase.
    ///
    /// This mirrors `extrapolatedRate` in Prometheus `promql/functions.go`
    /// (`isCounter == true`, the rate/increase case) step for step, including the
    /// order in which `durationToStart` is clamped (threshold first, then the
    /// counter zero-point clamp) and the `factor = 1.0` fallback when the sampled
    /// interval is zero. ASAP does not track per-sample created timestamps, so the
    /// start-timestamp / anchored / smoothed branches are not reproduced.
    ///
    /// Returns `None` when fewer than two samples were observed (Prometheus emits
    /// no point in that case, absent a created-timestamp reset) or when the query
    /// range is degenerate.
    pub fn extrapolated(
        &self,
        is_rate: bool,
        range_start_ms: i64,
        range_end_ms: i64,
    ) -> Option<f64> {
        // Without two samples (and lacking created-timestamp tracking), Prometheus
        // returns no point.
        if self.sample_count < 2 {
            return None;
        }
        let num_samples_minus_one = (self.sample_count - 1) as f64;

        let first_value = self.starting_measurement.value;
        let result_value = self.corrected_increase();

        // Duration between first/last samples and the boundary of the range.
        let mut duration_to_start = (self.starting_timestamp - range_start_ms) as f64 / 1000.0;
        let mut duration_to_end = (range_end_ms - self.last_seen_timestamp) as f64 / 1000.0;

        let sampled_interval = (self.last_seen_timestamp - self.starting_timestamp) as f64 / 1000.0;
        let average_duration_between_samples = sampled_interval / num_samples_minus_one;
        let extrapolation_threshold = average_duration_between_samples * 1.1;

        // If the first sample is close enough to the lower boundary, extrapolate to
        // it; otherwise only extrapolate half an average gap.
        if duration_to_start >= extrapolation_threshold {
            duration_to_start = average_duration_between_samples / 2.0;
        }
        // Counters cannot be negative: if the series has a positive slope, never
        // extrapolate the start earlier than where the counter would have been 0.
        // (Prometheus applies this AFTER the threshold clamp above.)
        let mut duration_to_zero = duration_to_start;
        if result_value > 0.0 && first_value >= 0.0 {
            duration_to_zero = sampled_interval * (first_value / result_value);
        }
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }

        if duration_to_end >= extrapolation_threshold {
            duration_to_end = average_duration_between_samples / 2.0;
        }

        let mut factor = 1.0;
        if sampled_interval != 0.0 {
            factor = (sampled_interval + duration_to_start + duration_to_end) / sampled_interval;
        }
        if is_rate {
            let range_seconds = (range_end_ms - range_start_ms) as f64 / 1000.0;
            if range_seconds <= 0.0 {
                return None;
            }
            factor /= range_seconds;
        }
        Some(result_value * factor)
    }

    /// Order-independent pairwise merge of two windows that together form a
    /// contiguous (tumbling) range. Keeps the earliest start and latest
    /// last-seen, sums both corrections and sample counts, and adds a *boundary*
    /// counter-reset correction when the later window's first value is below the
    /// earlier window's last value (a reset across the window seam). Folding this
    /// left-to-right over time-ordered windows is correct because the running
    /// result always carries the latest segment's `last_seen`.
    ///
    /// Assumes non-overlapping windows (the only kind the engine produces — see
    /// the tumbling-window merge loop in `simple_engine`), so the window with the
    /// earlier start also has the earlier `last_seen`.
    pub fn merge_pair(a: &IncreaseAccumulator, b: &IncreaseAccumulator) -> IncreaseAccumulator {
        let (earlier, later) = if a.starting_timestamp <= b.starting_timestamp {
            (a, b)
        } else {
            (b, a)
        };

        let boundary_correction =
            if later.starting_measurement.value < earlier.last_seen_measurement.value {
                earlier.last_seen_measurement.value
            } else {
                0.0
            };

        IncreaseAccumulator::new_full(
            earlier.starting_measurement.clone(),
            earlier.starting_timestamp,
            later.last_seen_measurement.clone(),
            later.last_seen_timestamp,
            earlier.counter_reset_correction + later.counter_reset_correction + boundary_correction,
            earlier.sample_count + later.sample_count,
        )
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
        // Tolerate legacy blobs written before these fields existed.
        let counter_reset_correction = data["counter_reset_correction"].as_f64().unwrap_or(0.0);
        let sample_count = data["sample_count"]
            .as_u64()
            .unwrap_or(DEFAULT_LEGACY_SAMPLE_COUNT);

        Ok(Self::new_full(
            starting_measurement,
            starting_timestamp,
            last_seen_measurement,
            last_seen_timestamp,
            counter_reset_correction,
            sample_count,
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
        offset += 8;

        // Counter-reset correction (f64) and sample count (u64) were appended
        // after the original four fields. Tolerate legacy buffers that lack them.
        let counter_reset_correction = if buffer.len() >= offset + 8 {
            let v = f64::from_le_bytes([
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
            v
        } else {
            0.0
        };
        let sample_count = if buffer.len() >= offset + 8 {
            u64::from_le_bytes([
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
            DEFAULT_LEGACY_SAMPLE_COUNT
        };

        Ok(Self::new_full(
            starting_measurement,
            starting_timestamp,
            last_seen_measurement,
            last_seen_timestamp,
            counter_reset_correction,
            sample_count,
        ))
    }

    /// Serialize to the Arroyo-compatible MessagePack form (see [`ArroyoIncrease`]).
    ///
    /// This is the format the DataFusion read path emits for single-population
    /// Increase sketches (via `serialize_accumulator_arroyo`), so downstream
    /// operators can reconstruct the accumulator with
    /// [`deserialize_from_bytes_arroyo`](Self::deserialize_from_bytes_arroyo).
    pub fn serialize_to_bytes_arroyo(&self) -> Vec<u8> {
        let wire = ArroyoIncrease {
            starting_measurement: self.starting_measurement.value,
            starting_timestamp: self.starting_timestamp,
            last_seen_measurement: self.last_seen_measurement.value,
            last_seen_timestamp: self.last_seen_timestamp,
            counter_reset_correction: self.counter_reset_correction,
            sample_count: self.sample_count,
        };
        rmp_serde::to_vec(&wire).expect("Failed to serialize IncreaseAccumulator to MessagePack")
    }

    /// Deserialize from the Arroyo-compatible MessagePack form. Mirrors
    /// `MultipleIncreaseAccumulator::deserialize_from_bytes_arroyo`, including the
    /// legacy handling where an absent `sample_count` (decoded as 0) falls back to
    /// [`DEFAULT_LEGACY_SAMPLE_COUNT`] so extrapolation stays well-defined.
    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let wire: ArroyoIncrease = rmp_serde::from_slice(buffer).map_err(|e| {
            format!("Failed to deserialize IncreaseAccumulator from MessagePack: {e}")
        })?;
        let sample_count = if wire.sample_count == 0 {
            DEFAULT_LEGACY_SAMPLE_COUNT
        } else {
            wire.sample_count
        };
        Ok(Self::new_full(
            Measurement::new(wire.starting_measurement),
            wire.starting_timestamp,
            Measurement::new(wire.last_seen_measurement),
            wire.last_seen_timestamp,
            wire.counter_reset_correction,
            sample_count,
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
            "counter_reset_correction": self.counter_reset_correction,
            "sample_count": self.sample_count,
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

        // Counter-reset correction (f64) and sample count (u64), appended last so
        // legacy readers/decoders that stop after the four original fields still work.
        buffer.extend_from_slice(&self.counter_reset_correction.to_le_bytes());
        buffer.extend_from_slice(&self.sample_count.to_le_bytes());

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

        // Sort by start time so boundary counter-reset corrections are evaluated
        // between temporally adjacent windows, then fold left-to-right.
        let mut sorted = accumulators;
        sorted.sort_by_key(|a| a.starting_timestamp);

        let mut result = sorted[0].clone();
        for acc in &sorted[1..] {
            result = IncreaseAccumulator::merge_pair(&result, acc);
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

        // Boundary-aware pairwise merge (order-independent).
        let merged = Self::merge_pair(self, other_increase);

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
        query_kwargs: &std::collections::HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        use crate::data_model::SingleSubpopulationAggregate;
        // Forward the kwargs so range-vector boundaries reach `query`.
        self.query(statistic, Some(query_kwargs))
    }
}

impl SingleSubpopulationAggregate for IncreaseAccumulator {
    fn query(
        &self,
        statistic: Statistic,
        query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        let is_rate = match statistic {
            Statistic::Increase => false,
            Statistic::Rate => true,
            _ => {
                return Err(
                    format!("Unsupported statistic in IncreaseAccumulator: {statistic:?}").into(),
                )
            }
        };

        // When the PromQL layer supplies the range-vector boundaries, reproduce
        // Prometheus `extrapolatedRate` (counter-reset correction + extrapolation).
        let range_bounds = query_kwargs.and_then(|kw| {
            let start = kw.get(RANGE_START_MS_KWARG)?.parse::<i64>().ok()?;
            let end = kw.get(RANGE_END_MS_KWARG)?.parse::<i64>().ok()?;
            Some((start, end))
        });

        if let Some((range_start_ms, range_end_ms)) = range_bounds {
            return self
                .extrapolated(is_rate, range_start_ms, range_end_ms)
                .ok_or_else(|| -> Box<dyn std::error::Error + Send + Sync> {
                    "Insufficient samples for rate/increase extrapolation".into()
                });
        }

        // Fallback (e.g. SQL/Elastic callers with no range vector): reset-corrected
        // increase, with rate divided by the sampled interval as before.
        let increase = self.corrected_increase();
        if is_rate {
            let time_diff = (self.last_seen_timestamp - self.starting_timestamp) as f64;
            if time_diff <= 0.0 {
                return Err("Invalid time difference for rate calculation".into());
            }
            Ok(increase / time_diff * 1000.0)
        } else {
            Ok(increase)
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

    use promql_utilities::query_logics::enums::{RANGE_END_MS_KWARG, RANGE_START_MS_KWARG};

    /// Feed a sample stream into an accumulator the way the ingest updater does:
    /// `new` on the first sample, then `update` for each subsequent one.
    fn feed(samples: &[(i64, f64)]) -> IncreaseAccumulator {
        let (ts0, v0) = samples[0];
        let mut acc =
            IncreaseAccumulator::new(Measurement::new(v0), ts0, Measurement::new(v0), ts0);
        for &(ts, v) in &samples[1..] {
            acc.update(Measurement::new(v), ts);
        }
        acc
    }

    #[test]
    fn test_reset_within_window_tracks_correction() {
        // 10 -> 100 -> 5 (reset, +100) -> 30. Increase = 30 - 10 + 100 = 120.
        let acc = feed(&[(1000, 10.0), (2000, 100.0), (3000, 5.0), (4000, 30.0)]);
        assert_eq!(acc.counter_reset_correction, 100.0);
        assert_eq!(acc.sample_count, 4);
        assert_eq!(acc.corrected_increase(), 120.0);
        // Fallback query (no range kwargs) returns the reset-corrected increase.
        assert_eq!(
            crate::SingleSubpopulationAggregate::query(&acc, Statistic::Increase, None).unwrap(),
            120.0
        );
    }

    #[test]
    fn test_multiple_resets_within_window() {
        // 10 ->50 ->5(+50) ->40 ->2(+40) ->20. Increase = 20 - 10 + 50 + 40 = 100.
        let acc = feed(&[
            (1000, 10.0),
            (2000, 50.0),
            (3000, 5.0),
            (4000, 40.0),
            (5000, 2.0),
            (6000, 20.0),
        ]);
        assert_eq!(acc.counter_reset_correction, 90.0);
        assert_eq!(acc.corrected_increase(), 100.0);
    }

    #[test]
    fn test_fallback_rate_uses_corrected_increase() {
        // Reset-corrected increase 120 over 3000 ms => 40/s.
        let acc = feed(&[(1000, 10.0), (2000, 100.0), (3000, 5.0), (4000, 30.0)]);
        assert_eq!(
            crate::SingleSubpopulationAggregate::query(&acc, Statistic::Rate, None).unwrap(),
            40.0
        );
    }

    #[test]
    fn test_merge_boundary_reset_correction() {
        // Window A: 10 -> 20. Window B: 5 -> 30 (B starts below A's last => reset).
        // Increase = 30 - 10 + boundary(20) = 40.
        let a =
            IncreaseAccumulator::new(Measurement::new(10.0), 1000, Measurement::new(20.0), 2000);
        let b = IncreaseAccumulator::new(Measurement::new(5.0), 3000, Measurement::new(30.0), 4000);

        let merged = IncreaseAccumulator::merge_pair(&a, &b);
        assert_eq!(merged.starting_measurement.value, 10.0);
        assert_eq!(merged.last_seen_measurement.value, 30.0);
        assert_eq!(merged.counter_reset_correction, 20.0);
        assert_eq!(merged.sample_count, 2);
        assert_eq!(merged.corrected_increase(), 40.0);

        // Order-independent: merging B,A yields the same result.
        let merged_rev = IncreaseAccumulator::merge_pair(&b, &a);
        assert_eq!(merged_rev.corrected_increase(), 40.0);
        assert_eq!(merged_rev.starting_measurement.value, 10.0);
        assert_eq!(merged_rev.last_seen_measurement.value, 30.0);

        // No reset at the boundary (B starts >= A's last) => no boundary correction.
        let c =
            IncreaseAccumulator::new(Measurement::new(25.0), 3000, Measurement::new(40.0), 4000);
        let merged_no_reset = IncreaseAccumulator::merge_pair(&a, &c);
        assert_eq!(merged_no_reset.counter_reset_correction, 0.0);
        assert_eq!(merged_no_reset.corrected_increase(), 30.0);
    }

    #[test]
    fn test_merge_accumulators_sorts_and_sums() {
        // Three contiguous windows fed out of order; corrections + counts sum.
        let w1 = feed(&[(1000, 0.0), (2000, 40.0)]); // +40, count 2
        let w2 = feed(&[(3000, 50.0), (4000, 10.0), (5000, 60.0)]); // reset +50, count 3
        let w3 = feed(&[(6000, 70.0), (7000, 90.0)]); // +20, count 2
        let merged =
            <IncreaseAccumulator as MergeableAccumulator<IncreaseAccumulator>>::merge_accumulators(
                vec![w3.clone(), w1.clone(), w2.clone()],
            )
            .unwrap();
        // boundaries: w1.last(40) -> w2.start(50): no reset. w2.last(60) -> w3.start(70): no reset.
        // total correction = 0 + 50 + 0 (intra w2) + boundaries(0) = 50.
        assert_eq!(merged.starting_measurement.value, 0.0);
        assert_eq!(merged.last_seen_measurement.value, 90.0);
        assert_eq!(merged.counter_reset_correction, 50.0);
        assert_eq!(merged.sample_count, 7);
        // increase = 90 - 0 + 50 = 140.
        assert_eq!(merged.corrected_increase(), 140.0);
    }

    #[test]
    fn test_extrapolation_parity() {
        // 6 samples spanning [5s, 55s] (sampledInterval 50s, avgGap 10s) inside the
        // range [0, 60s]. Hand-computed Prometheus extrapolatedRate:
        //   durationToStart = 5s (clamped by durationToZero = 50*(10/60)=8.33 -> 5)
        //   durationToEnd   = 5s, threshold = 11s
        //   extrapolateTo = 50 + 5 + 5 = 60 ; factor = 60/50 = 1.2
        //   increase = 60 * 1.2 = 72 ; rate = 72 / 60 = 1.2
        let acc = IncreaseAccumulator::new_full(
            Measurement::new(10.0),
            5_000,
            Measurement::new(70.0),
            55_000,
            0.0,
            6,
        );
        let inc = acc.extrapolated(false, 0, 60_000).unwrap();
        let rate = acc.extrapolated(true, 0, 60_000).unwrap();
        assert!((inc - 72.0).abs() < 1e-9, "increase = {inc}");
        assert!((rate - 1.2).abs() < 1e-9, "rate = {rate}");

        // Reached via query() with range kwargs.
        let mut kwargs = HashMap::new();
        kwargs.insert(RANGE_START_MS_KWARG.to_string(), "0".to_string());
        kwargs.insert(RANGE_END_MS_KWARG.to_string(), "60000".to_string());
        let q =
            crate::SingleSubpopulationAggregate::query(&acc, Statistic::Increase, Some(&kwargs))
                .unwrap();
        assert!((q - 72.0).abs() < 1e-9, "query increase = {q}");
    }

    #[test]
    fn test_extrapolation_threshold_then_zero_clamp_order() {
        // First sample far from the lower boundary (durationToStart 50s >> threshold
        // 11s) so Prometheus clamps it to avgGap/2 = 5s FIRST, then the zero-point
        // clamp (durationToZero = 50*(10/62.5) = 8s) does NOT apply (8 >= 5).
        //   factor = (50 + 5 + 0)/50 = 1.1 ; increase = 62.5 * 1.1 = 68.75.
        // (Applying the zero clamp before the threshold clamp would wrongly give
        // 72.5 — this test pins the Prometheus ordering.)
        let acc = IncreaseAccumulator::new_full(
            Measurement::new(10.0),
            60_000,
            Measurement::new(72.5),
            110_000,
            0.0,
            6,
        );
        let inc = acc.extrapolated(false, 10_000, 110_000).unwrap();
        assert!((inc - 68.75).abs() < 1e-9, "increase = {inc}");
    }

    #[test]
    fn test_extrapolation_zero_clamp_engages() {
        // Counter starts near zero (2) relative to its increase (100), far from the
        // boundary. After the threshold clamp (durationToStart -> 5s), the zero
        // clamp applies: durationToZero = 50*(2/100) = 1s < 5s.
        //   factor = (50 + 1 + 0)/50 = 1.02 ; increase = 100 * 1.02 = 102.
        let acc = IncreaseAccumulator::new_full(
            Measurement::new(2.0),
            60_000,
            Measurement::new(102.0),
            110_000,
            0.0,
            6,
        );
        let inc = acc.extrapolated(false, 10_000, 110_000).unwrap();
        assert!((inc - 102.0).abs() < 1e-9, "increase = {inc}");
    }

    #[test]
    fn test_extrapolation_requires_two_samples() {
        let acc =
            IncreaseAccumulator::new(Measurement::new(10.0), 1000, Measurement::new(10.0), 1000);
        assert_eq!(acc.sample_count, 1);
        assert!(acc.extrapolated(false, 0, 60_000).is_none());
    }

    #[test]
    fn test_serde_roundtrip_new_fields() {
        let acc = feed(&[(1000, 10.0), (2000, 100.0), (3000, 5.0), (4000, 30.0)]);
        // JSON
        let json = acc.serialize_to_json();
        let back = IncreaseAccumulator::deserialize_from_json(&json).unwrap();
        assert_eq!(back.counter_reset_correction, 100.0);
        assert_eq!(back.sample_count, 4);
        // Bytes
        let bytes = acc.serialize_to_bytes();
        let back_b = IncreaseAccumulator::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(back_b.counter_reset_correction, 100.0);
        assert_eq!(back_b.sample_count, 4);
    }

    #[test]
    fn test_legacy_bytes_decode_with_defaults() {
        // A buffer that ends after the original four fields (correction + count
        // absent) must decode with defaults instead of erroring.
        let acc =
            IncreaseAccumulator::new(Measurement::new(10.0), 1000, Measurement::new(25.0), 2000);
        let mut bytes = acc.serialize_to_bytes();
        bytes.truncate(bytes.len() - 16); // drop counter_reset_correction (8) + sample_count (8)
        let back = IncreaseAccumulator::deserialize_from_bytes(&bytes).unwrap();
        assert_eq!(back.counter_reset_correction, 0.0);
        assert_eq!(back.sample_count, DEFAULT_LEGACY_SAMPLE_COUNT);
        assert_eq!(back.last_seen_measurement.value, 25.0);
    }

    #[test]
    fn test_arroyo_roundtrip_preserves_all_fields() {
        // A window with a reset so every field is non-default.
        let acc = feed(&[(1000, 10.0), (2000, 100.0), (3000, 5.0), (4000, 30.0)]);
        let bytes = acc.serialize_to_bytes_arroyo();
        let back = IncreaseAccumulator::deserialize_from_bytes_arroyo(&bytes).unwrap();
        assert_eq!(back.starting_measurement.value, 10.0);
        assert_eq!(back.starting_timestamp, 1000);
        assert_eq!(back.last_seen_measurement.value, 30.0);
        assert_eq!(back.last_seen_timestamp, 4000);
        assert_eq!(back.counter_reset_correction, 100.0);
        assert_eq!(back.sample_count, 4);
        assert_eq!(back.corrected_increase(), 120.0);
    }

    #[test]
    fn test_arroyo_legacy_4field_blob_defaults_sample_count() {
        // A legacy 4-field MessagePack blob (from before reset support) must decode
        // with sample_count defaulted so extrapolation stays well-defined.
        #[derive(serde::Serialize)]
        struct LegacyFour {
            starting_measurement: f64,
            starting_timestamp: i64,
            last_seen_measurement: f64,
            last_seen_timestamp: i64,
        }
        let bytes = rmp_serde::to_vec(&LegacyFour {
            starting_measurement: 10.0,
            starting_timestamp: 1000,
            last_seen_measurement: 40.0,
            last_seen_timestamp: 2000,
        })
        .unwrap();
        let back = IncreaseAccumulator::deserialize_from_bytes_arroyo(&bytes).unwrap();
        assert_eq!(back.counter_reset_correction, 0.0);
        assert_eq!(back.sample_count, DEFAULT_LEGACY_SAMPLE_COUNT);
        assert_eq!(back.corrected_increase(), 30.0);
    }
}
