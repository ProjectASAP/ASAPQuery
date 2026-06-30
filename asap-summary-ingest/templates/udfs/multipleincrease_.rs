/*
[dependencies]
rmp-serde = "1.1"
serde = { version = "1.0", features = ["derive"] }
*/

use arroyo_udf_plugin::udf;
use rmp_serde::Serializer;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize)]
struct MeasurementData {
    starting_measurement: f64,
    starting_timestamp: i64,
    last_seen_measurement: f64,
    last_seen_timestamp: i64,
    // Prometheus `counterCorrection`: sum of pre-reset values within the window.
    // Field order MUST match IncreaseAccumulator's MeasurementData on the query
    // side (rmp-serde encodes structs positionally).
    counter_reset_correction: f64,
    // Number of samples folded into this window (for extrapolation).
    sample_count: u64,
}

#[udf]
fn multipleincrease_(keys: Vec<&str>, values: Vec<f64>, timestamps: Vec<i64>) -> Option<Vec<u8>> {
    // Group all (timestamp, value) samples per key first, because counter-reset
    // detection requires processing them in timestamp order and the input vectors
    // are not guaranteed to be ordered.
    let mut per_key_samples: HashMap<String, Vec<(i64, f64)>> = HashMap::new();
    for (i, &key) in keys.iter().enumerate() {
        if i < values.len() && i < timestamps.len() {
            per_key_samples
                .entry(key.to_string())
                .or_default()
                .push((timestamps[i], values[i]));
        }
    }

    let mut per_key_storage: HashMap<String, MeasurementData> = HashMap::new();
    for (key, mut samples) in per_key_samples {
        samples.sort_by_key(|&(ts, _)| ts);

        let (first_ts, first_val) = samples[0];
        let mut entry = MeasurementData {
            starting_measurement: first_val,
            starting_timestamp: first_ts,
            last_seen_measurement: first_val,
            last_seen_timestamp: first_ts,
            counter_reset_correction: 0.0,
            sample_count: 1,
        };

        for &(ts, value) in &samples[1..] {
            // A drop below the previous value is a counter reset; add the
            // pre-reset value back so the increase stays monotonic.
            if value < entry.last_seen_measurement {
                entry.counter_reset_correction += entry.last_seen_measurement;
            }
            entry.last_seen_measurement = value;
            entry.last_seen_timestamp = ts;
            entry.sample_count += 1;
        }

        per_key_storage.insert(key, entry);
    }

    let mut buf = Vec::new();
    per_key_storage
        .serialize(&mut Serializer::new(&mut buf))
        .ok()?;
    Some(buf)
}
