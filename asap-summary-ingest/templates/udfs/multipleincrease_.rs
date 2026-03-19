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
}

#[udf]
fn multipleincrease_(keys: Vec<&str>, values: Vec<f64>, timestamps: Vec<i64>) -> Option<Vec<u8>> {
    // Create a new hashmap to store measurement data with timestamps
    let mut per_key_storage: HashMap<String, MeasurementData> = HashMap::new();

    // Iterate through the keys, values, and timestamps
    for (i, &key) in keys.iter().enumerate() {
        if i < values.len() && i < timestamps.len() {
            let value = values[i];
            let timestamp = timestamps[i];

            let entry = per_key_storage.entry(key.to_string()).or_insert(MeasurementData {
                starting_measurement: value,
                starting_timestamp: timestamp,
                last_seen_measurement: value,
                last_seen_timestamp: timestamp,
            });

            // Update last seen measurement and timestamp
            entry.last_seen_measurement = value;
            entry.last_seen_timestamp = timestamp;

            // If this timestamp is earlier than our current starting timestamp, update starting values
            //if timestamp < entry.starting_timestamp {
            //    entry.starting_measurement = value;
            //    entry.starting_timestamp = timestamp;
            //}
        }
    }

    let mut buf = Vec::new();
    per_key_storage
        .serialize(&mut Serializer::new(&mut buf))
        .ok()?;
    Some(buf)
}
