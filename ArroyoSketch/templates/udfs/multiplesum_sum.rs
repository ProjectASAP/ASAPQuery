/*
[dependencies]
rmp-serde = "1.1"
serde = { version = "1.0", features = ["derive"] }
*/

use arroyo_udf_plugin::udf;
use rmp_serde::Serializer;
use serde::Serialize;
use std::collections::HashMap;

#[udf]
fn multiplesum_sum(keys: Vec<&str>, values: Vec<f64>) -> Option<Vec<u8>> {
    // Create a new hashmap to store the sum of values for each key
    let mut key_sums: HashMap<String, f64> = HashMap::new();

    // Iterate through the keys and values and update the sum for each key
    for (i, &key) in keys.iter().enumerate() {
        if i < values.len() {
            *key_sums.entry(key.to_string()).or_insert(0.0) += values[i];
        }
    }

    let mut buf = Vec::new();
    key_sums.serialize(&mut Serializer::new(&mut buf)).ok()?;
    Some(buf)
}
