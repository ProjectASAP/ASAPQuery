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
fn multipleminmax_max(keys: Vec<&str>, values: Vec<f64>) -> Option<Vec<u8>> {
    // Create a new hashmap
    let mut per_key_storage: HashMap<String, f64> = HashMap::new();

    // Iterate through the keys and values
    for (i, &key) in keys.iter().enumerate() {
        if i < values.len() {
            // If the key is not present or the value is less than the current stored value, update it
            per_key_storage
                .entry(key.to_string())
                .and_modify(|v| *v = (*v).max(values[i]))
                .or_insert(values[i]);
        }
    }

    let mut buf = Vec::new();
    per_key_storage
        .serialize(&mut Serializer::new(&mut buf))
        .ok()?;
    Some(buf)
}
