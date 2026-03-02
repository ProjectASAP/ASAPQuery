/*
[dependencies]
rmp-serde = "1.1"
serde = { version = "1.0", features = ["derive"] }
*/

use arroyo_udf_plugin::udf;
use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Serialize, Deserialize)]
struct StringSet {
    values: HashSet<String>,
}

#[udf]
fn setaggregator_(strings: Vec<&str>) -> Option<Vec<u8>> {
    // Return None if input is empty
    if strings.is_empty() {
        return None;
    }

    // Create a HashSet and collect all unique strings
    let mut unique_strings = HashSet::new();
    for s in strings {
        unique_strings.insert(s.to_string());
    }

    // Wrap in a serializable struct
    let string_set = StringSet {
        values: unique_strings,
    };

    let mut buf = Vec::new();
    string_set.serialize(&mut Serializer::new(&mut buf)).ok()?;
    Some(buf)
}
