/*
[dependencies]
ahash = "0.8.6"
*/

use arroyo_udf_plugin::udf;
use ahash::AHasher;
use std::hash::{Hash, Hasher};
use xxhash_rust::xxh32::xxh32;

#[udf]
fn string_to_hash(input: &str) -> u64 {
    let mut hasher = AHasher::default();
    input.hash(&mut hasher);
    hasher.finish()
}
