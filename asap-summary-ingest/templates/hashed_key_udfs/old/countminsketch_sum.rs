/*
[dependencies]
rmp-serde = "1.1"
serde = { version = "1.0", features = ["derive"] }
xxhash-rust = { version = "0.8", features = ["xxh32"] }
*/
use arroyo_udf_plugin::udf;
use rmp_serde::Serializer;
use serde::{Serialize, Deserialize};
use xxhash_rust::xxh32::xxh32;

// Count-Min Sketch parameters
const WIDTH: usize = 1024;  // Number of buckets per hash function
const DEPTH: usize = 4;     // Number of hash functions

#[derive(Serialize, Deserialize, Clone)]
struct CountMinSketch {
    table: Vec<Vec<f64>>,
    width: usize,
    depth: usize,
}

impl CountMinSketch {
    fn new() -> Self {
        CountMinSketch {
            table: vec![vec![0.0; WIDTH]; DEPTH],
            width: WIDTH,
            depth: DEPTH,
        }
    }

    // Update the sketch with a key-value pair
    fn update(&mut self, key: u64, value: f64) {
        for i in 0..self.depth {
            let hash_val = xxh32(&key.to_le_bytes(), i as u32);
            let bucket = (hash_val as usize) % self.width;
            self.table[i][bucket] += value;
        }
    }
}

#[udf]
fn countminsketch_sum(keys: Vec<u64>, values: Vec<f64>) -> Option<Vec<u8>> {
    // Create a new Count-Min Sketch
    let mut countminsketch = CountMinSketch::new();

    // Iterate through the keys and values and update the sketch for each entry
    for (i, &key) in keys.iter().enumerate() {
        if i < values.len() {
            countminsketch.update(key, values[i]);
        }
    }

    let mut buf = Vec::new();
    countminsketch.serialize(&mut Serializer::new(&mut buf)).ok()?;
    Some(buf)
}
