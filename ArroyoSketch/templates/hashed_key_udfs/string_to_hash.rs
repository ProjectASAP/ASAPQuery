/*
[dependencies]
twox-hash = "2.1.0"
*/

use arroyo_udf_plugin::udf;
use twox_hash::XxHash32;

#[udf]
fn string_to_hash(input: &str, seed: u32) -> u32 {
    //let mut hasher = XxHash32::with_seed(seed);
    //hasher.write(input.as_bytes());
    //hasher.finish() as u32
    XxHash32::oneshot(seed, input.as_bytes())
}
