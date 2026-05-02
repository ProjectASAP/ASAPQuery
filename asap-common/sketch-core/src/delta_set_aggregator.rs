// Adapted from QueryEngineRust/src/precompute_operators/delta_set_aggregator_accumulator.rs
// Changes:
//   - Only the wire format (DeltaResult) and its serialize/deserialize functions are extracted.
//   - The Arroyo side uses lazy_static for stateful window tracking — that streaming logic
//     stays in the Arroyo template and does NOT belong in sketch-core.
//   - DeltaResult made pub (was private inline struct in QE).
//   - serialize_msgpack / deserialize_msgpack are module-level free functions
//     (not methods on DeltaSetAggregatorAccumulator, which stays in QE).
//   - Removed: all QE accumulator struct/impls (stay in QE)

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Wire format for the delta set aggregator — shared between Arroyo and QueryEngineRust.
/// Both sides agree on `{ added: HashSet<String>, removed: HashSet<String> }` in msgpack.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeltaResult {
    pub added: HashSet<String>,
    pub removed: HashSet<String>,
}

/// Serialize a delta result to MessagePack.
pub fn serialize_msgpack(added: &HashSet<String>, removed: &HashSet<String>) -> Vec<u8> {
    let result = DeltaResult {
        added: added.clone(),
        removed: removed.clone(),
    };
    let mut buf = Vec::new();
    rmp_serde::encode::write(&mut buf, &result).unwrap();
    buf
}

/// Deserialize a delta result from MessagePack produced by the Arroyo UDF.
pub fn deserialize_msgpack(buffer: &[u8]) -> Result<DeltaResult, Box<dyn std::error::Error>> {
    rmp_serde::from_slice(buffer)
        .map_err(|e| format!("Failed to deserialize DeltaResult from MessagePack: {e}").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_msgpack_round_trip() {
        let mut added = HashSet::new();
        added.insert("web".to_string());
        added.insert("api".to_string());

        let mut removed = HashSet::new();
        removed.insert("db".to_string());

        let bytes = serialize_msgpack(&added, &removed);
        let result = deserialize_msgpack(&bytes).unwrap();

        assert_eq!(result.added.len(), 2);
        assert!(result.added.contains("web"));
        assert!(result.added.contains("api"));
        assert_eq!(result.removed.len(), 1);
        assert!(result.removed.contains("db"));
    }

    #[test]
    fn test_empty_sets() {
        let added = HashSet::new();
        let removed = HashSet::new();
        let bytes = serialize_msgpack(&added, &removed);
        let result = deserialize_msgpack(&bytes).unwrap();
        assert!(result.added.is_empty());
        assert!(result.removed.is_empty());
    }
}
