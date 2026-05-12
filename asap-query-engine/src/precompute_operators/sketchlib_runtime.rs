//! Thin runtime adapters over `asap_sketchlib::sketches::*`.
//!
//! ASAPQuery accumulators hold the pure-Rust runtime sketch types
//! directly (`sketches::CountMin<Vector2D<f64>, FastPath, DefaultXxHasher>`,
//! `sketches::KLL<f64>`) and reach for these helpers to translate the
//! accumulator surface (string keys, `Vec<Vec<f64>>` matrices,
//! Go-compatible msgpack envelopes) onto those underlying types.
//!
//! Cross-language byte parity for the underlying `sketches::*` paths
//! is locked in by
//! `asap_sketchlib::tests::sketches_go_parity_probe`.

use asap_sketchlib::message_pack_format::portable::countminsketch::CountMinSketchWire;
use asap_sketchlib::message_pack_format::portable::kll::KllSketchData;
use asap_sketchlib::sketches::countminsketch::CountMin;
use asap_sketchlib::sketches::kll::KLL;
use asap_sketchlib::{DataInput, DefaultXxHasher, FastPath, Vector2D};

// =============================================================================
// CountMinSketch — sketches::CountMin<Vector2D<f64>, FastPath, DefaultXxHasher>
// =============================================================================

/// Concrete runtime CMS type used by `CountMinSketchAccumulator`. Same
/// dimensions + hasher choice as `asap_sketchlib`'s wire-format
/// `CountMinSketch` facade, so the on-the-wire byte shape is identical.
pub type RuntimeCountMin = CountMin<Vector2D<f64>, FastPath, DefaultXxHasher>;

pub fn cms_new(rows: usize, cols: usize) -> RuntimeCountMin {
    CountMin::with_dimensions(rows, cols)
}

pub fn cms_update(sk: &mut RuntimeCountMin, key: &str, value: f64) {
    if value <= 0.0 {
        return;
    }
    sk.insert_many(&DataInput::String(key.to_owned()), value);
}

pub fn cms_estimate(sk: &RuntimeCountMin, key: &str) -> f64 {
    sk.estimate(&DataInput::String(key.to_owned()))
}

/// Snapshot the storage as `Vec<Vec<f64>>` (used for JSON output + wire DTO).
pub fn cms_matrix(sk: &RuntimeCountMin) -> Vec<Vec<f64>> {
    let storage = sk.as_storage();
    let rows = storage.rows();
    let cols = storage.cols();
    let mut out = vec![vec![0.0f64; cols]; rows];
    for r in 0..rows {
        for c in 0..cols {
            if let Some(v) = storage.get(r, c) {
                out[r][c] = *v;
            }
        }
    }
    out
}

/// Build a CountMin from an existing matrix (used by JSON / legacy
/// byte-format decoders).
pub fn cms_from_matrix(matrix: Vec<Vec<f64>>, rows: usize, cols: usize) -> RuntimeCountMin {
    let storage = Vector2D::from_fn(rows, cols, |r, c| {
        matrix
            .get(r)
            .and_then(|row| row.get(c))
            .copied()
            .unwrap_or(0.0)
    });
    CountMin::from_storage(storage)
}

/// Serialize to the Go-compatible MessagePack envelope.
pub fn cms_to_msgpack(sk: &RuntimeCountMin) -> Vec<u8> {
    let wire = CountMinSketchWire {
        sketch: cms_matrix(sk),
        rows: sk.rows(),
        cols: sk.cols(),
    };
    rmp_serde::to_vec(&wire).unwrap_or_default()
}

/// Deserialize from the Go-compatible MessagePack envelope.
pub fn cms_from_msgpack(bytes: &[u8]) -> Result<RuntimeCountMin, Box<dyn std::error::Error>> {
    let wire: CountMinSketchWire = rmp_serde::from_slice(bytes)?;
    Ok(cms_from_matrix(wire.sketch, wire.rows, wire.cols))
}

/// Merge a slice of CMS references into a single new sketch.
pub fn cms_merge_refs(
    sketches: &[&RuntimeCountMin],
) -> Result<RuntimeCountMin, Box<dyn std::error::Error + Send + Sync>> {
    let first = *sketches
        .first()
        .ok_or("cms_merge_refs called with empty input")?;
    let rows = first.rows();
    let cols = first.cols();
    for s in sketches {
        if s.rows() != rows || s.cols() != cols {
            return Err(format!(
                "CountMin dimension mismatch in merge: expected {rows}x{cols}, got {}x{}",
                s.rows(),
                s.cols()
            )
            .into());
        }
    }
    let mut merged = cms_new(rows, cols);
    for s in sketches {
        merged.merge(s);
    }
    Ok(merged)
}

// =============================================================================
// KllSketch — sketches::KLL<f64>
// =============================================================================

/// Concrete runtime KLL type used by `DatasketchesKLLAccumulator`.
pub type RuntimeKll = KLL<f64>;

pub fn kll_new(k: u16) -> RuntimeKll {
    KLL::init_kll(k as i32)
}

pub fn kll_update(sk: &mut RuntimeKll, value: f64) {
    sk.update(&value);
}

pub fn kll_quantile(sk: &RuntimeKll, q: f64) -> f64 {
    if sk.count() == 0 {
        return 0.0;
    }
    sk.quantile(q)
}

/// Raw msgpack bytes of the KLL backend (sans the `k`-envelope outer
/// wrapper). Used by JSON output (base64-encoded) and the wire codec.
pub fn kll_sketch_bytes(sk: &RuntimeKll) -> Vec<u8> {
    sk.serialize_to_bytes().unwrap_or_default()
}

/// Serialize to the Go-compatible `KllSketchData { k, sketch_bytes }`
/// MessagePack envelope.
pub fn kll_to_msgpack(sk: &RuntimeKll) -> Vec<u8> {
    let wire = KllSketchData {
        k: sk.k() as u16,
        sketch_bytes: kll_sketch_bytes(sk),
    };
    rmp_serde::to_vec(&wire).unwrap_or_default()
}

/// Deserialize from the Go-compatible `KllSketchData` envelope.
pub fn kll_from_msgpack(bytes: &[u8]) -> Result<RuntimeKll, Box<dyn std::error::Error>> {
    let wire: KllSketchData = rmp_serde::from_slice(bytes)?;
    Ok(KLL::deserialize_from_bytes(&wire.sketch_bytes)?)
}

/// Merge a slice of KLL references into a single new sketch. All
/// inputs must share the same `k`.
pub fn kll_merge_refs(
    sketches: &[&RuntimeKll],
) -> Result<RuntimeKll, Box<dyn std::error::Error + Send + Sync>> {
    let first = *sketches
        .first()
        .ok_or("kll_merge_refs called with empty input")?;
    let k = first.k();
    for s in sketches {
        if s.k() != k {
            return Err(format!("KLL k mismatch in merge: expected {k}, got {}", s.k()).into());
        }
    }
    let mut merged = kll_new(k as u16);
    for s in sketches {
        merged.merge(s);
    }
    Ok(merged)
}
