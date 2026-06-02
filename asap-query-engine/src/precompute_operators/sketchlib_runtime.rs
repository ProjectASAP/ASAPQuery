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
    // Count-min sketches model non-negative frequencies: every cell is a
    // monotonically increasing counter and `estimate` returns the row-wise
    // minimum. A zero update is a no-op, and a negative update would corrupt
    // the estimate for `key` *and* for every other key colliding in any of
    // its rows (collisions only ever inflate, never deflate, an estimate).
    // The accumulator contract upstream only ever feeds counts/durations,
    // which are >= 0, so dropping `value <= 0.0` preserves the invariant
    // rather than changing intended behavior. See the regression test
    // `cms_update_drops_non_positive_values`.
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
    for (r, row) in out.iter_mut().enumerate() {
        for (c, cell) in row.iter_mut().enumerate() {
            // `storage.get` only returns `None` for out-of-bounds access; we
            // iterate within the dimensions storage just reported, so this is
            // a programmer error if it ever fires.
            *cell = *storage
                .get(r, c)
                .expect("cms_matrix indexed within reported storage dimensions");
        }
    }
    out
}

/// Build a CountMin from an existing matrix (used by JSON / legacy
/// byte-format decoders).
///
/// `rows`/`cols` arrive from the envelope while `matrix` is parsed
/// separately, so a malformed payload can disagree. We reject the
/// mismatch instead of silently padding/truncating with `0.0`, which
/// would be invisible corruption.
// Note on error types in this module: the `*_merge_refs` helpers return
// `Box<dyn Error + Send + Sync>` because their callers feed into
// `AggregateCore::merge_with` (and friends), whose trait signature requires
// the `Send + Sync` bound. The `*_from_*` decoders return plain
// `Box<dyn Error>` because the `deserialize_from_*` methods on the
// accumulators (and their callers across the crate) use that shape and
// bumping the bound would ripple through every accumulator's API.
pub fn cms_from_matrix(
    matrix: Vec<Vec<f64>>,
    rows: usize,
    cols: usize,
) -> Result<RuntimeCountMin, Box<dyn std::error::Error>> {
    if matrix.len() != rows {
        return Err(format!(
            "CountMin matrix shape mismatch: envelope declares {rows} rows, matrix has {}",
            matrix.len()
        )
        .into());
    }
    if let Some(bad) = matrix.iter().position(|row| row.len() != cols) {
        return Err(format!(
            "CountMin matrix shape mismatch: envelope declares {cols} cols, row {bad} has {}",
            matrix[bad].len()
        )
        .into());
    }
    let storage = Vector2D::from_fn(rows, cols, |r, c| matrix[r][c]);
    Ok(CountMin::from_storage(storage))
}

/// Serialize to the Go-compatible MessagePack envelope.
pub fn cms_to_msgpack(sk: &RuntimeCountMin) -> Vec<u8> {
    let wire = CountMinSketchWire {
        sketch: cms_matrix(sk),
        rows: sk.rows(),
        cols: sk.cols(),
    };
    // A `Vec<Vec<f64>>` + two `usize`s has no unrepresentable state, so
    // failure here is a bug, not bad input. Panic loudly rather than emit
    // empty bytes that surface downstream as a misleading "buffer too short".
    rmp_serde::to_vec(&wire).expect("CountMinSketchWire msgpack serialization is infallible")
}

/// Deserialize from the Go-compatible MessagePack envelope.
pub fn cms_from_msgpack(bytes: &[u8]) -> Result<RuntimeCountMin, Box<dyn std::error::Error>> {
    let wire: CountMinSketchWire = rmp_serde::from_slice(bytes)?;
    cms_from_matrix(wire.sketch, wire.rows, wire.cols)
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
///
/// `serialize_to_bytes` failure is a bug, not bad input: a `KLL<f64>`
/// holding only finite samples has no unrepresentable state. Panic
/// loudly rather than emit empty bytes that surface downstream as a
/// misleading "buffer too short".
pub fn kll_sketch_bytes(sk: &RuntimeKll) -> Vec<u8> {
    sk.serialize_to_bytes()
        .expect("KLL<f64> serialize_to_bytes is infallible for finite samples")
}

/// Serialize to the Go-compatible `KllSketchData { k, sketch_bytes }`
/// MessagePack envelope.
pub fn kll_to_msgpack(sk: &RuntimeKll) -> Vec<u8> {
    let wire = KllSketchData {
        k: sk.k() as u16,
        sketch_bytes: kll_sketch_bytes(sk),
    };
    // Same reasoning as `cms_to_msgpack`: the wire struct has no
    // unrepresentable state, so failure is a bug.
    rmp_serde::to_vec(&wire).expect("KllSketchData msgpack serialization is infallible")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cms_update_drops_non_positive_values() {
        // The `value <= 0.0` guard in `cms_update` must be a no-op: a
        // count-min sketch only ever accumulates non-negative frequencies,
        // so zero and negative updates leave the estimate untouched.
        let mut sk = cms_new(4, 1000);
        cms_update(&mut sk, "k", 0.0);
        cms_update(&mut sk, "k", -5.0);
        assert_eq!(cms_estimate(&sk, "k"), 0.0);

        // A positive update is still recorded after the dropped ones.
        cms_update(&mut sk, "k", 3.0);
        assert_eq!(cms_estimate(&sk, "k"), 3.0);
    }

    #[test]
    fn kll_merge_refs_rejects_k_mismatch() {
        let a = kll_new(200);
        let b = kll_new(100);
        let err = kll_merge_refs(&[&a, &b])
            .expect_err("merging KLL sketches with different k must error");
        assert!(
            err.to_string().contains("KLL k mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn kll_merge_refs_rejects_empty_input() {
        let err = kll_merge_refs(&[]).expect_err("empty merge input must error");
        assert!(err.to_string().contains("empty input"), "unexpected error: {err}");
    }
}
