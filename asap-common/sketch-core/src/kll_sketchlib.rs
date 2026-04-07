use asap_sketchlib::{SketchInput, KLL};

/// Concrete KLL type from asap_sketchlib when sketchlib backend is enabled.
pub type SketchlibKll = KLL;

/// Creates a fresh sketchlib KLL sketch with the requested accuracy parameter `k`.
pub fn new_sketchlib_kll(k: u16) -> SketchlibKll {
    KLL::init_kll(k as i32)
}

/// Updates a sketchlib KLL with one numeric observation.
pub fn sketchlib_kll_update(inner: &mut SketchlibKll, value: f64) {
    // KLL accepts only numeric inputs. We intentionally ignore the error here because `value`
    // is always numeric.
    let _ = inner.update(&SketchInput::F64(value));
}

/// Queries a sketchlib KLL for the value at the requested quantile.
pub fn sketchlib_kll_quantile(inner: &SketchlibKll, q: f64) -> f64 {
    inner.quantile(q)
}

/// Merges `src` into `dst`.
pub fn sketchlib_kll_merge(dst: &mut SketchlibKll, src: &SketchlibKll) {
    dst.merge(src);
}

/// Serializes a sketchlib KLL into MessagePack bytes.
pub fn bytes_from_sketchlib_kll(inner: &SketchlibKll) -> Vec<u8> {
    inner.serialize_to_bytes().unwrap()
}

/// Deserializes a sketchlib KLL from MessagePack bytes.
pub fn sketchlib_kll_from_bytes(bytes: &[u8]) -> Result<SketchlibKll, Box<dyn std::error::Error>> {
    Ok(KLL::deserialize_from_bytes(bytes)?)
}
