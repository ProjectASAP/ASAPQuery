use sketchlib_rust::{CountMin, RegularPath, SketchInput, Vector2D};

/// Concrete Count-Min type from sketchlib-rust when sketchlib backend is enabled.
pub type SketchlibCms = CountMin<Vector2D<i64>, RegularPath>;

/// Creates a fresh sketchlib Count-Min sketch with the given dimensions.
pub fn new_sketchlib_cms(row_num: usize, col_num: usize) -> SketchlibCms {
    CountMin::with_dimensions(row_num, col_num)
}

/// Builds a sketchlib Count-Min sketch from an existing `sketch` matrix.
pub fn sketchlib_cms_from_matrix(
    row_num: usize,
    col_num: usize,
    sketch: &[Vec<f64>],
) -> SketchlibCms {
    let matrix = Vector2D::from_fn(row_num, col_num, |r, c| {
        // Values are stored as f64 in the wire format; treat them as integer counts.
        sketch
            .get(r)
            .and_then(|row| row.get(c))
            .copied()
            .unwrap_or(0.0)
            .round() as i64
    });
    CountMin::from_storage(matrix)
}

/// Converts a sketchlib Count-Min sketch into the legacy `Vec<Vec<f64>>` matrix.
pub fn matrix_from_sketchlib_cms(inner: &SketchlibCms) -> Vec<Vec<f64>> {
    let storage: &Vector2D<i64> = inner.as_storage();
    let rows = storage.rows();
    let cols = storage.cols();
    let mut sketch = vec![vec![0.0; cols]; rows];

    for r in 0..rows {
        for c in 0..cols {
            if let Some(v) = storage.get(r, c) {
                sketch[r][c] = *v as f64;
            }
        }
    }

    sketch
}

/// Helper to update a sketchlib Count-Min with a weighted key.
pub fn sketchlib_cms_update(inner: &mut SketchlibCms, key: &str, value: f64) {
    // Values arrive as `f64` (wire-format compatibility). The sketchlib Count-Min uses integer
    // counters, so we round to the nearest `i64` count. Non-positive values become no-ops.
    let many = value.round() as i64;
    if many <= 0 {
        return;
    }
    let input = SketchInput::String(key.to_owned());
    inner.insert_many(&input, many);
}

/// Helper to query a sketchlib Count-Min for a key, returning f64.
pub fn sketchlib_cms_query(inner: &SketchlibCms, key: &str) -> f64 {
    let input = SketchInput::String(key.to_owned());
    let est = inner.estimate(&input);
    est as f64
}
