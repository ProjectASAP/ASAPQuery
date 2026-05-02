use asap_sketchlib::{CountMin, DataInput, RegularPath, Vector2D};

/// Concrete Count-Min type from asap_sketchlib when sketchlib backend is enabled.
/// Uses f64 counters (Vector2D<f64>) for weighted updates without integer rounding.
pub type SketchlibCms = CountMin<Vector2D<f64>, RegularPath>;

/// Creates a fresh sketchlib Count-Min sketch with the given dimensions.
pub fn new_sketchlib_cms(row_num: usize, col_num: usize) -> SketchlibCms {
    SketchlibCms::with_dimensions(row_num, col_num)
}

/// Builds a sketchlib Count-Min sketch from an existing `sketch` matrix.
pub fn sketchlib_cms_from_matrix(
    row_num: usize,
    col_num: usize,
    sketch: &[Vec<f64>],
) -> SketchlibCms {
    let matrix = Vector2D::from_fn(row_num, col_num, |r, c| {
        sketch
            .get(r)
            .and_then(|row| row.get(c))
            .copied()
            .unwrap_or(0.0)
    });
    SketchlibCms::from_storage(matrix)
}

/// Converts a sketchlib Count-Min sketch into the legacy `Vec<Vec<f64>>` matrix.
pub fn matrix_from_sketchlib_cms(inner: &SketchlibCms) -> Vec<Vec<f64>> {
    let storage: &Vector2D<f64> = inner.as_storage();
    let rows = storage.rows();
    let cols = storage.cols();
    let mut sketch = vec![vec![0.0; cols]; rows];

    for (r, row) in sketch.iter_mut().enumerate().take(rows) {
        for (c, cell) in row.iter_mut().enumerate().take(cols) {
            if let Some(v) = storage.get(r, c) {
                *cell = *v;
            }
        }
    }

    sketch
}

/// Helper to update a sketchlib Count-Min with a weighted key.
pub fn sketchlib_cms_update(inner: &mut SketchlibCms, key: &str, value: f64) {
    if value <= 0.0 {
        return;
    }
    let input = DataInput::String(key.to_owned());
    inner.insert_many(&input, value);
}

/// Helper to query a sketchlib Count-Min for a key, returning f64.
pub fn sketchlib_cms_query(inner: &SketchlibCms, key: &str) -> f64 {
    let input = DataInput::String(key.to_owned());
    inner.estimate(&input)
}
