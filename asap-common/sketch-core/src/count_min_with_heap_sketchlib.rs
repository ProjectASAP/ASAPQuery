//! asap_sketchlib CMSHeap integration for CountMinSketchWithHeap.
//!
//! Uses CMSHeap (CountMin + HHHeap) from asap_sketchlib instead of CountMin + local heap,
//! providing automatic top-k tracking during insert and merge.

use asap_sketchlib::RegularPath;
use asap_sketchlib::{CMSHeap, DataInput, Vector2D};

/// Wire-format heap item (key, value) to avoid circular dependency with count_min_with_heap.
pub struct WireHeapItem {
    pub key: String,
    pub value: f64,
}

/// Concrete Count-Min-with-Heap type from asap_sketchlib (CMS + HHHeap).
pub type SketchlibCMSHeap = CMSHeap<Vector2D<i64>, RegularPath>;

/// Creates a fresh CMSHeap with the given dimensions and heap capacity.
pub fn new_sketchlib_cms_heap(
    row_num: usize,
    col_num: usize,
    heap_size: usize,
) -> SketchlibCMSHeap {
    CMSHeap::new(row_num, col_num, heap_size)
}

/// Builds a CMSHeap from an existing sketch matrix and optional heap items.
/// Used when deserializing or when ensuring sketchlib from legacy state.
pub fn sketchlib_cms_heap_from_matrix_and_heap(
    row_num: usize,
    col_num: usize,
    heap_size: usize,
    sketch: &[Vec<f64>],
    topk_heap: &[WireHeapItem],
) -> SketchlibCMSHeap {
    let matrix = Vector2D::from_fn(row_num, col_num, |r, c| {
        sketch
            .get(r)
            .and_then(|row| row.get(c))
            .copied()
            .unwrap_or(0.0)
            .round() as i64
    });
    let mut cms_heap = CMSHeap::from_storage(matrix, heap_size);

    // Populate the heap from wire-format topk_heap
    for item in topk_heap {
        let count = item.value.round() as i64;
        if count > 0 {
            let input = DataInput::Str(&item.key);
            cms_heap.heap_mut().update(&input, count);
        }
    }

    cms_heap
}

/// Converts a CMSHeap's storage into the legacy `Vec<Vec<f64>>` matrix.
pub fn matrix_from_sketchlib_cms_heap(cms_heap: &SketchlibCMSHeap) -> Vec<Vec<f64>> {
    let storage = cms_heap.cms().as_storage();
    let rows = storage.rows();
    let cols = storage.cols();
    let mut sketch = vec![vec![0.0; cols]; rows];

    for (r, row) in sketch.iter_mut().enumerate().take(rows) {
        for (c, cell) in row.iter_mut().enumerate().take(cols) {
            if let Some(v) = storage.get(r, c) {
                *cell = *v as f64;
            }
        }
    }

    sketch
}

/// Converts sketchlib HHHeap items to wire-format (key, value) pairs.
pub fn heap_to_wire(cms_heap: &SketchlibCMSHeap) -> Vec<WireHeapItem> {
    cms_heap
        .heap()
        .heap()
        .iter()
        .map(|hh_item| {
            let key = match &hh_item.key {
                asap_sketchlib::HeapItem::String(s) => s.clone(),
                other => format!("{:?}", other),
            };
            WireHeapItem {
                key,
                value: hh_item.count as f64,
            }
        })
        .collect()
}

/// Updates a CMSHeap with a weighted key. Automatically updates the heap.
pub fn sketchlib_cms_heap_update(cms_heap: &mut SketchlibCMSHeap, key: &str, value: f64) {
    let many = value.round() as i64;
    if many <= 0 {
        return;
    }
    let input = DataInput::String(key.to_owned());
    cms_heap.insert_many(&input, many);
}

/// Queries a CMSHeap for a key's frequency estimate.
pub fn sketchlib_cms_heap_query(cms_heap: &SketchlibCMSHeap, key: &str) -> f64 {
    let input = DataInput::String(key.to_owned());
    cms_heap.estimate(&input) as f64
}
