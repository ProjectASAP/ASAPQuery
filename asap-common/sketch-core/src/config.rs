use std::sync::OnceLock;

/// Implementation mode for sketch-core internals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImplMode {
    /// Use the original hand-written implementations.
    Legacy,
    /// Use sketchlib-rust backed implementations.
    Sketchlib,
}

fn parse_mode(var: Result<String, std::env::VarError>) -> ImplMode {
    match var {
        Ok(v) => match v.to_ascii_lowercase().as_str() {
            "legacy" => ImplMode::Legacy,
            _ => ImplMode::Sketchlib,
        },
        Err(_) => ImplMode::Sketchlib,
    }
}

static COUNTMIN_MODE: OnceLock<ImplMode> = OnceLock::new();

/// Returns true if Count-Min operations should use sketchlib-rust internally.
pub fn use_sketchlib_for_count_min() -> bool {
    *COUNTMIN_MODE
        .get_or_init(|| parse_mode(std::env::var("SKETCH_CORE_CMS_IMPL")))
        == ImplMode::Sketchlib
}

static KLL_MODE: OnceLock<ImplMode> = OnceLock::new();

/// Returns true if KLL operations should use sketchlib-rust internally.
pub fn use_sketchlib_for_kll() -> bool {
    *KLL_MODE
        .get_or_init(|| parse_mode(std::env::var("SKETCH_CORE_KLL_IMPL")))
        == ImplMode::Sketchlib
}

static COUNTMIN_WITH_HEAP_MODE: OnceLock<ImplMode> = OnceLock::new();

/// Returns true if Count-Min-With-Heap operations should use sketchlib-rust internally for the
/// Count-Min portion.
pub fn use_sketchlib_for_count_min_with_heap() -> bool {
    *COUNTMIN_WITH_HEAP_MODE
        .get_or_init(|| parse_mode(std::env::var("SKETCH_CORE_CMWH_IMPL")))
        == ImplMode::Sketchlib
}
