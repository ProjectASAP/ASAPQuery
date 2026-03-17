use std::sync::OnceLock;

/// Implementation mode for sketch-core internals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ImplMode {
    /// Use the original hand-written implementations.
    Legacy,
    /// Use sketchlib-rust backed implementations.
    Sketchlib,
}

static COUNTMIN_MODE: OnceLock<ImplMode> = OnceLock::new();

/// Returns true if Count-Min operations should use sketchlib-rust internally.
pub fn use_sketchlib_for_count_min() -> bool {
    *COUNTMIN_MODE.get_or_init(|| ImplMode::Sketchlib) == ImplMode::Sketchlib
}

static KLL_MODE: OnceLock<ImplMode> = OnceLock::new();

/// Returns true if KLL operations should use sketchlib-rust internally.
pub fn use_sketchlib_for_kll() -> bool {
    *KLL_MODE.get_or_init(|| ImplMode::Sketchlib) == ImplMode::Sketchlib
}

static COUNTMIN_WITH_HEAP_MODE: OnceLock<ImplMode> = OnceLock::new();

/// Returns true if Count-Min-With-Heap operations should use sketchlib-rust internally for the
/// Count-Min portion.
pub fn use_sketchlib_for_count_min_with_heap() -> bool {
    *COUNTMIN_WITH_HEAP_MODE.get_or_init(|| ImplMode::Sketchlib) == ImplMode::Sketchlib
}

/// Set backend modes for all sketch types. Call once at process startup,
/// before any sketch operation. Returns Err if any OnceLock was already set.
pub fn configure(cms: ImplMode, kll: ImplMode, cmwh: ImplMode) -> Result<(), &'static str> {
    let a = COUNTMIN_MODE.set(cms);
    let b = KLL_MODE.set(kll);
    let c = COUNTMIN_WITH_HEAP_MODE.set(cmwh);
    if a.is_err() || b.is_err() || c.is_err() {
        Err("configure() called after sketch backends were already initialised")
    } else {
        Ok(())
    }
}

pub fn force_legacy_mode_for_tests() {
    let _ = COUNTMIN_MODE.set(ImplMode::Legacy);
    let _ = KLL_MODE.set(ImplMode::Legacy);
    let _ = COUNTMIN_WITH_HEAP_MODE.set(ImplMode::Legacy);
}

/// Helper used by UDF templates and documentation examples to parse implementation mode
/// from environment variables in a robust way. This is not used in the hot path.
pub fn parse_mode(var: Result<String, std::env::VarError>) -> ImplMode {
    match var {
        Ok(v) => match v.to_ascii_lowercase().as_str() {
            "legacy" => ImplMode::Legacy,
            "sketchlib" => ImplMode::Sketchlib,
            other => {
                eprintln!(
                    "sketch-core: unrecognised IMPL value {other:?}, defaulting to Sketchlib"
                );
                ImplMode::Sketchlib
            }
        },
        Err(std::env::VarError::NotPresent) => ImplMode::Sketchlib,
        Err(std::env::VarError::NotUnicode(v)) => {
            eprintln!(
                "sketch-core: IMPL env var has invalid UTF-8 ({v:?}), defaulting to Sketchlib"
            );
            ImplMode::Sketchlib
        }
    }
}
