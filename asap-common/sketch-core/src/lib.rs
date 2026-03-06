// Force legacy sketch implementations during tests so that tests that mutate the
// matrix directly or rely on legacy behavior pass.
#[cfg(test)]
#[ctor::ctor]
fn init_sketch_legacy_for_tests() {
    std::env::set_var("SKETCH_CORE_CMS_IMPL", "legacy");
    std::env::set_var("SKETCH_CORE_CMWH_IMPL", "legacy");
    std::env::set_var("SKETCH_CORE_KLL_IMPL", "legacy");
}

pub mod config;
pub mod count_min;
pub mod count_min_sketchlib;
pub mod count_min_with_heap;
pub mod count_min_with_heap_sketchlib;
pub mod delta_set_aggregator;
pub mod hydra_kll;
pub mod kll;
pub mod kll_sketchlib;
pub mod set_aggregator;
