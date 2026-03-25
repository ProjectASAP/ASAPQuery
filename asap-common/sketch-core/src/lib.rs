#[cfg(test)]
#[ctor::ctor]
fn init_sketch_legacy_for_tests() {
    crate::config::force_legacy_mode_for_tests();
}

pub mod config;
pub mod count_min;
pub mod count_min_with_heap;
pub mod delta_set_aggregator;
pub mod hydra_kll;
pub mod kll;
pub mod set_aggregator;
