// Configure sketch-core implementations during tests.
// Use sketchlib-tests feature to choose backend: without it = Legacy, with it = Sketchlib.
// A single `cargo test -p query_engine_rust` runs both: lib tests use Legacy, then
// tests/test_both_backends.rs spawns the sketchlib run.
#[cfg(test)]
#[ctor::ctor]
fn init_sketch_backend_for_tests() {
    #[cfg(feature = "sketchlib-tests")]
    let _ = sketch_core::config::configure(
        sketch_core::config::ImplMode::Sketchlib,
        sketch_core::config::ImplMode::Sketchlib,
        sketch_core::config::ImplMode::Sketchlib,
    );
    #[cfg(not(feature = "sketchlib-tests"))]
    sketch_core::config::force_legacy_mode_for_tests();
}

pub mod data_model;
pub mod drivers;
pub mod engines;
pub mod precompute_operators;
pub mod stores;

#[cfg(test)]
pub mod tests;
pub mod utils;

// Re-export commonly used types to avoid glob import conflicts
pub use data_model::{
    AccumulatorFactory, AggregateCore, AggregationConfig, InferenceConfig, KeyByLabelValues,
    Measurement, MergeableAccumulator, MultipleSubpopulationAggregate,
    MultipleSubpopulationAggregateFactory, PrecomputedOutput, PromQLSchema, QueryConfig,
    SerializableToSink, SingleSubpopulationAggregate, SingleSubpopulationAggregateFactory,
};

pub use precompute_operators::{
    IncreaseAccumulator, MinMaxAccumulator, MultipleSumAccumulator, SumAccumulator,
};

pub use stores::{SimpleMapStore, Store, StoreResult};

pub use engines::{InstantVector, QueryResult, SimpleEngine};

pub use drivers::{HttpServer, HttpServerConfig, KafkaConsumer, KafkaConsumerConfig};

pub use utils::{normalize_spatial_filter, read_inference_config, read_streaming_config};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
