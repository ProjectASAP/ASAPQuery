// Force legacy sketch implementations during tests so that tests that mutate the
// matrix directly or rely on dsrs behavior pass without sketchlib compatibility.
#[cfg(test)]
#[ctor::ctor]
fn init_sketch_legacy_for_tests() {
    std::env::set_var("SKETCH_CORE_CMS_IMPL", "legacy");
    std::env::set_var("SKETCH_CORE_CMWH_IMPL", "legacy");
    std::env::set_var("SKETCH_CORE_KLL_IMPL", "legacy");
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
