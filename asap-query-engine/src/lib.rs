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

pub use drivers::{
    HttpServer, HttpServerConfig, KafkaConsumer, KafkaConsumerConfig, OtelIngestConfig,
    OtelIngestServer,
};

pub use utils::{normalize_spatial_filter, read_inference_config, read_streaming_config};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
