pub mod ingest;
pub mod query;

// Re-export commonly used types for convenience
pub use ingest::{KafkaConsumer, KafkaConsumerConfig, OtelIngestConfig, OtelIngestServer};
pub use query::{AdapterConfig, HttpServer, HttpServerConfig};
