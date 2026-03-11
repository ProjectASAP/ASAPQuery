pub mod ingest;
pub mod query;

// Re-export commonly used types for convenience
pub use ingest::{KafkaConsumer, KafkaConsumerConfig, OtlpReceiver, OtlpReceiverConfig};
pub use query::{AdapterConfig, HttpServer, HttpServerConfig};
