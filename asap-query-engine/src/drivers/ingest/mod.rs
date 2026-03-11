pub mod kafka;
pub mod otel;
pub mod prometheus_remote_write;
pub mod victoriametrics_remote_write;

pub use kafka::{KafkaConsumer, KafkaConsumerConfig};
pub use otel::{OtlpReceiver, OtlpReceiverConfig};
// pub use prometheus_remote_write::{PrometheusRemoteWriteConfig, PrometheusRemoteWriteServer};
// pub use victoriametrics_remote_write::{
//     VictoriaMetricsRemoteWriteConfig, VictoriaMetricsRemoteWriteServer,
// };
