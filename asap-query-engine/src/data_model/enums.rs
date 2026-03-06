#[derive(clap::ValueEnum, Clone, Debug)]
pub enum InputFormat {
    Json,
    Byte,
}

#[derive(clap::ValueEnum, Clone, Debug)]
pub enum StreamingEngine {
    Flink,
    Arroyo,
}

pub use sketch_db_common::enums::{CleanupPolicy, QueryLanguage};

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum QueryProtocol {
    #[value(alias = "PROMETHEUS_HTTP")]
    PrometheusHttp,
    #[value(alias = "CLICKHOUSE_HTTP")]
    ClickHouseHttp,
    #[value(alias = "ELASTIC_HTTP")]
    ElasticHttp,
    // Future: DuckDbHttp, etc.
}

#[derive(clap::ValueEnum, Clone, Debug, Copy, PartialEq)]
pub enum LockStrategy {
    #[value(name = "global")]
    Global,
    #[value(name = "per-key")]
    PerKey,
}
