#[derive(clap::ValueEnum, Clone, Debug)]
pub enum InputFormat {
    Json,
    Byte,
}

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum StreamingEngine {
    Arroyo,
    Precompute,
}

pub use asap_types::enums::{CleanupPolicy, QueryLanguage};

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
