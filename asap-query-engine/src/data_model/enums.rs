#[derive(clap::ValueEnum, Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputFormat {
    Json,
    Byte,
}

#[derive(clap::ValueEnum, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamingEngine {
    Arroyo,
    Precompute,
}

pub use asap_types::enums::{CleanupPolicy, QueryLanguage, WindowType};
pub use promql_utilities::query_logics::enums::AggregationType;

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

#[derive(clap::ValueEnum, Clone, Debug, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LockStrategy {
    #[value(name = "global")]
    #[serde(rename = "global")]
    Global,
    #[value(name = "per-key")]
    #[serde(rename = "per-key")]
    PerKey,
}
