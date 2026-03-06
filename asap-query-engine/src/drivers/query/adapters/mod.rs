pub mod clickhouse_http;
pub mod config;
pub mod elastic_http;
pub mod factory;
pub mod prometheus_http;
pub mod traits;

// Re-export main types
pub use config::AdapterConfig;
pub use factory::create_http_adapter;
pub use prometheus_http::{PrometheusHttpAdapter, PrometheusResponse};
pub use traits::{
    AdapterError, HttpProtocolAdapter, ParsedQueryRequest, ParsedRangeQueryRequest,
    QueryExecutionResult, QueryRequestAdapter, QueryResponseAdapter,
};
