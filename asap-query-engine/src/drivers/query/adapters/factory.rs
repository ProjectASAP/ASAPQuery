use super::clickhouse_http::ClickHouseHttpAdapter;
use super::config::AdapterConfig;
use super::elastic_http::ElasticHttpAdapter;
use super::prometheus_http::PrometheusHttpAdapter;
use super::traits::HttpProtocolAdapter;
use crate::data_model::enums::QueryProtocol;
use std::sync::Arc;

/// Factory function to create appropriate HTTP adapter based on protocol
pub fn create_http_adapter(config: AdapterConfig) -> Arc<dyn HttpProtocolAdapter> {
    match config.protocol {
        QueryProtocol::PrometheusHttp => Arc::new(PrometheusHttpAdapter::new(config)),
        QueryProtocol::ClickHouseHttp => Arc::new(ClickHouseHttpAdapter::new(config)),
        QueryProtocol::ElasticHttp => Arc::new(ElasticHttpAdapter::new(config)),
    }
}
