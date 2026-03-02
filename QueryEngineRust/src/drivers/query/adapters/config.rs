use crate::data_model::enums::{QueryLanguage, QueryProtocol};
use crate::drivers::query::fallback::FallbackClient;
use std::sync::Arc;

/// Configuration for a specific protocol adapter
#[derive(Clone)]
pub struct AdapterConfig {
    /// The query protocol to use
    pub protocol: QueryProtocol,

    /// The query language to use
    pub language: QueryLanguage,

    /// Optional fallback client for unsupported queries
    pub fallback: Option<Arc<dyn FallbackClient>>,
}

impl std::fmt::Debug for AdapterConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdapterConfig")
            .field("protocol", &self.protocol)
            .field("language", &self.language)
            .field(
                "fallback",
                &self.fallback.as_ref().map(|_| "Some(FallbackClient)"),
            )
            .finish()
    }
}

impl AdapterConfig {
    /// Generic constructor for adapter configuration
    pub fn new(
        protocol: QueryProtocol,
        language: QueryLanguage,
        fallback: Option<Arc<dyn FallbackClient>>,
    ) -> Self {
        Self {
            protocol,
            language,
            fallback,
        }
    }

    /// Create a configuration for Prometheus HTTP with PromQL
    /// Convenience constructor for backward compatibility
    pub fn prometheus_promql(fallback_url: String, forward_unsupported: bool) -> Self {
        use crate::drivers::query::fallback::PrometheusHttpFallback;

        let fallback = if forward_unsupported {
            Some(Arc::new(PrometheusHttpFallback::new(fallback_url)) as Arc<dyn FallbackClient>)
        } else {
            None
        };

        Self::new(
            QueryProtocol::PrometheusHttp,
            QueryLanguage::promql,
            fallback,
        )
    }

    /// Create a configuration for ClickHouse HTTP with SQL
    /// Convenience constructor for ClickHouse adapter
    pub fn clickhouse_sql(base_url: String, database: String, forward_unsupported: bool) -> Self {
        use crate::drivers::query::fallback::ClickHouseHttpFallback;

        let fallback = if forward_unsupported {
            Some(Arc::new(ClickHouseHttpFallback::new(base_url, database))
                as Arc<dyn FallbackClient>)
        } else {
            None
        };

        Self::new(QueryProtocol::ClickHouseHttp, QueryLanguage::sql, fallback)
    }

    /// Create a configuration for Elasticsearch HTTP with Elasticsearch QueryDSL
    /// Convenience constructor for Elasticsearch adapter
    pub fn elastic_querydsl(base_url: String, index: String, forward_unsupported: bool) -> Self {
        use crate::drivers::query::fallback::ElasticHttpFallback;

        let fallback = if forward_unsupported {
            Some(Arc::new(ElasticHttpFallback::new(
                base_url,
                index,
                QueryLanguage::elastic_querydsl,
            )) as Arc<dyn FallbackClient>)
        } else {
            None
        };

        Self::new(
            QueryProtocol::ElasticHttp,
            QueryLanguage::elastic_querydsl,
            fallback,
        )
    }

    /// Create a configuration for Elasticsearch HTTP with SQL
    /// Convenience constructor for Elasticsearch SQL adapter
    pub fn elastic_sql(base_url: String, index: String, forward_unsupported: bool) -> Self {
        use crate::drivers::query::fallback::ElasticHttpFallback;

        let fallback = if forward_unsupported {
            Some(Arc::new(ElasticHttpFallback::new(
                base_url,
                index,
                QueryLanguage::elastic_sql,
            )) as Arc<dyn FallbackClient>)
        } else {
            None
        };

        Self::new(
            QueryProtocol::ElasticHttp,
            QueryLanguage::elastic_sql,
            fallback,
        )
    }
}
