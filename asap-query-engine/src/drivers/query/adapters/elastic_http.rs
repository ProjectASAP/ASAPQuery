use super::config::AdapterConfig;
use super::traits::*;
use crate::QueryResult;
use crate::data_model::QueryLanguage;
use async_trait::async_trait;
use axum::{
    body::Bytes,
    extract::{Form, Query},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

/// Elasticsearch HTTP protocol adapter
pub struct ElasticHttpAdapter {
    #[allow(dead_code)]
    config: AdapterConfig,
}

impl ElasticHttpAdapter {
    pub fn new(config: AdapterConfig) -> Self {
        Self { config }
    }

    /// Parse Elasticsearch query from JSON body
    fn parse_elasticsearch_query(&self, body: &Bytes) -> Result<ParsedQueryRequest, AdapterError> {
        let json_body: Value = serde_json::from_slice(body)
            .map_err(|e| AdapterError::ParseError(format!("Invalid JSON: {}", e)))?;

        // Extract the SQL string from the "query" field
        let query = match &self.config.language {
            QueryLanguage::elastic_sql => {
                json_body
                    .get("query")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| AdapterError::MissingParameter("query".to_string()))?
                    .to_string()
            }
            _ => {
                // For QueryDSL, keep the full JSON as before
                serde_json::to_string(&json_body)
                    .map_err(|e| AdapterError::ParseError(format!("Failed to serialize query: {}", e)))?
            }
        };

        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        Ok(ParsedQueryRequest { query, time })
    }
}

#[async_trait]
impl QueryRequestAdapter for ElasticHttpAdapter {
    async fn parse_get_request(
        &self,
        _query_params: Query<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        // Elasticsearch primarily uses POST for _search queries
        // GET is sometimes used for URI search, but we don't support it
        Err(AdapterError::ProtocolError(
            "GET requests not supported for Elasticsearch queries. Use POST with Query DSL."
                .to_string(),
        ))
    }

    async fn parse_post_request(
        &self,
        _form_params: Form<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        // Elasticsearch doesn't use form-encoded data
        Err(AdapterError::ProtocolError(
            "Form-encoded POST not supported for Elasticsearch. Use JSON Query DSL.".to_string(),
        ))
    }

    /// Parse JSON POST request with Elasticsearch Query DSL
    async fn parse_json_post_request(
        &self,
        body: Bytes,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        debug!("Elasticsearch adapter: parsing JSON POST request");
        self.parse_elasticsearch_query(&body)
    }

    fn get_query_endpoint(&self) -> &'static str {
        match self.config.language {
            QueryLanguage::elastic_sql => "/_sql",
            QueryLanguage::elastic_querydsl => "/_search",
            _ => panic!("Invalid query language configured for Elastic"),
        }
    }

    async fn parse_range_get_request(
        &self,
        _query_params: Query<HashMap<String, String>>,
    ) -> Result<ParsedRangeQueryRequest, AdapterError> {
        Err(AdapterError::ProtocolError(
            "Range queries not supported by Elasticsearch adapter".to_string(),
        ))
    }

    async fn parse_range_post_request(
        &self,
        _form_params: Form<HashMap<String, String>>,
    ) -> Result<ParsedRangeQueryRequest, AdapterError> {
        Err(AdapterError::ProtocolError(
            "Range queries not supported by Elasticsearch adapter".to_string(),
        ))
    }

    fn get_range_query_endpoint(&self) -> &'static str {
        "/_search_range" // Not actually used, just satisfying the trait
    }
}

#[async_trait]
impl QueryResponseAdapter for ElasticHttpAdapter {
    async fn format_success_response(
    &self,
    result: &QueryExecutionResult,
) -> Result<Response, StatusCode> {
        info!("SKETCH HIT: serving {} rows from precomputed sketches", 
          match &result.query_result {
              QueryResult::Vector(v) => v.values.len(),
              QueryResult::Matrix(_) => 0,
          });

        let label_names = &result.query_output_labels.labels;

        let hits: Vec<Value> = match &result.query_result {
            QueryResult::Vector(instant_vector) => {
                instant_vector
                    .values
                    .iter()
                    .map(|element| {
                        let mut source = serde_json::Map::new();
                        for (i, label_name) in label_names.iter().enumerate() {
                            let label_value =
                                element.labels.get(i).map(|s| s.as_str()).unwrap_or("");
                            source.insert(label_name.clone(), json!(label_value));
                        }
                        source.insert("value".to_string(), json!(element.value));
                        json!({
                            "_source": source
                        })
                    })
                    .collect()
            }
            QueryResult::Matrix(_) => {
                return Err(StatusCode::NOT_IMPLEMENTED);
            }
        };

        let response = json!({
            "took": 0,
            "timed_out": false,
            "hits": {
                "total": {
                    "value": hits.len(),
                    "relation": "eq"
                },
                "hits": hits
            }
        });

        Ok(Json(response).into_response())
    }

    async fn format_error_response(&self, error: &AdapterError) -> Result<Response, StatusCode> {
        debug!(
            "Elasticsearch adapter: formatting error response: {:?}",
            error
        );

        let (status_code, error_type) = match error {
            AdapterError::MissingParameter(_) => (StatusCode::BAD_REQUEST, "parsing_exception"),
            AdapterError::InvalidParameter(_) => (StatusCode::BAD_REQUEST, "parsing_exception"),
            AdapterError::ParseError(_) => (StatusCode::BAD_REQUEST, "parsing_exception"),
            AdapterError::NetworkError(_) => (StatusCode::BAD_GATEWAY, "network_error"),
            AdapterError::ProtocolError(_) => {
                (StatusCode::BAD_REQUEST, "illegal_argument_exception")
            }
        };

        let response = json!({
            "error": {
                "type": error_type,
                "reason": error.to_string()
            },
            "status": status_code.as_u16()
        });

        // Return the error as JSON with appropriate status code
        Ok(Json(response).into_response())
    }

    async fn format_unsupported_query_response(&self) -> Result<Response, StatusCode> {
        debug!("Elasticsearch adapter: formatting unsupported query response");

        let response = json!({
            "error": {
                "type": "illegal_argument_exception",
                "reason": "Query not supported by local execution"
            },
            "status": 400
        });

        Ok(Json(response).into_response())
    }

    async fn format_range_success_response(
        &self,
        _result: &crate::engines::QueryResult,
        _labels: &promql_utilities::KeyByLabelNames,
    ) -> Result<Response, StatusCode> {
        // Elasticsearch doesn't support Prometheus-style range queries
        Err(StatusCode::NOT_IMPLEMENTED)
    }
}

#[async_trait]
impl HttpProtocolAdapter for ElasticHttpAdapter {
    fn adapter_name(&self) -> &'static str {
        "ElasticHttp"
    }

    fn get_runtime_info_path(&self) -> &'static str {
        "/_cluster/health"
    }

    async fn handle_runtime_info(
        &self,
        store: Arc<dyn crate::stores::Store>,
    ) -> Result<Json<Value>, StatusCode> {
        // Delegate to the version with headers
        self.handle_runtime_info_with_headers(store, HashMap::new())
            .await
    }

    // Override the new method
    async fn handle_runtime_info_with_headers(
        &self,
        store: Arc<dyn crate::stores::Store>,
        headers: HashMap<String, String>,
    ) -> Result<Json<Value>, StatusCode> {
        debug!("Handling runtime info request in Elasticsearch adapter");

        let earliest_timestamps = match store.get_earliest_timestamp_per_aggregation_id() {
            Ok(timestamps) => timestamps,
            Err(e) => {
                error!("Error getting earliest timestamps: {}", e);
                HashMap::new()
            }
        };

        let mut runtime_data = if let Some(fallback) = &self.config.fallback {
            debug!("Fetching runtime info from fallback Elasticsearch");
            match fallback.get_runtime_info_with_headers(headers).await {
                Ok(data) => data,
                Err(e) => {
                    error!("Failed to get runtime info from fallback: {:?}", e);
                    json!({
                        "cluster_name": "unknown",
                        "status": "yellow"
                    })
                }
            }
        } else {
            json!({
                "cluster_name": "local",
                "status": "green"
            })
        };

        if let Some(data_obj) = runtime_data.as_object_mut() {
            data_obj.insert(
                "earliest_timestamp_per_aggregation_id".to_string(),
                serde_json::to_value(earliest_timestamps).unwrap_or(json!({})),
            );
        }

        Ok(Json(runtime_data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::enums::{QueryLanguage, QueryProtocol};
    use crate::engines::QueryResult;
    use promql_utilities::data_model::KeyByLabelNames;

    fn create_test_adapter() -> ElasticHttpAdapter {
        let config = AdapterConfig::new(
            QueryProtocol::ElasticHttp,
            QueryLanguage::elastic_querydsl,
            None, // No fallback for unit tests
        );
        ElasticHttpAdapter::new(config)
    }

    fn create_test_adapter_sql() -> ElasticHttpAdapter {
        let config = AdapterConfig::new(
            QueryProtocol::ElasticHttp,
            QueryLanguage::elastic_sql,
            None, // No fallback for unit tests
        );
        ElasticHttpAdapter::new(config)
    }

    /// Test: Parse JSON POST request with Query DSL
    #[tokio::test]
    async fn test_parse_json_post_request() {
        let adapter = create_test_adapter();

        let query_dsl = json!({
            "query": {
                "match_all": {}
            }
        });

        let body = Bytes::from(serde_json::to_vec(&query_dsl).unwrap());
        let result = adapter.parse_json_post_request(body).await;

        assert!(result.is_ok(), "JSON POST request parsing should succeed");
        let parsed = result.unwrap();
        assert!(parsed.query.contains("match_all"));
    }

    /// Test: Parse Query DSL with timestamp
    #[tokio::test]
    async fn test_parse_query_with_timestamp() {
        let adapter = create_test_adapter();

        let query_dsl = json!({
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": 1640000000.0,
                                    "lte": 1640086400.0
                                }
                            }
                        }
                    ]
                }
            }
        });

        let body = Bytes::from(serde_json::to_vec(&query_dsl).unwrap());
        let result = adapter.parse_json_post_request(body).await;

        assert!(result.is_ok(), "Query with timestamp should parse");
    }

    /// Test: Parse SQL query
    #[tokio::test]
    async fn test_parse_sql_query() {
        let adapter = create_test_adapter_sql();

        let sql_query = json!({
            "query": "SELECT * FROM logs WHERE @timestamp > now() - INTERVAL 1 HOUR"
        });

        let body = Bytes::from(serde_json::to_vec(&sql_query).unwrap());
        let result = adapter.parse_json_post_request(body).await;

        assert!(result.is_ok(), "SQL query parsing should succeed");
        let parsed = result.unwrap();
        assert!(parsed.query.contains("SELECT"));
    }

    /// Test: Parse SQL query with parameters
    #[tokio::test]
    async fn test_parse_sql_query_with_params() {
        let adapter = create_test_adapter_sql();

        let sql_query = json!({
            "query": "SELECT status, COUNT(*) FROM logs GROUP BY status",
            "fetch_size": 100,
            "time_zone": "UTC"
        });

        let body = Bytes::from(serde_json::to_vec(&sql_query).unwrap());
        let result = adapter.parse_json_post_request(body).await;

        assert!(result.is_ok(), "SQL query with params should parse");
        let parsed = result.unwrap();
        assert!(parsed.query.contains("fetch_size"));
        assert!(parsed.query.contains("time_zone"));
    }

    /// Test: Invalid JSON should return error
    #[tokio::test]
    async fn test_invalid_json() {
        let adapter = create_test_adapter();

        let invalid_json = Bytes::from("not valid json");
        let result = adapter.parse_json_post_request(invalid_json).await;

        assert!(result.is_err(), "Invalid JSON should return error");
        match result {
            Err(AdapterError::ParseError(_)) => {} // Expected
            _ => panic!("Expected ParseError"),
        }
    }

    /// Test: GET requests not supported
    #[tokio::test]
    async fn test_get_not_supported() {
        let adapter = create_test_adapter();

        let params = HashMap::new();
        let result = adapter.parse_get_request(Query(params)).await;

        assert!(result.is_err(), "GET requests should not be supported");
        match result {
            Err(AdapterError::ProtocolError(_)) => {} // Expected
            _ => panic!("Expected ProtocolError"),
        }
    }

    /// Test: Form POST not supported
    #[tokio::test]
    async fn test_form_post_not_supported() {
        let adapter = create_test_adapter();

        let params = HashMap::new();
        let result = adapter.parse_post_request(Form(params)).await;

        assert!(result.is_err(), "Form POST should not be supported");
        match result {
            Err(AdapterError::ProtocolError(_)) => {} // Expected
            _ => panic!("Expected ProtocolError"),
        }
    }

    /// Test: Format success response
    #[tokio::test]
    async fn test_format_success_response() {
        let adapter = create_test_adapter();

        let result = QueryExecutionResult {
            query_output_labels: KeyByLabelNames::empty(),
            query_result: QueryResult::vector(vec![], 0),
        };

        let response = adapter.format_success_response(&result).await;
        assert!(response.is_ok(), "Response formatting should succeed");

        // Extract JSON from Response
        let http_response = response.unwrap();
        let body_bytes = axum::body::to_bytes(http_response.into_body(), usize::MAX)
            .await
            .expect("Failed to read body");
        let json_response: Value =
            serde_json::from_slice(&body_bytes).expect("Failed to parse JSON");

        // Verify Elasticsearch response structure
        assert!(json_response.get("hits").is_some());
        assert!(json_response["hits"].get("total").is_some());
    }

    /// Test: Format error response
    #[tokio::test]
    async fn test_format_error_response() {
        let adapter = create_test_adapter();

        let error = AdapterError::ParseError("Invalid query syntax".to_string());
        let response = adapter.format_error_response(&error).await;

        assert!(response.is_ok(), "Error formatting should succeed");

        // Extract JSON from Response
        let http_response = response.unwrap();
        let body_bytes = axum::body::to_bytes(http_response.into_body(), usize::MAX)
            .await
            .expect("Failed to read body");

        let json_response: Value =
            serde_json::from_slice(&body_bytes).expect("Failed to parse JSON");

        // Verify Elasticsearch error structure
        assert!(json_response.get("error").is_some());
        assert_eq!(json_response["error"]["type"], "parsing_exception");
        assert_eq!(json_response["status"], 400);
    }

    /// Test: Get query endpoint for Query DSL
    #[test]
    fn test_get_query_endpoint_querydsl() {
        let adapter = create_test_adapter();
        assert_eq!(adapter.get_query_endpoint(), "/_search");
    }

    /// Test: Get query endpoint for SQL
    #[test]
    fn test_get_query_endpoint_sql() {
        let adapter = create_test_adapter_sql();
        assert_eq!(adapter.get_query_endpoint(), "/_sql");
    }

    /// Test: Get adapter name
    #[test]
    fn test_adapter_name() {
        let adapter = create_test_adapter();
        assert_eq!(adapter.adapter_name(), "ElasticHttp");
    }

    /// Test: Get runtime info path
    #[test]
    fn test_get_runtime_info_path() {
        let adapter = create_test_adapter();
        assert_eq!(adapter.get_runtime_info_path(), "/_cluster/health");
    }

    /// Test: SQL adapter also uses same runtime info path
    #[test]
    fn test_get_runtime_info_path_sql() {
        let adapter = create_test_adapter_sql();
        assert_eq!(adapter.get_runtime_info_path(), "/_cluster/health");
    }
}
