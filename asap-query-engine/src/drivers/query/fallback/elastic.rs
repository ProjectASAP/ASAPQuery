use super::{FallbackClient, FallbackResponse};
use crate::data_model::QueryLanguage;
use crate::drivers::query::adapters::ParsedQueryRequest;
use async_trait::async_trait;
use axum::http::StatusCode;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
use tracing::{debug, error};

const ELASTIC_FETCH_SIZE: u64 = 1000;

/// Fallback client for Elasticsearch HTTP API
pub struct ElasticHttpFallback {
    client: Client,
    base_url: String,
    index: String,
    language: QueryLanguage,
}

impl ElasticHttpFallback {
    pub fn new(base_url: String, index: String, language: QueryLanguage) -> Self {
        Self {
            client: Client::new(),
            base_url,
            index,
            language,
        }
    }
}

#[async_trait]
impl FallbackClient for ElasticHttpFallback {
    async fn execute_query(
        &self,
        request: &ParsedQueryRequest,
    ) -> Result<FallbackResponse, StatusCode> {
        debug!("=== FORWARDING TO ELASTICSEARCH ===");
        debug!(
            "Forwarding query: '{}', time: {}",
            request.query, request.time
        );

        // Delegate to version with empty headers
        self.execute_query_with_headers(request, HashMap::new())
            .await
    }

    async fn execute_query_with_headers(
        &self,
        request: &ParsedQueryRequest,
        headers: HashMap<String, String>,
    ) -> Result<FallbackResponse, StatusCode> {
        debug!("=== FORWARDING TO ELASTICSEARCH ===");
        debug!(
            "Forwarding query: '{}', time: {}",
            request.query, request.time
        );

        // Build URL based on query language
        let full_url = match self.language {
            QueryLanguage::elastic_sql => {
                // SQL endpoint doesn't use index in path
                format!("{}/_sql", self.base_url.trim_end_matches('/'))
            }
            QueryLanguage::elastic_querydsl => {
                if self.index.is_empty() {
                    format!("{}/_search", self.base_url.trim_end_matches('/'))
                } else {
                    format!(
                        "{}/{}/_search",
                        self.base_url.trim_end_matches('/'),
                        self.index
                    )
                }
            }
            _ => {
                // Fallback to Query DSL endpoint
                format!("{}/_search", self.base_url.trim_end_matches('/'))
            }
        };

        debug!("Full forwarding URL: {}", full_url);

        let query_body: Value = match self.language {
            QueryLanguage::elastic_sql => {
                // query is a raw SQL string, need to wrap it for the ES SQL endpoint
                serde_json::json!({
                    "query": request.query.trim().trim_end_matches(';'),
                    "fetch_size": ELASTIC_FETCH_SIZE,
                })
            }
            _ => {
                // query is already a JSON string (Query DSL)
                match serde_json::from_str(&request.query) {
                    Ok(json) => json,
                    Err(e) => {
                        error!("Failed to parse query as JSON: {}", e);
                        return Err(StatusCode::INTERNAL_SERVER_ERROR);
                    }
                }
            }
        };

        debug!("Parsed query body: {:?}", query_body);

        // Build reqwest headers from HashMap
        let mut req_headers = reqwest::header::HeaderMap::new();
        req_headers.insert(
            reqwest::header::CONTENT_TYPE,
            reqwest::header::HeaderValue::from_static("application/json"),
        );

        if let Some(auth) = headers.get("Authorization") {
            if let Ok(auth_value) = reqwest::header::HeaderValue::from_str(auth) {
                debug!("Forwarding Authorization header");
                req_headers.insert(reqwest::header::AUTHORIZATION, auth_value);
            }
        }

        debug!("Sending POST request to Elasticsearch...");
        match self
            .client
            .post(&full_url)
            .headers(req_headers)
            .json(&query_body)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                debug!("Received response from Elasticsearch, status: {}", status);

                // Get response as JSON
                match response.json::<Value>().await {
                    Ok(es_response) => {
                        // Always return the response, whether it's success or error
                        // Elasticsearch includes error info in the JSON body
                        if let Some(error) = es_response.get("error") {
                            error!("Elasticsearch returned error: {:?}", error);
                            debug!("=== ELASTICSEARCH FORWARD ERROR ===");
                        } else {
                            debug!(
                                "Successfully received Elasticsearch response with {} hits",
                                es_response
                                    .get("hits")
                                    .and_then(|h| h.get("total"))
                                    .and_then(|t| t.get("value"))
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(0)
                            );
                            debug!("=== ELASTICSEARCH FORWARD SUCCESS ===");
                        }

                        Ok(FallbackResponse::Json(es_response))
                    }
                    Err(e) => {
                        error!("Failed to parse Elasticsearch response as JSON: {}", e);
                        debug!("=== ELASTICSEARCH FORWARD PARSE ERROR ===");

                        Err(StatusCode::BAD_REQUEST)
                    }
                }
            }
            Err(e) => {
                error!("Failed to forward query to Elasticsearch: {}", e);
                debug!("=== ELASTICSEARCH FORWARD REQUEST ERROR ===");

                Err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }

    async fn get_runtime_info_with_headers(
        &self,
        headers: HashMap<String, String>,
    ) -> Result<Value, StatusCode> {
        debug!("Fetching runtime info from Elasticsearch fallback with headers");

        // Build the cluster health URL (Elasticsearch's health endpoint)
        let url = format!("{}/_cluster/health", self.base_url.trim_end_matches('/'));

        debug!("Runtime info URL: {}", url);

        // Build reqwest headers from HashMap
        let mut req_headers = reqwest::header::HeaderMap::new();
        if let Some(auth) = headers.get("Authorization") {
            if let Ok(auth_value) = reqwest::header::HeaderValue::from_str(auth) {
                debug!("Forwarding Authorization header to cluster health");
                req_headers.insert(reqwest::header::AUTHORIZATION, auth_value);
            }
        }

        // Send request to Elasticsearch with headers
        match self
            .client
            .get(&url)
            .headers(req_headers)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            Ok(response) => match response.json::<Value>().await {
                Ok(health_info) => {
                    debug!("Elasticsearch cluster health: {:?}", health_info);
                    Ok(health_info)
                }
                Err(e) => {
                    error!("Failed to parse Elasticsearch health response: {}", e);
                    Ok(serde_json::json!({
                        "cluster_name": "unknown",
                        "status": "unknown"
                    }))
                }
            },
            Err(e) => {
                error!("Failed to fetch cluster health from Elasticsearch: {}", e);
                Ok(serde_json::json!({
                    "cluster_name": "unknown",
                    "status": "unknown",
                    "error": e.to_string()
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::enums::QueryLanguage;
    use axum::{routing::post, Json, Router};
    use std::time::SystemTime;
    use tokio::net::TcpListener;
    use tokio::time::{sleep, Duration};

    /// Test 1: Basic client creation for Query DSL
    #[test]
    fn test_elasticsearch_fallback_creation() {
        let base_url = "http://localhost:9200".to_string();
        let index = "logs-*".to_string();

        let fallback = ElasticHttpFallback::new(
            base_url.clone(),
            index.clone(),
            QueryLanguage::elastic_querydsl,
        );

        assert_eq!(fallback.base_url, base_url);
        assert_eq!(fallback.index, index);
    }

    /// Test 1b: Basic client creation for SQL
    #[test]
    fn test_elasticsearch_sql_fallback_creation() {
        let base_url = "http://localhost:9200".to_string();
        let index = "logs-*".to_string();

        let fallback =
            ElasticHttpFallback::new(base_url.clone(), index.clone(), QueryLanguage::elastic_sql);

        assert_eq!(fallback.base_url, base_url);
        assert_eq!(fallback.index, index);
    }

    /// Test 2: Query execution with mock server - success case
    #[tokio::test]
    async fn test_execute_query_success() {
        // Start mock Elasticsearch server
        let mock_port = 9201;
        start_mock_elasticsearch_server(mock_port).await.unwrap();

        // Create fallback client
        let fallback = ElasticHttpFallback::new(
            format!("http://127.0.0.1:{}", mock_port),
            "test_index".to_string(),
            QueryLanguage::elastic_querydsl,
        );

        // Create test request with Query DSL
        let query_dsl = serde_json::json!({
            "query": {
                "match_all": {}
            }
        });

        let request = ParsedQueryRequest {
            query: serde_json::to_string(&query_dsl).unwrap(),
            time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64(),
        };

        // Execute query
        let result = fallback.execute_query(&request).await;

        // Verify success
        assert!(result.is_ok(), "Query execution should succeed");

        // Verify we get a JSON response
        match result.unwrap() {
            FallbackResponse::Json(json) => {
                assert!(json.get("hits").is_some(), "Response should have hits");
                assert_eq!(json["hits"]["total"]["value"], 1);
            }
            FallbackResponse::Text(_) => {
                panic!("Expected JSON response, got Text");
            }
        }
    }

    /// Test 3: URL format with index
    #[tokio::test]
    async fn test_url_with_index() {
        let mock_port = 9202;
        start_mock_elasticsearch_server(mock_port).await.unwrap();

        let fallback = ElasticHttpFallback::new(
            format!("http://127.0.0.1:{}", mock_port),
            "my_index".to_string(),
            QueryLanguage::elastic_querydsl,
        );

        let query_dsl = serde_json::json!({"query": {"match_all": {}}});
        let request = ParsedQueryRequest {
            query: serde_json::to_string(&query_dsl).unwrap(),
            time: 0.0,
        };

        let result = fallback.execute_query(&request).await;
        assert!(result.is_ok(), "Query with index should succeed");
    }

    /// Test 4: Error handling - Elasticsearch returns error
    #[tokio::test]
    async fn test_execute_query_error_response() {
        let mock_port = 9203;
        start_mock_elasticsearch_error_server(mock_port)
            .await
            .unwrap();

        let fallback = ElasticHttpFallback::new(
            format!("http://127.0.0.1:{}", mock_port),
            "test_index".to_string(),
            QueryLanguage::elastic_querydsl,
        );

        let query_dsl = serde_json::json!({"query": {"invalid": {}}});
        let request = ParsedQueryRequest {
            query: serde_json::to_string(&query_dsl).unwrap(),
            time: 0.0,
        };

        let result = fallback.execute_query(&request).await;

        // Should still return Ok with error in JSON (like Prometheus does)
        assert!(result.is_ok(), "Should handle Elasticsearch error response");
        match result.unwrap() {
            FallbackResponse::Json(json) => {
                assert!(json.get("error").is_some(), "Should have error field");
            }
            _ => panic!("Expected JSON response"),
        }
    }

    /// Test 5: Network failure handling
    #[tokio::test]
    async fn test_elasticsearch_server_unreachable() {
        let fallback = ElasticHttpFallback::new(
            "http://127.0.0.1:9999".to_string(),
            "test_index".to_string(),
            QueryLanguage::elastic_querydsl,
        );

        let query_dsl = serde_json::json!({"query": {"match_all": {}}});
        let request = ParsedQueryRequest {
            query: serde_json::to_string(&query_dsl).unwrap(),
            time: 0.0,
        };

        let result = fallback.execute_query(&request).await;

        // Should return Err for connection failures
        assert!(result.is_err(), "Should return Err for unreachable server");
        assert_eq!(result.unwrap_err(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// Test 6: SQL query execution with mock server
    #[tokio::test]
    async fn test_execute_sql_query_success() {
        // Start mock Elasticsearch SQL server
        let mock_port = 9204;
        start_mock_elasticsearch_sql_server(mock_port)
            .await
            .unwrap();

        // Create fallback client for SQL
        let fallback = ElasticHttpFallback::new(
            format!("http://127.0.0.1:{}", mock_port),
            "test_index".to_string(),
            QueryLanguage::elastic_sql,
        );

        // Create SQL query request
        let sql_query = serde_json::json!({
            "query": "SELECT * FROM logs WHERE status = 200"
        });

        let request = ParsedQueryRequest {
            query: serde_json::to_string(&sql_query).unwrap(),
            time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64(),
        };

        // Execute query
        let result = fallback.execute_query(&request).await;

        // Verify success
        assert!(result.is_ok(), "SQL query execution should succeed");

        match result.unwrap() {
            FallbackResponse::Json(json) => {
                assert!(
                    json.get("columns").is_some(),
                    "SQL response should have columns"
                );
                assert!(json.get("rows").is_some(), "SQL response should have rows");
            }
            FallbackResponse::Text(_) => {
                panic!("Expected JSON response, got Text");
            }
        }
    }

    /// Test 7: Verify SQL endpoint URL format (no index in path)
    #[tokio::test]
    async fn test_sql_url_format() {
        let mock_port = 9205;
        start_mock_elasticsearch_sql_server(mock_port)
            .await
            .unwrap();

        let fallback = ElasticHttpFallback::new(
            format!("http://127.0.0.1:{}", mock_port),
            "some_index".to_string(), // Index should be ignored for SQL
            QueryLanguage::elastic_sql,
        );

        let sql_query = serde_json::json!({
            "query": "SELECT COUNT(*) FROM logs"
        });

        let request = ParsedQueryRequest {
            query: serde_json::to_string(&sql_query).unwrap(),
            time: 0.0,
        };

        let result = fallback.execute_query(&request).await;
        assert!(result.is_ok(), "SQL query should succeed");
        // URL should be /_sql, not /some_index/_sql
    }

    // ===== Mock Server Helpers =====

    async fn start_mock_elasticsearch_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
        async fn mock_search_handler(_body: Json<Value>) -> Json<Value> {
            Json(serde_json::json!({
                "took": 5,
                "hits": {
                    "total": {"value": 1, "relation": "eq"},
                    "hits": [{
                        "_source": {"field1": "value1"}
                    }]
                }
            }))
        }

        let app = Router::new()
            .route("/_search", post(mock_search_handler))
            .route("/:index/_search", post(mock_search_handler));

        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    async fn start_mock_elasticsearch_error_server(
        port: u16,
    ) -> Result<(), Box<dyn std::error::Error>> {
        async fn mock_error_handler(_body: Json<Value>) -> (StatusCode, Json<Value>) {
            (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": {
                        "type": "parsing_exception",
                        "reason": "Unknown query type"
                    },
                    "status": 400
                })),
            )
        }

        let app = Router::new()
            .route("/_search", post(mock_error_handler))
            .route("/:index/_search", post(mock_error_handler));

        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    async fn start_mock_elasticsearch_sql_server(
        port: u16,
    ) -> Result<(), Box<dyn std::error::Error>> {
        async fn mock_sql_handler(_body: Json<Value>) -> Json<Value> {
            Json(serde_json::json!({
                "columns": [
                    {"name": "status", "type": "integer"},
                    {"name": "count", "type": "long"}
                ],
                "rows": [
                    [200, 150],
                    [404, 23]
                ]
            }))
        }

        let app = Router::new().route("/_sql", post(mock_sql_handler));

        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        sleep(Duration::from_millis(100)).await;
        Ok(())
    }
}
