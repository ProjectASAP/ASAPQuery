use super::config::AdapterConfig;
use super::traits::*;
use crate::engines::QueryResult;
use async_trait::async_trait;
use axum::{
    extract::{Form, Query},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use promql_utilities::data_model::KeyByLabelNames;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// ClickHouse HTTP protocol adapter
pub struct ClickHouseHttpAdapter {
    #[allow(dead_code)]
    config: AdapterConfig,
}

impl ClickHouseHttpAdapter {
    pub fn new(config: AdapterConfig) -> Self {
        Self { config }
    }

    /// Helper to parse query parameters (used by both GET and POST)
    fn parse_params_common(
        &self,
        params: &HashMap<String, String>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        // Extract query parameter (required)
        let query = params
            .get("query")
            .ok_or_else(|| AdapterError::MissingParameter("query".to_string()))?
            .clone();

        // ClickHouse doesn't use a time parameter, but ParsedQueryRequest requires it
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        Ok(ParsedQueryRequest { query, time })
    }
}

#[async_trait]
impl QueryRequestAdapter for ClickHouseHttpAdapter {
    async fn parse_get_request(
        &self,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        debug!(
            "ClickHouse adapter: parsing GET request with params: {:?}",
            params
        );
        self.parse_params_common(&params)
    }

    async fn parse_post_request(
        &self,
        Form(params): Form<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        debug!(
            "ClickHouse adapter: parsing POST request with params: {:?}",
            params
        );
        self.parse_params_common(&params)
    }

    fn get_query_endpoint(&self) -> &'static str {
        "/clickhouse/query"
    }

    async fn parse_range_get_request(
        &self,
        _query_params: Query<HashMap<String, String>>,
    ) -> Result<ParsedRangeQueryRequest, AdapterError> {
        Err(AdapterError::ProtocolError(
            "Range queries not supported by ClickHouse adapter".to_string(),
        ))
    }

    async fn parse_range_post_request(
        &self,
        _form_params: Form<HashMap<String, String>>,
    ) -> Result<ParsedRangeQueryRequest, AdapterError> {
        Err(AdapterError::ProtocolError(
            "Range queries not supported by ClickHouse adapter".to_string(),
        ))
    }

    fn get_range_query_endpoint(&self) -> &'static str {
        "/clickhouse/query_range" // Not actually used, just satisfying the trait
    }
}

#[async_trait]
impl QueryResponseAdapter for ClickHouseHttpAdapter {
    async fn format_success_response(
        &self,
        result: &QueryExecutionResult,
    ) -> Result<Response, StatusCode> {
        // Convert QueryResult to ClickHouse TabSeparated format
        // Format: columns separated by tabs, rows separated by newlines
        let label_names = &result.query_output_labels.labels;

        // Build TabSeparated output
        let mut output = String::new();

        match &result.query_result {
            QueryResult::Vector(instant_vector) => {
                for element in &instant_vector.values {
                    // Add label values
                    for (i, _label_name) in label_names.iter().enumerate() {
                        let label_value = element.labels.get(i).map(|s| s.as_str()).unwrap_or("");
                        output.push_str(label_value);
                        output.push('\t');
                    }
                    // Add value column
                    output.push_str(&element.value.to_string());
                    output.push('\n');
                }
            }
            QueryResult::Matrix(_) => {
                // ClickHouse adapter doesn't support Matrix results
                return Err(StatusCode::NOT_IMPLEMENTED);
            }
        };

        debug!(
            "ClickHouse adapter: formatting TabSeparated response:\n{}",
            output
        );
        Ok(output.into_response())
    }

    async fn format_range_success_response(
        &self,
        _result: &QueryResult,
        _labels: &KeyByLabelNames,
    ) -> Result<Response, StatusCode> {
        // ClickHouse doesn't support Prometheus-style range queries
        Err(StatusCode::NOT_IMPLEMENTED)
    }

    async fn format_error_response(&self, error: &AdapterError) -> Result<Response, StatusCode> {
        // Return proper HTTP error status codes like ClickHouse does
        let status_code = match error {
            AdapterError::MissingParameter(_) => StatusCode::BAD_REQUEST,
            AdapterError::InvalidParameter(_) => StatusCode::BAD_REQUEST,
            AdapterError::ParseError(_) => StatusCode::BAD_REQUEST,
            AdapterError::NetworkError(_) => StatusCode::BAD_GATEWAY,
            AdapterError::ProtocolError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        debug!(
            "ClickHouse adapter: formatting error response for {:?}, returning status: {}",
            error, status_code
        );
        Err(status_code)
    }

    async fn format_unsupported_query_response(&self) -> Result<Response, StatusCode> {
        // Return HTTP 501 Not Implemented for unsupported queries
        Err(StatusCode::NOT_IMPLEMENTED)
    }
}

#[async_trait]
impl HttpProtocolAdapter for ClickHouseHttpAdapter {
    fn adapter_name(&self) -> &'static str {
        "ClickHouseHTTP"
    }

    fn get_runtime_info_path(&self) -> &'static str {
        "/clickhouse/ping"
    }

    async fn handle_runtime_info(
        &self,
        _store: Arc<dyn crate::stores::Store>,
    ) -> Result<Json<Value>, StatusCode> {
        // Stub implementation - return basic info
        // In the future, this could query the store for metrics
        // and/or forward to the fallback ClickHouse instance
        Ok(Json(json!({
            "status": "ok",
            "adapter": "ClickHouseHTTP",
            "version": "1.0.0"
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::enums::{QueryLanguage, QueryProtocol};
    use crate::engines::QueryResult;
    use promql_utilities::data_model::KeyByLabelNames;

    fn create_test_adapter() -> ClickHouseHttpAdapter {
        let config = AdapterConfig::new(
            QueryProtocol::ClickHouseHttp,
            QueryLanguage::sql,
            None, // No fallback for unit tests
        );
        ClickHouseHttpAdapter::new(config)
    }

    /// Test 7: Parse GET request with SQL query
    #[tokio::test]
    async fn test_parse_get_request() {
        let adapter = create_test_adapter();

        let mut params = HashMap::new();
        params.insert("query".to_string(), "SELECT 1".to_string());

        let result = adapter.parse_get_request(Query(params)).await;

        assert!(result.is_ok(), "GET request parsing should succeed");
        let parsed = result.unwrap();
        assert_eq!(parsed.query, "SELECT 1");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        assert!(
            (parsed.time - now).abs() < 1.0,
            "Time should be within 1 second of current time"
        );
    }

    /// Test 8: Parse POST request with form-encoded data
    #[tokio::test]
    async fn test_parse_post_request_form() {
        let adapter = create_test_adapter();

        let mut params = HashMap::new();
        params.insert("query".to_string(), "SELECT * FROM table".to_string());

        let result = adapter.parse_post_request(Form(params)).await;

        assert!(result.is_ok(), "POST request parsing should succeed");
        let parsed = result.unwrap();
        assert_eq!(parsed.query, "SELECT * FROM table");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        assert!(
            (parsed.time - now).abs() < 1.0,
            "Time should be within 1 second of current time"
        );
    }

    /// Test 9: Missing query parameter should return error
    #[tokio::test]
    async fn test_missing_query_parameter() {
        let adapter = create_test_adapter();

        // No query parameter provided
        let params = HashMap::new();

        let result = adapter.parse_get_request(Query(params)).await;

        assert!(
            result.is_err(),
            "Missing query parameter should return error"
        );
        match result {
            Err(AdapterError::MissingParameter(param)) => {
                assert_eq!(
                    param, "query",
                    "Error should indicate missing 'query' parameter"
                );
            }
            _ => panic!("Expected MissingParameter error"),
        }
    }

    /// Test 10: Format success response with actual data in TabSeparated format
    #[tokio::test]
    async fn test_format_success_response() {
        use crate::data_model::KeyByLabelValues;
        use crate::engines::query_result::InstantVectorElement;
        use axum::body::to_bytes;

        let adapter = create_test_adapter();

        // Create a mock QueryExecutionResult with actual data
        let label_names = KeyByLabelNames::new(vec!["hostname".to_string()]);
        let elements = vec![
            InstantVectorElement::new(
                KeyByLabelValues::new_with_labels(vec!["host1".to_string()]),
                91.0,
            ),
            InstantVectorElement::new(
                KeyByLabelValues::new_with_labels(vec!["host2".to_string()]),
                77.5,
            ),
        ];
        let result = QueryExecutionResult {
            query_output_labels: label_names,
            query_result: QueryResult::vector(elements, 1000),
        };

        let response = adapter.format_success_response(&result).await;
        assert!(response.is_ok(), "Response formatting should succeed");

        // Extract body from Response
        let response = response.unwrap();
        let body_bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

        // Verify TabSeparated format: "label_value\tvalue\n" for each row
        let lines: Vec<&str> = body_str.lines().collect();
        assert_eq!(lines.len(), 2, "Should have 2 rows");

        // First row: host1\t91
        let cols1: Vec<&str> = lines[0].split('\t').collect();
        assert_eq!(cols1.len(), 2);
        assert_eq!(cols1[0], "host1");
        assert_eq!(cols1[1], "91");

        // Second row: host2\t77.5
        let cols2: Vec<&str> = lines[1].split('\t').collect();
        assert_eq!(cols2.len(), 2);
        assert_eq!(cols2[0], "host2");
        assert_eq!(cols2[1], "77.5");
    }

    /// Test 11: Format error response
    #[tokio::test]
    async fn test_format_error_response() {
        let adapter = create_test_adapter();

        let error = AdapterError::InvalidParameter("Invalid SQL syntax".to_string());

        let response = adapter.format_error_response(&error).await;

        // Should return Err with BAD_REQUEST status code
        assert!(response.is_err(), "Error formatting should return Err");
        assert_eq!(response.unwrap_err(), StatusCode::BAD_REQUEST);
    }

    /// Test 12: Get query endpoint path
    #[test]
    fn test_get_query_endpoint() {
        let adapter = create_test_adapter();

        let endpoint = adapter.get_query_endpoint();
        assert_eq!(endpoint, "/clickhouse/query");
    }

    /// Test 13: Get adapter name
    #[test]
    fn test_adapter_name() {
        let adapter = create_test_adapter();

        let name = adapter.adapter_name();
        assert_eq!(name, "ClickHouseHTTP");
    }

    /// Test 14: Get runtime info path
    #[test]
    fn test_get_runtime_info_path() {
        let adapter = create_test_adapter();

        let path = adapter.get_runtime_info_path();
        assert_eq!(path, "/clickhouse/ping");
    }

    /// Test 15: Query parameter is required
    #[tokio::test]
    async fn test_query_parameter_required() {
        let adapter = create_test_adapter();

        let mut params = HashMap::new();
        params.insert("query".to_string(), "SELECT 1".to_string());

        let result = adapter.parse_get_request(Query(params)).await;

        assert!(result.is_ok(), "GET request with query should succeed");
        let parsed = result.unwrap();
        assert_eq!(parsed.query, "SELECT 1");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        assert!(
            (parsed.time - now).abs() < 1.0,
            "Time should be within 1 second of current time"
        );
    }
}
