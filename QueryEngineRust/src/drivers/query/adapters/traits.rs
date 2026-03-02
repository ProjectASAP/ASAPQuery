use async_trait::async_trait;
use axum::{
    body::Bytes,
    extract::{Form, Query},
    http::StatusCode,
    response::{Json, Response},
};
use promql_utilities::data_model::KeyByLabelNames;
use serde_json::Value;
use std::collections::HashMap;

use crate::engines::QueryResult;

/// Parsed query request data ready for engine processing
#[derive(Debug, Clone)]
pub struct ParsedQueryRequest {
    pub query: String,
    pub time: f64,
}

/// Parsed range query request with validated parameters
#[derive(Debug, Clone)]
pub struct ParsedRangeQueryRequest {
    pub query: String,
    pub start: f64, // epoch seconds
    pub end: f64,   // epoch seconds
    pub step: f64,  // seconds, must be multiple of tumbling window
}

/// Result of query execution (before formatting for protocol)
#[derive(Debug, Clone)]
pub struct QueryExecutionResult {
    pub query_output_labels: KeyByLabelNames,
    pub query_result: QueryResult,
}

/// Error types for adapters
#[derive(Debug)]
pub enum AdapterError {
    MissingParameter(String),
    InvalidParameter(String),
    ParseError(String),
    NetworkError(String),
    ProtocolError(String),
}

impl std::fmt::Display for AdapterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdapterError::MissingParameter(p) => write!(f, "Missing parameter: {}", p),
            AdapterError::InvalidParameter(p) => write!(f, "Invalid parameter: {}", p),
            AdapterError::ParseError(e) => write!(f, "Parse error: {}", e),
            AdapterError::NetworkError(e) => write!(f, "Network error: {}", e),
            AdapterError::ProtocolError(e) => write!(f, "Protocol error: {}", e),
        }
    }
}

impl std::error::Error for AdapterError {}

/// Trait for parsing incoming HTTP requests into internal query format
/// Handles Axum extractors directly for different request types (GET/POST)
#[async_trait]
pub trait QueryRequestAdapter: Send + Sync {
    /// Parse a GET request with query parameters
    async fn parse_get_request(
        &self,
        query_params: Query<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError>;

    /// Parse a POST request with form data
    async fn parse_post_request(
        &self,
        form_params: Form<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError>;

    /// Parse a POST request with JSON body (for Elasticsearch)
    /// Default implementation returns an error - adapters that support JSON
    /// POST requests should override this method.
    async fn parse_json_post_request(
        &self,
        _body: Bytes,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        Err(AdapterError::ProtocolError(
            "JSON POST requests not supported by this adapter".to_string(),
        ))
    }

    /// Get the HTTP path this adapter handles (e.g., "/api/v1/query")
    fn get_query_endpoint(&self) -> &'static str;

    /// Parse a GET request for range queries
    async fn parse_range_get_request(
        &self,
        query_params: Query<HashMap<String, String>>,
    ) -> Result<ParsedRangeQueryRequest, AdapterError>;

    /// Parse a POST request for range queries
    async fn parse_range_post_request(
        &self,
        form_params: Form<HashMap<String, String>>,
    ) -> Result<ParsedRangeQueryRequest, AdapterError>;

    /// Get the HTTP path for range queries (e.g., "/api/v1/query_range")
    fn get_range_query_endpoint(&self) -> &'static str;
}

/// Trait for formatting query results into protocol-specific HTTP responses
#[async_trait]
pub trait QueryResponseAdapter: Send + Sync {
    /// Format a successful query result into protocol response
    async fn format_success_response(
        &self,
        result: &QueryExecutionResult,
    ) -> Result<Response, StatusCode>;

    /// Format a successful range query result into protocol response
    async fn format_range_success_response(
        &self,
        result: &QueryResult,
        labels: &KeyByLabelNames,
    ) -> Result<Response, StatusCode>;

    /// Format an error into protocol response
    async fn format_error_response(&self, error: &AdapterError) -> Result<Response, StatusCode>;

    /// Format an error when query returns None (unsupported query)
    async fn format_unsupported_query_response(&self) -> Result<Response, StatusCode>;
}

/// Adapter trait for HTTP-based query protocols
/// (Prometheus HTTP, ClickHouse HTTP, etc.)
///
/// For non-HTTP protocols (Flight SQL, native protocols),
/// define separate adapter traits.
///
/// Note: Fallback logic is handled separately via FallbackClient
#[async_trait]
pub trait HttpProtocolAdapter: QueryRequestAdapter + QueryResponseAdapter + Send + Sync {
    /// Get a descriptive name for this adapter (for logging/debugging)
    fn adapter_name(&self) -> &'static str;

    /// Get the path for the runtime info endpoint
    ///
    /// Example: "/api/v1/status/runtimeinfo" for Prometheus
    fn get_runtime_info_path(&self) -> &'static str;

    /// Handle runtime info request
    ///
    /// The adapter can query the store for internal metrics and
    /// optionally forward to fallback backend for additional info.
    async fn handle_runtime_info(
        &self,
        store: std::sync::Arc<dyn crate::stores::Store>,
    ) -> Result<Json<Value>, StatusCode>;

    async fn handle_runtime_info_with_headers(
        &self,
        store: std::sync::Arc<dyn crate::stores::Store>,
        headers: HashMap<String, String>,
    ) -> Result<Json<Value>, StatusCode> {
        // Default implementation ignores headers and calls the old method
        let _ = headers;
        self.handle_runtime_info(store).await
    }
}
