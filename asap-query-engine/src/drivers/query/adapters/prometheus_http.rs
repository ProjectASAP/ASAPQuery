use super::config::AdapterConfig;
use super::traits::*;
use crate::engines::QueryResult;
use crate::utils::http::{convert_query_result_to_prometheus, convert_range_result_to_prometheus};
use async_trait::async_trait;
use axum::{
    extract::{Form, Query},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use promql_utilities::data_model::KeyByLabelNames;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error};

/// Prometheus-compatible response structure
#[derive(Debug, Serialize, Deserialize)]
pub struct PrometheusResponse {
    pub status: String,
    pub data: Option<Value>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl PrometheusResponse {
    pub fn success(data: Value) -> Self {
        Self {
            status: "success".to_string(),
            data: Some(data),
            error_type: None,
            error: None,
        }
    }

    pub fn error(error_type: &str, error: &str) -> Self {
        Self {
            status: "error".to_string(),
            data: None,
            error_type: Some(error_type.to_string()),
            error: Some(error.to_string()),
        }
    }
}

/// Prometheus HTTP protocol adapter
pub struct PrometheusHttpAdapter {
    config: AdapterConfig,
}

impl PrometheusHttpAdapter {
    pub fn new(config: AdapterConfig) -> Self {
        Self { config }
    }

    /// Helper to parse query parameters (used by both GET and POST)
    fn parse_params(
        &self,
        params: &HashMap<String, String>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        let query = params
            .get("query")
            .ok_or_else(|| AdapterError::MissingParameter("query".to_string()))?
            .clone();

        let time = if let Some(time_str) = params.get("time") {
            time_str.parse::<f64>().map_err(|e| {
                AdapterError::InvalidParameter(format!("Invalid time parameter: {}", e))
            })?
        } else {
            // Use current time as default
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64()
        };

        Ok(ParsedQueryRequest { query, time })
    }

    /// Helper to parse range query parameters
    fn parse_range_params(
        &self,
        params: &HashMap<String, String>,
    ) -> Result<ParsedRangeQueryRequest, AdapterError> {
        let query = params
            .get("query")
            .ok_or_else(|| AdapterError::MissingParameter("query".to_string()))?
            .clone();

        let start = params
            .get("start")
            .ok_or_else(|| AdapterError::MissingParameter("start".to_string()))?
            .parse::<f64>()
            .map_err(|e| AdapterError::InvalidParameter(format!("Invalid start: {}", e)))?;

        let end = params
            .get("end")
            .ok_or_else(|| AdapterError::MissingParameter("end".to_string()))?
            .parse::<f64>()
            .map_err(|e| AdapterError::InvalidParameter(format!("Invalid end: {}", e)))?;

        let step = params
            .get("step")
            .ok_or_else(|| AdapterError::MissingParameter("step".to_string()))?
            .parse::<f64>()
            .map_err(|e| AdapterError::InvalidParameter(format!("Invalid step: {}", e)))?;

        // Basic validation
        if start >= end {
            return Err(AdapterError::InvalidParameter(
                "start must be before end".to_string(),
            ));
        }
        if step <= 0.0 {
            return Err(AdapterError::InvalidParameter(
                "step must be positive".to_string(),
            ));
        }

        Ok(ParsedRangeQueryRequest {
            query,
            start,
            end,
            step,
        })
    }
}

#[async_trait]
impl QueryRequestAdapter for PrometheusHttpAdapter {
    async fn parse_get_request(
        &self,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        debug!(
            "Prometheus adapter: parsing GET request with params: {:?}",
            params
        );
        self.parse_params(&params)
    }

    async fn parse_post_request(
        &self,
        Form(params): Form<HashMap<String, String>>,
    ) -> Result<ParsedQueryRequest, AdapterError> {
        debug!(
            "Prometheus adapter: parsing POST request with params: {:?}",
            params
        );
        self.parse_params(&params)
    }

    fn get_query_endpoint(&self) -> &'static str {
        "/api/v1/query"
    }

    async fn parse_range_get_request(
        &self,
        Query(params): Query<HashMap<String, String>>,
    ) -> Result<ParsedRangeQueryRequest, AdapterError> {
        debug!(
            "Prometheus adapter: parsing range GET request with params: {:?}",
            params
        );
        self.parse_range_params(&params)
    }

    async fn parse_range_post_request(
        &self,
        Form(params): Form<HashMap<String, String>>,
    ) -> Result<ParsedRangeQueryRequest, AdapterError> {
        debug!(
            "Prometheus adapter: parsing range POST request with params: {:?}",
            params
        );
        self.parse_range_params(&params)
    }

    fn get_range_query_endpoint(&self) -> &'static str {
        "/api/v1/query_range"
    }
}

#[async_trait]
impl QueryResponseAdapter for PrometheusHttpAdapter {
    async fn format_success_response(
        &self,
        result: &QueryExecutionResult,
    ) -> Result<Response, StatusCode> {
        debug!("Prometheus adapter: formatting success response");

        let prometheus_data =
            convert_query_result_to_prometheus(&result.query_result, &result.query_output_labels)
                .map_err(|e| {
                error!("Failed to convert query result: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        let response = PrometheusResponse::success(prometheus_data);
        Ok(Json(serde_json::to_value(response).unwrap()).into_response())
    }

    async fn format_range_success_response(
        &self,
        result: &QueryResult,
        labels: &KeyByLabelNames,
    ) -> Result<Response, StatusCode> {
        debug!("Prometheus adapter: formatting range success response");

        let prometheus_data = convert_range_result_to_prometheus(result, labels).map_err(|e| {
            error!("Failed to convert range result: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        let response = PrometheusResponse::success(prometheus_data);
        Ok(Json(serde_json::to_value(response).unwrap()).into_response())
    }

    async fn format_error_response(&self, error: &AdapterError) -> Result<Response, StatusCode> {
        debug!("Prometheus adapter: formatting error response: {:?}", error);

        let (error_type, error_msg) = match error {
            AdapterError::MissingParameter(p) => ("bad_data", format!("Missing parameter: {}", p)),
            AdapterError::InvalidParameter(p) => ("bad_data", format!("Invalid parameter: {}", p)),
            AdapterError::ParseError(e) => ("bad_data", format!("Parse error: {}", e)),
            AdapterError::NetworkError(e) => ("internal", format!("Network error: {}", e)),
            AdapterError::ProtocolError(e) => ("internal", format!("Protocol error: {}", e)),
        };

        let response = PrometheusResponse::error(error_type, &error_msg);
        Ok(Json(serde_json::to_value(response).unwrap()).into_response())
    }

    async fn format_unsupported_query_response(&self) -> Result<Response, StatusCode> {
        debug!("Prometheus adapter: formatting unsupported query response");

        let response = PrometheusResponse::error("bad_data", "No result for query");
        Ok(Json(serde_json::to_value(response).unwrap()).into_response())
    }
}

#[async_trait]
impl HttpProtocolAdapter for PrometheusHttpAdapter {
    fn adapter_name(&self) -> &'static str {
        "PrometheusHTTP"
    }

    fn get_runtime_info_path(&self) -> &'static str {
        "/api/v1/status/runtimeinfo"
    }

    async fn handle_runtime_info(
        &self,
        store: Arc<dyn crate::stores::Store>,
    ) -> Result<Json<Value>, StatusCode> {
        debug!("Handling runtime info request in Prometheus adapter");

        // Get earliest timestamp per aggregation ID from store
        let earliest_timestamps = match store.get_earliest_timestamp_per_aggregation_id() {
            Ok(timestamps) => timestamps,
            Err(e) => {
                error!("Error getting earliest timestamps: {}", e);
                HashMap::new()
            }
        };

        // Get runtime info from fallback if available
        let mut runtime_data = if let Some(fallback) = &self.config.fallback {
            debug!("Fetching runtime info from fallback");
            match fallback.get_runtime_info().await {
                Ok(data) => data,
                Err(e) => {
                    error!("Failed to get runtime info from fallback: {:?}", e);
                    json!({})
                }
            }
        } else {
            json!({})
        };

        // Merge local data with fallback data
        if let Some(data_obj) = runtime_data.as_object_mut() {
            data_obj.insert(
                "earliest_timestamp_per_aggregation_id".to_string(),
                serde_json::to_value(earliest_timestamps).unwrap_or(json!({})),
            );
        } else {
            // If runtime_data is not an object, just create a new one with local data
            runtime_data = json!({
                "earliest_timestamp_per_aggregation_id": earliest_timestamps
            });
        }

        debug!("Successfully merged runtime info with local data");

        // Wrap in Prometheus response format
        let response = PrometheusResponse::success(runtime_data);
        Ok(Json(serde_json::to_value(response).unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::enums::{QueryLanguage, QueryProtocol};

    fn create_test_adapter() -> PrometheusHttpAdapter {
        let config = AdapterConfig::new(QueryProtocol::PrometheusHttp, QueryLanguage::promql, None);
        PrometheusHttpAdapter::new(config)
    }

    // Tests for parse_range_params

    #[test]
    fn test_parse_range_params_valid() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.query, "sum(metric)");
        assert_eq!(parsed.start, 1700000000.0);
        assert_eq!(parsed.end, 1700001000.0);
        assert_eq!(parsed.step, 60.0);
    }

    #[test]
    fn test_parse_range_params_missing_query() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::MissingParameter(p) => assert_eq!(p, "query"),
            _ => panic!("Expected MissingParameter error"),
        }
    }

    #[test]
    fn test_parse_range_params_missing_start() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::MissingParameter(p) => assert_eq!(p, "start"),
            _ => panic!("Expected MissingParameter error"),
        }
    }

    #[test]
    fn test_parse_range_params_missing_end() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::MissingParameter(p) => assert_eq!(p, "end"),
            _ => panic!("Expected MissingParameter error"),
        }
    }

    #[test]
    fn test_parse_range_params_missing_step() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("end".to_string(), "1700001000".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::MissingParameter(p) => assert_eq!(p, "step"),
            _ => panic!("Expected MissingParameter error"),
        }
    }

    #[test]
    fn test_parse_range_params_invalid_start() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "not_a_number".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::InvalidParameter(msg) => assert!(msg.contains("start")),
            _ => panic!("Expected InvalidParameter error"),
        }
    }

    #[test]
    fn test_parse_range_params_start_after_end() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700001000".to_string());
        params.insert("end".to_string(), "1700000000".to_string()); // end before start
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::InvalidParameter(msg) => {
                assert!(msg.contains("start must be before end"))
            }
            _ => panic!("Expected InvalidParameter error"),
        }
    }

    #[test]
    fn test_parse_range_params_zero_step() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "0".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::InvalidParameter(msg) => assert!(msg.contains("step must be positive")),
            _ => panic!("Expected InvalidParameter error"),
        }
    }

    #[test]
    fn test_parse_range_params_negative_step() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "-60".to_string());

        let result = adapter.parse_range_params(&params);

        assert!(result.is_err());
        match result.unwrap_err() {
            AdapterError::InvalidParameter(msg) => assert!(msg.contains("step must be positive")),
            _ => panic!("Expected InvalidParameter error"),
        }
    }

    #[test]
    fn test_get_range_query_endpoint() {
        let adapter = create_test_adapter();
        assert_eq!(adapter.get_range_query_endpoint(), "/api/v1/query_range");
    }

    #[tokio::test]
    async fn test_parse_range_get_request() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_get_request(Query(params)).await;

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.query, "sum(metric)");
    }

    #[tokio::test]
    async fn test_parse_range_post_request() {
        let adapter = create_test_adapter();
        let mut params = HashMap::new();
        params.insert("query".to_string(), "sum(metric)".to_string());
        params.insert("start".to_string(), "1700000000".to_string());
        params.insert("end".to_string(), "1700001000".to_string());
        params.insert("step".to_string(), "60".to_string());

        let result = adapter.parse_range_post_request(Form(params)).await;

        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.query, "sum(metric)");
    }
}
