use super::{FallbackClient, FallbackResponse};
use crate::drivers::query::adapters::ParsedQueryRequest;
use async_trait::async_trait;
use axum::http::StatusCode;
use reqwest::Client;
use serde_json::Value;
use tracing::{debug, error};

/// Fallback client for Prometheus HTTP API
pub struct PrometheusHttpFallback {
    client: Client,
    base_url: String,
}

impl PrometheusHttpFallback {
    pub fn new(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
        }
    }
}

#[async_trait]
impl FallbackClient for PrometheusHttpFallback {
    async fn execute_query(
        &self,
        request: &ParsedQueryRequest,
    ) -> Result<FallbackResponse, StatusCode> {
        debug!("=== FORWARDING TO PROMETHEUS ===");
        debug!(
            "Forwarding query: '{}', time: {}",
            request.query, request.time
        );

        // Build the full URL for the Prometheus endpoint
        let full_url = format!("{}/api/v1/query", self.base_url.trim_end_matches('/'));

        debug!("Full forwarding URL: {}", full_url);

        // Prepare query parameters for forwarding
        let query_params = vec![
            ("query", request.query.clone()),
            ("time", request.time.to_string()),
        ];

        debug!("Final query parameters for forwarding: {:?}", query_params);

        // Forward the request to Prometheus
        debug!("Sending request to Prometheus...");
        match self
            .client
            .get(&full_url)
            .query(&query_params)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                debug!("Received response from Prometheus, status: {}", status);
                match response.json::<Value>().await {
                    Ok(prometheus_response) => {
                        debug!(
                            "Successfully parsed Prometheus response: {:?}",
                            prometheus_response
                        );
                        debug!("=== PROMETHEUS FORWARD SUCCESS ===");
                        Ok(FallbackResponse::Json(prometheus_response))
                    }
                    Err(parse_err) => {
                        error!("Failed to parse Prometheus response: {}", parse_err);
                        debug!("=== PROMETHEUS FORWARD PARSE ERROR ===");

                        use crate::drivers::query::adapters::PrometheusResponse;
                        let error = PrometheusResponse::error(
                            "internal",
                            "Failed to parse Prometheus response",
                        );
                        Ok(FallbackResponse::Json(serde_json::to_value(error).unwrap()))
                    }
                }
            }
            Err(req_err) => {
                error!("Failed to forward query to Prometheus: {}", req_err);
                debug!("=== PROMETHEUS FORWARD REQUEST ERROR ===");

                use crate::drivers::query::adapters::PrometheusResponse;
                let error = PrometheusResponse::error(
                    "internal",
                    &format!("Failed to forward query to Prometheus: {}", req_err),
                );
                Ok(FallbackResponse::Json(serde_json::to_value(error).unwrap()))
            }
        }
    }

    async fn get_runtime_info(&self) -> Result<Value, StatusCode> {
        debug!("Fetching runtime info from Prometheus fallback");

        // Build the runtime info URL
        let url = format!(
            "{}/api/v1/status/runtimeinfo",
            self.base_url.trim_end_matches('/')
        );

        debug!("Runtime info URL: {}", url);

        // Send request to Prometheus
        match self
            .client
            .get(&url)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            Ok(response) => {
                match response.text().await {
                    Ok(text) => {
                        debug!("Prometheus runtime info response: {}", text);

                        // Check for VictoriaMetrics unsupported path error
                        if text.contains("unsupported path requested") {
                            debug!("VictoriaMetrics detected - returning empty runtime info");
                            return Ok(serde_json::json!({}));
                        }

                        // Try to parse as JSON
                        match serde_json::from_str::<Value>(&text) {
                            Ok(json) => {
                                // Extract the data field if it exists (Prometheus format)
                                if let Some(data) = json.get("data") {
                                    Ok(data.clone())
                                } else {
                                    Ok(json)
                                }
                            }
                            Err(e) => {
                                error!("Failed to parse runtime info response: {}", e);
                                Ok(serde_json::json!({}))
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to read runtime info response: {}", e);
                        Ok(serde_json::json!({}))
                    }
                }
            }
            Err(e) => {
                error!("Failed to fetch runtime info from Prometheus: {}", e);
                Ok(serde_json::json!({}))
            }
        }
    }
}
