use super::{FallbackClient, FallbackResponse};
use async_trait::async_trait;
use axum::http::StatusCode;
use query_engine_rust::engine_api::{ms_to_secs, QueryEval, QueryRequest};
use reqwest::Client;
use serde_json::Value;
use tracing::{debug, error};

/// Fallback client for Prometheus HTTP API
pub struct PrometheusHttpFallback {
    client: Client,
    base_url: String,
    timeout: std::time::Duration,
}

impl PrometheusHttpFallback {
    pub fn new(base_url: String, timeout_secs: u64) -> Self {
        Self {
            client: Client::new(),
            base_url,
            timeout: std::time::Duration::from_secs(timeout_secs),
        }
    }

    /// `/api/v1/query` parameters, times in Prometheus float seconds.
    fn instant_query_params(query: &str, time_ms: i64) -> Vec<(&'static str, String)> {
        vec![
            ("query", query.to_string()),
            ("time", ms_to_secs(time_ms).to_string()),
        ]
    }

    /// `/api/v1/query_range` parameters, times in Prometheus float seconds.
    fn range_query_params(
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Vec<(&'static str, String)> {
        vec![
            ("query", query.to_string()),
            ("start", ms_to_secs(start_ms).to_string()),
            ("end", ms_to_secs(end_ms).to_string()),
            ("step", ms_to_secs(step_ms).to_string()),
        ]
    }

    async fn forward_instant_query(
        &self,
        query: &str,
        time_ms: i64,
    ) -> Result<FallbackResponse, StatusCode> {
        debug!("=== FORWARDING TO PROMETHEUS ===");
        debug!("Forwarding query: '{}', time_ms: {}", query, time_ms);

        // Build the full URL for the Prometheus endpoint
        let full_url = format!("{}/api/v1/query", self.base_url.trim_end_matches('/'));

        debug!("Full forwarding URL: {}", full_url);

        // Prepare query parameters for forwarding
        let query_params = Self::instant_query_params(query, time_ms);

        debug!("Final query parameters for forwarding: {:?}", query_params);

        // Forward the request to Prometheus
        debug!("Sending request to Prometheus...");
        match self
            .client
            .get(&full_url)
            .query(&query_params)
            .timeout(self.timeout)
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

    async fn forward_range_query(
        &self,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<FallbackResponse, StatusCode> {
        debug!("=== FORWARDING RANGE QUERY TO PROMETHEUS ===");
        debug!(
            "Forwarding range query: '{}', start_ms: {}, end_ms: {}, step_ms: {}",
            query, start_ms, end_ms, step_ms
        );

        let full_url = format!("{}/api/v1/query_range", self.base_url.trim_end_matches('/'));

        let query_params = Self::range_query_params(query, start_ms, end_ms, step_ms);

        match self
            .client
            .get(&full_url)
            .query(&query_params)
            .timeout(self.timeout)
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                debug!(
                    "Received range query response from Prometheus, status: {}",
                    status
                );
                match response.json::<Value>().await {
                    Ok(prometheus_response) => {
                        debug!("=== PROMETHEUS RANGE FORWARD SUCCESS ===");
                        Ok(FallbackResponse::Json(prometheus_response))
                    }
                    Err(parse_err) => {
                        error!(
                            "Failed to parse Prometheus range query response: {}",
                            parse_err
                        );
                        use crate::drivers::query::adapters::PrometheusResponse;
                        let error = PrometheusResponse::error(
                            "internal",
                            "Failed to parse Prometheus range query response",
                        );
                        Ok(FallbackResponse::Json(serde_json::to_value(error).unwrap()))
                    }
                }
            }
            Err(req_err) => {
                error!("Failed to forward range query to Prometheus: {}", req_err);
                use crate::drivers::query::adapters::PrometheusResponse;
                let error = PrometheusResponse::error(
                    "internal",
                    &format!("Failed to forward range query to Prometheus: {}", req_err),
                );
                Ok(FallbackResponse::Json(serde_json::to_value(error).unwrap()))
            }
        }
    }
}

#[async_trait]
impl FallbackClient for PrometheusHttpFallback {
    async fn execute_query(&self, request: &QueryRequest) -> Result<FallbackResponse, StatusCode> {
        match request.eval {
            QueryEval::Instant { time_ms } => {
                self.forward_instant_query(&request.query, time_ms).await
            }
            QueryEval::Range {
                start_ms,
                end_ms,
                step_ms,
            } => {
                self.forward_range_query(&request.query, start_ms, end_ms, step_ms)
                    .await
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
        match self.client.get(&url).timeout(self.timeout).send().await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use query_engine_rust::engine_api::secs_to_ms;

    // Millisecond-aligned protocol times, including ones whose `* 1000.0`
    // is not exact (1095211880.820 -> 1095211880819.9998).
    const MS_ALIGNED_SECS: [&str; 6] = [
        "1700000000",
        "1700000000.1",
        "1700000000.123",
        "1095211880.820",
        "2189515850.298",
        "15",
    ];

    /// Forwarding reformats seconds as `f64::to_string()` of the parsed value,
    /// so for ms-aligned input it must equal what the f64-seconds path sent.
    #[test]
    fn instant_params_match_f64_seconds_formatting() {
        for s in MS_ALIGNED_SECS {
            let secs: f64 = s.parse().unwrap();
            let params = PrometheusHttpFallback::instant_query_params("up", secs_to_ms(secs));
            assert_eq!(
                params,
                vec![("query", "up".to_string()), ("time", secs.to_string())],
                "{s}"
            );
        }
    }

    #[test]
    fn range_params_match_f64_seconds_formatting() {
        for s in MS_ALIGNED_SECS {
            let secs: f64 = s.parse().unwrap();
            let ms = secs_to_ms(secs);
            let params = PrometheusHttpFallback::range_query_params("up", ms, ms + 1, ms);
            assert_eq!(
                params,
                vec![
                    ("query", "up".to_string()),
                    ("start", secs.to_string()),
                    ("end", ms_to_secs(ms + 1).to_string()),
                    ("step", secs.to_string()),
                ],
                "{s}"
            );
        }
    }
}
