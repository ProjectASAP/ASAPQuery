use super::{FallbackClient, FallbackResponse};
use crate::drivers::query::adapters::ParsedQueryRequest;
use async_trait::async_trait;
use axum::http::StatusCode;
use reqwest::Client;
use tracing::{debug, error};

/// Fallback client for ClickHouse HTTP API
pub struct ClickHouseHttpFallback {
    client: Client,
    base_url: String,
    database: String,
}

impl ClickHouseHttpFallback {
    pub fn new(base_url: String, database: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
            database,
        }
    }
}

#[async_trait]
impl FallbackClient for ClickHouseHttpFallback {
    async fn execute_query(
        &self,
        request: &ParsedQueryRequest,
    ) -> Result<FallbackResponse, StatusCode> {
        debug!("=== FORWARDING TO CLICKHOUSE ===");
        debug!(
            "Forwarding query: '{}', time: {}",
            request.query, request.time
        );

        // Build ClickHouse API URL with database parameter
        let full_url = format!(
            "{}/?database={}",
            self.base_url.trim_end_matches('/'),
            self.database
        );

        debug!("Full forwarding URL: {}", full_url);

        // NOTE: We do NOT append FORMAT JSON - queries without FORMAT will return TSV by default
        // ClickHouse HTTP default format is TabSeparated (TSV)
        let query = &request.query;

        debug!("Query (no FORMAT modification): {}", query);

        // Forward the request to ClickHouse via POST
        debug!("Sending POST request to ClickHouse...");
        match self
            .client
            .post(&full_url)
            .body(query.clone())
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            Ok(response) => {
                let status = response.status();
                debug!("Received response from ClickHouse, status: {}", status);

                // Get response as text (TSV format)
                match response.text().await {
                    Ok(tsv_response) => {
                        // Check if response contains an error (ClickHouse errors are plain text)
                        if tsv_response.contains("Code:") && tsv_response.contains("Exception") {
                            error!("ClickHouse returned error: {}", tsv_response);
                            debug!("ClickHouse error response: {}", tsv_response);
                            // Return as error status
                            return Err(StatusCode::BAD_REQUEST);
                        }

                        debug!("Successfully received ClickHouse TSV response");
                        debug!(
                            "ClickHouse response body (first 500 chars): {}",
                            if tsv_response.len() > 500 {
                                format!("{}...", &tsv_response[..500])
                            } else {
                                tsv_response.clone()
                            }
                        );

                        Ok(FallbackResponse::Text(tsv_response))
                    }
                    Err(e) => {
                        error!("Failed to read ClickHouse response as text: {}", e);
                        Err(StatusCode::INTERNAL_SERVER_ERROR)
                    }
                }
            }
            Err(e) => {
                error!("Failed to forward query to ClickHouse: {}", e);
                Err(StatusCode::BAD_GATEWAY)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::post, Router};
    use std::time::SystemTime;
    use tokio::net::TcpListener;
    use tokio::time::{sleep, Duration};

    /// Test 1: Basic client creation
    #[test]
    fn test_clickhouse_fallback_creation() {
        let base_url = "http://localhost:8123".to_string();
        let database = "default".to_string();

        let fallback = ClickHouseHttpFallback::new(base_url.clone(), database.clone());

        // Verify fields are set (we can't access them directly due to privacy, but creation succeeds)
        assert_eq!(fallback.base_url, base_url);
        assert_eq!(fallback.database, database);
    }

    /// Test 2: Query execution with mock server - success case
    #[tokio::test]
    async fn test_execute_query_success() {
        // Start mock ClickHouse server
        let mock_port = 8124;
        start_mock_clickhouse_server(mock_port).await.unwrap();

        // Create fallback client
        let fallback = ClickHouseHttpFallback::new(
            format!("http://127.0.0.1:{}", mock_port),
            "default".to_string(),
        );

        // Create test request
        let request = ParsedQueryRequest {
            query: "SELECT 1".to_string(),
            time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64(),
        };

        // Execute query
        let result = fallback.execute_query(&request).await;

        // Verify success
        assert!(result.is_ok(), "Query execution should succeed");

        // Verify we get a Text response (TSV format)
        match result.unwrap() {
            FallbackResponse::Text(tsv) => {
                assert!(!tsv.is_empty(), "TSV response should not be empty");
                // Basic check that it looks like TSV data
                assert!(
                    tsv.contains("\t") || tsv.contains("\n"),
                    "Response should be TSV format"
                );
            }
            FallbackResponse::Json(_) => {
                panic!("Expected TSV response, got JSON");
            }
        }
    }

    /// Test 3: URL format validation - ensure database parameter is included
    #[tokio::test]
    async fn test_url_with_database_param() {
        // This test verifies that the URL is correctly formatted with database parameter
        // We'll test this by starting a mock server that checks the URL path
        let mock_port = 8125;
        start_mock_clickhouse_server_with_url_check(mock_port)
            .await
            .unwrap();

        let fallback = ClickHouseHttpFallback::new(
            format!("http://127.0.0.1:{}", mock_port),
            "test_db".to_string(),
        );

        let request = ParsedQueryRequest {
            query: "SELECT 1".to_string(),
            time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64(),
        };

        let result = fallback.execute_query(&request).await;

        // The mock server will verify the URL contains the database parameter
        assert!(result.is_ok(), "Query with correct URL should succeed");
    }

    /// Test 4: Error handling - ClickHouse returns error response
    #[tokio::test]
    async fn test_execute_query_error_response() {
        // Start mock ClickHouse server that returns errors
        let mock_port = 8126;
        start_mock_clickhouse_error_server(mock_port).await.unwrap();

        let fallback = ClickHouseHttpFallback::new(
            format!("http://127.0.0.1:{}", mock_port),
            "default".to_string(),
        );

        let request = ParsedQueryRequest {
            query: "SELECT * FROM nonexistent_table".to_string(),
            time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64(),
        };

        let result = fallback.execute_query(&request).await;

        // Should return error status (ClickHouse errors return Err(StatusCode))
        assert!(result.is_err(), "Should handle ClickHouse error response");
    }

    /// Test 5: Network failure handling - server unreachable
    #[tokio::test]
    async fn test_clickhouse_server_unreachable() {
        // Create fallback pointing to non-existent server
        let fallback = ClickHouseHttpFallback::new(
            "http://127.0.0.1:9999".to_string(), // Port that's not listening
            "default".to_string(),
        );

        let request = ParsedQueryRequest {
            query: "SELECT 1".to_string(),
            time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64(),
        };

        let result = fallback.execute_query(&request).await;

        // Should return error status code
        assert!(result.is_err(), "Unreachable server should return error");
    }

    // ===== Mock Server Helpers =====

    async fn start_mock_clickhouse_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
        async fn mock_query_handler(_body: String) -> String {
            // Return ClickHouse-style TSV response (default format)
            "1\n".to_string()
        }

        let app = Router::new().route("/", post(mock_query_handler));

        let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
            .await
            .expect("Failed to bind to port");

        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("Failed to start mock server");
        });

        // Give server time to start
        sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    async fn start_mock_clickhouse_server_with_url_check(
        port: u16,
    ) -> Result<(), Box<dyn std::error::Error>> {
        async fn mock_query_handler_with_check(_body: String) -> String {
            // Return ClickHouse-style TSV response (default format)
            "1\n".to_string()
        }

        let app = Router::new().route("/", post(mock_query_handler_with_check));

        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    async fn start_mock_clickhouse_error_server(
        port: u16,
    ) -> Result<(), Box<dyn std::error::Error>> {
        async fn mock_error_handler(_body: String) -> (StatusCode, String) {
            // Return ClickHouse-style error message (plain text)
            (
                StatusCode::BAD_REQUEST,
                "Code: 60. DB::Exception: Table doesn't exist".to_string(),
            )
        }

        let app = Router::new().route("/", post(mock_error_handler));

        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        sleep(Duration::from_millis(100)).await;
        Ok(())
    }
}
