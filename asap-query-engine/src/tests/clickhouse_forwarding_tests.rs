#[cfg(test)]
use crate::data_model::{CleanupPolicy, InferenceConfig, QueryLanguage, StreamingConfig};
use crate::drivers::query::adapters::AdapterConfig;
use crate::drivers::query::servers::http::{HttpServer, HttpServerConfig};
use crate::engines::SimpleEngine;
use crate::stores::simple_map_store::SimpleMapStore;
use reqwest::Client;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};

/// Mock ClickHouse server for testing
async fn start_mock_clickhouse_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    use axum::{http::StatusCode, routing::post, Router};

    async fn mock_query_handler(body: String) -> (StatusCode, String) {
        // Simulate different types of queries based on the SQL content
        if body.contains("error_query") {
            // Return ClickHouse-style error in TSV format
            (
                StatusCode::BAD_REQUEST,
                "Code: 60. DB::Exception: Table doesn't exist".to_string(),
            )
        } else if body.contains("SELECT 1") {
            // Return TSV response (ClickHouse default format)
            (StatusCode::OK, "1\n".to_string())
        } else {
            // Generic success TSV response
            (StatusCode::OK, "success\n".to_string())
        }
    }

    let app = Router::new().route("/", post(mock_query_handler));

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give the server time to start
    sleep(Duration::from_millis(100)).await;
    Ok(())
}

async fn setup_test_server(clickhouse_port: u16, database: &str) -> (HttpServer, u16) {
    let config = HttpServerConfig {
        port: 0, // Use random port
        handle_http_requests: true,
        adapter_config: AdapterConfig::clickhouse_sql(
            format!("http://127.0.0.1:{clickhouse_port}"),
            database.to_string(),
            true, // Always forward for now
        ),
    };

    let inference_config = InferenceConfig::new(QueryLanguage::sql, CleanupPolicy::NoCleanup);
    let streaming_config = Arc::new(StreamingConfig::default());
    let store = Arc::new(SimpleMapStore::new(
        streaming_config.clone(),
        CleanupPolicy::NoCleanup,
    ));
    let query_engine = Arc::new(SimpleEngine::new(
        store.clone(),
        // None,
        inference_config,
        streaming_config.clone(),
        15000, // 15s scrape interval
        QueryLanguage::sql,
    ));

    let server = HttpServer::new(config, query_engine, store, None);
    let actual_port = server
        .start_test_server()
        .await
        .expect("Failed to start test server");

    (server, actual_port)
}

/// Test 13: Full forwarding flow with mock ClickHouse
#[tokio::test]
async fn test_clickhouse_forwarding_instant_query() {
    // Start mock ClickHouse server
    let clickhouse_port = 18123;
    start_mock_clickhouse_server(clickhouse_port).await.unwrap();

    // Start our HTTP server with ClickHouse adapter and forwarding enabled
    let (_server, server_port) = setup_test_server(clickhouse_port, "default").await;

    let client = Client::new();

    // Test forwarding of SQL query via GET
    let response = client
        .get(format!("http://127.0.0.1:{server_port}/clickhouse/query"))
        .query(&[("query", "SELECT 1")])
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let tsv_response = response.text().await.expect("Failed to read response");

    // Verify ClickHouse TSV response
    assert!(!tsv_response.is_empty(), "Response should not be empty");
    // TSV format should contain the value "1"
    assert!(
        tsv_response.contains("1"),
        "Response should contain the value '1'"
    );
}

/// Test 14: Database parameter in URL
#[tokio::test]
async fn test_database_parameter_in_url() {
    // Start mock ClickHouse server
    let clickhouse_port = 18124;
    start_mock_clickhouse_server(clickhouse_port).await.unwrap();

    // Start server with a specific database name
    let (_server, server_port) = setup_test_server(clickhouse_port, "test_db").await;

    let client = Client::new();

    // Send a query
    let response = client
        .get(format!("http://127.0.0.1:{server_port}/clickhouse/query"))
        .query(&[("query", "SELECT 1")])
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    // The mock server should have received the request with the database parameter
    // Since we can't directly verify the URL, we verify the response is TSV
    let tsv_response = response.text().await.expect("Failed to read response");
    assert!(!tsv_response.is_empty(), "TSV response should not be empty");
}

/// Test 15: Error forwarding from ClickHouse
#[tokio::test]
async fn test_error_handling() {
    // Start mock ClickHouse server
    let clickhouse_port = 18125;
    start_mock_clickhouse_server(clickhouse_port).await.unwrap();

    // Start our HTTP server
    let (_server, server_port) = setup_test_server(clickhouse_port, "default").await;

    let client = Client::new();

    // Test forwarding of query that causes error
    let response = client
        .get(format!("http://127.0.0.1:{server_port}/clickhouse/query"))
        .query(&[("query", "SELECT * FROM error_query")])
        .send()
        .await
        .expect("Failed to send request");

    // ClickHouse errors should return HTTP error status codes
    assert!(
        !response.status().is_success(),
        "Error query should return error status (got: {})",
        response.status()
    );
    assert_eq!(
        response.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "Should return BAD_REQUEST for ClickHouse error"
    );
}

/// Test 16: Unreachable ClickHouse server
#[tokio::test]
async fn test_server_unreachable() {
    // Don't start a mock server - use a port that's not listening
    let clickhouse_port = 19999;

    // Start our HTTP server pointing to non-existent ClickHouse
    let (_server, server_port) = setup_test_server(clickhouse_port, "default").await;

    let client = Client::new();

    // Try to query
    let response = client
        .get(format!("http://127.0.0.1:{server_port}/clickhouse/query"))
        .query(&[("query", "SELECT 1")])
        .send()
        .await
        .expect("Failed to send request");

    // Should return an error status (likely BAD_GATEWAY or similar)
    assert!(
        !response.status().is_success(),
        "Unreachable server should return error status"
    );
}

/// Test 17: Fallback is always used (no local execution)
#[tokio::test]
async fn test_fallback_always_used() {
    // Start mock ClickHouse server
    let clickhouse_port = 18126;
    start_mock_clickhouse_server(clickhouse_port).await.unwrap();

    // Start our HTTP server
    let (_server, server_port) = setup_test_server(clickhouse_port, "default").await;

    let client = Client::new();

    // Send any query - it should always be forwarded to fallback
    let response = client
        .get(format!("http://127.0.0.1:{server_port}/clickhouse/query"))
        .query(&[("query", "SELECT 1")])
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "Query should be forwarded successfully"
    );

    let tsv_response = response.text().await.expect("Failed to read response");

    // Verify we got a TSV response (from fallback)
    assert!(
        !tsv_response.is_empty(),
        "Should receive TSV format from fallback"
    );
}

/// Test 18: POST request support
#[tokio::test]
async fn test_post_request() {
    // Start mock ClickHouse server
    let clickhouse_port = 18127;
    start_mock_clickhouse_server(clickhouse_port).await.unwrap();

    // Start our HTTP server
    let (_server, server_port) = setup_test_server(clickhouse_port, "default").await;

    let client = Client::new();

    // Test POST request with form data
    let response = client
        .post(format!("http://127.0.0.1:{server_port}/clickhouse/query"))
        .form(&[("query", "SELECT 1")])
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let tsv_response = response.text().await.expect("Failed to read response");

    // Verify ClickHouse TSV response
    assert!(!tsv_response.is_empty(), "TSV response should not be empty");
}

/// Test 19: Missing query parameter
#[tokio::test]
async fn test_missing_query_parameter() {
    // Start mock ClickHouse server
    let clickhouse_port = 18128;
    start_mock_clickhouse_server(clickhouse_port).await.unwrap();

    // Start our HTTP server
    let (_server, server_port) = setup_test_server(clickhouse_port, "default").await;

    let client = Client::new();

    // Send request without query parameter
    let response = client
        .get(format!("http://127.0.0.1:{server_port}/clickhouse/query"))
        .send()
        .await
        .expect("Failed to send request");

    // Should return error for missing parameter
    assert!(
        !response.status().is_success(),
        "Missing query parameter should return error"
    );
}
