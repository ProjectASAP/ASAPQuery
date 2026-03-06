#[cfg(test)]
use crate::data_model::{CleanupPolicy, InferenceConfig, QueryLanguage, StreamingConfig};
use crate::drivers::query::adapters::AdapterConfig;
use crate::drivers::query::servers::http::{HttpServer, HttpServerConfig};
use crate::engines::SimpleEngine;
use crate::stores::simple_map_store::SimpleMapStore;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};

/// Mock Prometheus server for testing
async fn start_mock_prometheus_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    use axum::{extract::Query, response::Json, routing::get, Router};
    use serde_json::json;
    use std::collections::HashMap;

    async fn mock_query_handler(Query(params): Query<HashMap<String, String>>) -> Json<Value> {
        let query = params.get("query").unwrap_or(&"".to_string()).clone();

        // Simulate different types of queries
        if query.contains("unsupported_metric") {
            Json(json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {"__name__": "unsupported_metric"},
                            "value": [1672531200, "42.0"]
                        }
                    ]
                }
            }))
        } else if query.contains("error_query") {
            Json(json!({
                "status": "error",
                "errorType": "bad_data",
                "error": "invalid query syntax"
            }))
        } else {
            Json(json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": []
                }
            }))
        }
    }

    let app = Router::new().route("/api/v1/query", get(mock_query_handler));

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give the server time to start
    sleep(Duration::from_millis(100)).await;
    Ok(())
}

async fn setup_test_server(prometheus_port: u16) -> (HttpServer, u16) {
    let config = HttpServerConfig {
        port: 0, // Use random port
        handle_http_requests: true,
        adapter_config: AdapterConfig::prometheus_promql(
            format!("http://127.0.0.1:{prometheus_port}"),
            true,
        ),
    };

    let inference_config = InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
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
        crate::data_model::QueryLanguage::promql,
    ));

    let server = HttpServer::new(config, query_engine, store);
    let actual_port = server
        .start_test_server()
        .await
        .expect("Failed to start test server");

    (server, actual_port)
}

#[tokio::test]
async fn test_prometheus_forwarding_instant_query() {
    // Start mock Prometheus server
    let prometheus_port = 19090;
    start_mock_prometheus_server(prometheus_port).await.unwrap();

    // Start our HTTP server with forwarding enabled
    let (_server, server_port) = setup_test_server(prometheus_port).await;

    let client = Client::new();

    // Test forwarding of unsupported query
    let response = client
        .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
        .query(&[("query", "unsupported_metric")])
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let json_response: Value = response.json().await.expect("Failed to parse JSON");
    assert_eq!(json_response["status"], "success");
    assert_eq!(json_response["data"]["resultType"], "vector");

    // Verify the forwarded response contains the expected data
    let result = &json_response["data"]["result"][0];
    assert_eq!(result["metric"]["__name__"], "unsupported_metric");
    assert_eq!(result["value"][1], "42.0");
}

#[tokio::test]
async fn test_prometheus_forwarding_error_handling() {
    // Start mock Prometheus server
    let prometheus_port = 19092;
    start_mock_prometheus_server(prometheus_port).await.unwrap();

    // Start our HTTP server with forwarding enabled
    let (_server, server_port) = setup_test_server(prometheus_port).await;

    let client = Client::new();

    // Test forwarding of query that causes error in Prometheus
    let response = client
        .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
        .query(&[("query", "error_query")])
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let json_response: Value = response.json().await.expect("Failed to parse JSON");
    assert_eq!(json_response["status"], "error");
    assert_eq!(json_response["errorType"], "bad_data");
    assert_eq!(json_response["error"], "invalid query syntax");
}

#[tokio::test]
async fn test_forwarding_disabled() {
    let config = HttpServerConfig {
        port: 0,
        handle_http_requests: true,
        adapter_config: AdapterConfig::prometheus_promql(
            "http://127.0.0.1:19093".to_string(),
            false, // Forwarding disabled
        ),
    };

    let inference_config = InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
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
        crate::data_model::QueryLanguage::promql,
    ));

    let server = HttpServer::new(config, query_engine, store);
    let server_port = server
        .start_test_server()
        .await
        .expect("Failed to start test server");

    let client = Client::new();

    // Test that unsupported query returns error when forwarding is disabled
    let response = client
        .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
        .query(&[(
            "query",
            "definitely_unsupported_complex_query{invalid=syntax}",
        )])
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let response_json: serde_json::Value = response.json().await.expect("Failed to parse JSON");
    println!("Response JSON: {response_json}");

    // Should return an error response when forwarding is disabled
    assert_eq!(response_json["status"], "error");
}

#[tokio::test]
async fn test_prometheus_server_unreachable() {
    // Use an unreachable port for Prometheus
    let config = HttpServerConfig {
        port: 0,
        handle_http_requests: true,
        adapter_config: AdapterConfig::prometheus_promql(
            "http://127.0.0.1:99999".to_string(), // Unreachable port
            true,
        ),
    };

    let inference_config = InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup);
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
        crate::data_model::QueryLanguage::promql,
    ));

    let server = HttpServer::new(config, query_engine, store);
    let server_port = server
        .start_test_server()
        .await
        .expect("Failed to start test server");

    let client = Client::new();

    // Test that unreachable Prometheus server returns error when forwarding fails
    let response = client
        .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
        .query(&[(
            "query",
            "definitely_unsupported_complex_query{invalid=syntax}",
        )])
        .send()
        .await
        .expect("Failed to send request");

    // Should return 500 or similar error status when Prometheus is unreachable
    let status = response.status();
    println!("Response status: {status:?}");
    let response_json: serde_json::Value = response.json().await.expect("Failed to parse JSON");
    println!("Response JSON: {response_json}");

    // When Prometheus is unreachable, should return 500 status or error response
    assert!(
        status.is_server_error()
            || (status == reqwest::StatusCode::OK && response_json["status"] == "error")
    );
}
