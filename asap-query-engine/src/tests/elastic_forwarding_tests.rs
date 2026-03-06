#[cfg(test)]
use crate::data_model::{CleanupPolicy, InferenceConfig, QueryLanguage, StreamingConfig};
use crate::drivers::query::adapters::AdapterConfig;
use crate::drivers::query::servers::http::{HttpServer, HttpServerConfig};
use crate::engines::SimpleEngine;
use crate::stores::simple_map_store::SimpleMapStore;
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};

/// Mock Elasticsearch server for testing Query DSL
async fn start_mock_elasticsearch_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    use axum::{http::StatusCode, response::Json, routing::post, Router};

    async fn mock_handler(Json(body): Json<Value>) -> (StatusCode, Json<Value>) {
        // Simulate different query scenarios based on body content
        let body_str = serde_json::to_string(&body).unwrap_or_default();

        if body_str.contains("error_query") {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": {
                        "type": "parsing_exception",
                        "reason": "Unknown query type"
                    },
                    "status": 400
                })),
            )
        } else {
            (
                StatusCode::OK,
                Json(json!({
                    "took": 5,
                    "timed_out": false,
                    "hits": {
                        "total": {"value": 1, "relation": "eq"},
                        "hits": [{
                            "_index": "test_index",
                            "_id": "1",
                            "_source": {"field1": "value1"}
                        }]
                    }
                })),
            )
        }
    }

    let app = Router::new()
        .route("/_search", post(mock_handler))
        .route("/:index/_search", post(mock_handler));

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give the server time to start
    sleep(Duration::from_millis(100)).await;
    Ok(())
}

/// Mock Elasticsearch SQL server for testing SQL queries
async fn start_mock_elasticsearch_sql_server(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    use axum::{http::StatusCode, response::Json, routing::post, Router};

    async fn mock_sql_handler(Json(body): Json<Value>) -> (StatusCode, Json<Value>) {
        let body_str = serde_json::to_string(&body).unwrap_or_default();

        if body_str.contains("error_query") {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": {
                        "type": "parsing_exception",
                        "reason": "Invalid SQL syntax"
                    },
                    "status": 400
                })),
            )
        } else {
            (
                StatusCode::OK,
                Json(json!({
                    "columns": [
                        {"name": "field1", "type": "text"},
                        {"name": "count", "type": "long"}
                    ],
                    "rows": [
                        ["value1", 100],
                        ["value2", 200]
                    ]
                })),
            )
        }
    }

    let app = Router::new().route("/_sql", post(mock_sql_handler));

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    sleep(Duration::from_millis(100)).await;
    Ok(())
}

async fn setup_test_server(elasticsearch_port: u16, index: &str) -> (HttpServer, u16) {
    let config = HttpServerConfig {
        port: 0, // Use random port
        handle_http_requests: true,
        adapter_config: AdapterConfig::elastic_querydsl(
            format!("http://127.0.0.1:{elasticsearch_port}"),
            index.to_string(),
            true, // Always forward for now
        ),
    };

    let inference_config =
        InferenceConfig::new(QueryLanguage::elastic_querydsl, CleanupPolicy::NoCleanup);
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
        QueryLanguage::elastic_querydsl,
    ));

    let server = HttpServer::new(config, query_engine, store);
    let actual_port = server
        .start_test_server()
        .await
        .expect("Failed to start test server");

    (server, actual_port)
}

async fn setup_test_server_sql(elasticsearch_port: u16, index: &str) -> (HttpServer, u16) {
    let config = HttpServerConfig {
        port: 0,
        handle_http_requests: true,
        adapter_config: AdapterConfig::elastic_sql(
            format!("http://127.0.0.1:{elasticsearch_port}"),
            index.to_string(),
            true, // Always forward for now
        ),
    };

    let inference_config =
        InferenceConfig::new(QueryLanguage::elastic_sql, CleanupPolicy::NoCleanup);
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
        15000,
        QueryLanguage::elastic_sql,
    ));

    let server = HttpServer::new(config, query_engine, store);
    let actual_port = server
        .start_test_server()
        .await
        .expect("Failed to start test server");

    (server, actual_port)
}

/// Test 1: Full forwarding flow with mock Elasticsearch
#[tokio::test]
async fn test_elasticsearch_forwarding_search() {
    // Start mock Elasticsearch server
    let elasticsearch_port = 19200;
    start_mock_elasticsearch_server(elasticsearch_port)
        .await
        .unwrap();

    // Start our HTTP server with Elasticsearch adapter and forwarding enabled
    let (_server, server_port) = setup_test_server(elasticsearch_port, "test_index").await;

    let client = Client::new();

    // Test forwarding of search query
    let search_body = json!({
        "query": {
            "match_all": {}
        }
    });

    let response = client
        .post(format!("http://127.0.0.1:{server_port}/_search"))
        .json(&search_body)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let json_response: Value = response.json().await.expect("Failed to read response");

    // Verify Elasticsearch response structure
    assert!(json_response.get("hits").is_some());
    assert_eq!(json_response["hits"]["total"]["value"], 1);
    assert_eq!(
        json_response["hits"]["hits"][0]["_source"]["field1"],
        "value1"
    );
}

/// Test 2: Error handling from Elasticsearch
#[tokio::test]
async fn test_error_handling() {
    // Start mock Elasticsearch server
    let elasticsearch_port = 19201;
    start_mock_elasticsearch_server(elasticsearch_port)
        .await
        .unwrap();

    // Start our HTTP server
    let (_server, server_port) = setup_test_server(elasticsearch_port, "test_index").await;

    let client = Client::new();

    // Test forwarding of query that causes error
    let error_body = json!({
        "query": {
            "error_query": {}
        }
    });

    let response = client
        .post(format!("http://127.0.0.1:{server_port}/_search"))
        .json(&error_body)
        .send()
        .await
        .expect("Failed to send request");

    let status = response.status();
    let json_response: Value = response.json().await.expect("Failed to parse JSON");

    // Elasticsearch errors can be returned as JSON with OK status or with error status
    // Check for either case
    assert!(
        !status.is_success() || json_response.get("error").is_some(),
        "Error query should return error status or error in JSON (got status: {}, body: {})",
        status,
        json_response
    );

    // Verify error structure if present in response
    if let Some(error) = json_response.get("error") {
        assert_eq!(error["type"], "parsing_exception");
    }
}

/// Test 3: Unreachable Elasticsearch server
#[tokio::test]
async fn test_server_unreachable() {
    // Don't start a mock server - use a port that's not listening
    let elasticsearch_port = 19999;

    // Start our HTTP server pointing to non-existent Elasticsearch
    let (_server, server_port) = setup_test_server(elasticsearch_port, "test_index").await;

    let client = Client::new();

    let search_body = json!({
        "query": {
            "match_all": {}
        }
    });

    // Try to query
    let response = client
        .post(format!("http://127.0.0.1:{server_port}/_search"))
        .json(&search_body)
        .send()
        .await
        .expect("Failed to send request");

    // Should return an error status (likely BAD_GATEWAY or similar)
    assert!(
        !response.status().is_success(),
        "Unreachable server should return error status (got: {})",
        response.status()
    );
}

/// Test 4: Fallback is always used (no local execution)
#[tokio::test]
async fn test_fallback_always_used() {
    // Start mock Elasticsearch server
    let elasticsearch_port = 19202;
    start_mock_elasticsearch_server(elasticsearch_port)
        .await
        .unwrap();

    // Start our HTTP server
    let (_server, server_port) = setup_test_server(elasticsearch_port, "test_index").await;

    let client = Client::new();

    // Send any query - it should always be forwarded to fallback
    let search_body = json!({
        "query": {
            "match_all": {}
        }
    });

    let response = client
        .post(format!("http://127.0.0.1:{server_port}/_search"))
        .json(&search_body)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(
        response.status(),
        reqwest::StatusCode::OK,
        "Query should be forwarded successfully"
    );

    let json_response: Value = response.json().await.expect("Failed to parse JSON");

    // Verify we got a proper Elasticsearch response (from fallback)
    assert!(
        json_response.get("hits").is_some(),
        "Should receive Elasticsearch format from fallback"
    );
}

/// Test 5: POST request support
#[tokio::test]
async fn test_post_request() {
    // Start mock Elasticsearch server
    let elasticsearch_port = 19203;
    start_mock_elasticsearch_server(elasticsearch_port)
        .await
        .unwrap();

    // Start our HTTP server
    let (_server, server_port) = setup_test_server(elasticsearch_port, "test_index").await;

    let client = Client::new();

    let search_body = json!({
        "query": {
            "term": {"status": "active"}
        }
    });

    let response = client
        .post(format!("http://127.0.0.1:{server_port}/_search"))
        .json(&search_body)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let json_response: Value = response.json().await.expect("Failed to parse JSON");
    assert!(json_response.get("hits").is_some());
}

/// Test 6: Forwarding disabled
#[tokio::test]
async fn test_forwarding_disabled() {
    let config = HttpServerConfig {
        port: 0,
        handle_http_requests: true,
        adapter_config: AdapterConfig::elastic_querydsl(
            "http://127.0.0.1:19204".to_string(),
            "test_index".to_string(),
            false, // Forwarding disabled
        ),
    };

    let inference_config =
        InferenceConfig::new(QueryLanguage::elastic_querydsl, CleanupPolicy::NoCleanup);
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
        15000,
        QueryLanguage::elastic_querydsl,
    ));

    let server = HttpServer::new(config, query_engine, store);
    let server_port = server
        .start_test_server()
        .await
        .expect("Failed to start test server");

    let client = Client::new();

    let search_body = json!({
        "query": {
            "complex_unsupported_query": {}
        }
    });

    // Test that query returns error when forwarding is disabled
    let response = client
        .post(format!("http://127.0.0.1:{server_port}/_search"))
        .json(&search_body)
        .send()
        .await
        .expect("Failed to send request");

    let status = response.status();
    let response_json: Value = response.json().await.expect("Failed to parse JSON");
    println!("Response JSON: {response_json}");

    // Should return an error response when forwarding is disabled
    assert!(
        !status.is_success() || response_json.get("error").is_some(),
        "Should return error when forwarding is disabled"
    );
}

// ===== SQL-specific tests =====

/// Test 7: SQL query forwarding
#[tokio::test]
async fn test_elasticsearch_sql_forwarding() {
    // Start mock Elasticsearch SQL server
    let elasticsearch_port = 19205;
    start_mock_elasticsearch_sql_server(elasticsearch_port)
        .await
        .unwrap();

    // Start our HTTP server with SQL adapter
    let (_server, server_port) = setup_test_server_sql(elasticsearch_port, "test_index").await;

    let client = Client::new();

    // Test SQL query
    let sql_body = json!({
        "query": "SELECT * FROM logs WHERE status = 200"
    });

    let response = client
        .post(format!("http://127.0.0.1:{server_port}/_sql"))
        .json(&sql_body)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let json_response: Value = response.json().await.expect("Failed to read response");

    // Verify SQL response structure
    assert!(json_response.get("columns").is_some());
    assert!(json_response.get("rows").is_some());
    assert_eq!(json_response["rows"].as_array().unwrap().len(), 2);
}

/// Test 8: SQL error handling
#[tokio::test]
async fn test_sql_error_handling() {
    let elasticsearch_port = 19206;
    start_mock_elasticsearch_sql_server(elasticsearch_port)
        .await
        .unwrap();

    let (_server, server_port) = setup_test_server_sql(elasticsearch_port, "test_index").await;

    let client = Client::new();

    let error_body = json!({
        "query": "SELECT error_query FROM invalid"
    });

    let response = client
        .post(format!("http://127.0.0.1:{server_port}/_sql"))
        .json(&error_body)
        .send()
        .await
        .expect("Failed to send request");

    let status = response.status();
    let json_response: Value = response.json().await.expect("Failed to parse JSON");

    assert!(
        !status.is_success() || json_response.get("error").is_some(),
        "SQL error query should return error"
    );

    if let Some(error) = json_response.get("error") {
        assert_eq!(error["type"], "parsing_exception");
    }
}

/// Test 9: SQL with parameters
#[tokio::test]
async fn test_sql_with_parameters() {
    let elasticsearch_port = 19207;
    start_mock_elasticsearch_sql_server(elasticsearch_port)
        .await
        .unwrap();

    let (_server, server_port) = setup_test_server_sql(elasticsearch_port, "test_index").await;

    let client = Client::new();

    let sql_body = json!({
        "query": "SELECT status, COUNT(*) FROM logs GROUP BY status",
        "fetch_size": 100,
        "time_zone": "UTC"
    });

    let response = client
        .post(format!("http://127.0.0.1:{server_port}/_sql"))
        .json(&sql_body)
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let json_response: Value = response.json().await.expect("Failed to parse JSON");
    assert!(json_response.get("columns").is_some());
}

/// Test 10: SQL forwarding disabled
#[tokio::test]
async fn test_sql_forwarding_disabled() {
    let config = HttpServerConfig {
        port: 0,
        handle_http_requests: true,
        adapter_config: AdapterConfig::elastic_sql(
            "http://127.0.0.1:19208".to_string(),
            "test_index".to_string(),
            false, // Forwarding disabled
        ),
    };

    let inference_config =
        InferenceConfig::new(QueryLanguage::elastic_sql, CleanupPolicy::NoCleanup);
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
        15000,
        QueryLanguage::elastic_sql,
    ));

    let server = HttpServer::new(config, query_engine, store);
    let server_port = server
        .start_test_server()
        .await
        .expect("Failed to start test server");

    let client = Client::new();

    let sql_body = json!({
        "query": "SELECT * FROM logs"
    });

    let response = client
        .post(format!("http://127.0.0.1:{server_port}/_sql"))
        .json(&sql_body)
        .send()
        .await
        .expect("Failed to send request");

    let status = response.status();
    let response_json: Value = response.json().await.expect("Failed to parse JSON");

    assert!(
        !status.is_success() || response_json.get("error").is_some(),
        "Should return error when SQL forwarding is disabled"
    );
}
