use crate::drivers::query::adapters::{ParsedQueryRequest, ParsedRangeQueryRequest};
use axum::{
    body::Bytes,
    extract::{Form, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tracing::{debug, info};

use crate::drivers::query::adapters::{create_http_adapter, AdapterConfig, HttpProtocolAdapter};
use crate::engines::SimpleEngine;
use crate::stores::Store;

#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub port: u16,
    pub handle_http_requests: bool,
    pub adapter_config: AdapterConfig,
}

#[derive(Clone)]
pub struct HttpServer {
    config: HttpServerConfig,
    query_engine: Arc<SimpleEngine>,
    store: Arc<dyn Store>,
}

#[derive(Clone)]
struct AppState {
    config: HttpServerConfig,
    query_engine: Arc<SimpleEngine>,
    store: Arc<dyn Store>,
    adapter: Arc<dyn HttpProtocolAdapter>,
    fallback: Option<Arc<dyn crate::drivers::query::fallback::FallbackClient>>,
}

impl HttpServer {
    pub fn new(
        config: HttpServerConfig,
        query_engine: Arc<SimpleEngine>,
        store: Arc<dyn Store>,
    ) -> Self {
        Self {
            config,
            query_engine,
            store,
        }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Create adapter using factory
        let adapter = create_http_adapter(self.config.adapter_config.clone());

        let query_endpoint = adapter.get_query_endpoint();
        let runtime_info_path = adapter.get_runtime_info_path();
        info!(
            "Adapter '{}' configured for endpoint: {}",
            adapter.adapter_name(),
            query_endpoint
        );
        info!("Runtime info endpoint: {}", runtime_info_path);

        let app_state = AppState {
            config: self.config.clone(),
            query_engine: self.query_engine,
            store: self.store,
            adapter: adapter.clone(),
            fallback: self.config.adapter_config.fallback.clone(),
        };

        let range_query_endpoint = adapter.get_range_query_endpoint();

        let app = Router::new()
            .route(query_endpoint, get(handle_instant_query))
            .route(query_endpoint, post(handle_instant_query_post))
            .route(range_query_endpoint, get(handle_range_query))
            .route(range_query_endpoint, post(handle_range_query_post))
            .route(runtime_info_path, get(handle_runtime_info))
            .route(runtime_info_path, post(handle_runtime_info))
            .with_state(app_state);

        let listener = TcpListener::bind(format!("0.0.0.0:{}", self.config.port)).await?;
        info!("HTTP server listening on port {}", self.config.port);

        axum::serve(listener, app).await?;
        Ok(())
    }

    /// Start server for testing on a random available port
    /// Returns the actual port number used
    #[cfg(test)]
    pub async fn start_test_server(&self) -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
        // Create adapter using factory
        let adapter = create_http_adapter(self.config.adapter_config.clone());

        let query_endpoint = adapter.get_query_endpoint();
        let runtime_info_path = adapter.get_runtime_info_path();

        let app_state = AppState {
            config: self.config.clone(),
            query_engine: self.query_engine.clone(),
            store: self.store.clone(),
            adapter: adapter.clone(),
            fallback: self.config.adapter_config.fallback.clone(),
        };

        let range_query_endpoint = adapter.get_range_query_endpoint();

        let app = Router::new()
            .route(query_endpoint, get(handle_instant_query))
            .route(query_endpoint, post(handle_instant_query_post))
            .route(range_query_endpoint, get(handle_range_query))
            .route(range_query_endpoint, post(handle_range_query_post))
            .route(runtime_info_path, get(handle_runtime_info))
            .with_state(app_state);

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let actual_port = listener.local_addr()?.port();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Give the server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Ok(actual_port)
    }
}

/// Core query execution logic shared between GET and POST handlers
async fn process_query_request(
    state: &AppState,
    parsed_request: &ParsedQueryRequest,
    start_time: Instant,
    headers: HashMap<String, String>,
) -> Response {
    // Check if handling is enabled
    if !state.config.handle_http_requests {
        debug!("HTTP request handling is disabled");
        if let Some(fallback) = &state.fallback {
            debug!("Forwarding to fallback due to disabled handling");
            return match fallback
                .execute_query_with_headers(parsed_request, headers)
                .await
            {
                Ok(response) => response.into_response(),
                Err(status) => status.into_response(),
            };
        } else {
            debug!("Returning error - both handling and forwarding disabled");
            use crate::drivers::query::adapters::AdapterError;
            return match state
                .adapter
                .format_error_response(&AdapterError::ProtocolError(
                    "Query handling is disabled".to_string(),
                ))
                .await
            {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    }

    // Step 2: Execute query with engine (using parsed request)
    let query_start_time = Instant::now();
    debug!(
        "About to call query_engine.handle_query with query='{}' and time={}",
        parsed_request.query, parsed_request.time
    );
    match state
        .query_engine
        .handle_query(parsed_request.query.clone(), parsed_request.time)
    {
        Some((query_output_labels, query_result)) => {
            let query_duration = query_start_time.elapsed();
            info!("=== QUERY ENGINE SUCCESS ===");
            info!(
                "Query engine execution took: {:.2}ms",
                query_duration.as_secs_f64() * 1000.0
            );
            debug!("Query output labels: {:?}", query_output_labels);
            debug!("Query result: {:?}", query_result);

            // Step 3: Format success response using adapter
            // (Adapter handles protocol-specific formatting, e.g., convert_query_result_to_prometheus)
            use crate::drivers::query::adapters::QueryExecutionResult;
            let execution_result = QueryExecutionResult {
                query_output_labels,
                query_result,
            };

            let total_duration = start_time.elapsed();
            debug!(
                "Total request processing took: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );
            debug!("=== RETURNING SUCCESS RESPONSE ===");

            match state
                .adapter
                .format_success_response(&execution_result)
                .await
            {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            }
        }
        None => {
            let total_duration = start_time.elapsed();
            info!("=== QUERY ENGINE RETURNED NONE ===");
            info!(
                "Request failed after: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );
            info!("SKETCH MISS: query='{}', forwarding to fallback",
            parsed_request.query.chars().take(150).collect::<String>());

            // Step 4: Handle unsupported query using fallback client
            if let Some(fallback) = &state.fallback {
                debug!("Query not supported locally, forwarding to fallback");
                // Fallback client handles the HTTP call and returns formatted response
                match fallback
                    .execute_query_with_headers(parsed_request, headers)
                    .await
                {
                    Ok(response) => response.into_response(),
                    Err(status) => status.into_response(),
                }
            } else {
                debug!("Query not supported and forwarding disabled, returning error");
                // Adapter formats the unsupported query error for its protocol
                match state.adapter.format_unsupported_query_response().await {
                    Ok(json) => json.into_response(),
                    Err(status) => status.into_response(),
                }
            }
        }
    }
}

async fn handle_instant_query(
    query_params: Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Response {
    let start_time = Instant::now();
    debug!("=== INCOMING GET REQUEST ===");
    debug!("Raw query params: {:?}", query_params.0);

    let parsed_request = match state.adapter.parse_get_request(query_params).await {
        Ok(req) => {
            debug!(
                "Successfully parsed - query: '{}', time: {}",
                req.query, req.time
            );
            req
        }
        Err(parse_error) => {
            debug!("Failed to parse request: {:?}", parse_error);
            return match state.adapter.format_error_response(&parse_error).await {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    };

    process_query_request(&state, &parsed_request, start_time, HashMap::new()).await
}

async fn handle_instant_query_post(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let start_time = Instant::now();
    debug!("=== INCOMING POST REQUEST ===");

    // Extract headers we want to forward
    let mut forwarding_headers = HashMap::new();
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(auth_str) = auth.to_str() {
            forwarding_headers.insert("Authorization".to_string(), auth_str.to_string());
        }
    }

    // Check content type to determine how to parse the body
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    debug!("Content-Type: {}", content_type);

    let parsed_request = if content_type.contains("application/json") {
        // Handle JSON POST (Elasticsearch)
        debug!("Parsing as JSON POST request");
        match state.adapter.parse_json_post_request(body).await {
            Ok(req) => {
                debug!(
                    "Successfully parsed JSON POST - query: '{}', time: {}",
                    req.query, req.time
                );
                req
            }
            Err(parse_error) => {
                debug!("Failed to parse JSON POST request: {:?}", parse_error);
                return match state.adapter.format_error_response(&parse_error).await {
                    Ok(json) => json.into_response(),
                    Err(status) => status.into_response(),
                };
            }
        }
    } else {
        // Handle form-encoded POST
        debug!("Parsing as form-encoded POST request");

        // Parse the body as form data
        let body_str = match String::from_utf8(body.to_vec()) {
            Ok(s) => s,
            Err(e) => {
                debug!("Failed to parse body as UTF-8: {}", e);
                use crate::drivers::query::adapters::AdapterError;
                return match state
                    .adapter
                    .format_error_response(&AdapterError::ParseError(format!(
                        "Invalid UTF-8 in request body: {}",
                        e
                    )))
                    .await
                {
                    Ok(json) => json.into_response(),
                    Err(status) => status.into_response(),
                };
            }
        };

        // Parse form parameters
        let params: HashMap<String, String> = form_urlencoded::parse(body_str.as_bytes())
            .into_owned()
            .collect();
        debug!("Form params extracted: {:?}", params);

        // Use adapter to parse POST request (handles form-encoded parameters)
        match state.adapter.parse_post_request(Form(params)).await {
            Ok(req) => {
                debug!(
                    "Successfully parsed POST - query: '{}', time: {}",
                    req.query, req.time
                );
                req
            }
            Err(parse_error) => {
                debug!("Failed to parse POST request: {:?}", parse_error);
                return match state.adapter.format_error_response(&parse_error).await {
                    Ok(json) => json.into_response(),
                    Err(status) => status.into_response(),
                };
            }
        }
    };

    let result =
        process_query_request(&state, &parsed_request, start_time, forwarding_headers).await;

    let total_duration = start_time.elapsed();
    debug!(
        "Total POST request processing took: {:.2}ms",
        total_duration.as_secs_f64() * 1000.0
    );

    result
}

async fn handle_runtime_info(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Value>, StatusCode> {
    debug!("Delegating runtime info request to adapter");

    // Extract headers we want to forward
    let mut forwarding_headers = HashMap::new();
    if let Some(auth) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(auth_str) = auth.to_str() {
            debug!("Found Authorization header for runtime info: {}", auth_str);
            forwarding_headers.insert("Authorization".to_string(), auth_str.to_string());
        }
    } else {
        debug!("No Authorization header found in runtime info request");
    }

    // Delegate to adapter for protocol-specific handling
    state
        .adapter
        .handle_runtime_info_with_headers(state.store.clone(), forwarding_headers)
        .await
}

// ============================================================
// Range Query Handlers
// ============================================================

/// Core range query execution logic shared between GET and POST handlers
async fn process_range_query_request(
    state: &AppState,
    parsed_request: &ParsedRangeQueryRequest,
    start_time: Instant,
) -> Response {
    // Check if handling is enabled
    if !state.config.handle_http_requests {
        debug!("HTTP request handling is disabled for range query");
        // For now, return error - fallback for range queries can be added later
        use crate::drivers::query::adapters::AdapterError;
        return match state
            .adapter
            .format_error_response(&AdapterError::ProtocolError(
                "Range query handling is disabled".to_string(),
            ))
            .await
        {
            Ok(json) => json.into_response(),
            Err(status) => status.into_response(),
        };
    }

    // Execute range query with engine
    let query_start_time = Instant::now();
    debug!(
        "Executing range query: '{}' from {} to {} step {}",
        parsed_request.query, parsed_request.start, parsed_request.end, parsed_request.step
    );

    match state.query_engine.handle_range_query_promql(
        parsed_request.query.clone(),
        parsed_request.start,
        parsed_request.end,
        parsed_request.step,
    ) {
        Some((query_output_labels, query_result)) => {
            let query_duration = query_start_time.elapsed();
            debug!(
                "Range query execution took: {:.2}ms",
                query_duration.as_secs_f64() * 1000.0
            );

            let total_duration = start_time.elapsed();
            debug!(
                "Total range query processing took: {:.2}ms",
                total_duration.as_secs_f64() * 1000.0
            );

            // Format range success response
            match state
                .adapter
                .format_range_success_response(&query_result, &query_output_labels)
                .await
            {
                Ok(response) => response.into_response(),
                Err(status) => status.into_response(),
            }
        }
        None => {
            debug!("Range query returned None - query not supported");
            match state.adapter.format_unsupported_query_response().await {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            }
        }
    }
}

async fn handle_range_query(
    query_params: Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Response {
    let start_time = Instant::now();
    debug!("=== INCOMING RANGE QUERY GET REQUEST ===");
    debug!("Raw query params: {:?}", query_params.0);

    let parsed_request = match state.adapter.parse_range_get_request(query_params).await {
        Ok(req) => {
            debug!(
                "Successfully parsed range query - query: '{}', start: {}, end: {}, step: {}",
                req.query, req.start, req.end, req.step
            );
            req
        }
        Err(parse_error) => {
            debug!("Failed to parse range query request: {:?}", parse_error);
            return match state.adapter.format_error_response(&parse_error).await {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    };

    process_range_query_request(&state, &parsed_request, start_time).await
}

async fn handle_range_query_post(State(state): State<AppState>, body: Bytes) -> Response {
    let start_time = Instant::now();
    debug!("=== INCOMING RANGE QUERY POST REQUEST ===");

    // Parse the body as form data
    let body_str = match String::from_utf8(body.to_vec()) {
        Ok(s) => s,
        Err(e) => {
            debug!("Failed to parse body as UTF-8: {}", e);
            use crate::drivers::query::adapters::AdapterError;
            return match state
                .adapter
                .format_error_response(&AdapterError::ParseError(format!(
                    "Invalid UTF-8 in request body: {}",
                    e
                )))
                .await
            {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    };

    // Parse form parameters
    let params: HashMap<String, String> = form_urlencoded::parse(body_str.as_bytes())
        .into_owned()
        .collect();
    debug!("Form params extracted: {:?}", params);

    let parsed_request = match state.adapter.parse_range_post_request(Form(params)).await {
        Ok(req) => {
            debug!(
                "Successfully parsed range POST - query: '{}', start: {}, end: {}, step: {}",
                req.query, req.start, req.end, req.step
            );
            req
        }
        Err(parse_error) => {
            debug!("Failed to parse range POST request: {:?}", parse_error);
            return match state.adapter.format_error_response(&parse_error).await {
                Ok(json) => json.into_response(),
                Err(status) => status.into_response(),
            };
        }
    };

    process_range_query_request(&state, &parsed_request, start_time).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_model::{InferenceConfig, StreamingConfig};
    use crate::engines::SimpleEngine;
    use crate::stores::simple_map_store::SimpleMapStore;
    use reqwest::Client;
    use std::sync::Arc;

    async fn setup_test_server() -> u16 {
        let adapter_config = AdapterConfig::prometheus_promql(
            "http://127.0.0.1:9999".to_string(), // Unused for this test
            false,                               // forward_unsupported_queries
        );

        let config = HttpServerConfig {
            port: 0,
            handle_http_requests: true,
            adapter_config,
        };

        let inference_config = InferenceConfig::new(
            crate::data_model::QueryLanguage::promql,
            crate::data_model::CleanupPolicy::NoCleanup,
        );
        let streaming_config = Arc::new(StreamingConfig::default());
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            crate::data_model::CleanupPolicy::NoCleanup,
        ));
        let query_engine = Arc::new(SimpleEngine::new(
            store.clone(),
            inference_config,
            streaming_config.clone(),
            15000,
            crate::data_model::QueryLanguage::promql,
        ));

        let server = HttpServer::new(config, query_engine, store);
        server
            .start_test_server()
            .await
            .expect("Failed to start test server")
    }

    #[tokio::test]
    async fn test_get_endpoint_plus_symbol_decoding() {
        // Enable debug logging for this test
        // let _ = tracing_subscriber::fmt()
        //     .with_env_filter("debug")
        //     .try_init();

        let server_port = setup_test_server().await;
        let client = Client::new();

        // Test query with + symbols that should become spaces
        let test_query = "quantile by (instance, job) (0.95, fake_metric_total)";

        println!("Sending query: {test_query}");

        let response = client
            .get(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .query(&[("query", test_query)])
            .send()
            .await
            .expect("Failed to send request");

        let status = response.status();
        let response_json: serde_json::Value = response.json().await.expect("Failed to parse JSON");

        println!("Response status: {status}");
        println!("Response JSON: {response_json}");

        // The debug logs should show what query was actually parsed
        assert!(status.is_success() || status == reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn test_post_endpoint_form_decoding() {
        // let _ = tracing_subscriber::fmt()
        //     .with_env_filter("debug")
        //     .try_init();

        let server_port = setup_test_server().await;
        let client = Client::new();

        // Test the same query via POST with form encoding
        let test_query = "quantile+by+(instance,+job)+(0.95,+fake_metric_total)";

        println!("Sending POST with form data: {test_query}");

        let response = client
            .post(format!("http://127.0.0.1:{server_port}/api/v1/query"))
            .header("content-type", "application/x-www-form-urlencoded")
            .body(format!("query={test_query}&time=1758161478.205"))
            .send()
            .await
            .expect("Failed to send request");

        let status = response.status();
        let response_json: serde_json::Value = response.json().await.expect("Failed to parse JSON");

        println!("Response status: {status}");
        println!("Response JSON: {response_json}");

        assert!(status.is_success() || status == reqwest::StatusCode::OK);
    }
}
