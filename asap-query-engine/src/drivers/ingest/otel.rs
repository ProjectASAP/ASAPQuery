//! OTLP ingest driver.
//!
//! Accepts OTLP metrics via gRPC (4317) and HTTP (4318, POST /v1/metrics),
//! parses ExportMetricsServiceRequest, logs counts at DEBUG, and leaves
//! handoff to precompute engine as TODO.

use std::collections::HashMap;

use axum::{
    body::Bytes,
    extract::State,
    routing::post,
    Json, Router,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    metrics_service_server::MetricsService, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

/// Configuration for the OTLP ingest consumer.
#[derive(Debug, Clone)]
pub struct OtlpConsumerConfig {
    pub grpc_port: u16,
    pub http_port: u16,
}

/// OTLP consumer that accepts metrics via gRPC and HTTP.
pub struct OtlpConsumer {
    config: OtlpConsumerConfig,
}

impl OtlpConsumer {
    pub fn new(config: OtlpConsumerConfig) -> Self {
        Self { config }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let grpc_addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.config.grpc_port));
        let http_addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.config.http_port));

        let grpc_svc = MetricsServiceImpl;
        let grpc_svc =
            opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer::new(
                grpc_svc,
            );

        let app = Router::new()
            .route("/v1/metrics", post(handle_otlp_http))
            .with_state(());

        let grpc_listener = tokio::net::TcpListener::bind(grpc_addr).await?;
        let http_listener = tokio::net::TcpListener::bind(http_addr).await?;

        info!("OTLP gRPC listening on {}", grpc_addr);
        info!("OTLP HTTP listening on {} (POST /v1/metrics)", http_addr);

        tokio::select! {
            r = tonic::transport::Server::builder()
                .add_service(grpc_svc)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(grpc_listener)) => {
                    if let Err(e) = r {
                        error!("OTLP gRPC server error: {}", e);
                    }
                }
            r = axum::serve(http_listener, app) => {
                if let Err(e) = r {
                    error!("OTLP HTTP server error: {}", e);
                }
            }
        }

        Ok(())
    }
}

struct MetricsServiceImpl;

#[tonic::async_trait]
impl MetricsService for MetricsServiceImpl {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let req = request.into_inner();
        process_otlp_request(&req);
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

async fn handle_otlp_http(
    State(_state): State<()>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, (axum::http::StatusCode, String)> {
    let req = ExportMetricsServiceRequest::decode(body.as_ref())
        .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, format!("Protobuf decode error: {}", e)))?;
    process_otlp_request(&req);
    Ok(Json(serde_json::json!({"rejected": 0})))
}

fn process_otlp_request(request: &ExportMetricsServiceRequest) {
    let resource_count = request.resource_metrics.len();
    let total_points = otlp_to_record_count(request);
    debug!(
        "OTLP ingest: received {} resource metrics, {} total data points",
        resource_count, total_points
    );
    // TODO: Pass metrics to precompute engine. 
}

/// Count total data points by traversing resource_metrics -> scope_metrics -> metrics.
/// Reuses the same conversion traversal as asap-otel-ingest (Gauge, Sum, Histogram, etc.).
fn otlp_to_record_count(request: &ExportMetricsServiceRequest) -> usize {
    let mut count = 0;
    for resource_metrics in &request.resource_metrics {
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                if metric.name.is_empty() {
                    continue;
                }

                use opentelemetry_proto::tonic::metrics::v1::metric::Data;
                match &metric.data {
                    Some(Data::Gauge(g)) => count += g.data_points.len(),
                    Some(Data::Sum(s)) => count += s.data_points.len(),
                    Some(Data::Histogram(hist)) => {
                        for dp in &hist.data_points {
                            if dp.sum.is_some() {
                                count += 1;
                            }
                            count += 1; // _count
                            count += dp.bucket_counts.len(); // _bucket per le
                        }
                    }
                    Some(Data::ExponentialHistogram(eh)) => {
                        for dp in &eh.data_points {
                            if dp.sum.is_some() {
                                count += 1;
                            }
                            count += 1; // _count
                            count += 1; // _scale
                        }
                    }
                    Some(Data::Summary(summary)) => {
                        for dp in &summary.data_points {
                            count += 1; // _sum
                            count += 1; // _count
                            count += dp.quantile_values.len();
                        }
                    }
                    None => {}
                }
            }
        }
    }
    count
}

#[allow(dead_code)]
fn number_value_to_f64(v: &Option<NumberValue>) -> f64 {
    match v {
        Some(NumberValue::AsDouble(x)) => *x,
        Some(NumberValue::AsInt(x)) => *x as f64,
        None => 0.0,
    }
}

#[allow(dead_code)]
fn any_value_to_string(v: &opentelemetry_proto::tonic::common::v1::AnyValue) -> String {
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
    match &v.value {
        Some(AnyValueVariant::StringValue(s)) => s.clone(),
        Some(AnyValueVariant::IntValue(i)) => i.to_string(),
        Some(AnyValueVariant::DoubleValue(d)) => d.to_string(),
        Some(AnyValueVariant::BoolValue(b)) => b.to_string(),
        Some(AnyValueVariant::BytesValue(bytes)) => format!("{:?}", bytes),
        _ => String::new(),
    }
}

#[allow(dead_code)]
fn attributes_to_map(
    attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for kv in attrs {
        let key = kv.key.clone();
        let value = kv
            .value
            .as_ref()
            .map(any_value_to_string)
            .unwrap_or_default();
        if !key.is_empty() {
            m.insert(key, value);
        }
    }
    m
}
