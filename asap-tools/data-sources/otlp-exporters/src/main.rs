//! OTLP test data generator for ASAP pipeline testing.
//!
//! Sends synthetic metrics (Counter, Histogram, Gauge) to an OTLP/gRPC endpoint
//! at a configurable rate. Designed to exercise `asap-otel-ingest` end-to-end.

use clap::Parser;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::Resource;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Normal, Uniform};
use std::time::Duration;
use tracing::info;

#[derive(Parser, Debug)]
#[command(
    name = "otlp_exporter",
    about = "Synthetic OTLP metrics generator for testing asap-otel-ingest"
)]
struct Args {
    /// OTLP/gRPC endpoint to send metrics to
    #[arg(long, default_value = "http://localhost:4317")]
    endpoint: String,

    /// Number of services to simulate (max 5)
    #[arg(long, default_value_t = 3)]
    num_services: usize,

    /// Number of HTTP methods per service (max 5)
    #[arg(long, default_value_t = 3)]
    num_methods: usize,

    /// Interval between metric collection flushes in milliseconds
    #[arg(long, default_value_t = 1000)]
    interval_ms: u64,

    /// Number of iterations (0 = run forever)
    #[arg(long, default_value_t = 0)]
    iterations: u64,
}

static SERVICES: &[&str] = &[
    "auth-service",
    "api-gateway",
    "user-service",
    "billing-service",
    "search-service",
];
static METHODS: &[&str] = &["GET", "POST", "PUT", "DELETE", "PATCH"];
static STATUS_CODES: &[&str] = &["200", "201", "400", "404", "500", "503"];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    info!(
        "Starting OTLP exporter → {} ({} services, {} methods, {}ms interval)",
        args.endpoint, args.num_services, args.num_methods, args.interval_ms
    );

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&args.endpoint)
        .with_timeout(Duration::from_secs(5))
        .build()?;

    let reader =
        opentelemetry_sdk::metrics::PeriodicReader::builder(exporter, opentelemetry_sdk::runtime::Tokio)
            .with_interval(Duration::from_millis(args.interval_ms))
            .build();

    let provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            "asap.test.otlp_exporter",
        )]))
        .build();

    let meter = provider.meter("asap.test.otlp_exporter");

    let request_counter = meter
        .u64_counter("http_requests_total")
        .with_description("Total HTTP requests")
        .with_unit("1")
        .build();

    let request_duration = meter
        .f64_histogram("http_request_duration_seconds")
        .with_description("HTTP request duration in seconds")
        .with_unit("s")
        .build();

    let active_connections = meter
        .i64_up_down_counter("http_active_connections")
        .with_description("Number of active HTTP connections")
        .with_unit("1")
        .build();

    let response_size = meter
        .f64_histogram("http_response_size_bytes")
        .with_description("HTTP response size in bytes")
        .with_unit("By")
        .build();

    let mut rng = SmallRng::from_entropy();
    let normal = Normal::new(0.15_f64, 0.05_f64).unwrap();
    let uniform = Uniform::new(100.0_f64, 50_000.0_f64);

    let num_services = args.num_services.min(SERVICES.len());
    let num_methods = args.num_methods.min(METHODS.len());
    let services = &SERVICES[..num_services];
    let methods = &METHODS[..num_methods];

    let mut iteration: u64 = 0;
    loop {
        iteration += 1;

        for service in services {
            for method in methods {
                let status = STATUS_CODES[rng.gen_range(0..STATUS_CODES.len())];
                let labels = [
                    KeyValue::new("service.name", service.to_string()),
                    KeyValue::new("http.method", method.to_string()),
                    KeyValue::new("http.status_code", status.to_string()),
                ];

                let count = rng.gen_range(1u64..=10);
                request_counter.add(count, &labels);

                let latency = normal.sample(&mut rng).abs().max(0.001);
                request_duration.record(latency, &labels);

                let size = uniform.sample(&mut rng);
                response_size.record(size, &labels);
            }

            let delta: i64 = rng.gen_range(-5..=5);
            active_connections.add(
                delta,
                &[KeyValue::new("service.name", service.to_string())],
            );
        }

        info!(
            "Iteration {}: recorded metrics for {} services",
            iteration,
            services.len()
        );

        if args.iterations > 0 && iteration >= args.iterations {
            info!("Reached {} iterations, stopping", args.iterations);
            break;
        }

        tokio::time::sleep(Duration::from_millis(args.interval_ms)).await;
    }

    provider.shutdown()?;
    Ok(())
}
