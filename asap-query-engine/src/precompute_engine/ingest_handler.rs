use crate::drivers::ingest::prometheus_remote_write::decode_prometheus_remote_write;
use crate::drivers::ingest::victoriametrics_remote_write::decode_victoriametrics_remote_write;
use crate::precompute_engine::ingest_source::{route_decoded_samples, IngestContext, IngestSource};
use axum::{body::Bytes, extract::State, http::StatusCode, routing::post, Router};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

const INGEST_DIAG_INTERVAL: Duration = Duration::from_secs(30);

pub struct HttpIngestConfig {
    pub port: u16,
}

pub struct HttpIngestSource {
    config: HttpIngestConfig,
}

impl HttpIngestSource {
    pub fn new(config: HttpIngestConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl IngestSource for HttpIngestSource {
    async fn run(
        self: Box<Self>,
        ctx: IngestContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let state = Arc::new(HttpIngestState {
            ctx,
            samples_ingested: AtomicU64::new(0),
        });

        let addr = format!("0.0.0.0:{}", self.config.port);
        // Bind before spawning the ticker so a bind failure returns early
        // without leaving an orphaned diagnostics task behind.
        let listener = TcpListener::bind(&addr).await?;
        info!("HTTP ingest server listening on {}", addr);

        let ticker = tokio::spawn(log_ingest_throughput(state.clone()));

        let app = Router::new()
            .route("/api/v1/write", post(handle_prometheus_ingest))
            .route("/api/v1/import", post(handle_victoriametrics_ingest))
            .with_state(state);

        let result = axum::serve(listener, app).await;
        ticker.abort();
        result?;
        Ok(())
    }
}

/// Shared state for the Axum ingest handlers.
struct HttpIngestState {
    ctx: IngestContext,
    samples_ingested: AtomicU64,
}

/// Logs ingest throughput every `INGEST_DIAG_INTERVAL`, resetting the counter
/// each tick so the log reports a per-interval rate rather than a lifetime total.
async fn log_ingest_throughput(state: Arc<HttpIngestState>) {
    let mut interval = tokio::time::interval(INGEST_DIAG_INTERVAL);
    interval.tick().await; // first tick fires immediately; skip it
    loop {
        interval.tick().await;
        let samples = state.samples_ingested.swap(0, Ordering::Relaxed);
        let secs = INGEST_DIAG_INTERVAL.as_secs_f64();
        debug!(
            "[INGEST_DIAG] samples_ingested: {} in {:.0}s ({:.1} samples/sec)",
            samples,
            secs,
            samples as f64 / secs,
        );
    }
}

async fn handle_prometheus_ingest(
    State(state): State<Arc<HttpIngestState>>,
    body: Bytes,
) -> StatusCode {
    let ingest_received_at = Instant::now();
    let samples = match decode_prometheus_remote_write(&body) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to decode Prometheus remote write: {}", e);
            return StatusCode::BAD_REQUEST;
        }
    };
    state
        .samples_ingested
        .fetch_add(samples.len() as u64, Ordering::Relaxed);
    match route_decoded_samples(&state.ctx, samples, ingest_received_at).await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            warn!("Routing error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn handle_victoriametrics_ingest(
    State(state): State<Arc<HttpIngestState>>,
    body: Bytes,
) -> StatusCode {
    let ingest_received_at = Instant::now();
    let samples = match decode_victoriametrics_remote_write(&body) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to decode VictoriaMetrics remote write: {}", e);
            return StatusCode::BAD_REQUEST;
        }
    };
    state
        .samples_ingested
        .fetch_add(samples.len() as u64, Ordering::Relaxed);
    match route_decoded_samples(&state.ctx, samples, ingest_received_at).await {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(e) => {
            warn!("Routing error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
