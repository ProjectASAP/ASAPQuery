use crate::drivers::ingest::prometheus_remote_write::decode_prometheus_remote_write;
use crate::drivers::ingest::victoriametrics_remote_write::decode_victoriametrics_remote_write;
use crate::precompute_engine::series_router::SeriesRouter;
use axum::{body::Bytes, extract::State, http::StatusCode};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::warn;

/// Shared state for the ingest HTTP handler.
pub(crate) struct IngestState {
    pub(crate) router: SeriesRouter,
    pub(crate) samples_ingested: std::sync::atomic::AtomicU64,
}

/// Shared logic: group decoded samples by series key and route to workers.
async fn route_decoded_samples(
    state: &IngestState,
    samples: Vec<crate::drivers::ingest::prometheus_remote_write::DecodedSample>,
    ingest_received_at: Instant,
) -> StatusCode {
    if samples.is_empty() {
        return StatusCode::NO_CONTENT;
    }

    let count = samples.len() as u64;
    state
        .samples_ingested
        .fetch_add(count, std::sync::atomic::Ordering::Relaxed);

    // Group samples by series key for batch routing
    let mut by_series: HashMap<&str, Vec<(i64, f64)>> = HashMap::new();
    for s in &samples {
        by_series
            .entry(&s.labels)
            .or_default()
            .push((s.timestamp_ms, s.value));
    }

    // Convert to owned keys for batch routing
    let by_series_owned: HashMap<String, Vec<(i64, f64)>> = by_series
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

    // Route all series to workers concurrently
    if let Err(e) = state
        .router
        .route_batch(by_series_owned, ingest_received_at)
        .await
    {
        warn!("Batch routing error: {}", e);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    StatusCode::NO_CONTENT
}

/// Axum handler for Prometheus remote write (Snappy + Protobuf).
pub(crate) async fn handle_prometheus_ingest(
    State(state): State<Arc<IngestState>>,
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
    route_decoded_samples(&state, samples, ingest_received_at).await
}

/// Axum handler for VictoriaMetrics remote write (Zstd + Protobuf).
pub(crate) async fn handle_victoriametrics_ingest(
    State(state): State<Arc<IngestState>>,
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
    route_decoded_samples(&state, samples, ingest_received_at).await
}
