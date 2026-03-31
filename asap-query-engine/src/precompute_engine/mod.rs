pub mod accumulator_factory;
pub mod config;
pub mod output_sink;
pub mod series_buffer;
pub mod series_router;
pub mod window_manager;
pub mod worker;

use crate::data_model::StreamingConfig;
use crate::drivers::ingest::prometheus_remote_write::decode_prometheus_remote_write;
use crate::precompute_engine::config::PrecomputeEngineConfig;
use crate::precompute_engine::output_sink::OutputSink;
use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
use crate::precompute_engine::worker::Worker;
use axum::{body::Bytes, extract::State, http::StatusCode, routing::post, Router};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{debug_span, info, warn, Instrument};

/// Shared state for the ingest HTTP handler.
struct IngestState {
    router: SeriesRouter,
    samples_ingested: std::sync::atomic::AtomicU64,
}

/// The top-level precompute engine orchestrator.
///
/// Creates worker threads, the series router, and the Axum ingest server.
pub struct PrecomputeEngine {
    config: PrecomputeEngineConfig,
    streaming_config: Arc<StreamingConfig>,
    output_sink: Arc<dyn OutputSink>,
}

impl PrecomputeEngine {
    pub fn new(
        config: PrecomputeEngineConfig,
        streaming_config: Arc<StreamingConfig>,
        output_sink: Arc<dyn OutputSink>,
    ) -> Self {
        Self {
            config,
            streaming_config,
            output_sink,
        }
    }

    /// Start the precompute engine. This spawns worker tasks and the HTTP
    /// ingest server, then blocks until shutdown.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let num_workers = self.config.num_workers;
        let channel_size = self.config.channel_buffer_size;

        // Build MPSC channels for each worker
        let mut senders = Vec::with_capacity(num_workers);
        let mut receivers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let (tx, rx) = mpsc::channel::<WorkerMessage>(channel_size);
            senders.push(tx);
            receivers.push(rx);
        }

        // Build the router
        let router = SeriesRouter::new(senders);

        // Build aggregation config map from streaming config
        let agg_configs: HashMap<u64, _> =
            self.streaming_config.get_all_aggregation_configs().clone();

        // Spawn workers
        let mut worker_handles = Vec::with_capacity(num_workers);
        for (id, rx) in receivers.into_iter().enumerate() {
            let worker = Worker::new(
                id,
                rx,
                self.output_sink.clone(),
                agg_configs.clone(),
                self.config.max_buffer_per_series,
                self.config.allowed_lateness_ms,
                self.config.pass_raw_samples,
                self.config.raw_mode_aggregation_id,
            );
            let handle = tokio::spawn(async move {
                worker.run().await;
            });
            worker_handles.push(handle);
        }

        info!(
            "PrecomputeEngine started with {} workers on port {}",
            num_workers, self.config.ingest_port
        );

        // Build the ingest state
        let ingest_state = Arc::new(IngestState {
            router,
            samples_ingested: std::sync::atomic::AtomicU64::new(0),
        });

        // Start flush timer
        let flush_state = ingest_state.clone();
        let flush_interval_ms = self.config.flush_interval_ms;
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(flush_interval_ms));
            loop {
                interval.tick().await;
                if let Err(e) = flush_state.router.broadcast_flush().await {
                    warn!("Flush broadcast error: {}", e);
                    break;
                }
            }
        });

        // Start the Axum HTTP server for Prometheus remote write ingest
        let app = Router::new()
            .route("/api/v1/write", post(handle_ingest))
            .with_state(ingest_state);

        let addr = format!("0.0.0.0:{}", self.config.ingest_port);
        info!("Ingest server listening on {}", addr);

        let listener = TcpListener::bind(&addr).await?;
        axum::serve(listener, app).await?;

        // Wait for workers to finish (this only happens on shutdown)
        for handle in worker_handles {
            let _ = handle.await;
        }

        Ok(())
    }
}

/// Axum handler for Prometheus remote write.
async fn handle_ingest(State(state): State<Arc<IngestState>>, body: Bytes) -> StatusCode {
    let ingest_span = debug_span!("ingest", body_len = body.len());
    let ingest_received_at = Instant::now();

    async {
        let samples = match decode_prometheus_remote_write(&body) {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to decode remote write: {}", e);
                return StatusCode::BAD_REQUEST;
            }
        };

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
    .instrument(ingest_span)
    .await
}
