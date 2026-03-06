use crate::data_model::StreamingConfig;
use crate::precompute_engine::config::PrecomputeEngineConfig;
use crate::precompute_engine::ingest_handler::{
    handle_prometheus_ingest, handle_victoriametrics_ingest, IngestState,
};
use crate::precompute_engine::output_sink::OutputSink;
use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
use crate::precompute_engine::worker::{Worker, WorkerRuntimeConfig};
use axum::{routing::post, Router};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{info, warn};

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
                WorkerRuntimeConfig {
                    max_buffer_per_series: self.config.max_buffer_per_series,
                    allowed_lateness_ms: self.config.allowed_lateness_ms,
                    pass_raw_samples: self.config.pass_raw_samples,
                    raw_mode_aggregation_id: self.config.raw_mode_aggregation_id,
                    late_data_policy: self.config.late_data_policy,
                },
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

        // Start the Axum HTTP server for ingest (Prometheus + VictoriaMetrics)
        let app = Router::new()
            .route("/api/v1/write", post(handle_prometheus_ingest))
            .route("/api/v1/import", post(handle_victoriametrics_ingest))
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
