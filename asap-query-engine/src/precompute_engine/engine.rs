use crate::data_model::StreamingConfig;
use crate::precompute_engine::config::PrecomputeEngineConfig;
use crate::precompute_engine::ingest_handler::{
    handle_prometheus_ingest, handle_victoriametrics_ingest, IngestState,
};
use crate::precompute_engine::output_sink::OutputSink;
use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
use crate::precompute_engine::worker::{Worker, WorkerRuntimeConfig};
use arc_swap::ArcSwap;
use asap_types::aggregation_config::AggregationConfig;
use axum::{routing::post, Router};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicUsize};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Shared diagnostic counters readable from outside the engine.
pub struct PrecomputeWorkerDiagnostics {
    pub worker_group_counts: Vec<Arc<AtomicUsize>>,
    pub worker_watermarks: Vec<Arc<AtomicI64>>,
}

/// A cloneable handle for applying runtime config updates to a running engine.
///
/// Obtained via `PrecomputeEngine::handle()` before calling `run()`.
/// Calling `update_streaming_config` swaps the ingest handler's agg_configs
/// atomically (lock-free via ArcSwap) and broadcasts the new map to all workers.
pub struct PrecomputeEngineHandle {
    router: SeriesRouter,
    ingest_agg_configs: Arc<ArcSwap<Vec<Arc<AggregationConfig>>>>,
}

impl PrecomputeEngineHandle {
    /// Apply a new streaming config to the running engine.
    ///
    /// Updates the ingest handler's agg_configs via a lock-free ArcSwap store,
    /// then broadcasts the new config map to all workers via their message channels.
    pub async fn update_streaming_config(
        &self,
        config: &StreamingConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let agg_configs_map: HashMap<u64, Arc<AggregationConfig>> = config
            .get_all_aggregation_configs()
            .iter()
            .map(|(&id, cfg)| (id, Arc::new(cfg.clone())))
            .collect();
        let agg_configs_vec: Vec<Arc<AggregationConfig>> =
            agg_configs_map.values().cloned().collect();

        self.ingest_agg_configs.store(Arc::new(agg_configs_vec));
        self.router
            .broadcast_update_agg_configs(agg_configs_map)
            .await?;

        info!(
            "PrecomputeEngineHandle: applied new streaming config ({} aggregations)",
            config.get_all_aggregation_configs().len()
        );
        Ok(())
    }
}

/// The top-level precompute engine orchestrator.
///
/// Creates worker threads, the series router, and the Axum ingest server.
/// Call `handle()` before `run()` to obtain a `PrecomputeEngineHandle` for
/// applying runtime config updates while the engine is running.
pub struct PrecomputeEngine {
    config: PrecomputeEngineConfig,
    streaming_config: Arc<StreamingConfig>,
    output_sink: Arc<dyn OutputSink>,
    diagnostics: Arc<PrecomputeWorkerDiagnostics>,
    /// Channels created at construction so handle() can be extracted before run().
    senders: Vec<mpsc::Sender<WorkerMessage>>,
    receivers: Option<Vec<mpsc::Receiver<WorkerMessage>>>,
    /// Shared ingest agg_configs, swappable at runtime.
    ingest_agg_configs: Arc<ArcSwap<Vec<Arc<AggregationConfig>>>>,
}

impl PrecomputeEngine {
    pub fn new(
        config: PrecomputeEngineConfig,
        streaming_config: Arc<StreamingConfig>,
        output_sink: Arc<dyn OutputSink>,
    ) -> Self {
        let worker_group_counts = (0..config.num_workers)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();
        let worker_watermarks = (0..config.num_workers)
            .map(|_| Arc::new(AtomicI64::new(i64::MIN)))
            .collect();
        let diagnostics = Arc::new(PrecomputeWorkerDiagnostics {
            worker_group_counts,
            worker_watermarks,
        });

        // Build channels and initial agg_configs at construction time so that
        // handle() can be called before run().
        let channel_size = config.channel_buffer_size;
        let mut senders = Vec::with_capacity(config.num_workers);
        let mut receivers = Vec::with_capacity(config.num_workers);
        for _ in 0..config.num_workers {
            let (tx, rx) = mpsc::channel::<WorkerMessage>(channel_size);
            senders.push(tx);
            receivers.push(rx);
        }

        let agg_configs_vec: Vec<Arc<AggregationConfig>> = streaming_config
            .get_all_aggregation_configs()
            .values()
            .map(|cfg| Arc::new(cfg.clone()))
            .collect();
        let ingest_agg_configs = Arc::new(ArcSwap::from_pointee(agg_configs_vec));

        Self {
            config,
            streaming_config,
            output_sink,
            diagnostics,
            senders,
            receivers: Some(receivers),
            ingest_agg_configs,
        }
    }

    /// Get a handle to worker diagnostics, readable even after `run()` starts.
    pub fn diagnostics(&self) -> Arc<PrecomputeWorkerDiagnostics> {
        self.diagnostics.clone()
    }

    /// Return a handle for applying runtime config updates to this engine.
    /// Must be called before `run()`.
    pub fn handle(&self) -> PrecomputeEngineHandle {
        PrecomputeEngineHandle {
            router: SeriesRouter::new(self.senders.clone()),
            ingest_agg_configs: self.ingest_agg_configs.clone(),
        }
    }

    /// Start the precompute engine. This spawns worker tasks and the HTTP
    /// ingest server, then blocks until shutdown.
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let num_workers = self.config.num_workers;

        let receivers = self
            .receivers
            .take()
            .expect("PrecomputeEngine::run() called twice");

        // Build the router from the pre-created senders.
        let router = SeriesRouter::new(self.senders.clone());

        // Build aggregation config map from streaming config for workers.
        let agg_configs: HashMap<u64, Arc<AggregationConfig>> = self
            .streaming_config
            .get_all_aggregation_configs()
            .iter()
            .map(|(&id, cfg)| (id, Arc::new(cfg.clone())))
            .collect();

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
                self.diagnostics.worker_group_counts[id].clone(),
                self.diagnostics.worker_watermarks[id].clone(),
                self.diagnostics.worker_watermarks.to_vec(),
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

        // Build the ingest state, sharing the same Arc<ArcSwap> as the handle so
        // that PrecomputeEngineHandle::update_streaming_config swaps are visible here.
        let ingest_state = Arc::new(IngestState {
            router,
            samples_ingested: std::sync::atomic::AtomicU64::new(0),
            agg_configs: self.ingest_agg_configs.clone(),
            pass_raw_samples: self.config.pass_raw_samples,
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
