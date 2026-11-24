use crate::precompute_engine::build_spec::{BuildSpec, SeriesRouting};
use crate::precompute_engine::config::PrecomputeEngineConfig;
use crate::precompute_engine::ingest_source::{IngestContext, IngestSource};
use crate::precompute_engine::output_sink::OutputSink;
use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
use crate::precompute_engine::worker::{PipelineWorker, WorkerRuntimeConfig};
use arc_swap::ArcSwap;
use std::sync::atomic::{AtomicI64, AtomicUsize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Shared diagnostic counters readable from outside the engine.
pub struct PrecomputeWorkerDiagnostics {
    pub worker_group_counts: Vec<Arc<AtomicUsize>>,
    pub worker_watermarks: Vec<Arc<AtomicI64>>,
}

/// A cloneable handle for swapping the build specification of a running
/// pipeline.
///
/// Obtained via `PrecomputePipeline::handle()` before calling `run()`.
pub struct PipelineHandle<R: SeriesRouting> {
    router: SeriesRouter<R::Build>,
    routing: Arc<ArcSwap<R>>,
}

impl<R: SeriesRouting> PipelineHandle<R> {
    /// Swap the ingest routing atomically (lock-free via ArcSwap), then
    /// broadcast the new builds to all workers via their message channels.
    pub async fn update_spec(
        &self,
        spec: BuildSpec<R>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.routing.store(Arc::new(spec.routing));
        self.router.broadcast_update_agg_configs(spec.builds).await
    }
}

/// The precompute pipeline: routes series to workers, runs each build's
/// windows, watermarks and late-data policy per group, and writes closed
/// windows to the output sink. What it builds comes from the `BuildSpec`
/// given to `run`.
pub struct PrecomputePipeline<R: SeriesRouting> {
    config: PrecomputeEngineConfig,
    output_sink: Arc<dyn OutputSink>,
    diagnostics: Arc<PrecomputeWorkerDiagnostics>,
    sources: Vec<Box<dyn IngestSource<R>>>,
    /// Channels created at construction so handle() can be extracted before run().
    senders: Vec<mpsc::Sender<WorkerMessage<R::Build>>>,
    receivers: Option<Vec<mpsc::Receiver<WorkerMessage<R::Build>>>>,
    /// Shared ingest routing, swappable at runtime.
    routing: Arc<ArcSwap<R>>,
    /// Test-support wall-clock override, applied to every spawned worker.
    /// See `with_now_ms_fn`. `None` in production — each worker keeps its
    /// default `SystemTime::now`-backed clock.
    now_ms_fn: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
}

impl<R: SeriesRouting> PrecomputePipeline<R> {
    pub fn new(
        config: PrecomputeEngineConfig,
        output_sink: Arc<dyn OutputSink>,
        sources: Vec<Box<dyn IngestSource<R>>>,
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

        let channel_size = config.channel_buffer_size;
        let mut senders = Vec::with_capacity(config.num_workers);
        let mut receivers = Vec::with_capacity(config.num_workers);
        for _ in 0..config.num_workers {
            let (tx, rx) = mpsc::channel::<WorkerMessage<R::Build>>(channel_size);
            senders.push(tx);
            receivers.push(rx);
        }

        let routing = Arc::new(ArcSwap::from_pointee(R::default()));

        Self {
            config,
            output_sink,
            diagnostics,
            sources,
            senders,
            receivers: Some(receivers),
            routing,
            now_ms_fn: None,
        }
    }

    /// Get a handle to worker diagnostics, readable even after `run()` starts.
    pub fn diagnostics(&self) -> Arc<PrecomputeWorkerDiagnostics> {
        self.diagnostics.clone()
    }

    /// Test-support builder: override the wall-clock source used by every
    /// worker's wall-clock fallback (`flush_all`). Must be called before
    /// `run()`. Production code never calls this.
    pub fn with_now_ms_fn(mut self, f: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.now_ms_fn = Some(Arc::new(f));
        self
    }

    /// Return a handle for swapping the build specification of this
    /// pipeline. Must be called before `run()`.
    pub fn handle(&self) -> PipelineHandle<R> {
        PipelineHandle {
            router: SeriesRouter::new(self.senders.clone()),
            routing: self.routing.clone(),
        }
    }

    /// Install `spec`, spawn worker tasks and all registered ingest sources,
    /// then block until shutdown.
    pub async fn run(
        mut self,
        spec: BuildSpec<R>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let BuildSpec { routing, builds } = spec;
        self.routing.store(Arc::new(routing));

        let num_workers = self.config.num_workers;

        let receivers = self
            .receivers
            .take()
            .expect("PrecomputeEngine::run() called twice");

        // Build the router from the pre-created senders.
        let router = SeriesRouter::new(self.senders.clone());

        // Spawn workers
        let mut worker_handles = Vec::with_capacity(num_workers);
        for (id, rx) in receivers.into_iter().enumerate() {
            let mut worker = PipelineWorker::new(
                id,
                rx,
                self.output_sink.clone(),
                builds.clone(),
                WorkerRuntimeConfig {
                    max_buffer_per_series: self.config.max_buffer_per_series,
                    allowed_lateness_ms: self.config.allowed_lateness_ms,
                    pass_raw_samples: self.config.pass_raw_samples,
                    raw_mode_aggregation_id: self.config.raw_mode_aggregation_id,
                    late_data_policy: self.config.late_data_policy,
                    wall_clock_grace_period_ms: self.config.wall_clock_grace_period_ms,
                },
                self.diagnostics.worker_group_counts[id].clone(),
                self.diagnostics.worker_watermarks[id].clone(),
                self.diagnostics.worker_watermarks.to_vec(),
            );
            if let Some(now_ms_fn) = &self.now_ms_fn {
                let now_ms_fn = now_ms_fn.clone();
                worker.set_now_ms_fn(Box::new(move || now_ms_fn()));
            }
            let handle = tokio::spawn(async move {
                worker.run().await;
            });
            worker_handles.push(handle);
        }

        info!("PrecomputeEngine started with {} workers", num_workers);

        // Build the ingest context shared by all sources.
        // The ArcSwap pointer is the same one held by PipelineHandle, so
        // handle.update_spec() is immediately visible to every source.
        let ctx = IngestContext {
            router: router.clone(),
            routing_configs: self.routing.clone(),
            pass_raw_samples: self.config.pass_raw_samples,
        };

        // Flush timer: periodically signal workers to close idle windows.
        let flush_router = router.clone();
        let flush_interval_ms = self.config.flush_interval_ms;
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(flush_interval_ms));
            loop {
                interval.tick().await;
                if let Err(e) = flush_router.broadcast_flush().await {
                    warn!("Flush broadcast error: {}", e);
                    break;
                }
            }
        });

        // Spawn each ingest source.
        let mut source_handles = Vec::with_capacity(self.sources.len());
        for source in self.sources {
            let ctx = ctx.clone();
            let handle = tokio::spawn(async move {
                if let Err(e) = source.run(ctx).await {
                    warn!("Ingest source error: {}", e);
                }
            });
            source_handles.push(handle);
        }

        // Block until all sources finish (normally only on shutdown).
        for handle in source_handles {
            let _ = handle.await;
        }

        for handle in worker_handles {
            let _ = handle.await;
        }

        Ok(())
    }
}
