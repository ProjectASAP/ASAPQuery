use clap::Parser;
use query_engine_rust::data_model::QueryLanguage;
use query_engine_rust::data_model::{
    CleanupPolicy, InferenceConfig, LockStrategy, StreamingConfig,
};
use query_engine_rust::drivers::query::adapters::AdapterConfig;
use query_engine_rust::engines::SimpleEngine;
use query_engine_rust::precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
use query_engine_rust::precompute_engine::output_sink::{RawPassthroughSink, StoreOutputSink};
use query_engine_rust::precompute_engine::PrecomputeEngine;
use query_engine_rust::stores::SimpleMapStore;
use query_engine_rust::{HttpServer, HttpServerConfig};
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Parser, Debug)]
#[command(name = "precompute_engine")]
#[command(about = "Standalone precompute engine for SketchDB")]
struct Args {
    /// Path to streaming config YAML file
    #[arg(long)]
    streaming_config: String,

    /// Port for Prometheus remote write ingest
    #[arg(long, default_value_t = 9090)]
    ingest_port: u16,

    /// Number of worker threads
    #[arg(long, default_value_t = 4)]
    num_workers: usize,

    /// Maximum allowed lateness for out-of-order samples (ms)
    #[arg(long, default_value_t = 5000)]
    allowed_lateness_ms: i64,

    /// Maximum buffered samples per series
    #[arg(long, default_value_t = 10000)]
    max_buffer_per_series: usize,

    /// Flush interval for idle window detection (ms)
    #[arg(long, default_value_t = 1000)]
    flush_interval_ms: u64,

    /// MPSC channel buffer size per worker
    #[arg(long, default_value_t = 10000)]
    channel_buffer_size: usize,

    /// Port for the query HTTP server (0 to disable)
    #[arg(long, default_value_t = 8080)]
    query_port: u16,

    /// Lock strategy for the store
    #[arg(long, value_enum, default_value_t = LockStrategy::PerKey)]
    lock_strategy: LockStrategy,

    /// Skip aggregation and pass each raw sample directly to the store
    #[arg(long, default_value_t = false)]
    pass_raw_samples: bool,

    /// Aggregation ID to stamp on each raw-mode output
    #[arg(long, default_value_t = 0)]
    raw_mode_aggregation_id: u64,

    /// Policy for handling late samples that arrive after their window has closed
    #[arg(long, value_enum, default_value_t = LateDataPolicy::Drop)]
    late_data_policy: LateDataPolicy,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let args = Args::parse();

    info!("Loading streaming config from: {}", args.streaming_config);
    let streaming_config = Arc::new(StreamingConfig::from_yaml_file(&args.streaming_config)?);

    info!(
        "Loaded {} aggregation configs",
        streaming_config.get_all_aggregation_configs().len()
    );

    // Create the store
    let store: Arc<dyn query_engine_rust::stores::Store> =
        Arc::new(SimpleMapStore::new_with_strategy(
            streaming_config.clone(),
            CleanupPolicy::CircularBuffer,
            args.lock_strategy,
        ));

    // Optionally start the query HTTP server
    if args.query_port > 0 {
        let inference_config =
            InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::CircularBuffer);
        let query_engine = Arc::new(SimpleEngine::new(
            store.clone(),
            inference_config,
            streaming_config.clone(),
            15, // default prometheus scrape interval
            QueryLanguage::promql,
        ));
        let http_config = HttpServerConfig {
            port: args.query_port,
            handle_http_requests: true,
            adapter_config: AdapterConfig {
                protocol: query_engine_rust::data_model::QueryProtocol::PrometheusHttp,
                language: QueryLanguage::promql,
                fallback: None,
            },
        };
        let http_server = HttpServer::new(http_config, query_engine, store.clone());
        tokio::spawn(async move {
            if let Err(e) = http_server.run().await {
                tracing::error!("Query server error: {}", e);
            }
        });
        info!("Query server started on port {}", args.query_port);
    }

    // Build the precompute engine config
    let engine_config = PrecomputeEngineConfig {
        num_workers: args.num_workers,
        ingest_port: args.ingest_port,
        allowed_lateness_ms: args.allowed_lateness_ms,
        max_buffer_per_series: args.max_buffer_per_series,
        flush_interval_ms: args.flush_interval_ms,
        channel_buffer_size: args.channel_buffer_size,
        pass_raw_samples: args.pass_raw_samples,
        raw_mode_aggregation_id: args.raw_mode_aggregation_id,
        late_data_policy: args.late_data_policy,
    };

    // Create the output sink (writes directly to the store)
    let output_sink: Arc<dyn query_engine_rust::precompute_engine::output_sink::OutputSink> =
        if args.pass_raw_samples {
            Arc::new(RawPassthroughSink::new(store))
        } else {
            Arc::new(StoreOutputSink::new(store))
        };

    // Build and run the engine
    let engine = PrecomputeEngine::new(engine_config, streaming_config, output_sink);

    info!("Starting precompute engine...");
    engine.run().await?;

    Ok(())
}
