use clap::Parser;
use query_engine_rust::data_model::QueryLanguage;
use std::fs;
use std::sync::Arc;
use tokio::signal;
use tracing::{error, info};

use query_engine_rust::data_model::enums::{InputFormat, LockStrategy, StreamingEngine};
use query_engine_rust::drivers::AdapterConfig;
use query_engine_rust::utils::file_io::{read_inference_config, read_streaming_config};
use query_engine_rust::{
    HttpServer, HttpServerConfig, KafkaConsumer, KafkaConsumerConfig, PrometheusRemoteWriteConfig,
    PrometheusRemoteWriteServer, Result, SimpleEngine, SimpleMapStore,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Kafka topic to consume from
    #[arg(long)]
    kafka_topic: String,

    /// Input format for Kafka messages
    #[arg(long, value_enum)]
    input_format: InputFormat,

    /// Configuration file path
    #[arg(long)]
    config: String,

    /// File path for streaming_config
    #[arg(long)]
    streaming_config: String,

    /// Streaming engine to use
    #[arg(long, value_enum)]
    streaming_engine: StreamingEngine,

    /// Prometheus scrape interval in seconds
    #[arg(long)]
    prometheus_scrape_interval: u64,

    /// HTTP server port
    #[arg(long, default_value = "8088")]
    http_port: u16,

    /// Prometheus server URL
    #[arg(long, default_value = "http://localhost:9090")]
    prometheus_server: String,

    /// Forward unsupported queries to Prometheus
    #[arg(long)]
    forward_unsupported_queries: bool,

    /// Kafka broker address
    #[arg(long, default_value = "localhost:9092")]
    kafka_broker: String,

    /// Database path (currently unused, kept for compatibility)
    #[arg(long, default_value = "sketchdb.db")]
    db_path: String,

    /// Delete existing database (currently unused, kept for compatibility)
    #[arg(long)]
    delete_existing_db: bool,

    /// Output directory for logs
    #[arg(long)]
    output_dir: String,

    /// Log level
    #[arg(long, default_value = "INFO")]
    log_level: String,

    /// Enable profiling (currently unused, kept for compatibility)
    #[arg(long)]
    do_profiling: bool,

    /// Decompress JSON messages
    #[arg(long)]
    decompress_json: bool,

    /// Enable dumping received precomputes to files for debugging
    #[arg(long)]
    dump_precomputes: bool,

    /// Differentiate between query languages of input query
    #[arg(long, value_enum)]
    query_language: QueryLanguage,

    /// Lock strategy for SimpleMapStore: "global" for single mutex, "per-key" for fine-grained locking
    #[arg(long, value_enum)]
    lock_strategy: LockStrategy,

    /// Enable Prometheus remote write ingest endpoint
    #[arg(long)]
    enable_prometheus_remote_write: bool,

    /// Port for the Prometheus remote write endpoint
    #[arg(long, default_value = "9090")]
    prometheus_remote_write_port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Create output directory
    fs::create_dir_all(&args.output_dir)?;

    // Initialize logging similar to Python's create_loggers function
    // Keep the guard alive for the entire lifetime of the application
    let _log_guard = setup_logging(&args.output_dir, &args.log_level)?;

    info!("Starting Query Engine Rust");
    info!("Config file: {}", args.config);
    info!("Output directory: {}", args.output_dir);

    // Read config (equivalent to utils.file_io.read_inference_config)
    let inference_config = read_inference_config(&args.config, args.query_language)?;
    info!(
        "Loaded inference config with {} query configs",
        inference_config.query_configs.len()
    );
    info!("Inference config: {:?}", inference_config);

    let streaming_config = Arc::new(read_streaming_config(
        &args.streaming_config,
        &inference_config,
    )?);
    info!(
        "Loaded streaming config with {} entries",
        streaming_config.get_all_aggregation_configs().len()
    );
    info!("Streaming config: {:?}", streaming_config);

    // Setup store (equivalent to Python's SimpleMapStore())
    // Get cleanup policy from inference config
    let cleanup_policy = inference_config.cleanup_policy;
    info!("Using cleanup policy: {:?}", cleanup_policy);
    let store = Arc::new(SimpleMapStore::new_with_strategy(
        streaming_config.clone(),
        cleanup_policy,
        args.lock_strategy,
    ));

    // Setup query engine
    let engine = Arc::new(SimpleEngine::new(
        store.clone(),
        inference_config,
        streaming_config.clone(),
        args.prometheus_scrape_interval,
        args.query_language,
    ));

    // Setup Kafka consumer (equivalent to Python's kafka_thread)
    let kafka_config = KafkaConsumerConfig {
        broker: args.kafka_broker.clone(),
        topic: args.kafka_topic.clone(),
        group_id: "query-engine-rust".to_string(),
        auto_offset_reset: "beginning".to_string(),
        input_format: args.input_format,
        decompress_json: args.decompress_json,
        batch_size: 1000,
        poll_timeout_ms: 1000,
        streaming_engine: args.streaming_engine.clone(),
        dump_precomputes: args.dump_precomputes,
        dump_output_dir: if args.dump_precomputes {
            Some(args.output_dir.clone())
        } else {
            None
        },
    };

    let store_for_kafka = store.clone();
    let kafka_consumer_result =
        KafkaConsumer::new(kafka_config, store_for_kafka, streaming_config.clone());
    let kafka_handle = match kafka_consumer_result {
        Ok(mut consumer) => {
            info!("Starting Kafka consumer for topic: {}", args.kafka_topic);
            Some(tokio::spawn(async move {
                if let Err(e) = consumer.run().await {
                    error!("Kafka consumer error: {}", e);
                }
            }))
        }
        Err(e) => {
            error!("Failed to create Kafka consumer: {}", e);
            info!("Continuing without Kafka consumer");
            None
        }
    };

    // Setup Prometheus remote write server
    let prometheus_remote_write_handle = if args.enable_prometheus_remote_write {
        let prw_config = PrometheusRemoteWriteConfig {
            port: args.prometheus_remote_write_port,
        };
        let server = PrometheusRemoteWriteServer::new(prw_config);
        info!(
            "Starting Prometheus remote write server on port {}",
            args.prometheus_remote_write_port
        );
        Some(tokio::spawn(async move {
            if let Err(e) = server.run().await {
                error!("Prometheus remote write server error: {}", e);
            }
        }))
    } else {
        None
    };

    //info!("=== TEMPORARY: Using ClickHouse HTTP adapter ===");
    //info!("ClickHouse endpoint will be available at: /clickhouse/query");
    //info!("ClickHouse fallback URL: http://localhost:8123/?database=default");

    //let adapter_config = AdapterConfig::clickhouse_sql(
    //    "http://localhost:8123".to_string(), // ClickHouse server URL
    //    "default".to_string(),               // Database name
    //    true,                                // Always forward (fallback for every query)
    //);

    // Original Prometheus config (commented out temporarily):
    let adapter_config = AdapterConfig::elastic_sql(
        args.prometheus_server.clone(),
        "h2o_benchmark".to_string(),
        args.forward_unsupported_queries,
    );

    let http_config = HttpServerConfig {
        port: args.http_port,
        handle_http_requests: true,
        adapter_config,
    };

    let server = HttpServer::new(http_config, engine, store);
    info!("Starting HTTP server on port {}", args.http_port);

    // Wait for shutdown signal
    tokio::select! {
        result = server.run() => {
            if let Err(e) = result {
                error!("HTTP server error: {}", e);
            }
        }
        _ = signal::ctrl_c() => {
            info!("Shutdown signal received");
        }
    }

    // Cleanup - gracefully shutdown background tasks
    if let Some(handle) = kafka_handle {
        info!("Shutting down Kafka consumer...");
        handle.abort();
        let _ = handle.await;
    }

    if let Some(handle) = prometheus_remote_write_handle {
        info!("Shutting down Prometheus remote write server...");
        handle.abort();
        let _ = handle.await;
    }

    info!("Shutdown complete");
    Ok(())
}

fn setup_logging(
    output_dir: &str,
    log_level: &str,
) -> Result<tracing_appender::non_blocking::WorkerGuard> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    // Create env filter that respects RUST_LOG, with fallback to command line arg
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(log_level))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // Create file appender for logging to file
    let file_appender = tracing_appender::rolling::never(output_dir, "query_engine.log");
    let (non_blocking_file, guard) = tracing_appender::non_blocking(file_appender);

    // Create console layer for stdout
    let console_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .with_writer(std::io::stdout);

    // Create file layer for file output
    let file_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .with_ansi(false) // Disable ANSI color codes in log file
        .with_writer(non_blocking_file);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(console_layer)
        .with(file_layer)
        .init();

    info!("Logging initialized (respects RUST_LOG environment variable)");
    info!("Logs will be written to: {}/query_engine.log", output_dir);
    Ok(guard)
}
