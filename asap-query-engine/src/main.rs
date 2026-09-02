mod engine_config;

use std::future::Future;

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::enums::{CleanupPolicy, QueryLanguage};
    use query_engine_rust::planner_client::PlannerResult;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn rejected_precompute_plan_does_not_apply_other_components() {
        let applied = Arc::new(AtomicBool::new(false));
        let applied_by_callback = applied.clone();
        let result = PlannerResult {
            streaming_config: StreamingConfig::default(),
            inference_config: InferenceConfig::new(QueryLanguage::promql, CleanupPolicy::NoCleanup),
            punted_queries: Vec::new(),
        };

        let update_result = apply_plan_if_precompute_succeeds(
            result,
            |_| async { Err::<(), _>("invalid streaming config".into()) },
            move |_| {
                applied_by_callback.store(true, Ordering::SeqCst);
            },
        )
        .await;

        assert!(update_result.is_err());
        assert!(!applied.load(Ordering::SeqCst));
    }
}

use clap::Parser;
use engine_config::{BackendConfig, EngineConfig, IngestConfig};
use figment::{
    providers::{Format, Yaml},
    Figment,
};
use std::fs;
use std::sync::{Arc, RwLock};
use tokio::signal;
use tracing::{debug, error, info, warn};

use asap_types::streaming_config::StreamingConfig;
use query_engine_rust::data_model::enums::{CleanupPolicy, StreamingEngine};
use query_engine_rust::drivers::AdapterConfig;
use query_engine_rust::precompute_engine::config::LateDataPolicy;
use query_engine_rust::precompute_engine::csv_ingest::{CsvFileIngestConfig, CsvFileIngestSource};
use query_engine_rust::precompute_engine::json_ingest::{
    JsonFileIngestConfig, JsonFileIngestSource, TimestampUnit,
};
use query_engine_rust::precompute_engine::PrecomputeWorkerDiagnostics;
use query_engine_rust::utils::file_io::{read_inference_config, read_streaming_config};
use query_engine_rust::InferenceConfig;
use query_engine_rust::{
    HttpIngestConfig, HttpIngestSource, HttpServer, HttpServerConfig, IngestSource, KafkaConsumer,
    KafkaConsumerConfig, OtlpReceiver, OtlpReceiverConfig, PrecomputeEngine,
    PrecomputeEngineConfig, PrecomputeEngineHandle, Result, SimpleEngine, SimpleMapStore,
    StoreOutputSink,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the engine YAML configuration file
    #[arg(long)]
    config_file: String,

    /// KEY=VALUE overrides applied on top of the config file (e.g. http_server.port=9000)
    overrides: Vec<String>,
}

async fn apply_plan_if_precompute_succeeds<F, Fut>(
    result: query_engine_rust::planner_client::PlannerResult,
    update_precompute: F,
    apply_other_components: impl FnOnce(query_engine_rust::planner_client::PlannerResult),
) -> Result<()>
where
    F: FnOnce(&StreamingConfig) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    update_precompute(&result.streaming_config).await?;
    apply_other_components(result);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut figment = Figment::new().merge(Yaml::file_exact(&args.config_file));
    for kv in &args.overrides {
        let (key, val_str) = kv
            .split_once('=')
            .ok_or_else(|| format!("invalid override '{kv}': expected KEY=VALUE"))?;
        // Parse as JSON so booleans and numbers get the right type; fall back to string.
        let val: serde_json::Value = serde_json::from_str(val_str)
            .unwrap_or_else(|_| serde_json::Value::String(val_str.to_string()));
        figment = figment.merge((key, val));
    }
    let config: EngineConfig = figment
        .extract()
        .map_err(|e| format!("Config error in {}: {e}", args.config_file))?;

    engine_config::check_config(&config).map_err(|e| format!("Invalid config: {e}"))?;

    // Create output directory
    fs::create_dir_all(&config.output_dir)?;

    // Keep the guard alive for the entire lifetime of the application
    let _log_guard = setup_logging(&config.output_dir, &config.log_level)?;

    debug!("Loaded config:\n{:#?}", config);
    info!("Starting Query Engine Rust");
    info!("Output directory: {}", config.output_dir);

    let query_language = config.backend.query_language();

    let inference_config = match &config.inference_config {
        Some(path) => {
            info!("Config file: {}", path);
            read_inference_config(path, query_language)?
        }
        None => {
            info!("No config file provided; starting with empty inference config");
            InferenceConfig::new(query_language, CleanupPolicy::NoCleanup)
        }
    };
    info!(
        "Loaded inference config with {} query configs",
        inference_config.query_configs.len()
    );

    let streaming_config = Arc::new(match &config.streaming_config {
        Some(path) => read_streaming_config(path, &inference_config)?,
        None => {
            info!("No streaming config file provided; starting with empty streaming config");
            StreamingConfig::default()
        }
    });
    info!(
        "Loaded streaming config with {} entries",
        streaming_config.get_all_aggregation_configs().len()
    );

    // Shared config refs — passed to QueryTracker so it can populate ControllerConfig
    // with the current configs as context for the planner. The applier task updates
    // them after applying a new plan so that subsequent windows see the latest state.
    let streaming_config_ref = Arc::new(RwLock::new(streaming_config.clone()));
    let inference_config_ref = Arc::new(RwLock::new(Arc::new(inference_config.clone())));

    let cleanup_policy = inference_config.cleanup_policy;
    info!("Using cleanup policy: {:?}", cleanup_policy);
    let store = Arc::new(SimpleMapStore::new_with_strategy(
        streaming_config.clone(),
        cleanup_policy,
        config.store.lock_strategy,
    ));

    let engine = Arc::new(SimpleEngine::new(
        store.clone(),
        inference_config,
        streaming_config.clone(),
        config.data_ingestion_interval_ms,
        query_language,
    ));

    // Kafka consumer — only when streaming_engine=arroyo and ingest.type=kafka.
    let kafka_handle = if config.streaming_engine == StreamingEngine::Arroyo {
        match &config.ingest {
            IngestConfig::Kafka {
                broker,
                topic,
                input_format,
                decompress_json,
            } => {
                let kafka_config = KafkaConsumerConfig {
                    broker: broker.clone(),
                    topic: topic.clone(),
                    group_id: "query-engine-rust".to_string(),
                    auto_offset_reset: "beginning".to_string(),
                    input_format: input_format.clone(),
                    decompress_json: *decompress_json,
                    batch_size: 1000,
                    poll_timeout_ms: 1000,
                    streaming_engine: config.streaming_engine.clone(),
                    dump_precomputes: config.precompute_engine.dump_precomputes,
                    dump_output_dir: if config.precompute_engine.dump_precomputes {
                        Some(config.output_dir.clone())
                    } else {
                        None
                    },
                };
                match KafkaConsumer::new(kafka_config, store.clone(), streaming_config.clone()) {
                    Ok(mut consumer) => {
                        info!("Starting Kafka consumer for topic: {}", topic);
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
                }
            }
            // OTLP uses its own receiver started below; kafka_handle is not needed.
            IngestConfig::Otlp { .. } => None,
            _ => unreachable!("check_config enforces arroyo requires kafka"),
        }
    } else {
        info!("Using precompute engine as streaming backend — skipping Kafka consumer");
        None
    };

    // OTLP receiver — only when ingest.type=otlp.
    let otel_handle = if let IngestConfig::Otlp {
        grpc_port,
        http_port,
    } = &config.ingest
    {
        let receiver = OtlpReceiver::new(OtlpReceiverConfig {
            grpc_port: *grpc_port,
            http_port: *http_port,
        });
        info!(
            "Starting OTLP receiver (gRPC port {}, HTTP port {})",
            grpc_port, http_port
        );
        Some(tokio::spawn(async move {
            if let Err(e) = receiver.run().await {
                error!("OTLP receiver error: {}", e);
            }
        }))
    } else {
        None
    };

    // Precompute engine — driven by streaming_engine=precompute.
    // check_config() already enforces the ingest source is compatible (http_remote_write or csv).
    let mut pe_engine_handle: Option<PrecomputeEngineHandle> = None;

    let _precompute_runtime = if config.streaming_engine == StreamingEngine::Precompute {
        let precompute_config = PrecomputeEngineConfig {
            num_workers: config.precompute_engine.num_workers,
            allowed_lateness_ms: config.precompute_engine.allowed_lateness_ms,
            max_buffer_per_series: config.precompute_engine.max_buffer_per_series,
            flush_interval_ms: config.precompute_engine.flush_interval_ms,
            channel_buffer_size: config.precompute_engine.channel_buffer_size,
            pass_raw_samples: false,
            raw_mode_aggregation_id: 0,
            late_data_policy: LateDataPolicy::Drop,
            wall_clock_grace_period_ms: config.precompute_engine.wall_clock_grace_period_ms,
        };
        let output_sink = Arc::new(StoreOutputSink::new(store.clone()));
        let sources: Vec<Box<dyn IngestSource>> = match &config.ingest {
            IngestConfig::Csv {
                path,
                metric_name,
                value_col,
                label_cols,
                timestamp_col,
                start_ts_ms,
                ts_step_ms,
                batch_size,
            } => {
                // ts_step_ms is only used for timestamp synthesis (when timestamp_col is absent).
                // check_config ensures it is present in that case.
                let ts_step = if timestamp_col.is_none() {
                    ts_step_ms.unwrap()
                } else {
                    0
                };
                info!("File ingest mode: {}", path);
                vec![Box::new(CsvFileIngestSource::new(CsvFileIngestConfig {
                    path: path.clone(),
                    metric_name: metric_name.clone(),
                    value_col: value_col.clone(),
                    label_cols: label_cols.clone(),
                    timestamp_col: timestamp_col.clone(),
                    start_ts_ms: *start_ts_ms,
                    ts_step_ms: ts_step,
                    batch_size: *batch_size,
                }))]
            }
            IngestConfig::Json {
                path,
                metric_name,
                value_col,
                label_cols,
                timestamp_col,
                timestamp_unit,
                batch_size,
            } => {
                let unit = timestamp_unit
                    .parse::<TimestampUnit>()
                    .map_err(|e| format!("Invalid timestamp_unit: {e}"))?;
                info!("JSON file ingest mode: {}", path);
                vec![Box::new(JsonFileIngestSource::new(JsonFileIngestConfig {
                    path: path.clone(),
                    metric_name: metric_name.clone(),
                    value_col: value_col.clone(),
                    label_cols: label_cols.clone(),
                    timestamp_col: timestamp_col.clone(),
                    timestamp_unit: unit,
                    batch_size: *batch_size,
                    batch_delay_ms: 0,
                }))]
            }
            IngestConfig::HttpRemoteWrite { port } => {
                info!("Starting precompute engine on port {}", port);
                vec![Box::new(HttpIngestSource::new(HttpIngestConfig {
                    port: *port,
                }))]
            }
            _ => unreachable!(
                "check_config enforces precompute requires http_remote_write, csv, or json"
            ),
        };
        let pe = PrecomputeEngine::new(
            precompute_config,
            streaming_config.clone(),
            output_sink,
            sources,
        );
        let worker_diagnostics = pe.diagnostics();
        pe_engine_handle = Some(pe.handle());

        let diag_store = store.clone();
        tokio::spawn(async move {
            spawn_memory_diagnostics(diag_store, Some(worker_diagnostics)).await;
        });

        let rt = tokio::runtime::Builder::new_multi_thread()
            .thread_name("pc-worker")
            .worker_threads(config.precompute_engine.num_workers)
            .enable_all()
            .build()
            .expect("failed to build precompute runtime");
        rt.spawn(async move {
            if let Err(e) = pe.run().await {
                error!("Precompute engine error: {}", e);
            }
        });
        Some(rt)
    } else {
        let diag_store = store.clone();
        tokio::spawn(async move {
            spawn_memory_diagnostics(diag_store, None).await;
        });
        None
    };

    let adapter_config = match &config.backend {
        BackendConfig::Prometheus {
            server,
            forward_unsupported_queries,
            fallback_timeout_secs,
        } => AdapterConfig::prometheus_promql(
            server.clone(),
            *forward_unsupported_queries,
            *fallback_timeout_secs,
        ),
        BackendConfig::Clickhouse {
            url,
            database,
            forward_unsupported_queries,
        } => AdapterConfig::clickhouse_sql(
            url.clone(),
            database.clone(),
            *forward_unsupported_queries,
        ),
        BackendConfig::ElasticQuerydsl {
            url,
            index,
            forward_unsupported_queries,
        } => AdapterConfig::elastic_querydsl(
            url.clone(),
            index.clone(),
            *forward_unsupported_queries,
        ),
        BackendConfig::ElasticSql {
            url,
            index,
            forward_unsupported_queries,
        } => AdapterConfig::elastic_sql(url.clone(), index.clone(), *forward_unsupported_queries),
    };

    let http_config = HttpServerConfig {
        port: config.http_server.port,
        handle_http_requests: true,
        adapter_config,
    };

    if config.backend.forward_unsupported_queries() {
        let client = reqwest::Client::new();
        let (health_url, backend_label) = match &config.backend {
            BackendConfig::Prometheus { server, .. } => (
                format!("{}/api/v1/status/runtimeinfo", server.trim_end_matches('/')),
                server.clone(),
            ),
            BackendConfig::Clickhouse { url, .. } => {
                (format!("{}/ping", url.trim_end_matches('/')), url.clone())
            }
            BackendConfig::ElasticQuerydsl { url, .. } | BackendConfig::ElasticSql { url, .. } => (
                format!("{}/_cluster/health", url.trim_end_matches('/')),
                url.clone(),
            ),
        };
        match client
            .get(&health_url)
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                info!("Backend reachable at {}", backend_label);
            }
            Ok(resp) => {
                error!(
                    "Backend at {} returned HTTP {} — cannot start",
                    backend_label,
                    resp.status()
                );
                std::process::exit(1);
            }
            Err(e) => {
                error!("Cannot reach backend at {}: {}", backend_label, e);
                std::process::exit(1);
            }
        }
    }

    let query_tracker = if config.query_tracker.enabled {
        use query_engine_rust::planner_client::{LocalPlannerClient, PlannerResult};
        use query_engine_rust::QueryTrackerConfig;

        let tracker_config = QueryTrackerConfig {
            observation_window_secs: config.query_tracker.observation_window_secs,
            data_ingestion_interval_ms: config.data_ingestion_interval_ms,
        };
        let runtime_options = asap_planner::RuntimeOptions {
            data_ingestion_interval_ms: config.data_ingestion_interval_ms,
            streaming_engine: asap_planner::StreamingEngine::Precompute,
            enable_punting: false,
            range_duration_ms: 300_000,
            step_ms: config.data_ingestion_interval_ms,
        };
        let prometheus_url = match &config.backend {
            BackendConfig::Prometheus { server, .. } => server.clone(),
            _ => unreachable!("check_config rejects non-prometheus backends with query_tracker"),
        };
        let planner_client = Arc::new(LocalPlannerClient::new(
            runtime_options,
            query_language,
            prometheus_url,
        ));

        let (plan_tx, plan_rx) = tokio::sync::watch::channel(None::<PlannerResult>);

        let tracker = Arc::new(query_engine_rust::QueryTracker::new(
            tracker_config,
            streaming_config_ref.clone(),
            inference_config_ref.clone(),
        ));
        let _tracker_handle = tracker.start_background_loop(planner_client, plan_tx);

        // Applier task: watches for plan results and applies them to all running components.
        // NOTE: streaming_config and inference_config are not applied atomically across
        // components. A brief window may exist where one component has the new config and
        // another still has the old one, causing query misses that fall back to Prometheus.
        // This is acceptable for a one-shot first-plan apply.
        let engine_for_applier = engine.clone();
        let store_for_applier = store.clone();
        let streaming_config_ref_for_applier = streaming_config_ref.clone();
        let inference_config_ref_for_applier = inference_config_ref.clone();
        tokio::spawn(async move {
            let mut rx = plan_rx;
            loop {
                if rx.changed().await.is_err() {
                    break;
                }
                let result = rx.borrow().clone();
                if let Some(result) = result {
                    let apply_result = apply_plan_if_precompute_succeeds(
                        result,
                        |streaming_config| {
                            let streaming_config = streaming_config.clone();
                            let precompute_handle = pe_engine_handle.as_ref();
                            async move {
                                if let Some(handle) = precompute_handle {
                                    handle.update_streaming_config(&streaming_config).await
                                } else {
                                    Ok(())
                                }
                            }
                        },
                        |result| {
                            engine_for_applier
                                .update_streaming_config(Arc::new(result.streaming_config.clone()));
                            engine_for_applier
                                .update_inference_config(result.inference_config.clone());
                            store_for_applier
                                .update_streaming_config(result.streaming_config.clone());
                            *streaming_config_ref_for_applier.write().unwrap() =
                                Arc::new(result.streaming_config);
                            *inference_config_ref_for_applier.write().unwrap() =
                                Arc::new(result.inference_config);
                            info!("Applier: applied new plan from query tracker");
                        },
                    )
                    .await;
                    if let Err(e) = apply_result {
                        warn!("Applier: failed to apply plan: {}", e);
                    }
                }
            }
        });

        info!(
            "Query tracker enabled (observation window: {}s)",
            config.query_tracker.observation_window_secs
        );
        Some(tracker)
    } else {
        None
    };

    let server = HttpServer::new(http_config, engine, store, query_tracker);
    info!("Starting HTTP server on port {}", config.http_server.port);

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

    if let Some(handle) = otel_handle {
        info!("Shutting down OTLP receiver...");
        handle.abort();
        let _ = handle.await;
    }

    info!("Shutdown complete");
    Ok(())
}

/// Periodic memory diagnostics logger — runs every 30 seconds.
async fn spawn_memory_diagnostics(
    store: Arc<SimpleMapStore>,
    worker_diagnostics: Option<Arc<PrecomputeWorkerDiagnostics>>,
) {
    use std::sync::atomic::Ordering;

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
    loop {
        interval.tick().await;

        // 1. Store diagnostics
        let store_diag = store.diagnostic_info();
        debug!(
            "[MEMORY_DIAG] Store: {} aggregation(s), {} total time_map entries, {:.2} KB total sketch bytes",
            store_diag.num_aggregations,
            store_diag.total_time_map_entries,
            store_diag.total_sketch_bytes as f64 / 1024.0,
        );
        for agg in &store_diag.per_aggregation {
            debug!(
                "[MEMORY_DIAG]   agg_id={}: time_map_len={}, read_counts_len={}, aggregate_objects={}, sketch_bytes={:.2} KB",
                agg.aggregation_id,
                agg.time_map_len,
                agg.read_counts_len,
                agg.num_aggregate_objects,
                agg.sketch_bytes as f64 / 1024.0,
            );
        }

        // 2. Worker diagnostics (precompute engine only)
        if let Some(ref diag) = worker_diagnostics {
            let total_groups: usize = diag
                .worker_group_counts
                .iter()
                .map(|c| c.load(Ordering::Relaxed))
                .sum();
            debug!(
                "[MEMORY_DIAG] PrecomputeEngine: {} total groups across {} workers",
                total_groups,
                diag.worker_group_counts.len(),
            );
            for (i, counter) in diag.worker_group_counts.iter().enumerate() {
                debug!(
                    "[MEMORY_DIAG]   worker_{}: group_states_len={}",
                    i,
                    counter.load(Ordering::Relaxed),
                );
            }
        }
    }
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
