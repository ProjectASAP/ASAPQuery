use asap_types::aggregation_config::AggregationConfig;
use asap_types::enums::{AggregationType, WindowType};
use clap::Parser;
use prost::Message;
use query_engine_rust::data_model::{
    AggregateCore, CleanupPolicy, LockStrategy, PrecomputedOutput, StreamingConfig,
};
use query_engine_rust::drivers::ingest::prometheus_remote_write::{
    Label, Sample, TimeSeries, WriteRequest,
};
use query_engine_rust::precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
use query_engine_rust::precompute_engine::output_sink::OutputSink;
use query_engine_rust::precompute_engine::PrecomputeEngine;
use query_engine_rust::stores::{SimpleMapStore, Store};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(name = "bench_precompute_sketch")]
#[command(about = "Benchmark the precompute engine with DatasketchesKLL accumulators")]
struct Args {
    #[arg(long, default_value_t = 4)]
    workers: usize,

    #[arg(long, default_value_t = 4)]
    concurrent_senders: usize,

    #[arg(long, default_value_t = 50)]
    num_series: usize,

    #[arg(long, default_value_t = 100)]
    samples_per_series: usize,

    #[arg(long, default_value_t = 100)]
    num_requests: usize,

    #[arg(long, default_value_t = 5)]
    latency_repetitions: usize,

    #[arg(long, default_value_t = 10)]
    window_size_secs: u64,

    #[arg(long, default_value_t = 200)]
    k: u16,

    #[arg(long, default_value_t = 19300)]
    latency_port: u16,

    #[arg(long, default_value_t = 19301)]
    throughput_port: u16,
}

struct TrackingStoreSink {
    store: Arc<dyn Store>,
    emitted_outputs: AtomicU64,
}

impl TrackingStoreSink {
    fn new(store: Arc<dyn Store>) -> Self {
        Self {
            store,
            emitted_outputs: AtomicU64::new(0),
        }
    }

    fn emitted(&self) -> u64 {
        self.emitted_outputs.load(Ordering::Relaxed)
    }
}

impl OutputSink for TrackingStoreSink {
    fn emit_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if outputs.is_empty() {
            return Ok(());
        }
        let emitted = outputs.len() as u64;
        self.store.insert_precomputed_output_batch(outputs)?;
        self.emitted_outputs.fetch_add(emitted, Ordering::Relaxed);
        Ok(())
    }
}

fn build_remote_write_body(timeseries: Vec<TimeSeries>) -> Vec<u8> {
    let write_req = WriteRequest { timeseries };
    let proto_bytes = write_req.encode_to_vec();
    snap::raw::Encoder::new()
        .compress_vec(&proto_bytes)
        .expect("snappy compression should succeed")
}

fn make_timeseries(metric: &str, label_0: &str, samples: Vec<Sample>) -> TimeSeries {
    TimeSeries {
        labels: vec![
            Label {
                name: "__name__".into(),
                value: metric.into(),
            },
            Label {
                name: "instance".into(),
                value: "bench".into(),
            },
            Label {
                name: "job".into(),
                value: "bench".into(),
            },
            Label {
                name: "label_0".into(),
                value: label_0.into(),
            },
        ],
        samples,
    }
}

fn make_kll_streaming_config(
    aggregation_id: u64,
    window_size_secs: u64,
    k: u16,
) -> Arc<StreamingConfig> {
    let mut params = HashMap::new();
    params.insert("K".to_string(), serde_json::Value::from(k as u64));

    let agg_config = AggregationConfig::new(
        aggregation_id,
        AggregationType::DatasketchesKLL,
        String::new(),
        params,
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        String::new(),
        window_size_secs,
        window_size_secs,
        WindowType::Tumbling,
        "bench_metric".to_string(),
        "bench_metric".to_string(),
        None,
        None,
        None,
        None,
    );

    let mut agg_map = HashMap::new();
    agg_map.insert(aggregation_id, agg_config);
    Arc::new(StreamingConfig::new(agg_map))
}

fn make_store(streaming_config: Arc<StreamingConfig>) -> Arc<dyn Store> {
    Arc::new(SimpleMapStore::new_with_strategy(
        streaming_config,
        CleanupPolicy::CircularBuffer,
        LockStrategy::PerKey,
    ))
}

async fn start_engine(
    port: u16,
    workers: usize,
    streaming_config: Arc<StreamingConfig>,
    sink: Arc<dyn OutputSink>,
) {
    let config = PrecomputeEngineConfig {
        num_workers: workers,
        ingest_port: port,
        allowed_lateness_ms: 5_000,
        max_buffer_per_series: 100_000,
        flush_interval_ms: 100,
        channel_buffer_size: 50_000,
        pass_raw_samples: false,
        raw_mode_aggregation_id: 0,
        late_data_policy: LateDataPolicy::Drop,
    };
    let engine = PrecomputeEngine::new(config, streaming_config, sink);
    tokio::spawn(async move {
        if let Err(err) = engine.run().await {
            eprintln!("precompute engine on port {port} failed: {err}");
        }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
}

fn build_window_body(
    metric: &str,
    num_series: usize,
    samples_per_series: usize,
    window_start_ms: i64,
    window_size_ms: i64,
) -> Vec<u8> {
    let mut timeseries = Vec::with_capacity(num_series);
    for series_idx in 0..num_series {
        let label = format!("series_{series_idx}");
        let mut samples = Vec::with_capacity(samples_per_series);
        for sample_idx in 0..samples_per_series {
            let offset = (sample_idx as i64) % window_size_ms.max(1);
            samples.push(Sample {
                value: (series_idx * samples_per_series + sample_idx) as f64,
                timestamp: window_start_ms + offset,
            });
        }
        timeseries.push(make_timeseries(metric, &label, samples));
    }
    build_remote_write_body(timeseries)
}

fn build_watermark_body(metric: &str, num_series: usize, timestamp_ms: i64) -> Vec<u8> {
    let mut timeseries = Vec::with_capacity(num_series);
    for series_idx in 0..num_series {
        let label = format!("series_{series_idx}");
        timeseries.push(make_timeseries(
            metric,
            &label,
            vec![Sample {
                value: 0.0,
                timestamp: timestamp_ms,
            }],
        ));
    }
    build_remote_write_body(timeseries)
}

async fn post_body(
    client: &reqwest::Client,
    port: u16,
    body: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let response = client
        .post(format!("http://localhost:{port}/api/v1/write"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(body)
        .send()
        .await?;
    if response.status() != reqwest::StatusCode::NO_CONTENT {
        return Err(format!("unexpected HTTP status {}", response.status()).into());
    }
    Ok(())
}

async fn wait_for_emitted_outputs(
    sink: &TrackingStoreSink,
    expected_outputs: u64,
    timeout: Duration,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();
    let deadline = start + timeout;
    loop {
        if sink.emitted() >= expected_outputs {
            return Ok(start.elapsed());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for outputs: expected {expected_outputs}, saw {}",
                sink.emitted()
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn run_latency_benchmark(
    client: &reqwest::Client,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let aggregation_id = 101;
    let metric = "bench_metric";
    let window_size_ms = (args.window_size_secs * 1000) as i64;
    let streaming_config = make_kll_streaming_config(aggregation_id, args.window_size_secs, args.k);
    let store = make_store(streaming_config.clone());
    let sink = Arc::new(TrackingStoreSink::new(store.clone()));

    start_engine(
        args.latency_port,
        args.workers,
        streaming_config,
        sink.clone(),
    )
    .await;

    let warmup_body = build_window_body(metric, 1, 1, 0, window_size_ms);
    let warmup_watermark = build_watermark_body(metric, 1, window_size_ms);
    post_body(client, args.latency_port, warmup_body).await?;
    post_body(client, args.latency_port, warmup_watermark).await?;
    wait_for_emitted_outputs(&sink, 1, Duration::from_secs(5)).await?;

    let mut latencies_ms = Vec::with_capacity(args.latency_repetitions);
    let mut rtts_ms = Vec::with_capacity(args.latency_repetitions);

    for rep in 0..args.latency_repetitions {
        let baseline = sink.emitted();
        let window_start_ms = ((rep as i64) + 2) * 2 * window_size_ms;
        let batch_body = build_window_body(
            metric,
            args.num_series,
            args.samples_per_series,
            window_start_ms,
            window_size_ms,
        );
        let watermark_body =
            build_watermark_body(metric, args.num_series, window_start_ms + window_size_ms);

        let t0 = Instant::now();
        post_body(client, args.latency_port, batch_body).await?;
        let batch_rtt = t0.elapsed();
        post_body(client, args.latency_port, watermark_body).await?;
        let e2e = wait_for_emitted_outputs(
            &sink,
            baseline + args.num_series as u64,
            Duration::from_secs(10),
        )
        .await?;

        latencies_ms.push(e2e.as_secs_f64() * 1000.0);
        rtts_ms.push(batch_rtt.as_secs_f64() * 1000.0);
    }

    let latency_store_results = store.query_precomputed_output(
        metric,
        aggregation_id,
        0,
        ((args.latency_repetitions as u64) + 10) * args.window_size_secs * 1000,
    )?;
    let stored_windows: usize = latency_store_results
        .values()
        .map(|buckets| buckets.len())
        .sum();

    println!("\n=== DatasketchesKLL latency benchmark ===");
    println!(
        "  Config: {} workers, {} series, {} samples/series, K={}, {} repetitions",
        args.workers, args.num_series, args.samples_per_series, args.k, args.latency_repetitions
    );
    println!(
        "  HTTP RTT ms: min {:.2}, mean {:.2}, max {:.2}",
        min_ms(&rtts_ms),
        mean_ms(&rtts_ms),
        max_ms(&rtts_ms)
    );
    println!(
        "  E2E latency ms: min {:.2}, mean {:.2}, max {:.2}",
        min_ms(&latencies_ms),
        mean_ms(&latencies_ms),
        max_ms(&latencies_ms)
    );
    println!(
        "  Stored windows: {} (expected at least {})",
        stored_windows,
        1 + args.latency_repetitions * args.num_series
    );

    Ok(())
}

async fn run_throughput_benchmark(
    client: &reqwest::Client,
    args: &Args,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let aggregation_id = 202;
    let metric = "bench_metric";
    let window_size_ms = (args.window_size_secs * 1000) as i64;
    let total_samples = (args.num_requests * args.num_series * args.samples_per_series) as u64;
    let expected_outputs = (args.num_requests * args.num_series) as u64;

    let streaming_config = make_kll_streaming_config(aggregation_id, args.window_size_secs, args.k);
    let store = make_store(streaming_config.clone());
    let sink = Arc::new(TrackingStoreSink::new(store.clone()));

    start_engine(
        args.throughput_port,
        args.workers,
        streaming_config,
        sink.clone(),
    )
    .await;

    let mut bodies = Vec::with_capacity(args.num_requests);
    for req_idx in 0..args.num_requests {
        let window_start_ms = (req_idx as i64) * window_size_ms;
        bodies.push(build_window_body(
            metric,
            args.num_series,
            args.samples_per_series,
            window_start_ms,
            window_size_ms,
        ));
    }
    let final_watermark = build_watermark_body(
        metric,
        args.num_series,
        (args.num_requests as i64) * window_size_ms,
    );

    let throughput_start = Instant::now();
    let mut chunks = vec![Vec::new(); args.concurrent_senders];
    for (idx, body) in bodies.into_iter().enumerate() {
        chunks[idx % args.concurrent_senders].push(body);
    }

    let mut handles = Vec::with_capacity(args.concurrent_senders);
    for chunk in chunks {
        let client = client.clone();
        let port = args.throughput_port;
        handles.push(tokio::spawn(async move {
            for body in chunk {
                post_body(&client, port, body).await?;
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }));
    }

    for handle in handles {
        handle.await??;
    }
    post_body(client, args.throughput_port, final_watermark).await?;
    let send_elapsed = throughput_start.elapsed();

    let wait_elapsed =
        wait_for_emitted_outputs(&sink, expected_outputs, Duration::from_secs(60)).await?;
    let total_elapsed = throughput_start.elapsed();

    let store_results = store.query_precomputed_output(
        metric,
        aggregation_id,
        0,
        ((args.num_requests as u64) + 2) * args.window_size_secs * 1000,
    )?;
    let stored_windows: usize = store_results.values().map(|buckets| buckets.len()).sum();

    println!("\n=== DatasketchesKLL throughput benchmark ===");
    println!(
        "  Config: {} workers, {} senders, {} requests, {} series, {} samples/series, K={}",
        args.workers,
        args.concurrent_senders,
        args.num_requests,
        args.num_series,
        args.samples_per_series,
        args.k
    );
    println!("  Total samples: {total_samples}");
    println!(
        "  Send throughput: {:.0} samples/sec ({:.1}ms)",
        total_samples as f64 / send_elapsed.as_secs_f64(),
        send_elapsed.as_secs_f64() * 1000.0
    );
    println!(
        "  E2E throughput: {:.0} samples/sec ({:.1}ms, drain wait {:.1}ms)",
        total_samples as f64 / total_elapsed.as_secs_f64(),
        total_elapsed.as_secs_f64() * 1000.0,
        wait_elapsed.as_secs_f64() * 1000.0
    );
    println!(
        "  Stored windows: {} (expected {})",
        stored_windows, expected_outputs
    );

    if stored_windows as u64 != expected_outputs {
        return Err(format!(
            "throughput benchmark stored {stored_windows} windows, expected {expected_outputs}"
        )
        .into());
    }

    Ok(())
}

fn min_ms(values: &[f64]) -> f64 {
    values.iter().copied().fold(f64::INFINITY, f64::min)
}

fn mean_ms(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn max_ms(values: &[f64]) -> f64 {
    values.iter().copied().fold(f64::NEG_INFINITY, f64::max)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .init();

    let client = reqwest::Client::new();

    run_latency_benchmark(&client, &args).await?;
    run_throughput_benchmark(&client, &args).await?;

    Ok(())
}
