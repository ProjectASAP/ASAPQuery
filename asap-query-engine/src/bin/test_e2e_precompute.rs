//! End-to-end test for the standalone precompute_engine binary.
//!
//! This binary:
//! 1. Starts a PrecomputeEngine in-process (same as the precompute_engine binary)
//! 2. Sends Prometheus remote write samples via HTTP
//! 3. Queries the PromQL endpoint and prints results
//!
//! Usage:
//!   cargo run --bin test_e2e_precompute

use prost::Message;
use query_engine_rust::data_model::{LockStrategy, QueryLanguage, StreamingConfig};
use query_engine_rust::drivers::ingest::prometheus_remote_write::{
    Label, Sample, TimeSeries, WriteRequest,
};
use query_engine_rust::drivers::query::adapters::AdapterConfig;
use query_engine_rust::engines::SimpleEngine;
use query_engine_rust::precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
use query_engine_rust::precompute_engine::output_sink::{
    NoopOutputSink, RawPassthroughSink, StoreOutputSink,
};
use query_engine_rust::precompute_engine::PrecomputeEngine;
use query_engine_rust::stores::SimpleMapStore;
use query_engine_rust::utils::file_io::{read_inference_config, read_streaming_config};
use query_engine_rust::{HttpServer, HttpServerConfig};
use sketch_db_common::aggregation_config::AggregationConfig;
use std::collections::HashMap;
use std::sync::Arc;

const INGEST_PORT: u16 = 19090;
const QUERY_PORT: u16 = 18080;
const RAW_INGEST_PORT: u16 = 19091;
const SCRAPE_INTERVAL: u64 = 1; // 1 second to match tumblingWindowSize

fn build_remote_write_body(timeseries: Vec<TimeSeries>) -> Vec<u8> {
    let write_req = WriteRequest { timeseries };
    let proto_bytes = write_req.encode_to_vec();
    snap::raw::Encoder::new()
        .compress_vec(&proto_bytes)
        .expect("snappy compress failed")
}

fn make_sample(metric: &str, label_0: &str, timestamp_ms: i64, value: f64) -> TimeSeries {
    TimeSeries {
        labels: vec![
            Label {
                name: "__name__".into(),
                value: metric.into(),
            },
            Label {
                name: "instance".into(),
                value: "i1".into(),
            },
            Label {
                name: "job".into(),
                value: "test".into(),
            },
            Label {
                name: "label_0".into(),
                value: label_0.into(),
            },
            Label {
                name: "label_1".into(),
                value: "v1".into(),
            },
        ],
        samples: vec![Sample {
            value,
            timestamp: timestamp_ms,
        }],
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .init();

    // Load configs the same way main.rs does
    let inference_config = read_inference_config(
        "examples/promql/inference_config.yaml",
        QueryLanguage::promql,
    )?;
    println!(
        "Loaded inference config with {} query configs",
        inference_config.query_configs.len()
    );
    for qc in &inference_config.query_configs {
        println!("  Query: '{}' -> {:?}", qc.query, qc.aggregations);
    }

    let cleanup_policy = inference_config.cleanup_policy;
    let streaming_config = Arc::new(read_streaming_config(
        "examples/promql/streaming_config.yaml",
        &inference_config,
    )?);
    println!(
        "Loaded streaming config with {} aggregation configs",
        streaming_config.get_all_aggregation_configs().len()
    );

    println!("\n=== Starting precompute engine (ingest={INGEST_PORT}, query={QUERY_PORT}) ===");

    // Create store
    let store: Arc<dyn query_engine_rust::stores::Store> =
        Arc::new(SimpleMapStore::new_with_strategy(
            streaming_config.clone(),
            cleanup_policy,
            LockStrategy::PerKey,
        ));

    // Start query server
    let query_engine = Arc::new(SimpleEngine::new(
        store.clone(),
        None, // no PromSketchStore for precompute E2E test
        inference_config,
        streaming_config.clone(),
        SCRAPE_INTERVAL,
        QueryLanguage::promql,
    ));
    let http_config = HttpServerConfig {
        port: QUERY_PORT,
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
            eprintln!("Query server error: {e}");
        }
    });

    // Start precompute engine
    let engine_config = PrecomputeEngineConfig {
        num_workers: 2,
        ingest_port: INGEST_PORT,
        allowed_lateness_ms: 5000,
        max_buffer_per_series: 10000,
        flush_interval_ms: 200,
        channel_buffer_size: 10000,
        pass_raw_samples: false,
        raw_mode_aggregation_id: 0,
        late_data_policy: LateDataPolicy::Drop,
    };
    let output_sink = Arc::new(StoreOutputSink::new(store.clone()));
    let engine = PrecomputeEngine::new(engine_config, streaming_config.clone(), output_sink);
    tokio::spawn(async move {
        if let Err(e) = engine.run().await {
            eprintln!("Precompute engine error: {e}");
        }
    });

    // Wait for servers to bind
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let client = reqwest::Client::new();

    // -----------------------------------------------------------------------
    // Send samples across multiple 1-second tumbling windows.
    // tumblingWindowSize=1 means windows are [0,1000), [1000,2000), etc.
    // We need enough windows of data so the query engine can find results.
    // -----------------------------------------------------------------------
    println!("\n=== Sending remote write samples ===");

    // Send 20 windows worth of data (timestamps 0ms..20000ms = 0s..20s)
    // Each window gets one sample.
    for window in 0..20 {
        let ts = window * 1000 + 500; // mid-window
        let val = 10.0 + window as f64;
        let body = build_remote_write_body(vec![make_sample("fake_metric", "groupA", ts, val)]);

        let resp = client
            .post(format!("http://localhost:{INGEST_PORT}/api/v1/write"))
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .body(body)
            .send()
            .await?;

        println!("  Sent t={ts}ms v={val} -> HTTP {}", resp.status().as_u16());
    }

    // Advance watermark well past to close all windows
    println!("\n=== Advancing watermark to close all windows ===");
    let body = build_remote_write_body(vec![make_sample("fake_metric", "groupA", 25000, 0.0)]);
    let resp = client
        .post(format!("http://localhost:{INGEST_PORT}/api/v1/write"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(body)
        .send()
        .await?;
    println!("  Sent t=25000ms v=0 -> HTTP {}", resp.status().as_u16());

    // Wait for flush + processing
    println!("\n  Waiting for flush...");
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // -----------------------------------------------------------------------
    // Query the PromQL endpoint
    // The inference_config has: "quantile by (label_0) (0.99, fake_metric)"
    // which maps to aggregation_id 1.
    // -----------------------------------------------------------------------
    println!("\n=== Querying PromQL endpoint ===");

    // Use the exact query pattern from inference_config
    let queries_instant = vec![
        (
            "quantile by (label_0) (0.99, fake_metric)",
            "10",
            "Configured query at t=10",
        ),
        (
            "quantile by (label_0) (0.99, fake_metric)",
            "15",
            "Configured query at t=15",
        ),
        (
            "sum_over_time(fake_metric[1s])",
            "10",
            "Temporal: sum_over_time at t=10",
        ),
        ("sum(fake_metric)", "10", "Spatial: sum at t=10"),
    ];

    for (query, time, label) in &queries_instant {
        println!("\n--- Instant query: {label} ---");
        let resp = client
            .get(format!("http://localhost:{QUERY_PORT}/api/v1/query"))
            .query(&[("query", *query), ("time", *time)])
            .send()
            .await?
            .text()
            .await?;
        print_json(&resp);
    }

    // Range query
    println!("\n--- Range query: quantile by (label_0) (0.99, fake_metric) t=5..20 step=1 ---");
    let resp = client
        .get(format!("http://localhost:{QUERY_PORT}/api/v1/query_range"))
        .query(&[
            ("query", "quantile by (label_0) (0.99, fake_metric)"),
            ("start", "5"),
            ("end", "20"),
            ("step", "1"),
        ])
        .send()
        .await?
        .text()
        .await?;
    print_json(&resp);

    // Runtime info
    println!("\n--- Runtime info ---");
    let resp = client
        .get(format!(
            "http://localhost:{QUERY_PORT}/api/v1/status/runtimeinfo"
        ))
        .send()
        .await?
        .text()
        .await?;
    print_json(&resp);

    // -----------------------------------------------------------------------
    // RAW MODE TEST
    // -----------------------------------------------------------------------
    println!("\n=== Starting raw-mode precompute engine (ingest={RAW_INGEST_PORT}) ===");

    // The raw engine reuses the same store so we can query results directly.
    // Pick aggregation_id = 1 to match the existing streaming config.
    let raw_agg_id: u64 = 1;
    let raw_engine_config = PrecomputeEngineConfig {
        num_workers: 4,
        ingest_port: RAW_INGEST_PORT,
        allowed_lateness_ms: 5000,
        max_buffer_per_series: 10000,
        flush_interval_ms: 200,
        channel_buffer_size: 10000,
        pass_raw_samples: true,
        raw_mode_aggregation_id: raw_agg_id,
        late_data_policy: LateDataPolicy::Drop,
    };
    let raw_sink = Arc::new(RawPassthroughSink::new(store.clone()));
    let raw_engine = PrecomputeEngine::new(raw_engine_config, streaming_config.clone(), raw_sink);
    tokio::spawn(async move {
        if let Err(e) = raw_engine.run().await {
            eprintln!("Raw precompute engine error: {e}");
        }
    });

    // Wait for server to bind
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Send a few raw samples — no need to advance watermark.
    println!("\n=== Sending raw-mode samples ===");
    let raw_timestamps = vec![100_000i64, 101_000, 102_000];
    let raw_values = vec![42.0f64, 43.0, 44.0];
    for (&ts, &val) in raw_timestamps.iter().zip(raw_values.iter()) {
        let body = build_remote_write_body(vec![make_sample("fake_metric", "groupA", ts, val)]);
        let resp = client
            .post(format!("http://localhost:{RAW_INGEST_PORT}/api/v1/write"))
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .body(body)
            .send()
            .await?;
        println!(
            "  Sent raw t={ts}ms v={val} -> HTTP {}",
            resp.status().as_u16()
        );
    }

    // Short wait for processing (no watermark advancement needed)
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Verify raw samples appeared in the store
    println!("\n=== Verifying raw samples in store ===");
    let results = store.query_precomputed_output("fake_metric", raw_agg_id, 100_000, 103_000)?;
    let total_buckets: usize = results.values().map(|v| v.len()).sum();
    println!("  Found {total_buckets} buckets for aggregation_id={raw_agg_id} in [100000, 103000)");
    assert!(
        total_buckets >= 3,
        "Expected at least 3 raw samples in store, got {total_buckets}"
    );

    for (key, buckets) in &results {
        for ((start, end), _acc) in buckets {
            println!("    key={key:?} start={start} end={end}");
        }
    }
    println!("  Raw mode test PASSED");

    // -----------------------------------------------------------------------
    // BATCH LATENCY TEST
    // Send 1000 samples in a single HTTP request to measure realistic e2e
    // latency. Uses the raw-mode engine to avoid window-close dependencies.
    // -----------------------------------------------------------------------
    println!("\n=== Batch latency test: 1000 samples in one request ===");

    // Build a single WriteRequest with 1000 TimeSeries entries spread across
    // 10 series × 100 timestamps each, so routing fans out to workers.
    let mut batch_timeseries = Vec::with_capacity(1000);
    for series_idx in 0..10 {
        let label_val = format!("batch_{series_idx}");
        for t in 0..100 {
            let ts = 200_000 + (series_idx as i64) * 1000 + t; // unique ts per sample
            let val = (series_idx * 100 + t) as f64;
            batch_timeseries.push(make_sample("fake_metric", &label_val, ts, val));
        }
    }
    let batch_body = build_remote_write_body(batch_timeseries);
    println!(
        "  Payload size: {} bytes (snappy-compressed)",
        batch_body.len()
    );

    let t0 = std::time::Instant::now();
    let resp = client
        .post(format!("http://localhost:{RAW_INGEST_PORT}/api/v1/write"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(batch_body)
        .send()
        .await?;
    let client_rtt = t0.elapsed();
    println!(
        "  HTTP response: {} in {:.3}ms",
        resp.status().as_u16(),
        client_rtt.as_secs_f64() * 1000.0,
    );

    // Wait for all workers to finish processing
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Verify samples landed in the store
    let batch_results =
        store.query_precomputed_output("fake_metric", raw_agg_id, 200_000, 210_000)?;
    let batch_buckets: usize = batch_results.values().map(|v| v.len()).sum();
    println!("  Stored {batch_buckets} buckets from batch (expected 1000)");
    assert!(
        batch_buckets >= 1000,
        "Expected at least 1000 raw samples in store, got {batch_buckets}"
    );
    println!("  Batch latency test PASSED");

    // -----------------------------------------------------------------------
    // THROUGHPUT TEST
    // Send many requests back-to-back and measure sustained throughput
    // (samples/sec). Uses the raw-mode engine for a clean measurement.
    // -----------------------------------------------------------------------
    println!("\n=== Throughput test: 1000 requests × 10000 samples ===");

    let num_requests = 1000u64;
    let samples_per_request = 10_000u64;
    let total_samples = num_requests * samples_per_request;

    // Pre-build all request bodies so serialization doesn't count against throughput
    let mut bodies = Vec::with_capacity(num_requests as usize);
    for req_idx in 0..num_requests {
        let mut timeseries = Vec::with_capacity(samples_per_request as usize);
        for s in 0..samples_per_request {
            let series_label = format!("tp_{}", s % 50); // 50 distinct series
            let ts = 300_000 + req_idx as i64 * 10_000 + s as i64;
            timeseries.push(make_sample("fake_metric", &series_label, ts, s as f64));
        }
        bodies.push(build_remote_write_body(timeseries));
    }

    let throughput_start = std::time::Instant::now();

    for (i, body) in bodies.into_iter().enumerate() {
        let resp = client
            .post(format!("http://localhost:{RAW_INGEST_PORT}/api/v1/write"))
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .body(body)
            .send()
            .await?;
        if resp.status() != reqwest::StatusCode::NO_CONTENT {
            eprintln!("  Request {i} failed: {}", resp.status());
        }
    }

    let send_elapsed = throughput_start.elapsed();
    println!(
        "  All {} requests sent in {:.1}ms",
        num_requests,
        send_elapsed.as_secs_f64() * 1000.0,
    );

    // Poll until workers drain or timeout after 60s
    let max_ts = 300_000u64 + num_requests * 10_000 + samples_per_request;
    let drain_deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut tp_buckets: usize;
    loop {
        let tp_results =
            store.query_precomputed_output("fake_metric", raw_agg_id, 300_000, max_ts)?;
        tp_buckets = tp_results.values().map(|v| v.len()).sum();
        if tp_buckets as u64 >= total_samples || std::time::Instant::now() > drain_deadline {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }
    let total_elapsed = throughput_start.elapsed();

    let send_throughput = total_samples as f64 / send_elapsed.as_secs_f64();
    let e2e_throughput = tp_buckets as f64 / total_elapsed.as_secs_f64();
    println!("  Stored {tp_buckets}/{total_samples} samples");
    println!(
        "  Send throughput:  {:.0} samples/sec ({:.1}ms for {total_samples} samples)",
        send_throughput,
        send_elapsed.as_secs_f64() * 1000.0,
    );
    println!(
        "  E2E throughput:   {:.0} samples/sec ({:.1}ms until all stored)",
        e2e_throughput,
        total_elapsed.as_secs_f64() * 1000.0,
    );
    assert!(
        tp_buckets as u64 >= total_samples,
        "Expected at least {total_samples} samples in store, got {tp_buckets}"
    );
    println!("  Throughput test PASSED");

    // -----------------------------------------------------------------------
    // WINDOWED AGGREGATION THROUGHPUT BENCHMARKS
    // Compare tumbling vs sliding window performance with the pane-based
    // engine. Each benchmark spins up its own PrecomputeEngine with a
    // NoopOutputSink (to isolate worker throughput from store I/O).
    // -----------------------------------------------------------------------
    let bench_results = run_windowed_benchmarks(&client).await?;
    println!("\n=== Windowed aggregation benchmark summary ===");
    println!(
        "  {:<30} {:>12} {:>12} {:>14}",
        "Config", "Send (s/s)", "E2E (s/s)", "Latency (ms)"
    );
    for r in &bench_results {
        println!(
            "  {:<30} {:>12.0} {:>12.0} {:>14.1}",
            r.label, r.send_throughput, r.e2e_throughput, r.batch_latency_ms
        );
    }

    println!("\n=== E2E test complete ===");

    Ok(())
}

// ---------------------------------------------------------------------------
// Windowed aggregation benchmarks
// ---------------------------------------------------------------------------

struct BenchResult {
    label: String,
    send_throughput: f64,
    e2e_throughput: f64,
    batch_latency_ms: f64,
}

/// Build an AggregationConfig for Sum with specified window parameters.
fn make_sum_agg_config(
    agg_id: u64,
    window_size_secs: u64,
    slide_interval_secs: u64,
) -> AggregationConfig {
    let window_type = if slide_interval_secs == 0 || slide_interval_secs == window_size_secs {
        "tumbling"
    } else {
        "sliding"
    };
    AggregationConfig::new(
        agg_id,
        "SingleSubpopulation".to_string(),
        "Sum".to_string(),
        HashMap::new(),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
        String::new(),
        window_size_secs,
        "bench_metric".to_string(),
        "bench_metric".to_string(),
        None,
        None,
        Some(window_size_secs),
        Some(slide_interval_secs),
        Some(window_type.to_string()),
        None,
        None,
    )
}

/// Run a single windowed benchmark and return the results.
async fn run_single_bench(
    client: &reqwest::Client,
    label: &str,
    port: u16,
    streaming_config: Arc<StreamingConfig>,
    num_requests: u64,
    samples_per_request: u64,
    num_series: u64,
) -> Result<BenchResult, Box<dyn std::error::Error + Send + Sync>> {
    let total_samples = num_requests * samples_per_request;

    let noop_sink = Arc::new(NoopOutputSink::new());
    let engine_config = PrecomputeEngineConfig {
        num_workers: 4,
        ingest_port: port,
        allowed_lateness_ms: 5000,
        max_buffer_per_series: 100_000,
        flush_interval_ms: 100,
        channel_buffer_size: 50_000,
        pass_raw_samples: false,
        raw_mode_aggregation_id: 0,
        late_data_policy: LateDataPolicy::Drop,
    };
    let engine = PrecomputeEngine::new(engine_config, streaming_config, noop_sink.clone());
    tokio::spawn(async move {
        if let Err(e) = engine.run().await {
            eprintln!("Bench engine error: {e}");
        }
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Pre-build request bodies. Timestamps are monotonically increasing
    // across requests so windows close naturally as the watermark advances.
    let mut bodies = Vec::with_capacity(num_requests as usize);
    for req_idx in 0..num_requests {
        let mut timeseries = Vec::with_capacity(samples_per_request as usize);
        for s in 0..samples_per_request {
            let series_label = format!("s_{}", s % num_series);
            // Each request advances time by 1000ms (1 second)
            let ts = (req_idx as i64) * 1000 + (s as i64 % 1000);
            timeseries.push(make_sample("bench_metric", &series_label, ts, s as f64));
        }
        bodies.push(build_remote_write_body(timeseries));
    }

    // --- Batch latency: single request ---
    let latency_body = bodies[0].clone();
    let t0 = std::time::Instant::now();
    client
        .post(format!("http://localhost:{port}/api/v1/write"))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .body(latency_body)
        .send()
        .await?;
    let batch_latency_ms = t0.elapsed().as_secs_f64() * 1000.0;

    // --- Throughput: all requests ---
    let throughput_start = std::time::Instant::now();
    for body in bodies {
        let resp = client
            .post(format!("http://localhost:{port}/api/v1/write"))
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .body(body)
            .send()
            .await?;
        if resp.status() != reqwest::StatusCode::NO_CONTENT {
            eprintln!("  {label}: request failed: {}", resp.status());
        }
    }
    let send_elapsed = throughput_start.elapsed();

    // Wait for workers to drain (poll emit_count on noop sink)
    let drain_deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let emitted = noop_sink
            .emit_count
            .load(std::sync::atomic::Ordering::Relaxed);
        if emitted > 0 || std::time::Instant::now() > drain_deadline {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    // Give workers a bit more time to finish in-flight work
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    let total_elapsed = throughput_start.elapsed();

    let emitted = noop_sink
        .emit_count
        .load(std::sync::atomic::Ordering::Relaxed);
    let send_throughput = total_samples as f64 / send_elapsed.as_secs_f64();
    let e2e_throughput = total_samples as f64 / total_elapsed.as_secs_f64();

    println!("  {label}:");
    println!(
        "    Sent {total_samples} samples in {:.1}ms ({:.0} samples/sec)",
        send_elapsed.as_secs_f64() * 1000.0,
        send_throughput
    );
    println!(
        "    E2E: {:.1}ms ({:.0} samples/sec), emitted {emitted} windows",
        total_elapsed.as_secs_f64() * 1000.0,
        e2e_throughput
    );
    println!("    Batch latency: {batch_latency_ms:.1}ms");

    Ok(BenchResult {
        label: label.to_string(),
        send_throughput,
        e2e_throughput,
        batch_latency_ms,
    })
}

async fn run_windowed_benchmarks(
    client: &reqwest::Client,
) -> Result<Vec<BenchResult>, Box<dyn std::error::Error + Send + Sync>> {
    let num_requests = 200u64;
    let samples_per_request = 5_000u64;
    let num_series = 50u64;

    let configs: Vec<(&str, u16, u64, u64)> = vec![
        // (label, port, window_size_secs, slide_interval_secs)
        ("Tumbling 10s Sum", 19100, 10, 0),
        ("Sliding 30s/10s Sum", 19101, 30, 10),
        ("Sliding 60s/10s Sum (W=6)", 19102, 60, 10),
    ];

    println!("\n=== Windowed aggregation benchmarks ({num_requests} req × {samples_per_request} samples, {num_series} series) ===");

    let mut results = Vec::new();
    for (label, port, window_size, slide_interval) in configs {
        let agg_config = make_sum_agg_config(100, window_size, slide_interval);
        let mut agg_map = HashMap::new();
        agg_map.insert(100u64, agg_config);
        let sc = Arc::new(StreamingConfig::new(agg_map));

        let r = run_single_bench(
            client,
            label,
            port,
            sc,
            num_requests,
            samples_per_request,
            num_series,
        )
        .await?;
        results.push(r);
    }

    Ok(results)
}

fn print_json(s: &str) {
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v) => println!("{}", serde_json::to_string_pretty(&v).unwrap()),
        Err(_) => println!("{s}"),
    }
}
