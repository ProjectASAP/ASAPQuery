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
use query_engine_rust::data_model::{LockStrategy, QueryLanguage};
use query_engine_rust::drivers::ingest::prometheus_remote_write::{
    Label, Sample, TimeSeries, WriteRequest,
};
use query_engine_rust::drivers::query::adapters::AdapterConfig;
use query_engine_rust::engines::SimpleEngine;
use query_engine_rust::precompute_engine::config::PrecomputeEngineConfig;
use query_engine_rust::precompute_engine::output_sink::{RawPassthroughSink, StoreOutputSink};
use query_engine_rust::precompute_engine::PrecomputeEngine;
use query_engine_rust::stores::SimpleMapStore;
use query_engine_rust::utils::file_io::{read_inference_config, read_streaming_config};
use query_engine_rust::{HttpServer, HttpServerConfig};
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
        num_workers: 1,
        ingest_port: RAW_INGEST_PORT,
        allowed_lateness_ms: 5000,
        max_buffer_per_series: 10000,
        flush_interval_ms: 200,
        channel_buffer_size: 10000,
        pass_raw_samples: true,
        raw_mode_aggregation_id: raw_agg_id,
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
    let raw_timestamps = [100_000i64, 101_000, 102_000];
    let raw_values = [42.0f64, 43.0, 44.0];
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

    println!("\n=== E2E test complete ===");

    Ok(())
}

fn print_json(s: &str) {
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v) => println!("{}", serde_json::to_string_pretty(&v).unwrap()),
        Err(_) => println!("{s}"),
    }
}
