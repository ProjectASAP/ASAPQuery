//! E2E resource usage test for the precompute engine with quickstart-like data patterns.
//!
//! Simulates 7 fake exporters × 27,000 series each = 189,000 series of `sensor_reading`,
//! scraped at 1s intervals via Prometheus remote write, matching the quickstart setup.
//! After 10 seconds of ingestion, reports CPU and memory usage.
//!
//! Usage:
//!   cargo run --release --bin e2e_quickstart_resource_test

use prost::Message;
use query_engine_rust::data_model::{CleanupPolicy, LockStrategy, StreamingConfig};
use query_engine_rust::drivers::ingest::prometheus_remote_write::{
    Label, Sample, TimeSeries, WriteRequest,
};
use query_engine_rust::precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
use query_engine_rust::precompute_engine::output_sink::StoreOutputSink;
use query_engine_rust::precompute_engine::PrecomputeEngine;
use query_engine_rust::stores::{SimpleMapStore, Store};
use sketch_db_common::aggregation_config::AggregationConfig;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

const INGEST_PORT: u16 = 19400;
const NUM_WORKERS: usize = 4;
const DURATION_SECS: u64 = 10;

// Quickstart pattern: 7 exporters × 30×30×30 = 189,000 series
const PATTERNS: &[&str] = &[
    "constant",
    "linear-up",
    "linear-down",
    "sine",
    "sine-noise",
    "step",
    "exp-up",
];
const NUM_REGIONS: usize = 30;
const NUM_SERVICES: usize = 30;
const NUM_HOSTS: usize = 30;

fn build_remote_write_body(timeseries: Vec<TimeSeries>) -> Vec<u8> {
    let write_req = WriteRequest { timeseries };
    let proto_bytes = write_req.encode_to_vec();
    snap::raw::Encoder::new()
        .compress_vec(&proto_bytes)
        .expect("snappy compress failed")
}

fn make_sensor_reading(
    pattern: &str,
    region: &str,
    service: &str,
    host: &str,
    instance: &str,
    timestamp_ms: i64,
    value: f64,
) -> TimeSeries {
    TimeSeries {
        labels: vec![
            Label {
                name: "__name__".into(),
                value: "sensor_reading".into(),
            },
            Label {
                name: "host".into(),
                value: host.into(),
            },
            Label {
                name: "instance".into(),
                value: instance.into(),
            },
            Label {
                name: "job".into(),
                value: "pattern-exporters".into(),
            },
            Label {
                name: "pattern".into(),
                value: pattern.into(),
            },
            Label {
                name: "region".into(),
                value: region.into(),
            },
            Label {
                name: "service".into(),
                value: service.into(),
            },
        ],
        samples: vec![Sample {
            value,
            timestamp: timestamp_ms,
        }],
    }
}

/// Generate a value based on pattern type and timestamp
fn pattern_value(pattern: &str, t_secs: f64, base: f64) -> f64 {
    match pattern {
        "constant" => base * 1000.0,
        "linear-up" => base * 1000.0 + t_secs * 10.0,
        "linear-down" => base * 1000.0 - t_secs * 10.0,
        "sine" => base * 1000.0 + 500.0 * (t_secs * std::f64::consts::PI / 30.0).sin(),
        "sine-noise" => {
            base * 1000.0
                + 500.0 * (t_secs * std::f64::consts::PI / 30.0).sin()
                + 50.0 * ((t_secs * 7.3).sin())
        }
        "step" => {
            if (t_secs as i64 / 10) % 2 == 0 {
                base * 1000.0
            } else {
                base * 1000.0 + 500.0
            }
        }
        "exp-up" => base * 1000.0 * (1.0 + t_secs * 0.01).powf(2.0),
        _ => base * 1000.0,
    }
}

fn make_kll_streaming_config() -> Arc<StreamingConfig> {
    // Match quickstart: DatasketchesKLL, K=200, quantile by (pattern), window=10s tumbling
    let mut params = HashMap::new();
    params.insert("K".to_string(), serde_json::Value::from(200u64));

    // Grouping by pattern (spatial key), rolling up region/service/host/instance/job
    let grouping = promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![
        "pattern".to_string(),
    ]);
    let rollup = promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![
        "instance".to_string(),
        "job".to_string(),
        "region".to_string(),
        "service".to_string(),
        "host".to_string(),
    ]);
    let aggregated = promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]);

    let agg_config = AggregationConfig::new(
        1,
        "DatasketchesKLL".to_string(),
        String::new(),
        params,
        grouping,
        rollup,
        aggregated,
        String::new(),
        10, // window size = 10s (matching quickstart range-duration/step)
        10, // tumbling
        "tumbling".to_string(),
        "sensor_reading".to_string(),
        "sensor_reading".to_string(),
        None,
        None,
        None,
        None,
    );

    let mut agg_map = HashMap::new();
    agg_map.insert(1u64, agg_config);
    Arc::new(StreamingConfig::new(agg_map))
}

fn read_proc_status() -> (u64, u64, u64) {
    // Returns (VmRSS in KB, VmPeak in KB, VmSize in KB)
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let mut vm_rss = 0u64;
    let mut vm_peak = 0u64;
    let mut vm_size = 0u64;
    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            vm_rss = line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        } else if line.starts_with("VmPeak:") {
            vm_peak = line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        } else if line.starts_with("VmSize:") {
            vm_size = line
                .split_whitespace()
                .nth(1)
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
        }
    }
    (vm_rss, vm_peak, vm_size)
}

fn read_proc_cpu_time() -> (f64, f64) {
    // Returns (user_time_secs, system_time_secs) from /proc/self/stat
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let parts: Vec<&str> = stat.split_whitespace().collect();
    if parts.len() > 14 {
        let ticks_per_sec = 100.0; // typical Linux CLK_TCK
        let utime = parts[13].parse::<f64>().unwrap_or(0.0) / ticks_per_sec;
        let stime = parts[14].parse::<f64>().unwrap_or(0.0) / ticks_per_sec;
        (utime, stime)
    } else {
        (0.0, 0.0)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let streaming_config = make_kll_streaming_config();
    let store: Arc<dyn Store> = Arc::new(SimpleMapStore::new_with_strategy(
        streaming_config.clone(),
        CleanupPolicy::CircularBuffer,
        LockStrategy::PerKey,
    ));

    let engine_config = PrecomputeEngineConfig {
        num_workers: NUM_WORKERS,
        ingest_port: INGEST_PORT,
        allowed_lateness_ms: 5_000,
        max_buffer_per_series: 10_000,
        flush_interval_ms: 1_000,
        channel_buffer_size: 50_000,
        pass_raw_samples: false,
        raw_mode_aggregation_id: 0,
        late_data_policy: LateDataPolicy::Drop,
    };
    let output_sink = Arc::new(StoreOutputSink::new(store.clone()));
    let engine = PrecomputeEngine::new(engine_config, streaming_config, output_sink);
    tokio::spawn(async move {
        if let Err(e) = engine.run().await {
            eprintln!("Precompute engine error: {e}");
        }
    });

    // Wait for server to bind
    tokio::time::sleep(Duration::from_secs(1)).await;

    let series_per_pattern = NUM_REGIONS * NUM_SERVICES * NUM_HOSTS; // 27,000
    let total_series = PATTERNS.len() * series_per_pattern; // 189,000

    println!("=== Precompute Engine E2E Resource Test ===");
    println!("  Patterns: {} ({:?})", PATTERNS.len(), PATTERNS);
    println!(
        "  Series per pattern: {} ({}×{}×{})",
        series_per_pattern, NUM_REGIONS, NUM_SERVICES, NUM_HOSTS
    );
    println!("  Total series: {}", total_series);
    println!("  Workers: {}", NUM_WORKERS);
    println!("  Duration: {}s", DURATION_SECS);
    println!("  Aggregation: DatasketchesKLL K=200, tumbling 10s, group by pattern");
    println!();

    let (rss_before, _, _) = read_proc_status();
    let (cpu_user_before, cpu_sys_before) = read_proc_cpu_time();
    println!(
        "Before ingestion: VmRSS = {} KB ({:.1} MB)",
        rss_before,
        rss_before as f64 / 1024.0
    );

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(8)
        .build()?;

    let start = Instant::now();
    let mut total_samples_sent = 0u64;
    let mut tick = 0u64;

    println!("\n--- Sending data (simulating Prometheus scrape at 1s intervals) ---");

    while start.elapsed() < Duration::from_secs(DURATION_SECS) {
        let tick_start = Instant::now();
        let timestamp_ms = (tick * 1000 + 500) as i64; // mid-second
        let t_secs = tick as f64;

        // Build all timeseries for this tick.
        // In the quickstart, Prometheus batches all scraped series into remote write.
        // We send in chunks to avoid building a single massive request.
        let chunk_size = 10_000; // series per HTTP request
        let mut all_timeseries = Vec::with_capacity(total_series);

        for (p_idx, pattern) in PATTERNS.iter().enumerate() {
            let instance = format!("fake-exporter-{}:5000{}", pattern, p_idx);
            for r in 0..NUM_REGIONS {
                let region = format!("region{}", r);
                for s in 0..NUM_SERVICES {
                    let service = format!("svc{}", s);
                    for h in 0..NUM_HOSTS {
                        let host = format!("host{}", h);
                        let base = (r * NUM_SERVICES * NUM_HOSTS + s * NUM_HOSTS + h) as f64
                            / (series_per_pattern as f64);
                        let value = pattern_value(pattern, t_secs, base);
                        all_timeseries.push(make_sensor_reading(
                            pattern,
                            &region,
                            &service,
                            &host,
                            &instance,
                            timestamp_ms,
                            value,
                        ));
                    }
                }
            }
        }

        // Send in parallel chunks
        let mut handles = Vec::new();
        for chunk in all_timeseries.chunks(chunk_size) {
            let body = build_remote_write_body(chunk.to_vec());
            let client = client.clone();
            handles.push(tokio::spawn(async move {
                let resp = client
                    .post(format!("http://localhost:{INGEST_PORT}/api/v1/write"))
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .body(body)
                    .send()
                    .await;
                matches!(resp, Ok(r) if r.status().is_success() || r.status() == reqwest::StatusCode::NO_CONTENT)
            }));
        }

        let mut all_ok = true;
        for handle in handles {
            if !handle.await.unwrap_or(false) {
                all_ok = false;
            }
        }

        total_samples_sent += total_series as u64;
        let send_time = tick_start.elapsed();

        if tick % 2 == 0 || !all_ok {
            println!(
                "  tick={} t={}ms samples={} send_time={:.0}ms ok={}",
                tick,
                timestamp_ms,
                total_series,
                send_time.as_secs_f64() * 1000.0,
                all_ok
            );
        }

        tick += 1;

        // Sleep until next 1-second tick
        let elapsed_in_tick = tick_start.elapsed();
        if elapsed_in_tick < Duration::from_secs(1) {
            tokio::time::sleep(Duration::from_secs(1) - elapsed_in_tick).await;
        }
    }

    let wall_time = start.elapsed();

    // Wait a bit for processing to finish
    tokio::time::sleep(Duration::from_secs(2)).await;

    let (rss_after, vm_peak, vm_size) = read_proc_status();
    let (cpu_user_after, cpu_sys_after) = read_proc_cpu_time();

    let cpu_user = cpu_user_after - cpu_user_before;
    let cpu_sys = cpu_sys_after - cpu_sys_before;
    let cpu_total = cpu_user + cpu_sys;

    println!("\n=== Resource Usage Report (after {}s) ===", DURATION_SECS);
    println!("  Wall time:          {:.1}s", wall_time.as_secs_f64());
    println!("  Ticks completed:    {}", tick);
    println!("  Total samples sent: {}", total_samples_sent);
    println!(
        "  Avg throughput:     {:.0} samples/sec",
        total_samples_sent as f64 / wall_time.as_secs_f64()
    );
    println!();
    println!("  --- Memory ---");
    println!(
        "  VmRSS (current):    {} KB ({:.1} MB)",
        rss_after,
        rss_after as f64 / 1024.0
    );
    println!(
        "  VmPeak:             {} KB ({:.1} MB)",
        vm_peak,
        vm_peak as f64 / 1024.0
    );
    println!(
        "  VmSize:             {} KB ({:.1} MB)",
        vm_size,
        vm_size as f64 / 1024.0
    );
    println!(
        "  RSS delta:          {} KB ({:.1} MB)",
        rss_after.saturating_sub(rss_before),
        rss_after.saturating_sub(rss_before) as f64 / 1024.0
    );
    println!();
    println!("  --- CPU ---");
    println!("  User time:          {:.2}s", cpu_user);
    println!("  System time:        {:.2}s", cpu_sys);
    println!("  Total CPU time:     {:.2}s", cpu_total);
    println!(
        "  CPU utilization:    {:.1}% (of {:.1}s wall time)",
        cpu_total / wall_time.as_secs_f64() * 100.0,
        wall_time.as_secs_f64()
    );

    println!("\n=== Test complete ===");

    Ok(())
}
