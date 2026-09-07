//! Dashboard-panel evaluation harness for the precompute (non-ClickHouse)
//! side of the BGP end-to-end evaluation.
//!
//! Given a fixed set of "dashboard panel" query templates
//! (local_experiments/dashboard_panels.json) and a real MRT file, this:
//! 1. Instantiates every panel at every requested time range (1h/1d/3d/…)
//!    against a given range-end anchor, substituting the panel's `{T0}`/
//!    `{T1}` placeholders with literal timestamps.
//! 2. Builds a real planner input YAML from every instantiated query and
//!    runs the actual `asap-planner` binary against it - the same
//!    real-registration step every other verification this session used,
//!    not a shortcut.
//! 3. Ingests the real MRT file through the real MrtFileIngestSource into
//!    a real PrecomputeEngine, then explicitly shuts it down so every
//!    window - including month-sized ones - force-closes deterministically
//!    (relying on the wall-clock grace-period fallback instead would mean
//!    waiting out that period in real time, which is impractical at
//!    month-scale window sizes).
//! 4. Calls the real SimpleEngine::handle_query for every instantiated
//!    query, timing wall-clock latency and incremental process CPU time
//!    (via getrusage) per query.
//!
//! Usage:
//!   cargo run --release --bin dashboard_eval -- \
//!     --panels local_experiments/dashboard_panels.json \
//!     --mrt-path /path/to/file.gz \
//!     --range-end "2021-11-20 19:50:00" \
//!     --ranges 1h,1d \
//!     --planner-bin target/release/asap-planner \
//!     --repo-root .

use clap::Parser;
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use query_engine_rust::data_model::{LockStrategy, QueryLanguage};
use query_engine_rust::engines::SimpleEngine;
use query_engine_rust::precompute_engine::config::{LateDataPolicy, PrecomputeEngineConfig};
use query_engine_rust::precompute_engine::output_sink::StoreOutputSink;
use query_engine_rust::precompute_engine::{
    IngestSource, MrtBatchDirectoryIngestConfig, MrtBatchDirectoryIngestSource,
    MrtFileIngestConfig, MrtFileIngestSource, PrecomputeEngine,
};
use query_engine_rust::stores::SimpleMapStore;
use query_engine_rust::utils::file_io::{read_inference_config, read_streaming_config};

#[derive(Parser, Debug)]
#[command(about = "Dashboard-panel evaluation harness for the precompute system")]
struct Args {
    /// Path to dashboard_panels.json.
    #[arg(long)]
    panels: PathBuf,

    /// Path to a single real MRT file to ingest. Mutually exclusive with
    /// --mrt-dir; exactly one of the two must be given.
    #[arg(long)]
    mrt_path: Option<String>,

    /// Path to a directory of MRT files to ingest as one batch (e.g. a
    /// month of 5-minute RIS dumps) - every file present is ingested once,
    /// concurrently, then the router flushes and shuts down. Mutually
    /// exclusive with --mrt-path.
    #[arg(long)]
    mrt_dir: Option<String>,

    /// Max files ingested concurrently when using --mrt-dir.
    #[arg(long, default_value_t = 16)]
    mrt_dir_concurrency: usize,

    /// Collector name to attribute ingested samples to.
    #[arg(long, default_value = "rrc00")]
    collector: String,

    /// Metric name the panels' FROM clause targets (must match the
    /// planner input's table name).
    #[arg(long, default_value = "bgp_updates")]
    metric_name: String,

    /// End-of-range anchor, e.g. "2021-11-20 19:50:00" - each panel is
    /// instantiated as [range_end - duration, range_end).
    #[arg(long)]
    range_end: String,

    /// Comma-separated ranges: 1h,1d,3d,1w,1mo (1mo = 30 days).
    #[arg(long, default_value = "1h")]
    ranges: String,

    /// Only run these panel ids (comma-separated); default: all.
    #[arg(long)]
    only: Option<String>,

    /// Directory to write the planner's streaming_config/inference_config into.
    #[arg(long, default_value = "/tmp/dashboard_eval_planner_out")]
    planner_out: PathBuf,

    /// Seconds to wait after ingest completes before querying.
    #[arg(long, default_value_t = 3)]
    settle_secs: u64,

    /// Path to the asap-planner release binary.
    #[arg(long, default_value = "target/release/asap-planner")]
    planner_bin: PathBuf,

    /// Working directory to run the planner from (repo root).
    #[arg(long, default_value = ".")]
    repo_root: PathBuf,
}

#[derive(Deserialize, Clone)]
struct PanelDef {
    id: String,
    category: String,
    #[allow(dead_code)]
    shape: String,
    #[allow(dead_code)]
    source_id: String,
    title: String,
    sql: String,
}

#[derive(Deserialize)]
struct PanelsFile {
    panels: Vec<PanelDef>,
}

struct Instantiated {
    panel_id: String,
    category: String,
    title: String,
    range_label: String,
    range_ms: i64,
    query_group_id: u64,
    sql: String,
    time_secs: f64,
}

fn parse_datetime_ms(s: &str) -> i64 {
    use chrono::NaiveDateTime;
    let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .unwrap_or_else(|e| panic!("failed to parse datetime {s:?}: {e}"));
    dt.and_utc().timestamp_millis()
}

fn format_datetime(ms: i64) -> String {
    use chrono::{TimeZone, Utc};
    Utc.timestamp_millis_opt(ms)
        .single()
        .expect("timestamp out of range")
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

fn parse_range_ms(token: &str) -> i64 {
    let (num_str, unit) = token.split_at(
        token
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(token.len()),
    );
    let n: i64 = num_str
        .parse()
        .unwrap_or_else(|_| panic!("invalid range token {token:?}"));
    match unit {
        "h" => n * 3_600_000,
        "d" => n * 86_400_000,
        "w" => n * 7 * 86_400_000,
        "mo" => n * 30 * 86_400_000, // 30-day approximation, noted in output
        _ => panic!("unknown range unit in {token:?} (expected h/d/w/mo)"),
    }
}

/// Reads getrusage(RUSAGE_SELF), returning (peak RSS in KB, cumulative
/// user+sys CPU time in ms). Peak RSS is a high-water mark (never
/// decreases); CPU time is cumulative since process start, so callers
/// diff two readings to get an incremental cost.
fn read_rusage() -> (i64, f64) {
    unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut usage);
        #[cfg(target_os = "macos")]
        let peak_rss_kb = usage.ru_maxrss / 1024; // macOS reports bytes
        #[cfg(not(target_os = "macos"))]
        let peak_rss_kb = usage.ru_maxrss; // Linux reports KB already
        let cpu_ms = (usage.ru_utime.tv_sec as f64 * 1000.0
            + usage.ru_utime.tv_usec as f64 / 1000.0)
            + (usage.ru_stime.tv_sec as f64 * 1000.0 + usage.ru_stime.tv_usec as f64 / 1000.0);
        (peak_rss_kb, cpu_ms)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let panels_raw = std::fs::read_to_string(&args.panels)?;
    let panels_file: PanelsFile = serde_json::from_str(&panels_raw)?;
    let only: Option<Vec<String>> = args
        .only
        .as_ref()
        .map(|s| s.split(',').map(|x| x.trim().to_string()).collect());
    let panels: Vec<PanelDef> = panels_file
        .panels
        .into_iter()
        .filter(|p| only.as_ref().is_none_or(|o| o.contains(&p.id)))
        .collect();
    if panels.is_empty() {
        return Err("no panels selected (check --only against dashboard_panels.json ids)".into());
    }

    let range_end_ms = parse_datetime_ms(&args.range_end);
    let range_tokens: Vec<&str> = args.ranges.split(',').map(|s| s.trim()).collect();

    println!(
        "=== Instantiating {} panel(s) x {} range(s) = {} quer{} ===",
        panels.len(),
        range_tokens.len(),
        panels.len() * range_tokens.len(),
        if panels.len() * range_tokens.len() == 1 { "y" } else { "ies" }
    );

    let mut instantiated: Vec<Instantiated> = Vec::new();
    let mut group_id: u64 = 1;
    for range_tok in &range_tokens {
        let range_ms = parse_range_ms(range_tok);
        let t0_ms = range_end_ms - range_ms;
        let t1_ms = range_end_ms;
        let t0_str = format_datetime(t0_ms);
        let t1_str = format_datetime(t1_ms);
        for panel in &panels {
            let sql = panel.sql.replace("{T0}", &t0_str).replace("{T1}", &t1_str);
            instantiated.push(Instantiated {
                panel_id: panel.id.clone(),
                category: panel.category.clone(),
                title: panel.title.clone(),
                range_label: range_tok.to_string(),
                range_ms,
                query_group_id: group_id,
                sql,
                time_secs: t1_ms as f64 / 1000.0,
            });
            group_id += 1;
        }
    }

    // ---- Step 1: build planner input YAML and run the real planner ----
    let mut query_groups_yaml = String::new();
    for inst in &instantiated {
        query_groups_yaml.push_str(&format!(
            "  - id: {}\n    repetition_delay_ms: {}\n    controller_options:\n      accuracy_sla: 0.99\n      latency_sla: 100.0\n    queries:\n      - {:?}\n",
            inst.query_group_id, inst.range_ms, inst.sql,
        ));
    }
    let planner_input = format!(
        "tables:\n  - name: {metric}\n    time_column: timestamp\n    value_columns: []\n    metadata_columns: [operation, prefix, peer_ip, peer_asn, as_path, origin, next_hop, local_pref, med, communities, atomic, aggr_asn, aggr_ip, collector, source_file]\n\nquery_groups:\n{groups}\naggregate_cleanup:\n  policy: read_based\n",
        metric = args.metric_name,
        groups = query_groups_yaml,
    );

    let planner_input_path = args.planner_out.join("planner_input.yaml");
    std::fs::create_dir_all(&args.planner_out)?;
    std::fs::write(&planner_input_path, &planner_input)?;

    // The planner's own data-ingestion-interval-ms represents how often the
    // raw feed produces samples - a property of the data source, unrelated
    // to any individual panel's query window. Fixed at 60s, decoupled from
    // --ranges (each query_group carries its own repetition_delay_ms
    // matching its own range instead).
    const PLANNER_INGESTION_INTERVAL_MS: i64 = 60_000;
    println!("=== Running real planner ===");
    let planner_status = std::process::Command::new(&args.planner_bin)
        .args([
            "--query-language",
            "sql",
            "--input_config",
            planner_input_path.to_str().unwrap(),
            "--output_dir",
            args.planner_out.join("out").to_str().unwrap(),
            "--data-ingestion-interval-ms",
            &PLANNER_INGESTION_INTERVAL_MS.to_string(),
            "--streaming_engine",
            "precompute",
        ])
        .current_dir(&args.repo_root)
        .status()?;
    if !planner_status.success() {
        return Err(format!("planner exited with {planner_status}").into());
    }

    let streaming_config_path = args.planner_out.join("out/streaming_config.yaml");
    let inference_config_path = args.planner_out.join("out/inference_config.yaml");

    let inference_config = read_inference_config(
        inference_config_path.to_str().unwrap(),
        QueryLanguage::sql,
    )?;
    let planned_query_count = inference_config.query_configs.len();
    println!(
        "Planner registered {}/{} queries (rest fell back / punted - see planner log above)",
        planned_query_count,
        instantiated.len()
    );

    let cleanup_policy = inference_config.cleanup_policy;
    let streaming_config = Arc::new(read_streaming_config(
        streaming_config_path.to_str().unwrap(),
        &inference_config,
    )?);

    // ---- Step 2: real ingest ----
    let store: Arc<dyn query_engine_rust::stores::Store> = Arc::new(
        SimpleMapStore::new_with_strategy(streaming_config.clone(), cleanup_policy, LockStrategy::PerKey),
    );

    // One SimpleEngine per DISTINCT range/window size, all sharing the same
    // store - `data_ingestion_interval_ms` (the scrape interval) is used to
    // align a query's end timestamp down to a window boundary, so a single
    // shared engine configured for the smallest range would misalign every
    // other range's queries right off their actual registered window
    // (confirmed: this silently produced "no precomputed outputs found"
    // for every range except the one matching the shared interval).
    let mut distinct_ranges: Vec<i64> = instantiated.iter().map(|i| i.range_ms).collect();
    distinct_ranges.sort_unstable();
    distinct_ranges.dedup();
    let query_engines: std::collections::HashMap<i64, SimpleEngine> = distinct_ranges
        .iter()
        .map(|&range_ms| {
            let engine = SimpleEngine::new(
                store.clone(),
                inference_config.clone(),
                streaming_config.clone(),
                range_ms as u64,
                QueryLanguage::sql,
            );
            (range_ms, engine)
        })
        .collect();

    let engine_config = PrecomputeEngineConfig {
        num_workers: 4,
        allowed_lateness_ms: 5000,
        max_buffer_per_series: 50_000,
        flush_interval_ms: 200,
        channel_buffer_size: 20_000,
        pass_raw_samples: false,
        raw_mode_aggregation_id: 0,
        late_data_policy: LateDataPolicy::Drop,
        wall_clock_grace_period_ms: 5000,
    };
    let output_sink = Arc::new(StoreOutputSink::new(store.clone()));
    // Pull in whatever computed_label_cols/derived_value_cols the planner
    // auto-detected (e.g. a MOAS-style query needs `origin` re-emitted as
    // its own derived_value_origin_bgp_updates stream) - main.rs does this
    // exact merge for every real deployment; skipping it here silently
    // starves any aggregation that depends on a derived/computed column of
    // samples (confirmed: without this, moas_detection's CARDINALITY
    // aggregation matched zero of 8160 real samples).
    println!(
        "Merging {} computed label(s) and {} derived-value stream(s) the planner auto-detected",
        streaming_config.computed_label_cols.len(),
        streaming_config.derived_value_cols.len()
    );
    let (source, ingest_label): (Box<dyn IngestSource>, String) = match (&args.mrt_path, &args.mrt_dir) {
        (Some(path), None) => (
            Box::new(MrtFileIngestSource::new(MrtFileIngestConfig {
                path: path.clone(),
                metric_name: args.metric_name.clone(),
                collector: args.collector.clone(),
                computed_label_cols: streaming_config.computed_label_cols.clone(),
                stateful_transitions: streaming_config.stateful_transitions.clone(),
                derived_value_cols: streaming_config.derived_value_cols.clone(),
                batch_size: 5000,
            })),
            path.clone(),
        ),
        (None, Some(dir)) => (
            Box::new(MrtBatchDirectoryIngestSource::new(MrtBatchDirectoryIngestConfig {
                dir_path: dir.clone(),
                metric_name: args.metric_name.clone(),
                collector: args.collector.clone(),
                computed_label_cols: streaming_config.computed_label_cols.clone(),
                stateful_transitions: streaming_config.stateful_transitions.clone(),
                derived_value_cols: streaming_config.derived_value_cols.clone(),
                batch_size: 5000,
                concurrency: args.mrt_dir_concurrency,
                pace_interval_ms: None,
                max_files: None,
            })),
            dir.clone(),
        ),
        _ => return Err("exactly one of --mrt-path or --mrt-dir must be given".into()),
    };
    let sources: Vec<Box<dyn IngestSource>> = vec![source];
    let engine = PrecomputeEngine::new(engine_config, streaming_config.clone(), output_sink, sources);

    println!("=== Ingesting {} ===", ingest_label);
    let (rss_before_kb, cpu_before_ms) = read_rusage();
    let ingest_start = Instant::now();

    // Dedicated runtime, kept alive for the process's lifetime - matches
    // the pattern this session already verified live for MrtFileIngestSource
    // (a plain tokio::spawn on the ambient runtime panics on drop; see
    // mrt_ingest.rs's own live-verification test for why).
    let precompute_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let run_handle = precompute_rt.spawn(async move { engine.run().await });
    // MrtFileIngestSource shuts its router down explicitly once the file is
    // fully read, force-closing every window regardless of size - so
    // waiting on this join is the real ingest-complete signal, not a guess.
    // JoinHandles are cross-runtime awaitable, so this awaits directly on
    // the ambient runtime rather than block_on-ing precompute_rt (which
    // would panic: this whole fn already runs inside a tokio runtime, and
    // you can't block_on a second one from within the first).
    let ingest_result = run_handle.await;
    std::mem::forget(precompute_rt);
    match ingest_result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(format!("ingest source error: {e}").into()),
        Err(e) => return Err(format!("ingest task panicked: {e}").into()),
    }

    tokio::time::sleep(std::time::Duration::from_secs(args.settle_secs)).await;

    let ingest_wall_ms = ingest_start.elapsed().as_millis();
    let (rss_after_kb, cpu_after_ms) = read_rusage();
    println!(
        "Ingest complete: {}ms wall, {:.0}ms CPU, peak RSS {}KB -> {}KB",
        ingest_wall_ms,
        cpu_after_ms - cpu_before_ms,
        rss_before_kb,
        rss_after_kb,
    );

    // ---- Step 3: run every instantiated query, timing each ----
    println!("\n=== Query results ===");
    println!(
        "{:<26} {:<6} {:>10} {:>10} {:>8} {:<8}",
        "panel", "range", "latency_ms", "cpu_ms", "rows", "status"
    );
    let mut ok_count = 0;
    let mut empty_count = 0;
    let mut none_count = 0;
    for inst in &instantiated {
        let (_, cpu_before) = read_rusage();
        let t0 = Instant::now();
        let query_engine = query_engines
            .get(&inst.range_ms)
            .expect("engine for this range was built above");
        let result = query_engine.handle_query(inst.sql.clone(), inst.time_secs);
        let latency_ms = t0.elapsed().as_secs_f64() * 1000.0;
        let (_, cpu_after) = read_rusage();
        let cpu_ms = cpu_after - cpu_before;

        let (status, rows) = match &result {
            Some((_, query_engine_rust::engines::QueryResult::Vector(v))) => {
                ok_count += 1;
                if v.values.is_empty() {
                    empty_count += 1;
                    ("EMPTY", 0)
                } else {
                    ("OK", v.values.len())
                }
            }
            Some((_, query_engine_rust::engines::QueryResult::Matrix(m))) => {
                ok_count += 1;
                (if m.values.is_empty() { "EMPTY" } else { "OK" }, m.values.len())
            }
            None => {
                none_count += 1;
                ("NONE", 0)
            }
        };
        println!(
            "{:<26} {:<6} {:>10.2} {:>10.2} {:>8} {:<8}",
            inst.panel_id, inst.range_label, latency_ms, cpu_ms, rows, status
        );
        if std::env::var("DASHBOARD_EVAL_VERBOSE").is_ok() {
            println!("    [{}] {}", inst.category, inst.title);
            println!("    sql: {}", inst.sql);
            println!("    result: {:?}", result);
        }
    }

    println!(
        "\n=== Summary: {}/{} OK ({} empty, {} NONE/unplanned) ===",
        ok_count,
        instantiated.len(),
        empty_count,
        none_count
    );
    if range_tokens.iter().any(|t| *t == "1mo") {
        println!("(note: \"1mo\" range = 30 days, an approximation, not calendar-month-aware)");
    }

    Ok(())
}
