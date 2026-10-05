//! Paced feeder: generates a seeded synthetic workload on the fly and sends it
//! in real time, one second of event time per wall-clock second, to either
//! ASAPQuery (Prometheus remote write) or ClickHouse (RowBinary INSERT).
//!
//! Two feeders with the same workload flags send byte-identical data to their
//! systems, so the systems can be compared on the same stream. Per-tick timing
//! is written to `--stats-out` so a run can be checked for keeping up.

mod encode;
mod workload;

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use encode::{Encoder, RemoteWriteEncoder, RowBinaryEncoder, ROW_BINARY_COLUMNS};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;
use workload::Workload;

#[derive(Debug, Clone, Copy, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
enum SinkKind {
    /// Prometheus remote write, e.g. to ASAPQuery's `/api/v1/write`.
    RemoteWrite,
    /// ClickHouse HTTP interface, `INSERT … FORMAT RowBinary`.
    Clickhouse,
}

#[derive(Debug, Parser)]
#[command(about = "Send a seeded synthetic workload in real time to ASAPQuery or ClickHouse")]
struct Args {
    #[arg(long, value_enum)]
    sink: SinkKind,
    /// Remote write endpoint (`http://host:port/api/v1/write`) or ClickHouse
    /// HTTP base URL (`http://host:8123`).
    #[arg(long)]
    url: String,
    /// Metric name sent as `__name__` (remote write only).
    #[arg(long, default_value = "data")]
    metric: String,
    /// Target table, optionally `database.table` (ClickHouse only). Must have
    /// columns `ts DateTime64(3), label_0 String, instance String, value Float64`.
    #[arg(long, default_value = "data")]
    table: String,
    #[arg(long, env = "CLICKHOUSE_USER")]
    clickhouse_user: Option<String>,
    #[arg(long, env = "CLICKHOUSE_PASSWORD")]
    clickhouse_password: Option<String>,

    /// Number of `label_0` values (C).
    #[arg(long)]
    groups: usize,
    /// Number of `instance` values per group (s).
    #[arg(long)]
    series_per_group: usize,
    #[arg(long, default_value_t = 1)]
    samples_per_sec: u32,
    #[arg(long, default_value_t = 1.5)]
    pareto_shape: f64,
    #[arg(long, default_value_t = 1.0)]
    pareto_scale: f64,
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Seconds of event time to send.
    #[arg(long)]
    duration_secs: u64,
    /// Event time of the first tick, in ms since the epoch. Defaults to the
    /// next whole wall-clock second, so `NOW()`-relative queries see live data.
    #[arg(long)]
    start_ms: Option<i64>,

    /// Rows per HTTP request. Defaults to 50,000 for remote write, which keeps
    /// a compressed request under axum's 2 MB body limit in ASAPQuery's ingest,
    /// and 100,000 for ClickHouse, where each INSERT creates a part.
    #[arg(long)]
    batch_rows: Option<usize>,
    /// Concurrent requests within one tick.
    #[arg(long, default_value_t = 16)]
    concurrency: usize,
    /// Where to write per-tick timing and totals as JSON.
    #[arg(long)]
    stats_out: Option<String>,
}

#[derive(Debug, Serialize)]
struct TickStats {
    tick: u64,
    rows: usize,
    bytes: usize,
    /// How late the tick started relative to its schedule.
    start_lag_ms: f64,
    /// Generation, encoding and sending, until the last request returned.
    elapsed_ms: f64,
}

#[derive(Debug, Serialize)]
struct RunStats {
    sink: SinkKind,
    groups: usize,
    series_per_group: usize,
    samples_per_sec: u32,
    pareto_shape: f64,
    pareto_scale: f64,
    seed: u64,
    start_ms: i64,
    duration_secs: u64,
    target_rows_per_sec: usize,
    rows_sent: usize,
    bytes_sent: usize,
    /// Rows sent divided by wall-clock time from the first tick's schedule to
    /// the later of the last tick's end and the run's nominal end.
    achieved_rows_per_sec: f64,
    /// Ticks whose work took longer than one second.
    overrun_ticks: usize,
    max_start_lag_ms: f64,
    failed_requests: usize,
    first_errors: Vec<String>,
    ticks: Vec<TickStats>,
}

struct Sink {
    client: reqwest::Client,
    request: reqwest::RequestBuilder,
    encoder: Box<dyn Encoder>,
}

impl Sink {
    fn new(args: &Args, workload: &Workload) -> Result<Self> {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(args.concurrency)
            .build()?;
        let (request, encoder): (_, Box<dyn Encoder>) = match args.sink {
            SinkKind::RemoteWrite => (
                client
                    .post(&args.url)
                    .header("Content-Encoding", "snappy")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Prometheus-Remote-Write-Version", "0.1.0"),
                Box::new(RemoteWriteEncoder::new(workload, &args.metric)),
            ),
            SinkKind::Clickhouse => {
                let query = format!(
                    "INSERT INTO {} ({ROW_BINARY_COLUMNS}) FORMAT RowBinary",
                    args.table
                );
                let mut request = client.post(&args.url).query(&[("query", query)]);
                if let Some(user) = &args.clickhouse_user {
                    request = request.header("X-ClickHouse-User", user);
                }
                if let Some(password) = &args.clickhouse_password {
                    request = request.header("X-ClickHouse-Key", password);
                }
                (request, Box::new(RowBinaryEncoder::new(workload)))
            }
        };
        Ok(Self {
            client,
            request,
            encoder,
        })
    }

    async fn send(&self, body: Vec<u8>) -> Result<()> {
        let request = self
            .request
            .try_clone()
            .context("request builder must be clonable")?
            .body(body)
            .build()?;
        let response = self.client.execute(request).await?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            bail!("HTTP {status}: {}", text.trim());
        }
        Ok(())
    }
}

fn next_whole_second_ms() -> i64 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the epoch")
        .as_millis() as i64;
    (now_ms / 1_000 + 1) * 1_000
}

/// Sleep until wall-clock `epoch_ms`, then return the monotonic instant it maps to.
async fn wait_for_wall_clock(epoch_ms: i64) -> Instant {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the epoch")
        .as_millis() as i64;
    if epoch_ms > now_ms {
        tokio::time::sleep(Duration::from_millis((epoch_ms - now_ms) as u64)).await;
    }
    Instant::now()
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.groups == 0 || args.series_per_group == 0 || args.samples_per_sec == 0 {
        bail!("--groups, --series-per-group and --samples-per-sec must be positive");
    }
    if args.pareto_shape <= 0.0 || args.pareto_scale <= 0.0 {
        bail!("--pareto-shape and --pareto-scale must be positive");
    }
    let batch_rows = args.batch_rows.unwrap_or(match args.sink {
        SinkKind::RemoteWrite => 50_000,
        SinkKind::Clickhouse => 100_000,
    });
    if batch_rows == 0 || args.concurrency == 0 {
        bail!("--batch-rows and --concurrency must be positive");
    }

    let workload = Workload {
        seed: args.seed,
        groups: args.groups,
        series_per_group: args.series_per_group,
        samples_per_sec: args.samples_per_sec,
        pareto_shape: args.pareto_shape,
        pareto_scale: args.pareto_scale,
    };
    let sink = Arc::new(Sink::new(&args, &workload)?);
    let permits = Arc::new(Semaphore::new(args.concurrency));
    let start_ms = args.start_ms.unwrap_or_else(next_whole_second_ms);

    eprintln!(
        "paced_feeder: {:?} -> {}, {} series x {}/s = {} rows/s for {}s, start_ms={start_ms}",
        args.sink,
        args.url,
        workload.num_series(),
        args.samples_per_sec,
        workload.rows_per_tick(),
        args.duration_secs
    );

    let base = wait_for_wall_clock(start_ms).await;
    let mut ticks = Vec::with_capacity(args.duration_secs as usize);
    let mut errors: Vec<String> = Vec::new();
    let mut failed_requests = 0;

    for tick in 0..args.duration_secs {
        let scheduled = base + Duration::from_secs(tick);
        tokio::time::sleep_until(scheduled.into()).await;
        let started = Instant::now();

        let rows = Arc::new(workload.tick_rows(tick, start_ms));
        let mut tasks = tokio::task::JoinSet::new();
        for begin in (0..rows.len()).step_by(batch_rows) {
            let end = (begin + batch_rows).min(rows.len());
            let (rows, sink) = (Arc::clone(&rows), Arc::clone(&sink));
            let permit = Arc::clone(&permits).acquire_owned().await?;
            tasks.spawn(async move {
                let _permit = permit;
                let body = sink.encoder.encode(&rows[begin..end]);
                let bytes = body.len();
                sink.send(body).await.map(|()| bytes)
            });
        }

        let mut bytes = 0;
        while let Some(joined) = tasks.join_next().await {
            match joined? {
                Ok(n) => bytes += n,
                Err(e) => {
                    failed_requests += 1;
                    if errors.len() < 10 {
                        errors.push(format!("tick {tick}: {e:#}"));
                    }
                }
            }
        }

        let stats = TickStats {
            tick,
            rows: rows.len(),
            bytes,
            start_lag_ms: started.duration_since(scheduled).as_secs_f64() * 1e3,
            elapsed_ms: started.elapsed().as_secs_f64() * 1e3,
        };
        if tick % 10 == 0 || stats.elapsed_ms > 1_000.0 {
            eprintln!(
                "tick {tick}: {} rows in {:.0} ms (start lag {:.0} ms)",
                stats.rows, stats.elapsed_ms, stats.start_lag_ms
            );
        }
        ticks.push(stats);
    }

    // A run that keeps up still spans the full duration, even though the last
    // tick's work ends before its second does.
    let wall_secs = base.elapsed().as_secs_f64().max(args.duration_secs as f64);
    let rows_sent: usize = ticks.iter().map(|t| t.rows).sum();
    let run = RunStats {
        sink: args.sink,
        groups: args.groups,
        series_per_group: args.series_per_group,
        samples_per_sec: args.samples_per_sec,
        pareto_shape: args.pareto_shape,
        pareto_scale: args.pareto_scale,
        seed: args.seed,
        start_ms,
        duration_secs: args.duration_secs,
        target_rows_per_sec: workload.rows_per_tick(),
        rows_sent,
        bytes_sent: ticks.iter().map(|t| t.bytes).sum(),
        achieved_rows_per_sec: rows_sent as f64 / wall_secs.max(f64::EPSILON),
        overrun_ticks: ticks.iter().filter(|t| t.elapsed_ms > 1_000.0).count(),
        max_start_lag_ms: ticks.iter().map(|t| t.start_lag_ms).fold(0.0, f64::max),
        failed_requests,
        first_errors: errors,
        ticks,
    };

    eprintln!(
        "done: {} rows, {:.0} rows/s achieved (target {}), {} overrun ticks, max start lag {:.0} ms, {} failed requests",
        run.rows_sent,
        run.achieved_rows_per_sec,
        run.target_rows_per_sec,
        run.overrun_ticks,
        run.max_start_lag_ms,
        run.failed_requests
    );
    if let Some(path) = &args.stats_out {
        std::fs::write(path, serde_json::to_vec_pretty(&run)?)
            .with_context(|| format!("writing {path}"))?;
    }
    if run.failed_requests > 0 {
        bail!(
            "{} requests failed; first: {:?}",
            run.failed_requests,
            run.first_errors
        );
    }
    Ok(())
}
