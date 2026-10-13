//! Paced feeder: generates a seeded synthetic workload on the fly and sends it
//! in real time, one second of event time per wall-clock second, to either
//! ASAPQuery (Prometheus remote write) or ClickHouse (RowBinary INSERT).
//!
//! Two feeders with the same workload flags send byte-identical data to their
//! systems, so the systems can be compared on the same stream. Per-tick timing
//! is written to `--stats-out` so a run can be checked for keeping up.

mod encode;
mod records;
mod table;
mod workload;

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use encode::{
    record_columns, row_binary_columns, Encoder, RecordsRemoteWriteEncoder,
    RecordsRowBinaryEncoder, RemoteWriteEncoder, RowBinaryEncoder,
};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;
use workload::{Rows, Workload};

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
    #[arg(long, value_enum, required_unless_present = "verify_facts")]
    sink: Option<SinkKind>,
    /// Remote write endpoint (`http://host:port/api/v1/write`) or ClickHouse
    /// HTTP base URL (`http://host:8123`).
    #[arg(long, required_unless_present = "verify_facts")]
    url: Option<String>,
    /// With `--groups`: the metric name sent as `__name__` (remote write
    /// only; default `data`). With `--workload-table`: send only this metric.
    #[arg(long)]
    metric: Option<String>,
    /// Target table, optionally `database.table` (ClickHouse with `--groups`
    /// only), made of `[A-Za-z0-9_]` characters. Must have columns `ts DateTime64(3), label_0 String, instance String, value Float64`.
    #[arg(long, default_value = "data", conflicts_with = "workload_table")]
    table: String,
    #[arg(long, env = "CLICKHOUSE_USER")]
    clickhouse_user: Option<String>,
    #[arg(long, env = "CLICKHOUSE_PASSWORD")]
    clickhouse_password: Option<String>,

    /// Generate the metrics of a sketch-bench workload table's
    /// `workloads[0].e2e.metrics` instead of the `--groups` workload. Each
    /// metric's ClickHouse table is named after it.
    #[arg(long, conflicts_with_all = ["groups", "series_per_group", "pareto_shape", "pareto_scale"])]
    workload_table: Option<String>,
    /// Number of `label_0` values (C).
    #[arg(long, required_unless_present = "workload_table")]
    groups: Option<usize>,
    /// Number of `instance` values per group (s).
    #[arg(long, required_unless_present = "workload_table")]
    series_per_group: Option<usize>,
    /// Samples per second per series. Defaults to 1 with `--groups`, and to
    /// the table's `samples_per_sec_per_series` with `--workload-table`.
    #[arg(long)]
    samples_per_sec: Option<u32>,
    /// Records per second of each `records` metric of `--workload-table`,
    /// overriding the table's `records_per_sec`, e.g. to scale a run down.
    #[arg(long, requires = "workload_table")]
    records_per_sec: Option<u64>,
    #[arg(long, default_value_t = 1.5)]
    pareto_shape: f64,
    #[arg(long, default_value_t = 1.0)]
    pareto_scale: f64,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    /// Print, for every subset of each metric's labels, the expected and the
    /// generated number of distinct label-value tuples, and for records
    /// metrics the expected and generated shares of each label's heaviest
    /// values, then exit without sending; non-zero if any check fails.
    #[arg(long)]
    verify_facts: bool,

    /// Seconds of event time to send.
    #[arg(long, required_unless_present = "verify_facts")]
    duration_secs: Option<u64>,
    /// Event time of the first tick, in ms since the epoch. Defaults to the
    /// next whole wall-clock second, so `NOW()`-relative queries see live data.
    #[arg(long)]
    start_ms: Option<i64>,

    /// Rows per HTTP request. Defaults to 50,000 for remote write, which keeps
    /// a compressed request under axum's 2 MB body limit in ASAPQuery's ingest,
    /// and 100,000 for ClickHouse, where each INSERT creates a part. For
    /// remote write it counts samples: a record is one per value field.
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
    /// Unset with `--workload-table`.
    groups: Option<usize>,
    series_per_group: Option<usize>,
    pareto_shape: Option<f64>,
    pareto_scale: Option<f64>,
    workload_table: Option<String>,
    metrics: Vec<String>,
    /// Unset without gauge/counter metrics.
    samples_per_sec: Option<u32>,
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

/// Where one metric's rows go: the request to send them with, their encoder
/// and the rows per request.
struct Target {
    request: reqwest::RequestBuilder,
    encoder: Box<dyn Encoder>,
    batch_rows: usize,
}

struct Sink {
    client: reqwest::Client,
    /// One per metric of the workload, in [`Workload::names`] order.
    targets: Vec<Target>,
}

impl Sink {
    fn new(
        args: &Args,
        sink: SinkKind,
        url: &str,
        workload: &Workload,
        batch_rows: usize,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(args.concurrency)
            .build()?;
        let remote_write = || {
            client
                .post(url)
                .header("Content-Encoding", "snappy")
                .header("Content-Type", "application/x-protobuf")
                .header("X-Prometheus-Remote-Write-Version", "0.1.0")
        };
        let clickhouse = |table: &str, columns: String| {
            let query = format!("INSERT INTO {table} ({columns}) FORMAT RowBinary");
            let mut request = client.post(url).query(&[("query", query)]);
            if let Some(user) = &args.clickhouse_user {
                request = request.header("X-ClickHouse-User", user);
            }
            if let Some(password) = &args.clickhouse_password {
                request = request.header("X-ClickHouse-Key", password);
            }
            request
        };
        let mut targets: Vec<Target> = workload
            .metrics
            .iter()
            .map(|metric| match sink {
                SinkKind::RemoteWrite => Target {
                    request: remote_write(),
                    encoder: Box::new(RemoteWriteEncoder::new(metric, &metric.name)),
                    batch_rows,
                },
                SinkKind::Clickhouse => {
                    let table = if args.workload_table.is_some() {
                        &metric.name
                    } else {
                        &args.table
                    };
                    Target {
                        request: clickhouse(table, row_binary_columns(metric)),
                        encoder: Box::new(RowBinaryEncoder::new(metric)),
                        batch_rows,
                    }
                }
            })
            .collect();
        targets.extend(workload.records.iter().map(|metric| match sink {
            // Remote write's batch counts samples, one per value field.
            SinkKind::RemoteWrite => Target {
                request: remote_write(),
                encoder: Box::new(RecordsRemoteWriteEncoder::new(Arc::clone(metric))),
                batch_rows: (batch_rows / metric.fields.len()).max(1),
            },
            SinkKind::Clickhouse => Target {
                request: clickhouse(&metric.name, record_columns(metric)),
                encoder: Box::new(RecordsRowBinaryEncoder::new(Arc::clone(metric))),
                batch_rows,
            },
        }));
        Ok(Self { client, targets })
    }

    async fn send(
        &self,
        metric: usize,
        rows: &Rows,
        range: std::ops::Range<usize>,
    ) -> Result<usize> {
        let target = &self.targets[metric];
        let body = target.encoder.encode(rows, range);
        let bytes = body.len();
        let request = target
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
        Ok(bytes)
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

fn build_workload(args: &Args) -> Result<Workload> {
    if let Some(path) = &args.workload_table {
        return table::load(
            path,
            args.metric.as_deref(),
            args.samples_per_sec,
            args.records_per_sec,
            args.seed,
        );
    }
    let (groups, series_per_group) = (
        args.groups.unwrap_or_default(),
        args.series_per_group.unwrap_or_default(),
    );
    if groups == 0 || series_per_group == 0 {
        bail!("--groups and --series-per-group must be positive");
    }
    if args.pareto_shape <= 0.0 || args.pareto_scale <= 0.0 {
        bail!("--pareto-shape and --pareto-scale must be positive");
    }
    Ok(Workload::grouped(
        args.metric.as_deref().unwrap_or("data"),
        args.seed,
        groups,
        series_per_group,
        args.samples_per_sec.unwrap_or(1),
        args.pareto_shape,
        args.pareto_scale,
    ))
}

/// Prints the facts check of every metric; true if every fact holds.
fn verify_facts(workload: &Workload) -> Result<bool> {
    let mut ok = true;
    for metric in &workload.metrics {
        println!("{}: {} series", metric.name, metric.series.len());
        ok &= print_label_facts(&metric.labels, &metric.series);
    }
    for metric in &workload.records {
        // Every combination of label values a record can have.
        let domain = table::series_labels(&metric.labels)?;
        println!(
            "{}: {} records/s, {} label combinations",
            metric.name,
            metric.records_per_sec,
            domain.len()
        );
        ok &= print_label_facts(&metric.labels, &domain);
        let report = metric.share_checks(records::SHARE_SAMPLE_RECORDS);
        println!(
            "  shares of {} records (tolerance {} standard errors)",
            report.records,
            records::SHARE_TOLERANCE_SIGMAS
        );
        println!(
            "  {:<12} {:<28} {:>10} {:>10} {:>10}",
            "label", "value", "expected", "generated", "tolerance"
        );
        for check in &report.checks {
            let holds = check.holds();
            ok &= holds;
            println!(
                "  {:<12} {:<28} {:>10.5} {:>10.5} {:>10.5}{}",
                check.label,
                check.value,
                check.expected,
                check.actual,
                check.tolerance,
                if holds { "" } else { "  MISMATCH" }
            );
        }
        if let Some((seen, keys)) = report.burst_keys {
            println!("  burst keys: {seen} distinct of {keys}");
        }
    }
    Ok(ok)
}

/// Prints the distinct label-value tuples of every label subset of `series`;
/// true if each matches the description.
fn print_label_facts(labels: &[workload::LabelSpec], series: &[Vec<String>]) -> bool {
    let mut ok = true;
    println!("  {:<40} {:>12} {:>12}", "labels", "expected", "generated");
    for fact in table::facts(labels, series) {
        let holds = fact.expected == fact.actual;
        ok &= holds;
        println!(
            "  {:<40} {:>12} {:>12}{}",
            fact.labels.join(","),
            fact.expected,
            fact.actual,
            if holds { "" } else { "  MISMATCH" }
        );
    }
    ok
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let workload = build_workload(&args)?;
    if !workload.metrics.is_empty() {
        if workload.samples_per_sec == 0 {
            bail!("samples per second must be positive");
        }
        // Timestamps are in ms, so more samples per second would repeat a
        // timestamp within a series, which remote write drops and ClickHouse keeps.
        if workload.samples_per_sec > 1_000 {
            bail!("samples per second must be at most 1000");
        }
    }
    if args.verify_facts {
        if !verify_facts(&workload)? {
            bail!("the generated data does not match the description");
        }
        return Ok(());
    }
    let (Some(sink_kind), Some(url), Some(duration_secs)) =
        (args.sink, args.url.as_deref(), args.duration_secs)
    else {
        bail!("--sink, --url and --duration-secs are required");
    };

    // The table name is interpolated into the INSERT query.
    if args.table.is_empty()
        || !args
            .table
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        bail!("--table must be made of [A-Za-z0-9_.] characters");
    }
    let batch_rows = args.batch_rows.unwrap_or(match sink_kind {
        SinkKind::RemoteWrite => 50_000,
        SinkKind::Clickhouse => 100_000,
    });
    if batch_rows == 0 || args.concurrency == 0 {
        bail!("--batch-rows and --concurrency must be positive");
    }

    let sink = Arc::new(Sink::new(&args, sink_kind, url, &workload, batch_rows)?);
    let permits = Arc::new(Semaphore::new(args.concurrency));
    let start_ms = args.start_ms.unwrap_or_else(next_whole_second_ms);
    let metric_names = workload.names();

    let series = if workload.metrics.is_empty() {
        String::new()
    } else {
        format!(
            " ({} series x {}/s)",
            workload.num_series(),
            workload.samples_per_sec
        )
    };
    eprintln!(
        "paced_feeder: {sink_kind:?} -> {url}, {}{series} = {} rows/s for {duration_secs}s, start_ms={start_ms}",
        metric_names.join(", "),
        workload.rows_per_tick(),
    );

    let base = wait_for_wall_clock(start_ms).await;
    let mut ticks = Vec::with_capacity(duration_secs as usize);
    let mut errors: Vec<String> = Vec::new();
    let mut failed_requests = 0;

    for tick in 0..duration_secs {
        let scheduled = base + Duration::from_secs(tick);
        tokio::time::sleep_until(scheduled.into()).await;
        let started = Instant::now();

        let (mut rows, mut bytes) = (0, 0);
        // A tick's requests go out concurrently, so a group's samples within
        // the tick may arrive out of order. That is safe for ASAPQuery: the
        // tick lies in one window (see `tick_rows`) and is under one second
        // behind the watermark, within the allowed lateness.
        let mut tasks = tokio::task::JoinSet::new();
        for metric in 0..metric_names.len() {
            let metric_rows = Arc::new(workload.tick_rows(metric, tick, start_ms));
            let batch_rows = sink.targets[metric].batch_rows;
            for begin in (0..metric_rows.len()).step_by(batch_rows) {
                let end = (begin + batch_rows).min(metric_rows.len());
                let (metric_rows, sink) = (Arc::clone(&metric_rows), Arc::clone(&sink));
                let permit = Arc::clone(&permits).acquire_owned().await?;
                tasks.spawn(async move {
                    let _permit = permit;
                    let sent = sink.send(metric, &metric_rows, begin..end).await;
                    sent.map(|bytes| (end - begin, bytes))
                });
            }
        }

        while let Some(joined) = tasks.join_next().await {
            // A panicked task counts as a failed request, so the stats of
            // the ticks before it are still written.
            match joined.map_err(anyhow::Error::from).and_then(|sent| sent) {
                Ok((n_rows, n_bytes)) => {
                    rows += n_rows;
                    bytes += n_bytes;
                }
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
            rows,
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
        // Nothing got through, e.g. a wrong --url, table or credentials: stop
        // rather than fail every request for the rest of the run.
        if rows == 0 {
            eprintln!("tick {tick}: every request failed, stopping");
            break;
        }
    }

    // A run that keeps up still spans the full duration, even though the last
    // tick's work ends before its second does.
    let wall_secs = base.elapsed().as_secs_f64().max(duration_secs as f64);
    let rows_sent: usize = ticks.iter().map(|t| t.rows).sum();
    let grouped = args.workload_table.is_none();
    let run = RunStats {
        sink: sink_kind,
        groups: args.groups,
        series_per_group: args.series_per_group,
        pareto_shape: grouped.then_some(args.pareto_shape),
        pareto_scale: grouped.then_some(args.pareto_scale),
        workload_table: args.workload_table.clone(),
        metrics: metric_names,
        samples_per_sec: (!workload.metrics.is_empty()).then_some(workload.samples_per_sec),
        seed: args.seed,
        start_ms,
        duration_secs,
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
