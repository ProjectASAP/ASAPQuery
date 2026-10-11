//! Remote-write load generator for throughput benchmarks of the standalone
//! `precompute_engine` binary. Run it as a separate process on separate cores
//! so its own CPU never counts against the engine.
//!
//! Series `i` carries labels `l0..l4` = the base-10 digits of `i` (`l4` takes
//! the remainder), so grouping by a label prefix gives a known fan-in. Each
//! pass sends one sample per series and advances event time by `step_ms`, so
//! window closes per sample are fixed regardless of send rate.
//!
//! Prints one JSON line: acked samples/sec and engine CPU cores over the
//! measurement window (after warmup).

use clap::Parser;
use prost::Message;
use query_engine_rust::drivers::ingest::prometheus_remote_write::{
    Label, Sample, TimeSeries, WriteRequest,
};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

const METRIC_NAME: &str = "bench_metric";
const NUM_LABELS: usize = 5;
const LABEL_RADIX: usize = 10;
/// Sample values are uniform in `[0, VALUE_RANGE)`.
const VALUE_RANGE: f64 = 10_000.0;
/// Bodies an encoder thread may build ahead of its sender.
const ENCODE_AHEAD: usize = 8;
/// Max passes a sender may lead the slowest one. Groups span senders and the
/// engine keeps one watermark per group, so `MAX_PASS_SKEW * step_ms` must
/// stay below the engine's `--allowed_lateness_ms`, or the lagging sender's
/// samples are dropped as late.
const MAX_PASS_SKEW: u64 = 2;
const SKEW_POLL_INTERVAL: Duration = Duration::from_micros(200);

#[derive(Parser, Debug)]
#[command(name = "bench_ingest_sender")]
struct Args {
    /// PID of the precompute_engine process to measure.
    #[arg(long)]
    engine_pid: u32,

    #[arg(long)]
    port: u16,

    #[arg(long)]
    num_series: usize,

    /// Target samples/sec across all senders; 0 sends open-loop (saturation).
    #[arg(long)]
    target_rate: f64,

    /// Concurrent senders (capped at --num-series). Each owns a disjoint slice
    /// of the series and keeps one request in flight (preserves per-series
    /// order). The engine handles a request mostly serially, so this must be
    /// at least the engine's core count to saturate it, even if that makes
    /// requests smaller at low cardinality.
    #[arg(long)]
    concurrency: usize,

    /// Max samples per remote-write request (Prometheus' default
    /// `max_samples_per_send` is 2000). Smaller when a sender's slice is.
    #[arg(long)]
    samples_per_request: usize,

    /// Event-time advance per pass (the simulated scrape interval).
    #[arg(long)]
    step_ms: i64,

    #[arg(long)]
    warmup_secs: u64,

    #[arg(long)]
    measure_secs: u64,
}

type Body = (u64, u64, Vec<u8>); // (pass, samples, snappy-compressed body)

fn make_series(i: usize) -> TimeSeries {
    let mut labels = vec![Label {
        name: "__name__".into(),
        value: METRIC_NAME.into(),
    }];
    let mut rest = i;
    for l in 0..NUM_LABELS {
        let digit = if l + 1 == NUM_LABELS {
            rest
        } else {
            rest % LABEL_RADIX
        };
        rest /= LABEL_RADIX;
        labels.push(Label {
            name: format!("l{l}"),
            value: format!("v{digit}"),
        });
    }
    TimeSeries {
        labels,
        samples: vec![Sample::default()],
    }
}

/// splitmix64; seeded per encoder so senders don't emit identical values.
fn next_value(state: &mut u64) -> f64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    (z >> 11) as f64 / (1u64 << 53) as f64 * VALUE_RANGE
}

/// Builds request bodies pass after pass until the sender hangs up. Labels are
/// built once; each pass only rewrites the sample.
fn encode_loop(
    series: Range<usize>,
    samples_per_request: usize,
    start_ms: i64,
    step_ms: i64,
    tx: mpsc::Sender<Body>,
) {
    let ids: Vec<usize> = series.clone().collect();
    let mut requests: Vec<WriteRequest> = ids
        .chunks(samples_per_request)
        .map(|chunk| WriteRequest {
            timeseries: chunk.iter().map(|&i| make_series(i)).collect(),
        })
        .collect();
    let mut rng = series.start as u64;
    let mut encoder = snap::raw::Encoder::new();
    for pass in 0u64.. {
        let timestamp = start_ms + pass as i64 * step_ms;
        for request in &mut requests {
            for ts in &mut request.timeseries {
                ts.samples[0] = Sample {
                    value: next_value(&mut rng),
                    timestamp,
                };
            }
            let body = encoder
                .compress_vec(&request.encode_to_vec())
                .expect("snappy compression should succeed");
            if tx
                .blocking_send((pass, request.timeseries.len() as u64, body))
                .is_err()
            {
                return;
            }
        }
    }
}

struct Shared {
    client: reqwest::Client,
    url: String,
    /// Pass each sender is currently on, for the skew bound.
    passes: Vec<AtomicU64>,
    acked_samples: AtomicU64,
    stop: AtomicBool,
}

async fn send_loop(
    shared: Arc<Shared>,
    idx: usize,
    rate: f64,
    mut rx: mpsc::Receiver<Body>,
) -> Result<(), String> {
    let start = Instant::now();
    let mut sent_samples = 0u64;
    while let Some((pass, samples, body)) = rx.recv().await {
        shared.passes[idx].store(pass, Ordering::Relaxed);
        loop {
            if shared.stop.load(Ordering::Relaxed) {
                return Ok(());
            }
            let slowest = shared
                .passes
                .iter()
                .map(|p| p.load(Ordering::Relaxed))
                .min()
                .expect("concurrency >= 1");
            if pass <= slowest + MAX_PASS_SKEW {
                break;
            }
            tokio::time::sleep(SKEW_POLL_INTERVAL).await;
        }
        if rate > 0.0 {
            let due = start + Duration::from_secs_f64(sent_samples as f64 / rate);
            tokio::time::sleep_until(due.into()).await;
        }
        let response = shared
            .client
            .post(&shared.url)
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .body(body)
            .send()
            .await
            .map_err(|e| format!("sender {idx}: request failed: {e}"))?;
        if response.status() != reqwest::StatusCode::NO_CONTENT {
            return Err(format!("sender {idx}: HTTP {}", response.status()));
        }
        sent_samples += samples;
        shared.acked_samples.fetch_add(samples, Ordering::Relaxed);
    }
    Ok(())
}

fn clock_ticks_per_sec() -> u64 {
    let out = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .expect("getconf CLK_TCK should run");
    String::from_utf8(out.stdout)
        .expect("getconf output is ASCII")
        .trim()
        .parse()
        .expect("CLK_TCK is an integer")
}

/// utime + stime of the whole process, in clock ticks.
fn process_cpu_ticks(pid: u32) -> u64 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .unwrap_or_else(|e| panic!("pid {pid} not readable: {e}"));
    // Fields after the parenthesised comm; utime/stime are fields 14/15.
    let fields: Vec<&str> = stat[stat.rfind(')').expect("stat has comm") + 2..]
        .split_whitespace()
        .collect();
    let field = |i: usize| -> u64 { fields[i].parse().expect("stat field is an integer") };
    field(11) + field(12)
}

fn engine_peak_rss_mb(pid: u32) -> f64 {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .unwrap_or_else(|e| panic!("engine pid {pid} not readable: {e}"));
    let kb: f64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))
        .expect("status has VmHWM")
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse()
        .expect("VmHWM is a number");
    kb / 1024.0
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    assert!(args.num_series > 0 && args.concurrency > 0 && args.samples_per_request > 0);
    let concurrency = args.concurrency.min(args.num_series);
    let ticks_per_sec = clock_ticks_per_sec();
    let start_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64;

    let shared = Arc::new(Shared {
        client: reqwest::Client::new(),
        url: format!("http://127.0.0.1:{}/api/v1/write", args.port),
        passes: (0..concurrency).map(|_| AtomicU64::new(0)).collect(),
        acked_samples: AtomicU64::new(0),
        stop: AtomicBool::new(false),
    });

    let mut senders = Vec::with_capacity(concurrency);
    for idx in 0..concurrency {
        let series = idx * args.num_series / concurrency..(idx + 1) * args.num_series / concurrency;
        let rate = args.target_rate * series.len() as f64 / args.num_series as f64;
        let (tx, rx) = mpsc::channel(ENCODE_AHEAD);
        let step_ms = args.step_ms;
        let samples_per_request = args.samples_per_request;
        std::thread::spawn(move || encode_loop(series, samples_per_request, start_ms, step_ms, tx));
        senders.push(tokio::spawn(send_loop(shared.clone(), idx, rate, rx)));
    }

    tokio::time::sleep(Duration::from_secs(args.warmup_secs)).await;
    let acked_0 = shared.acked_samples.load(Ordering::Relaxed);
    let ticks_0 = process_cpu_ticks(args.engine_pid);
    let sender_ticks_0 = process_cpu_ticks(std::process::id());
    let t0 = Instant::now();
    assert!(acked_0 > 0, "engine acked no samples during warmup");

    tokio::time::sleep(Duration::from_secs(args.measure_secs)).await;
    let acked_1 = shared.acked_samples.load(Ordering::Relaxed);
    let ticks_1 = process_cpu_ticks(args.engine_pid);
    let sender_ticks_1 = process_cpu_ticks(std::process::id());
    let elapsed = t0.elapsed().as_secs_f64();
    let cores = |ticks: u64| ticks as f64 / ticks_per_sec as f64 / elapsed;

    shared.stop.store(true, Ordering::Relaxed);
    for sender in senders {
        if let Err(e) = sender.await.expect("sender task panicked") {
            panic!("{e}");
        }
    }

    println!(
        "{}",
        serde_json::json!({
            "num_series": args.num_series,
            "concurrency": concurrency,
            "target_rate": args.target_rate,
            "achieved_rate": (acked_1 - acked_0) as f64 / elapsed,
            "engine_cpu_cores": cores(ticks_1 - ticks_0),
            // Near the sender's core count means the sender, not the engine,
            // capped an open-loop run.
            "sender_cpu_cores": cores(sender_ticks_1 - sender_ticks_0),
            "engine_peak_rss_mb": engine_peak_rss_mb(args.engine_pid),
            "measure_secs": elapsed,
        })
    );
    // Encoder threads may be parked on a full channel; don't wait for them.
    std::process::exit(0);
}
