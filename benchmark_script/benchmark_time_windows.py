import os
import subprocess
import statistics
import time
from datetime import datetime

# Use the same time predicate for baseline and ASAP so both read the same rows.
# (Replay uses wall-clock ms; a fixed BASELINE_END in the past makes baseline scan
# almost nothing while ASAP still hits live data — that inverts speedups.)
USE_NOW_FOR_BASELINE = True
# If False, set BASELINE_END to a moment inside your ingested data range (e.g. bulk load).
BASELINE_END = "2026-04-12 17:35:36"

# Time windows to test
TIME_WINDOWS = [
    ("1 min", 60),
    ("5 min", 300),
    ("15 min", 900),
    ("30 min", 1800),
    ("1 hour", 3600),
    ("3 hours", 10800),
    ("6 hours", 21600),
]

RUNS = 5
VERIFY_COUNTS = os.environ.get("BENCHMARK_VERIFY_COUNTS", "1") == "1"


def drop_cache():
    subprocess.run("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'", shell=True)
    time.sleep(0.5)


def run_query(url, query):
    result = subprocess.run(
        [
            "curl",
            "-o",
            "/dev/null",
            "-s",
            "-S",
            "-w",
            "%{http_code} %{time_total}",
            "-G",
            url,
            "--data-urlencode",
            f"query={query}",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed: {result.stderr}")
    parts = result.stdout.strip().split()
    if len(parts) != 2:
        raise RuntimeError(f"bad curl output: {result.stdout!r}")
    code, t = parts[0], float(parts[1])
    if not code.startswith("2"):
        raise RuntimeError(f"HTTP {code} for query (check ClickHouse / QueryEngine logs)")
    return t


def run_scalar(url, query):
    """Return first line of body (e.g. count)."""
    result = subprocess.run(
        ["curl", "-s", "-S", "-G", url, "--data-urlencode", f"query={query}"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr)
    return result.stdout.strip().split("\n")[0]


def time_predicate(window_seconds):
    if USE_NOW_FOR_BASELINE:
        return f"time BETWEEN DATEADD(s, -{window_seconds}, NOW()) AND NOW()"
    return f"time BETWEEN DATEADD(s, -{window_seconds}, '{BASELINE_END}') AND '{BASELINE_END}'"


def baseline_query(window_seconds):
    return f"""SELECT proto, srcip, dstip, quantile(0.99)(pkt_len) AS p99_pkt_len
FROM flow_table_streaming
WHERE {time_predicate(window_seconds)}
GROUP BY proto, srcip, dstip
ORDER BY p99_pkt_len DESC
LIMIT 10"""


def asap_query(window_seconds):
    return f"""SELECT proto, srcip, dstip, quantile(0.99)(pkt_len) AS p99_pkt_len
FROM netflow_table
WHERE {time_predicate(window_seconds)}
GROUP BY proto, srcip, dstip
ORDER BY p99_pkt_len DESC
LIMIT 10"""


def count_sql(window_seconds, table):
    return f"SELECT count() FROM {table} WHERE {time_predicate(window_seconds)}"


print(f"Benchmark started at: {datetime.now()}")
if USE_NOW_FOR_BASELINE:
    print("Time range: NOW() for both baseline and ASAP (same live window).")
else:
    print(f"Time range: fixed end {BASELINE_END} for both (only if data spans this range).")
print(f"Runs per window: {RUNS}")
print("=" * 80)
print(
    f"{'Window':<12} {'B_avg':>8} {'B_med':>8} {'B_min':>8} {'B_max':>8} "
    f"{'A_avg':>8} {'A_med':>8} {'Speedup':>8}"
)
print("=" * 80)

results = []

for label, seconds in TIME_WINDOWS:
    if VERIFY_COUNTS:
        try:
            cb = run_scalar("http://localhost:8123/", count_sql(seconds, "flow_table_streaming"))
            ca = run_scalar("http://localhost:8123/", count_sql(seconds, "netflow_table"))
            print(f"  [{label}] rows in window — baseline table: {cb}, asap table: {ca}")
        except Exception as e:
            print(f"  [{label}] count check failed: {e}")

    baseline_times = []
    asap_times = []

    for i in range(RUNS):
        drop_cache()
        b = run_query("http://localhost:8123/", baseline_query(seconds))
        baseline_times.append(b)

        a = run_query("http://localhost:8088/clickhouse/query", asap_query(seconds))
        asap_times.append(a)
        print(f"  [{label}] run {i+1}: baseline={b:.3f}s asap={a:.3f}s", flush=True)

    b_avg = statistics.mean(baseline_times)
    b_med = statistics.median(baseline_times)
    b_min = min(baseline_times)
    b_max = max(baseline_times)
    a_avg = statistics.mean(asap_times)
    a_med = statistics.median(asap_times)
    speedup = b_avg / a_avg if a_avg > 0 else 0

    print(
        f"\n{label:<12} {b_avg:>7.3f}s {b_med:>7.3f}s {b_min:>7.3f}s {b_max:>7.3f}s "
        f"{a_avg:>7.3f}s {a_med:>7.3f}s {speedup:>7.2f}x\n"
    )

    results.append(
        {
            "window": label,
            "baseline_avg": b_avg,
            "baseline_med": b_med,
            "asap_avg": a_avg,
            "asap_med": a_med,
            "speedup": speedup,
        }
    )

print("=" * 80)
print("\nFinal Summary:")
print(f"{'Window':<12} {'Baseline Avg':>14} {'ASAP Avg':>10} {'Speedup':>8}")
print("-" * 50)
for r in results:
    print(
        f"{r['window']:<12} {r['baseline_avg']:>13.3f}s {r['asap_avg']:>9.3f}s {r['speedup']:>7.2f}x"
    )

print(f"\nBenchmark completed at: {datetime.now()}")
print(
    """
Interpretation hints:
- If row counts stop growing for larger windows (e.g. 1 h == 3 h), you only have that much
  history in MergeTree; widen the '6 hours' story accordingly.
- Short windows: baseline scan is already cheap; QueryEngine + merging many 1s-tumbling
  sketches can lose. Speedup usually appears once raw scan + per-row quantile work dominates.
- Small baseline vs ASAP row deltas are often Kafka consumer lag between the two topics."""
)
