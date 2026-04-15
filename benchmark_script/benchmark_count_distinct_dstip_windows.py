#!/usr/bin/env python3
"""
Benchmark baseline ClickHouse vs ASAP query engine for:
    COUNT(DISTINCT dstip) GROUP BY srcip

Supports:
- Window presets: 1s, 5s, 10s, 1m, 5m
- Query variants: basic, orderby, orderby_limit
"""

from __future__ import annotations

import argparse
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime

BASELINE_URL = os.environ.get("BASELINE_URL", "http://localhost:8123/")
ASAP_URL = os.environ.get("ASAP_URL", "http://localhost:8088/clickhouse/query")
BASELINE_TABLE = os.environ.get("BASELINE_TABLE", "flow_table_streaming")
ASAP_TABLE = os.environ.get("ASAP_TABLE", "netflow_table")
DEFAULT_RUNS = int(os.environ.get("BENCHMARK_RUNS", "10"))
DROP_CACHES = os.environ.get("BENCHMARK_DROP_CACHES", "0") == "1"

WINDOWS = {
    "1s": (11, 10),   # [-11s, -10s] => 1 second span
    "5s": (15, 10),   # [-15s, -10s] => 5 second span
    "10s": (20, 10),  # [-20s, -10s] => 10 second span
    "1m": (70, 10),   # [-70s, -10s] => 60 second span
    "5m": (310, 10),  # [-310s, -10s] => 300 second span
}


def drop_caches() -> None:
    if not DROP_CACHES:
        return
    result = subprocess.run(
        "sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'",
        shell=True,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print("warning: drop_caches failed; continuing", file=sys.stderr)
    time.sleep(0.2)


def curl_timed(url: str, query: str) -> float:
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
        raise RuntimeError(f"unexpected curl output: {result.stdout!r}")
    status, elapsed = parts[0], float(parts[1])
    if not status.startswith("2"):
        raise RuntimeError(f"HTTP {status}")
    return elapsed


def build_query(
    table: str,
    query_type: str,
    start_ago_s: int,
    end_ago_s: int,
    fixed_end: str | None,
) -> str:
    if fixed_end:
        where = (
            f"time BETWEEN DATEADD(s, -{start_ago_s}, '{fixed_end}') "
            f"AND DATEADD(s, -{end_ago_s}, '{fixed_end}')"
        )
    else:
        where = (
            f"time BETWEEN DATEADD(s, -{start_ago_s}, NOW()) "
            f"AND DATEADD(s, -{end_ago_s}, NOW())"
        )

    base = (
        "SELECT srcip, COUNT(DISTINCT dstip) AS unique_peers\n"
        f"FROM {table}\n"
        f"WHERE {where}\n"
        "GROUP BY srcip\n"
    )

    if query_type == "basic":
        suffix = "FORMAT TabSeparated"
    elif query_type == "orderby":
        suffix = "ORDER BY unique_peers DESC\nFORMAT TabSeparated"
    elif query_type == "orderby_limit":
        suffix = "ORDER BY unique_peers DESC\nLIMIT 10\nFORMAT TabSeparated"
    else:
        raise ValueError(f"unknown query_type: {query_type}")

    return base + suffix


def print_stats(label: str, values: list[float]) -> None:
    print(
        f"{label:<9} avg={statistics.mean(values):.4f}s  "
        f"med={statistics.median(values):.4f}s  "
        f"min={min(values):.4f}s  max={max(values):.4f}s"
    )


def run_variant(
    window_name: str,
    query_type: str,
    runs: int,
    warmup: int,
    fixed_end: str | None,
) -> None:
    start_ago, end_ago = WINDOWS[window_name]
    baseline_query = build_query(BASELINE_TABLE, query_type, start_ago, end_ago, fixed_end)
    asap_query = build_query(ASAP_TABLE, query_type, start_ago, end_ago, fixed_end)

    print(f"\n=== Window: {window_name} | Variant: {query_type} ===")
    print(f"Baseline: {BASELINE_URL}  table={BASELINE_TABLE}")
    print(f"ASAP:     {ASAP_URL}  table={ASAP_TABLE}")
    print(f"Runs:     {runs}  warmup={warmup}")
    print("-" * 78)

    for i in range(warmup):
        drop_caches()
        _ = curl_timed(BASELINE_URL, baseline_query)
        drop_caches()
        _ = curl_timed(ASAP_URL, asap_query)
        print(f"warmup {i + 1}/{warmup} done")

    baseline_times: list[float] = []
    asap_times: list[float] = []
    for i in range(runs):
        drop_caches()
        baseline_t = curl_timed(BASELINE_URL, baseline_query)
        baseline_times.append(baseline_t)

        drop_caches()
        asap_t = curl_timed(ASAP_URL, asap_query)
        asap_times.append(asap_t)

        print(
            f"run {i + 1:>2}/{runs}: baseline={baseline_t:.4f}s  "
            f"asap={asap_t:.4f}s  speedup={baseline_t / asap_t if asap_t > 0 else 0.0:.2f}x"
        )

    print("-" * 78)
    print_stats("baseline", baseline_times)
    print_stats("asap", asap_times)
    avg_speedup = statistics.mean(baseline_times) / statistics.mean(asap_times)
    med_speedup = statistics.median(baseline_times) / statistics.median(asap_times)
    print(f"speedup  avg={avg_speedup:.2f}x  med={med_speedup:.2f}x")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark COUNT(DISTINCT dstip) query across windows and variants."
    )
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument(
        "--window",
        choices=["all", "1s", "5s", "10s", "1m", "5m"],
        default="all",
        help="Window size preset to benchmark.",
    )
    parser.add_argument(
        "--query-type",
        choices=["all", "basic", "orderby", "orderby_limit"],
        default="all",
        help="Query variant to benchmark.",
    )
    parser.add_argument(
        "--fixed-end",
        metavar="TS",
        help="Use fixed end timestamp, e.g. '2026-04-13 23:50:00'",
    )
    parser.add_argument(
        "--use-live-now",
        action="store_true",
        help="Deprecated no-op: live NOW() is already the default.",
    )
    args = parser.parse_args()

    runs = max(1, args.runs)
    warmup = max(0, args.warmup)
    windows = ["1s", "5s", "10s", "1m", "5m"] if args.window == "all" else [args.window]
    variants = (
        ["basic", "orderby", "orderby_limit"]
        if args.query_type == "all"
        else [args.query_type]
    )

    if args.fixed_end:
        effective_fixed_end = args.fixed_end
    else:
        # Default behavior: live NOW() window (matches existing benchmark workflow).
        effective_fixed_end = None

    print(f"Started:  {datetime.now()}")
    print(f"Windows:  {', '.join(windows)}")
    print(f"Variants: {', '.join(variants)}")
    print(f"FixedEnd: {effective_fixed_end if effective_fixed_end else 'LIVE_NOW'}")
    print(f"Caches:   {'drop each request' if DROP_CACHES else 'normal OS cache'}")

    for window_name in windows:
        for query_type in variants:
            run_variant(window_name, query_type, runs, warmup, effective_fixed_end)

    print(f"Finished: {datetime.now()}")


if __name__ == "__main__":
    main()
