#!/usr/bin/env python3
"""
Benchmark baseline ClickHouse vs ASAP query engine for three SQL variants:

1) Basic:
   SELECT ... GROUP BY srcip
2) Ordered:
   SELECT ... GROUP BY srcip ORDER BY COUNT(pkt_len) DESC
3) Ordered + limited:
   SELECT ... GROUP BY srcip ORDER BY COUNT(pkt_len) DESC LIMIT 10

The 1s window uses a settled slice (-11s..-10s) to better align with 1s tumbling output.
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


def build_query(table: str, fixed_end: str | None, query_type: str) -> str:
    if fixed_end:
        where = (
            f"time BETWEEN DATEADD(s, -11, '{fixed_end}') "
            f"AND DATEADD(s, -10, '{fixed_end}')"
        )
    else:
        where = "time BETWEEN DATEADD(s, -11, NOW()) AND DATEADD(s, -10, NOW())"

    base = (
        "SELECT srcip, COUNT(pkt_len) AS transfer_events\n"
        f"FROM {table}\n"
        f"WHERE {where}\n"
        "GROUP BY srcip\n"
    )

    if query_type == "basic":
        suffix = "FORMAT TabSeparated"
    elif query_type == "orderby":
        suffix = "ORDER BY COUNT(pkt_len) DESC\nFORMAT TabSeparated"
    elif query_type == "orderby_limit":
        suffix = "ORDER BY COUNT(pkt_len) DESC\nLIMIT 10\nFORMAT TabSeparated"
    else:
        raise ValueError(f"unknown query_type: {query_type}")

    return base + suffix


def print_stats(label: str, values: list[float]) -> None:
    print(
        f"{label:<9} avg={statistics.mean(values):.4f}s  "
        f"med={statistics.median(values):.4f}s  "
        f"min={min(values):.4f}s  max={max(values):.4f}s"
    )


def run_single_variant(
    query_type: str,
    runs: int,
    warmup: int,
    fixed_end: str | None,
) -> None:
    baseline_query = build_query(BASELINE_TABLE, fixed_end, query_type)
    asap_query = build_query(ASAP_TABLE, fixed_end, query_type)

    print(f"\n=== Variant: {query_type} ===")
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
        description="Benchmark COUNT(srcip) query for 1s tumbling window."
    )
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    parser.add_argument(
        "--fixed-end",
        metavar="TS",
        help="Use fixed end timestamp, e.g. '2026-04-13 23:50:00'",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=2,
        help="Untimed warmup requests per side before measurements.",
    )
    parser.add_argument(
        "--query-type",
        choices=["all", "basic", "orderby", "orderby_limit"],
        default="all",
        help="Which query variant to benchmark.",
    )
    args = parser.parse_args()
    runs = max(1, args.runs)

    print(f"Started:  {datetime.now()}")
    print(f"Variants: {args.query_type}")
    print(f"Caches:   {'drop each request' if DROP_CACHES else 'normal OS cache'}")

    warmup = max(0, args.warmup)
    variants = (
        ["basic", "orderby", "orderby_limit"]
        if args.query_type == "all"
        else [args.query_type]
    )
    for variant in variants:
        run_single_variant(variant, runs, warmup, args.fixed_end)

    print(f"Finished: {datetime.now()}")


if __name__ == "__main__":
    main()
