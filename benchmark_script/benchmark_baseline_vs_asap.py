#!/usr/bin/env python3
"""
Compare baseline ClickHouse vs ASAP query engine over increasing time windows.

Default: widening windows (1 s, 5 s, 10 s, then 1 min → … → 6 h). Sub-minute rows use **settled**
ranges (end ~10s ago). For W≥60, windows use a **70s lag** on the upper bound:
DATEADD(s,-(W+70),NOW())..DATEADD(s,-70,NOW()) (same as netflow_inference agg2 templates).

Run the query engine with **--prometheus-scrape-interval 1** (or equivalent) so 1s tumbling netflow
matches the SQL pattern matcher; a larger value can reject short windows and also yield fallback.
Baseline always scans flow_table_streaming on :8123. ASAP (:8088) uses sketches only
if netflow_inference_config.yaml has a matching template per window; otherwise
8088 forwards to ClickHouse (you are timing proxy+CH).

template1s mode: single 1-second slice matching the default quantile template
(DATEADD -11 .. -10).

Environment
-----------
BASELINE_URL, ASAP_URL, BASELINE_TABLE, ASAP_TABLE, BENCHMARK_RUNS,
BENCHMARK_DROP_CACHES (1=run sudo drop_caches between each timed curl)
"""

from __future__ import annotations

import argparse
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime
from typing import List, Tuple

BASELINE_URL = os.environ.get("BASELINE_URL", "http://localhost:8123/")
ASAP_URL = os.environ.get("ASAP_URL", "http://localhost:8088/clickhouse/query")
BASELINE_TABLE = os.environ.get("BASELINE_TABLE", "flow_table_streaming")
ASAP_TABLE = os.environ.get("ASAP_TABLE", "netflow_table")
DEFAULT_RUNS = int(os.environ.get("BENCHMARK_RUNS", "5"))
DROP_CACHES = os.environ.get("BENCHMARK_DROP_CACHES", "1") == "1"

# Seconds before NOW() for the **end** of each W≥60 window (matches netflow_inference_config).
SETTLED_LAG_S = 70

# (label, seconds) — increasing windows
DEFAULT_WINDOWS: List[Tuple[str, int]] = [
    ("1 s", 1),
    ("5 s", 5),
    ("10 s", 10),
    ("1 min", 60),
    ("5 min", 300),
    ("15 min", 900),
    ("30 min", 1800),
    ("1 hour", 3600),
    ("3 hours", 10800),
    ("6 hours", 21600),
]


def drop_caches() -> None:
    if not DROP_CACHES:
        return
    r = subprocess.run(
        "sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'",
        shell=True,
        capture_output=True,
    )
    if r.returncode != 0:
        print("warning: drop_caches failed (run without sudo?); continuing", file=sys.stderr)
    time.sleep(0.3)


def curl_timed(url: str, query: str) -> Tuple[float, str]:
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
        raise RuntimeError(f"HTTP {code} — check ClickHouse / query_engine logs")
    return t, code


def quantile_sql(table: str, where_clause: str, with_order_limit: bool) -> str:
    core = f"""SELECT proto, srcip, dstip, quantile(0.99)(pkt_len) AS p99_pkt_len
FROM {table}
WHERE {where_clause}
GROUP BY proto, srcip, dstip"""
    if with_order_limit:
        return core + "\nORDER BY p99_pkt_len DESC\nLIMIT 10\nFORMAT TabSeparated"
    return core + "\nFORMAT TabSeparated"


def time_predicate(seconds: int, use_now: bool, fixed_end: str | None) -> str:
    # Sub-minute: settled upper bound so 1s tumbling buckets tend to exist in the store.
    if seconds == 1:
        if use_now:
            return "time BETWEEN DATEADD(s, -11, NOW()) AND DATEADD(s, -10, NOW())"
        if not fixed_end:
            raise ValueError("fixed_end required when not using NOW()")
        return (
            f"time BETWEEN DATEADD(s, -11, '{fixed_end}') "
            f"AND DATEADD(s, -10, '{fixed_end}')"
        )
    if seconds == 5:
        if use_now:
            return "time BETWEEN DATEADD(s, -15, NOW()) AND DATEADD(s, -10, NOW())"
        if not fixed_end:
            raise ValueError("fixed_end required when not using NOW()")
        return (
            f"time BETWEEN DATEADD(s, -15, '{fixed_end}') "
            f"AND DATEADD(s, -10, '{fixed_end}')"
        )
    if seconds == 10:
        if use_now:
            return "time BETWEEN DATEADD(s, -25, NOW()) AND DATEADD(s, -15, NOW())"
        if not fixed_end:
            raise ValueError("fixed_end required when not using NOW()")
        return (
            f"time BETWEEN DATEADD(s, -25, '{fixed_end}') "
            f"AND DATEADD(s, -15, '{fixed_end}')"
        )
    if seconds >= 60:
        lag = SETTLED_LAG_S
        if use_now:
            return (
                f"time BETWEEN DATEADD(s, -{seconds + lag}, NOW()) "
                f"AND DATEADD(s, -{lag}, NOW())"
            )
        if not fixed_end:
            raise ValueError("fixed_end required when not using NOW()")
        return (
            f"time BETWEEN DATEADD(s, -{seconds + lag}, '{fixed_end}') "
            f"AND DATEADD(s, -{lag}, '{fixed_end}')"
        )
    if use_now:
        return f"time BETWEEN DATEADD(s, -{seconds}, NOW()) AND NOW()"
    if not fixed_end:
        raise ValueError("fixed_end required when not using NOW()")
    return f"time BETWEEN DATEADD(s, -{seconds}, '{fixed_end}') AND '{fixed_end}'"


def parse_windows_arg(spec: str) -> List[Tuple[str, int]]:
    """e.g. '60,300,900' or '60:1min,300:5min' (label optional)."""
    out: List[Tuple[str, int]] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            sec_s, label = part.split(":", 1)
            out.append((label.strip(), int(sec_s.strip())))
        else:
            s = int(part)
            out.append((f"{s}s", s))
    if not out:
        raise ValueError("empty --windows")
    return out


def run_increasing_windows(
    windows_sec: List[Tuple[str, int]],
    runs: int,
    order_limit: bool,
    use_now: bool,
    fixed_end: str | None,
    verify_counts: bool,
) -> None:
    windows: List[Tuple[str, str]] = [
        (label, time_predicate(sec, use_now, fixed_end)) for label, sec in windows_sec
    ]

    print(f"Started: {datetime.now()}")
    if use_now:
        print(
            f"Time range: W≥60 → DATEADD(s,-(W+{SETTLED_LAG_S}),NOW()).."
            f"DATEADD(s,-{SETTLED_LAG_S},NOW()); "
            "1s/5s/10s use settled sub-minute offsets (see docstring)."
        )
    else:
        print(f"Time range: fixed end '{fixed_end}' for all windows.")
    print(f"Baseline: {BASELINE_URL}  table={BASELINE_TABLE}")
    print(f"ASAP:     {ASAP_URL}  table={ASAP_TABLE}")
    print(f"Runs per (window, side): {runs}")
    print(
        "\nASAP: use --prometheus-scrape-interval 1 with 1s tumbling netflow; else short windows\n"
        "may never match the SQL matcher. Without templates or store data, 8088 falls back —\n"
        "check QUERY ENGINE SUCCESS vs FORWARDING TO CLICKHOUSE in logs.\n"
    )
    print("=" * 88)
    print(
        f"{'Window':<12} {'B_avg':>9} {'B_med':>9} {'B_min':>9} {'B_max':>9} "
        f"{'A_avg':>9} {'A_med':>9} {'B/A':>8}"
    )
    print("=" * 88)

    summary: List[dict] = []

    for label, pred in windows:
        if verify_counts:
            cq = f"SELECT count() FROM {BASELINE_TABLE} WHERE {pred}"
            try:
                r = subprocess.run(
                    ["curl", "-s", "-S", "-G", BASELINE_URL, "--data-urlencode", f"query={cq}"],
                    capture_output=True,
                    text=True,
                )
                cnt = r.stdout.strip().split("\n")[0] if r.returncode == 0 else "?"
                print(f"  [{label}] rows in window (baseline table): {cnt}")
            except Exception as e:
                print(f"  [{label}] count check failed: {e}")

        bt: List[float] = []
        at: List[float] = []
        for i in range(runs):
            qb = quantile_sql(BASELINE_TABLE, pred, order_limit)
            qa = quantile_sql(ASAP_TABLE, pred, order_limit)

            drop_caches()
            tb, _ = curl_timed(BASELINE_URL, qb)
            bt.append(tb)
            drop_caches()
            ta, _ = curl_timed(ASAP_URL, qa)
            at.append(ta)
            print(
                f"  [{label}] run {i + 1}/{runs}: baseline={tb:.4f}s asap={ta:.4f}s",
                flush=True,
            )

        b_avg = statistics.mean(bt)
        b_med = statistics.median(bt)
        a_avg = statistics.mean(at)
        a_med = statistics.median(at)
        ratio = b_avg / a_avg if a_avg > 0 else 0.0
        print(
            f"{label:<12} {b_avg:>8.4f}s {b_med:>8.4f}s {min(bt):>8.4f}s {max(bt):>8.4f}s "
            f"{a_avg:>8.4f}s {a_med:>8.4f}s {ratio:>7.2f}x\n"
        )
        summary.append(
            {
                "label": label,
                "b_avg": b_avg,
                "a_avg": a_avg,
                "speedup": ratio,
            }
        )

    print("=" * 88)
    print("Summary (baseline avg / ASAP avg):")
    for r in summary:
        print(f"  {r['label']:<12}  {r['b_avg']:.4f}s / {r['a_avg']:.4f}s  →  {r['speedup']:.2f}x")
    print(f"\nFinished: {datetime.now()}")


def main() -> None:
    p = argparse.ArgumentParser(
        description="Baseline vs ASAP over increasing time windows (default) or 1s template."
    )
    p.add_argument(
        "mode",
        nargs="?",
        default="windows",
        choices=("windows", "template1s"),
        help="windows=increasing NOW() ranges; template1s=1s slice from inference YAML",
    )
    p.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    p.add_argument(
        "--windows",
        metavar="SPEC",
        help=(
            "Comma-separated seconds for increasing windows, e.g. "
            "'60,300,900,1800,3600'. Optional labels: '60:1min,300:5min'."
        ),
    )
    p.add_argument(
        "--order-limit",
        action="store_true",
        help="ORDER BY p99 DESC LIMIT 10 (CH honors; ASAP local path ignores)",
    )
    p.add_argument(
        "--fixed-end",
        metavar="TS",
        help="Use fixed end timestamp instead of NOW()",
    )
    p.add_argument(
        "--no-verify-counts",
        action="store_true",
        help="Skip SELECT count() per window on baseline table",
    )
    args = p.parse_args()
    runs = max(1, args.runs)
    use_now = args.fixed_end is None
    verify = not args.no_verify_counts

    if args.mode == "template1s":
        if args.windows:
            print("warning: --windows ignored in template1s mode", file=sys.stderr)
        where = "time BETWEEN DATEADD(s, -11, NOW()) AND DATEADD(s, -10, NOW())"
        if not use_now:
            where = (
                f"time BETWEEN DATEADD(s, -11, '{args.fixed_end}') "
                f"AND DATEADD(s, -10, '{args.fixed_end}')"
            )
        print(f"Started: {datetime.now()}")
        print("Mode: template1s (matches default netflow_inference quantile window)")
        print(f"Baseline: {BASELINE_URL}  table={BASELINE_TABLE}")
        print(f"ASAP:     {ASAP_URL}  table={ASAP_TABLE}")
        print(f"Runs per side: {runs}\n")
        bt, at = [], []
        for i in range(runs):
            qb = quantile_sql(BASELINE_TABLE, where, args.order_limit)
            qa = quantile_sql(ASAP_TABLE, where, args.order_limit)
            drop_caches()
            tb, _ = curl_timed(BASELINE_URL, qb)
            bt.append(tb)
            drop_caches()
            ta, _ = curl_timed(ASAP_URL, qa)
            at.append(ta)
            print(f"  run {i + 1}/{runs}: baseline={tb:.4f}s asap={ta:.4f}s", flush=True)
        print(
            f"\n1s slice      {statistics.mean(bt):>8.4f}s {statistics.median(bt):>8.4f}s  "
            f"{statistics.mean(at):>8.4f}s {statistics.median(at):>8.4f}s  "
            f"{statistics.mean(bt) / statistics.mean(at):>7.2f}x\n"
        )
        print(
            "Check query_engine log for QUERY ENGINE SUCCESS vs FORWARDING TO CLICKHOUSE.\n"
            f"Finished: {datetime.now()}"
        )
        return

    if args.windows:
        windows_sec = parse_windows_arg(args.windows)
    else:
        windows_sec = DEFAULT_WINDOWS

    run_increasing_windows(
        windows_sec,
        runs,
        args.order_limit,
        use_now,
        args.fixed_end,
        verify_counts=verify,
    )


if __name__ == "__main__":
    main()
