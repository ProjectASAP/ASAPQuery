#!/usr/bin/env python3
"""run_asap.py — queries ASAPQuery engine for each entry in promql_suite.json.

Runs each query 3 times to gather latency samples, then writes results to
benchmarks/reports/asap_results.json.

Usage:
    python benchmarks/scripts/run_asap.py \
        [--asap-url URL] \
        [--output FILE]
"""

import argparse
import json
import os
import time
import urllib.parse
from datetime import datetime, timezone

import requests

SUITE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "queries", "promql_suite.json"
)
DEFAULT_OUTPUT = os.path.join(
    os.path.dirname(__file__), "..", "reports", "asap_results.json"
)
RUNS_PER_QUERY = 3


def query_asap(base_url: str, expr: str, ts: float) -> tuple[dict, float]:
    """Issue a single instant query to the ASAP query engine, return (parsed_json, latency_ms)."""
    encoded = urllib.parse.quote(expr, safe="")
    url = f"{base_url}/api/v1/query?query={encoded}&time={ts}"
    t0 = time.monotonic()
    resp = requests.get(url, timeout=30)
    latency_ms = (time.monotonic() - t0) * 1000.0
    resp.raise_for_status()
    return resp.json(), latency_ms


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ASAP query engine benchmark queries")
    parser.add_argument(
        "--asap-url",
        default="http://localhost:8088",
        help="ASAP query engine base URL (default: http://localhost:8088)",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output JSON file path",
    )
    args = parser.parse_args()

    with open(SUITE_PATH) as f:
        suite = json.load(f)

    results: dict[str, dict] = {}
    now = time.time()

    for q in suite["queries"]:
        qid = q["id"]
        expr = q["expr"]
        asap_native = q.get("asap_native", False)
        latencies = []
        last_data = []
        last_error = None
        last_status = "success"

        print(f"[asap] Running query '{qid}' (native={asap_native}): {expr}")
        for run in range(1, RUNS_PER_QUERY + 1):
            try:
                payload, lat = query_asap(args.asap_url, expr, now)
                latencies.append(lat)
                if payload.get("status") == "success":
                    last_data = payload.get("data", {}).get("result", [])
                    last_status = "success"
                    last_error = None
                else:
                    last_status = "error"
                    last_error = payload.get("error", "unknown error")
                    last_data = []
                print(f"  run {run}/{RUNS_PER_QUERY}: {lat:.1f} ms  status={last_status}")
            except Exception as exc:  # noqa: BLE001
                last_status = "error"
                last_error = str(exc)
                last_data = []
                latencies.append(None)
                print(f"  run {run}/{RUNS_PER_QUERY}: ERROR — {exc}")

        results[qid] = {
            "status": last_status,
            "asap_native": asap_native,
            "latencies_ms": latencies,
            "data": last_data,
            "error": last_error,
        }

    output = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "asap_url": args.asap_url,
        "results": results,
    }

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n[asap] Results saved to {args.output}")


if __name__ == "__main__":
    main()
