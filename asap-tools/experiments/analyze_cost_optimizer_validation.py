#!/usr/bin/env python3
"""Reduce the retained KLL cost-optimizer E2E experiment outputs.

This is intentionally a result reducer, not an accuracy oracle.  It compares
SketchDB and baseline values only where their independently timed replays have
the same `(repetition_idx, result_labels)` key.  KLL rank-error feasibility is
reported from sketch-bench's atomic-cost profile, not inferred here.
"""

import argparse
import gzip
import json
import math
import statistics
from pathlib import Path


def read_jsonl_gz(path):
    with gzip.open(path, "rt") as source:
        return [json.loads(line) for line in source]


def percentile(values, fraction):
    values = sorted(values)
    return values[math.ceil(fraction * len(values)) - 1]


def client_summary(output_dir):
    client_dir = output_dir / "prometheus_client_output"
    latencies = [row["latency"] for row in read_jsonl_gz(client_dir / "query_latencies.jsonl.gz")]
    results = read_jsonl_gz(client_dir / "query_results.jsonl.gz")
    return {
        "latency_seconds": {
            "count": len(latencies),
            "mean": statistics.mean(latencies),
            "median": statistics.median(latencies),
            "min": min(latencies),
            "max": max(latencies),
        },
        "result_rows_by_repetition": {
            str(repetition): sum(
                row["repetition_idx"] == repetition for row in results
            )
            for repetition in sorted({row["repetition_idx"] for row in results})
        },
        "results": results,
    }


def result_map(rows):
    return {
        (row["repetition_idx"], row["result_labels"]): float(row["result_value"])
        for row in rows
    }


def matched_value_summary(sketch_rows, baseline_rows):
    sketch = result_map(sketch_rows)
    baseline = result_map(baseline_rows)
    common = sketch.keys() & baseline.keys()
    absolute = [abs(sketch[key] - baseline[key]) for key in common]
    relative = [
        abs(sketch[key] - baseline[key]) / abs(baseline[key])
        for key in common
        if baseline[key] != 0
    ]
    return {
        "matched_rows": len(common),
        "sketchdb_only_rows": len(sketch.keys() - baseline.keys()),
        "baseline_only_rows": len(baseline.keys() - sketch.keys()),
        "absolute_value_difference": {
            "p50": percentile(absolute, 0.50),
            "p95": percentile(absolute, 0.95),
            "max": max(absolute),
        },
        "note": "Value differences are a same-key replay smoke check, not KLL rank error.",
    }


def query_engine_summary(output_dir):
    monitor = json.loads(
        (output_dir / "remote_monitor_output" / "monitor_output.json").read_text()
    )
    engines = [entry for entry in monitor.values() if entry.get("keyword") == "sketchdb-queryengine-rust"]
    if len(engines) != 1:
        raise RuntimeError(f"expected exactly one SketchDB query engine, found {len(engines)}")
    engine = engines[0]
    memory = engine["memory_info"]
    cpu = engine["cpu_percent"]
    return {
        "samples": len(memory),
        "peak_rss_bytes": max(memory),
        "mean_cpu_percent": statistics.mean(cpu),
        "max_cpu_percent": max(cpu),
    }


def summarize(run_dir):
    sketchdb = client_summary(run_dir / "sketchdb")
    baseline = client_summary(run_dir / "baseline")
    return {
        "run": run_dir.name,
        "sketchdb": {
            "client": {key: value for key, value in sketchdb.items() if key != "results"},
            "query_engine": query_engine_summary(run_dir / "sketchdb"),
        },
        "baseline": {
            "client": {key: value for key, value in baseline.items() if key != "results"},
        },
        "matched_value_check": matched_value_summary(sketchdb["results"], baseline["results"]),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run", type=Path, nargs="+", help="one or more E2E output directories")
    parser.add_argument("--output", type=Path, help="write JSON summary to this path")
    args = parser.parse_args()
    document = {"runs": [summarize(run) for run in args.run]}
    rendered = json.dumps(document, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
