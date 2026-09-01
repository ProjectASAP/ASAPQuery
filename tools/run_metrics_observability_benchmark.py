#!/usr/bin/env python3
"""Run the metrics-observability corpus through asap-planner.

All generated files are temporary unless --report is supplied. The report is
summary-only and is intended to be attached to a planner-support PR.
"""

import argparse
import csv
import json
import re
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path

try:
    import yaml
except ImportError:
    raise SystemExit("PyYAML is required: python3 -m pip install pyyaml")


MACROS = re.compile(r"\$(?:__rate_interval|__interval|interval|resolution)")
QUERY_LOG = re.compile(r'query="((?:\\.|[^"\\])*)"')
SELECTOR = re.compile(r"([a-zA-Z_:][a-zA-Z0-9_:]*)\s*\{([^{}]*)\}")
LABEL = re.compile(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:!?=~?)")
FUNCTION = re.compile(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\(")
METRIC_TOKEN = re.compile(r"\b[a-zA-Z_:][a-zA-Z0-9_:]*\b")

AGGREGATIONS = {"sum", "count", "avg", "min", "max", "quantile", "topk", "bottomk"}
TEMPORAL = {
    "rate",
    "irate",
    "increase",
    "delta",
    "idelta",
    "deriv",
    "predict_linear",
    "changes",
    "resets",
    "avg_over_time",
    "min_over_time",
    "max_over_time",
    "sum_over_time",
    "count_over_time",
    "quantile_over_time",
    "last_over_time",
    "present_over_time",
    "mad_over_time",
    "stddev_over_time",
    "stdvar_over_time",
}
RESERVED = {
    "and",
    "or",
    "unless",
    "by",
    "without",
    "on",
    "ignoring",
    "group_left",
    "group_right",
    "bool",
    "offset",
}


def normalize(query):
    return " ".join(MACROS.sub("1m", query.replace("\r", "")).split())


def add(expressions, query, source):
    query = normalize(query.strip())
    if query and query not in expressions:
        expressions[query] = source


def walk(value, source, expressions):
    if isinstance(value, dict):
        for key, child in value.items():
            if key in {"expr", "expression"} and isinstance(child, str):
                add(expressions, child, source)
            walk(child, source, expressions)
    elif isinstance(value, list):
        for child in value:
            walk(child, source, expressions)


def collect(root):
    expressions = {}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = str(path.relative_to(root))
        if path.suffix == ".json":
            try:
                walk(json.loads(path.read_text()), rel, expressions)
            except (OSError, json.JSONDecodeError):
                pass
        elif path.suffix in {".yaml", ".yml"}:
            try:
                walk(yaml.safe_load(path.read_text()), rel, expressions)
            except (OSError, yaml.YAMLError):
                pass
        elif rel.startswith("from-claude/") and path.suffix == ".log":
            for line in path.read_text(errors="replace").splitlines():
                match = QUERY_LOG.search(line)
                if match:
                    add(expressions, json.loads('"' + match.group(1) + '"'), rel)
        elif rel.startswith("cmu_chad/") and path.name in {
            "all_expressions.txt",
            "alert_expressions.txt",
            "recording_expressions.txt",
        }:
            for line in path.read_text(errors="replace").splitlines():
                if line.strip() and not line.lstrip().startswith("#"):
                    add(expressions, line, rel)
        elif rel.startswith("kaggle_promeset/") and path.suffix == ".csv":
            with path.open(newline="") as handle:
                for row in csv.DictReader(handle):
                    add(expressions, row.get("PromQL", ""), rel)
    return expressions


def eligible(query):
    """Keep queries with an aggregation function or temporal aggregation."""
    functions = set(FUNCTION.findall(query))
    return bool(functions & (AGGREGATIONS | TEMPORAL))


def runnable_by_current_planner(query):
    """Exclude known inputs that can terminate the current batch planner."""
    return (
        "@ end" not in query
        and "@end" not in query
        and not re.search(r"(?:^|[ (])\{", query)
    )


def make_manifest(expressions, path):
    queries = [
        {"id": index, "query": query, "source": source, "eligible": eligible(query)}
        for index, (query, source) in enumerate(sorted(expressions.items()))
    ]
    path.write_text(json.dumps({"queries": queries}, indent=2) + "\n")
    return queries


def make_workload(queries, path, scrape_ms, repeat_ms):
    labels = defaultdict(set)
    for entry in queries:
        functions = set(FUNCTION.findall(entry["query"]))
        for token in METRIC_TOKEN.findall(entry["query"]):
            if token not in functions and token not in RESERVED:
                labels.setdefault(token, set())
        for metric, body in SELECTOR.findall(entry["query"]):
            labels[metric].update(LABEL.findall(body))
    schema = [
        {"metric": metric, "labels": sorted(names)}
        for metric, names in sorted(labels.items())
    ]
    workload = {
        "metrics": schema,
        "query_groups": [
            {
                "id": 1,
                "repetition_delay_ms": repeat_ms,
                "controller_options": {"accuracy_sla": 0.95, "latency_sla": 100.0},
                "queries": [
                    entry["query"]
                    for entry in queries
                    if entry["eligible"] and runnable_by_current_planner(entry["query"])
                ],
            }
        ],
        "aggregate_cleanup": {"policy": "read_based"},
    }
    path.write_text(yaml.safe_dump(workload, sort_keys=False, width=200))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queries-dir", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--scrape-interval-ms", type=int, default=1000)
    parser.add_argument("--repetition-interval-ms", type=int, default=1000)
    parser.add_argument("--top", type=int, default=3)
    args = parser.parse_args()
    root = args.queries_dir or find_default_queries_dir()
    if not root.is_dir():
        raise SystemExit(f"benchmark directory not found: {root}")

    repo = Path(__file__).resolve().parents[1]
    binary_dir = repo / "target/release"
    subprocess.run(
        [
            "cargo",
            "build",
            "--release",
            "-p",
            "asap_planner",
            "--bin",
            "asap-planner",
            "--bin",
            "benchmark-promql-status",
        ],
        cwd=repo,
        check=True,
    )
    with tempfile.TemporaryDirectory(prefix="asap-metrics-observability-") as temp:
        temp = Path(temp)
        expressions = collect(root)
        queries = make_manifest(expressions, temp / "manifest.json")
        make_workload(
            queries,
            temp / "workload.yaml",
            args.scrape_interval_ms,
            args.repetition_interval_ms,
        )
        status_path = temp / "status.json"
        subprocess.run(
            [
                str(binary_dir / "benchmark-promql-status"),
                "--manifest",
                str(temp / "manifest.json"),
                "--output",
                str(status_path),
            ],
            check=True,
        )
        statuses = {item["id"]: item for item in json.loads(status_path.read_text())}
        eligible_queries = [entry for entry in queries if entry["eligible"]]
        parsed_eligible = [
            entry for entry in eligible_queries if statuses[entry["id"]]["parsed"]
        ]
        planner_queries = [
            entry
            for entry in parsed_eligible
            if runnable_by_current_planner(entry["query"])
        ]
        make_workload(
            planner_queries,
            temp / "workload.yaml",
            args.scrape_interval_ms,
            args.repetition_interval_ms,
        )
        output_dir = temp / "planner-output"
        log_path = temp / "planner.log"
        with log_path.open("w") as log:
            subprocess.run(
                [
                    str(binary_dir / "asap-planner"),
                    "--input_config",
                    str(temp / "workload.yaml"),
                    "--output_dir",
                    str(output_dir),
                    "--data-ingestion-interval-ms",
                    str(args.scrape_interval_ms),
                    "--streaming_engine",
                    "precompute",
                    "-v",
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
            )
        inference = (
            yaml.safe_load((output_dir / "inference_config.yaml").read_text()) or {}
        )
        planned = {normalize(item["query"]) for item in inference.get("queries", [])}
        planned_queries = [
            entry for entry in planner_queries if normalize(entry["query"]) in planned
        ]
        missing = [
            entry
            for entry in planner_queries
            if normalize(entry["query"]) not in planned
        ]
        report = make_report(
            queries,
            eligible_queries,
            parsed_eligible,
            planned_queries,
            missing,
            args.top,
        )
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(report)
        else:
            print(report, end="")


def make_report(all_queries, eligible_queries, parsed, planned, missing, top):
    clusters = defaultdict(list)
    for entry in missing:
        functions = ",".join(sorted(set(FUNCTION.findall(entry["query"])))) or "none"
        aggregators = (
            ",".join(sorted(set(FUNCTION.findall(entry["query"])) & AGGREGATIONS))
            or "none"
        )
        shape = re.sub(r"[a-zA-Z_:][a-zA-Z0-9_:]*", "METRIC", entry["query"])
        shape = re.sub(r"\s+", " ", shape).strip()
        clusters[(shape, functions, aggregators)].append(entry)
    ranked = sorted(
        clusters.values(), key=lambda group: (-len(group), group[0]["query"])
    )[:top]
    lines = [
        "# Metrics observability planner coverage",
        "",
        "| Measure | Count |",
        "| --- | ---: |",
        f"| Extracted unique expressions | {len(all_queries)} |",
        f"| Eligible aggregation/temporal expressions | {len(eligible_queries)} |",
        f"| Parseable eligible expressions | {len(parsed)} |",
        f"| Eligible expressions with an inference entry | {len(planned)} |",
        f"| Eligible parseable expressions without an inference entry | {len(missing)} |",
        "",
        "## Top unplanned structural clusters",
        "",
        "| Rank | Count | Share of misses | Functions | Aggregations | Representative |",
        "| ---: | ---: | ---: | --- | --- | --- |",
    ]
    for index, group in enumerate(ranked, 1):
        functions = ",".join(sorted(set(FUNCTION.findall(group[0]["query"])))) or "none"
        aggregators = (
            ",".join(sorted(set(FUNCTION.findall(group[0]["query"])) & AGGREGATIONS))
            or "none"
        )
        example = group[0]["query"].replace("|", "\\|")
        lines.append(
            f"| {index} | {len(group)} | {len(group) / max(len(missing), 1):.1%} | `{functions}` | `{aggregators}` | `{example}` |"
        )
    return "\n".join(lines) + "\n"


def find_default_queries_dir():
    for ancestor in Path(__file__).resolve().parents:
        candidate = ancestor / "benchmarks/metrics_observability/queries"
        if candidate.is_dir():
            return candidate
    raise SystemExit("benchmark directory not found; pass --queries-dir explicitly")


if __name__ == "__main__":
    main()
