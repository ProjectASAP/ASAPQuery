"""Run sketch-bench's recommended sketch configs end to end in ASAPQuery.

`generate` reads sketch-bench's recommendations.csv (smallest config per
dataset, query, range and sketch family that meets the query's accuracy
target, computed from the dataset-analysis skew summary) and writes two
experiment_type configs per query: one with the recommended config as the
planner's global `sketch_parameters` override and a `default` twin that keeps
config.yaml's sketch parameters.

`summarize` reads finished experiments and prints, per query and config, the
measured error against Prometheus (ARE over the 100 largest keys for key
queries, the metric sketch-bench's CMS estimate uses, with ARE over all keys as
a second column; rank error against the replayed trace values for the p99
query) next to the predicted error, plus query latencies.

Usage (from asap-tools/experiments):
  python recommended_sketch_configs/recommended_sketch_configs.py generate \
      --recommendations .../recommendations.csv
  python recommended_sketch_configs/recommended_sketch_configs.py summarize \
      --recommendations .../recommendations.csv --experiments-dir <dir>
"""

import argparse
import csv
import gzip
import json
import os
import re
import sys
from typing import Dict, List, Optional, Tuple

import numpy as np
import yaml

EXPERIMENTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATASET_ANALYSIS_DIR = os.path.join(
    os.path.dirname(EXPERIMENTS_DIR), "dataset-analysis"
)
DEFAULT_OUTPUT_DIR = os.path.join(
    EXPERIMENTS_DIR, "config", "experiment_type", "recommended_sketch_configs"
)

# Queries run by default: (dataset, query_id, range).
DEFAULT_QUERIES = [
    ("google_2011", "cpu_by_job_id", "instant"),
    ("google_2011", "cpu_by_job_id", "5m"),
    ("google_2011", "cpu_p99", "instant"),
    ("alibaba_v2022", "ms_cpu_by_msname", "instant"),
    ("alibaba_v2022", "ms_cpu_by_msname", "5m"),
]

# dataset-analysis (dataset, table) -> cluster_data_exporter config, metric
# names per value column and label names per label column. Tables without an
# exporter (Alibaba MSRTMCR and CallGraph) are absent.
EXPORTERS: Dict[Tuple[str, str], dict] = {
    ("google_2011", "task_usage"): {
        "exporter": {
            "provider": "google",
            "port": 40000,
            "metrics": "mean-cpu-usage-rate",
            "parts_mode": "part-index",
            "part_index": 0,
            "scrape_timeout": "1s",
        },
        "data_subdir": "google",
        # The exporter splits each column by the trace's aggregation_type; 0
        # holds almost every row.
        "metrics": {
            "cpu_rate": "google_mean_cpu_usage_rate_0",
        },
        "labels": {
            "job_id": "job_id",
            "task_index": "task_index",
            "machine_id": "machine_id",
        },
    },
    ("alibaba_v2022", "MSMetrics"): {
        "exporter": {
            "provider": "alibaba",
            "port": 40000,
            "data_type": "msresource",
            "data_year": 2022,
            "parts_mode": "part-index",
            "part_index": 0,
            "scrape_timeout": "10s",
        },
        # One scrape holds about 470k series (117 MB) and takes about 5 s.
        "scrape_interval": "10s",
        "data_subdir": "alibaba_msmetrics",
        "metrics": {
            "cpu_utilization": "alibaba_microservice_cpu_usage",
            "memory_utilization": "alibaba_microservice_memory_usage",
        },
        "labels": {
            "msname": "ms_name",
            "msinstanceid": "ms_instance_id",
            "nodeid": "node_id",
        },
    },
    ("alibaba_v2022", "NodeMetrics"): {
        "exporter": {
            "provider": "alibaba",
            "port": 40000,
            "data_type": "node",
            "data_year": 2022,
            "parts_mode": "part-index",
            "part_index": 0,
        },
        "data_subdir": "alibaba_nodemetrics",
        "metrics": {
            "cpu_utilization": "alibaba_node_cpu_usage",
            "memory_utilization": "alibaba_node_memory_usage",
        },
        "labels": {"nodeid": "node_id"},
    },
}

# Planner sketch_parameters key per sketch-bench family. CountSketch and
# DDSketch have no planner equivalent; the CMS-heap top-k family only applies
# to topk queries, which the dataset-analysis query sets do not contain.
PLANNER_FAMILIES = {"cms": "CountMinSketch", "kll": "DatasketchesKLL"}

# Query client timing. A range query needs one full range of data before its
# first answer, so its starting delay grows with the range.
REPETITIONS = 20
REPETITION_DELAY_MS = 5000
MIN_STARTING_DELAY_S = 90

# remote_monitor.py keyword of the containerized query engine.
QUERY_ENGINE_MONITOR_KEYWORD = "sketchdb-queryengine-rust"

# The only quantile query is cpu_p99. A run replays Google at 1/10 speed, so by
# its queries the exporter has exported the rows starting by 615 s (the
# 5-minute window starting at 600 s).
P99 = 0.99
P99_REPLAY_CUTOFF_US = 615_000_000


def parse_duration_s(text: str) -> int:
    """'5m' -> 300."""
    match = re.fullmatch(r"(\d+)([smh])", text)
    if match is None:
        raise ValueError(f"Unsupported duration: {text}")
    return int(match.group(1)) * {"s": 1, "m": 60, "h": 3600}[match.group(2)]


def planner_sketch_parameters(family: str, config: str) -> dict:
    """recommendations.csv family and config -> planner sketch_parameters.

    'rows=3 cols=4096' -> {'CountMinSketch': {'depth': 3, 'width': 4096}};
    'k=200' -> {'DatasketchesKLL': {'K': 200}}.
    """
    fields = dict(item.split("=") for item in config.split())
    if family == "cms":
        return {
            "CountMinSketch": {
                "depth": int(fields["rows"]),
                "width": int(fields["cols"]),
            }
        }
    if family == "kll":
        return {"DatasketchesKLL": {"K": int(fields["k"])}}
    raise ValueError(f"No planner sketch for family {family}")


def translate_promql(promql: str, exporter: dict) -> str:
    """Rename dataset-analysis metric and label names to the exporter's."""
    names = {**exporter["metrics"], **exporter["labels"]}
    pattern = r"\b(" + "|".join(re.escape(name) for name in names) + r")\b"
    return re.sub(pattern, lambda m: names[m.group(1)], promql)


def load_queries(dataset: str) -> Dict[str, dict]:
    path = os.path.join(DATASET_ANALYSIS_DIR, "queries", f"{dataset}.yaml")
    with open(path) as f:
        spec = yaml.safe_load(f)
    # Some query ids appear twice (key and value forms); keep the first.
    queries: Dict[str, dict] = {}
    for query in spec["queries"]:
        queries.setdefault(query["id"], query)
    return queries


def load_recommendations(path: str) -> List[dict]:
    with open(path) as f:
        return list(csv.DictReader(f))


def experiment_name(dataset: str, query_id: str, range_: str, variant: str) -> str:
    return f"{dataset}_{query_id}_{range_}_{variant}"


def build_experiment_config(
    query_spec: dict,
    range_: str,
    exporter: dict,
    cluster_data_root: str,
    sketch_parameters: Optional[dict],
) -> dict:
    """One experiment_type config (package _global_) for one query."""
    if range_ == "instant":
        promql = query_spec["promql"]
        starting_delay = MIN_STARTING_DELAY_S
    else:
        promql = query_spec["promql_range"].format(range=range_)
        starting_delay = MIN_STARTING_DELAY_S + parse_duration_s(range_)
    # The planner needs queries no more often than the scrape interval.
    repetition_delay_ms = REPETITION_DELAY_MS
    if "scrape_interval" in exporter:
        scrape_interval_ms = 1000 * parse_duration_s(exporter["scrape_interval"])
        repetition_delay_ms = max(repetition_delay_ms, scrape_interval_ms)
    query = translate_promql(promql, exporter)
    labels = ["instance", "job"] + list(exporter["labels"].values())
    metric = exporter["metrics"][query_spec["value"]]

    experiment_params = {
        # One mode that queries both servers, so each ASAP answer has a
        # Prometheus answer for the same timestamp.
        "experiment": [
            {"mode": "sketchdb", "server": "sketchdb", "query_prometheus_too": True}
        ],
        "monitoring": {"tool": "prometheus", "deployment_mode": "bare_metal"},
        "servers": [
            {"name": "prometheus", "url": "http://localhost:9090"},
            {"name": "sketchdb", "url": "http://localhost:8088"},
        ],
        "exporters": {
            "only_start_if_queries_exist": True,
            "exporter_list": {"cluster_data_exporter": dict(exporter["exporter"])},
        },
        "query_groups": [
            {
                "id": 1,
                "queries": [query],
                "repetition_delay_ms": repetition_delay_ms,
                "client_options": {
                    "repetitions": REPETITIONS,
                    "query_time_offset": 10,
                    "starting_delay": starting_delay,
                },
                "controller_options": {"accuracy_sla": 0.99},
            }
        ],
        "metrics": [
            {
                "metric": metric,
                "labels": labels,
                "exporter": "cluster_data_exporter",
            }
        ],
    }
    config: dict = {
        "experiment_params": experiment_params,
        "cluster_data_directory": os.path.join(
            cluster_data_root, exporter["data_subdir"]
        ),
    }
    if "scrape_interval" in exporter:
        config["prometheus"] = {"scrape_interval": exporter["scrape_interval"]}
    if sketch_parameters is not None:
        config["sketch_parameters"] = sketch_parameters
    return config


def generate(
    recommendations: List[dict],
    queries: List[Tuple[str, str, str]],
    cluster_data_root: str,
) -> Tuple[Dict[str, dict], List[str]]:
    """Return (experiment name -> config, skipped notes)."""
    configs: Dict[str, dict] = {}
    skipped: List[str] = []
    for dataset, query_id, range_ in queries:
        query_spec = load_queries(dataset)[query_id]
        exporter = EXPORTERS.get((dataset, query_spec["table"]))
        if exporter is None:
            skipped.append(
                f"{dataset}/{query_id}: no exporter for table {query_spec['table']}"
            )
            continue
        rows = [
            r
            for r in recommendations
            if (r["dataset"], r["query_id"], r["range"]) == (dataset, query_id, range_)
        ]
        if not rows:
            skipped.append(f"{dataset}/{query_id}/{range_}: no recommendation")
            continue
        # Experiment names carry no family, and sketch-bench emits one row per
        # family per query kind, so a query id with both a keys and a values
        # form has a CMS and a KLL row; the second would replace the first.
        planner_rows = [r for r in rows if r["family"] in PLANNER_FAMILIES]
        if len(planner_rows) > 1:
            families = ", ".join(r["family"] for r in planner_rows)
            raise ValueError(
                f"{dataset}/{query_id}/{range_}: more than one planner family "
                f"({families})"
            )
        for row in rows:
            if row["family"] not in PLANNER_FAMILIES:
                skipped.append(
                    f"{dataset}/{query_id}/{range_}: {row['family']} "
                    f"{row['config']} (no planner sketch for this family)"
                )
                continue
            if row["meets_target"] != "True":
                skipped.append(
                    f"{dataset}/{query_id}/{range_}: {row['family']} "
                    f"{row['config']} does not meet the target"
                )
                continue
            name = experiment_name(dataset, query_id, range_, "recommended")
            configs[name] = build_experiment_config(
                query_spec,
                range_,
                exporter,
                cluster_data_root,
                planner_sketch_parameters(row["family"], row["config"]),
            )
            name = experiment_name(dataset, query_id, range_, "default")
            configs[name] = build_experiment_config(
                query_spec, range_, exporter, cluster_data_root, None
            )
    return configs, skipped


def write_configs(configs: Dict[str, dict], output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    for name, config in configs.items():
        with open(os.path.join(output_dir, f"{name}.yaml"), "w") as f:
            f.write("# @package _global_\n")
            f.write("# Generated by recommended_sketch_configs.py; do not edit.\n")
            yaml.safe_dump(config, f, sort_keys=False)


def are_top_keys(exact: Dict, estimate: Dict, num_keys: int = 100) -> float:
    """Mean |estimate - exact| / exact over the num_keys largest exact keys.

    A key missing from the estimate counts as estimate 0.
    """
    keys = sorted(exact, key=lambda k: exact[k], reverse=True)[:num_keys]
    keys = [k for k in keys if exact[k] != 0]
    errors = [abs(estimate.get(k, 0.0) - exact[k]) / abs(exact[k]) for k in keys]
    return float(np.mean(errors)) if errors else float("nan")


def replayed_google_cpu_values(trace: str, cutoff_us: int) -> np.ndarray:
    """Sorted mean CPU usage of the task_usage rows the exporter replayed.

    Rows with start_time <= cutoff_us and aggregation_type 0 (the series the
    p99 query reads).
    """
    values = []
    with gzip.open(trace, "rt") as f:
        for line in f:
            c = line.rstrip("\n").split(",")
            if int(c[0]) <= cutoff_us and c[18] in ("", "0") and c[5] != "":
                values.append(float(c[5]))
    return np.sort(values)


def rank_errors(sorted_values: np.ndarray, estimates: List[float], q: float):
    """|F(estimate) - q| per estimate, F the empirical CDF of sorted_values."""
    ranks = np.searchsorted(sorted_values, estimates, side="right")
    return np.abs(ranks / len(sorted_values) - q)


def monitor_output_path(experiment_dir: str) -> str:
    """Written by remote_monitor.py when the run finishes."""
    return os.path.join(
        experiment_dir, "sketchdb", "remote_monitor_output", "monitor_output.json"
    )


def summarize_experiment(
    experiment_dir: str, quantile_values: Optional[np.ndarray] = None
) -> dict:
    """Measured error and latency of one finished experiment.

    With quantile_values (sorted replayed values), the measured error is the
    p99 rank error instead of the ARE, and there is no all-keys column.
    """
    sys.path.insert(0, EXPERIMENTS_DIR)
    from post_experiment.lib.results_loader import load_results

    results = load_results(
        os.path.join(experiment_dir, "sketchdb", "prometheus_client_output")
    )
    exact = results["prometheus"][0].query_results
    estimate = results["sketchdb"][0].query_results
    errors = []
    errors_all_keys = []
    for exact_rep, estimate_rep in zip(exact, estimate):
        if not (exact_rep.result and estimate_rep.result):
            continue
        if quantile_values is not None:
            (value,) = estimate_rep.result.values()
            errors.append(float(rank_errors(quantile_values, [value], P99)[0]))
            continue
        errors.append(are_top_keys(exact_rep.result, estimate_rep.result))
        errors_all_keys.append(
            are_top_keys(exact_rep.result, estimate_rep.result, len(exact_rep.result))
        )
    latencies = {
        server: [r.latency for r in results[server][0].query_results if r.latency]
        for server in ("prometheus", "sketchdb")
    }
    with open(monitor_output_path(experiment_dir)) as f:
        monitor = json.load(f)
    peak_rss_mb = {
        process["keyword"]: max(process["memory_info"]) / 1e6
        for process in monitor.values()
    }
    return {
        "measured_error": float(np.nanmedian(errors)) if errors else float("nan"),
        "measured_error_all_keys": (
            float(np.nanmedian(errors_all_keys)) if errors_all_keys else ""
        ),
        "answered": f"{len(errors)}/{len(exact)}",
        "asap_latency_ms": 1000 * float(np.median(latencies["sketchdb"])),
        "prom_latency_ms": 1000 * float(np.median(latencies["prometheus"])),
        "asap_peak_rss_mb": peak_rss_mb[QUERY_ENGINE_MONITOR_KEYWORD],
    }


def summarize(
    recommendations: List[dict], experiments_dir: str, google_trace: str
) -> List[dict]:
    """One row per finished recommended or default experiment."""
    rows = []
    quantile_values = None
    for row in recommendations:
        if row["family"] not in PLANNER_FAMILIES:
            continue
        for variant in ("recommended", "default"):
            name = experiment_name(
                row["dataset"], row["query_id"], row["range"], variant
            )
            experiment_dir = os.path.join(experiments_dir, name)
            if not os.path.isdir(experiment_dir):
                continue
            if not os.path.exists(monitor_output_path(experiment_dir)):
                print(f"skipped unfinished {name}", file=sys.stderr)
                continue
            if row["family"] == "kll" and quantile_values is None:
                quantile_values = replayed_google_cpu_values(
                    google_trace, P99_REPLAY_CUTOFF_US
                )
            summary = summarize_experiment(
                experiment_dir, quantile_values if row["family"] == "kll" else None
            )
            recommended = variant == "recommended"
            rows.append(
                {
                    "experiment": name,
                    "family": row["family"],
                    "config": row["config"] if recommended else "default",
                    # Only the recommended config has a prediction.
                    "predicted_error": float(row["est_error"]) if recommended else "",
                    "target": float(row["target"]),
                    **summary,
                    "meets_target": summary["measured_error"] <= float(row["target"]),
                }
            )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    gen = sub.add_parser("generate")
    gen.add_argument("--recommendations", required=True)
    gen.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    gen.add_argument("--cluster-data-root", default="/data/cluster_traces")
    summ = sub.add_parser("summarize")
    summ.add_argument("--recommendations", required=True)
    summ.add_argument("--experiments-dir", required=True)
    summ.add_argument("--output-csv")
    summ.add_argument(
        "--google-trace",
        default="/data/cluster_traces/google/part-00000-of-00500.csv.gz",
        help="task_usage part the p99 runs replayed, for their rank error",
    )
    args = parser.parse_args()

    recommendations = load_recommendations(args.recommendations)
    if args.command == "generate":
        configs, skipped = generate(
            recommendations, DEFAULT_QUERIES, args.cluster_data_root
        )
        write_configs(configs, args.output_dir)
        for name in configs:
            print(f"wrote {name}")
        for note in skipped:
            print(f"skipped {note}")
        return

    rows = summarize(recommendations, args.experiments_dir, args.google_trace)
    if not rows:
        print("No finished experiments found")
        return
    writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    if args.output_csv:
        with open(args.output_csv, "w") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    main()
