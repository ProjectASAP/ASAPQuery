"""Write an experiment_run_e2e.py config for a sketch-bench workload table.

The table's `workloads[0].e2e` block lists the end-to-end queries and the
metrics the paced feeder generates. The config has:

- one query group per repeat interval T (`interval_secs`), repeating every T
  with evaluation times aligned to multiples of T (`align_to_interval`). PromQL
  entries keep the table's order, as in `asap-planner --planner saved`.
- structured entries (no `promql`, the multi-grouping templates) as SQL query
  groups, only when `--sql-queries` gives their SQL; otherwise they are
  skipped with a note. The file holds one `;`-terminated statement per
  structured entry, in table order.
- the table's gauge and counter metrics, sent by the paced feeder and listed
  under `metrics`. Records metrics, which only the structured queries read,
  are fed only with `--sql-queries`, and then only to ASAPQuery: the config
  has no baseline (Prometheus) mode, and they are not listed under `metrics`
  (with prebuilt planner configs that list only keeps fed metrics out of
  Prometheus' forwarding, and records are never scraped).
- `controller.prebuilt_output_dir`, when given, so ASAPQuery runs those
  planner configs (e.g. from `asap-planner --planner saved`).

Usage (from asap-tools/experiments):
  python sketch_bench_e2e/sketch_bench_e2e_configs.py \\
      --workload-table <sketch-bench>/.../synthetic-templatesclassic-shared1.json \\
      --seed 42 --starting-delay-secs 120 --query-duration-secs 3600 \\
      [--prebuilt-output-dir DIR] [--sql-queries FILE]
Then:
  python experiment_run_e2e.py experiment_type=sketch_bench_e2e/<table stem> ...
"""

import argparse
import json
import os
import sys
from typing import Dict, List, Optional, Tuple

import yaml

EXPERIMENTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, EXPERIMENTS_DIR)

from experiment_utils.config import (  # noqa: E402
    _load_sql_queries,
    check_prebuilt_controller_output,
)
from experiment_utils.services.paced_feeder import (  # noqa: E402
    PACED_FEEDER_EXPORTER,
    PROMETHEUS_METRIC_TYPES,
    RECORDS_METRIC_TYPE,
)

DEFAULT_OUTPUT_DIR = os.path.join(
    EXPERIMENTS_DIR, "config", "experiment_type", "sketch_bench_e2e"
)
SERVERS = [
    {"name": "prometheus", "url": "http://localhost:9090"},
    {"name": "sketchdb", "url": "http://localhost:8088"},
]
SKETCHDB_MODE = {"mode": "sketchdb", "server": "sketchdb"}
BASELINE_MODE = {"mode": "baseline", "server": "prometheus"}
# Seconds the window ending at an evaluation time gets for its data to arrive.
DEFAULT_QUERY_TIME_OFFSET_SECS = 10
# Feeder time beyond the query phase: covers flow.steady_state_wait (60 s in
# config.yaml) and service startup. The runner checks the total.
FEEDER_SLACK_SECS = 300


def load_e2e(table_path: str) -> dict:
    with open(table_path) as f:
        (workload,) = json.load(f)["workloads"]
    return workload["e2e"]


def by_interval(entries: List[Tuple[int, str]]) -> Dict[int, List[str]]:
    groups: Dict[int, List[str]] = {}
    for interval_secs, query in entries:
        groups.setdefault(interval_secs, []).append(query)
    return dict(sorted(groups.items()))


def build_config(
    table_path: str,
    seed: int,
    starting_delay_secs: int,
    query_duration_secs: int,
    query_time_offset_secs: int = DEFAULT_QUERY_TIME_OFFSET_SECS,
    feeder_duration_secs: Optional[int] = None,
    samples_per_sec: Optional[int] = None,
    prebuilt_output_dir: Optional[str] = None,
    sql_queries_file: Optional[str] = None,
) -> Tuple[dict, List[str]]:
    """Return (config, notes on what was skipped)."""
    e2e = load_e2e(table_path)
    notes: List[str] = []

    promql = [
        (q["interval_secs"], q["promql"]) for q in e2e["queries"] if "promql" in q
    ]
    structured = [q for q in e2e["queries"] if "promql" not in q]
    sql: List[Tuple[int, str]] = []
    if structured and sql_queries_file is None:
        notes.append(
            f"skipped {len(structured)} structured (multi-grouping) queries: "
            "no --sql-queries file"
        )
    elif structured:
        statements = _load_sql_queries(sql_queries_file)
        if len(statements) != len(structured):
            raise ValueError(
                f"{sql_queries_file} has {len(statements)} statements; the table "
                f"has {len(structured)} structured queries"
            )
        sql = [(q["interval_secs"], s) for q, s in zip(structured, statements)]

    query_groups = []
    for queries_by_interval in (by_interval(promql), by_interval(sql)):
        for interval_secs, queries in queries_by_interval.items():
            if query_duration_secs < interval_secs:
                raise ValueError(
                    f"--query-duration-secs {query_duration_secs} is shorter than "
                    f"the {interval_secs} s repeat interval"
                )
            query_groups.append(
                {
                    "id": len(query_groups) + 1,
                    "queries": queries,
                    "repetition_delay_ms": interval_secs * 1000,
                    "client_options": {
                        "repetitions": query_duration_secs // interval_secs,
                        "starting_delay": starting_delay_secs,
                        "query_time_offset": query_time_offset_secs,
                        "align_to_interval": True,
                    },
                }
            )
    if not query_groups:
        raise ValueError(f"{table_path}: no queries to run")

    metrics = []
    records_metrics = []
    for name, desc in e2e["metrics"].items():
        if desc["type"] == RECORDS_METRIC_TYPE:
            if sql:
                records_metrics.append(name)
            else:
                notes.append(
                    f"skipped records metric {name!r}: only structured queries read it"
                )
            continue
        if desc["type"] not in PROMETHEUS_METRIC_TYPES:
            raise ValueError(f"metric {name!r} has unknown type {desc['type']!r}")
        metrics.append(
            {
                "metric": name,
                "labels": [label["name"] for label in desc["labels"]],
                "exporter": PACED_FEEDER_EXPORTER,
            }
        )

    feeder = {
        "workload_table": os.path.abspath(table_path),
        "metrics": [m["metric"] for m in metrics] + records_metrics,
        "seed": seed,
        "duration_secs": (
            feeder_duration_secs
            if feeder_duration_secs is not None
            else starting_delay_secs + query_duration_secs + FEEDER_SLACK_SECS
        ),
    }
    if samples_per_sec is not None:
        feeder["samples_per_sec"] = samples_per_sec

    modes = [SKETCHDB_MODE]
    if records_metrics:
        notes.append(
            "no baseline mode: records metrics "
            f"{records_metrics} go to ASAPQuery only"
        )
    else:
        modes.append(BASELINE_MODE)

    config: dict = {
        "experiment_params": {
            "experiment": modes,
            "monitoring": {"tool": "prometheus", "deployment_mode": "bare_metal"},
            "servers": SERVERS,
            "exporters": {
                "only_start_if_queries_exist": False,
                "exporter_list": {PACED_FEEDER_EXPORTER: feeder},
            },
            "query_groups": query_groups,
            "metrics": metrics,
        }
    }
    if prebuilt_output_dir is not None:
        prebuilt_output_dir = os.path.abspath(prebuilt_output_dir)
        check_prebuilt_controller_output(prebuilt_output_dir)
        config["controller"] = {"prebuilt_output_dir": prebuilt_output_dir}
    return config, notes


def write_config(config: dict, table_path: str, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write("# @package _global_\n")
        f.write(f"# Generated by sketch_bench_e2e_configs.py from {table_path}\n")
        yaml.safe_dump(config, f, sort_keys=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--workload-table", required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--starting-delay-secs", type=int, required=True)
    parser.add_argument(
        "--query-duration-secs",
        type=int,
        required=True,
        help="query phase length; each group repeats this // T times",
    )
    parser.add_argument(
        "--query-time-offset-secs",
        type=int,
        default=DEFAULT_QUERY_TIME_OFFSET_SECS,
    )
    parser.add_argument(
        "--feeder-duration-secs",
        type=int,
        help=f"default: starting delay + query duration + {FEEDER_SLACK_SECS}",
    )
    parser.add_argument("--samples-per-sec", type=int)
    parser.add_argument("--prebuilt-output-dir")
    parser.add_argument("--sql-queries")
    parser.add_argument("--output", help="default: <output dir>/<table stem>.yaml")
    args = parser.parse_args()

    config, notes = build_config(
        args.workload_table,
        seed=args.seed,
        starting_delay_secs=args.starting_delay_secs,
        query_duration_secs=args.query_duration_secs,
        query_time_offset_secs=args.query_time_offset_secs,
        feeder_duration_secs=args.feeder_duration_secs,
        samples_per_sec=args.samples_per_sec,
        prebuilt_output_dir=args.prebuilt_output_dir,
        sql_queries_file=args.sql_queries,
    )
    stem = os.path.splitext(os.path.basename(args.workload_table))[0]
    output = args.output or os.path.join(DEFAULT_OUTPUT_DIR, f"{stem}.yaml")
    write_config(config, args.workload_table, output)
    for note in notes:
        print(f"note: {note}")
    print(output)


if __name__ == "__main__":
    main()
