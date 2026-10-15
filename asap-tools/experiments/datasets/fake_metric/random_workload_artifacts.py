#!/usr/bin/env python3
"""Render frozen random-workload manifests into offline planner inputs and,
with --pilot-dir, the five-system pilot's run configs."""

import argparse
import copy
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

import yaml

import sketch_bench_workloads as sbw
from random_workloads import CLICKHOUSE_LABELS, rqe_query

EXPERIMENTS_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, EXPERIMENTS_DIR)
from sketch_bench_e2e.sketch_bench_e2e_configs import (  # noqa: E402
    BASELINE_MODE,
    build_config,
)

ACCURACY_SLA = 0.95
PILOT_SUPPORT = "common-promql"
PILOT_DURATION_MS = 10 * 60 * 1000
# Runs stop their feeders once the queries are done, so a feed's duration
# only bounds a run that hangs: this many times the scheduled query phase,
# past the slowest pilot arm (raw ClickHouse, ~4x).
FEED_BOUND_QUERY_PHASES = 10
QUERY_FRESHNESS_DELAY_SECS = 10
# Relative to EXPERIMENTS_DIR, where the runners read configs and SQL.
CONFIG_DIR = "config/experiment_type/random_workload"
VICTORIAMETRICS_URL = "http://localhost:8428"
# 26.6 and later need AVX2, which CloudLab's Xeon E5-2450 nodes lack; the
# ClickHouse SQL's semantics tests also pass on 23.8.
CLICKHOUSE_IMAGE_TAG = "23.8"
PROMQL_METRICS = ("data", "data_total")
SQL_EVENTS_TABLE = {
    "name": "events",
    "time_column": "ts",
    "value_columns": ["user_id"],
    "metadata_columns": ["region", "service", "job", "label_0"],
}


def paced_feeder_table(workload):
    """Render the manifest's data facts as a paced-feeder workload table."""
    facts = workload["data_facts"]
    labels = workload["labels"]
    series = facts["series"]
    samples_per_sec = workload["samples_per_sec_per_series"]
    return {
        "workloads": [
            {
                "e2e": {
                    "metrics": {
                        "data": {
                            "type": "gauge",
                            "labels": labels,
                            "samples_per_sec_per_series": samples_per_sec,
                            "values": {
                                "value": {
                                    "zipf": series["ranking"]["exponent"],
                                    "keys": series["leaf_count"],
                                },
                                "dist": {
                                    "pareto": facts["metrics"]["data"]["pareto_shape"]
                                },
                            },
                        },
                        "data_total": {
                            "type": "counter",
                            "labels": labels,
                            "samples_per_sec_per_series": samples_per_sec,
                            "values": {
                                "increment": {
                                    "zipf": series["ranking"]["exponent"],
                                    "keys": series["leaf_count"],
                                }
                            },
                        },
                        "events": {
                            "type": "records",
                            "records_per_sec": series["leaf_count"] * samples_per_sec,
                            "labels": [{**label, "skew": 0.0} for label in labels],
                            "values": {
                                facts["events"]["distinct_key"]: {
                                    "rotating_pool": {
                                        "universe_keys": facts["events"][
                                            "user_id_universe_size"
                                        ],
                                        "keys_per_series": facts["events"][
                                            "user_ids_per_leaf"
                                        ],
                                    }
                                }
                            },
                        },
                    }
                }
            }
        ]
    }


def query_groups(queries, query_key):
    """Group rendered query text by its fixed repetition interval."""
    grouped = defaultdict(list)
    for query in queries:
        rendered = query[query_key]
        if rendered:
            grouped[query["interval_ms"]].append(rendered)
    return [
        {
            "id": index,
            "repetition_delay_ms": interval_ms,
            "queries": rendered,
            "controller_options": {"accuracy_sla": ACCURACY_SLA},
        }
        for index, (interval_ms, rendered) in enumerate(
            sorted(grouped.items()), start=1
        )
    ]


def pilot_configs(workload, pilot_dir):
    """The five-system pilot's run configs, {path under EXPERIMENTS_DIR:
    content}: ASAPQuery (the saved plan in `pilot_dir`) and Prometheus in
    one experiment_run_e2e.py config, VictoriaMetrics in another, and raw
    and MV ClickHouse in one experiment_run_clickhouse.py config each.

    Every arm feeds the metrics of `pilot_dir`'s eval table with the
    manifest's feeder seed, waits out the longest lookback plus the
    freshness delay, then evaluates each query at multiples of its interval
    for PILOT_DURATION_MS: the same windows relative to its data start.
    Raw ClickHouse runs the manifest's clickhouse_sql, and ClickHouse+MV
    the same RQEs' Query over exact views (sketch_bench_workloads)."""
    queries = [q for q in workload["queries"] if q["support"] == PILOT_SUPPORT]
    lookback_secs = max(q["lookback_ms"] for q in queries) // 1000
    starting_delay = lookback_secs + QUERY_FRESHNESS_DELAY_SECS
    duration = PILOT_DURATION_MS // 1000
    feed_secs = starting_delay + FEED_BOUND_QUERY_PHASES * duration
    seed = workload["data_facts"]["feeder_seed"]
    table = pilot_dir / "eval_table.json"
    out_dir = Path(CONFIG_DIR) / workload["id"]

    e2e, _ = build_config(
        str(table),
        seed,
        starting_delay,
        duration,
        QUERY_FRESHNESS_DELAY_SECS,
        feeder_duration_secs=feed_secs,
        prebuilt_output_dir=str(pilot_dir),
    )
    # One monitoring tool per run, so VictoriaMetrics is its own baseline run.
    vm = copy.deepcopy(e2e)
    del vm["controller"]
    vm_params = vm["experiment_params"]
    vm_params["experiment"] = [BASELINE_MODE]
    vm_params["monitoring"] = {
        "tool": "victoriametrics",
        "deployment_mode": "containerized",
    }
    for server in vm_params["servers"]:
        if server["name"] == "prometheus":
            server["url"] = VICTORIAMETRICS_URL

    metrics = json.loads(table.read_text())["workloads"][0]["e2e"]["metrics"]
    rqe_queries = [rqe_query(query["rqes"][0]) for query in queries]
    views = sbw.plan_views(rqe_queries, metrics)
    files = {
        "init.sql": sbw.init_sql(metrics, [], create_views=False),
        "init_mv.sql": sbw.init_sql(metrics, list(views.values()), create_views=True),
    }
    statements = defaultdict(lambda: {sbw.BASELINE: [], sbw.BASELINE_MV: []})
    for query, q in zip(queries, rqe_queries):
        sql = {
            sbw.BASELINE: query["clickhouse_sql"],
            sbw.BASELINE_MV: sbw.mv_sql(q, views, CLICKHOUSE_LABELS),
        }
        for mode, text in sql.items():
            statements[q.interval_secs][mode].append(f"-- {query['promql']}\n{text}")
    groups = []
    for interval, by_mode in sorted(statements.items()):
        sql_files = {}
        for mode, texts in by_mode.items():
            name = f"interval_{interval}s_{mode}.sql"
            files[name] = sbw.statements_text(texts)
            sql_files[mode] = str(out_dir / name)
        groups.append(
            {
                "name": f"interval_{interval}s",
                "sql_file": sql_files,
                "repetition_delay_ms": interval * 1000,
                # The SQL reads only {{ end_time }}; the client requires a value.
                "time_window_seconds": 0,
                "client_options": {
                    "starting_delay": starting_delay,
                    "repetitions": duration // interval,
                    "align_to_interval": True,
                    "query_time_offset": QUERY_FRESHNESS_DELAY_SECS,
                },
            }
        )
    feeder = {
        "workload_table": str(table.resolve()),
        "metrics": list(metrics),
        "seed": seed,
        "duration_secs": feed_secs,
    }
    for mode, init, name in (
        (sbw.BASELINE, "init.sql", "clickhouse.yaml"),
        (sbw.BASELINE_MV, "init_mv.sql", "clickhouse_mv.yaml"),
    ):
        clickhouse = {
            "experiment": [{"mode": mode, "server": "clickhouse"}],
            "servers": [{"name": "clickhouse", "url": sbw.CLICKHOUSE_URL}],
            "dataset": {
                "name": "custom",
                "init_sql_file": str(out_dir / init),
                "paced_feeder": feeder,
                "clickhouse_image_tag": CLICKHOUSE_IMAGE_TAG,
            },
            "query_groups": groups,
            "data_ingestion_interval_ms": workload["data_facts"]["scrape_interval_ms"],
        }
        files[name] = "# @package experiment_params\n" + yaml.safe_dump(
            clickhouse, sort_keys=False
        )
    for name, config in (("e2e.yaml", e2e), ("victoriametrics.yaml", vm)):
        files[name] = "# @package _global_\n" + yaml.safe_dump(config, sort_keys=False)
    return {out_dir / name: content for name, content in files.items()}


def render(workload):
    """Return planner inputs and query schedule derived from one manifest."""
    labels = [label["name"] for label in workload["labels"]]
    queries = workload["queries"]
    return {
        "workload_id": workload["id"],
        "data_facts": workload["data_facts"],
        "queries": queries,
        "paced_feeder_table": paced_feeder_table(workload),
        "promql_planner": {
            "metrics": [
                {"metric": metric, "labels": labels} for metric in PROMQL_METRICS
            ],
            "query_groups": query_groups(queries, "promql"),
            "aggregate_cleanup": {"policy": "read_based"},
        },
        "sql_planner": {
            "tables": [SQL_EVENTS_TABLE],
            "query_groups": query_groups(queries, "sql"),
            "aggregate_cleanup": {"policy": "read_based"},
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--pilot-dir",
        type=Path,
        help="the workload's archived pilot plan (eval_table.json and planner "
        f"configs): also write the pilot's run configs under {CONFIG_DIR}",
    )
    args = parser.parse_args()
    workload = json.loads(args.manifest.read_text())
    rendered = render(workload)
    args.out.mkdir(parents=True, exist_ok=True)
    for name in ("promql_planner", "sql_planner"):
        path = args.out / f"{name}.yaml"
        path.write_text(yaml.safe_dump(rendered[name], sort_keys=False))
        print(path)
    schedule = args.out / "queries.json"
    schedule.write_text(
        json.dumps(
            {
                "workload_id": rendered["workload_id"],
                "data_facts": rendered["data_facts"],
                "queries": rendered["queries"],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    print(schedule)
    table = args.out / "paced_feeder_table.json"
    table.write_text(
        json.dumps(rendered["paced_feeder_table"], indent=2, sort_keys=True) + "\n"
    )
    print(table)
    if args.pilot_dir:
        for path, content in pilot_configs(workload, args.pilot_dir).items():
            path = Path(EXPERIMENTS_DIR) / path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
            print(path)


if __name__ == "__main__":
    main()
