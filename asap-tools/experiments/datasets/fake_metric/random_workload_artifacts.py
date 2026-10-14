#!/usr/bin/env python3
"""Render frozen random-workload manifests into offline planner inputs."""

import argparse
import json
from collections import defaultdict
from pathlib import Path

import yaml


ACCURACY_SLA = 0.95
PROMQL_METRICS = ("data", "data_total")
SQL_EVENTS_TABLE = {
    "name": "events",
    "time_column": "ts",
    "value_columns": ["user_id"],
    "metadata_columns": ["region", "service", "job", "label_0"],
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
        for index, (interval_ms, rendered) in enumerate(sorted(grouped.items()), start=1)
    ]


def render(workload):
    """Return planner inputs and query schedule derived from one manifest."""
    labels = [label["name"] for label in workload["labels"]]
    queries = workload["queries"]
    return {
        "workload_id": workload["id"],
        "data_facts": workload["data_facts"],
        "queries": queries,
        "promql_planner": {
            "metrics": [{"metric": metric, "labels": labels} for metric in PROMQL_METRICS],
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


if __name__ == "__main__":
    main()
