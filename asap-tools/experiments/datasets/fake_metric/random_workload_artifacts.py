#!/usr/bin/env python3
"""Render frozen random-workload manifests into offline planner inputs."""

import argparse
import json
from collections import defaultdict
from pathlib import Path

import yaml


ACCURACY_SLA = 0.95
PILOT_DURATION_MS = 10 * 60 * 1000
QUERY_FRESHNESS_DELAY_SECS = 10
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
                                        "universe_keys": facts["events"]["user_id_universe_size"],
                                        "keys_per_series": facts["events"]["user_ids_per_leaf"],
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
        for index, (interval_ms, rendered) in enumerate(sorted(grouped.items()), start=1)
    ]


def pilot_profile(workload):
    """Five-system workload-01 profile for the common PromQL query subset."""
    queries = [query for query in workload["queries"] if query["support"] == "common-promql"]
    max_lookback_ms = max(query["lookback_ms"] for query in queries)
    starting_delay = max_lookback_ms // 1000 + QUERY_FRESHNESS_DELAY_SECS

    def groups(query_key):
        result = []
        for group in query_groups(queries, query_key):
            interval = group["repetition_delay_ms"]
            result.append(
                {
                    **group,
                    "client_options": {
                        "starting_delay": starting_delay,
                        "repetitions": max(1, PILOT_DURATION_MS // interval),
                        "align_to_interval": True,
                        "query_time_offset": QUERY_FRESHNESS_DELAY_SECS,
                    },
                }
            )
        return result

    return {
        "workload_id": workload["id"],
        "included_query_ids": [query["id"] for query in queries],
        "excluded_query_ids": [query["id"] for query in workload["queries"] if query not in queries],
        "data_ingestion_interval_ms": workload["data_facts"]["scrape_interval_ms"],
        "promql_query_groups": groups("promql"),
        "clickhouse_query_groups": groups("clickhouse_sql"),
    }


def pilot_e2e_table(workload):
    """Render the common pilot as the existing sketch-bench E2E table shape."""
    table = paced_feeder_table(workload)
    common = [query for query in workload["queries"] if query["support"] == "common-promql"]
    table["workloads"][0]["e2e"]["queries"] = [
        {
            "promql": query["promql"],
            "interval_secs": query["interval_ms"] // 1000,
            "rqes": [rqe["id"] for rqe in query["rqes"]],
        }
        for query in common
    ]
    return table


def render(workload):
    """Return planner inputs and query schedule derived from one manifest."""
    labels = [label["name"] for label in workload["labels"]]
    queries = workload["queries"]
    return {
        "workload_id": workload["id"],
        "data_facts": workload["data_facts"],
        "queries": queries,
        "paced_feeder_table": paced_feeder_table(workload),
        "pilot_profile": pilot_profile(workload),
        "pilot_e2e_table": pilot_e2e_table(workload),
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
    table = args.out / "paced_feeder_table.json"
    table.write_text(json.dumps(rendered["paced_feeder_table"], indent=2, sort_keys=True) + "\n")
    print(table)
    profile = args.out / "pilot_profile.yaml"
    profile.write_text(yaml.safe_dump(rendered["pilot_profile"], sort_keys=False))
    print(profile)
    e2e_table = args.out / "pilot_e2e_table.json"
    e2e_table.write_text(json.dumps(rendered["pilot_e2e_table"], indent=2, sort_keys=True) + "\n")
    print(e2e_table)


if __name__ == "__main__":
    main()
