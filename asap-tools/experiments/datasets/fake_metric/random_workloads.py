#!/usr/bin/env python3
"""Generate deterministic manifests for accelerated random E2E workloads.

The manifest is the source of truth for an optimizer RQE and the query text
that the E2E runners execute.  It deliberately does not invoke a planner or
write runner-specific configuration.
"""

import argparse
import json
import math
import random
from pathlib import Path


WORKLOAD_COUNT = 10
QUERIES_PER_WORKLOAD = 10
SAMPLES_PER_SEC_PER_SERIES = 10
ACCURACY_SLA = 0.95
SECOND_MS = 1_000
MINUTE_MS = 60 * SECOND_MS
SPATIAL_LOOKBACK_MS = 100
SPATIAL_INTERVAL_MS = 100
LABEL_HIERARCHY = (
    ("region", 4, None),
    ("service", 5, "region"),
    ("job", 10, "service"),
    ("label_0", 50, "job"),
)
LABEL_CARDINALITIES = {name: cardinality for name, cardinality, _ in LABEL_HIERARCHY}
METRIC_LABELS = [
    {
        "name": name,
        "cardinality": cardinality,
        **({"child_of": parent} if parent else {}),
    }
    for name, cardinality, parent in LABEL_HIERARCHY
]
GROUPINGS = (("region",), ("region", "service"), ("region", "service", "job"))
GROUP_COUNTS = {
    grouping: math.prod(LABEL_CARDINALITIES[label] for label in grouping)
    for grouping in GROUPINGS
}
TEMPORAL_TIMES = (
    (MINUTE_MS, (SECOND_MS, 2 * SECOND_MS)),
    (5 * MINUTE_MS, (5 * SECOND_MS, 10 * SECOND_MS)),
    (10 * MINUTE_MS, (10 * SECOND_MS, 20 * SECOND_MS)),
    (20 * MINUTE_MS, (30 * SECOND_MS, MINUTE_MS)),
)
QUANTILES = (0.50, 0.75, 0.90, 0.95, 0.99)
TOPK_VALUES = (4, 8)


def duration(milliseconds):
    """Render a whole-minute duration for the supported PromQL templates."""
    return f"{milliseconds // MINUTE_MS}m"


def grouping_text(grouping):
    return ", ".join(grouping)


def choose_temporal_time(rng):
    lookback_ms, intervals = rng.choice(TEMPORAL_TIMES)
    return lookback_ms, rng.choice(intervals)


def query(
    query_id,
    capability,
    grouping,
    lookback_ms,
    interval_ms,
    rendered,
    support,
    **params,
):
    """Create one manifest query and its single optimizer-facing RQE."""
    rqe = {
        "id": f"{query_id}/rqe",
        "capability": capability,
        "metric": "data_total" if capability in ("rate", "topk_by_count") else "data",
        "grouping_labels": list(grouping),
        "lookback_ms": lookback_ms,
        "interval_ms": interval_ms,
        "accuracy_sla": ACCURACY_SLA,
        **params,
    }
    return {
        "id": query_id,
        "support": support,
        "grouping": list(grouping),
        "output_groups": GROUP_COUNTS[grouping],
        "output_series_upper_bound": GROUP_COUNTS[grouping] * params.get("k", 1),
        "lookback_ms": lookback_ms,
        "interval_ms": interval_ms,
        "promql": rendered if support != "sql-only" else None,
        "sql": rendered if support == "sql-only" else None,
        "rqes": [rqe],
    }


def sum_query(query_id, grouping, lookback_ms, interval_ms):
    rendered = f"sum by ({grouping_text(grouping)}) (sum_over_time(data[{duration(lookback_ms)}]))"
    return query(
        query_id, "sum", grouping, lookback_ms, interval_ms, rendered, "common-promql"
    )


def quantile_query(query_id, grouping, phi):
    rendered = f"quantile by ({grouping_text(grouping)}) ({phi:.2f}, data)"
    return query(
        query_id,
        "quantile",
        grouping,
        SPATIAL_LOOKBACK_MS,
        SPATIAL_INTERVAL_MS,
        rendered,
        "common-promql",
        phi=phi,
    )


def topk_sum_query(query_id, grouping, lookback_ms, interval_ms, k):
    rendered = f"topk by ({grouping_text(grouping)}) ({k}, sum_over_time(data[{duration(lookback_ms)}]))"
    return query(
        query_id,
        "topk_by_value",
        grouping,
        lookback_ms,
        interval_ms,
        rendered,
        "common-promql",
        k=k,
    )


def topk_count_query(query_id, grouping, lookback_ms, interval_ms, k):
    rendered = f"topk by ({grouping_text(grouping)}) ({k}, increase(data_total[{duration(lookback_ms)}]))"
    return query(
        query_id,
        "topk_by_count",
        grouping,
        lookback_ms,
        interval_ms,
        rendered,
        "counter-gated-promql",
        k=k,
    )


def rate_query(query_id, grouping, lookback_ms, interval_ms, k):
    rendered = f"topk by ({grouping_text(grouping)}) ({k}, rate(data_total[{duration(lookback_ms)}]))"
    return query(
        query_id,
        "rate",
        grouping,
        lookback_ms,
        interval_ms,
        rendered,
        "counter-gated-promql",
        k=k,
    )


def cardinality_query(query_id, grouping, lookback_ms, interval_ms):
    group = grouping_text(grouping)
    lookback_secs = lookback_ms // SECOND_MS
    rendered = (
        f"SELECT {group}, COUNT(DISTINCT user_id) FROM events "
        f"WHERE ts BETWEEN DATEADD(s, -{lookback_secs}, NOW()) AND NOW() "
        f"GROUP BY {group}"
    )
    return query(
        query_id,
        "cardinality",
        grouping,
        lookback_ms,
        interval_ms,
        rendered,
        "sql-only",
        value="user_id",
    )


def generate_workload(seed, index):
    """Return one validated ten-query workload for `seed` and one-based `index`."""
    rng = random.Random(f"{seed}/{index}")
    fine, coarse = GROUPINGS[2], GROUPINGS[1]
    rollup_w, rollup_t = choose_temporal_time(rng)
    short_w, short_t = MINUTE_MS, rng.choice((SECOND_MS, 2 * SECOND_MS))
    long_w, long_t = 20 * MINUTE_MS, rng.choice((30 * SECOND_MS, MINUTE_MS))
    quantile_specs = rng.sample(
        [(grouping, phi) for grouping in GROUPINGS for phi in QUANTILES], 3
    )
    queries = [
        sum_query("q01-sum-fine", fine, rollup_w, rollup_t),
        sum_query("q02-sum-coarse", coarse, rollup_w, rollup_t),
        quantile_query("q03-quantile", *quantile_specs[0]),
        quantile_query("q04-quantile", *quantile_specs[1]),
        quantile_query("q05-quantile", *quantile_specs[2]),
        topk_sum_query(
            "q06-topk-sum",
            rng.choice(GROUPINGS[:2]),
            short_w,
            short_t,
            rng.choice(TOPK_VALUES),
        ),
        topk_sum_query(
            "q07-topk-sum",
            rng.choice(GROUPINGS[:2]),
            long_w,
            long_t,
            rng.choice(TOPK_VALUES),
        ),
        topk_count_query(
            "q08-topk-count",
            rng.choice(GROUPINGS[:2]),
            rollup_w,
            rollup_t,
            rng.choice(TOPK_VALUES),
        ),
        rate_query(
            "q09-rate",
            rng.choice(GROUPINGS[:2]),
            long_w,
            long_t,
            rng.choice(TOPK_VALUES),
        ),
        cardinality_query("q10-cardinality", rng.choice(GROUPINGS), rollup_w, rollup_t),
    ]
    workload = {
        "version": 1,
        "id": f"workload-{index:02d}",
        "seed": seed,
        "labels": METRIC_LABELS,
        "samples_per_sec_per_series": SAMPLES_PER_SEC_PER_SERIES,
        "queries": queries,
    }
    validate_workload(workload)
    return workload


def validate_workload(workload):
    """Reject manifests that violate the experiment's stable public contract."""
    queries = workload["queries"]
    if len(queries) != QUERIES_PER_WORKLOAD:
        raise ValueError(
            f"a workload must contain exactly {QUERIES_PER_WORKLOAD} queries"
        )
    if len({query["id"] for query in queries}) != len(queries):
        raise ValueError("query IDs must be unique")
    rqe_keys = {
        (
            rqe["capability"],
            tuple(rqe["grouping_labels"]),
            rqe["lookback_ms"],
            rqe["interval_ms"],
            rqe.get("phi"),
            rqe.get("k"),
        )
        for item in queries
        for rqe in item["rqes"]
    }
    if len(rqe_keys) != len(queries):
        raise ValueError("a workload must not repeat an atomic RQE")
    if sum(query["rqes"][0]["capability"] == "quantile" for query in queries) < 3:
        raise ValueError("a workload must contain at least three quantile queries")
    if sum(query["rqes"][0]["capability"].startswith("topk_") for query in queries) < 2:
        raise ValueError("a workload must contain at least two top-k queries")
    if not any(query["rqes"][0]["capability"] == "sum" for query in queries):
        raise ValueError("a workload must contain a sum query")
    for item in queries:
        grouping = tuple(item["grouping"])
        if grouping not in GROUP_COUNTS:
            raise ValueError(f"unsupported grouping: {grouping}")
        if len(item["rqes"]) != 1:
            raise ValueError(f"{item['id']} must map to one RQE")
        if not item["rqes"][0]["id"]:
            raise ValueError(f"{item['id']} has no RQE ID")
    if not any(
        q["rqes"][0]["capability"] == "sum" and tuple(q["grouping"]) == GROUPINGS[2]
        for q in queries
    ):
        raise ValueError("a workload must contain the fine side of a roll-up chain")


def generate_workloads(seed, workload_count=WORKLOAD_COUNT):
    """Generate the deterministic workload suite for one experiment seed."""
    return [generate_workload(seed, index) for index in range(1, workload_count + 1)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    for workload in generate_workloads(args.seed):
        path = args.out / f"{workload['id']}.json"
        path.write_text(json.dumps(workload, indent=2, sort_keys=True) + "\n")
        print(path)


if __name__ == "__main__":
    main()
