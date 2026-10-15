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

from sketch_bench_workloads import Query, raw_sql

WORKLOAD_COUNT = 10
QUERIES_PER_WORKLOAD = 10
SAMPLES_PER_SEC_PER_SERIES = 10
ACCURACY_SLA = 0.95
SECOND_MS = 1_000
MINUTE_MS = 60 * SECOND_MS
# 100 s at 10 samples/s gives each series' quantile sketch 1,000 samples, the
# first N sketch-bench's accuracy curves cover.
QUANTILE_LOOKBACK_MS = 100 * SECOND_MS
QUANTILE_INTERVAL_MS = SECOND_MS
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
BACKFILL_HISTORY_MS = 20 * MINUTE_MS
LEAF_SERIES_COUNT = math.prod(LABEL_CARDINALITIES.values())
LEAF_LABELS = tuple(name for name, _, _ in LABEL_HIERARCHY)
# Every ClickHouse table has the leaf labels (sketch_bench_workloads' tables).
CLICKHOUSE_LABELS = {
    table: list(LEAF_LABELS) for table in ("data", "data_total", "events")
}
ZIPF_EXPONENT = 1.1
USER_ID_UNIVERSE_SIZE = 10_000
USER_IDS_PER_LEAF = 1_000
# A variant stretches one dimension of a seed's workloads and keeps every
# random choice, so its workloads pair with the base's: (lookback scale,
# label_0 values per job).
VARIANTS = {
    "base": (1, 50),
    # Lookbacks x3: 1m -> 3m, ..., 20m -> 60m; quantiles 100s -> 300s.
    "long": (3, 50),
    # x2 series: 20,000 at 10/s, 200,000 samples/s.
    "x2": (1, 100),
    # x5 series: 50,000 at 10/s, 500,000 samples/s.
    "x5": (1, 250),
}


def duration(milliseconds):
    """Render a whole-minute duration for the supported PromQL templates."""
    return f"{milliseconds // MINUTE_MS}m"


def grouping_text(grouping):
    return ", ".join(grouping)


def rqe_query(rqe):
    """The ClickHouse renderers' Query for one RQE, built from its fields,
    never from its PromQL. Raw and MV ClickHouse SQL both render it."""
    capability, grouping = rqe["capability"], tuple(rqe["grouping_labels"])
    common = {
        "table": rqe["metric"],
        "interval_secs": rqe["interval_ms"] // SECOND_MS,
        "window_secs": rqe["lookback_ms"] // SECOND_MS,
    }
    if capability == "sum":
        return Query(**common, grouping=grouping, series_fn=None, agg="sum")
    if capability == "quantile" and rqe.get("pooled"):
        return Query(
            **common,
            grouping=grouping,
            series_fn=None,
            agg="quantile",
            quantile=f"{rqe['phi']:.2f}",
        )
    if capability == "quantile":
        quantile = f"{rqe['phi']:.2f}"
        return Query(
            **common,
            grouping=grouping,
            series_fn="quantile",
            agg="avg",
            quantile=quantile,
        )
    if capability == "topk_by_value" and rqe.get("pooled"):
        return Query(
            **common, grouping=grouping, series_fn=None, agg="sum", topk=rqe["k"]
        )
    if capability == "topk_by_value":
        return Query(
            **common,
            grouping=LEAF_LABELS,
            series_fn=None,
            agg="sum",
            topk=rqe["k"],
            topk_by=grouping,
        )
    if capability in ("topk_by_count", "rate"):
        series_fn = "increase" if capability == "topk_by_count" else "rate"
        return Query(
            **common,
            grouping=LEAF_LABELS,
            series_fn=series_fn,
            agg=None,
            topk=rqe["k"],
            topk_by=grouping,
        )
    if capability == "cardinality":
        # Planner SQL windows are left-open, (E - W, E].
        return Query(
            **common,
            grouping=grouping,
            series_fn=None,
            agg="cardinality",
            value=rqe["value"],
            left_open=True,
        )
    raise ValueError(f"unknown capability {capability!r}")


def metric_labels(label_0_cardinality):
    """METRIC_LABELS with `label_0_cardinality` label_0 values per job."""
    return [
        (
            {**label, "cardinality": label_0_cardinality}
            if label["name"] == "label_0"
            else label
        )
        for label in METRIC_LABELS
    ]


def choose_temporal_time(rng):
    lookback_ms, intervals = rng.choice(TEMPORAL_TIMES)
    return lookback_ms, rng.choice(intervals)


def workload_data_facts(
    seed, leaf_count=LEAF_SERIES_COUNT, backfill_history_ms=BACKFILL_HISTORY_MS
):
    """Return the feeder contract shared by every workload from one seed."""
    return {
        "feeder_seed": seed,
        "scrape_interval_ms": SECOND_MS // SAMPLES_PER_SEC_PER_SERIES,
        "backfill_history_ms": backfill_history_ms,
        "series": {
            "leaf_count": leaf_count,
            "ranking": {
                "kind": "deterministic_zipf_like",
                "exponent": ZIPF_EXPONENT,
                "rank_scope": "leaf_series",
            },
        },
        "metrics": {
            "data": {
                "kind": "gauge",
                "value_shape": "rank_scaled_pareto",
                "pareto_shape": 2.0,
            },
            "data_total": {
                "kind": "counter",
                "increment_shape": "rank_scaled_positive_deterministic",
                "resets": "none",
            },
        },
        "events": {
            "table": "events",
            "time_column": "ts",
            "distinct_key": "user_id",
            "user_id_universe_size": USER_ID_UNIVERSE_SIZE,
            "user_ids_per_leaf": USER_IDS_PER_LEAF,
            "assignment": "deterministic_rotating_pool_with_cross_leaf_overlap",
        },
    }


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
    metrics = {
        "rate": "data_total",
        "topk_by_count": "data_total",
        "cardinality": "events",
    }
    rqe = {
        "id": f"{query_id}/rqe",
        "capability": capability,
        "metric": metrics.get(capability, "data"),
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
        "clickhouse_sql": raw_sql(rqe_query(rqe), CLICKHOUSE_LABELS),
        "rqes": [rqe],
    }


def sum_query(query_id, grouping, lookback_ms, interval_ms):
    rendered = f"sum by ({grouping_text(grouping)}) (sum_over_time(data[{duration(lookback_ms)}]))"
    return query(
        query_id,
        "sum",
        grouping,
        lookback_ms,
        interval_ms,
        rendered,
        "common-promql",
    )


def quantile_query(
    query_id, grouping, phi, lookback_ms=QUANTILE_LOOKBACK_MS, pooled=False
):
    """A quantile by `grouping`: the avg of per-series quantiles, or pooled,
    the quantile of all of a group's samples. PromQL can't say the pooled
    one, so its PromQL is a stand-in: the quantile of per-series quantiles."""
    window = f"data[{lookback_ms // SECOND_MS}s]"
    if pooled:
        rendered = f"quantile by ({grouping_text(grouping)}) ({phi:.2f}, quantile_over_time({phi:.2f}, {window}))"
    else:
        rendered = f"avg by ({grouping_text(grouping)}) (quantile_over_time({phi:.2f}, {window}))"
    return query(
        query_id,
        "quantile",
        grouping,
        lookback_ms,
        QUANTILE_INTERVAL_MS,
        rendered,
        "common-promql",
        phi=phi,
        **({"pooled": True} if pooled else {}),
    )


def topk_sum_query(query_id, grouping, lookback_ms, interval_ms, k, pooled=False):
    """The k largest windowed sums: per series within each `grouping` group,
    or pooled, of `grouping`'s groups over all series."""
    if pooled:
        rendered = f"topk({k}, sum by ({grouping_text(grouping)}) (sum_over_time(data[{duration(lookback_ms)}])))"
    else:
        rendered = f"topk by ({grouping_text(grouping)}) ({k}, sum_over_time(data[{duration(lookback_ms)}]))"
    item = query(
        query_id,
        "topk_by_value",
        grouping,
        lookback_ms,
        interval_ms,
        rendered,
        "common-promql",
        k=k,
        **({"pooled": True} if pooled else {}),
    )
    if pooled:
        item["output_groups"], item["output_series_upper_bound"] = 1, k
    return item


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


def generate_workload(seed, index, variant="base", pooled=False):
    """Return one validated ten-query workload for `seed` and one-based
    `index`, stretched as VARIANTS[`variant`] says. `pooled` makes its
    quantiles pooled and its sum top-k rank the fine grouping's groups; its
    random choices stay the base's, so its queries pair with them."""
    scale, label_0_cardinality = VARIANTS[variant]
    rng = random.Random(f"{seed}/{index}")
    fine, coarse = GROUPINGS[2], GROUPINGS[1]
    rollup_w, rollup_t = choose_temporal_time(rng)
    rollup_w *= scale
    short_w, short_t = scale * MINUTE_MS, rng.choice((SECOND_MS, 2 * SECOND_MS))
    long_w, long_t = scale * 20 * MINUTE_MS, rng.choice((30 * SECOND_MS, MINUTE_MS))
    quantile_w = scale * QUANTILE_LOOKBACK_MS

    def topk_grouping():
        # Drawn either way, so later draws match the base's.
        drawn = rng.choice(GROUPINGS[:2])
        return fine if pooled else drawn

    quantile_specs = rng.sample(
        [(grouping, phi) for grouping in GROUPINGS for phi in QUANTILES], 3
    )
    queries = [
        sum_query("q01-sum-fine", fine, rollup_w, rollup_t),
        sum_query("q02-sum-coarse", coarse, rollup_w, rollup_t),
        quantile_query("q03-quantile", *quantile_specs[0], quantile_w, pooled),
        quantile_query("q04-quantile", *quantile_specs[1], quantile_w, pooled),
        quantile_query("q05-quantile", *quantile_specs[2], quantile_w, pooled),
        topk_sum_query(
            "q06-topk-sum",
            topk_grouping(),
            short_w,
            short_t,
            rng.choice(TOPK_VALUES),
            pooled,
        ),
        topk_sum_query(
            "q07-topk-sum",
            topk_grouping(),
            long_w,
            long_t,
            rng.choice(TOPK_VALUES),
            pooled,
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
    labels = metric_labels(label_0_cardinality)
    workload = {
        "version": 1,
        "id": f"workload-{index:02d}",
        "seed": seed,
        "labels": labels,
        "samples_per_sec_per_series": SAMPLES_PER_SEC_PER_SERIES,
        "data_facts": workload_data_facts(
            seed,
            math.prod(label["cardinality"] for label in labels),
            scale * BACKFILL_HISTORY_MS,
        ),
        "queries": queries,
    }
    if variant != "base":
        workload["variant"] = variant
    if pooled:
        workload["pooled"] = True
    validate_workload(workload)
    return workload


def validate_workload(workload):
    """Reject manifests that violate the experiment's stable public contract."""
    queries = workload["queries"]
    data_facts = workload["data_facts"]
    if data_facts["scrape_interval_ms"] != SECOND_MS // SAMPLES_PER_SEC_PER_SERIES:
        raise ValueError("data facts must declare the 100 ms scrape interval")
    leaf_count = math.prod(label["cardinality"] for label in workload["labels"])
    if data_facts["series"]["leaf_count"] != leaf_count:
        raise ValueError("data facts must declare every leaf series")
    if data_facts["metrics"]["data_total"]["resets"] != "none":
        raise ValueError("the pilot counter contract must be monotonic")
    events = data_facts["events"]
    if not 0 < events["user_ids_per_leaf"] <= events["user_id_universe_size"]:
        raise ValueError("distinct user pools must fit in the declared universe")
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
            rqe.get("pooled", False),
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


def generate_workloads(
    seed, workload_count=WORKLOAD_COUNT, variant="base", pooled=False
):
    """Generate the deterministic workload suite for one experiment seed."""
    return [
        generate_workload(seed, index, variant, pooled)
        for index in range(1, workload_count + 1)
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--variant", choices=sorted(VARIANTS), default="base")
    parser.add_argument(
        "--pooled", action="store_true", help="pooled quantiles and top-k"
    )
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    for workload in generate_workloads(
        args.seed, variant=args.variant, pooled=args.pooled
    ):
        path = args.out / f"{workload['id']}.json"
        path.write_text(json.dumps(workload, indent=2, sort_keys=True) + "\n")
        print(path)


if __name__ == "__main__":
    main()
