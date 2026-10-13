#!/usr/bin/env python3
"""Translate a sketch-bench table's end-to-end queries into ClickHouse configs.

A sketch-bench table's `workloads[0].e2e` block lists its queries, either
`{promql, interval_secs, rqes}` or structured multi-grouping queries, and
describes each metric (`metrics`). Every metric is one ClickHouse table named
after it: `ts DateTime64(3)`, then the metric's labels in description order as
String, then `value Float64`, or for a `records` metric one Float64 column per
value field in description order.

For a table `<stem>.json` this writes, under OUT_CONFIG_DIR:
- <stem>/init.sql: the metric tables (baseline);
- <stem>/init_mv.sql: the metric tables and their materialized views
  (baseline_mv);
- <stem>/interval_<T>s_<mode>.sql: one statement per query that repeats every
  T s, for the baseline (raw table) and baseline_mv (views) arms;
- <stem>/asapquery.sql: the structured queries in ASAPQuery's SQL dialect;
- <stem>_clickhouse.yaml, <stem>_clickhouse_mv.yaml: one
  experiment_run_clickhouse.py config per arm, each loading data through its
  own init SQL, with one query group per T, repeating every T seconds.

baseline_mv keeps exact mergeable states, no sketches: one view per (metric,
value, statistic, T) holds per T-second bucket and group the state of sums,
counts, exact distinct counts, exact quantiles, each series' latest sample (instant vectors) or each
series' first/last sample, sample times and count (rate, increase). A query
merges the W/T buckets of its window plus the samples at exactly T - W (kept
apart in each bucket's `edge` rows), so W must be a multiple of T; instant
vectors read [T - 1s, T], which is bucket-aligned only when T = 1 s.

Semantics are those of Prometheus 2.53 (the version installation/prometheus
installs for bare-metal runs), whose windows are closed on both ends: range
vectors [d] at T read [T - d, T]; instant vectors read the latest sample of
each series in [T - 1s, T] (a lookback of one scrape interval); quantiles use
quantileExactInclusive; rate and increase reproduce extrapolatedRate (counters
never reset). Structured queries involve no Prometheus: they read ASAPQuery's
left-open window (T - W, T], the window its SQL frontend and precompute
windows use, and pool all samples of a group: cardinality is uniqExact of
the value field, quantile is quantileExactInclusive. T is the client's
{{ end_time }} floored to a multiple of the query's interval. Unsupported
query shapes raise.

Run from asap-tools/experiments:
    python3 datasets/fake_metric/sketch_bench_workloads.py \
        --table <sketch-bench table.json> --data-file <path> \
        --starting-delay-secs <s> --duration-secs <s>
"""
import argparse
import json
import os
import re
import sys
from dataclasses import dataclass, replace
from typing import Callable, Dict, List, Optional, Tuple

import yaml

from generate_workloads import (
    END,
    RANGE,
    SCRAPE_INTERVAL_SECONDS,
    rate_from_samples,
    window_seconds,
)

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
import constants  # noqa: E402

BASELINE = constants.BASELINE_EXPERIMENT_NAME
BASELINE_MV = constants.BASELINE_MV_EXPERIMENT_NAME

OUT_CONFIG_DIR = "config/experiment_type/sketch_bench_clickhouse"
CLICKHOUSE_URL = "http://localhost:8123"
VALUE_COLUMN = "value"
# Instant vectors read the latest sample of each series in the last scrape.
INSTANT_WINDOW_SECONDS = SCRAPE_INTERVAL_SECONDS

# Structured entries' capabilities, all pooled over a group's samples.
STRUCTURED_AGGS = ("cardinality", "quantile")

LABELS = r"(\w+(?:, \w+)*)"
LEVEL = r"([\d.]+)"
TOPK = re.compile(r"^topk\((\d+), (.+)\)$")
AGG_BY = re.compile(rf"^(sum|count) by \({LABELS}\) \((.+)\)$")
QUANTILE_BY = re.compile(rf"^quantile by \({LABELS}\) \({LEVEL}, (.+)\)$")
OVER_TIME = re.compile(rf"^(sum|count)_over_time\({RANGE}\)$")
QUANTILE_OVER_TIME = re.compile(rf"^quantile_over_time\({LEVEL}, {RANGE}\)$")
QUANTILE_RATIO = re.compile(
    rf"^quantile_over_time\({LEVEL}, {RANGE}\) / quantile_over_time\({LEVEL}, {RANGE}\)$"
)
COUNTER = re.compile(rf"^(rate|increase)\({RANGE}\)$")
SELECTOR = re.compile(r"^(\w+)$")


@dataclass(frozen=True)
class Query:
    """One query as the SQL renderers take it.

    With `series_fn` None, `agg` runs over the raw samples of each `grouping`
    group in the window (sum_over_time, sum by (job) (sum_over_time), ...).
    Otherwise `series_fn` (last, rate, increase) gives one value per series,
    and `agg` combines those per group; `agg` None keeps them per series.
    `ratio_quantile` divides the `quantile` by a second quantile of the same
    samples. Structured (non-PromQL) query entries map onto these fields and
    set `left_open`: their window is (end - W, end], not PromQL's closed one.
    """

    table: str
    interval_secs: int
    window_secs: int
    grouping: Tuple[str, ...]
    series_fn: Optional[str]
    agg: Optional[str]
    quantile: Optional[str] = None  # level as written, e.g. "0.99"
    ratio_quantile: Optional[str] = None
    topk: Optional[int] = None
    value: str = VALUE_COLUMN
    left_open: bool = False


def _parse_series(
    text: str, interval: int, labels: Dict[str, List[str]], promql: str
) -> Query:
    """A per-series expression: a selector, *_over_time, rate or increase."""

    def per_series(name, window, series_fn, agg, quantile=None, ratio=None):
        if name not in labels:
            raise ValueError(f"unknown metric {name!r} in {promql!r}")
        return Query(
            name,
            interval,
            window,
            tuple(labels[name]),
            series_fn,
            agg,
            quantile,
            ratio,
        )

    m = SELECTOR.match(text)
    if m:
        return per_series(m[1], INSTANT_WINDOW_SECONDS, "last", None)
    m = OVER_TIME.match(text)
    if m:
        fn, name, amount, unit = m.groups()
        return per_series(name, window_seconds(amount, unit), None, fn)
    m = QUANTILE_OVER_TIME.match(text)
    if m:
        q, name, amount, unit = m.groups()
        return per_series(name, window_seconds(amount, unit), None, "quantile", q)
    m = QUANTILE_RATIO.match(text)
    if m:
        q, name, amount, unit, q2, name2, amount2, unit2 = m.groups()
        if (name, amount, unit) != (name2, amount2, unit2):
            raise ValueError(f"ratio of different range vectors: {promql!r}")
        window = window_seconds(amount, unit)
        return per_series(name, window, None, "quantile", q, q2)
    m = COUNTER.match(text)
    if m:
        fn, name, amount, unit = m.groups()
        return per_series(name, window_seconds(amount, unit), fn, None)
    raise ValueError(f"unsupported PromQL shape: {promql!r}")


def _grouped(inner: Query, agg: str, by: str, quantile: Optional[str]) -> Query:
    grouping = tuple(by.split(", "))
    unknown = set(grouping) - set(inner.grouping)
    if unknown:
        raise ValueError(f"unknown labels {sorted(unknown)} for {inner.table!r}")
    if inner.series_fn is not None and inner.agg is None:
        return replace(inner, grouping=grouping, agg=agg, quantile=quantile)
    # sum by of a pooled sum/count is the pooled sum/count of the larger group.
    if agg == "sum" and inner.agg in ("sum", "count") and inner.ratio_quantile is None:
        return replace(inner, grouping=grouping)
    raise ValueError(f"cannot aggregate {inner} by {agg}")


def parse_promql(promql: str, interval: int, labels: Dict[str, List[str]]) -> Query:
    """`labels`: each metric's label names in description order."""
    text = " ".join(promql.split())
    m = TOPK.match(text)
    if m:
        inner = parse_promql(m[2], interval, labels)
        if inner.topk is not None:
            raise ValueError(f"nested topk: {promql!r}")
        return replace(inner, topk=int(m[1]))
    m = AGG_BY.match(text)
    if m:
        agg, by, inner = m.groups()
        return _grouped(_parse_series(inner, interval, labels, promql), agg, by, None)
    m = QUANTILE_BY.match(text)
    if m:
        by, q, inner = m.groups()
        series = _parse_series(inner, interval, labels, promql)
        return _grouped(series, "quantile", by, q)
    return _parse_series(text, interval, labels, promql)


def query_from_entry(entry: dict, metrics: dict) -> Query:
    """One `e2e.queries` entry: PromQL, or a structured multi-grouping query
    `{table, value, capability, grouping, quantile, window_secs,
    interval_secs}` that pools every sample of a group over the window."""
    if "promql" in entry:
        return parse_promql(
            entry["promql"], entry["interval_secs"], metric_labels(metrics)
        )
    table, capability = entry["table"], entry["capability"]
    if capability not in STRUCTURED_AGGS:
        raise ValueError(f"unsupported capability {capability!r}: {entry}")
    labels = metric_labels(metrics)[table]
    unknown = set(entry["grouping"]) - set(labels)
    if unknown or entry["value"] not in value_columns(metrics[table]):
        raise ValueError(f"unknown labels or value for {table!r}: {entry}")
    return Query(
        table,
        entry["interval_secs"],
        entry["window_secs"],
        tuple(entry["grouping"]),
        None,
        capability,
        str(entry["quantile"]) if capability == "quantile" else None,
        value=entry["value"],
        left_open=True,
    )


def describe(entry: dict) -> str:
    """A one-line comment naming the query."""
    if "promql" in entry:
        return entry["promql"]
    level = f"{entry['quantile']}, " if entry["capability"] == "quantile" else ""
    return (
        f"{entry['capability']}({level}{entry['value']}) by "
        f"({', '.join(entry['grouping'])}) over {entry['window_secs']}s"
    )


def eval_time(interval_secs: int) -> str:
    """The client's {{ end_time }} floored to a multiple of the interval."""
    return f"intDiv({END}, {interval_secs}) * {interval_secs}"


def render(
    q: Query, series: Tuple[str, ...], source: str, where: Callable, agg: Callable
) -> str:
    """SQL for `q` reading the `source` rows that `where(end, window_secs)`
    keeps for the window [end - window_secs, end], or (end - window_secs, end]
    when `q.left_open`.

    `series` is the table's full label list; `agg(stat, quantile)` gives the
    aggregate expression of a statistic over the source rows: sum, count,
    quantile, last (per-series latest value) or counter (the select list
    rate_from_samples reads).
    """
    end = eval_time(q.interval_secs)
    window = f"FROM {source} WHERE {where(end, q.window_secs)}"
    group = ", ".join(q.grouping)
    cols = ", ".join(series)
    if q.series_fn is None:
        value = agg(q.agg, q.quantile)
        if q.ratio_quantile is not None:
            value = f"{value} / {agg('quantile', q.ratio_quantile)}"
        sql = f"SELECT {group}, {value} AS value {window} GROUP BY {group}"
    else:
        if q.series_fn == "last":
            sql = (
                f"SELECT {cols}, {agg('last', None)} AS value {window} GROUP BY {cols}"
            )
        else:
            samples = (
                f"SELECT {cols}, {agg('counter', None)} {window} "
                f"GROUP BY {cols} HAVING n >= 2"
            )
            is_rate = q.series_fn == "rate"
            sql = rate_from_samples(cols, samples, end, q.window_secs, is_rate)
        if q.agg is not None:
            combine = raw_agg(q.agg, q.quantile)
            sql = f"SELECT {group}, {combine} AS value FROM ({sql}) GROUP BY {group}"
    if q.topk is not None:
        sql += f" ORDER BY value DESC, {group} LIMIT {q.topk}"
    return sql


def raw_agg(stat: str, quantile: Optional[str], value: str = VALUE_COLUMN) -> str:
    if stat == "sum":
        return f"sum({value})"
    if stat == "count":
        return "count()"
    if stat == "cardinality":
        return f"uniqExact({value})"
    if stat == "quantile":
        return f"quantileExactInclusive({quantile})({value})"
    if stat == "last":
        return f"argMax({value}, ts)"
    if stat == "counter":
        return (
            f"argMin({value}, ts) AS first_v, argMax({value}, ts) AS last_v, "
            "toFloat64(min(ts)) AS first_t, toFloat64(max(ts)) AS last_t, count() AS n"
        )
    raise ValueError(f"unknown statistic {stat!r}")


def raw_sql(q: Query, labels: Dict[str, List[str]]) -> str:
    """`q` over the raw metric table."""
    return render(
        q,
        tuple(labels[q.table]),
        q.table,
        lambda end, w: f"ts {'>' if q.left_open else '>='} {end} - {w} AND ts <= {end}",
        lambda stat, quantile: raw_agg(stat, quantile, q.value),
    )


def asapquery_sql(q: Query) -> str:
    """A structured query in the dialect of ASAPQuery's SQL frontend.

    The frontend takes a datetime literal but cannot floor it, so the window
    ends `{{ end_time }} % T` seconds before the client's {{ end_datetime }}
    (UTC; the frontend reads timezone-naive literals as server-local time):
    the same floored T as the ClickHouse SQL. The frontend maps the BETWEEN to
    (start, duration) = (T - W, W) and answers over (T - W, T], as the
    ClickHouse SQL does for structured queries.
    """
    group = ", ".join(q.grouping)
    if q.agg == "cardinality":
        agg = f"COUNT(DISTINCT {q.value})"
    elif q.agg == "quantile":
        agg = f"QUANTILE({q.quantile}, {q.value})"
    else:
        raise ValueError(f"no ASAPQuery SQL for {q}")
    t = q.interval_secs
    end = "'{{ end_datetime }}'"
    return (
        f"SELECT {group}, {agg} FROM {q.table} WHERE ts BETWEEN "
        f"DATEADD(s, -{{{{ end_time % {t} + {q.window_secs} }}}}, {end}) AND "
        f"DATEADD(s, -{{{{ end_time % {t} }}}}, {end}) GROUP BY {group}"
    )


def value_columns(desc: dict) -> List[str]:
    """A records metric has one column per value field; a gauge or counter
    has one value column."""
    return list(desc["values"]) if desc["type"] == "records" else [VALUE_COLUMN]


@dataclass(frozen=True)
class View:
    """A materialized view of `table` holding the mergeable state of one
    statistic per bucket of `interval_secs` and `keys` group."""

    name: str
    table: str
    stat: str  # sum | count | cardinality | quantile | last | counter
    interval_secs: int
    keys: Tuple[str, ...]
    levels: Tuple[str, ...]  # quantile levels the view's state answers
    value: str = VALUE_COLUMN


PER_SERIES_STATS = ("last", "counter")


def view_stat(q: Query) -> str:
    if q.series_fn is None:
        return q.agg
    return "last" if q.series_fn == "last" else "counter"


def plan_views(queries: List[Query], metrics: dict) -> Dict[tuple, View]:
    """One view per (table, value, statistic, interval), keyed by
    view_key(). Pooled statistics keep the union of their queries' groupings
    plus every label's child_of parents, so coarser groupings roll up;
    per-series statistics keep every label."""
    grouped: Dict[tuple, List[Query]] = {}
    for q in queries:
        if q.window_secs % q.interval_secs:
            raise ValueError(
                f"window {q.window_secs}s is not a multiple of the "
                f"{q.interval_secs}s buckets: {q}"
            )
        grouped.setdefault(view_key(q), []).append(q)
    views = {}
    for key, qs in grouped.items():
        table, value, stat, interval = key
        labels = metrics[table]["labels"]
        parent = {label["name"]: label.get("child_of") for label in labels}
        keep = set(parent) if stat in PER_SERIES_STATS else set()
        for name in (n for q in qs for n in q.grouping):
            while name and name not in keep:
                keep.add(name)
                name = parent[name]
        levels = {lv for q in qs for lv in (q.quantile, q.ratio_quantile) if lv}
        views[key] = View(
            name=f"{table}_{value}_{stat}_{interval}s",
            table=table,
            stat=stat,
            interval_secs=interval,
            keys=tuple(label["name"] for label in labels if label["name"] in keep),
            levels=tuple(sorted(levels, key=float)) if stat == "quantile" else (),
            value=value,
        )
    return views


def view_key(q: Query) -> tuple:
    return (q.table, q.value, view_stat(q), q.interval_secs)


def view_states(view: View) -> List[Tuple[str, str]]:
    """(column, -State expression) pairs the view stores."""
    v = view.value
    if view.stat == "sum":
        return [("sum_state", f"sumState({v})")]
    if view.stat == "count":
        return [("count_state", "countState()")]
    if view.stat == "cardinality":
        return [("cardinality_state", f"uniqExactState({v})")]
    if view.stat == "quantile":
        levels = ", ".join(view.levels)
        return [("quantile_state", f"quantilesExactInclusiveState({levels})({v})")]
    if view.stat == "last":
        return [("last_state", f"argMaxState({v}, ts)")]
    if view.stat == "counter":
        return [
            ("first_state", f"argMinState({v}, ts)"),
            ("last_state", f"argMaxState({v}, ts)"),
            ("first_ts_state", "minState(ts)"),
            ("last_ts_state", "maxState(ts)"),
            ("n_state", "countState()"),
        ]
    raise ValueError(f"unknown statistic {view.stat!r}")


def view_ddl(view: View) -> str:
    # Bucket b holds the samples in (b - T, b]; `edge` marks the one instant
    # b. A closed window [E - W, E] is the buckets in (E - W, E] plus the edge
    # of bucket E - W.
    t = view.interval_secs
    bucket = (
        f"toStartOfInterval(ts - INTERVAL 1 MILLISECOND, INTERVAL {t} SECOND) "
        f"+ INTERVAL {t} SECOND"
    )
    edge = f"toUnixTimestamp64Milli(ts) % {t * 1000} = 0"
    keys = ", ".join(("bucket", "edge") + view.keys)
    states = ", ".join(f"{expr} AS {col}" for col, expr in view_states(view))
    return (
        f"CREATE MATERIALIZED VIEW {view.name} ENGINE = AggregatingMergeTree "
        f"ORDER BY ({keys}) AS SELECT {bucket} AS bucket, {edge} AS edge, "
        f"{', '.join(view.keys)}, {states} FROM {view.table} GROUP BY {keys}"
    )


def mv_agg(view: View, stat: str, quantile: Optional[str]) -> str:
    """-Merge of the view's state; a quantile picks its level from the
    view's levels array."""
    if stat == "sum":
        return "sumMerge(sum_state)"
    if stat == "count":
        return "countMerge(count_state)"
    if stat == "cardinality":
        return "uniqExactMerge(cardinality_state)"
    if stat == "quantile":
        index = view.levels.index(quantile) + 1
        levels = ", ".join(view.levels)
        return f"quantilesExactInclusiveMerge({levels})(quantile_state)[{index}]"
    if stat == "last":
        return "argMaxMerge(last_state)"
    if stat == "counter":
        return (
            "argMinMerge(first_state) AS first_v, argMaxMerge(last_state) AS last_v, "
            "toFloat64(minMerge(first_ts_state)) AS first_t, "
            "toFloat64(maxMerge(last_ts_state)) AS last_t, countMerge(n_state) AS n"
        )
    raise ValueError(f"unknown statistic {stat!r}")


def mv_sql(q: Query, views: Dict[tuple, View], labels: Dict[str, List[str]]) -> str:
    """`q` over its view's buckets, merging their states."""
    view = views[view_key(q)]
    return render(
        q,
        tuple(labels[q.table]),
        view.name,
        # Buckets b in (end - w, end] hold (end - w, end]; the edge row of
        # bucket end - w holds the window's left boundary instant.
        lambda end, w: (
            f"bucket >= {end} - {w} AND bucket <= {end} "
            f"AND (edge OR bucket > {end} - {w})"
        ),
        lambda stat, quantile: mv_agg(view, stat, quantile),
    )


def init_sql(metrics: dict, views: List[View], create_views: bool) -> str:
    """The metric tables, and with `create_views` their views. A view only
    aggregates rows inserted after it exists, so this must run before any
    data arrives. The views are dropped either way, so none left by an
    earlier baseline_mv run feeds on the recreated tables."""
    statements = [f"DROP TABLE IF EXISTS {view.name}" for view in views]
    for table, desc in metrics.items():
        names = [label["name"] for label in desc["labels"]]
        columns = [f"{name} String" for name in names] + [
            f"{value} Float64" for value in value_columns(desc)
        ]
        statements += [
            f"DROP TABLE IF EXISTS {table}",
            f"CREATE TABLE {table} (ts DateTime64(3), {', '.join(columns)}) "
            f"ENGINE = MergeTree ORDER BY ({', '.join(names)}, ts)",
        ]
    if create_views:
        statements += [view_ddl(view) for view in views]
    return ";\n\n".join(statements) + ";\n"


def metric_labels(metrics: dict) -> Dict[str, List[str]]:
    return {
        name: [label["name"] for label in desc["labels"]]
        for name, desc in metrics.items()
    }


def write_statements(path: str, statements: List[str]) -> None:
    with open(path, "w") as f:
        f.write(";\n\n".join(statements) + ";\n")


def convert(
    table_path: str, data_file: str, starting_delay_secs: int, duration_secs: int
) -> List[str]:
    """Writes the SQL files and one config per arm; returns the config paths."""
    with open(table_path) as f:
        (workload,) = json.load(f)["workloads"]
    e2e = workload["e2e"]
    metrics = e2e["metrics"]
    labels = metric_labels(metrics)
    stem = os.path.splitext(os.path.basename(table_path))[0]
    sql_dir = os.path.join(OUT_CONFIG_DIR, stem)
    os.makedirs(sql_dir, exist_ok=True)

    entries = [(e, query_from_entry(e, metrics)) for e in e2e["queries"]]
    views = plan_views([q for _, q in entries], metrics)
    # Each arm loads data through its own init SQL, so baseline ingest never
    # feeds the views.
    init_paths = {
        BASELINE: os.path.join(sql_dir, "init.sql"),
        BASELINE_MV: os.path.join(sql_dir, "init_mv.sql"),
    }
    for mode, path in init_paths.items():
        with open(path, "w") as f:
            f.write(init_sql(metrics, list(views.values()), mode == BASELINE_MV))

    by_interval: Dict[int, Dict[str, List[str]]] = {}
    asapquery: List[str] = []
    for entry, q in entries:
        files = by_interval.setdefault(q.interval_secs, {BASELINE: [], BASELINE_MV: []})
        files[BASELINE].append(f"-- {describe(entry)}\n{raw_sql(q, labels)}")
        files[BASELINE_MV].append(f"-- {describe(entry)}\n{mv_sql(q, views, labels)}")
        if "promql" not in entry:
            asapquery.append(f"-- {describe(entry)}\n{asapquery_sql(q)}")
    if asapquery:
        # For the ASAPQuery arm; the runner does not read it.
        write_statements(os.path.join(sql_dir, "asapquery.sql"), asapquery)

    query_groups = []
    for interval, files in sorted(by_interval.items()):
        name = f"interval_{interval}s"
        sql_files = {}
        for mode, statements in files.items():
            sql_files[mode] = os.path.join(sql_dir, f"{name}_{mode}.sql")
            write_statements(sql_files[mode], statements)
        query_groups.append(
            {
                "name": name,
                "sql_file": sql_files,
                "repetition_delay_ms": interval * 1000,
                # Queries use only {{ end_time }}; the client still requires a value.
                "time_window_seconds": 0,
                "client_options": {
                    "starting_delay": starting_delay_secs,
                    "repetitions": duration_secs // interval,
                },
            }
        )

    out_paths = []
    for mode, suffix in [(BASELINE, ""), (BASELINE_MV, "_mv")]:
        out = {
            "experiment": [{"mode": mode, "server": "clickhouse"}],
            "servers": [{"name": "clickhouse", "url": CLICKHOUSE_URL}],
            "dataset": {
                "name": "custom",
                "local_data_file": data_file,
                # The runner's loader fills one table; the paced feeder writes all.
                "table": next(iter(labels)),
                "init_sql_file": init_paths[mode],
                "max_rows": 0,
            },
            # Both arms' SQL, so the saved config labels queries by baseline's text.
            "query_groups": query_groups,
            "data_ingestion_interval_ms": SCRAPE_INTERVAL_SECONDS * 1000,
        }
        out_path = os.path.join(OUT_CONFIG_DIR, f"{stem}_clickhouse{suffix}.yaml")
        with open(out_path, "w") as f:
            f.write(f"# @package experiment_params\n# Generated from {table_path}\n")
            yaml.safe_dump(out, f, sort_keys=False)
        out_paths.append(out_path)
    return out_paths


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, help="sketch-bench table .json")
    parser.add_argument(
        "--data-file", required=True, help="dataset path as seen from experiments/"
    )
    parser.add_argument("--starting-delay-secs", type=int, required=True)
    parser.add_argument(
        "--duration-secs", type=int, required=True, help="query phase length"
    )
    args = parser.parse_args()
    for path in convert(
        args.table, args.data_file, args.starting_delay_secs, args.duration_secs
    ):
        print(path)


if __name__ == "__main__":
    main()
