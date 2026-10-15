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
- <stem>/interval_<T>s[_structured]_<mode>.sql: one statement per query that
  repeats every T s, for the baseline (raw table) and baseline_mv (views)
  arms, and for structured entries also sketchdb's: the entry's `sql`
  (ASAPQuery's SQL, as sketch-bench's exporter writes it) with NOW() replaced
  by the floored eval time;
- <stem>_clickhouse.yaml, <stem>_clickhouse_mv.yaml: one
  experiment_run_clickhouse.py config per arm, each creating the tables
  through its own init SQL and feeding every metric with the paced feeder,
  with one query group per T and kind (structured groups first), repeating
  every T seconds;
- <stem>_clickhouse_sketchdb.yaml, with --prebuilt-output-dir: baseline and
  sketchdb in one experiment, so baseline's results score sketchdb's.
  sketchdb runs the structured groups only (PromQL entries have no ASAPQuery
  SQL) on those planner configs; since they come first, its query indices
  are baseline's.

baseline_mv keeps exact mergeable states, no sketches: one view per (metric,
value, statistic, T) holds per T-second bucket and group the state of sums,
counts, exact distinct counts, exact quantiles, each series' latest sample (instant vectors) or each
series' first/last sample, sample times and count (rate, increase). A query
at eval time E merges the W/T buckets of its window, plus for PromQL the
samples at exactly E - W (kept apart in each bucket's `edge` rows), so W must
be a multiple of T; instant vectors read [E - 1s, E], which is bucket-aligned
only when T = 1 s. The views live in VIEW_DATABASE.
A table with any query the views cannot serve gets no baseline_mv arm (no
init_mv.sql, MV SQL or _mv config) and a note on stderr naming the queries:
dropping just those queries would shift the arm's query indices against the
baseline SQL that labels both arms' results.

Semantics are those of Prometheus 2.53 (the version installation/prometheus
installs for bare-metal runs), whose windows are closed on both ends: range
vectors [d] at E read [E - d, E]; instant vectors read the latest sample of
each series in [E - 1s, E] (a lookback of one scrape interval, not
Prometheus's 5m default: the feeder writes every series every scrape, so the
two agree unless a sample is missing, where Prometheus would return the
series' last value and this SQL drops the series); quantiles use
quantileExactInclusive; rate and increase reproduce extrapolatedRate (counters
never reset). Structured queries involve no Prometheus: they read ASAPQuery's
left-open window (E - W, E], the window its SQL frontend and precompute
windows use, and pool all samples of a group: cardinality is uniqExact of
the value field, quantile is quantileExactInclusive. E is the client's
{{ end_time }} floored to a multiple of the query's interval T. Unsupported
query shapes raise.

Evaluation times: each query group sets `align_to_interval` and a
`query_time_offset` of QUERY_TIME_OFFSET_SECS, so the query client evaluates
repetition k at floor((start - O) / T) * T + k * T and sends it O seconds
later; every run and arm answers the same windows.

Run from asap-tools/experiments:
    python3 datasets/fake_metric/sketch_bench_workloads.py \
        --table <sketch-bench table.json> --seed <n> \
        --starting-delay-secs <s> --duration-secs <s>
"""
import argparse
import json
import os
import re
import shutil
import sys
import warnings
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
from experiment_utils.config import check_prebuilt_controller_output  # noqa: E402

BASELINE = constants.BASELINE_EXPERIMENT_NAME
BASELINE_MV = constants.BASELINE_MV_EXPERIMENT_NAME
SKETCHDB = constants.SKETCHDB_EXPERIMENT_NAME

OUT_CONFIG_DIR = "config/experiment_type/sketch_bench_clickhouse"
CLICKHOUSE_URL = "http://localhost:8123"
# ASAPQuery's ClickHouse adapter serves SQL here, not at ClickHouse's `/`.
ASAP_URL = "http://localhost:8088/clickhouse/query"
# Seconds after an eval time the client sends it, so data up to the eval
# time has arrived.
QUERY_TIME_OFFSET_SECS = 10
# Feed time beyond the query phase, for the feeder's startup check; the
# runner checks the total.
FEEDER_SLACK_SECS = 60
VALUE_COLUMN = "value"
# Every view lives here, so dropping it removes views any earlier workload
# attached to the metric tables; they would keep firing on inserts.
VIEW_DATABASE = "sketch_bench_views"
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
    Otherwise `series_fn` (last, rate, increase, or quantile at `quantile`)
    gives one value per series, and `agg` combines those per group (avg by
    (job) (quantile_over_time)); `agg` None keeps them per series.
    `ratio_quantile` divides the `quantile` by a second quantile of the same
    samples. `topk` keeps the k largest values, per `topk_by` group when set.
    Structured (non-PromQL) query entries map onto these fields and set
    `left_open`: their window is (end - W, end], not PromQL's closed one.
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
    topk_by: Tuple[str, ...] = ()
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


def query_from_entry(entry: dict, metrics: dict, labels: Dict[str, List[str]]) -> Query:
    """One `e2e.queries` entry: PromQL, or a structured multi-grouping query
    `{table, value, capability, grouping, quantile, window_secs,
    interval_secs}` that pools every sample of a group over the window.
    `labels` is metric_labels(metrics)."""
    if "promql" in entry:
        q = parse_promql(entry["promql"], entry["interval_secs"], labels)
        if metrics[q.table]["type"] == "records":
            raise ValueError(f"PromQL over records metric {q.table!r}: {entry}")
        return q
    table, capability = entry["table"], entry["capability"]
    if capability not in STRUCTURED_AGGS:
        raise ValueError(f"unsupported capability {capability!r}: {entry}")
    if table not in labels:
        raise ValueError(f"unknown table {table!r}: {entry}")
    unknown = set(entry["grouping"]) - set(labels[table])
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
        return " ".join(entry["promql"].split())
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
        if group:
            sql = f"SELECT {group}, {value} AS value {window} GROUP BY {group}"
        else:
            # The grand total: one row, none for an empty window.
            sql = f"SELECT {value} AS value {window} HAVING count() > 0"
    else:
        if q.series_fn in ("last", "quantile"):
            value = agg(q.series_fn, q.quantile)
            sql = f"SELECT {cols}, {value} AS value {window} GROUP BY {cols}"
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
        if q.topk_by:
            sql += f" BY {', '.join(q.topk_by)}"
    return sql


def raw_agg(stat: str, quantile: Optional[str], value: str = VALUE_COLUMN) -> str:
    if stat == "sum":
        return f"sum({value})"
    if stat == "count":
        return "count()"
    if stat == "avg":
        return f"avg({value})"
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
        # DateTime64 bounds: ClickHouse 26.10 prunes `ts > <integer>` on the
        # DateTime64 key as `ts >= <integer> + 1`, dropping the samples in
        # between from the result.
        lambda end, w: (
            f"ts {'>' if q.left_open else '>='} toDateTime64({end} - {w}, 3) "
            f"AND ts <= toDateTime64({end}, 3)"
        ),
        lambda stat, quantile: raw_agg(stat, quantile, q.value),
    )


def floored_end_datetime(interval_secs: int) -> str:
    """A Jinja template for the client's {{ end_datetime }} floored to a
    multiple of the interval T, as the ClickHouse SQL floors {{ end_time }}.

    end_datetime is UTC 'YYYY-MM-DD HH:MM:SS', and UTC timestamps are
    multiples of 60 exactly at whole minutes, so flooring rewrites the seconds
    (T divides 60) or the minutes (T divides 3600 and is a multiple of 60).
    """
    t = interval_secs
    if 60 % t == 0:
        floored = f"end_time % 60 // {t} * {t}"
        return f'{{{{ end_datetime[:17] }}}}{{{{ "%02d" | format({floored}) }}}}'
    if t % 60 == 0 and 3600 % t == 0:
        floored = f"end_time % 3600 // {t} * {t} // 60"
        return f'{{{{ end_datetime[:14] }}}}{{{{ "%02d" | format({floored}) }}}}:00'
    raise ValueError(f"cannot floor a datetime to {t}s: T must divide 60 or 3600")


def asapquery_sql(entry: dict) -> str:
    """A structured entry's `sql` with NOW() replaced by the floored eval time.

    ASAPQuery's SQL frontend takes a datetime literal but cannot floor it, so
    the literal is floored in the client's template. The literal is UTC and
    the frontend reads timezone-naive literals as server-local time, so the
    ASAPQuery server must run in UTC. The frontend maps
    `ts BETWEEN DATEADD(s, -W, E) AND E` to (start, duration) = (E - W, W) and
    answers over (E - W, E], as the ClickHouse SQL does for structured queries.
    """
    if "NOW()" not in entry.get("sql", ""):
        raise ValueError(f"structured entry without an ASAPQuery sql on NOW(): {entry}")
    end = floored_end_datetime(entry["interval_secs"])
    sql = entry["sql"].rstrip().rstrip(";")
    return sql.replace("NOW()", f"'{end}'")


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
    if q.series_fn in ("last", "quantile"):
        return q.series_fn
    return "counter"


def mv_servable(q: Query) -> bool:
    """Whether the views' T-second buckets tile q's window."""
    return q.window_secs % q.interval_secs == 0


def plan_views(queries: List[Query], metrics: dict) -> Dict[tuple, View]:
    """One view per (table, value, statistic, interval), keyed by
    view_key(). Pooled statistics keep the union of their queries' groupings
    plus every label's child_of parents, so coarser groupings roll up;
    per-series statistics, and any view a per-series query reads, keep every
    label."""
    grouped: Dict[tuple, List[Query]] = {}
    for q in queries:
        if not mv_servable(q):
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
        unknown = {p for p in parent.values() if p} - set(parent)
        if unknown:
            raise ValueError(f"{table!r}: child_of names unknown labels {unknown}")
        per_series = stat in PER_SERIES_STATS or any(q.series_fn for q in qs)
        keep = set(parent) if per_series else set()
        for name in (n for q in qs for n in q.grouping):
            while name and name not in keep:
                keep.add(name)
                name = parent[name]
        levels = {lv for q in qs for lv in (q.quantile, q.ratio_quantile) if lv}
        views[key] = View(
            name=f"{VIEW_DATABASE}.{table}_{value}_{stat}_{interval}s",
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
    select = ", ".join(
        (f"{bucket} AS bucket", f"{edge} AS edge")
        + view.keys
        + tuple(f"{expr} AS {col}" for col, expr in view_states(view))
    )
    return (
        f"CREATE MATERIALIZED VIEW {view.name} ENGINE = AggregatingMergeTree "
        f"ORDER BY ({keys}) AS SELECT {select} FROM {view.table} GROUP BY {keys}"
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
        # Buckets b in (end - w, end] hold (end - w, end]; for a closed
        # window, the edge row of bucket end - w holds its left boundary.
        # `bucket` is a DateTime, so its bounds are too (see raw_sql).
        lambda end, w: (
            f"bucket > toDateTime({end} - {w}) AND bucket <= toDateTime({end})"
            if q.left_open
            else f"bucket >= toDateTime({end} - {w}) AND bucket <= toDateTime({end}) "
            f"AND (edge OR bucket > toDateTime({end} - {w}))"
        ),
        lambda stat, quantile: mv_agg(view, stat, quantile),
    )


def init_sql(metrics: dict, views: List[View], create_views: bool) -> str:
    """The metric tables, and with `create_views` their views. A view only
    aggregates rows inserted after it exists, so this must run before any
    data arrives. VIEW_DATABASE is dropped either way, so no view left by an
    earlier baseline_mv run, of this or any other workload, feeds on the
    recreated tables."""
    statements = [f"DROP DATABASE IF EXISTS {VIEW_DATABASE}"]
    if create_views:
        statements.append(f"CREATE DATABASE {VIEW_DATABASE}")
    for table, desc in metrics.items():
        names = [label["name"] for label in desc["labels"]]
        if not names:
            raise ValueError(f"metric {table!r} has no labels")
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


def statements_text(statements: List[str]) -> str:
    return ";\n\n".join(statements) + ";\n"


def convert(
    table_path: str,
    seed: int,
    starting_delay_secs: int,
    duration_secs: int,
    samples_per_sec: Optional[int] = None,
    records_per_sec: Optional[int] = None,
    prebuilt_output_dir: Optional[str] = None,
) -> List[str]:
    """Writes the SQL files and one config per arm; returns the config paths.
    Everything is rendered before anything is written, so a table that fails
    leaves an earlier run's files intact."""
    with open(table_path) as f:
        workloads = json.load(f)["workloads"]
    if len(workloads) != 1:
        raise ValueError(f"{table_path}: expected 1 workload, got {len(workloads)}")
    e2e = workloads[0]["e2e"]
    metrics = e2e["metrics"]
    labels = metric_labels(metrics)
    stem = os.path.splitext(os.path.basename(table_path))[0]
    sql_dir = os.path.join(OUT_CONFIG_DIR, stem)
    outputs: Dict[str, str] = {}  # path -> content

    entries = [(e, query_from_entry(e, metrics, labels)) for e in e2e["queries"]]
    for entry, q in entries:
        if q.interval_secs > duration_secs:
            raise ValueError(
                f"interval {q.interval_secs}s exceeds --duration-secs "
                f"{duration_secs}: {describe(entry)}"
            )
    unservable = [describe(e) for e, q in entries if not mv_servable(q)]
    if unservable:
        print(
            f"{table_path}: no {BASELINE_MV} arm; its views need windows that "
            f"are multiples of the interval: {'; '.join(unservable)}",
            file=sys.stderr,
        )
        modes, views = [BASELINE], {}
    else:
        modes = [BASELINE, BASELINE_MV]
        views = plan_views([q for _, q in entries], metrics)
    # Each arm loads data through its own init SQL, so baseline ingest never
    # feeds the views.
    init_paths = {
        BASELINE: os.path.join(sql_dir, "init.sql"),
        BASELINE_MV: os.path.join(sql_dir, "init_mv.sql"),
    }
    for mode in modes:
        outputs[init_paths[mode]] = init_sql(
            metrics, list(views.values()), mode == BASELINE_MV
        )

    # Structured groups come first: sketchdb runs only those, and results pair
    # across modes by query index, so its queries must be baseline's first.
    groups: Dict[Tuple[bool, int], Dict[str, List[str]]] = {}
    sketchdb_arm = prebuilt_output_dir is not None
    for entry, q in entries:
        structured = "promql" not in entry
        files = groups.setdefault(
            (not structured, q.interval_secs),
            {
                m: []
                for m in modes + ([SKETCHDB] if structured and sketchdb_arm else [])
            },
        )
        comment = f"-- {describe(entry)}\n"
        files[BASELINE].append(comment + raw_sql(q, labels))
        if BASELINE_MV in files:
            files[BASELINE_MV].append(comment + mv_sql(q, views, labels))
        if structured:
            # Rendered either way, so a table without ASAPQuery SQL fails alike.
            sql = asapquery_sql(entry)
            if SKETCHDB in files:
                files[SKETCHDB].append(comment + sql)
    has_structured = any(not promql for promql, _ in groups)
    if sketchdb_arm and not has_structured:
        raise ValueError(
            f"{table_path}: --prebuilt-output-dir given, but no structured entry "
            "has ASAPQuery SQL for a sketchdb arm"
        )
    max_window = max((q.window_secs for _, q in entries), default=0)
    if starting_delay_secs < QUERY_TIME_OFFSET_SECS + max_window:
        # Same data for every arm, so arms stay comparable; absolutes are biased.
        warnings.warn(
            f"--starting-delay-secs {starting_delay_secs} < "
            f"{QUERY_TIME_OFFSET_SECS + max_window}: early windows start "
            "before ingestion"
        )

    query_groups = []
    for (promql, interval), files in sorted(groups.items()):
        name = f"interval_{interval}s" + ("" if promql else "_structured")
        sql_files = {}
        for mode, statements in files.items():
            sql_files[mode] = os.path.join(sql_dir, f"{name}_{mode}.sql")
            outputs[sql_files[mode]] = statements_text(statements)
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
                    "align_to_interval": True,
                    "query_time_offset": QUERY_TIME_OFFSET_SECS,
                },
            }
        )

    feeder = {
        "workload_table": os.path.abspath(table_path),
        "metrics": list(metrics),
        "seed": seed,
        "duration_secs": starting_delay_secs + duration_secs + FEEDER_SLACK_SECS,
    }
    if samples_per_sec is not None:
        feeder["samples_per_sec"] = samples_per_sec
    if records_per_sec is not None:
        feeder["records_per_sec"] = records_per_sec

    def experiment_params(arm_modes: List[str], init_mode: str) -> dict:
        return {
            "experiment": [
                {"mode": m, "server": "asap" if m == SKETCHDB else "clickhouse"}
                for m in arm_modes
            ],
            "servers": [{"name": "clickhouse", "url": CLICKHOUSE_URL}]
            + ([{"name": "asap", "url": ASAP_URL}] if SKETCHDB in arm_modes else []),
            "dataset": {
                "name": "custom",
                "init_sql_file": init_paths[init_mode],
                "paced_feeder": feeder,
            },
            # Every arm's SQL, so the saved config labels queries by baseline's text.
            "query_groups": query_groups,
            "data_ingestion_interval_ms": SCRAPE_INTERVAL_SECONDS * 1000,
        }

    out_paths = []
    header = f"# Generated from {table_path}\n"
    for mode in modes:
        suffix = "_mv" if mode == BASELINE_MV else ""
        out_path = os.path.join(OUT_CONFIG_DIR, f"{stem}_clickhouse{suffix}.yaml")
        outputs[out_path] = (
            "# @package experiment_params\n"
            + header
            + yaml.safe_dump(experiment_params([mode], mode), sort_keys=False)
        )
        out_paths.append(out_path)
    if sketchdb_arm:
        prebuilt_output_dir = os.path.abspath(prebuilt_output_dir)
        check_prebuilt_controller_output(prebuilt_output_dir)
        # With baseline in the same experiment, so its results score sketchdb's.
        out = {
            "experiment_params": experiment_params([BASELINE, SKETCHDB], BASELINE),
            "controller": {"prebuilt_output_dir": prebuilt_output_dir},
        }
        out_path = os.path.join(OUT_CONFIG_DIR, f"{stem}_clickhouse_sketchdb.yaml")
        outputs[out_path] = (
            "# @package _global_\n" + header + yaml.safe_dump(out, sort_keys=False)
        )
        out_paths.append(out_path)

    # Clear files an earlier run wrote, e.g. a baseline_mv or sketchdb arm or
    # an interval this table no longer has.
    shutil.rmtree(sql_dir, ignore_errors=True)
    for suffix in ("_mv", "_sketchdb"):
        stale = os.path.join(OUT_CONFIG_DIR, f"{stem}_clickhouse{suffix}.yaml")
        if os.path.exists(stale):
            os.remove(stale)
    os.makedirs(sql_dir)
    for path, content in outputs.items():
        with open(path, "w") as f:
            f.write(content)
    return out_paths


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", required=True, help="sketch-bench table .json")
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--starting-delay-secs", type=int, required=True)
    parser.add_argument(
        "--duration-secs", type=int, required=True, help="query phase length"
    )
    parser.add_argument("--samples-per-sec", type=int, help="gauges and counters")
    parser.add_argument("--records-per-sec", type=int, help="records metrics")
    parser.add_argument(
        "--prebuilt-output-dir",
        help="planner configs for the sketchdb arm (e.g. from asap-planner "
        "--planner saved); without it there is no sketchdb arm",
    )
    args = parser.parse_args()
    for path in convert(
        args.table,
        args.seed,
        args.starting_delay_secs,
        args.duration_secs,
        args.samples_per_sec,
        args.records_per_sec,
        args.prebuilt_output_dir,
    ):
        print(path)


if __name__ == "__main__":
    main()
