#!/usr/bin/env python3
"""Translate a sketch-bench table's end-to-end queries into ClickHouse configs.

A sketch-bench table's `workloads[0].e2e` block lists its queries, either
`{promql, interval_secs, rqes}` or structured multi-grouping queries, and
describes each metric (`metrics`). Every metric is one ClickHouse table named
after it: `ts DateTime64(3)`, then the metric's labels in description order as
String, then `value Float64`, or for a `records` metric one Float64 column per
value field in description order.

For a table `<stem>.json` this writes, under OUT_CONFIG_DIR:
- <stem>/init.sql: the metric tables;
- <stem>/interval_<T>s.sql: one statement per query that repeats every T s;
- <stem>/asapquery.sql: each structured entry's `sql` (ASAPQuery's SQL, as
  sketch-bench's exporter writes it) with NOW() replaced by the floored eval
  time;
- <stem>_clickhouse.yaml: a baseline-only experiment_run_clickhouse.py config
  with one query group per T, repeating every T seconds.

Semantics are those of Prometheus 2.53 (the version installation/prometheus
installs for bare-metal runs), whose windows are closed on both ends: range
vectors [d] at T read [T - d, T]; instant vectors read the latest sample of
each series in [T - 1s, T] (a lookback of one scrape interval, not
Prometheus's 5m default: the feeder writes every series every scrape, so the
two agree unless a sample is missing, where Prometheus would return the
series' last value and this SQL drops the series); quantiles use
quantileExactInclusive; rate and increase reproduce extrapolatedRate (counters
never reset). Structured queries involve no Prometheus: they read ASAPQuery's
left-open window (T - W, T], the window its SQL frontend and precompute
windows use, and pool all samples of a group: cardinality is uniqExact of
the value field, quantile is quantileExactInclusive. T is the client's
{{ end_time }} floored to a multiple of the query's interval. Unsupported
query shapes raise.

Evaluation times: each query group sets `align_to_interval` and a
`query_time_offset` of QUERY_TIME_OFFSET_SECS, so the query client evaluates
repetition k at floor((start - O) / T) * T + k * T and sends it O seconds
later; every run and arm answers the same windows.

Run from asap-tools/experiments:
    python3 datasets/fake_metric/sketch_bench_workloads.py \
        --table <sketch-bench table.json> --data-file <path> \
        --starting-delay-secs <s> --duration-secs <s>
"""
import argparse
import json
import os
import re
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

OUT_CONFIG_DIR = "config/experiment_type/sketch_bench_clickhouse"
CLICKHOUSE_URL = "http://localhost:8123"
# Seconds after an eval time the client sends it, so data up to the eval
# time has arrived.
QUERY_TIME_OFFSET_SECS = 10
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


def init_sql(metrics: dict) -> str:
    statements = []
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
) -> str:
    with open(table_path) as f:
        workloads = json.load(f)["workloads"]
    if len(workloads) != 1:
        raise ValueError(f"{table_path}: expected 1 workload, got {len(workloads)}")
    e2e = workloads[0]["e2e"]
    metrics = e2e["metrics"]
    labels = metric_labels(metrics)
    stem = os.path.splitext(os.path.basename(table_path))[0]
    sql_dir = os.path.join(OUT_CONFIG_DIR, stem)
    os.makedirs(sql_dir, exist_ok=True)

    init_path = os.path.join(sql_dir, "init.sql")
    with open(init_path, "w") as f:
        f.write(init_sql(metrics))

    by_interval: Dict[int, List[str]] = {}
    asapquery: List[str] = []
    max_window = 0
    for entry in e2e["queries"]:
        q = query_from_entry(entry, metrics, labels)
        if q.interval_secs > duration_secs:
            raise ValueError(
                f"interval {q.interval_secs}s exceeds --duration-secs "
                f"{duration_secs}: {describe(entry)}"
            )
        max_window = max(max_window, q.window_secs)
        by_interval.setdefault(q.interval_secs, []).append(
            f"-- {describe(entry)}\n{raw_sql(q, labels)}"
        )
        if "promql" not in entry:
            asapquery.append(f"-- {describe(entry)}\n{asapquery_sql(entry)}")
    if asapquery:
        # For the ASAPQuery arm; the runner does not read it.
        write_statements(os.path.join(sql_dir, "asapquery.sql"), asapquery)
    if starting_delay_secs < QUERY_TIME_OFFSET_SECS + max_window:
        # Same data for every arm, so arms stay comparable; absolutes are biased.
        warnings.warn(
            f"--starting-delay-secs {starting_delay_secs} < "
            f"{QUERY_TIME_OFFSET_SECS + max_window}: early windows start "
            "before ingestion"
        )

    query_groups = []
    for interval, statements in sorted(by_interval.items()):
        name = f"interval_{interval}s"
        sql_path = os.path.join(sql_dir, f"{name}.sql")
        write_statements(sql_path, statements)
        query_groups.append(
            {
                "name": name,
                "sql_file": {"baseline": sql_path},
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

    out = {
        "experiment": [{"mode": "baseline", "server": "clickhouse"}],
        "servers": [{"name": "clickhouse", "url": CLICKHOUSE_URL}],
        "dataset": {
            "name": "custom",
            "local_data_file": data_file,
            # The runner's loader fills only this table; nothing fills the
            # other metrics' tables yet.
            "table": next(iter(labels)),
            "init_sql_file": init_path,
            "max_rows": 0,
        },
        "query_groups": query_groups,
        "data_ingestion_interval_ms": SCRAPE_INTERVAL_SECONDS * 1000,
    }
    out_path = os.path.join(OUT_CONFIG_DIR, f"{stem}_clickhouse.yaml")
    with open(out_path, "w") as f:
        f.write(f"# @package experiment_params\n# Generated from {table_path}\n")
        yaml.safe_dump(out, f, sort_keys=False)
    return out_path


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
    print(
        convert(
            args.table, args.data_file, args.starting_delay_secs, args.duration_secs
        )
    )


if __name__ == "__main__":
    main()
