#!/usr/bin/env python3
"""Translate a sketch-bench table's end-to-end queries into ClickHouse configs.

A sketch-bench table's `workloads[0].e2e` block lists its queries as
`{promql, interval_secs, rqes}` and describes each metric (`metrics`). Every
metric is one ClickHouse table named after it: `ts DateTime64(3)`, then the
metric's labels in description order as String, then `value Float64`.

For a table `<stem>.json` this writes, under OUT_CONFIG_DIR:
- <stem>/init.sql: the metric tables;
- <stem>/interval_<T>s.sql: one statement per query that repeats every T s;
- <stem>_clickhouse.yaml: a baseline-only experiment_run_clickhouse.py config
  with one query group per T, repeating every T seconds.

Semantics are generate_workloads.py's (Prometheus 2.53, left-open windows):
range vectors [d] at T read (T - d, T]; instant vectors read the latest sample
of each series in (T - 1s, T]; quantiles use quantileExactInclusive; rate and
increase reproduce extrapolatedRate (counters never reset). T is the client's
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
VALUE_COLUMN = "value"
# Instant vectors read the latest sample of each series in the last scrape.
INSTANT_WINDOW_SECONDS = SCRAPE_INTERVAL_SECONDS

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
    samples. Structured (non-PromQL) query entries map onto these fields.
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


def query_from_entry(entry: dict, labels: Dict[str, List[str]]) -> Query:
    """One `e2e.queries` entry. Entries are PromQL; structured entries (no
    `promql`) are not supported yet and raise."""
    if "promql" not in entry:
        raise ValueError(f"query entry without promql is not supported: {entry}")
    return parse_promql(entry["promql"], entry["interval_secs"], labels)


def eval_time(interval_secs: int) -> str:
    """The client's {{ end_time }} floored to a multiple of the interval."""
    return f"intDiv({END}, {interval_secs}) * {interval_secs}"


def render(
    q: Query, series: Tuple[str, ...], source: str, time_col: str, agg: Callable
) -> str:
    """SQL for `q` reading `source` rows with `time_col` in the window.

    `series` is the table's full label list; `agg(stat, quantile)` gives the
    aggregate expression of a statistic over the source rows: sum, count,
    quantile, last (per-series latest value) or counter (the select list
    rate_from_samples reads).
    """
    end = eval_time(q.interval_secs)
    window = (
        f"FROM {source} WHERE {time_col} > {end} - {q.window_secs} "
        f"AND {time_col} <= {end}"
    )
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
        "ts",
        lambda stat, quantile: raw_agg(stat, quantile, q.value),
    )


def init_sql(labels: Dict[str, List[str]]) -> str:
    statements = []
    for table, names in labels.items():
        columns = ", ".join(f"{name} String" for name in names)
        statements += [
            f"DROP TABLE IF EXISTS {table}",
            f"CREATE TABLE {table} (ts DateTime64(3), {columns}, "
            f"{VALUE_COLUMN} Float64) ENGINE = MergeTree "
            f"ORDER BY ({', '.join(names)}, ts)",
        ]
    return ";\n\n".join(statements) + ";\n"


def metric_labels(e2e: dict) -> Dict[str, List[str]]:
    return {
        name: [label["name"] for label in desc["labels"]]
        for name, desc in e2e["metrics"].items()
    }


def convert(
    table_path: str, data_file: str, starting_delay_secs: int, duration_secs: int
) -> str:
    with open(table_path) as f:
        (workload,) = json.load(f)["workloads"]
    e2e = workload["e2e"]
    labels = metric_labels(e2e)
    stem = os.path.splitext(os.path.basename(table_path))[0]
    sql_dir = os.path.join(OUT_CONFIG_DIR, stem)
    os.makedirs(sql_dir, exist_ok=True)

    init_path = os.path.join(sql_dir, "init.sql")
    with open(init_path, "w") as f:
        f.write(init_sql(labels))

    by_interval: Dict[int, List[str]] = {}
    for entry in e2e["queries"]:
        q = query_from_entry(entry, labels)
        by_interval.setdefault(q.interval_secs, []).append(
            f"-- {entry['promql']}\n{raw_sql(q, labels)}"
        )

    query_groups = []
    for interval, statements in sorted(by_interval.items()):
        name = f"interval_{interval}s"
        sql_path = os.path.join(sql_dir, f"{name}.sql")
        with open(sql_path, "w") as f:
            f.write(";\n\n".join(statements) + ";\n")
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
                },
            }
        )

    out = {
        "experiment": [{"mode": "baseline", "server": "clickhouse"}],
        "servers": [{"name": "clickhouse", "url": CLICKHOUSE_URL}],
        "dataset": {
            "name": "custom",
            "local_data_file": data_file,
            # The runner's loader fills one table; the paced feeder writes all.
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
