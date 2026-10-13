#!/usr/bin/env python3
"""Translate generated2 PromQL workloads into ClickHouse experiment configs.

For each generated2 workload yaml (Prometheus variants only), writes a
baseline-only ClickHouse experiment config plus one templated SQL file per
query group under config/experiment_type/generated2_clickhouse/ (gitignored,
like its generated2 inputs). Query groups keep their
original delays, repetitions and query_time_offset; the client fills in
{{ end_time }} (= T, the PromQL evaluation time) per repetition.

Semantics follow Prometheus 2.53 with these windows:
- instant vector at T: samples in (T - SCRAPE_INTERVAL_SECONDS, T], i.e. the
  one scrape at T;
- range vector [d] at T: samples in (T - d, T].
rate/increase reproduce extrapolatedRate. Only the query shapes that appear
in generated2 are supported; anything else raises.

Run from asap-tools/experiments:
    python3 datasets/fake_metric/generate_workloads.py \
        --src-dir config/experiment_type/generated2 --data-file <jsonl.gz>
"""
import argparse
import glob
import os
import re
from typing import List

import yaml

from generate_data import DATA_START

SCRAPE_INTERVAL_SECONDS = 1  # config/config.yaml prometheus.scrape_interval
OUT_CONFIG_DIR = "config/experiment_type/generated2_clickhouse"
INIT_SQL = "datasets/fake_metric/init.sql"
END = "{{ end_time }}"

DURATION_UNITS = {"s": 1, "m": 60, "h": 3600}
RANGE = r"(\w+)\[(\d+)([smh])\]"
SHAPES = [
    ("instant_agg", re.compile(r"^(count|sum) by \((\w+)\) \((\w+)\)$")),
    ("instant_quantile", re.compile(r"^quantile by \((\w+)\) \(([\d.]+), (\w+)\)$")),
    (
        "agg_over_time",
        re.compile(rf"^sum by \((\w+)\) \((sum|count)_over_time\({RANGE}\)\)$"),
    ),
    ("over_time", re.compile(rf"^(sum|count)_over_time\({RANGE}\)$")),
    ("quantile_over_time", re.compile(rf"^quantile_over_time\(([\d.]+), {RANGE}\)$")),
    ("rate", re.compile(rf"^(rate|increase)\({RANGE}\)$")),
]
AGG_SQL = {"sum": "sum(value)", "count": "count()"}


def window_seconds(amount: str, unit: str) -> int:
    return int(amount) * DURATION_UNITS[unit]


def select(cols: str, agg: str, table: str, window: int) -> str:
    return (
        f"SELECT {cols}, {agg} AS value FROM {table} "
        f"WHERE ts > {END} - {window} AND ts <= {END} GROUP BY {cols}"
    )


def extrapolated_rate(cols: str, table: str, window: int, is_rate: bool) -> str:
    """Prometheus 2.53 extrapolatedRate, isCounter=true."""
    # ponytail: no counter-reset correction, fake_exporter counters only grow
    samples = (
        f"SELECT {cols}, argMin(value, ts) AS first_v, argMax(value, ts) AS last_v, "
        f"toFloat64(toUnixTimestamp(min(ts))) AS first_t, "
        f"toFloat64(toUnixTimestamp(max(ts))) AS last_t, count() AS n "
        f"FROM {table} WHERE ts > {END} - {window} AND ts <= {END} "
        f"GROUP BY {cols} HAVING n >= 2"
    )
    return rate_from_samples(cols, samples, END, window, is_rate)


def rate_from_samples(
    cols: str, samples: str, end: str, window: int, is_rate: bool
) -> str:
    """extrapolatedRate over `samples`, a query yielding per series `cols`,
    first_v, last_v, first_t, last_t (unix seconds) and n >= 2 samples in
    (end - window, end]."""
    durations = (
        f"SELECT {cols}, last_v - first_v AS delta, first_v, "
        f"last_t - first_t AS sampled, sampled / (n - 1) AS avg_step, "
        f"if(first_t - ({end} - {window}) >= 1.1 * avg_step, avg_step / 2, "
        f"first_t - ({end} - {window})) AS to_start_clamped, "
        f"if(delta > 0 AND first_v >= 0, "
        f"least(to_start_clamped, sampled * first_v / delta), to_start_clamped) AS to_start, "
        f"if({end} - last_t >= 1.1 * avg_step, avg_step / 2, {end} - last_t) AS to_end "
        f"FROM ({samples})"
    )
    per_second = f" / {window}" if is_rate else ""
    return (
        f"SELECT {cols}, delta * (sampled + to_start + to_end) / sampled{per_second} "
        f"AS value FROM ({durations})"
    )


def translate(promql: str, metric: str, series_cols: str) -> str:
    query = " ".join(promql.split())
    for shape, pattern in SHAPES:
        m = pattern.match(query)
        if not m:
            continue
        g = m.groups()
        if shape == "instant_agg":
            fn, by, name = g
            agg, cols, window = AGG_SQL[fn], by, SCRAPE_INTERVAL_SECONDS
        elif shape == "instant_quantile":
            by, q, name = g
            agg, cols, window = (
                f"quantileExactInclusive({q})(value)",
                by,
                SCRAPE_INTERVAL_SECONDS,
            )
        elif shape == "agg_over_time":
            by, fn, name, amount, unit = g
            agg, cols, window = AGG_SQL[fn], by, window_seconds(amount, unit)
        elif shape == "over_time":
            fn, name, amount, unit = g
            agg, cols, window = AGG_SQL[fn], series_cols, window_seconds(amount, unit)
        elif shape == "quantile_over_time":
            q, name, amount, unit = g
            agg, cols, window = (
                f"quantileExactInclusive({q})(value)",
                series_cols,
                window_seconds(amount, unit),
            )
        else:
            fn, name, amount, unit = g
            if name != metric:
                raise ValueError(f"unexpected metric {name!r} in {promql!r}")
            return extrapolated_rate(
                series_cols, metric, window_seconds(amount, unit), fn == "rate"
            )
        if name != metric:
            raise ValueError(f"unexpected metric {name!r} in {promql!r}")
        return select(cols, agg, metric, window)
    raise ValueError(f"unsupported PromQL shape: {promql!r}")


def convert(src_path: str, data_file: str) -> str:
    with open(src_path) as f:
        src = yaml.safe_load(f)
    (metric_cfg,) = src["metrics"]
    metric = metric_cfg["metric"]
    series_cols = ", ".join(metric_cfg["labels"])
    stem = os.path.splitext(os.path.basename(src_path))[0]
    sql_dir = os.path.join(OUT_CONFIG_DIR, stem)
    os.makedirs(sql_dir, exist_ok=True)

    query_groups = []
    for group in src["query_groups"]:
        sql_path = os.path.join(sql_dir, f"group_{group['id']}.sql")
        statements: List[str] = [
            f"-- {q}\n{translate(q, metric, series_cols)}" for q in group["queries"]
        ]
        with open(sql_path, "w") as f:
            f.write(";\n\n".join(statements) + ";\n")
        query_groups.append(
            {
                "name": f"group_{group['id']}",
                "sql_file": {"baseline": sql_path},
                "repetition_delay_ms": group["repetition_delay_ms"],
                # Queries use only {{ end_time }}; the client still requires a value.
                "time_window_seconds": 0,
                "client_options": {
                    **group["client_options"],
                    "simulated_start_time": DATA_START,
                },
                "controller_options": group["controller_options"],
            }
        )

    out = {
        "experiment": [{"mode": "baseline", "server": "clickhouse"}],
        "servers": [{"name": "clickhouse", "url": "http://localhost:8123"}],
        "dataset": {
            "name": "custom",
            "local_data_file": data_file,
            "table": metric,
            "init_sql_file": INIT_SQL,
            "max_rows": 0,
        },
        "query_groups": query_groups,
        "data_ingestion_interval_ms": SCRAPE_INTERVAL_SECONDS * 1000,
    }
    out_path = os.path.join(OUT_CONFIG_DIR, f"{stem}_clickhouse.yaml")
    with open(out_path, "w") as f:
        f.write(f"# @package experiment_params\n# Generated from {src_path}\n")
        yaml.safe_dump(out, f, sort_keys=False)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src-dir", required=True, help="generated2 config dir")
    parser.add_argument(
        "--data-file",
        required=True,
        help="generate_data.py output; path as seen from asap-tools/experiments",
    )
    args = parser.parse_args()
    sources = sorted(
        p
        for p in glob.glob(os.path.join(args.src_dir, "*.yaml"))
        if not p.endswith("_victoriametrics.yaml")
    )
    for src in sources:
        print(convert(src, args.data_file))


if __name__ == "__main__":
    main()
