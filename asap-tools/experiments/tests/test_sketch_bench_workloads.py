"""Tests for datasets/fake_metric/sketch_bench_workloads.py.

ClickHouseSemanticsTest runs every generated statement in clickhouse-local
over a small dataset and compares it with a separate Python implementation of
the Prometheus semantics. It needs a `clickhouse` binary on PATH (or
CLICKHOUSE_BINARY) and is skipped otherwise.
"""

import contextlib
import datetime
import io
import itertools
import json
import math
import os
import random
import shutil
import subprocess
import sys
import tempfile
import unittest
import warnings
from typing import Callable, Dict, List, Tuple

import jinja2
import yaml

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "datasets", "fake_metric")
)

import sketch_bench_workloads as sbw  # noqa: E402

SERIES_LABELS = [{"name": "job"}, {"name": "label_0", "child_of": "job"}]
HTTP_LABELS = ["region", "service", "endpoint", "status"]
METRICS = {
    "data": {"type": "gauge", "labels": SERIES_LABELS},
    "data_total": {"type": "counter", "labels": SERIES_LABELS},
    "http": {
        "type": "records",
        "labels": [
            {"name": "region"},
            {"name": "service"},
            {"name": "endpoint", "child_of": "service"},
            {"name": "status"},
        ],
        "values": {"latency": {}, "user_id": {}},
    },
}
LABELS = sbw.metric_labels(METRICS)
CLICKHOUSE = os.environ.get("CLICKHOUSE_BINARY") or shutil.which("clickhouse")

# The PromQL of sketch-bench's ten classic templates (windows vary per table).
CLASSIC = [
    (1, "sum by (job) (data)"),
    (60, "topk(32, sum by (label_0) (sum_over_time(data[15m])))"),
    (1, "quantile by (job) (0.9, data)"),
    (60, "sum_over_time(data[15m])"),
    (60, "quantile_over_time(0.99, data[15m])"),
    (60, "rate(data_total[15m])"),
    (60, "sum by (job) (rate(data_total[15m]))"),
    (60, "sum by (job) (sum_over_time(data[15m]))"),
    (60, "topk(32, sum by (label_0) (rate(data_total[15m])))"),
    (60, "quantile_over_time(0.9, data[15m]) / quantile_over_time(0.5, data[15m])"),
]


class ParseTest(unittest.TestCase):
    def test_classic_templates(self):
        got = [sbw.parse_promql(p, t, LABELS) for t, p in CLASSIC]
        series = ("job", "label_0")
        self.assertEqual(got[0], sbw.Query("data", 1, 1, ("job",), "last", "sum"))
        self.assertEqual(
            got[1], sbw.Query("data", 60, 900, ("label_0",), None, "sum", topk=32)
        )
        self.assertEqual(
            got[2], sbw.Query("data", 1, 1, ("job",), "last", "quantile", "0.9")
        )
        self.assertEqual(got[3], sbw.Query("data", 60, 900, series, None, "sum"))
        self.assertEqual(
            got[4], sbw.Query("data", 60, 900, series, None, "quantile", "0.99")
        )
        self.assertEqual(got[5], sbw.Query("data_total", 60, 900, series, "rate", None))
        self.assertEqual(
            got[6], sbw.Query("data_total", 60, 900, ("job",), "rate", "sum")
        )
        self.assertEqual(got[7], sbw.Query("data", 60, 900, ("job",), None, "sum"))
        self.assertEqual(
            got[8],
            sbw.Query("data_total", 60, 900, ("label_0",), "rate", "sum", topk=32),
        )
        self.assertEqual(
            got[9], sbw.Query("data", 60, 900, series, None, "quantile", "0.9", "0.5")
        )

    def test_metrics_m_names(self):
        labels = {"data_3": ["job", "label_0"], "data_3_total": ["job", "label_0"]}
        q = sbw.parse_promql("sum by (job) (rate(data_3_total[1h]))", 60, labels)
        self.assertEqual((q.table, q.window_secs), ("data_3_total", 3600))

    def test_unsupported_shapes_raise(self):
        for promql in [
            "avg by (job) (data)",
            "max_over_time(data[1m])",
            # Not poolable: a count of per-series sums or a sum of quantiles.
            "count by (job) (sum_over_time(data[1m]))",
            "sum by (job) (quantile_over_time(0.5, data[1m]))",
            "sum by (job) (quantile_over_time(0.9, data[1m]) / "
            "quantile_over_time(0.5, data[1m]))",
            "quantile by (job) (0.5, sum_over_time(data[1m]))",
            "quantile_over_time(0.9, data[1m]) / quantile_over_time(0.5, data[2m])",
            "topk(3, topk(2, data))",
            "sum by (instance) (data)",
            "rate(other_total[1m])",
        ]:
            with self.subTest(promql=promql):
                with self.assertRaises(ValueError):
                    sbw.parse_promql(promql, 60, LABELS)

    def test_structured_entries(self):
        cardinality = mg_entry("cardinality", ["region", "service"], 300)
        quantile = mg_entry("quantile", ["service"], 900)
        self.assertEqual(
            sbw.query_from_entry(cardinality, METRICS, LABELS),
            sbw.Query(
                "http",
                60,
                300,
                ("region", "service"),
                None,
                "cardinality",
                value="user_id",
                left_open=True,
            ),
        )
        self.assertEqual(
            sbw.query_from_entry(quantile, METRICS, LABELS),
            sbw.Query(
                "http",
                60,
                900,
                ("service",),
                None,
                "quantile",
                "0.99",
                value="latency",
                left_open=True,
            ),
        )

    def test_unsupported_structured_entries_raise(self):
        for entry in [
            {**mg_entry("cardinality", ["region"], 300), "capability": "sum"},
            mg_entry("cardinality", ["instance"], 300),
            {**mg_entry("quantile", ["region"], 300), "value": "bytes"},
            {**mg_entry("quantile", ["region"], 300), "table": "nope"},
            # records tables have no `value` column.
            {"promql": "sum_over_time(http[1m])", "interval_secs": 60, "rqes": []},
        ]:
            with self.subTest(entry=entry), self.assertRaises(ValueError):
                sbw.query_from_entry(entry, METRICS, LABELS)

    def test_asapquery_sql_without_now_raises(self):
        entry = mg_entry("quantile", ["service"], 300)
        entry["sql"] = entry["sql"].replace("NOW()", "now()")
        with self.assertRaises(ValueError):
            sbw.asapquery_sql(entry)

    def test_asapquery_sql_strips_trailing_semicolon(self):
        entry = mg_entry("quantile", ["service"], 300)
        plain = sbw.asapquery_sql(entry)
        entry["sql"] += "; \n"
        self.assertEqual(sbw.asapquery_sql(entry), plain)

    def test_unknown_child_of_raises(self):
        metrics = {
            "data": {"type": "gauge", "labels": [{"name": "job", "child_of": "dc"}]}
        }
        q = sbw.parse_promql("sum_over_time(data[1m])", 60, {"data": ["job"]})
        with self.assertRaises(ValueError):
            sbw.plan_views([q], metrics)

    def test_metric_without_labels_raises(self):
        with self.assertRaises(ValueError):
            sbw.init_sql({"data": {"type": "gauge", "labels": []}}, [], False)

    def test_describe_is_one_line(self):
        entry = {"promql": "sum by (job) (\n  data\n)", "interval_secs": 1}
        self.assertEqual(sbw.describe(entry), "sum by (job) ( data )")


def mg_entry(capability: str, grouping: List[str], window_secs: int) -> dict:
    """A structured multi-grouping entry over `http`, as sketch-bench writes it."""
    entry = {
        "table": "http",
        "value": "latency" if capability == "quantile" else "user_id",
        "capability": capability,
        "grouping": grouping,
        "window_secs": window_secs,
        "interval_secs": 60,
        "rqes": [],
    }
    agg = "COUNT(DISTINCT user_id)"
    if capability == "quantile":
        entry["quantile"] = 0.99
        agg = "QUANTILE(0.99, latency)"
    group = ", ".join(grouping)
    entry["sql"] = (
        f"SELECT {group + ', ' if group else ''}{agg} FROM http WHERE ts BETWEEN "
        f"DATEADD(s, -{window_secs}, NOW()) AND NOW()"
        + (f" GROUP BY {group}" if group else "")
    )
    return entry


def snapshot(root: str) -> Dict[str, str]:
    """Every file under `root`, by path, with its content."""
    out = {}
    for d, _, names in os.walk(root):
        for name in names:
            with open(os.path.join(d, name)) as f:
                out[os.path.join(d, name)] = f.read()
    return out


class PlanViewsTest(unittest.TestCase):
    def views(self, cases):
        queries = [sbw.parse_promql(p, t, LABELS) for t, p in cases]
        return sorted(sbw.plan_views(queries, METRICS).values(), key=lambda v: v.name)

    def test_classic_templates(self):
        series = ("job", "label_0")
        db = "sketch_bench_views."
        self.assertEqual(
            [(v.name, v.keys, v.levels) for v in self.views(CLASSIC)],
            [
                (db + "data_total_value_counter_60s", series, ()),
                (db + "data_value_last_1s", series, ()),
                (db + "data_value_quantile_60s", series, ("0.5", "0.9", "0.99")),
                (db + "data_value_sum_60s", series, ()),
            ],
        )

    def test_keys_are_union_of_groupings_with_parents(self):
        by_label_0 = (60, "sum by (label_0) (sum_over_time(data[1m]))")
        by_job = (60, "sum by (job) (sum_over_time(data[1m]))")
        self.assertEqual(self.views([by_job])[0].keys, ("job",))
        self.assertEqual(self.views([by_label_0])[0].keys, ("job", "label_0"))

    def test_window_not_multiple_of_interval_raises(self):
        for case in [
            (60, "sum_over_time(data[90s])"),
            # An instant vector reads [E - 1s, E]: bucket-aligned only when T = 1 s.
            (60, "sum by (job) (data)"),
        ]:
            with self.subTest(case=case), self.assertRaises(ValueError):
                self.views([case])


class FlooredEndDatetimeTest(unittest.TestCase):
    def test_renders_floored_utc_datetime(self):
        env = jinja2.Environment()
        for t in (1, 15, 60, 300, 3600):
            template = env.from_string(sbw.floored_end_datetime(t))
            for end in (1767229237, 1767229200, 1767225599, 1767232799):
                with self.subTest(t=t, end=end):
                    # As the client's TimeRange.end_datetime formats it.
                    got = template.render(
                        end_time=end,
                        end_datetime=datetime.datetime.fromtimestamp(
                            end, tz=datetime.timezone.utc
                        ).strftime("%Y-%m-%d %H:%M:%S"),
                    )
                    want = datetime.datetime.fromtimestamp(
                        end // t * t, tz=datetime.timezone.utc
                    )
                    self.assertEqual(got, want.strftime("%Y-%m-%d %H:%M:%S"))

    def test_unfloorable_interval_raises(self):
        for t in (7, 420, 7200):
            with self.subTest(t=t), self.assertRaises(ValueError):
                sbw.floored_end_datetime(t)


class ConvertTest(unittest.TestCase):
    def test_writes_config_grouped_by_interval(self):
        e2e = {
            "metrics": METRICS,
            "queries": [
                {"promql": p, "interval_secs": t, "rqes": []} for t, p in CLASSIC
            ]
            + [mg_entry("quantile", ["service"], 300)],
        }
        with tempfile.TemporaryDirectory() as tmp:
            table = os.path.join(tmp, "classic.json")
            with open(table, "w") as f:
                json.dump({"workloads": [{"e2e": e2e}]}, f)
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                # 30 s < offset + the 15m windows: early windows predate data.
                with self.assertWarns(UserWarning):
                    out_paths = sbw.convert(table, "data.json", 30, 600)
                cfgs = {}
                for path in out_paths:
                    with open(path) as f:
                        cfg = yaml.safe_load(f)
                    (arm,) = cfg["experiment"]
                    with open(cfg["dataset"]["init_sql_file"]) as f:
                        cfgs[arm["mode"]] = (cfg, f.read())
                self.assertEqual(sorted(cfgs), ["baseline", "baseline_mv"])
                cfg, init_mv = cfgs["baseline_mv"]
                _, init = cfgs["baseline"]
                groups = {g["name"]: g for g in cfg["query_groups"]}
                self.assertEqual(sorted(groups), ["interval_1s", "interval_60s"])
                g60 = groups["interval_60s"]
                self.assertEqual(g60["repetition_delay_ms"], 60000)
                self.assertEqual(
                    g60["client_options"],
                    {
                        "starting_delay": 30,
                        "repetitions": 10,
                        "align_to_interval": True,
                        "query_time_offset": 10,
                    },
                )
                self.assertEqual(
                    groups["interval_1s"]["client_options"]["repetitions"], 600
                )
                self.assertEqual(sorted(g60["sql_file"]), ["baseline", "baseline_mv"])
                for path in g60["sql_file"].values():
                    with open(path) as f:
                        statements = [s for s in f.read().split(";") if s.strip()]
                    self.assertEqual(len(statements), 9)
                self.assertIn(
                    "endpoint String, status String, latency Float64, "
                    "user_id Float64)",
                    init,
                )
                # Baseline ingest feeds no views, and drops any an earlier
                # baseline_mv run left attached to the tables.
                self.assertNotIn("CREATE MATERIALIZED VIEW", init)
                self.assertIn("DROP DATABASE IF EXISTS sketch_bench_views", init)
                # Views only see rows inserted after them: tables, then views.
                self.assertLess(
                    init_mv.index("CREATE TABLE data_total (ts DateTime64(3)"),
                    init_mv.index(
                        "CREATE MATERIALIZED VIEW "
                        "sketch_bench_views.data_total_value_counter_60s"
                    ),
                )
                sql_dir = os.path.dirname(g60["sql_file"]["baseline"])
                # The table's sql as written, with only NOW() substituted.
                end = "'" + sbw.floored_end_datetime(60) + "'"
                with open(os.path.join(sql_dir, "asapquery.sql")) as f:
                    self.assertEqual(
                        f.read(),
                        "-- quantile(0.99, latency) by (service) over 300s\n"
                        + e2e["queries"][-1]["sql"].replace("NOW()", end)
                        + ";\n",
                    )
            finally:
                os.chdir(cwd)

    def test_unservable_query_skips_mv_arm(self):
        # An instant vector at E with T = 60 s reads [E - 1s, E], not whole
        # buckets.
        servable = {
            "promql": "sum_over_time(data[1m])",
            "interval_secs": 60,
            "rqes": [],
        }
        instant = {"promql": "sum by (job) (data)", "interval_secs": 60, "rqes": []}
        with tempfile.TemporaryDirectory() as tmp:
            table = os.path.join(tmp, "instant.json")
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                # An earlier run of the same table that had a baseline_mv arm
                # and a 1 s group; none of its files may survive.
                with open(table, "w") as f:
                    e2e = {
                        "metrics": METRICS,
                        "queries": [
                            servable,
                            {
                                **servable,
                                "interval_secs": 1,
                                "promql": "sum_over_time(data[1s])",
                            },
                        ],
                    }
                    json.dump({"workloads": [{"e2e": e2e}]}, f)
                with self.assertWarns(UserWarning):
                    sbw.convert(table, "data.json", 30, 600)
                with open(table, "w") as f:
                    e2e = {"metrics": METRICS, "queries": [instant, servable]}
                    json.dump({"workloads": [{"e2e": e2e}]}, f)
                with contextlib.redirect_stderr(io.StringIO()) as err:
                    (path,) = sbw.convert(table, "data.json", 30, 600)
                self.assertIn("no baseline_mv arm", err.getvalue())
                self.assertIn("sum by (job) (data)", err.getvalue())
                with open(path) as f:
                    cfg = yaml.safe_load(f)
                self.assertEqual(
                    cfg["experiment"], [{"mode": "baseline", "server": "clickhouse"}]
                )
                (group,) = cfg["query_groups"]
                self.assertEqual(list(group["sql_file"]), ["baseline"])
                with open(group["sql_file"]["baseline"]) as f:
                    self.assertEqual(f.read().count(";"), 2)
                with open(cfg["dataset"]["init_sql_file"]) as f:
                    self.assertNotIn("MATERIALIZED VIEW", f.read())
                sql_dir = os.path.dirname(cfg["dataset"]["init_sql_file"])
                self.assertEqual(
                    sorted(os.listdir(sql_dir)),
                    ["init.sql", "interval_60s_baseline.sql"],
                )
                self.assertEqual(
                    sorted(os.listdir(sbw.OUT_CONFIG_DIR)),
                    ["instant", "instant_clickhouse.yaml"],
                )
            finally:
                os.chdir(cwd)

    def test_failed_reconvert_keeps_earlier_files(self):
        # Cleared before validating, a failing re-run left the earlier config
        # pointing at deleted SQL files.
        good = mg_entry("quantile", ["service"], 300)
        bad = {**good, "sql": good["sql"].replace("NOW()", "now()")}
        with tempfile.TemporaryDirectory() as tmp:
            table = os.path.join(tmp, "t.json")
            cwd = os.getcwd()
            os.chdir(tmp)
            try:

                def convert(queries):
                    with open(table, "w") as f:
                        e2e = {"metrics": METRICS, "queries": queries}
                        json.dump({"workloads": [{"e2e": e2e}]}, f)
                    return sbw.convert(table, "data.json", 310, 600)

                self.assertEqual(len(convert([good])), 2)
                before = snapshot(sbw.OUT_CONFIG_DIR)
                # Fails while rendering, after validation and the init SQL.
                with self.assertRaises(ValueError):
                    convert([good, bad])
                self.assertEqual(snapshot(sbw.OUT_CONFIG_DIR), before)
            finally:
                os.chdir(cwd)

    def test_checks_workloads_duration_and_delay(self):
        e2e = {"metrics": METRICS, "queries": [mg_entry("quantile", ["service"], 300)]}
        with tempfile.TemporaryDirectory() as tmp:
            table = os.path.join(tmp, "t.json")
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                with open(table, "w") as f:
                    json.dump({"workloads": [{"e2e": e2e}, {"e2e": e2e}]}, f)
                with self.assertRaises(ValueError):
                    sbw.convert(table, "data.json", 310, 600)
                with open(table, "w") as f:
                    json.dump({"workloads": [{"e2e": e2e}]}, f)
                # Interval 60 s, so 30 s would yield 0 repetitions.
                with self.assertRaises(ValueError):
                    sbw.convert(table, "data.json", 310, 30)
                # Delay covers offset + window: no warning.
                with warnings.catch_warnings():
                    warnings.simplefilter("error")
                    sbw.convert(table, "data.json", 310, 600)
            finally:
                os.chdir(cwd)


# --- Python reference of the Prometheus semantics -------------------------

Series = Tuple[str, ...]
Samples = List[Tuple[int, float]]  # (ts ms, value), ascending
Vector = Dict[Series, float]
DATA_START_MS = 1767225600 * 1000
INSTANT_MS = 1000
BRANCHES_HIT = set()


def in_window(samples: Samples, t_ms: int, w_ms: int) -> Samples:
    # Prometheus 2.x selects [t - w, t]; 3.x made it left-open (t - w, t].
    return [(t, v) for t, v in samples if t_ms - w_ms <= t <= t_ms]


def prom_quantile(q: float, values: List[float]) -> float:
    values = sorted(values)
    rank = q * (len(values) - 1)
    lower = max(0, math.floor(rank))
    upper = min(len(values) - 1, lower + 1)
    weight = rank - math.floor(rank)
    return values[lower] * (1 - weight) + values[upper] * weight


def extrapolated_rate(s: Samples, t_ms: int, w_ms: int, is_rate: bool) -> float:
    """Prometheus 2.53 extrapolatedRate for a counter without resets."""
    first_t, first_v = s[0]
    last_t, last_v = s[-1]
    result = last_v - first_v
    to_start = (first_t - (t_ms - w_ms)) / 1000
    to_end = (t_ms - last_t) / 1000
    sampled = (last_t - first_t) / 1000
    avg = sampled / (len(s) - 1)
    if to_start >= avg * 1.1:
        BRANCHES_HIT.add("start_avg")
        to_start = avg / 2
    else:
        BRANCHES_HIT.add("start_boundary")
    if result > 0 and first_v >= 0 and sampled * first_v / result < to_start:
        BRANCHES_HIT.add("zero_clamp")
        to_start = sampled * first_v / result
    if to_end >= avg * 1.1:
        BRANCHES_HIT.add("end_avg")
        to_end = avg / 2
    else:
        BRANCHES_HIT.add("end_boundary")
    factor = (sampled + to_start + to_end) / sampled
    return result * factor / (w_ms / 1000 if is_rate else 1)


def range_fn(data, t_ms: int, w_ms: int, fn: Callable, min_n: int = 1) -> Vector:
    out = {}
    for series, samples in data.items():
        window = in_window(samples, t_ms, w_ms)
        if len(window) >= min_n:
            out[series] = fn(window)
    return out


def instant(data, t_ms: int) -> Vector:
    return range_fn(data, t_ms, INSTANT_MS, lambda w: w[-1][1])


def rate(data, t_ms, w_ms, is_rate=True) -> Vector:
    return range_fn(
        data, t_ms, w_ms, lambda w: extrapolated_rate(w, t_ms, w_ms, is_rate), 2
    )


def agg_by(vec: Vector, idx: List[int], fn: Callable) -> Vector:
    groups: Dict[Series, List[float]] = {}
    for series, v in vec.items():
        groups.setdefault(tuple(series[i] for i in idx), []).append(v)
    return {k: fn(vs) for k, vs in groups.items()}


def topk(k: int, vec: Vector) -> Vector:
    return dict(sorted(vec.items(), key=lambda kv: (-kv[1], kv[0]))[:k])


def values(w: Samples) -> List[float]:
    return [v for _, v in w]


JOB, LABEL_0 = [0], [1]
W2, W5 = 120_000, 300_000


def q_ratio(t, gauge):
    return {
        s: prom_quantile(0.9, values(w)) / prom_quantile(0.5, values(w))
        for s, w in range_fn(gauge, t, W2, lambda w: w).items()
    }


# (interval secs, PromQL, reference over (gauge, counter) at eval time t ms)
CASES = [
    (1, "sum by (job) (data)", lambda t, g, c: agg_by(instant(g, t), JOB, sum)),
    (1, "count by (job) (data)", lambda t, g, c: agg_by(instant(g, t), JOB, len)),
    *[
        (
            1,
            f"quantile by (job) ({q}, data)",
            lambda t, g, c, q=q: agg_by(
                instant(g, t), JOB, lambda vs: prom_quantile(q, vs)
            ),
        )
        for q in (0.5, 0.99)
    ],
    (
        60,
        "topk(4, sum by (label_0) (sum_over_time(data[2m])))",
        lambda t, g, c: topk(
            4, agg_by(range_fn(g, t, W2, lambda w: sum(values(w))), LABEL_0, sum)
        ),
    ),
    (
        60,
        "sum_over_time(data[5m])",
        lambda t, g, c: range_fn(g, t, W5, lambda w: sum(values(w))),
    ),
    (60, "count_over_time(data[2m])", lambda t, g, c: range_fn(g, t, W2, len)),
    *[
        (
            60,
            f"quantile_over_time({q}, data[2m])",
            lambda t, g, c, q=q: range_fn(
                g, t, W2, lambda w: prom_quantile(q, values(w))
            ),
        )
        for q in (0.5, 0.9, 0.99)
    ],
    (60, "rate(data_total[2m])", lambda t, g, c: rate(c, t, W2)),
    (60, "increase(data_total[5m])", lambda t, g, c: rate(c, t, W5, False)),
    (
        60,
        "sum by (job) (rate(data_total[2m]))",
        lambda t, g, c: agg_by(rate(c, t, W2), JOB, sum),
    ),
    (
        60,
        "sum by (job) (sum_over_time(data[2m]))",
        lambda t, g, c: agg_by(range_fn(g, t, W2, lambda w: sum(values(w))), JOB, sum),
    ),
    (
        60,
        "sum by (job) (count_over_time(data[2m]))",
        lambda t, g, c: agg_by(range_fn(g, t, W2, len), JOB, sum),
    ),
    (
        60,
        "quantile by (job) (0.5, rate(data_total[2m]))",
        lambda t, g, c: agg_by(rate(c, t, W2), JOB, lambda vs: prom_quantile(0.5, vs)),
    ),
    (
        60,
        "topk(4, sum by (label_0) (rate(data_total[2m])))",
        lambda t, g, c: topk(4, agg_by(rate(c, t, W2), LABEL_0, sum)),
    ),
    (
        60,
        "quantile_over_time(0.9, data[2m]) / quantile_over_time(0.5, data[2m])",
        lambda t, g, c: q_ratio(t, g),
    ),
]
EVAL_OFFSETS_SECS = {
    # Before data, data start (the only sample sits on the window's end),
    # mid-run, around series 2's gap and series 1's end, and after all data.
    1: [-5, 0, 1, 37, 151, 202, 203, 301, 419, 420, 421, 500],
    60: [0, 60, 120, 180, 300, 360, 420, 540],
}
DURATION_SECS = 420


def make_dataset(seed: int = 7):
    """Two jobs x three label_0s, ~4 samples/s at ms timestamps, one on every
    whole second. Series 0 starts late with its counter at 0 (rate's zero
    clamp), series 1 stops early, series 2 has a gap in (200s, 203s]."""
    rng = random.Random(seed)
    gauge, counter = {}, {}
    for i in range(6):
        series = (f"job_{i // 3}", f"label_0_{i}")
        start, stop = (150, DURATION_SECS) if i == 0 else (0, DURATION_SECS)
        if i == 1:
            stop = 300
        total = 0.0 if i == 0 else 1e6 * i
        g, c = [], []
        for k in range(start * 4, stop * 4 + 1):
            t = DATA_START_MS + k * 250 + (0 if k % 4 == 0 else rng.randint(1, 249))
            if i == 2 and DATA_START_MS + 200_000 < t <= DATA_START_MS + 203_000:
                continue
            g.append((t, rng.paretovariate(2.0) * 10))
            c.append((t, total))
            total += rng.randint(1, 100)
        gauge[series], counter[series] = g, c
    return gauge, counter


def make_records(seed: int = 11) -> List[tuple]:
    """`http` records every 50 ms (one on every whole second) with skewed
    labels. Endpoint names repeat across services (endpoint is a child of
    service), so grouping by endpoint alone pools several services."""
    rng = random.Random(seed)
    records = []
    for k in range(DURATION_SECS * 20 + 1):
        t = DATA_START_MS + k * 50 + (0 if k % 20 == 0 else rng.randint(0, 49))
        service = rng.choices(["s0", "s1", "s2", "s3"], [8, 4, 2, 1])[0]
        labels = (
            rng.choices(["r0", "r1", "r2"], [6, 3, 1])[0],
            service,
            rng.choices(["e0", "e1", "e2"], [5, 2, 1])[0],
            rng.choices(["200", "404", "500"], [20, 3, 1])[0],
        )
        latency = rng.paretovariate(2.0) * (1 + "s0s1s2s3".index(service))
        records.append((t, labels, (latency, float(int(rng.paretovariate(1.2) * 10)))))
    return records


def series_rows(by_series: Dict[Series, Samples]) -> List[tuple]:
    return [(t, s, (v,)) for s, samples in by_series.items() for t, v in samples]


def pooled(records, t_ms: int, w_ms: int, q: "sbw.Query") -> Vector:
    """A structured query's reference: every record of a group in ASAPQuery's
    left-open window (t - w, t]."""
    idx = [HTTP_LABELS.index(name) for name in q.grouping]
    col = list(METRICS["http"]["values"]).index(q.value)
    groups: Dict[Series, List[float]] = {}
    for t, labels, values in records:
        if t_ms - w_ms < t <= t_ms:
            groups.setdefault(tuple(labels[i] for i in idx), []).append(values[col])
    if q.agg == "cardinality":
        return {k: len(set(vs)) for k, vs in groups.items()}
    return {k: prom_quantile(float(q.quantile), vs) for k, vs in groups.items()}


# Template 16's CUBE over (region, service, endpoint) and template 14/15-like
# cardinality groupings, including status and the full label set.
MULTIGROUP = [
    mg_entry("quantile", list(g), w)
    for w in (120, 300)
    for n in (0, 1, 2, 3)
    for g in itertools.combinations(["region", "service", "endpoint"], n)
] + [
    mg_entry("cardinality", g, w)
    for w in (120, 300)
    for g in [
        ["region"],
        ["service"],
        ["endpoint"],
        ["status"],
        ["region", "service"],
        ["service", "endpoint"],
        ["region", "service", "endpoint", "status"],
    ]
]


def run_clickhouse(script: str, tmp: str) -> List[dict]:
    path = os.path.join(tmp, "script.sql")
    with open(path, "w") as f:
        f.write(script)
    out = subprocess.run(
        [CLICKHOUSE, "local", "--queries-file", path],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [json.loads(line) for line in out.splitlines()]


def load_script(tmp: str, rows: Dict[str, List[tuple]], views) -> str:
    """Init SQL, then each table's (ts ms, labels, values) rows inserted
    through it (and any views on it)."""
    parts = [sbw.init_sql(METRICS, views, create_views=True)]
    for table, table_rows in rows.items():
        names = LABELS[table]
        values = sbw.value_columns(METRICS[table])
        csv = os.path.join(tmp, f"{table}.csv")
        with open(csv, "w") as f:
            f.writelines(
                ",".join([str(t), *labels, *map(repr, vs)]) + "\n"
                for t, labels, vs in table_rows
            )
        structure = ", ".join(
            ["ts_ms Int64"]
            + [f"{n} String" for n in names]
            + [f"{v} Float64" for v in values]
        )
        parts.append(
            f"INSERT INTO {table} SELECT fromUnixTimestamp64Milli(ts_ms), "
            f"{', '.join(names + values)} FROM file('{csv}', 'CSV', '{structure}');\n"
        )
    return "".join(parts)


def query_script(statements: List[Tuple[str, str]]) -> str:
    """Each (id, sql) as JSON rows tagged with the id."""
    return "".join(
        f"SELECT '{qid}' AS qid, * FROM ({sql}) FORMAT JSONEachRow;\n"
        for qid, sql in statements
    )


def rows_by_id(rows: List[dict], labels: Dict[str, Tuple[str, ...]]) -> dict:
    out: Dict[str, Vector] = {qid: {} for qid in labels}
    for row in rows:
        qid = row["qid"]
        out[qid][tuple(row[name] for name in labels[qid])] = row["value"]
    return out


def assert_vectors_close(test, got: Vector, want: Vector, rel: float) -> None:
    test.assertEqual(sorted(got), sorted(want))
    for key, value in want.items():
        test.assertTrue(
            math.isclose(got[key], value, rel_tol=rel), (key, got[key], value)
        )


def all_cases(gauge, counter, records) -> List[Tuple[str, "sbw.Query", Callable]]:
    """(name, query, reference at eval time t ms) for CASES and MULTIGROUP."""
    cases = [
        (p, sbw.parse_promql(p, t, LABELS), lambda e, ref=ref: ref(e, gauge, counter))
        for t, p, ref in CASES
    ]
    for entry in MULTIGROUP:
        q = sbw.query_from_entry(entry, METRICS, LABELS)
        w_ms = q.window_secs * 1000
        cases.append(
            (sbw.describe(entry), q, lambda e, q=q, w=w_ms: pooled(records, e, w, q))
        )
    return cases


@unittest.skipUnless(CLICKHOUSE, "clickhouse binary not found")
class KeyPruningTest(unittest.TestCase):
    """ClickHouse 26.10 prunes `ts > <integer>` on a DateTime64 key as
    `ts >= <integer> + 1`, so a left-open window lost the samples in its first
    second. Small granules let the index prune them here too. The view's
    DateTime `bucket` bounds are checked the same way."""

    def test_samples_just_after_window_start_are_kept(self):
        end_s = DATA_START_MS // 1000 + 3600
        start_ms = (end_s - 60) * 1000
        # Every 7 ms from just before the window start to past its end; each
        # record has its own user_id, so the distinct count is a row count.
        times = range(start_ms - 50, end_s * 1000 + 50, 7)
        records = [(t, ("r0", "s0", "e0", "200"), (1.0, float(t))) for t in times]
        series = [(t, ("job_0", "label_0_0"), (1.0,)) for t in times]
        cases = [
            (
                sbw.query_from_entry(
                    mg_entry("cardinality", ["region"], 60), METRICS, LABELS
                ),
                sum(start_ms < t <= end_s * 1000 for t in times),
            ),
            (
                sbw.parse_promql("count_over_time(data[1m])", 60, LABELS),
                sum(start_ms <= t <= end_s * 1000 for t in times),
            ),
            # The only query of its view and grouped by nothing, so the view
            # has no label keys.
            (
                sbw.query_from_entry(mg_entry("quantile", [], 60), METRICS, LABELS),
                1.0,
            ),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            views = sbw.plan_views([q for q, _ in cases], METRICS)
            rows = {"http": records, "data": series}
            script = (
                load_script(tmp, rows, list(views.values()))
                .replace("ts);", "ts) SETTINGS index_granularity = 8;")
                .replace(") AS SELECT ", ") SETTINGS index_granularity = 1 AS SELECT ")
            )
            self.assertEqual(script.count("index_granularity = 8"), len(METRICS))
            self.assertEqual(script.count("index_granularity = 1 "), len(views))
            statements = [
                (f"{mode}/{i}", sql.replace(sbw.END, str(end_s)))
                for i, (q, _) in enumerate(cases)
                for mode, sql in [
                    ("raw", sbw.raw_sql(q, LABELS)),
                    ("mv", sbw.mv_sql(q, views, LABELS)),
                ]
            ]
            out = run_clickhouse(script + query_script(statements), tmp)
        got = {row["qid"]: row["value"] for row in out}
        want = {
            f"{mode}/{i}": n for i, (_, n) in enumerate(cases) for mode in ("raw", "mv")
        }
        self.assertEqual(got, want)


@unittest.skipUnless(CLICKHOUSE, "clickhouse binary not found")
class StaleViewsTest(unittest.TestCase):
    def test_init_drops_other_workloads_views(self):
        """A view stays attached to a dropped and recreated table, so one
        left by another workload's baseline_mv run would keep firing."""
        q = sbw.parse_promql("sum_over_time(data[1m])", 60, LABELS)
        other = list(sbw.plan_views([q], METRICS).values())
        with tempfile.TemporaryDirectory() as tmp:
            script = (
                sbw.init_sql(METRICS, other, True)
                # This workload has no views of its own.
                + load_script(tmp, {"data": [(DATA_START_MS, ("j", "l"), (1.0,))]}, [])
                + "SELECT count() AS n FROM system.tables "
                f"WHERE database = '{sbw.VIEW_DATABASE}' FORMAT JSONEachRow;\n"
            )
            self.assertEqual(run_clickhouse(script, tmp), [{"n": 0}])


@unittest.skipUnless(CLICKHOUSE, "clickhouse binary not found")
class ClickHouseSemanticsTest(unittest.TestCase):
    """Every CASES and MULTIGROUP query at every eval offset, over the raw
    table (baseline) and over its view (baseline_mv), in one clickhouse-local
    run."""

    @classmethod
    def setUpClass(cls):
        gauge, counter = make_dataset()
        records = make_records()
        cls.cases = all_cases(gauge, counter, records)
        views = sbw.plan_views([q for _, q, _ in cls.cases], METRICS)
        statements, groupings = [], {}
        for i, (_, q, _) in enumerate(cls.cases):
            for offset in EVAL_OFFSETS_SECS[q.interval_secs]:
                end = str(DATA_START_MS // 1000 + offset)
                for mode, sql in [
                    ("raw", sbw.raw_sql(q, LABELS)),
                    ("mv", sbw.mv_sql(q, views, LABELS)),
                ]:
                    qid = f"{mode}/{i}@{offset}"
                    statements.append((qid, sql.replace(sbw.END, end)))
                    groupings[qid] = q.grouping
        rows = {
            "data": series_rows(gauge),
            "data_total": series_rows(counter),
            "http": records,
        }
        with tempfile.TemporaryDirectory() as tmp:
            script = load_script(tmp, rows, list(views.values()))
            cls.got = rows_by_id(
                run_clickhouse(script + query_script(statements), tmp), groupings
            )

    def test_raw_sql_matches_prometheus_reference(self):
        BRANCHES_HIT.clear()
        nonempty = total = 0
        for i, (name, q, ref) in enumerate(self.cases):
            for offset in EVAL_OFFSETS_SECS[q.interval_secs]:
                with self.subTest(query=name, offset=offset):
                    want = ref(DATA_START_MS + offset * 1000)
                    nonempty, total = nonempty + bool(want), total + 1
                    got = self.got[f"raw/{i}@{offset}"]
                    assert_vectors_close(self, got, want, 1e-9)
        self.assertEqual(
            BRANCHES_HIT,
            {"start_avg", "start_boundary", "zero_clamp", "end_avg", "end_boundary"},
        )
        # Both empty and non-empty windows are compared.
        self.assertGreater(nonempty, total // 2)
        self.assertLess(nonempty, total)

    def test_mv_sql_matches_raw_sql(self):
        for i, (name, q, _) in enumerate(self.cases):
            # Only sums are re-associated (per bucket, then across buckets and
            # groups); every other statistic merges to the identical value.
            rel = 1e-9 if q.agg == "sum" else 0.0
            for offset in EVAL_OFFSETS_SECS[q.interval_secs]:
                with self.subTest(query=name, offset=offset):
                    raw = self.got[f"raw/{i}@{offset}"]
                    assert_vectors_close(self, self.got[f"mv/{i}@{offset}"], raw, rel)


if __name__ == "__main__":
    unittest.main()
