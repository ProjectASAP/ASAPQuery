"""Tests for datasets/fake_metric/sketch_bench_workloads.py.

ClickHouseSemanticsTest runs every generated statement in clickhouse-local
over a small dataset and compares it with a separate Python implementation of
the Prometheus semantics. It needs a `clickhouse` binary on PATH (or
CLICKHOUSE_BINARY) and is skipped otherwise.
"""

import json
import math
import os
import random
import shutil
import subprocess
import sys
import tempfile
import unittest
from typing import Callable, Dict, List, Tuple

import yaml

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "datasets", "fake_metric")
)

import sketch_bench_workloads as sbw  # noqa: E402

LABELS = {"data": ["job", "label_0"], "data_total": ["job", "label_0"]}
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

    def test_structured_entry_raises(self):
        entry = {"table": "http", "capability": "quantile", "interval_secs": 60}
        with self.assertRaises(ValueError):
            sbw.query_from_entry(entry, LABELS)


class ConvertTest(unittest.TestCase):
    def test_writes_config_grouped_by_interval(self):
        e2e = {
            "metrics": {
                name: {"labels": [{"name": "job"}, {"name": "label_0"}]}
                for name in LABELS
            },
            "queries": [
                {"promql": p, "interval_secs": t, "rqes": []} for t, p in CLASSIC
            ],
        }
        with tempfile.TemporaryDirectory() as tmp:
            table = os.path.join(tmp, "classic.json")
            with open(table, "w") as f:
                json.dump({"workloads": [{"e2e": e2e}]}, f)
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                with open(sbw.convert(table, "data.json", 30, 600)) as f:
                    cfg = yaml.safe_load(f)
                groups = {g["name"]: g for g in cfg["query_groups"]}
                self.assertEqual(sorted(groups), ["interval_1s", "interval_60s"])
                g60 = groups["interval_60s"]
                self.assertEqual(g60["repetition_delay_ms"], 60000)
                self.assertEqual(
                    g60["client_options"], {"starting_delay": 30, "repetitions": 10}
                )
                self.assertEqual(
                    groups["interval_1s"]["client_options"]["repetitions"], 600
                )
                with open(g60["sql_file"]["baseline"]) as f:
                    statements = [s for s in f.read().split(";") if s.strip()]
                self.assertEqual(len(statements), 8)
                with open(cfg["dataset"]["init_sql_file"]) as f:
                    self.assertIn("CREATE TABLE data_total (ts DateTime64(3)", f.read())
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
    return [(t, v) for t, v in samples if t_ms - w_ms < t <= t_ms]


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


def load_script(tmp: str, data: Dict[str, Dict[Series, Samples]]) -> str:
    """Init SQL, then the data inserted through it (and any views on it)."""
    parts = [sbw.init_sql(LABELS)]
    for table, by_series in data.items():
        csv = os.path.join(tmp, f"{table}.csv")
        with open(csv, "w") as f:
            for (job, label_0), samples in by_series.items():
                f.writelines(f"{t},{job},{label_0},{v!r}\n" for t, v in samples)
        parts.append(
            f"INSERT INTO {table} SELECT fromUnixTimestamp64Milli(ts_ms), job, "
            f"label_0, value FROM file('{csv}', 'CSV', "
            "'ts_ms Int64, job String, label_0 String, value Float64');\n"
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


@unittest.skipUnless(CLICKHOUSE, "clickhouse binary not found")
class ClickHouseSemanticsTest(unittest.TestCase):
    def test_raw_sql_matches_prometheus_reference(self):
        gauge, counter = make_dataset()
        queries = [(t, p, sbw.parse_promql(p, t, LABELS)) for t, p, _ in CASES]
        statements, groupings = [], {}
        for i, (interval, _, q) in enumerate(queries):
            for offset in EVAL_OFFSETS_SECS[interval]:
                end = DATA_START_MS // 1000 + offset
                qid = f"{i}@{offset}"
                sql = sbw.raw_sql(q, LABELS).replace(sbw.END, str(end))
                statements.append((qid, sql))
                groupings[qid] = q.grouping
        with tempfile.TemporaryDirectory() as tmp:
            script = load_script(tmp, {"data": gauge, "data_total": counter})
            got = rows_by_id(
                run_clickhouse(script + query_script(statements), tmp), groupings
            )

        BRANCHES_HIT.clear()
        nonempty = 0
        for i, (interval, promql, ref) in enumerate(CASES):
            for offset in EVAL_OFFSETS_SECS[interval]:
                with self.subTest(promql=promql, offset=offset):
                    want = ref(DATA_START_MS + offset * 1000, gauge, counter)
                    nonempty += bool(want)
                    assert_vectors_close(self, got[f"{i}@{offset}"], want, 1e-9)
        self.assertEqual(
            BRANCHES_HIT,
            {"start_avg", "start_boundary", "zero_clamp", "end_avg", "end_boundary"},
        )
        # Both empty and non-empty windows are compared.
        self.assertGreater(nonempty, len(statements) // 2)
        self.assertLess(nonempty, len(statements))


if __name__ == "__main__":
    unittest.main()
