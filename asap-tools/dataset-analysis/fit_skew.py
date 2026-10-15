#!/usr/bin/env python3
"""Fit label skew (Zipf theta) and value skew (power-law alpha) for trace query sets.

Key queries fit a discrete Zipf exponent to the rank-frequency of the group-by
key; value queries fit a continuous power law to the queried column. Every
query is evaluated at each step of its table, as an instant query (latest
sample per series) and/or as range queries over the last S seconds: lower/upper
are the min/max over evaluation times and mle is the fit on all data.
"""

import argparse
import glob
import io
import logging
import os
import re
import tarfile
import time
import warnings
from contextlib import redirect_stdout
from multiprocessing import Pool
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd
import powerlaw
import pyarrow as pa
import pyarrow.csv as pacsv
import yaml
from matplotlib.figure import Figure
from numpy.typing import ArrayLike
from scipy import sparse
from scipy.optimize import minimize_scalar

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_QUERIES = sorted(str(p) for p in (SCRIPT_DIR / "queries").glob("*.yaml"))
DEFAULT_SUMMARY = SCRIPT_DIR / "results" / "skew_summary.csv"
DEFAULT_OUT = SCRIPT_DIR / "out"

ZIPF_THETA_BOUNDS = (0.0, 5.0)
# Each power-law fit uses a uniform subsample of at most this many values.
MAX_FIT_SAMPLES = 100_000
# numpy's multivariate_hypergeometric needs a total under 1e9 (merge_samples).
HYPERGEOMETRIC_MAX_TOTAL = 1_000_000_000
# xmin is chosen by KS distance over this many quantiles of the sample, and only
# where at least MIN_TAIL_SAMPLES values remain in the tail.
XMIN_GRID_SIZE = 50
XMIN_GRID_QUANTILES = (0.5, 0.999)
MIN_TAIL_SAMPLES = 100
SAMPLE_SEED = 0
DEFAULT_MIN_EVAL_ROWS = 200
DEFAULT_MIN_EVAL_KEYS = 2
# A likelihood-ratio comparison is significant when its p is below this.
COMPARE_P_THRESHOLD = 0.1
TAIL_LIGHT = "light"
TAIL_POWER_LAW = "power_law"
TAIL_LOGNORMAL = "lognormal"
TAIL_HEAVY_INCONCLUSIVE = "heavy_inconclusive"
POWER_LAW_ALTERNATIVES = ("lognormal", "exponential")

INSTANT = "instant"
# An instant query sees a series' latest sample at most this old (Prometheus
# default), or one sampling period if that is longer.
PROMETHEUS_LOOKBACK_S = 300
DURATION_UNITS_S = {"s": 1, "m": 60, "h": 3600, "d": 86400}
# Range key sums are materialized for this many evaluation times at a time.
EVAL_CHUNK = 32
# Steps per chunk when stitching instant samples across files (resolve_boundaries).
BOUNDARY_CHUNK_STEPS = 60
# Accuracy each sketch must reach on a query; a query's `targets` overrides.
DEFAULT_TARGETS = {
    "are_top100": 0.05,  # CMS / CountSketch mean relative error of the top 100 keys
    "precision_at_k": 0.95,  # top-k precision@k
    "hll_rel_err": 0.02,  # HLL relative cardinality error
    "rank_err": 0.01,  # KLL / DDSketch mean rank error
}

CSV_BLOCK_BYTES = 64 << 20
CSV_NULL_VALUES = ["", "None", "NULL", "NaN", "nan"]
LABEL_TYPE = pa.dictionary(pa.int32(), pa.string())

# Step index w holds timestamps in ((w - 1) * step_s, w * step_s], so the
# evaluation at t = w * step_s over range S sums steps w - S / step_s + 1 .. w.
STEP_COL = "_step"
TIME_COL = "_time_s"
# Label tuples are hashed to uint64; MISSING_KEY marks a null label.
KEY_COL = "_key"
SERIES_COL = "_series"
NEXT_COL = "_next_step"
FILE_COL = "_file"
MISSING_KEY = np.uint64(0)
COUNT_COL = "count"
VALUE_SUM_COL = "value_sum"
KEY_WEIGHTS = {"count", "value"}
QUERY_KINDS = {"keys", "values"}

# Enough digits to keep row counts exact.
SUMMARY_FLOAT_FORMAT = "%.10g"
SUMMARY_COLUMNS = [
    "dataset",
    "query_id",
    "promql",
    "kind",
    "weight",
    "range",
    "range_s",
    "step_s",
    "K_total",
    "rows_total",
    "K_win_min",
    "K_win_median",
    "K_win_max",
    "rows_win_min",
    "rows_win_median",
    "rows_win_max",
    "n_evals",
    "lower",
    "mle",
    "upper",
    "worst_theta_cms",
    "worst_K",
    "min_N",
    "max_N",
    "worst_alpha_rank",
    "worst_alpha_memory",
    *(f"target_{name}" for name in DEFAULT_TARGETS),
    "top1_share",
    "theta_ls",
    "dropped_frac",
    "xmin",
    "ks_d",
    "tail_frac",
    "R_lognormal",
    "p_lognormal",
    "R_exponential",
    "p_exponential",
    "tail_class",
    "ok_frac",
]

# Plot colors: observed data in neutral ink, the three estimates as one blue ramp.
OBSERVED_COLOR = "#8a8984"
BOUND_COLORS = {"lower": "#86b6ef", "mle": "#2a78d6", "upper": "#104281"}

log = logging.getLogger("fit_skew")


# ---------------------------------------------------------------- config


def load_config(path: str) -> Dict[str, Any]:
    with open(path) as f:
        cfg = yaml.safe_load(f)
    validate_config(cfg)
    return cfg


def duration_secs(token: str) -> int:
    """'5m' -> 300."""
    match = re.fullmatch(r"(\d+)([smhd])", token)
    if match is None:
        raise ValueError(f"bad duration {token!r}, expected e.g. 30s, 5m, 1h")
    return int(match.group(1)) * DURATION_UNITS_S[match.group(2)]


def range_secs(token: str) -> float:
    """Range duration in seconds; NaN for instant."""
    return np.nan if token == INSTANT else duration_secs(token)


def range_steps(token: str, step_s: int) -> int:
    """Steps an evaluation covers: one for instant (per-evaluation aggregates)."""
    return 1 if token == INSTANT else duration_secs(token) // step_s


def lookback_steps(table: Dict[str, Any]) -> int:
    lookback_s = max(PROMETHEUS_LOOKBACK_S, table["step_s"])
    return -(-lookback_s // table["step_s"])


def validate_ranges(q: Dict[str, Any], table: Dict[str, Any], where: str) -> None:
    if not q.get("range"):
        raise ValueError(f"{where}: range must list instant and/or durations")
    for token in q["range"]:
        if token == INSTANT:
            if "series_key" not in table:
                raise ValueError(f"{where}: instant needs a table series_key")
            if "promql" not in q:
                raise ValueError(f"{where}: instant needs promql")
            continue
        secs = duration_secs(token)
        if secs % table["step_s"]:
            raise ValueError(f"{where}: range {token} is not a multiple of step_s")
        if "{range}" not in q.get("promql_range", ""):
            raise ValueError(f"{where}: range queries need promql_range with {{range}}")


def validate_query(q: Dict[str, Any], table: Dict[str, Any], where: str) -> None:
    if q["kind"] not in QUERY_KINDS:
        raise ValueError(f"{where}: kind must be one of {sorted(QUERY_KINDS)}")
    for col in q["group_by"]:
        if col not in table["label_columns"]:
            raise ValueError(f"{where}: {col!r} is not a label column")
    value = q.get("value")
    if value is not None and value not in table["value_columns"]:
        raise ValueError(f"{where}: {value!r} is not a value column")
    unknown = set(q.get("targets", {})) - set(DEFAULT_TARGETS)
    if unknown:
        raise ValueError(f"{where}: unknown targets {sorted(unknown)}")
    if table.get("format") != "boom_arrow":
        validate_ranges(q, table, where)
    if q["kind"] == "values":
        if value is None:
            raise ValueError(f"{where}: value queries need a value column")
        return
    if not q["group_by"]:
        raise ValueError(f"{where}: key queries need group_by")
    weights = set(q["weights"])
    if not weights or not weights <= KEY_WEIGHTS:
        raise ValueError(f"{where}: weights must be a subset of {KEY_WEIGHTS}")
    if "value" in weights and value is None:
        raise ValueError(f"{where}: weight 'value' needs a value column")
    if cms_weight(q) not in weights:
        raise ValueError(f"{where}: cms_weight must be one of the weights")


def validate_config(cfg: Dict[str, Any]) -> None:
    for name, table in cfg["tables"].items():
        where = f"{cfg['dataset']}/{name}"
        if table.get("format") == "boom_arrow":
            continue
        if not isinstance(table.get("step_s"), int) or table["step_s"] <= 0:
            raise ValueError(f"{where}: step_s must be a positive integer")
        if not set(table.get("series_key", [])) <= set(table["label_columns"]):
            raise ValueError(f"{where}: series_key must be label columns")
    for q in cfg["queries"]:
        where = f"{cfg['dataset']}/{q['id']}"
        table = cfg["tables"].get(q["table"])
        if table is None:
            raise ValueError(f"{where}: unknown table {q['table']!r}")
        validate_query(q, table, where)


def cms_weight(q: Dict[str, Any]) -> str:
    """Weight a per-key counter sketch sees: count for count-by, value for sum-by."""
    return q.get("cms_weight", "count")


def query_targets(q: Dict[str, Any]) -> Dict[str, float]:
    targets = {**DEFAULT_TARGETS, **q.get("targets", {})}
    return {f"target_{name}": value for name, value in targets.items()}


def range_promql(q: Dict[str, Any], token: str) -> str:
    if token == INSTANT:
        return q["promql"]
    return q["promql_range"].format(range=token)


def expand_files(data_root: str, patterns: Sequence[str]) -> List[str]:
    paths = []
    for pattern in patterns:
        matches = sorted(glob.glob(os.path.join(data_root, pattern)))
        if not matches:
            raise FileNotFoundError(f"no files match {pattern} under {data_root}")
        paths.extend(matches)
    return paths


# ---------------------------------------------------------------- reading


def google_column_names(schema_path: str, table: str) -> List[str]:
    """Column names for a headerless Google table, e.g. 'CPU rate' -> 'cpu_rate'."""
    schema = pd.read_csv(schema_path)
    rows = schema[schema["file pattern"].str.startswith(table + "/")]
    if rows.empty:
        raise ValueError(f"table {table!r} not found in {schema_path}")
    contents = rows.sort_values("field number")["content"]
    return [c.strip().lower().replace(" ", "_") for c in contents]


def csv_batches(
    source: Any,
    columns: Sequence[str],
    float_columns: Set[str],
    column_names: Optional[List[str]],
    bad_rows: List[str],
) -> Iterator[pd.DataFrame]:
    """Stream a CSV as DataFrames. Float columns are float64, the rest categorical."""

    def skip_row(row: Any) -> str:
        bad_rows.append(row.text)
        return "skip"

    types = {c: pa.float64() if c in float_columns else LABEL_TYPE for c in columns}
    reader = pacsv.open_csv(
        source,
        read_options=pacsv.ReadOptions(
            column_names=column_names, block_size=CSV_BLOCK_BYTES
        ),
        parse_options=pacsv.ParseOptions(invalid_row_handler=skip_row),
        convert_options=pacsv.ConvertOptions(
            include_columns=list(columns),
            column_types=types,
            null_values=CSV_NULL_VALUES,
            strings_can_be_null=True,
        ),
    )
    for batch in reader:
        yield batch.to_pandas()


def table_frames(
    data_root: str,
    table: Dict[str, Any],
    path: str,
    columns: Sequence[str],
    float_columns: Set[str],
    bad_rows: List[str],
) -> Iterator[pd.DataFrame]:
    fmt = table["format"]
    if fmt == "google_csv":
        # The table name is the directory name, as in the schema's file patterns.
        schema = os.path.join(data_root, table["schema"])
        names = google_column_names(schema, Path(path).parent.name)
        yield from csv_batches(path, columns, float_columns, names, bad_rows)
    elif fmt == "alibaba_tar":
        # Stream members straight out of the archive instead of extracting.
        with tarfile.open(path, mode="r|gz") as archive:
            for member in archive:
                f = archive.extractfile(member)
                if f is not None:
                    yield from csv_batches(f, columns, float_columns, None, bad_rows)
    else:
        raise ValueError(f"unsupported table format {fmt!r}")


def load_join(
    data_root: str, table: Dict[str, Any], join: Dict[str, Any]
) -> pd.DataFrame:
    """Last non-null value of each join column per key, ordered by sort_by."""
    columns = join["keys"] + [join["sort_by"]] + join["columns"]
    bad_rows: List[str] = []
    frames = [
        frame
        for path in expand_files(data_root, join["files"])
        for frame in table_frames(
            data_root, table, path, columns, {join["sort_by"]}, bad_rows
        )
    ]
    df = pd.concat(frames, ignore_index=True)
    df = df.astype({c: object for c in join["keys"] + join["columns"]})
    df = df.sort_values(join["sort_by"], kind="stable")
    return df.groupby(join["keys"])[join["columns"]].last()


def read_boom_series(path: str) -> np.ndarray:
    """Target of a BOOM series as a (variates, time) array."""
    with pa.memory_map(path) as src:
        table = pa.ipc.open_stream(src).read_all()
    if table.num_rows != 1:
        raise ValueError(f"{path}: expected one row, found {table.num_rows}")
    return np.atleast_2d(np.array(table.column("target")[0].as_py(), dtype=float))


# ---------------------------------------------------------------- aggregation


def query_key(q: Dict[str, Any]) -> Tuple[str, str]:
    return q["id"], q["kind"]


def has_range(q: Dict[str, Any]) -> bool:
    return any(token != INSTANT for token in q["range"])


def label_hash(frame: pd.DataFrame, cols: Sequence[str]) -> np.ndarray:
    """uint64 hash of each row's label tuple, equal across batches and files."""
    return pd.util.hash_pandas_object(frame[list(cols)], index=False).to_numpy()


def key_hash(frame: pd.DataFrame, cols: Sequence[str]) -> np.ndarray:
    """label_hash, with MISSING_KEY where any label is null (such rows are dropped)."""
    hashes = label_hash(frame, cols)
    hashes[frame[list(cols)].isna().any(axis=1).to_numpy()] = MISSING_KEY
    return hashes


def group_col(q: Dict[str, Any]) -> str:
    return f"{KEY_COL}:{','.join(q['group_by'])}"


def merge_key_parts(parts: List[pd.DataFrame]) -> pd.DataFrame:
    """Sum per-(step, key) counts and value sums."""
    return (
        pd.concat(parts, ignore_index=True)
        .groupby([STEP_COL, KEY_COL], sort=False)
        .sum()
        .reset_index()
    )


def key_part(frame: pd.DataFrame, q: Dict[str, Any]) -> pd.DataFrame:
    """Row count and clipped value sum per (step, key); frame carries KEY_COL."""
    frame = frame[frame[KEY_COL] != MISSING_KEY]
    keys = [STEP_COL, KEY_COL]
    if "value" in q["weights"]:
        clipped = frame.assign(**{VALUE_SUM_COL: frame[q["value"]].clip(lower=0)})
        part = clipped.groupby(keys, sort=False).agg(
            **{
                COUNT_COL: (VALUE_SUM_COL, "size"),
                VALUE_SUM_COL: (VALUE_SUM_COL, "sum"),
            }
        )
    else:
        part = frame.groupby(keys, sort=False).size().to_frame(COUNT_COL)
    return part.reset_index()


def add_values(acc: Dict[str, Any], steps: np.ndarray, values: np.ndarray) -> None:
    finite = np.isfinite(values)
    positive = values > 0
    acc["n_finite"] += int(finite.sum())
    for w in np.unique(steps[positive]):
        acc["steps"].setdefault(int(w), []).append(values[positive & (steps == w)])


def value_sample(x: np.ndarray) -> Tuple[int, np.ndarray]:
    """(len(x), up to MAX_FIT_SAMPLES of x in random order): any prefix of the
    sample is a uniform sample of x, which merge_samples relies on."""
    rng = np.random.default_rng(SAMPLE_SEED)
    return len(x), x[rng.permutation(len(x))[:MAX_FIT_SAMPLES]]


def merge_samples(parts: Sequence[Tuple[int, np.ndarray]]) -> Tuple[int, np.ndarray]:
    """Uniform sample of the union of value_sample parts: each part gives a
    multivariate-hypergeometric share of its prefix. numpy draws that only
    for totals under 1e9; above, a multinomial share (drawing 1e5 of 1e9 or
    more, with and without replacement agree), capped at each prefix."""
    counts = np.array([n for n, _ in parts], dtype=np.int64)
    total = int(counts.sum())
    rng = np.random.default_rng(SAMPLE_SEED)
    n = min(total, MAX_FIT_SAMPLES)
    if total < HYPERGEOMETRIC_MAX_TOTAL:
        take = rng.multivariate_hypergeometric(counts, n)
    else:
        lengths = np.array([len(x) for _, x in parts])
        take = np.minimum(rng.multinomial(n, counts / total), lengths)
    merged = np.concatenate([x[:k] for (_, x), k in zip(parts, take)])
    return total, rng.permutation(merged)


def sample_steps(acc: Dict[str, Any]) -> Dict[str, Any]:
    """Replace each step's value arrays by one value_sample."""
    steps = {w: value_sample(np.concatenate(xs)) for w, xs in acc["steps"].items()}
    return {"n_finite": acc["n_finite"], "steps": steps}


def merge_step_samples(accs: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    parts: Dict[int, List[Tuple[int, np.ndarray]]] = {}
    for acc in accs:
        for w, sample in acc["steps"].items():
            parts.setdefault(w, []).append(sample)
    return {
        "n_finite": sum(acc["n_finite"] for acc in accs),
        "steps": {w: merge_samples(ps) for w, ps in parts.items()},
    }


def latest_per_step(rows: pd.DataFrame) -> pd.DataFrame:
    """Each series' last sample in each step."""
    rows = rows.sort_values(TIME_COL, kind="stable")
    return rows.drop_duplicates([SERIES_COL, STEP_COL], keep="last")


def latest_part(
    frame: pd.DataFrame, series_key: List[str], queries: List[Dict[str, Any]]
) -> pd.DataFrame:
    """Per (series, step) latest sample with the group keys and values the
    instant queries need."""
    cols: Dict[str, Any] = {
        SERIES_COL: label_hash(frame, series_key),
        STEP_COL: frame[STEP_COL].to_numpy(),
        TIME_COL: frame[TIME_COL].to_numpy(),
    }
    for q in queries:
        if q["kind"] == "keys":
            cols[group_col(q)] = key_hash(frame, q["group_by"])
        if q.get("value"):
            cols[q["value"]] = frame[q["value"]].to_numpy(dtype=float)
    return latest_per_step(pd.DataFrame(cols))


def next_step_of_series(rows: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    """For rows sorted by (series, step): the series' next step (NaN if none)
    and whether the row is the series' first."""
    series = rows[SERIES_COL].to_numpy()
    steps = rows[STEP_COL].to_numpy()
    same = series[1:] == series[:-1]
    has_next = np.append(same, False)
    next_step = np.where(has_next, np.append(steps[1:], 0), np.nan)
    return next_step, ~np.append(False, same)


def split_instant(
    rows: pd.DataFrame, queries: List[Dict[str, Any]], lookback: int
) -> Tuple[Dict[Tuple[str, str], Any], pd.DataFrame]:
    """Instant aggregates of one file's latest samples, except each series'
    first and last sample in the file, which are returned for resolve_boundaries
    because another file may hold the same step or the next sample."""
    rows = latest_per_step(rows).sort_values([SERIES_COL, STEP_COL], kind="stable")
    next_step, first = next_step_of_series(rows)
    rows = rows.assign(**{NEXT_COL: next_step})
    interior = ~first & ~np.isnan(next_step)
    return instant_parts(rows[interior], queries, lookback), rows[~interior]


def check_file_order(rows: pd.DataFrame) -> None:
    """split_instant takes a series' next sample from the same file, so a
    series' steps in one file must not interleave with its steps in another."""
    spans = (
        rows.groupby([SERIES_COL, FILE_COL])[STEP_COL]
        .agg(["min", "max"])
        .reset_index()
        .sort_values([SERIES_COL, "min", "max"])
    )
    series = spans[SERIES_COL].to_numpy()
    lo, hi = spans["min"].to_numpy(), spans["max"].to_numpy()
    overlap = (series[1:] == series[:-1]) & (lo[1:] < hi[:-1])
    if overlap.any():
        raise ValueError(
            f"{int(overlap.sum())} series have samples interleaved across files; "
            "instant evaluation needs files that are consecutive time chunks"
        )


def resolve_boundaries(
    boundary: Sequence[pd.DataFrame],
    queries: List[Dict[str, Any]],
    lookback: int,
    chunk_steps: int = BOUNDARY_CHUNK_STEPS,
) -> Dict[Tuple[str, str], Any]:
    """Instant aggregates of the per-file first/last samples: keep the latest
    sample per (series, step) over files, and take its next step as the
    nearer of the in-file next step and the next boundary sample.

    Short files make almost every sample a boundary one, so this runs over
    chunks of `chunk_steps` steps. A sample counts for at most `lookback`
    steps, so a chunk owning steps [start, end) reads the boundary samples
    up to end + lookback and gives the same aggregates as one pass."""
    files = [b.assign(**{FILE_COL: i}) for i, b in enumerate(boundary) if len(b)]
    if not files:
        return instant_parts(pd.concat(boundary, ignore_index=True), queries, lookback)
    spans = [(int(f[STEP_COL].min()), int(f[STEP_COL].max())) for f in files]
    first, last = min(lo for lo, _ in spans), max(hi for _, hi in spans)
    chunks = []
    for start in range(first, last + 1, chunk_steps):
        end = start + chunk_steps
        overlapping = [
            f[(f[STEP_COL] >= start) & (f[STEP_COL] < end + lookback)]
            for f, (lo, hi) in zip(files, spans)
            if lo < end + lookback and hi >= start
        ]
        if not overlapping:
            continue
        rows = pd.concat(overlapping, ignore_index=True)
        check_file_order(rows)
        rows = rows.sort_values([SERIES_COL, STEP_COL, TIME_COL], kind="stable")
        in_file_next = rows.groupby([SERIES_COL, STEP_COL])[NEXT_COL].transform("min")
        rows = rows.assign(**{NEXT_COL: in_file_next})
        rows = rows.drop_duplicates([SERIES_COL, STEP_COL], keep="last")
        next_step, _ = next_step_of_series(rows)
        rows = rows.assign(**{NEXT_COL: np.fmin(rows[NEXT_COL].to_numpy(), next_step)})
        chunks.append(instant_parts(rows[rows[STEP_COL] < end], queries, lookback))
    return {
        query_key(q): (merge_key_parts if q["kind"] == "keys" else merge_step_samples)(
            [c[query_key(q)] for c in chunks]
        )
        for q in queries
    }


def instant_parts(
    rows: pd.DataFrame, queries: List[Dict[str, Any]], lookback: int
) -> Dict[Tuple[str, str], Any]:
    """Per-evaluation aggregates of latest samples. A sample at step w is its
    series' latest for evaluations w .. min(next step, w + lookback) - 1, e.g.
    with lookback 5 a series that stops after step 3 counts at steps 3..7."""
    start = rows[STEP_COL].to_numpy()
    end = np.fmin(rows[NEXT_COL].to_numpy(), start + lookback).astype(np.int64)
    reps = end - start
    idx = np.repeat(np.arange(len(rows)), reps)
    offsets = np.arange(len(idx)) - np.repeat(np.cumsum(reps) - reps, reps)
    evals = rows.iloc[idx].assign(**{STEP_COL: start[idx] + offsets})
    out: Dict[Tuple[str, str], Any] = {}
    for q in queries:
        if q["kind"] == "keys":
            out[query_key(q)] = key_part(
                evals.rename(columns={group_col(q): KEY_COL}), q
            )
        else:
            acc: Dict[str, Any] = {"n_finite": 0, "steps": {}}
            add_values(
                acc, evals[STEP_COL].to_numpy(), evals[q["value"]].to_numpy(float)
            )
            out[query_key(q)] = sample_steps(acc)
    return out


def aggregate_file(task: Tuple[Any, ...]) -> Dict[str, Any]:
    """Per-step key aggregates and value samples (range queries) and instant
    aggregates (instant queries) for one file."""
    data_root, table, path, queries, joins, max_rows = task
    time_col = table["time_column"]
    range_queries = [q for q in queries if has_range(q)]
    instant_queries = [q for q in queries if INSTANT in q["range"]]
    value_cols = {q["value"] for q in queries if q.get("value")}
    join_cols = [c for j in table.get("joins", []) for c in j["columns"]]
    needed = {time_col} | value_cols
    needed |= {c for q in queries for c in q["group_by"] if c not in join_cols}
    needed |= {c for j in table.get("joins", []) for c in j["keys"]}
    if instant_queries:
        needed |= set(table["series_key"])

    key_parts: Dict[Tuple[str, str], List[pd.DataFrame]] = {}
    values: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for q in range_queries:
        if q["kind"] == "keys":
            key_parts[query_key(q)] = []
        else:
            values[query_key(q)] = {"n_finite": 0, "steps": {}}
    latest: List[pd.DataFrame] = []
    rows_read = 0
    step_span = (np.inf, -np.inf)
    bad_rows: List[str] = []
    frames = table_frames(
        data_root, table, path, sorted(needed), value_cols | {time_col}, bad_rows
    )
    for frame in frames:
        if max_rows is not None:
            frame = frame.iloc[: max_rows - rows_read]
        rows_read += len(frame)
        secs = frame[time_col] * table["time_unit_secs"]
        keep = secs.notna()
        frame = frame[keep].copy()
        frame[TIME_COL] = secs[keep]
        frame[STEP_COL] = np.ceil(secs[keep] / table["step_s"]).astype(np.int64)
        if len(frame):
            step_span = (
                min(step_span[0], frame[STEP_COL].min()),
                max(step_span[1], frame[STEP_COL].max()),
            )
        for join, lookup in joins:
            frame = frame.astype({c: object for c in join["keys"]})
            frame = frame.join(lookup, on=join["keys"])
        for q in range_queries:
            if q["kind"] == "keys":
                keyed = frame.assign(**{KEY_COL: key_hash(frame, q["group_by"])})
                key_parts[query_key(q)].append(key_part(keyed, q))
            else:
                add_values(
                    values[query_key(q)],
                    frame[STEP_COL].to_numpy(),
                    frame[q["value"]].to_numpy(dtype=float),
                )
        if instant_queries:
            latest.append(latest_part(frame, table["series_key"], instant_queries))
        if max_rows is not None and rows_read >= max_rows:
            break
    out: Dict[str, Any] = {
        "keys": {k: merge_key_parts(parts) for k, parts in key_parts.items()},
        "values": {k: sample_steps(acc) for k, acc in values.items()},
        "span": step_span,
        "rows_read": rows_read,
        "bad_rows": len(bad_rows),
    }
    if instant_queries:
        out["instant"], out["boundary"] = split_instant(
            pd.concat(latest, ignore_index=True), instant_queries, lookback_steps(table)
        )
    return out


# ---------------------------------------------------------------- fitting


def zipf_mle(weights: ArrayLike) -> float:
    """Discrete Zipf exponent over ranks 1..K maximizing the likelihood of the
    sorted weights (counts, or value sums used as fractional counts)."""
    w = np.sort(np.asarray(weights, dtype=float))[::-1]
    w = w[w > 0]
    if len(w) < 2:
        return float("nan")
    log_rank = np.log(np.arange(1, len(w) + 1))
    total, weighted_log_rank = w.sum(), (w * log_rank).sum()

    def neg_log_likelihood(s: float) -> float:
        return s * weighted_log_rank + total * np.log(np.exp(-s * log_rank).sum())

    res = minimize_scalar(
        neg_log_likelihood, bounds=ZIPF_THETA_BOUNDS, method="bounded"
    )
    return float(res.x)


def loglog_slope(weights: ArrayLike) -> float:
    """Negated least-squares slope of log(weight) vs log(rank)."""
    w = np.sort(np.asarray(weights, dtype=float))[::-1]
    w = w[w > 0]
    if len(w) < 2:
        return float("nan")
    return float(-np.polyfit(np.log(np.arange(1, len(w) + 1)), np.log(w), 1)[0])


def window_bounds(
    window_estimates: Sequence[float], pooled: float
) -> Tuple[float, float, int]:
    """(lower, upper, n_evals): min and max over the finite per-window
    estimates together with the pooled estimate, so lower <= pooled <= upper."""
    windows = [e for e in window_estimates if np.isfinite(e)]
    candidates = windows + ([pooled] if np.isfinite(pooled) else [])
    if not candidates:
        return float("nan"), float("nan"), 0
    return min(candidates), max(candidates), len(windows)


def spread(prefix: str, per_window: Sequence[float]) -> Dict[str, float]:
    """{prefix}_min, _median and _max over per-window values."""
    if not len(per_window):
        return {}
    return {
        f"{prefix}_min": float(np.min(per_window)),
        f"{prefix}_median": float(np.median(per_window)),
        f"{prefix}_max": float(np.max(per_window)),
    }


def subsample(x: np.ndarray) -> np.ndarray:
    if len(x) <= MAX_FIT_SAMPLES:
        return x
    rng = np.random.default_rng(SAMPLE_SEED)
    return x[rng.choice(len(x), MAX_FIT_SAMPLES, replace=False)]


def significant(comparison: Tuple[float, float], sign: int) -> bool:
    """Whether (R, p) favors the power law (sign=1) or the alternative (sign=-1)."""
    ratio, p = comparison
    return ratio * sign > 0 and p < COMPARE_P_THRESHOLD


def tail_class(comparisons: Dict[str, Tuple[float, float]]) -> str:
    """light unless the power law significantly beats the exponential; then
    power_law or lognormal by whichever significantly wins, else inconclusive."""
    if not significant(comparisons["exponential"], 1):
        return TAIL_LIGHT
    if significant(comparisons["lognormal"], 1):
        return TAIL_POWER_LAW
    if significant(comparisons["lognormal"], -1):
        return TAIL_LOGNORMAL
    return TAIL_HEAVY_INCONCLUSIVE


def xmin_candidates(x_sorted: np.ndarray) -> np.ndarray:
    """Quantile grid of xmin values that leave at least MIN_TAIL_SAMPLES in the tail."""
    levels = np.linspace(*XMIN_GRID_QUANTILES, XMIN_GRID_SIZE)
    grid = np.unique(np.quantile(x_sorted, levels))
    return grid[grid <= x_sorted[-MIN_TAIL_SAMPLES]]


def fit_power_law(x: np.ndarray, compare: bool) -> Dict[str, Any]:
    """Continuous power-law fit of positive values with xmin minimizing the KS
    distance over a quantile grid; with compare, also the likelihood ratio
    against each alternative distribution."""
    result: Dict[str, Any] = {"alpha": np.nan, "xmin": np.nan, "ks_d": np.nan}
    if len(x) < MIN_TAIL_SAMPLES:
        return result
    x = np.sort(x)
    candidates = xmin_candidates(x)
    if not len(candidates):
        return result
    # powerlaw prints xmin search progress unconditionally.
    with warnings.catch_warnings(), np.errstate(all="ignore"), redirect_stdout(
        io.StringIO()
    ):
        warnings.simplefilter("ignore")
        fits = [powerlaw.Fit(x, xmin=xmin, verbose=False) for xmin in candidates]
        distances = np.array([f.power_law.D for f in fits], dtype=float)
        if np.all(np.isnan(distances)):
            return result
        fit = fits[int(np.nanargmin(distances))]
        result.update(
            alpha=fit.power_law.alpha,
            xmin=fit.xmin,
            ks_d=fit.power_law.D,
            tail_frac=np.mean(x >= fit.xmin),
        )
        if not compare or not np.isfinite(result["alpha"]):
            return result
        comparisons = {}
        for alt in POWER_LAW_ALTERNATIVES:
            ratio, p = fit.distribution_compare("power_law", alt)
            result[f"R_{alt}"], result[f"p_{alt}"] = ratio, p
            comparisons[alt] = (ratio, p)
    result["tail_class"] = tail_class(comparisons)
    return result


def median_or_nan(values: Sequence[float]) -> float:
    finite = [v for v in values if v is not None and np.isfinite(v)]
    return float(np.median(finite)) if finite else float("nan")


# ---------------------------------------------------------------- plots


def plot_rank_frequency(
    weights_desc: np.ndarray, thetas: Dict[str, float], title: str, path: Path
) -> None:
    ranks = np.arange(1, len(weights_desc) + 1)
    fig = Figure(figsize=(6, 4))
    ax = fig.subplots()
    ax.loglog(ranks, weights_desc, ".", ms=3, color=OBSERVED_COLOR, label="observed")
    for name, theta in thetas.items():
        if np.isfinite(theta):
            ref = weights_desc.sum() * ranks**-theta / np.sum(ranks**-theta)
            style = "-" if name == "mle" else "--"
            ax.loglog(
                ranks,
                ref,
                style,
                lw=2,
                color=BOUND_COLORS[name],
                label=f"{name} θ={theta:.2f}",
            )
    ax.set(xlabel="rank", ylabel="weight", title=title)
    ax.legend()
    fig.savefig(path, dpi=120, bbox_inches="tight")


def plot_ccdf(x: np.ndarray, fit: Dict[str, Any], title: str, path: Path) -> None:
    xs = np.sort(x)
    ccdf = 1.0 - np.arange(len(xs)) / len(xs)
    fig = Figure(figsize=(6, 4))
    ax = fig.subplots()
    ax.loglog(xs, ccdf, ".", ms=3, color=OBSERVED_COLOR, label="observed")
    alpha, xmin = fit["alpha"], fit["xmin"]
    if np.isfinite(alpha):
        tail = xs[xs >= xmin]
        ref = np.mean(xs >= xmin) * (tail / xmin) ** (1.0 - alpha)
        label = f"power law α={alpha:.2f}, xmin={xmin:.3g}"
        ax.loglog(tail, ref, "-", lw=2, color=BOUND_COLORS["mle"], label=label)
    ax.set(xlabel="value", ylabel="P(X ≥ x)", title=title)
    ax.legend()
    fig.savefig(path, dpi=120, bbox_inches="tight")


def plot_path(plot_dir: Path, dataset: str, name: str) -> Path:
    return plot_dir / f"{dataset}__{name}.png"


# ---------------------------------------------------------------- summaries


def rolling_key_sums(
    agg: pd.DataFrame, columns: Sequence[str], n_steps: int, span: Tuple[int, int]
) -> Iterator[Dict[str, np.ndarray]]:
    """For each evaluation step t in [first + n_steps - 1, last], the per-key
    sums of `columns` over steps t - n_steps + 1 .. t (keys present only).
    Computed as a banded 0/1 matrix times the sparse step x key matrix."""
    first, last = span
    agg = agg[(agg[STEP_COL] >= first) & (agg[STEP_COL] <= last)]
    n_total = last - first + 1
    n_evals = n_total - n_steps + 1
    if n_evals <= 0 or agg.empty:
        return
    _, key_idx = np.unique(agg[KEY_COL].to_numpy(), return_inverse=True)
    coords = (agg[STEP_COL].to_numpy() - first, key_idx)
    shape = (n_total, int(key_idx.max()) + 1)
    mats = {
        c: sparse.csr_matrix((agg[c].to_numpy(dtype=float), coords), shape=shape)
        for c in columns
    }
    # Row e covers steps e .. e + n_steps - 1, i.e. evaluation first + e + n_steps - 1.
    band_rows = np.repeat(np.arange(n_evals), n_steps)
    band_cols = band_rows + np.tile(np.arange(n_steps), n_evals)
    band = sparse.csr_matrix(
        (np.ones(len(band_rows)), (band_rows, band_cols)), shape=(n_evals, n_total)
    )
    for start in range(0, n_evals, EVAL_CHUNK):
        chunk = {
            c: (band[start : start + EVAL_CHUNK] @ m).tocsr() for c, m in mats.items()
        }
        for i in range(min(EVAL_CHUNK, n_evals - start)):
            yield {c: m.data[m.indptr[i] : m.indptr[i + 1]] for c, m in chunk.items()}


def summarize_keys(
    dataset: str,
    q: Dict[str, Any],
    token: str,
    step_s: int,
    agg: pd.DataFrame,
    span: Tuple[int, int],
    min_rows: int,
    min_keys: int,
    plot_dir: Optional[Path],
) -> List[Dict[str, Any]]:
    """One row per weight for one range; agg holds per-step key aggregates
    (range queries) or per-evaluation ones (instant, one step each)."""
    n_steps = range_steps(token, step_s)
    rows = int(agg[COUNT_COL].sum())
    if rows == 0:
        raise ValueError(f"{dataset}/{q['id']}: no rows with non-null group keys")
    columns = {w: COUNT_COL if w == "count" else VALUE_SUM_COL for w in q["weights"]}
    per_key = {}
    for weight, col in columns.items():
        totals = agg.groupby(KEY_COL)[col].sum().to_numpy()
        per_key[weight] = np.sort(totals[totals > 0])[::-1]
    pooled = {weight: zipf_mle(w) for weight, w in per_key.items()}
    estimates: Dict[str, List[float]] = {weight: [] for weight in columns}
    keys_per_eval, rows_per_eval = [], []
    for sums in rolling_key_sums(agg, sorted(set(columns.values())), n_steps, span):
        counts = sums[COUNT_COL]
        n_rows = counts.sum()
        if n_rows == 0:
            # No data in range (a gap in the trace), as summarize_values skips.
            continue
        keys_per_eval.append(int(np.sum(counts > 0)))
        rows_per_eval.append(n_rows)
        for weight, col in columns.items():
            w = sums[col]
            if n_rows >= min_rows and np.sum(w > 0) >= min_keys:
                estimates[weight].append(zipf_mle(w))
    eval_stats = {
        **spread("K_win", keys_per_eval),
        **spread("rows_win", rows_per_eval),
    }
    out = []
    for weight in columns:
        lower, upper, n_evals = window_bounds(estimates[weight], pooled[weight])
        keys = per_key[weight]
        out.append(
            {
                "dataset": dataset,
                "query_id": q["id"],
                "promql": range_promql(q, token),
                "kind": "keys",
                "weight": weight,
                "range": token,
                "range_s": range_secs(token),
                "step_s": step_s,
                "K_total": len(keys),
                "rows_total": rows,
                **eval_stats,
                "n_evals": n_evals,
                "lower": lower,
                "mle": pooled[weight],
                "upper": upper,
                "worst_theta_cms": lower if weight == cms_weight(q) else np.nan,
                "worst_K": eval_stats.get("K_win_max", np.nan),
                "min_N": eval_stats.get("rows_win_min", np.nan),
                "max_N": eval_stats.get("rows_win_max", np.nan),
                **query_targets(q),
                "top1_share": keys[0] / keys.sum() if len(keys) else np.nan,
                "theta_ls": loglog_slope(keys),
            }
        )
        if plot_dir is not None and len(keys):
            plot_rank_frequency(
                keys,
                {"lower": lower, "mle": pooled[weight], "upper": upper},
                f"{dataset}: {range_promql(q, token)} [{weight}]",
                plot_path(plot_dir, dataset, f"{q['id']}__rank_{weight}__{token}"),
            )
    return out


def summarize_values(
    dataset: str,
    q: Dict[str, Any],
    token: str,
    step_s: int,
    acc: Dict[str, Any],
    span: Tuple[int, int],
    pool: Any,
    min_rows: int,
    plot_dir: Optional[Path],
) -> Dict[str, Any]:
    """Row for one range; acc holds a value_sample per step (range queries)
    or per evaluation (instant). Each evaluation merges its steps' samples."""
    n_steps = range_steps(token, step_s)
    steps = acc["steps"]
    if not steps:
        raise ValueError(f"{dataset}/{q['id']}: no positive values")
    n_positive, mle_sample = merge_samples(list(steps.values()))
    jobs = [(mle_sample, True)]
    rows_per_eval = []
    first, last = span
    for t in range(first + n_steps - 1, last + 1):
        parts = [steps[s] for s in range(t - n_steps + 1, t + 1) if s in steps]
        if not parts:
            continue
        n_t, sample = merge_samples(parts)
        rows_per_eval.append(n_t)
        if n_t >= min_rows:
            jobs.append((sample, False))
    fits = pool.starmap(fit_power_law, jobs)
    mle_fit = fits[0]
    if plot_dir is not None:
        plot_ccdf(
            mle_sample,
            mle_fit,
            f"{dataset}: {range_promql(q, token)}",
            plot_path(plot_dir, dataset, f"{q['id']}__ccdf__{token}"),
        )
    lower, upper, n_evals = window_bounds(
        [f["alpha"] for f in fits[1:]], mle_fit["alpha"]
    )
    eval_stats = spread("rows_win", rows_per_eval)
    return {
        "dataset": dataset,
        "query_id": q["id"],
        "promql": range_promql(q, token),
        "kind": "values",
        "weight": "",
        "range": token,
        "range_s": range_secs(token),
        "step_s": step_s,
        "rows_total": acc["n_finite"],
        **eval_stats,
        "n_evals": n_evals,
        "lower": lower,
        "mle": mle_fit["alpha"],
        "upper": upper,
        "min_N": eval_stats.get("rows_win_min", np.nan),
        "max_N": eval_stats.get("rows_win_max", np.nan),
        "worst_alpha_rank": upper,
        "worst_alpha_memory": lower,
        **query_targets(q),
        "dropped_frac": 1.0 - n_positive / acc["n_finite"],
        **{k: v for k, v in mle_fit.items() if k != "alpha"},
    }


def analyze_table(
    cfg: Dict[str, Any],
    table: Dict[str, Any],
    queries: List[Dict[str, Any]],
    data_root: str,
    pool: Any,
    args: argparse.Namespace,
    plot_dir: Optional[Path],
) -> List[Dict[str, Any]]:
    # Load each join once here rather than in every file task.
    joins = [(j, load_join(data_root, table, j)) for j in table.get("joins", [])]
    tasks = [
        (data_root, table, path, queries, joins, args.max_rows)
        for path in expand_files(data_root, table["files"])
    ]
    partials = pool.map(aggregate_file, tasks)
    span = (
        int(min(p["span"][0] for p in partials)),
        int(max(p["span"][1] for p in partials)),
    )
    log.info(
        "read %d rows (%d malformed rows skipped) from %d files, steps %d..%d",
        sum(p["rows_read"] for p in partials),
        sum(p["bad_rows"] for p in partials),
        len(partials),
        *span,
    )
    instant_queries = [q for q in queries if INSTANT in q["range"]]
    boundary: Dict[Tuple[str, str], Any] = {}
    if instant_queries:
        boundary = resolve_boundaries(
            [p.pop("boundary") for p in partials],
            instant_queries,
            lookback_steps(table),
        )
    out: List[Dict[str, Any]] = []
    for q in queries:
        key = query_key(q)
        merge: Any = merge_key_parts if q["kind"] == "keys" else merge_step_samples
        # Pop parts as they are merged to free memory early.
        per_step: Any = (
            merge([p[q["kind"]].pop(key) for p in partials]) if has_range(q) else None
        )
        for token in q["range"]:
            agg: Any
            if token == INSTANT:
                agg = merge([p["instant"].pop(key) for p in partials] + [boundary[key]])
            else:
                agg = per_step
            if q["kind"] == "keys":
                out.extend(
                    summarize_keys(
                        cfg["dataset"],
                        q,
                        token,
                        table["step_s"],
                        agg,
                        span,
                        args.min_eval_rows,
                        args.min_eval_keys,
                        plot_dir,
                    )
                )
            else:
                out.append(
                    summarize_values(
                        cfg["dataset"],
                        q,
                        token,
                        table["step_s"],
                        agg,
                        span,
                        pool,
                        args.min_eval_rows,
                        plot_dir,
                    )
                )
            log.info("%s %s %s %s done", cfg["dataset"], q["id"], q["kind"], token)
    return out


def boom_fit_jobs(
    shifted: np.ndarray, n_chunks: int, min_rows: int
) -> Tuple[List[Tuple[np.ndarray, bool]], List[int]]:
    """Fit jobs for each variate (full series with comparisons, then each
    chunk) and the variate each job belongs to."""
    jobs: List[Tuple[np.ndarray, bool]] = []
    owners: List[int] = []
    for v, y in enumerate(shifted):
        jobs.append((subsample(y[y > 0]), True))
        owners.append(v)
        for chunk in np.array_split(y, n_chunks):
            chunk = chunk[chunk > 0]
            if len(chunk) >= min_rows:
                jobs.append((subsample(chunk), False))
                owners.append(v)
    return jobs, owners


def summarize_boom_series(
    dataset: str,
    q: Dict[str, Any],
    series: str,
    shifted: np.ndarray,
    jobs: List[Tuple[np.ndarray, bool]],
    owners: List[int],
    fits: List[Dict[str, Any]],
    plot_dir: Optional[Path],
) -> Dict[str, Any]:
    """Majority tail class over the variates; alpha and diagnostics are medians
    over the non-light variates, empty if every variate is light."""
    full: Dict[int, Dict[str, Any]] = {}
    samples: Dict[int, np.ndarray] = {}
    chunk_alphas: Dict[int, List[float]] = {v: [] for v in range(len(shifted))}
    for (x, is_full), v, fit in zip(jobs, owners, fits):
        if is_full:
            full[v], samples[v] = fit, x
        else:
            chunk_alphas[v].append(fit["alpha"])
    classes = [f["tail_class"] for f in full.values() if "tail_class" in f]
    chosen = [
        v for v, f in full.items() if f.get("tail_class", TAIL_LIGHT) != TAIL_LIGHT
    ]
    finite = int(np.isfinite(shifted).sum())
    row: Dict[str, Any] = {
        "dataset": dataset,
        "query_id": f"{q['id']}[{series}]",
        "promql": q["promql"],
        "kind": "values",
        "weight": "",
        "K_total": len(shifted),
        "rows_total": finite,
        "dropped_frac": 1.0 - np.sum(shifted > 0) / finite,
        "tail_class": max(sorted(set(classes)), key=classes.count) if classes else "",
        "ok_frac": len(chosen) / len(classes) if classes else np.nan,
        "n_evals": 0,
        **query_targets(q),
    }
    if chosen:
        bounds = [window_bounds(chunk_alphas[v], full[v]["alpha"]) for v in chosen]
        row.update(
            n_evals=sum(b[2] for b in bounds),
            lower=median_or_nan([b[0] for b in bounds]),
            mle=median_or_nan([full[v]["alpha"] for v in chosen]),
            upper=median_or_nan([b[1] for b in bounds]),
        )
        row.update(worst_alpha_rank=row["upper"], worst_alpha_memory=row["lower"])
    for col in ("xmin", "ks_d", "tail_frac") + tuple(
        f"{k}_{alt}" for alt in POWER_LAW_ALTERNATIVES for k in ("R", "p")
    ):
        row[col] = median_or_nan([full[v].get(col, np.nan) for v in chosen])
    if plot_dir is not None and chosen:
        # Show the chosen variate whose alpha is closest to their median.
        median_alpha = median_or_nan([full[v]["alpha"] for v in chosen])
        v = min(chosen, key=lambda v: abs(full[v]["alpha"] - median_alpha))
        plot_ccdf(
            samples[v],
            full[v],
            f"boom {series}: variate {v} of {len(shifted)}",
            plot_path(plot_dir, dataset, f"{series}__ccdf"),
        )
    return row


def analyze_boom(
    cfg: Dict[str, Any],
    table: Dict[str, Any],
    q: Dict[str, Any],
    data_root: str,
    pool: Any,
    args: argparse.Namespace,
    plot_dir: Optional[Path],
) -> List[Dict[str, Any]]:
    """Per-variate tail fits on x - min(x), summarized per series."""
    out = []
    for path in expand_files(data_root, table["files"]):
        variates = read_boom_series(path)
        if args.max_rows is not None:
            variates = variates[:, : args.max_rows]
        shifted = variates - np.nanmin(variates, axis=1, keepdims=True)
        jobs, owners = boom_fit_jobs(shifted, cfg["n_chunks"], args.min_eval_rows)
        fits = pool.starmap(fit_power_law, jobs)
        series = Path(path).parent.name
        out.append(
            summarize_boom_series(
                cfg["dataset"], q, series, shifted, jobs, owners, fits, plot_dir
            )
        )
        log.info("boom %s done", series)
    return out


def analyze_dataset(
    cfg: Dict[str, Any],
    data_root: str,
    pool: Any,
    args: argparse.Namespace,
    plot_dir: Optional[Path],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for name, table in cfg["tables"].items():
        queries = [q for q in cfg["queries"] if q["table"] == name]
        if not queries:
            continue
        log.info("%s: table %s, %d queries", cfg["dataset"], name, len(queries))
        if table["format"] == "boom_arrow":
            for q in queries:
                out.extend(analyze_boom(cfg, table, q, data_root, pool, args, plot_dir))
        else:
            out.extend(
                analyze_table(cfg, table, queries, data_root, pool, args, plot_dir)
            )
    return out


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", required=True, help="fetch_data.sh DATA_ROOT")
    parser.add_argument("--queries", nargs="+", default=DEFAULT_QUERIES)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="plot dir")
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--no-plots", action="store_true")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="rows read per file (BOOM: time steps per series), for smoke runs",
    )
    parser.add_argument(
        "--min-eval-rows",
        type=int,
        default=DEFAULT_MIN_EVAL_ROWS,
        help="skip evaluations with fewer rows (values: fewer positive values)",
    )
    parser.add_argument(
        "--min-eval-keys",
        type=int,
        default=DEFAULT_MIN_EVAL_KEYS,
        help="skip key-query evaluations with fewer distinct keys",
    )
    parser.add_argument("--workers", type=int, default=os.cpu_count())
    return parser.parse_args(argv)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    args = parse_args()
    configs = [load_config(path) for path in args.queries]
    plot_dir = None if args.no_plots else args.out
    if plot_dir is not None:
        plot_dir.mkdir(parents=True, exist_ok=True)
    start = time.time()
    rows: List[Dict[str, Any]] = []
    with Pool(args.workers) as pool:
        for cfg in configs:
            rows.extend(analyze_dataset(cfg, args.data_root, pool, args, plot_dir))
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    summary = pd.DataFrame(rows).reindex(columns=SUMMARY_COLUMNS)
    summary.to_csv(args.summary, index=False, float_format=SUMMARY_FLOAT_FORMAT)
    log.info(
        "wrote %s (%d rows) in %.0fs", args.summary, len(rows), time.time() - start
    )


if __name__ == "__main__":
    main()
