#!/usr/bin/env python3
"""Fit label skew (Zipf theta) and value skew (power-law alpha) for trace query sets.

Key queries fit a discrete Zipf exponent to the rank-frequency of the group-by
key; value queries fit a continuous power law to the queried column. Every fit
is repeated per window: lower/upper are the min/max over windows and mle is the
fit on all data.
"""

import argparse
import glob
import io
import logging
import os
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
from scipy.optimize import minimize_scalar

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_QUERIES = sorted(str(p) for p in (SCRIPT_DIR / "queries").glob("*.yaml"))
DEFAULT_SUMMARY = SCRIPT_DIR / "results" / "skew_summary.csv"
DEFAULT_OUT = SCRIPT_DIR / "out"

ZIPF_THETA_BOUNDS = (0.0, 5.0)
# Each power-law fit uses a uniform subsample of at most this many values.
MAX_FIT_SAMPLES = 100_000
# xmin is chosen by KS distance over this many quantiles of the sample, and only
# where at least MIN_TAIL_SAMPLES values remain in the tail.
XMIN_GRID_SIZE = 50
XMIN_GRID_QUANTILES = (0.5, 0.999)
MIN_TAIL_SAMPLES = 100
SAMPLE_SEED = 0
DEFAULT_MIN_WINDOW_ROWS = 200
DEFAULT_MIN_WINDOW_KEYS = 2
# A likelihood-ratio comparison is significant when its p is below this.
COMPARE_P_THRESHOLD = 0.1
TAIL_LIGHT = "light"
TAIL_POWER_LAW = "power_law"
TAIL_LOGNORMAL = "lognormal"
TAIL_HEAVY_INCONCLUSIVE = "heavy_inconclusive"
POWER_LAW_ALTERNATIVES = ("lognormal", "exponential")

CSV_BLOCK_BYTES = 64 << 20
CSV_NULL_VALUES = ["", "None", "NULL", "NaN", "nan"]
LABEL_TYPE = pa.dictionary(pa.int32(), pa.string())

WINDOW_COL = "_window"
COUNT_COL = "count"
VALUE_SUM_COL = "value_sum"
KEY_WEIGHTS = {"count", "value"}
QUERY_KINDS = {"keys", "values"}

SUMMARY_COLUMNS = [
    "dataset",
    "query_id",
    "promql",
    "kind",
    "weight",
    "window_len_s",
    "K",
    "rows",
    "n_windows",
    "lower",
    "mle",
    "upper",
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


def validate_query(q: Dict[str, Any], table: Dict[str, Any], where: str) -> None:
    if q["kind"] not in QUERY_KINDS:
        raise ValueError(f"{where}: kind must be one of {sorted(QUERY_KINDS)}")
    for col in q["group_by"]:
        if col not in table["label_columns"]:
            raise ValueError(f"{where}: {col!r} is not a label column")
    value = q.get("value")
    if value is not None and value not in table["value_columns"]:
        raise ValueError(f"{where}: {value!r} is not a value column")
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


def validate_window_lengths(lengths: Sequence[int], where: str) -> None:
    """Coarser windows are built by merging finest windows, so every length
    must be a multiple of the first (finest) one."""
    if not lengths or list(lengths) != sorted(set(lengths)):
        raise ValueError(f"{where}: window_lengths_s must be ascending and unique")
    if any(length % lengths[0] for length in lengths):
        raise ValueError(f"{where}: window_lengths_s must be multiples of the first")


def validate_config(cfg: Dict[str, Any]) -> None:
    if "window_lengths_s" in cfg:
        validate_window_lengths(cfg["window_lengths_s"], cfg["dataset"])
    for q in cfg["queries"]:
        where = f"{cfg['dataset']}/{q['id']}"
        table = cfg["tables"].get(q["table"])
        if table is None:
            raise ValueError(f"{where}: unknown table {q['table']!r}")
        validate_query(q, table, where)


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


def merge_key_parts(parts: List[pd.DataFrame], group_by: List[str]) -> pd.DataFrame:
    """Sum per-(window, key) counts and value sums; indexed by window and keys."""
    return pd.concat(parts, ignore_index=True).groupby([WINDOW_COL] + group_by).sum()


def key_part(frame: pd.DataFrame, q: Dict[str, Any]) -> pd.DataFrame:
    keys = [WINDOW_COL] + q["group_by"]
    if "value" in q["weights"]:
        clipped = frame.assign(**{VALUE_SUM_COL: frame[q["value"]].clip(lower=0)})
        part = clipped.groupby(keys, observed=True, sort=False).agg(
            **{
                COUNT_COL: (VALUE_SUM_COL, "size"),
                VALUE_SUM_COL: (VALUE_SUM_COL, "sum"),
            }
        )
    else:
        part = frame.groupby(keys, observed=True, sort=False).size().to_frame(COUNT_COL)
    # Categories differ between batches, so merge on plain values.
    return part.reset_index().astype({c: object for c in q["group_by"]})


def add_values(acc: Dict[str, Any], windows: np.ndarray, values: np.ndarray) -> None:
    finite = np.isfinite(values)
    positive = values > 0
    acc["n_finite"] += int(finite.sum())
    for w in np.unique(windows[positive]):
        acc["windows"].setdefault(int(w), []).append(values[positive & (windows == w)])


def aggregate_file(task: Tuple[Any, ...]) -> Dict[str, Any]:
    """Per-window key aggregates and positive values for one file."""
    data_root, table, path, queries, window_len_s, max_time_secs, max_rows = task
    time_col = table["time_column"]
    value_cols = {q["value"] for q in queries if q.get("value")}
    join_cols = [c for j in table.get("joins", []) for c in j["columns"]]
    needed = {time_col} | value_cols
    needed |= {c for q in queries for c in q["group_by"] if c not in join_cols}
    needed |= {c for j in table.get("joins", []) for c in j["keys"]}
    joins = [(j, load_join(data_root, table, j)) for j in table.get("joins", [])]

    key_parts: Dict[Tuple[str, str], List[pd.DataFrame]] = {}
    values: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for q in queries:
        if q["kind"] == "keys":
            key_parts[query_key(q)] = []
        else:
            values[query_key(q)] = {"n_finite": 0, "windows": {}}
    rows_read = 0
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
        if max_time_secs is not None:
            keep &= secs < max_time_secs
        frame = frame[keep].copy()
        frame[WINDOW_COL] = np.floor(secs[keep] / window_len_s).astype(np.int64)
        for join, lookup in joins:
            frame = frame.astype({c: object for c in join["keys"]})
            frame = frame.join(lookup, on=join["keys"])
        for q in queries:
            if q["kind"] == "keys":
                key_parts[query_key(q)].append(key_part(frame, q))
            else:
                add_values(
                    values[query_key(q)],
                    frame[WINDOW_COL].to_numpy(),
                    frame[q["value"]].to_numpy(dtype=float),
                )
        if max_rows is not None and rows_read >= max_rows:
            break
    keys = {
        query_key(q): merge_key_parts(key_parts[query_key(q)], q["group_by"])
        for q in queries
        if q["kind"] == "keys"
    }
    return {
        "keys": keys,
        "values": values,
        "rows_read": rows_read,
        "bad_rows": len(bad_rows),
    }


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
    """(lower, upper, n_windows): min and max over the finite per-window
    estimates together with the pooled estimate, so lower <= pooled <= upper."""
    windows = [e for e in window_estimates if np.isfinite(e)]
    candidates = windows + ([pooled] if np.isfinite(pooled) else [])
    if not candidates:
        return float("nan"), float("nan"), 0
    return min(candidates), max(candidates), len(windows)


def coarsen_keys(agg: pd.DataFrame, factor: int, group_by: List[str]) -> pd.DataFrame:
    """Merge every `factor` consecutive finest windows of a key aggregate."""
    frame = agg.reset_index()
    frame[WINDOW_COL] //= factor
    return merge_key_parts([frame], group_by)


def coarsen_values(
    windows: Dict[int, np.ndarray], factor: int
) -> Dict[int, np.ndarray]:
    """Concatenate the values of every `factor` consecutive finest windows."""
    parts: Dict[int, List[np.ndarray]] = {}
    for w, x in windows.items():
        parts.setdefault(w // factor, []).append(x)
    return {w: np.concatenate(xs) for w, xs in parts.items()}


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


def summarize_keys(
    dataset: str,
    q: Dict[str, Any],
    agg: pd.DataFrame,
    window_lengths: Sequence[int],
    min_rows: int,
    min_keys: int,
    plot_dir: Optional[Path],
) -> List[Dict[str, Any]]:
    """One row per weight and window length; agg holds finest-window counts."""
    rows = int(agg[COUNT_COL].sum())
    if rows == 0:
        raise ValueError(f"{dataset}/{q['id']}: no rows with non-null group keys")
    columns = {w: COUNT_COL if w == "count" else VALUE_SUM_COL for w in q["weights"]}
    per_key = {}
    for weight, col in columns.items():
        totals = agg.groupby(level=q["group_by"])[col].sum().to_numpy()
        per_key[weight] = np.sort(totals[totals > 0])[::-1]
    pooled = {weight: zipf_mle(w) for weight, w in per_key.items()}
    out = []
    for window_len in window_lengths:
        coarse = coarsen_keys(agg, window_len // window_lengths[0], q["group_by"])
        for weight, col in columns.items():
            estimates = []
            for _, window in coarse.groupby(level=WINDOW_COL):
                w = window[col].to_numpy()
                if window[COUNT_COL].sum() >= min_rows and np.sum(w > 0) >= min_keys:
                    estimates.append(zipf_mle(w))
            lower, upper, n_windows = window_bounds(estimates, pooled[weight])
            keys = per_key[weight]
            out.append(
                {
                    "dataset": dataset,
                    "query_id": q["id"],
                    "promql": q["promql"],
                    "kind": "keys",
                    "weight": weight,
                    "window_len_s": window_len,
                    "K": len(keys),
                    "rows": rows,
                    "n_windows": n_windows,
                    "lower": lower,
                    "mle": pooled[weight],
                    "upper": upper,
                    "top1_share": keys[0] / keys.sum() if len(keys) else np.nan,
                    "theta_ls": loglog_slope(keys),
                }
            )
            if plot_dir is not None and len(keys):
                plot_rank_frequency(
                    keys,
                    {"lower": lower, "mle": pooled[weight], "upper": upper},
                    f"{dataset}: {q['promql']} [{weight}, {window_len}s windows]",
                    plot_path(
                        plot_dir, dataset, f"{q['id']}__rank_{weight}__{window_len}s"
                    ),
                )
    return out


def summarize_values(
    dataset: str,
    q: Dict[str, Any],
    acc: Dict[str, Any],
    window_lengths: Sequence[int],
    pool: Any,
    min_rows: int,
    plot_dir: Optional[Path],
) -> List[Dict[str, Any]]:
    """One row per window length; acc holds positive values per finest window.
    Coarser windows concatenate the full finest-window values, then subsample."""
    finest = {w: np.concatenate(parts) for w, parts in acc["windows"].items()}
    if not finest:
        raise ValueError(f"{dataset}/{q['id']}: no positive values")
    all_values = np.concatenate(list(finest.values()))
    mle_sample = subsample(all_values)
    jobs = [(mle_sample, True)]
    job_window_lens = [0]
    for window_len in window_lengths:
        coarse = coarsen_values(finest, window_len // window_lengths[0])
        for x in coarse.values():
            if len(x) >= min_rows:
                jobs.append((subsample(x), False))
                job_window_lens.append(window_len)
    fits = pool.starmap(fit_power_law, jobs)
    mle_fit = fits[0]
    if plot_dir is not None:
        plot_ccdf(
            mle_sample,
            mle_fit,
            f"{dataset}: {q['value']} ({q['id']})",
            plot_path(plot_dir, dataset, f"{q['id']}__ccdf"),
        )
    out = []
    for window_len in window_lengths:
        alphas = [
            f["alpha"] for f, wl in zip(fits, job_window_lens) if wl == window_len
        ]
        lower, upper, n_windows = window_bounds(alphas, mle_fit["alpha"])
        out.append(
            {
                "dataset": dataset,
                "query_id": q["id"],
                "promql": q["promql"],
                "kind": "values",
                "weight": "",
                "window_len_s": window_len,
                "rows": acc["n_finite"],
                "n_windows": n_windows,
                "lower": lower,
                "mle": mle_fit["alpha"],
                "upper": upper,
                "dropped_frac": 1.0 - len(all_values) / acc["n_finite"],
                **{k: v for k, v in mle_fit.items() if k != "alpha"},
            }
        )
    return out


def analyze_table(
    cfg: Dict[str, Any],
    table: Dict[str, Any],
    queries: List[Dict[str, Any]],
    data_root: str,
    pool: Any,
    args: argparse.Namespace,
    plot_dir: Optional[Path],
) -> List[Dict[str, Any]]:
    tasks = [
        (
            data_root,
            table,
            path,
            queries,
            cfg["window_lengths_s"][0],
            cfg.get("max_time_secs"),
            args.max_rows,
        )
        for path in expand_files(data_root, table["files"])
    ]
    partials = pool.map(aggregate_file, tasks)
    log.info(
        "read %d rows (%d malformed rows skipped) from %d files",
        sum(p["rows_read"] for p in partials),
        sum(p["bad_rows"] for p in partials),
        len(partials),
    )
    out: List[Dict[str, Any]] = []
    for q in queries:
        key = query_key(q)
        if q["kind"] == "keys":
            agg = merge_key_parts(
                [p["keys"][key].reset_index() for p in partials], q["group_by"]
            )
            out.extend(
                summarize_keys(
                    cfg["dataset"],
                    q,
                    agg,
                    cfg["window_lengths_s"],
                    args.min_window_rows,
                    args.min_window_keys,
                    plot_dir,
                )
            )
        else:
            acc: Dict[str, Any] = {"n_finite": 0, "windows": {}}
            for p in partials:
                acc["n_finite"] += p["values"][key]["n_finite"]
                for w, parts in p["values"][key]["windows"].items():
                    acc["windows"].setdefault(w, []).extend(parts)
            out.extend(
                summarize_values(
                    cfg["dataset"],
                    q,
                    acc,
                    cfg["window_lengths_s"],
                    pool,
                    args.min_window_rows,
                    plot_dir,
                )
            )
        log.info("%s %s %s done", cfg["dataset"], q["id"], q["kind"])
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
        "K": len(shifted),
        "rows": finite,
        "dropped_frac": 1.0 - np.sum(shifted > 0) / finite,
        "tail_class": max(sorted(set(classes)), key=classes.count) if classes else "",
        "ok_frac": len(chosen) / len(classes) if classes else np.nan,
        "n_windows": 0,
    }
    if chosen:
        bounds = [window_bounds(chunk_alphas[v], full[v]["alpha"]) for v in chosen]
        row.update(
            n_windows=sum(b[2] for b in bounds),
            lower=median_or_nan([b[0] for b in bounds]),
            mle=median_or_nan([full[v]["alpha"] for v in chosen]),
            upper=median_or_nan([b[1] for b in bounds]),
        )
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
        jobs, owners = boom_fit_jobs(shifted, cfg["n_chunks"], args.min_window_rows)
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
        "--min-window-rows",
        type=int,
        default=DEFAULT_MIN_WINDOW_ROWS,
        help="skip windows with fewer rows (values: fewer positive values)",
    )
    parser.add_argument(
        "--min-window-keys",
        type=int,
        default=DEFAULT_MIN_WINDOW_KEYS,
        help="skip key-query windows with fewer distinct keys",
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
    summary.to_csv(args.summary, index=False, float_format="%.6g")
    log.info(
        "wrote %s (%d rows) in %.0fs", args.summary, len(rows), time.time() - start
    )


if __name__ == "__main__":
    main()
