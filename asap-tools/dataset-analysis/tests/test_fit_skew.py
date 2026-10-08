"""Tests for the skew fits, window bounds, config validation and readers."""

import copy
import io
import tarfile
import tempfile
import unittest
from multiprocessing.pool import ThreadPool
from pathlib import Path

import numpy as np
import pandas as pd

import fit_skew

ZIPF_K = 1000
ZIPF_SAMPLES = 2_000_000
ZIPF_TOLERANCE = 0.05
PARETO_SAMPLES = 20_000
PARETO_REL_TOLERANCE = 0.05


def zipf_counts(theta: float, rng: np.random.Generator) -> np.ndarray:
    ranks = np.arange(1, ZIPF_K + 1)
    probs = ranks**-theta / np.sum(ranks**-theta)
    return rng.multinomial(ZIPF_SAMPLES, probs)


class ZipfFitTest(unittest.TestCase):
    def test_recovers_theta(self):
        rng = np.random.default_rng(1)
        for theta in (0.8, 1.2):
            with self.subTest(theta=theta):
                counts = zipf_counts(theta, rng)
                # Rank order is recovered by sorting; key order must not matter.
                rng.shuffle(counts)
                self.assertAlmostEqual(
                    fit_skew.zipf_mle(counts), theta, delta=ZIPF_TOLERANCE
                )

    def test_uniform_is_zero(self):
        self.assertAlmostEqual(fit_skew.zipf_mle(np.full(100, 50.0)), 0.0, places=3)

    def test_zero_weights_ignored(self):
        counts = zipf_counts(1.0, np.random.default_rng(2)).astype(float)
        padded = np.concatenate([counts, np.zeros(500)])
        self.assertAlmostEqual(
            fit_skew.zipf_mle(padded), fit_skew.zipf_mle(counts), places=6
        )

    def test_fewer_than_two_keys_is_nan(self):
        self.assertTrue(np.isnan(fit_skew.zipf_mle([10.0])))
        self.assertTrue(np.isnan(fit_skew.zipf_mle([10.0, 0.0])))
        self.assertTrue(np.isnan(fit_skew.loglog_slope([])))


class PowerLawFitTest(unittest.TestCase):
    def test_recovers_pareto_alpha(self):
        rng = np.random.default_rng(3)
        for shape in (1.0, 2.0, 4.0):
            alpha = shape + 1.0  # pdf exponent of a Pareto with this shape
            with self.subTest(alpha=alpha):
                x = rng.pareto(shape, PARETO_SAMPLES) + 1.0
                fit = fit_skew.fit_power_law(x, compare=False)
                self.assertAlmostEqual(
                    fit["alpha"], alpha, delta=PARETO_REL_TOLERANCE * alpha
                )
                tail = np.sum(x >= fit["xmin"])
                self.assertGreaterEqual(tail, fit_skew.MIN_TAIL_SAMPLES)

    def test_tail_class_synthetic(self):
        exponential = np.random.default_rng(11).exponential(1.0, 20_000)
        self.assertEqual(
            fit_skew.fit_power_law(exponential, compare=True)["tail_class"],
            fit_skew.TAIL_LIGHT,
        )
        # The KS-chosen tail of a lognormal(sigma=1) is its top few percent,
        # where it decays too fast for the power law to beat an exponential,
        # so it is classed light; only heavier lognormal tails reach
        # lognormal or heavy_inconclusive.
        lognormal = np.random.default_rng(12).lognormal(0.0, 1.0, 20_000)
        fit = fit_skew.fit_power_law(lognormal, compare=True)
        self.assertEqual(fit["tail_class"], fit_skew.TAIL_LIGHT)
        self.assertTrue(np.isfinite(fit["alpha"]))
        # A lognormal with large sigma mimics a power-law tail, so an exact
        # Pareto(alpha=2) beats the exponential but not the lognormal.
        pareto = np.random.default_rng(13).pareto(1.0, 20_000) + 1.0
        fit = fit_skew.fit_power_law(pareto, compare=True)
        self.assertIn(
            fit["tail_class"],
            (fit_skew.TAIL_POWER_LAW, fit_skew.TAIL_HEAVY_INCONCLUSIVE),
        )

    def test_tail_class_rule(self):
        def classify(lognormal, exponential):
            return fit_skew.tail_class(
                {"lognormal": lognormal, "exponential": exponential}
            )

        beats_exp = (5.0, 0.01)
        self.assertEqual(classify((2.0, 0.01), (5.0, 0.2)), fit_skew.TAIL_LIGHT)
        self.assertEqual(classify((2.0, 0.01), (-5.0, 0.01)), fit_skew.TAIL_LIGHT)
        self.assertEqual(classify((2.0, 0.01), beats_exp), fit_skew.TAIL_POWER_LAW)
        self.assertEqual(classify((-2.0, 0.01), beats_exp), fit_skew.TAIL_LOGNORMAL)
        self.assertEqual(
            classify((-2.0, 0.5), beats_exp), fit_skew.TAIL_HEAVY_INCONCLUSIVE
        )
        self.assertEqual(
            classify((2.0, 0.5), beats_exp), fit_skew.TAIL_HEAVY_INCONCLUSIVE
        )

    def test_xmin_grid(self):
        x = np.sort(np.random.default_rng(9).pareto(1.5, PARETO_SAMPLES) + 1.0)
        grid = fit_skew.xmin_candidates(x)
        self.assertLessEqual(len(grid), fit_skew.XMIN_GRID_SIZE)
        self.assertGreaterEqual(grid[0], np.quantile(x, 0.5))
        self.assertGreaterEqual(np.sum(x >= grid[-1]), fit_skew.MIN_TAIL_SAMPLES)
        fit = fit_skew.fit_power_law(x, compare=False)
        self.assertIn(fit["xmin"], grid)

    def test_xmin_grid_small_sample(self):
        rng = np.random.default_rng(10)
        # 300 values: p99.9 leaves one value, so the grid stops at 100 tail values.
        x = np.sort(rng.pareto(1.5, 300) + 1.0)
        grid = fit_skew.xmin_candidates(x)
        self.assertGreaterEqual(np.sum(x >= grid[-1]), fit_skew.MIN_TAIL_SAMPLES)
        # 150 values: 100 tail values would reach below the median, so no fit.
        x = np.sort(rng.pareto(1.5, 150) + 1.0)
        self.assertEqual(len(fit_skew.xmin_candidates(x)), 0)
        self.assertTrue(np.isnan(fit_skew.fit_power_law(x, compare=True)["alpha"]))

    def test_too_few_samples_is_nan(self):
        fit = fit_skew.fit_power_law(
            np.arange(1.0, fit_skew.MIN_TAIL_SAMPLES), compare=True
        )
        self.assertTrue(np.isnan(fit["alpha"]))
        self.assertNotIn("tail_class", fit)


def key_step_frames(thetas, rng):
    """Per-step key count frames, one Zipf distribution per step."""
    return [
        pd.DataFrame(
            {
                fit_skew.STEP_COL: step,
                fit_skew.KEY_COL: np.arange(1, ZIPF_K + 1, dtype=np.uint64),
                fit_skew.COUNT_COL: zipf_counts(theta, rng),
            }
        )
        for step, theta in enumerate(thetas)
    ]


COUNT_QUERY = {
    "id": "q",
    "promql": "count by (k) (x)",
    "promql_range": "sum by (k) (count_over_time(x[{range}]))",
    "group_by": ["k"],
    "weights": ["count"],
}
VALUE_QUERY = {
    "id": "v",
    "promql": "quantile(0.99, x)",
    "promql_range": "quantile_over_time(0.99, x[{range}])",
    "value": "x",
}


def summarize_counts(frames, token, span, min_keys=2):
    agg = fit_skew.merge_key_parts(frames)
    return fit_skew.summarize_keys(
        "test", COUNT_QUERY, token, 60, agg, span, 1, min_keys, None
    )


class WindowBoundsTest(unittest.TestCase):
    def test_min_max_count(self):
        self.assertEqual(fit_skew.window_bounds([1.1, 0.7, 1.4], 1.0), (0.7, 1.4, 3))

    def test_pooled_extends_bounds(self):
        # The pooled fit need not lie between the window fits; it widens them.
        self.assertEqual(fit_skew.window_bounds([1.1, 1.2], 1.5), (1.1, 1.5, 2))
        self.assertEqual(fit_skew.window_bounds([1.1, 1.2], 0.9), (0.9, 1.2, 2))

    def test_nan_windows_skipped(self):
        self.assertEqual(
            fit_skew.window_bounds([np.nan, 0.9, np.nan, 1.2], 1.0), (0.9, 1.2, 2)
        )

    def test_no_windows_uses_pooled(self):
        for estimates in ([], [np.nan]):
            self.assertEqual(fit_skew.window_bounds(estimates, 1.3), (1.3, 1.3, 0))

    def test_nothing_finite(self):
        lower, upper, n = fit_skew.window_bounds([np.nan], np.nan)
        self.assertTrue(np.isnan(lower) and np.isnan(upper))
        self.assertEqual(n, 0)


class RangeEvaluationTest(unittest.TestCase):
    def test_skips_small_evaluations(self):
        frames = key_step_frames((0.8, 1.2), np.random.default_rng(5))
        # Step 2 has too few keys to be fitted.
        frames.append(
            pd.DataFrame(
                {
                    fit_skew.STEP_COL: 2,
                    fit_skew.KEY_COL: np.array([1, 2], dtype=np.uint64),
                    fit_skew.COUNT_COL: [1e6, 1],
                }
            )
        )
        (row,) = summarize_counts(frames, "1m", (0, 2), min_keys=10)
        self.assertEqual(row["n_evals"], 2)
        self.assertEqual(row["range_s"], 60)
        self.assertEqual(row["promql"], "sum by (k) (count_over_time(x[1m]))")
        self.assertAlmostEqual(row["lower"], 0.8, delta=ZIPF_TOLERANCE)
        self.assertAlmostEqual(row["upper"], 1.2, delta=ZIPF_TOLERANCE)
        self.assertTrue(row["lower"] <= row["mle"] <= row["upper"])
        self.assertEqual(row["K_total"], ZIPF_K)

    def test_sliding_ranges(self):
        # A 2-step range is evaluated at every step once it is full (3 times
        # over 4 steps), not over disjoint windows.
        frames = key_step_frames((0.8, 1.2, 0.8, 1.2), np.random.default_rng(6))
        (one,) = summarize_counts(frames, "1m", (0, 3))
        (two,) = summarize_counts(frames, "2m", (0, 3))
        self.assertEqual((one["n_evals"], two["n_evals"]), (4, 3))
        self.assertEqual(one["mle"], two["mle"])
        # Merging a 0.8 and a 1.2 step gives something in between.
        self.assertGreater(two["lower"], one["lower"])
        self.assertLess(two["upper"], one["upper"])

    def test_per_evaluation_stats(self):
        # Steps 0..3 with 3, 1, 2, 2 keys; key 1 appears in all.
        frame = pd.DataFrame(
            {
                fit_skew.STEP_COL: [0, 0, 0, 1, 2, 2, 3, 3],
                fit_skew.KEY_COL: np.array([1, 2, 3, 1, 1, 2, 1, 4], dtype=np.uint64),
                fit_skew.COUNT_COL: [5, 1, 1, 7, 2, 2, 1, 9],
            }
        )
        (one,) = summarize_counts([frame], "1m", (0, 3))
        (two,) = summarize_counts([frame], "2m", (0, 3))
        self.assertEqual((one["K_total"], one["rows_total"]), (4, 28))
        self.assertEqual(
            (one["K_win_min"], one["K_win_median"], one["K_win_max"]), (1, 2, 3)
        )
        self.assertEqual(
            (one["rows_win_min"], one["rows_win_median"], one["rows_win_max"]),
            (4, 7, 10),
        )
        # Ranges {0,1}, {1,2}, {2,3}: keys {1,2,3}, {1,2}, {1,2,4}; rows 14, 11, 14.
        self.assertEqual((two["K_win_min"], two["K_win_max"]), (2, 3))
        self.assertEqual((two["rows_win_min"], two["rows_win_max"]), (11, 14))
        self.assertLessEqual(set(one), set(fit_skew.SUMMARY_COLUMNS))

    def test_empty_steps_inside_span(self):
        # Steps 1 and 2 have no rows; a 1-step range there is not evaluated.
        frame = pd.DataFrame(
            {
                fit_skew.STEP_COL: [0, 0, 3, 3],
                fit_skew.KEY_COL: np.array([1, 2, 1, 2], dtype=np.uint64),
                fit_skew.COUNT_COL: [3, 1, 3, 1],
            }
        )
        (row,) = summarize_counts([frame], "1m", (0, 3))
        self.assertEqual(row["n_evals"], 2)
        # Empty evaluations are gaps, not evaluations that saw zero rows.
        self.assertEqual((row["rows_win_min"], row["K_win_min"]), (4, 2))

    def test_merge_samples_is_uniform(self):
        # Two parts, 10x apart in size, merged into a sample capped at
        # MAX_FIT_SAMPLES: each part's share follows its size.
        big = fit_skew.value_sample(np.zeros(10 * fit_skew.MAX_FIT_SAMPLES))
        small = fit_skew.value_sample(np.ones(fit_skew.MAX_FIT_SAMPLES))
        total, merged = fit_skew.merge_samples([big, small])
        self.assertEqual(total, 11 * fit_skew.MAX_FIT_SAMPLES)
        self.assertEqual(len(merged), fit_skew.MAX_FIT_SAMPLES)
        self.assertAlmostEqual(merged.mean(), 1 / 11, delta=0.01)

    def test_merge_samples_over_a_billion(self):
        # Parts standing for 9e8 and 1e8 values (over numpy's hypergeometric
        # limit together): shares still follow the sizes, within each prefix.
        n = fit_skew.MAX_FIT_SAMPLES
        big = (900_000_000, np.zeros(n))
        small = (100_000_000, np.ones(n))
        total, merged = fit_skew.merge_samples([big, small])
        self.assertEqual(total, 1_000_000_000)
        self.assertEqual(len(merged), n)
        self.assertAlmostEqual(merged.mean(), 0.1, delta=0.01)

    def test_summarize_values_ranges(self):
        rng = np.random.default_rng(7)
        acc = {
            "n_finite": 4000,
            "steps": {
                w: fit_skew.value_sample(rng.pareto(1.5, 1000) + 1.0) for w in range(4)
            },
        }
        rows = []
        # One thread: redirect_stdout in fit_power_law is process-wide.
        with ThreadPool(1) as pool:
            for token in ("1m", "4m"):
                rows.append(
                    fit_skew.summarize_values(
                        "test", VALUE_QUERY, token, 60, acc, (0, 3), pool, 1, None
                    )
                )
        self.assertEqual([r["n_evals"] for r in rows], [4, 1])
        self.assertEqual([r["rows_win_median"] for r in rows], [1000, 4000])
        self.assertNotIn("K_win_median", rows[0])
        self.assertLessEqual(set(rows[0]), set(fit_skew.SUMMARY_COLUMNS))
        for row in rows:
            self.assertTrue(row["lower"] <= row["mle"] <= row["upper"])


def latest_rows(samples):
    """(series, time_s) pairs -> latest_part-style rows with step = ceil(t / 60)."""
    series, times = zip(*samples)
    times = np.array(times, dtype=float)
    return pd.DataFrame(
        {
            fit_skew.SERIES_COL: np.array(series, dtype=np.uint64),
            fit_skew.STEP_COL: np.ceil(times / 60).astype(np.int64),
            fit_skew.TIME_COL: times,
            "_key:k": np.array(series, dtype=np.uint64),
        }
    )


INSTANT_QUERY = {**COUNT_QUERY, "kind": "keys", "range": ["instant"]}
INSTANT_KEY = fit_skew.query_key(INSTANT_QUERY)


def instant_counts(parts):
    """{(step, key): count} from instant_parts outputs."""
    agg = fit_skew.merge_key_parts([p[INSTANT_KEY] for p in parts])
    return {
        (int(s), int(k)): int(c)
        for s, k, c in agg[
            [fit_skew.STEP_COL, fit_skew.KEY_COL, fit_skew.COUNT_COL]
        ].itertuples(index=False)
    }


def split_files(files, lookback, chunk_steps=fit_skew.BOUNDARY_CHUNK_STEPS):
    """instant parts computed per file then merged via resolve_boundaries."""
    parts, boundary = [], []
    for samples in files:
        inner, edge = fit_skew.split_instant(
            latest_rows(samples), [INSTANT_QUERY], lookback
        )
        parts.append(inner)
        boundary.append(edge)
    parts.append(
        fit_skew.resolve_boundaries(boundary, [INSTANT_QUERY], lookback, chunk_steps)
    )
    return instant_counts(parts)


class InstantEvaluationTest(unittest.TestCase):
    def test_lookback_carries_last_sample(self):
        # Series 1 is sampled at steps 1 and 2 then stops: with a 3-step
        # lookback it is still seen at steps 3 and 4, not at 5.
        counts = split_files([[(1, 60), (1, 120)]], lookback=3)
        self.assertEqual(sorted(counts), [(1, 1), (2, 1), (3, 1), (4, 1)])

    def test_gap_shorter_than_lookback(self):
        # Samples at steps 1 and 3: step 2 still sees the step-1 sample once.
        counts = split_files([[(1, 60), (1, 180)]], lookback=5)
        self.assertEqual(counts[(2, 1)], 1)
        self.assertEqual(counts[(3, 1)], 1)

    def test_latest_sample_per_step(self):
        # Two samples in step 1 count once.
        counts = split_files([[(1, 30), (1, 55)]], lookback=1)
        self.assertEqual(counts, {(1, 1): 1})

    def test_split_across_files_matches_one_file(self):
        rng = np.random.default_rng(8)
        samples = [
            (int(series), float(t))
            for series in range(1, 6)
            for t in np.sort(rng.choice(np.arange(1, 1200), 15, replace=False))
        ]
        samples.sort(key=lambda st: st[1])
        whole = split_files([samples], lookback=5)
        # Consecutive time chunks, cut inside a step so step 10 spans both files.
        cut = [s for s in samples if s[1] <= 570], [s for s in samples if s[1] > 570]
        self.assertEqual(split_files(list(cut), lookback=5), whole)

    def test_boundary_chunks_match_one_pass(self):
        # Many short files (almost every sample a boundary one), stitched in
        # chunks of 1 and 3 steps or in one pass: the same counts.
        rng = np.random.default_rng(9)
        samples = [
            (int(series), float(t))
            for series in range(1, 6)
            for t in np.sort(rng.choice(np.arange(1, 1200), 25, replace=False))
        ]
        samples.sort(key=lambda st: st[1])
        files = [
            [s for s in samples if lo < s[1] <= lo + 90] for lo in range(0, 1200, 90)
        ]
        whole = split_files(files, lookback=5, chunk_steps=1000)
        for chunk_steps in (1, 3):
            self.assertEqual(
                split_files(files, lookback=5, chunk_steps=chunk_steps), whole
            )

    def test_interleaved_files_fail(self):
        files = [[(1, 60), (1, 300)], [(1, 180)]]
        with self.assertRaises(ValueError):
            split_files(files, lookback=5)

    def test_lookback_steps(self):
        self.assertEqual(fit_skew.lookback_steps({"step_s": 60}), 5)
        # A sampling period longer than 5 minutes is one step.
        self.assertEqual(fit_skew.lookback_steps({"step_s": 600}), 1)


def boom_inputs(classes):
    """Fake per-variate full fits (alpha = index + 2) with the given classes."""
    shifted = np.ones((len(classes), 10))
    jobs = [(np.ones(10), True) for _ in classes]
    fits = [
        {
            "alpha": v + 2.0,
            "xmin": 1.0,
            "ks_d": 0.01,
            "tail_frac": 0.1,
            "R_lognormal": -1.0,
            "p_lognormal": 0.5,
            "R_exponential": -2.0 if cls == fit_skew.TAIL_LIGHT else 4.0,
            "p_exponential": 0.01,
            "tail_class": cls,
        }
        for v, cls in enumerate(classes)
    ]
    return shifted, jobs, list(range(len(classes))), fits


LIGHT = fit_skew.TAIL_LIGHT
HEAVY = fit_skew.TAIL_HEAVY_INCONCLUSIVE


class BoomSummaryTest(unittest.TestCase):
    def summarize(self, classes):
        q = {"id": "t", "promql": "quantile(0.99, target)"}
        return fit_skew.summarize_boom_series(
            "boom", q, "s", *boom_inputs(classes), None
        )

    def test_alpha_over_non_light_variates(self):
        row = self.summarize([HEAVY, LIGHT, HEAVY, fit_skew.TAIL_POWER_LAW])
        self.assertEqual(row["tail_class"], HEAVY)
        self.assertEqual(row["ok_frac"], 0.75)
        # Non-light variates 0, 2, 3 have alpha 2, 4, 5.
        self.assertEqual(row["mle"], 4.0)
        self.assertTrue(row["lower"] <= row["mle"] <= row["upper"])
        self.assertEqual(
            (row["worst_alpha_memory"], row["worst_alpha_rank"]),
            (row["lower"], row["upper"]),
        )
        # Every summary field must be a CSV column, or it is silently dropped.
        self.assertLessEqual(set(row), set(fit_skew.SUMMARY_COLUMNS))
        # R/p come from the same non-light variates.
        self.assertEqual(row["R_exponential"], 4.0)

    def test_majority_light_still_reports_heavy_alpha(self):
        row = self.summarize([HEAVY, LIGHT, LIGHT, LIGHT])
        self.assertEqual(row["tail_class"], LIGHT)
        self.assertEqual(row["ok_frac"], 0.25)
        self.assertEqual(row["mle"], 2.0)
        self.assertEqual(row["R_exponential"], 4.0)

    def test_all_light_has_no_alpha(self):
        row = self.summarize([LIGHT, LIGHT])
        self.assertEqual(row["tail_class"], LIGHT)
        self.assertEqual(row["ok_frac"], 0.0)
        self.assertNotIn("mle", row)
        self.assertTrue(np.isnan(row["R_exponential"]))

    def test_no_compared_variates(self):
        q = {"id": "t", "promql": "quantile(0.99, target)"}
        shifted, jobs, owners, _ = boom_inputs([LIGHT])
        fits = [{"alpha": np.nan, "xmin": np.nan, "ks_d": np.nan}]
        row = fit_skew.summarize_boom_series(
            "boom", q, "s", shifted, jobs, owners, fits, None
        )
        self.assertEqual(row["tail_class"], "")
        self.assertTrue(np.isnan(row["ok_frac"]))


VALID_CONFIG = {
    "dataset": "d",
    "tables": {
        "t": {
            "label_columns": ["a", "b"],
            "value_columns": ["v"],
            "step_s": 60,
            "series_key": ["a", "b"],
        },
    },
    "queries": [
        {
            "id": "keys",
            "table": "t",
            "kind": "keys",
            "group_by": ["a"],
            "value": "v",
            "weights": ["count", "value"],
            "promql": "count by (a) (v)",
            "promql_range": "sum by (a) (count_over_time(v[{range}]))",
            "range": ["instant", "5m"],
        },
        {
            "id": "vals",
            "table": "t",
            "kind": "values",
            "group_by": [],
            "value": "v",
            "promql": "quantile(0.99, v)",
            "range": ["instant"],
        },
    ],
}


class ValidateConfigTest(unittest.TestCase):
    def test_valid(self):
        fit_skew.validate_config(VALID_CONFIG)

    def test_invalid(self):
        cases = {
            "unknown table": ("table", "missing"),
            "unknown label": ("group_by", ["c"]),
            "unknown value": ("value", "w"),
            "bad kind": ("kind", "other"),
            "bad weight": ("weights", ["sum"]),
            "no weights": ("weights", []),
            "no group_by": ("group_by", []),
        }
        for name, (field, value) in cases.items():
            with self.subTest(name):
                cfg = copy.deepcopy(VALID_CONFIG)
                cfg["queries"][0][field] = value
                with self.assertRaises(ValueError):
                    fit_skew.validate_config(cfg)

    def test_bad_ranges(self):
        cases = {
            "no range": [],
            "bad duration": ["5x"],
            "not a multiple of step_s": ["90s"],
        }
        for name, ranges in cases.items():
            with self.subTest(name):
                cfg = copy.deepcopy(VALID_CONFIG)
                cfg["queries"][0]["range"] = ranges
                with self.assertRaises(ValueError):
                    fit_skew.validate_config(cfg)

    def test_range_needs_promql_range(self):
        cfg = copy.deepcopy(VALID_CONFIG)
        cfg["queries"][1]["range"] = ["5m"]
        with self.assertRaises(ValueError):
            fit_skew.validate_config(cfg)

    def test_instant_needs_series_key(self):
        cfg = copy.deepcopy(VALID_CONFIG)
        del cfg["tables"]["t"]["series_key"]
        with self.assertRaises(ValueError):
            fit_skew.validate_config(cfg)

    def test_bad_step(self):
        for step in (None, 0, 1.5):
            with self.subTest(step=step):
                cfg = copy.deepcopy(VALID_CONFIG)
                cfg["tables"]["t"]["step_s"] = step
                with self.assertRaises(ValueError):
                    fit_skew.validate_config(cfg)

    def test_value_weight_needs_value(self):
        cfg = copy.deepcopy(VALID_CONFIG)
        del cfg["queries"][0]["value"]
        with self.assertRaises(ValueError):
            fit_skew.validate_config(cfg)

    def test_values_query_needs_value(self):
        cfg = copy.deepcopy(VALID_CONFIG)
        del cfg["queries"][1]["value"]
        with self.assertRaises(ValueError):
            fit_skew.validate_config(cfg)

    def test_missing_files_fail(self):
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaises(FileNotFoundError):
                fit_skew.expand_files(root, ["nothing-*.csv"])


class ReaderTest(unittest.TestCase):
    def test_alibaba_tar_streaming(self):
        # CallGraph shards contain rows with extra fields and rt == "None".
        csv = (
            "timestamp,um,dm,rt\n"
            "1000,MS_1,MS_2,3.0\n"
            "2000,MS_1,MS_3,None\n"
            "3000,MS_1,MS_2,4.0,extra\n"
            "61000,MS_2,MS_3,5.0\n"
        ).encode()
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "t.tar.gz"
            with tarfile.open(path, "w:gz") as archive:
                info = tarfile.TarInfo("t.csv")
                info.size = len(csv)
                archive.addfile(info, io.BytesIO(csv))
            table = {"format": "alibaba_tar"}
            bad: list = []
            frames = list(
                fit_skew.table_frames(
                    root, table, str(path), ["timestamp", "um", "rt"], {"rt"}, bad
                )
            )
        df = frames[0]
        self.assertEqual(len(bad), 1)
        self.assertEqual(list(df["um"].astype(str)), ["MS_1", "MS_1", "MS_2"])
        self.assertTrue(np.isnan(df["rt"][1]))

    def test_google_column_names(self):
        schema = (
            "file pattern,field number,content,format,mandatory\n"
            "task_usage/part-?????-of-?????.csv.gz,2,CPU rate,FLOAT,NO\n"
            "task_usage/part-?????-of-?????.csv.gz,1,start time,INTEGER,YES\n"
            "job_events/part-?????-of-?????.csv.gz,1,time,INTEGER,YES\n"
        )
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "schema.csv"
            path.write_text(schema)
            self.assertEqual(
                fit_skew.google_column_names(str(path), "task_usage"),
                ["start_time", "cpu_rate"],
            )
            with self.assertRaises(ValueError):
                fit_skew.google_column_names(str(path), "machine_events")


if __name__ == "__main__":
    unittest.main()
