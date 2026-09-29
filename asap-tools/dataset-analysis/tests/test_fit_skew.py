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


def key_window_frames(thetas, rng):
    """Finest-window key count frames, one Zipf window per theta."""
    frames = [
        pd.DataFrame(
            {
                fit_skew.WINDOW_COL: window,
                "k": np.arange(ZIPF_K).astype(str).astype(object),
                fit_skew.COUNT_COL: zipf_counts(theta, rng),
            }
        )
        for window, theta in enumerate(thetas)
    ]
    return frames


COUNT_QUERY = {
    "id": "q",
    "promql": "count by (k) (x)",
    "group_by": ["k"],
    "weights": ["count"],
}


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

    def test_summarize_keys_skips_small_windows(self):
        frames = key_window_frames((0.8, 1.2), np.random.default_rng(5))
        # Window 2 has too few keys to be fitted.
        frames.append(
            pd.DataFrame(
                {fit_skew.WINDOW_COL: 2, "k": ["0", "1"], fit_skew.COUNT_COL: [1e6, 1]}
            )
        )
        agg = fit_skew.merge_key_parts(frames, ["k"])
        (row,) = fit_skew.summarize_keys("test", COUNT_QUERY, agg, [60], 1, 10, None)
        self.assertEqual(row["n_windows"], 2)
        self.assertAlmostEqual(row["lower"], 0.8, delta=ZIPF_TOLERANCE)
        self.assertAlmostEqual(row["upper"], 1.2, delta=ZIPF_TOLERANCE)
        self.assertTrue(row["lower"] <= row["mle"] <= row["upper"])
        self.assertEqual(row["K_total"], ZIPF_K)
        self.assertEqual(row["window_len_s"], 60)

    def test_summarize_keys_window_lengths(self):
        frames = key_window_frames((0.8, 1.2, 0.8, 1.2), np.random.default_rng(6))
        agg = fit_skew.merge_key_parts(frames, ["k"])
        fine, coarse = fit_skew.summarize_keys(
            "test", COUNT_QUERY, agg, [60, 120], 1, 2, None
        )
        self.assertEqual((fine["window_len_s"], coarse["window_len_s"]), (60, 120))
        self.assertEqual((fine["n_windows"], coarse["n_windows"]), (4, 2))
        self.assertEqual(fine["mle"], coarse["mle"])
        # Merging a 0.8 and a 1.2 window gives something in between.
        self.assertGreater(coarse["lower"], fine["lower"])
        self.assertLess(coarse["upper"], fine["upper"])
        for row in (fine, coarse):
            self.assertTrue(row["lower"] <= row["mle"] <= row["upper"])

    def test_per_window_stats(self):
        # Finest windows 0..3 with 3, 1, 2, 2 keys; key "a" appears in all.
        frame = pd.DataFrame(
            {
                fit_skew.WINDOW_COL: [0, 0, 0, 1, 2, 2, 3, 3],
                "k": ["a", "b", "c", "a", "a", "b", "a", "d"],
                fit_skew.COUNT_COL: [5, 1, 1, 7, 2, 2, 1, 9],
            }
        )
        agg = fit_skew.merge_key_parts([frame], ["k"])
        fine, coarse = fit_skew.summarize_keys(
            "test", COUNT_QUERY, agg, [60, 120], 1, 2, None
        )
        self.assertEqual((fine["K_total"], fine["rows_total"]), (4, 28))
        self.assertEqual(
            (fine["K_win_min"], fine["K_win_median"], fine["K_win_max"]), (1, 2, 3)
        )
        self.assertEqual(
            (fine["rows_win_min"], fine["rows_win_median"], fine["rows_win_max"]),
            (4, 7, 10),
        )
        # Merged windows {0,1} and {2,3}: keys {a,b,c} and {a,b,d}, rows 14 and 14.
        self.assertEqual((coarse["K_win_min"], coarse["K_win_max"]), (3, 3))
        self.assertEqual((coarse["rows_win_min"], coarse["rows_win_max"]), (14, 14))
        self.assertEqual(coarse["K_total"], fine["K_total"])

    def test_coarsen_values(self):
        windows = {0: np.array([1.0]), 1: np.array([2.0]), 2: np.array([3.0])}
        coarse = fit_skew.coarsen_values(windows, 2)
        self.assertEqual(sorted(coarse), [0, 1])
        np.testing.assert_array_equal(coarse[0], [1.0, 2.0])
        np.testing.assert_array_equal(coarse[1], [3.0])

    def test_summarize_values_window_lengths(self):
        rng = np.random.default_rng(7)
        acc = {
            "n_finite": 4000,
            "windows": {w: [rng.pareto(1.5, 1000) + 1.0] for w in range(4)},
        }
        q = {"id": "v", "promql": "quantile(0.99, x)", "value": "x"}
        # One thread: redirect_stdout in fit_power_law is process-wide.
        with ThreadPool(1) as pool:
            rows = fit_skew.summarize_values("test", q, acc, [60, 240], pool, 1, None)
        self.assertEqual([r["window_len_s"] for r in rows], [60, 240])
        self.assertEqual([r["n_windows"] for r in rows], [4, 1])
        self.assertEqual([r["rows_win_median"] for r in rows], [1000, 4000])
        self.assertNotIn("K_win_median", rows[0])
        for row in rows:
            self.assertTrue(row["lower"] <= row["mle"] <= row["upper"])


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
        "t": {"label_columns": ["a", "b"], "value_columns": ["v"]},
    },
    "queries": [
        {
            "id": "keys",
            "table": "t",
            "kind": "keys",
            "group_by": ["a"],
            "value": "v",
            "weights": ["count", "value"],
        },
        {"id": "vals", "table": "t", "kind": "values", "group_by": [], "value": "v"},
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

    def test_window_lengths(self):
        fit_skew.validate_window_lengths([60, 300, 1800], "d")
        for lengths in ([], [300, 60], [60, 60], [60, 90]):
            with self.subTest(lengths=lengths):
                with self.assertRaises(ValueError):
                    fit_skew.validate_window_lengths(lengths, "d")

    def test_table_window_lengths(self):
        cfg = copy.deepcopy(VALID_CONFIG)
        cfg["tables"]["t"]["window_lengths_s"] = [60, 86400]
        fit_skew.validate_config(cfg)
        cfg["tables"]["t"]["window_lengths_s"] = [60, 90]
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
