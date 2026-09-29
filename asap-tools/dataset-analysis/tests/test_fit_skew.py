"""Tests for the skew fits, window bounds, config validation and readers."""

import copy
import io
import tarfile
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

import fit_skew

ZIPF_K = 1000
ZIPF_SAMPLES = 2_000_000
ZIPF_TOLERANCE = 0.05
PARETO_SAMPLES = fit_skew.MAX_FIT_SAMPLES
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
                fit = fit_skew.fit_power_law(x, compare=True)
                self.assertAlmostEqual(
                    fit["alpha"], alpha, delta=PARETO_REL_TOLERANCE * alpha
                )
                self.assertTrue(fit["power_law_ok"])
                tail = np.sum(x >= fit["xmin"])
                self.assertGreaterEqual(tail, fit_skew.MIN_TAIL_SAMPLES)

    def test_power_law_ok_rule(self):
        self.assertTrue(fit_skew.power_law_ok([(1.0, 0.01), (-1.0, 0.5)]))
        self.assertFalse(fit_skew.power_law_ok([(1.0, 0.01), (-1.0, 0.05)]))
        self.assertTrue(fit_skew.power_law_ok([]))

    def test_too_few_samples_is_nan(self):
        fit = fit_skew.fit_power_law(
            np.arange(1.0, fit_skew.MIN_TAIL_SAMPLES), compare=True
        )
        self.assertTrue(np.isnan(fit["alpha"]))
        self.assertNotIn("power_law_ok", fit)


class WindowBoundsTest(unittest.TestCase):
    def test_min_max_count(self):
        self.assertEqual(fit_skew.window_bounds([1.1, 0.7, 1.4]), (0.7, 1.4, 3))

    def test_nan_windows_skipped(self):
        self.assertEqual(
            fit_skew.window_bounds([np.nan, 0.9, np.nan, 1.2]), (0.9, 1.2, 2)
        )

    def test_no_windows(self):
        for estimates in ([], [np.nan]):
            lower, upper, n = fit_skew.window_bounds(estimates)
            self.assertTrue(np.isnan(lower) and np.isnan(upper))
            self.assertEqual(n, 0)

    def test_summarize_keys_skips_small_windows(self):
        rng = np.random.default_rng(5)
        frames = []
        for window, theta in ((0, 0.8), (1, 1.2)):
            counts = zipf_counts(theta, rng)
            frames.append(
                pd.DataFrame(
                    {
                        fit_skew.WINDOW_COL: window,
                        "k": np.arange(ZIPF_K).astype(str).astype(object),
                        fit_skew.COUNT_COL: counts,
                    }
                )
            )
        # Window 2 has too few keys to be fitted.
        frames.append(
            pd.DataFrame(
                {fit_skew.WINDOW_COL: 2, "k": ["0", "1"], fit_skew.COUNT_COL: [1e6, 1]}
            )
        )
        agg = fit_skew.merge_key_parts(frames, ["k"])
        q = {"id": "q", "promql": "count by (k) (x)", "group_by": ["k"]}
        q["weights"] = ["count"]
        (row,) = fit_skew.summarize_keys("test", q, agg, 1, 10, None)
        self.assertEqual(row["n_windows"], 2)
        self.assertAlmostEqual(row["lower"], 0.8, delta=ZIPF_TOLERANCE)
        self.assertAlmostEqual(row["upper"], 1.2, delta=ZIPF_TOLERANCE)
        self.assertTrue(row["lower"] <= row["mle"] <= row["upper"])
        self.assertEqual(row["K"], ZIPF_K)


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
