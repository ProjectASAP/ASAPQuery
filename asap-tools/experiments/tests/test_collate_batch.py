"""Tests for collate_batch.py's answer checks."""

import os
import sys
import unittest
from types import SimpleNamespace

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), "..", "post_experiment", "multi_experiment"
    ),
)

import collate_batch  # noqa: E402

SUM = "sum by (job) (sum_over_time(data[1m]))"
QUANTILE = "avg by (job) (quantile_over_time(0.95, data[100s]))"


def arm(sum_value, quantile_value):
    """One window of a sum and a quantile query, both for job="a"."""
    labels = (("job", "a"),)
    return SimpleNamespace(
        names={(0, 0): SUM, (0, 1): QUANTILE},
        results={(0, 0, 0): {labels: sum_value}, (0, 1, 0): {labels: quantile_value}},
    )


class DisagreementsTest(unittest.TestCase):
    def test_only_checked_queries_count(self):
        # A pooled quantile differs from Prometheus's avg-of-series stand-in by
        # design; leaving it unchecked keeps the sum's check.
        prom, asap = arm(100.0, 1.0), arm(100.0, 2.0)
        self.assertEqual(len(collate_batch.disagreements("asap", asap, prom)), 1)
        not_quantile = lambda name: "quantile" not in name  # noqa: E731
        self.assertEqual(
            collate_batch.disagreements("asap", asap, prom, not_quantile), []
        )
        bad_sum = arm(90.0, 2.0)
        self.assertEqual(
            len(collate_batch.disagreements("asap", bad_sum, prom, not_quantile)), 1
        )


if __name__ == "__main__":
    unittest.main()
