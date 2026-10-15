"""Tests for post_experiment/multi_experiment/compare_arms.py."""

import os
import sys
import unittest

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), "..", "post_experiment", "multi_experiment"
    ),
)

import compare_arms  # noqa: E402


class CompareArmsTest(unittest.TestCase):
    def test_sql_labels_skip_the_description_and_stop_at_the_value(self):
        sql = (
            "-- topk by (region) (4, sum_over_time(data[1m]))\n"
            "SELECT region, label_0, sum(value) AS value FROM data GROUP BY region, label_0"
        )
        self.assertEqual(compare_arms.sql_labels(sql), ["region", "label_0"])
        self.assertEqual(
            compare_arms.query_name(sql),
            "topk by (region) (4, sum_over_time(data[1m]))",
        )

    def test_agreement_scores_values_and_topk_sets(self):
        a, b = (("region", "0"),), (("region", "1"),)
        self.assertEqual(
            compare_arms.agreement({a: 11.0}, {a: 10.0, b: 5.0}, False), (0.1, 1)
        )
        self.assertEqual(
            compare_arms.agreement({a: 1.0}, {a: 9.0, b: 8.0}, True), (0.5, 0)
        )


if __name__ == "__main__":
    unittest.main()
