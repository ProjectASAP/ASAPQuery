"""Tests for scoring SQL (ClickHouse TabSeparated) query results."""

import math
import os
import sys
import unittest

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "..",
        "..",
        "asap-common",
        "dependencies",
        "py",
        "promql_utilities",
    ),
)

from promql_utilities.query_results.classes import (  # noqa: E402
    QueryResult,
    QueryResultAcrossTime,
    parse_tabseparated_result,
)
from post_experiment.single_experiment.calculate_fidelity import (  # noqa: E402
    get_timeseries_similarity_scores,
    mape,
)

GROUPED_QUERY = (
    "SELECT job, instance, QUANTILE(0.9, value) FROM data "
    "WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW() GROUP BY job, instance"
)


class ParseTabSeparatedResultTest(unittest.TestCase):
    def test_group_by_columns_become_labels_named_from_select_list(self):
        parsed = parse_tabseparated_result(
            GROUPED_QUERY, "api\ta:1\t3.5\nweb\tb:2\t7\n"
        )
        self.assertEqual(
            parsed,
            {
                frozenset({("job", "api"), ("instance", "a:1")}): 3.5,
                frozenset({("job", "web"), ("instance", "b:2")}): 7.0,
            },
        )

    def test_value_column_is_located_by_select_position(self):
        query = "SELECT SUM(value) AS v, job FROM data GROUP BY job"
        self.assertEqual(
            parse_tabseparated_result(query, "4\tapi\n"),
            {frozenset({("job", "api")}): 4.0},
        )

    def test_commas_inside_nested_query_do_not_split_outer_select(self):
        query = (
            "SELECT job, MAX(s) FROM (SELECT job, instance, SUM(value) AS s "
            "FROM data GROUP BY job, instance) GROUP BY job"
        )
        self.assertEqual(
            parse_tabseparated_result(query, "api\t9\n"),
            {frozenset({("job", "api")}): 9.0},
        )

    def test_single_value_without_group_by(self):
        query = "SELECT QUANTILE(0.5, value) FROM data"
        self.assertEqual(
            parse_tabseparated_result(query, "42.5\n"), {frozenset(): 42.5}
        )

    def test_empty_result(self):
        self.assertEqual(parse_tabseparated_result(GROUPED_QUERY, ""), {})

    def test_nan_and_inf_values(self):
        # ClickHouse prints nan/inf/-inf; ASAPQuery's adapter prints Rust's NaN.
        parsed = parse_tabseparated_result(
            "SELECT job, AVG(value) FROM data GROUP BY job",
            "a\tnan\nb\tinf\nc\t-inf\nd\tNaN\n",
        )
        self.assertTrue(math.isnan(parsed[frozenset({("job", "a")})]))
        self.assertEqual(parsed[frozenset({("job", "b")})], math.inf)
        self.assertEqual(parsed[frozenset({("job", "c")})], -math.inf)
        self.assertTrue(math.isnan(parsed[frozenset({("job", "d")})]))

    def test_non_numeric_value_fails(self):
        with self.assertRaises(ValueError):
            parse_tabseparated_result(GROUPED_QUERY, "api\ta:1\tnot-a-number\n")

    def test_column_count_mismatch_fails(self):
        with self.assertRaises(ValueError):
            parse_tabseparated_result(GROUPED_QUERY, "api\t3.5\n")

    def test_duplicate_label_set_fails(self):
        with self.assertRaises(ValueError):
            parse_tabseparated_result(GROUPED_QUERY, "api\ta:1\t1\napi\ta:1\t2\n")

    def test_select_list_without_exactly_one_aggregate_fails(self):
        with self.assertRaises(ValueError):
            parse_tabseparated_result(
                "SELECT job, SUM(value), MAX(value) FROM data GROUP BY job", ""
            )

    def test_query_without_select_from_fails(self):
        with self.assertRaises(ValueError):
            parse_tabseparated_result("rate(x[1m])", "")


def _sql_results(server_name, raw_texts):
    across_time = QueryResultAcrossTime(server_name, GROUPED_QUERY, 0, len(raw_texts))
    for repetition_idx, raw_text in enumerate(raw_texts):
        across_time.add_result(
            QueryResult(
                server_name,
                GROUPED_QUERY,
                0,
                repetition_idx,
                result=None,
                latency=0.1,
                cumulative_latency=None,
                raw_text_result=raw_text,
            )
        )
    return across_time


class SqlFidelityTest(unittest.TestCase):
    def test_sql_query_is_scored(self):
        exact = _sql_results("clickhouse", ["api\ta:1\t10\n", "api\ta:1\t20\n"])
        estimate = _sql_results("asap", ["api\ta:1\t11\n", "api\ta:1\t22\n"])

        scores = get_timeseries_similarity_scores(
            [exact], [estimate], [GROUPED_QUERY], [mape], verbose=False
        )

        self.assertAlmostEqual(scores["mape"][GROUPED_QUERY], 10.0)

    def test_sql_exact_matches_prometheus_json_estimate(self):
        # An estimate answered as Prometheus JSON keys series by label name,
        # so it must line up with the SELECT-list names given to the TSV.
        exact = _sql_results("clickhouse", ["api\ta:1\t10\n"])
        estimate = QueryResultAcrossTime("asap", GROUPED_QUERY, 0, 1)
        estimate.add_result(
            QueryResult(
                "asap",
                GROUPED_QUERY,
                0,
                0,
                result=[
                    {
                        "metric": {"instance": "a:1", "job": "api"},
                        "value": [0, "12"],
                    }
                ],
                latency=0.1,
                cumulative_latency=None,
            )
        )

        scores = get_timeseries_similarity_scores(
            [exact], [estimate], [GROUPED_QUERY], [mape], verbose=False
        )

        self.assertAlmostEqual(scores["mape"][GROUPED_QUERY], 20.0)

    def test_only_the_estimate_modes_queries_are_scored(self):
        # sketchdb may skip trailing query groups; baseline ran them all.
        exact = _sql_results("clickhouse", ["api\ta:1\t10\n"])
        estimate = _sql_results("asap", ["api\ta:1\t11\n"])

        scores = get_timeseries_similarity_scores(
            {0: exact, 1: exact},
            {0: estimate},
            [GROUPED_QUERY, "SELECT 'skipped'"],
            [mape],
            verbose=False,
        )

        self.assertEqual(scores["mape"], {GROUPED_QUERY: 10.0})

    def test_a_query_missing_among_the_estimate_modes_fails(self):
        exact = _sql_results("clickhouse", ["api\ta:1\t10\n"])
        with self.assertRaises(KeyError):
            get_timeseries_similarity_scores(
                {0: exact, 1: exact},
                {1: exact},
                [GROUPED_QUERY, GROUPED_QUERY + " "],
                [mape],
                verbose=False,
            )


if __name__ == "__main__":
    unittest.main()
