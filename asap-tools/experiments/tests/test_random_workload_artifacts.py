"""Tests for manifest-to-planner workload rendering."""

import os
import sys
import unittest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "datasets", "fake_metric"))

import random_workload_artifacts as artifacts  # noqa: E402
import random_workloads as workloads  # noqa: E402


class RenderArtifactsTest(unittest.TestCase):
    def test_promql_input_groups_non_sql_queries_by_repeat_interval(self):
        workload = workloads.generate_workloads(seed=862)[0]
        rendered = artifacts.render(workload)

        planner = rendered["promql_planner"]
        expected = [q for q in rendered["queries"] if q["promql"]]
        self.assertEqual(
            sum(len(group["queries"]) for group in planner["query_groups"]), len(expected)
        )
        self.assertEqual(
            {tuple(metric["labels"]) for metric in planner["metrics"]},
            {("region", "service", "job", "label_0")},
        )
        self.assertEqual(rendered["data_facts"], workload["data_facts"])
        for group in planner["query_groups"]:
            self.assertTrue(all(query["interval_ms"] == group["repetition_delay_ms"]
                                for query in expected if query["promql"] in group["queries"]))

    def test_sql_input_contains_only_sql_queries_and_their_table_schema(self):
        rendered = artifacts.render(workloads.generate_workloads(seed=862)[0])

        self.assertTrue(rendered["sql_planner"]["query_groups"])
        self.assertEqual(rendered["sql_planner"]["tables"][0]["name"], "events")
        sql = [query for group in rendered["sql_planner"]["query_groups"] for query in group["queries"]]
        self.assertTrue(all("COUNT(DISTINCT user_id)" in query for query in sql))


if __name__ == "__main__":
    unittest.main()
