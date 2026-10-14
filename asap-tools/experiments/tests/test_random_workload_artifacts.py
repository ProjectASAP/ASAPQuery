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

    def test_paced_feeder_table_preserves_all_manifest_data_facts(self):
        table = artifacts.paced_feeder_table(workloads.generate_workloads(seed=862)[0])
        metrics = table["workloads"][0]["e2e"]["metrics"]

        self.assertEqual(metrics["data"]["samples_per_sec_per_series"], 10)
        self.assertEqual(metrics["data"]["values"]["value"]["keys"], 10_000)
        self.assertEqual(metrics["data_total"]["type"], "counter")
        self.assertEqual(metrics["events"]["records_per_sec"], 100_000)
        self.assertEqual(
            metrics["events"]["values"]["user_id"]["rotating_pool"],
            {"universe_keys": 10_000, "keys_per_series": 1_000},
        )

    def test_pilot_profile_contains_only_common_queries(self):
        profile = artifacts.pilot_profile(workloads.generate_workloads(seed=862)[0])

        self.assertEqual(len(profile["included_query_ids"]), 7)
        self.assertEqual(len(profile["excluded_query_ids"]), 3)
        self.assertEqual(profile["data_ingestion_interval_ms"], 100)
        self.assertTrue(profile["promql_query_groups"])
        self.assertTrue(profile["clickhouse_query_groups"])

    def test_pilot_e2e_table_is_consumable_by_existing_config_renderer(self):
        table = artifacts.pilot_e2e_table(workloads.generate_workloads(seed=862)[0])
        e2e = table["workloads"][0]["e2e"]

        self.assertEqual(len(e2e["queries"]), 7)
        self.assertEqual(set(e2e["metrics"]), {"data", "data_total", "events"})
        self.assertTrue(all(query["interval_secs"] >= 1 for query in e2e["queries"]))


if __name__ == "__main__":
    unittest.main()
