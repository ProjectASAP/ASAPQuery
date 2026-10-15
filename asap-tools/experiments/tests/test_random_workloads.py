"""Behavior tests for the accelerated random-workload manifest generator."""

import os
import sys
import unittest
from copy import deepcopy


sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "datasets", "fake_metric")
)

import random_workloads as workloads  # noqa: E402


class GenerateWorkloadsTest(unittest.TestCase):
    def test_seeded_generation_produces_ten_distinct_ten_query_workloads(self):
        generated = workloads.generate_workloads(seed=17)

        self.assertEqual(len(generated), 10)
        self.assertEqual(generated, workloads.generate_workloads(seed=17))
        self.assertEqual(
            {workload["id"] for workload in generated},
            {f"workload-{index:02d}" for index in range(1, 11)},
        )
        for workload in generated:
            queries = workload["queries"]
            self.assertEqual(len(queries), 10)
            self.assertEqual(len({query["id"] for query in queries}), 10)
            self.assertTrue(all(query["rqes"] for query in queries))

    def test_manifest_declares_each_querys_rendering_and_output_cardinality(self):
        queries = workloads.generate_workloads(seed=1)[0]["queries"]

        for query in queries:
            self.assertIn(
                query["support"], {"common-promql", "counter-gated-promql", "sql-only"}
            )
            self.assertIn(query["output_groups"], {4, 20, 200})
            if query["support"] == "sql-only":
                self.assertIsNone(query["promql"])
                self.assertIn("COUNT(DISTINCT user_id)", query["sql"])
                self.assertIn("DATEADD(s, -", query["sql"])
            else:
                self.assertIsNone(query["sql"])
                self.assertIn(" by (", query["promql"])
                if query["support"] == "common-promql":
                    self.assertIsNotNone(query["clickhouse_sql"])

    def test_grouped_quantiles_use_average_of_per_series_quantiles(self):
        # The SQL each Query renders is checked against Prometheus in
        # test_sketch_bench_workloads.
        for query in workloads.generate_workloads(seed=4)[0]["queries"]:
            rqe = query["rqes"][0]
            if rqe["capability"] == "quantile":
                self.assertIn("avg by", query["promql"])
                q = workloads.rqe_query(rqe)
                self.assertEqual((q.series_fn, q.agg), ("quantile", "avg"))
                self.assertEqual(q.grouping, tuple(query["grouping"]))
                self.assertEqual(q.window_secs, 100)

    def test_visible_query_outputs_stay_bounded(self):
        for workload in workloads.generate_workloads(seed=4):
            for query in workload["queries"]:
                rqe = query["rqes"][0]
                expected = query["output_groups"] * rqe.get("k", 1)
                self.assertEqual(query["output_series_upper_bound"], expected)
                self.assertLessEqual(query["output_series_upper_bound"], 200)

    def test_quantiles_cover_a_thousand_samples_per_series(self):
        # sketch-bench's accuracy curves start at N = 1,000; below it no
        # saved plan can serve the quantile.
        for workload in workloads.generate_workloads(seed=4):
            for query in workload["queries"]:
                if query["rqes"][0]["capability"] == "quantile":
                    samples = (
                        query["lookback_ms"]
                        // workload["data_facts"]["scrape_interval_ms"]
                    )
                    self.assertEqual(samples, 1_000)
                    self.assertIn("data[100s]", query["promql"])
                    self.assertEqual(query["interval_ms"], workloads.SECOND_MS)

    def test_manifest_declares_deterministic_feeder_facts(self):
        facts = workloads.generate_workloads(seed=4)[0]["data_facts"]

        self.assertEqual(facts["scrape_interval_ms"], 100)
        self.assertEqual(facts["series"]["leaf_count"], 10_000)
        self.assertEqual(facts["series"]["ranking"]["exponent"], 1.1)
        self.assertEqual(facts["metrics"]["data"]["pareto_shape"], 2.0)
        self.assertEqual(facts["metrics"]["data_total"]["resets"], "none")
        self.assertEqual(facts["events"]["user_id_universe_size"], 10_000)
        self.assertEqual(facts["events"]["user_ids_per_leaf"], 1_000)

    def test_workloads_do_not_repeat_an_atomic_rqe(self):
        for workload in workloads.generate_workloads(seed=1):
            keys = []
            for query in workload["queries"]:
                rqe = query["rqes"][0]
                keys.append(
                    (
                        rqe["capability"],
                        tuple(rqe["grouping_labels"]),
                        rqe["lookback_ms"],
                        rqe["interval_ms"],
                        rqe.get("phi"),
                        rqe.get("k"),
                    )
                )
            self.assertEqual(len(keys), len(set(keys)))

    def test_validation_rejects_duplicate_atomic_rqes(self):
        workload = deepcopy(workloads.generate_workloads(seed=1)[0])
        workload["queries"][4]["rqes"][0] = deepcopy(workload["queries"][3]["rqes"][0])

        with self.assertRaisesRegex(ValueError, "must not repeat an atomic RQE"):
            workloads.validate_workload(workload)


if __name__ == "__main__":
    unittest.main()
