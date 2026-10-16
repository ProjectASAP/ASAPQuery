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

    def test_variants_pair_with_the_base_query_for_query(self):
        # A variant stretches one dimension and keeps every random choice, so
        # its results compare with the base's query for query.
        base = workloads.generate_workloads(seed=862)
        for variant, scale, series in (("long", 3, 10_000), ("x5", 1, 50_000)):
            for b, v in zip(
                base, workloads.generate_workloads(seed=862, variant=variant)
            ):
                with self.subTest(variant=variant, workload=b["id"]):
                    self.assertEqual(v["variant"], variant)
                    self.assertEqual(v["data_facts"]["series"]["leaf_count"], series)
                    for bq, vq in zip(b["queries"], v["queries"]):
                        self.assertEqual(vq["lookback_ms"], scale * bq["lookback_ms"])
                        bq_rqe, vq_rqe = bq["rqes"][0], vq["rqes"][0]
                        for key in ("capability", "grouping_labels", "interval_ms"):
                            self.assertEqual(vq_rqe[key], bq_rqe[key])
                        self.assertEqual(vq_rqe.get("phi"), bq_rqe.get("phi"))
                        self.assertEqual(vq_rqe.get("k"), bq_rqe.get("k"))
        self.assertNotIn("variant", base[0])

    def test_pooled_workloads_pair_with_the_base_query_for_query(self):
        # Only the quantile and sum top-k shapes change; every random choice
        # stays the base's.
        base = workloads.generate_workloads(seed=862)
        for b, p in zip(base, workloads.generate_workloads(seed=862, pooled=True)):
            with self.subTest(workload=b["id"]):
                self.assertTrue(p["pooled"])
                for bq, pq in zip(b["queries"], p["queries"]):
                    b_rqe, p_rqe = bq["rqes"][0], pq["rqes"][0]
                    for key in ("capability", "lookback_ms", "interval_ms"):
                        self.assertEqual(p_rqe[key], b_rqe[key])
                    self.assertEqual(p_rqe.get("phi"), b_rqe.get("phi"))
                    self.assertEqual(p_rqe.get("k"), b_rqe.get("k"))
                    if b_rqe["capability"] not in ("quantile", "topk_by_value"):
                        self.assertEqual(pq, bq)
                    if b_rqe["capability"] == "quantile":
                        self.assertEqual(pq["grouping"], bq["grouping"])
        self.assertNotIn("pooled", base[0])

    def test_a_pooled_quantile_is_one_over_each_groups_samples(self):
        # PromQL's stand-in is the quantile of per-series quantiles; the RQE
        # and ClickHouse take the quantile of the group's raw samples.
        q = workloads.quantile_query("q", ("region",), 0.95, pooled=True)
        self.assertEqual(
            q["promql"],
            "quantile by (region) (0.95, quantile_over_time(0.95, data[100s]))",
        )
        self.assertTrue(q["rqes"][0]["pooled"])
        self.assertIn("quantileExactInclusive(0.95)(value)", q["clickhouse_sql"])
        self.assertIn("GROUP BY region", q["clickhouse_sql"])
        self.assertNotIn("label_0", q["clickhouse_sql"])

    def test_a_pooled_topk_ranks_groups_globally(self):
        q = workloads.topk_sum_query(
            "q", ("region", "service", "job"), 60_000, 1_000, 4, pooled=True
        )
        self.assertEqual(
            q["promql"],
            "topk(4, sum by (region, service, job) (sum_over_time(data[1m])))",
        )
        self.assertEqual(q["output_series_upper_bound"], 4)
        sql = q["clickhouse_sql"]
        self.assertIn("GROUP BY region, service, job ORDER BY value DESC", sql)
        self.assertTrue(sql.endswith("LIMIT 4"))
        self.assertNotIn("label_0", sql)

    def test_dashboard_workloads_follow_their_template(self):
        for w in workloads.generate_workloads(seed=862, dashboard=True):
            with self.subTest(workload=w["id"]):
                qs = w["queries"]
                self.assertEqual(len(qs), 10)
                self.assertTrue(w["pooled"])
                # A sum roll-up chain: one window at every grouping.
                sums = qs[:3]
                self.assertEqual(
                    [tuple(q["grouping"]) for q in sums], list(workloads.GROUPINGS)
                )
                self.assertEqual(len({q["lookback_ms"] for q in sums}), 1)
                # A p50/p90/p99 panel: one grouping and window.
                panel = qs[3:6]
                self.assertEqual(
                    [q["rqes"][0]["phi"] for q in panel],
                    list(workloads.DASHBOARD_PANEL_PHIS),
                )
                self.assertEqual(
                    len({(tuple(q["grouping"]), q["lookback_ms"]) for q in panel}), 1
                )
                # The other quantiles cover the other groupings.
                self.assertEqual(
                    {tuple(q["grouping"]) for q in qs[3:8]}, set(workloads.GROUPINGS)
                )
                for q in qs:
                    rqe = q["rqes"][0]
                    if rqe["capability"] != "sum":
                        self.assertTrue(rqe["pooled"])
                    if q["lookback_ms"] == workloads.SECOND_MS:
                        # PromQL's closed 1 s window holds one more sample than
                        # ASAPQuery's left-open one: a tenth of a sum.
                        self.assertNotEqual(rqe["capability"], "sum")
                        if rqe["capability"] == "quantile":
                            self.assertIn(
                                tuple(q["grouping"]),
                                workloads.DASHBOARD_ONE_SECOND_GROUPINGS,
                            )
                    self.assertLessEqual(q["lookback_ms"], 20 * workloads.MINUTE_MS)

    def test_sub_minute_windows_render_in_seconds(self):
        # PromQL rejects "[0m]"; a 1 s top-k window once rendered as that.
        self.assertEqual(workloads.duration(1_000), "1s")
        self.assertEqual(workloads.duration(60_000), "1m")
        for w in workloads.generate_workloads(seed=862, dashboard=True):
            for q in w["queries"]:
                self.assertNotIn("[0m]", q["promql"])

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
