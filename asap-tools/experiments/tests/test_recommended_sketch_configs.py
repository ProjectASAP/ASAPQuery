"""Tests for recommended_sketch_configs.py (config generation and error metric)."""

import os
import unittest

from hydra import compose, initialize_config_dir

from recommended_sketch_configs import recommended_sketch_configs as rsc

CONFIG_DIR = os.path.join(rsc.EXPERIMENTS_DIR, "config")


def recommendation(dataset, query_id, range_, family, config, meets_target="True"):
    return {
        "dataset": dataset,
        "query_id": query_id,
        "range": range_,
        "family": family,
        "config": config,
        "est_error": "0.04",
        "target": "0.05",
        "meets_target": meets_target,
    }


class PlannerSketchParametersTest(unittest.TestCase):
    def test_cms_rows_cols_become_depth_width(self):
        self.assertEqual(
            rsc.planner_sketch_parameters("cms", "rows=3 cols=4096"),
            {"CountMinSketch": {"depth": 3, "width": 4096}},
        )

    def test_kll_k(self):
        self.assertEqual(
            rsc.planner_sketch_parameters("kll", "k=200"),
            {"DatasketchesKLL": {"K": 200}},
        )

    def test_family_without_planner_sketch_raises(self):
        with self.assertRaises(ValueError):
            rsc.planner_sketch_parameters("countsketch", "rows=3 cols=4096")


class TranslatePromqlTest(unittest.TestCase):
    def test_alibaba_metric_and_label_names(self):
        exporter = rsc.EXPORTERS[("alibaba_v2022", "MSMetrics")]
        self.assertEqual(
            rsc.translate_promql(
                "sum by (msname) (sum_over_time(cpu_utilization[5m]))", exporter
            ),
            "sum by (ms_name) (sum_over_time(alibaba_microservice_cpu_usage[5m]))",
        )

    def test_only_whole_names_are_renamed(self):
        # A label that merely starts with a dataset name is not renamed.
        exporter = rsc.EXPORTERS[("alibaba_v2022", "MSMetrics")]
        self.assertEqual(
            rsc.translate_promql("sum by (msname_x) (m)", exporter),
            "sum by (msname_x) (m)",
        )


class GenerateTest(unittest.TestCase):
    def setUp(self):
        self.recommendations = [
            recommendation(
                "google_2011", "cpu_by_job_id", "5m", "cms", "rows=3 cols=4096"
            ),
            recommendation(
                "google_2011", "cpu_by_job_id", "5m", "countsketch", "rows=3 cols=4096"
            ),
            recommendation("google_2011", "cpu_p99", "instant", "kll", "k=200"),
            recommendation("google_2011", "cpu_p99", "instant", "dd", "alpha=0.01"),
        ]

    def test_recommended_and_default_twin(self):
        configs, _ = rsc.generate(
            self.recommendations,
            [("google_2011", "cpu_by_job_id", "5m")],
            "/traces",
        )
        self.assertEqual(
            sorted(configs),
            [
                "google_2011_cpu_by_job_id_5m_default",
                "google_2011_cpu_by_job_id_5m_recommended",
            ],
        )
        recommended = configs["google_2011_cpu_by_job_id_5m_recommended"]
        default = configs["google_2011_cpu_by_job_id_5m_default"]
        self.assertEqual(
            recommended["sketch_parameters"],
            {"CountMinSketch": {"depth": 3, "width": 4096}},
        )
        self.assertNotIn("sketch_parameters", default)
        group = recommended["experiment_params"]["query_groups"][0]
        self.assertEqual(
            group["queries"],
            ["sum by (job_id) (sum_over_time(google_mean_cpu_usage_rate_0[5m]))"],
        )
        # A range query waits one full range before its first answer.
        self.assertEqual(
            group["client_options"]["starting_delay"],
            rsc.MIN_STARTING_DELAY_S + 300,
        )
        self.assertEqual(recommended["cluster_data_directory"], "/traces/google")

    def test_families_without_planner_sketch_are_skipped(self):
        configs, skipped = rsc.generate(
            self.recommendations,
            [
                ("google_2011", "cpu_by_job_id", "5m"),
                ("google_2011", "cpu_p99", "instant"),
            ],
            "/traces",
        )
        self.assertEqual(len(configs), 4)
        self.assertEqual(len(skipped), 2)
        self.assertIn("countsketch", skipped[0])
        self.assertIn("dd", skipped[1])

    def test_query_without_exporter_is_skipped(self):
        # MSRTMCR (call-rate) data has no cluster_data_exporter.
        configs, skipped = rsc.generate(
            [
                recommendation(
                    "alibaba_v2022",
                    "mcr_by_msname",
                    "instant",
                    "cms",
                    "rows=3 cols=16384",
                )
            ],
            [("alibaba_v2022", "mcr_by_msname", "instant")],
            "/traces",
        )
        self.assertEqual(configs, {})
        self.assertIn("no exporter", skipped[0])

    def test_config_that_misses_target_is_skipped(self):
        configs, skipped = rsc.generate(
            [
                recommendation(
                    "google_2011", "cpu_p99", "instant", "kll", "k=200", "False"
                )
            ],
            [("google_2011", "cpu_p99", "instant")],
            "/traces",
        )
        self.assertEqual(configs, {})
        self.assertIn("does not meet the target", skipped[0])


class ComposeGeneratedConfigTest(unittest.TestCase):
    def test_recommended_config_overrides_config_yaml_sketch_parameters(self):
        # The generated files are package _global_ so sketch_parameters lands at
        # the top level, where the runner reads it, not under experiment_params.
        name = "recommended_sketch_configs/google_2011_cpu_p99_instant_recommended"
        with initialize_config_dir(version_base=None, config_dir=CONFIG_DIR):
            cfg = compose(
                "config",
                overrides=[f"experiment_type={name}"],
            )
        self.assertEqual(cfg.sketch_parameters.DatasketchesKLL.K, 200)
        self.assertEqual(
            list(cfg.experiment_params.query_groups[0].queries),
            ["quantile(0.99, google_mean_cpu_usage_rate_0)"],
        )


class AreTopKeysTest(unittest.TestCase):
    def test_largest_keys_only(self):
        exact = {"a": 100.0, "b": 10.0, "c": 1.0}
        estimate = {"a": 110.0, "b": 10.0, "c": 5.0}
        self.assertAlmostEqual(rsc.are_top_keys(exact, estimate, num_keys=2), 0.05)

    def test_missing_estimate_counts_as_zero(self):
        self.assertEqual(rsc.are_top_keys({"a": 4.0}, {}), 1.0)


if __name__ == "__main__":
    unittest.main()
