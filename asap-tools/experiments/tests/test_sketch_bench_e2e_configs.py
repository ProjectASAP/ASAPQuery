"""Tests for sketch_bench_e2e_configs.py (e2e configs from a sketch-bench table)."""

import json
import os
import shutil
import tempfile
import unittest

from hydra import compose, initialize_config_dir
from omegaconf import OmegaConf

from experiment_utils import config as experiment_config
from experiment_utils.core import read_exporter_config
from experiment_utils.services.paced_feeder import check_paced_feeder_config
from sketch_bench_e2e import sketch_bench_e2e_configs as sbe
from test_paced_feeder import E2E_METRICS

CONFIG_DIR = os.path.join(sbe.EXPERIMENTS_DIR, "config")
PREFIX = "synthetic/templates=classic/shared=1"

# A slice of synthetic-templatesclassic-shared1.json's e2e.queries, plus a
# structured entry from the multi-grouping tables.
QUERIES = [
    {
        "interval_secs": 1,
        "promql": "sum by (job) (data)",
        "rqes": [f"{PREFIX}/sum_by_job/keys/1s"],
    },
    {
        "interval_secs": 60,
        "promql": "sum_over_time(data[15m])",
        "rqes": [f"{PREFIX}/sum_over_time/keys/15m"],
    },
    {
        "interval_secs": 1,
        "promql": "quantile by (job) (0.5, data)",
        "rqes": [f"{PREFIX}/quantile_by_job_p0.5/values/1s"],
    },
    {
        "interval_secs": 60,
        "promql": "rate(data_total[15m])",
        "rqes": [f"{PREFIX}/rate/keys/15m"],
    },
    {
        "capability": "cardinality",
        "grouping": ["dst_subnet"],
        "interval_secs": 60,
        "rqes": ["synthetic/templates=all/shared=1/t11_distinct_src/keys/5m"],
        "table": "flows",
        "value": "src_ip",
        "window_secs": 300,
    },
]


class BuildConfigTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.table = os.path.join(self.tmp.name, "table.json")
        with open(self.table, "w") as f:
            json.dump(
                {"workloads": [{"e2e": {"metrics": E2E_METRICS, "queries": QUERIES}}]},
                f,
            )

    def tearDown(self):
        self.tmp.cleanup()

    def build(self, **kwargs):
        return sbe.build_config(
            self.table,
            seed=7,
            starting_delay_secs=120,
            query_duration_secs=600,
            **kwargs,
        )

    def test_query_groups_by_interval_in_table_order(self):
        config, _ = self.build()
        groups = config["experiment_params"]["query_groups"]
        self.assertEqual(
            [(g["id"], g["repetition_delay_ms"], g["queries"]) for g in groups],
            [
                (1, 1000, ["sum by (job) (data)", "quantile by (job) (0.5, data)"]),
                (2, 60000, ["sum_over_time(data[15m])", "rate(data_total[15m])"]),
            ],
        )
        self.assertEqual(
            [g["client_options"] for g in groups],
            [
                {
                    "repetitions": repetitions,
                    "starting_delay": 120,
                    "query_time_offset": sbe.DEFAULT_QUERY_TIME_OFFSET_SECS,
                    "align_to_interval": True,
                }
                for repetitions in (600, 10)
            ],
        )

    def test_feeder_sends_the_classic_metrics_without_sql(self):
        config, notes = self.build()
        params = config["experiment_params"]
        self.assertEqual(
            params["metrics"],
            [
                {
                    "metric": "data",
                    "labels": ["job", "label_0"],
                    "exporter": "paced_feeder",
                },
                {
                    "metric": "data_total",
                    "labels": ["job", "label_0"],
                    "exporter": "paced_feeder",
                },
            ],
        )
        self.assertEqual(
            params["exporters"]["exporter_list"]["paced_feeder"],
            {
                "workload_table": self.table,
                "metrics": ["data", "data_total"],
                "seed": 7,
                "duration_secs": 120 + 600 + sbe.FEEDER_SLACK_SECS,
            },
        )
        self.assertIn(
            "skipped records metric 'flows': only structured queries read it", notes
        )
        self.assertEqual(
            [m["mode"] for m in params["experiment"]], ["sketchdb", "baseline"]
        )

    def test_structured_queries_skipped_without_sql(self):
        config, notes = self.build()
        self.assertEqual(len(config["experiment_params"]["query_groups"]), 2)
        self.assertIn(
            "skipped 1 structured (multi-grouping) queries: no --sql-queries file",
            notes,
        )

    def test_structured_queries_from_sql_file(self):
        sql = os.path.join(self.tmp.name, "multigroup.sql")
        with open(sql, "w") as f:
            f.write("SELECT dst_subnet, uniq(src_ip) FROM flows GROUP BY dst_subnet;\n")
        config, notes = self.build(sql_queries_file=sql)
        group = config["experiment_params"]["query_groups"][2]
        self.assertEqual(group["id"], 3)
        self.assertEqual(group["repetition_delay_ms"], 60000)
        self.assertEqual(
            group["queries"],
            ["SELECT dst_subnet, uniq(src_ip) FROM flows GROUP BY dst_subnet"],
        )
        # The client refuses to render {{ end_time }} templates without a
        # window; the SQL carries its own, so 0. PromQL groups have none.
        self.assertEqual(group["time_window_seconds"], 0)
        self.assertNotIn(
            "time_window_seconds", config["experiment_params"]["query_groups"][0]
        )
        self.assertFalse(any("structured" in n for n in notes))
        # The records metric goes to ASAPQuery only: fed, but no baseline
        # mode and not under `metrics`.
        params = config["experiment_params"]
        self.assertEqual(
            params["exporters"]["exporter_list"]["paced_feeder"]["metrics"],
            ["data", "data_total", "flows"],
        )
        self.assertEqual([m["mode"] for m in params["experiment"]], ["sketchdb"])
        self.assertEqual(
            [m["metric"] for m in params["metrics"]], ["data", "data_total"]
        )
        self.assertIn(
            "no baseline mode: records metrics ['flows'] go to ASAPQuery only", notes
        )

    def test_records_rate_only_with_records_metrics(self):
        sql = os.path.join(self.tmp.name, "multigroup.sql")
        with open(sql, "w") as f:
            f.write("SELECT 1;\n")
        config, _ = self.build(sql_queries_file=sql, records_per_sec=1000)
        feeder = config["experiment_params"]["exporters"]["exporter_list"]
        self.assertEqual(feeder["paced_feeder"]["records_per_sec"], 1000)
        config, _ = self.build(records_per_sec=1000)
        feeder = config["experiment_params"]["exporters"]["exporter_list"]
        self.assertNotIn("records_per_sec", feeder["paced_feeder"])

    def test_sql_file_must_match_the_structured_queries(self):
        sql = os.path.join(self.tmp.name, "multigroup.sql")
        with open(sql, "w") as f:
            f.write("SELECT 1;\nSELECT 2;\n")
        with self.assertRaisesRegex(ValueError, "2 statements; the table has 1"):
            self.build(sql_queries_file=sql)

    def test_query_phase_shorter_than_an_interval(self):
        with self.assertRaisesRegex(ValueError, "shorter than the 60 s"):
            sbe.build_config(
                self.table, seed=7, starting_delay_secs=0, query_duration_secs=30
            )

    def test_prebuilt_output_dir(self):
        prebuilt = os.path.join(self.tmp.name, "prebuilt")
        os.makedirs(prebuilt)
        with self.assertRaisesRegex(ValueError, "streaming_config.yaml"):
            self.build(prebuilt_output_dir=prebuilt)
        for name in experiment_config.PLANNER_OUTPUT_FILES:
            open(os.path.join(prebuilt, name), "w").close()
        config, _ = self.build(prebuilt_output_dir=prebuilt)
        self.assertEqual(config["controller"], {"prebuilt_output_dir": prebuilt})

    def compose_and_validate(self, config):
        # A dry run of what experiment_run_e2e.py does with the file before it
        # starts any service; returns the experiment modes.
        config_dir = os.path.join(self.tmp.name, "config")
        shutil.copytree(CONFIG_DIR, config_dir)
        sbe.write_config(
            config,
            self.table,
            os.path.join(config_dir, "experiment_type", "sketch_bench_e2e", "t.yaml"),
        )
        with initialize_config_dir(version_base=None, config_dir=config_dir):
            cfg = compose("config", overrides=["experiment_type=sketch_bench_e2e/t"])
        params = cfg.experiment_params
        experiment_config.validate_experiment_config(params)
        exporter_config, reason = read_exporter_config(params)
        self.assertIsNotNone(exporter_config, reason)
        check_paced_feeder_config(
            OmegaConf.to_container(exporter_config.exporter_list.paced_feeder),
            modes=OmegaConf.to_container(params.experiment),
            parallelism=cfg.streaming.parallelism,
            min_feed_secs=cfg.flow.steady_state_wait
            + experiment_config.get_minimum_experiment_running_time(params),
        )
        modes, _ = experiment_config.generate_controller_client_configs(
            params, self.tmp.name
        )
        return modes

    def test_generated_config_composes_and_validates(self):
        self.assertEqual(
            self.compose_and_validate(self.build()[0]), ["sketchdb", "baseline"]
        )

    def test_generated_config_with_records_validates(self):
        sql = os.path.join(self.tmp.name, "multigroup.sql")
        with open(sql, "w") as f:
            f.write("SELECT 1;\n")
        config, _ = self.build(sql_queries_file=sql)
        self.assertEqual(self.compose_and_validate(config), ["sketchdb"])


if __name__ == "__main__":
    unittest.main()
