"""Regression tests for ClickHouse experiment controller wiring."""

import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import yaml

from omegaconf import OmegaConf

from experiment_run_clickhouse import (
    _check_clickhouse_paced_feeder,
    _deploy_prebuilt_clickhouse_controller_output,
    _ingest_mode,
    _prepare_clickhouse_controller_output,
    _start_clickhouse_controller,
    _start_clickhouse_feed,
)
from test_paced_feeder import feeder_config, write_table


class ClickHouseControllerConfigTest(unittest.TestCase):
    def test_controller_receives_configured_punting_setting(self):
        controller_service = Mock()
        cfg = OmegaConf.create({"controller": {"punting": True}})

        _start_clickhouse_controller(
            controller_service,
            cfg,
            controller_input_file="planner_input.yaml",
            streaming_engine="precompute",
        )

        controller_service.start.assert_called_once_with(
            punting=True,
            controller_input_file="planner_input.yaml",
            streaming_engine="precompute",
        )

    @patch("experiment_run_clickhouse.sync.rsync_streaming_configs")
    @patch("experiment_run_clickhouse.config.copy_prebuilt_controller_output")
    def test_prebuilt_output_is_validated_and_deployed(
        self, copy_prebuilt_controller_output, rsync_streaming_configs
    ):
        provider = Mock()

        _deploy_prebuilt_clickhouse_controller_output(
            provider,
            "/plans/frozen",
            "/tmp/local-controller",
            "/remote/controller",
            "/tmp/client.yaml",
            1000,
            2,
        )

        copy_prebuilt_controller_output.assert_called_once_with(
            "/plans/frozen",
            "/tmp/local-controller",
            "/tmp/client.yaml",
            1000,
            query_language="sql",
        )
        rsync_streaming_configs.assert_called_once_with(
            provider, "/tmp/local-controller", "/remote/controller", 2
        )

    def test_prebuilt_path_does_not_generate_a_controller_output(self):
        deploy_prebuilt_output = Mock()
        generate_controller_output = Mock()

        _prepare_clickhouse_controller_output(
            "/plans/frozen", deploy_prebuilt_output, generate_controller_output
        )

        deploy_prebuilt_output.assert_called_once_with("/plans/frozen")
        generate_controller_output.assert_not_called()

    def test_unset_prebuilt_path_generates_a_controller_output(self):
        deploy_prebuilt_output = Mock()
        generate_controller_output = Mock()

        _prepare_clickhouse_controller_output(
            None, deploy_prebuilt_output, generate_controller_output
        )

        generate_controller_output.assert_called_once_with()
        deploy_prebuilt_output.assert_not_called()


class IngestModeTest(unittest.TestCase):
    def test_baseline_when_present_else_first_mode(self):
        self.assertEqual(_ingest_mode(["baseline", "sketchdb"]), "baseline")
        self.assertEqual(_ingest_mode(["sketchdb", "baseline"]), "baseline")
        # A baseline_mv-only run monitors ingest under its own mode.
        self.assertEqual(_ingest_mode(["baseline_mv"]), "baseline_mv")


def aligned_client_config(*intervals_secs):
    return {
        "query_groups": [
            {
                "id": i,
                "repetition_delay_ms": secs * 1000,
                "client_options": {"align_to_interval": True, "query_time_offset": 10},
            }
            for i, secs in enumerate(intervals_secs)
        ]
    }


class ClickHousePacedFeederCheckTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.table = write_table(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def check(self, modes, **overrides):
        return _check_clickhouse_paced_feeder(
            feeder_config(self.table, **overrides),
            modes,
            aligned_client_config(10, 60),
            minimum_experiment_running_time=300,
        )

    def test_aligns_to_the_query_intervals(self):
        self.assertEqual(self.check(["baseline", "baseline_mv"]), 60)

    def test_records_metrics_go_to_clickhouse(self):
        # Unlike Prometheus, ClickHouse keeps every record of a slot.
        self.check(["baseline"], metrics=["data", "flows"])

    def test_sketchdb_mode_refused(self):
        # sketchdb's engine ingests local_data_file, which a feed has none of.
        with self.assertRaisesRegex(ValueError, "sketchdb"):
            self.check(["baseline", "sketchdb"])

    def test_feed_ending_before_the_queries_refused(self):
        with self.assertRaisesRegex(ValueError, "ends before the queries"):
            self.check(["baseline"], duration_secs=300)


class StartClickHouseFeedTest(unittest.TestCase):
    def test_tables_are_created_before_the_feed_and_queries_follow_it(self):
        calls = Mock()
        calls.feeder.start.return_value = 1700000060000
        with tempfile.TemporaryDirectory() as root, patch(
            "experiment_run_clickhouse.sync.rsync_controller_client_configs"
        ) as rsync:
            config_dir = os.path.join(root, "controller_client_configs")
            os.makedirs(config_dir)
            config_path = os.path.join(config_dir, "baseline.yaml")
            with open(config_path, "w") as f:
                yaml.safe_dump(aligned_client_config(60), f)

            _start_clickhouse_feed(
                provider=Mock(),
                node_offset=0,
                data_loader=calls.loader,
                feeder_service=calls.feeder,
                feeder_config={"metrics": ["data"]},
                feeder_align_secs=60,
                init_sql_file="init.sql",
                clickhouse_http_port=8123,
                experiment_mode="baseline",
                experiment_output_dir="/exp/baseline",
                local_experiment_dir=os.path.join(root, "baseline"),
                experiment_root_output_dir="/exp",
                local_experiment_root_dir=root,
            )
            with open(config_path) as f:
                client_config = yaml.safe_load(f)

        # A view sees only rows inserted after it exists.
        self.assertEqual(
            [c[0] for c in calls.mock_calls], ["loader.run_init_sql", "feeder.start"]
        )
        self.assertEqual(
            calls.feeder.start.call_args.kwargs["sinks"],
            {"clickhouse": "http://localhost:8123/?database=default"},
        )
        self.assertEqual(
            client_config["query_groups"][0]["client_options"]["data_start_time"],
            1700000060,
        )
        rsync.assert_called_once()


if __name__ == "__main__":
    unittest.main()
