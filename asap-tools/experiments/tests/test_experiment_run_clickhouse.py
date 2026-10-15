"""Regression tests for ClickHouse experiment controller wiring."""

import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import yaml

from omegaconf import OmegaConf

from experiment_run_clickhouse import (
    PACED_FEEDER_STOP_TIMEOUT_SECS,
    QUERY_CLIENT_STARTUP_SECS,
    _check_clickhouse_paced_feeder,
    _deploy_prebuilt_clickhouse_controller_output,
    _ingest_mode,
    _prepare_clickhouse_controller_output,
    _start_clickhouse_controller,
    _finish_clickhouse_feed,
    _feed_sinks,
    _start_feed,
)
from experiment_utils.config import validate_experiment_config
from experiment_utils.services.paced_feeder import PACED_FEEDER_STARTUP_CHECK_SECS
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

    def check(self, modes, prebuilt_output_dir=None, **overrides):
        return _check_clickhouse_paced_feeder(
            feeder_config(self.table, **overrides),
            modes,
            aligned_client_config(10, 60),
            minimum_experiment_running_time=300,
            controller_prebuilt_output_dir=prebuilt_output_dir,
        )

    def test_aligns_to_the_query_intervals(self):
        self.assertEqual(self.check(["baseline", "baseline_mv"]), 60)

    def test_records_metrics_go_to_clickhouse(self):
        # Unlike Prometheus, ClickHouse keeps every record of a slot.
        self.check(["baseline"], metrics=["data", "flows"])

    def test_sketchdb_without_a_prebuilt_plan_refused(self):
        # The planner's input needs dataset.precompute, which a feed has none of.
        with self.assertRaisesRegex(ValueError, "prebuilt_output_dir"):
            self.check(["baseline", "sketchdb"])

    def test_sketchdb_with_a_prebuilt_plan_is_fed(self):
        self.check(["baseline", "sketchdb"], prebuilt_output_dir="/plans/saved")

    def test_feed_ending_before_the_queries_refused(self):
        with self.assertRaisesRegex(ValueError, "ends before the queries"):
            self.check(["baseline"], duration_secs=300)

    def test_feed_must_outlast_the_query_client_startup(self):
        # The client starts after remote_monitor and its container, not right
        # after the feeders' startup check.
        startup_check_only = 300 + PACED_FEEDER_STARTUP_CHECK_SECS
        with self.assertRaisesRegex(ValueError, "ends before the queries"):
            self.check(["baseline"], duration_secs=startup_check_only)
        self.check(
            ["baseline"],
            duration_secs=startup_check_only + QUERY_CLIENT_STARTUP_SECS,
        )


CLICKHOUSE_FEED_URL = "http://10.10.1.1:8123/?database=default"
ENGINE_FEED_URL = "http://10.10.1.1:8080/api/v1/write"


class FeedSinksTest(unittest.TestCase):
    def test_baseline_modes_feed_clickhouse(self):
        for mode in ("baseline", "baseline_mv"):
            self.assertEqual(
                _feed_sinks(mode, CLICKHOUSE_FEED_URL, ENGINE_FEED_URL),
                {"clickhouse": CLICKHOUSE_FEED_URL},
            )

    def test_sketchdb_feeds_only_the_engine(self):
        # It forwards no queries, and ClickHouse ingesting a copy would load
        # the node its CPU and latency are measured on.
        self.assertEqual(
            _feed_sinks("sketchdb", CLICKHOUSE_FEED_URL, ENGINE_FEED_URL),
            {"query_engine": ENGINE_FEED_URL},
        )


class StartFeedTest(unittest.TestCase):
    def start(self, calls, exited=True, mode="baseline"):
        calls.feeder.start.return_value = 1700000060000
        calls.feeder.wait_for_exit.return_value = exited
        with tempfile.TemporaryDirectory() as root, patch(
            "experiment_run_clickhouse.sync.rsync_controller_client_configs"
        ) as self.rsync:
            config_dir = os.path.join(root, "controller_client_configs")
            os.makedirs(config_dir)
            config_path = os.path.join(config_dir, f"{mode}.yaml")
            with open(config_path, "w") as f:
                yaml.safe_dump(aligned_client_config(60), f)

            provider = Mock()
            provider.get_node_ip.return_value = "10.10.1.1"
            start_ms = _start_feed(
                provider=provider,
                node_offset=0,
                data_loader=calls.loader,
                feeder_service=calls.feeder,
                feeder_config={"metrics": ["data"]},
                feeder_align_secs=60,
                dataset_name="custom",
                init_sql_file="init.sql",
                sinks={"clickhouse": CLICKHOUSE_FEED_URL},
                experiment_mode=mode,
                experiment_root_output_dir="/exp",
                local_experiment_root_dir=root,
            )
            with open(config_path) as f:
                self.client_config = yaml.safe_load(f)
        return start_ms

    def test_tables_are_created_before_the_feed_and_queries_follow_it(self):
        calls = Mock()
        self.assertEqual(self.start(calls), 1700000060000)
        # Old feeders gone before the tables are recreated; a view sees only
        # rows inserted after it exists.
        self.assertEqual(
            [c[0] for c in calls.mock_calls],
            [
                "feeder.stop",
                "feeder.wait_for_exit",
                "loader.run_init_sql",
                "feeder.start",
            ],
        )
        # The feed starts on a boundary of the aligned intervals' LCM.
        start_kwargs = calls.feeder.start.call_args.kwargs
        self.assertEqual(start_kwargs["align_secs"], 60)
        self.assertEqual(
            start_kwargs["sinks"],
            # From the feeder's own node to ClickHouse's.
            {"clickhouse": "http://10.10.1.1:8123/?database=default"},
        )
        self.assertEqual(start_kwargs["experiment_output_dir"], "/exp/baseline")
        self.assertEqual(
            self.client_config["query_groups"][0]["client_options"]["data_start_time"],
            1700000060,
        )
        self.rsync.assert_called_once()

    def test_sketchdb_queries_follow_its_own_feed(self):
        calls = Mock()
        self.start(calls, mode="sketchdb")
        start_kwargs = calls.feeder.start.call_args.kwargs
        self.assertEqual(start_kwargs["experiment_output_dir"], "/exp/sketchdb")
        self.assertEqual(
            self.client_config["query_groups"][0]["client_options"]["data_start_time"],
            1700000060,
        )

    def test_feeders_that_outlive_pkill_stop_the_run(self):
        # With no_teardown, or an INSERT in flight, they would write into the
        # next mode's recreated tables.
        calls = Mock()
        with self.assertRaisesRegex(RuntimeError, "still running"):
            self.start(calls, exited=False)
        calls.loader.run_init_sql.assert_not_called()
        calls.feeder.start.assert_not_called()


class FinishClickHouseFeedTest(unittest.TestCase):
    def test_stops_the_feed_once_the_queries_are_done(self):
        # Waiting out the feed's duration let it, not the queries, end the run;
        # SIGTERM still lets feeders write their stats.
        calls = Mock()
        calls.wait_for_exit.return_value = True
        _finish_clickhouse_feed(calls)
        self.assertEqual([c[0] for c in calls.mock_calls], ["stop", "wait_for_exit"])
        calls.wait_for_exit.assert_called_once_with(PACED_FEEDER_STOP_TIMEOUT_SECS)


class FedClickHouseConfigValidationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.table = write_table(self.tmp.name)
        self.init_sql = os.path.join(self.tmp.name, "init.sql")
        self.sql = os.path.join(self.tmp.name, "q.sql")
        for path in (self.init_sql, self.sql):
            with open(path, "w") as f:
                f.write("SELECT 1;\n")

    def tearDown(self):
        self.tmp.cleanup()

    def validate(self, feeder=None, sql_file=None, modes=("baseline",), **dataset):
        validate_experiment_config(
            OmegaConf.create(
                {
                    "experiment": [{"mode": m, "server": "clickhouse"} for m in modes],
                    "servers": [{"name": "clickhouse", "url": "http://localhost:8123"}],
                    "dataset": {
                        "name": "custom",
                        "init_sql_file": self.init_sql,
                        "paced_feeder": feeder_config(self.table, **(feeder or {})),
                        **dataset,
                    },
                    "query_groups": [{"sql_file": sql_file or {"baseline": self.sql}}],
                }
            )
        )

    def test_missing_sketchdb_sql_refused(self):
        missing = os.path.join(self.tmp.name, "missing.sql")
        with self.assertRaisesRegex(ValueError, "mode 'sketchdb'"):
            self.validate(
                sql_file={"baseline": self.sql, "sketchdb": missing},
                modes=("baseline", "sketchdb"),
            )

    def test_fed_config_needs_no_data_file(self):
        self.validate()

    def test_missing_init_sql_refused(self):
        # It is the only thing that creates the metric tables.
        with self.assertRaisesRegex(ValueError, "init_sql_file"):
            self.validate(init_sql_file=None)

    def test_missing_workload_table_refused(self):
        with self.assertRaisesRegex(ValueError, "workload_table"):
            self.validate(feeder={"workload_table": None})

    def test_builtin_dataset_refused(self):
        # The feeder generates sketch-bench tables, not clickbench's or h2o's.
        with self.assertRaisesRegex(ValueError, "custom"):
            self.validate(name="clickbench")


if __name__ == "__main__":
    unittest.main()
