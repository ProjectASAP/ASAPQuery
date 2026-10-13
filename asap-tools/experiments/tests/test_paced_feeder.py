"""Tests for running the paced feeder from the e2e runner."""

import json
import os
import subprocess
import tempfile
import unittest
from unittest import mock

import yaml

from omegaconf import OmegaConf

import constants
import generate_prometheus_config
from experiment_utils.core import (
    GeneratePrometheusArgs,
    call_generate_prometheus_config,
    read_exporter_config,
)
from experiment_utils.services import create_prometheus_service
from experiment_utils.services.docker_prometheus import DockerPrometheusService
from experiment_utils.services.paced_feeder import (
    PacedFeederService,
    check_paced_feeder_config,
    paced_feeder_command,
    paced_feeder_sinks,
)
from experiment_utils.services.prometheus import PrometheusService

QE_URL = "http://10.10.1.1:8080/api/v1/write"
SKETCHDB = {"mode": "sketchdb", "server": "sketchdb"}
BASELINE = {"mode": "baseline", "server": "prometheus"}
BOTH_MODES = [SKETCHDB, BASELINE]
PROM_URL = "http://10.10.1.1:9090/api/v1/write"

# workloads[0].e2e.metrics of synthetic-templatesclassic-shared1.json, plus a
# records metric from the multi-grouping tables.
LABELS = [
    {"cardinality": 10, "name": "job"},
    {"cardinality": 1000, "child_of": "job", "name": "label_0"},
]
E2E_METRICS = {
    "data": {
        "labels": LABELS,
        "samples_per_sec_per_series": 200,
        "type": "gauge",
        "values": {"dist": {"pareto": 2.0}, "value": {"keys": 10000, "zipf": 1.1}},
    },
    "data_total": {
        "labels": LABELS,
        "samples_per_sec_per_series": 200,
        "type": "counter",
        "values": {"increment": {"keys": 10000, "zipf": 1.1}},
    },
    "flows": {
        "labels": [{"cardinality": 3, "name": "proto", "skew": 0.0}],
        "records_per_sec": 4000000,
        "type": "records",
        "values": {"src_ip": {"keys": 1000000, "zipf": 0.0}},
    },
}


def write_table(directory, queries=()):
    path = os.path.join(directory, "table.json")
    with open(path, "w") as f:
        json.dump(
            {"workloads": [{"e2e": {"metrics": E2E_METRICS, "queries": queries}}]}, f
        )
    return path


def feeder_config(table, **overrides):
    return {
        "workload_table": table,
        "metrics": ["data", "data_total"],
        "seed": 42,
        "duration_secs": 600,
        **overrides,
    }


class PacedFeederCommandTest(unittest.TestCase):
    def test_command_line(self):
        cmd = paced_feeder_command(
            feeder_config("/t/table.json"),
            "/out/table.json",
            QE_URL,
            "data",
            1700000005000,
            "/out/query_engine_data_stats.json",
        )
        self.assertEqual(
            cmd,
            "./target/release/paced_feeder --sink remote-write"
            f" --url {QE_URL} --workload-table /out/table.json --metric data"
            " --seed 42 --duration-secs 600 --start-ms 1700000005000"
            " --stats-out /out/query_engine_data_stats.json",
        )

    def test_optional_flags_are_appended(self):
        cmd = paced_feeder_command(
            feeder_config("/t", samples_per_sec=20, extra_flags="--concurrency 4"),
            "/t",
            QE_URL,
            "data",
            0,
            "/s",
        )
        self.assertTrue(cmd.endswith(" --samples-per-sec 20 --concurrency 4"))


class PacedFeederSinksTest(unittest.TestCase):
    def test_sketchdb_feeds_the_query_engine_only(self):
        self.assertEqual(
            paced_feeder_sinks({"mode": "sketchdb"}, QE_URL, PROM_URL),
            {"query_engine": QE_URL},
        )

    def test_sketchdb_querying_prometheus_too_feeds_both(self):
        self.assertEqual(
            paced_feeder_sinks(
                {"mode": "sketchdb", "query_prometheus_too": True}, QE_URL, PROM_URL
            ),
            {"query_engine": QE_URL, "prometheus": PROM_URL},
        )

    def test_baseline_feeds_prometheus(self):
        self.assertEqual(
            paced_feeder_sinks({"mode": "baseline"}, QE_URL, PROM_URL),
            {"prometheus": PROM_URL},
        )


class CheckPacedFeederConfigTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.table = write_table(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_valid_config(self):
        check_paced_feeder_config(feeder_config(self.table), BOTH_MODES, 1, 600)

    def test_unknown_metric(self):
        with self.assertRaisesRegex(ValueError, "'cpu' is not in"):
            check_paced_feeder_config(
                feeder_config(self.table, metrics=["cpu"]), BOTH_MODES, 1, 600
            )

    def test_records_metric_to_query_engine_only(self):
        check_paced_feeder_config(
            feeder_config(self.table, metrics=["data", "flows"]), [SKETCHDB], 1, 600
        )

    def test_records_metric_refused_for_prometheus(self):
        # Many records share a series and timestamp; Prometheus' receiver
        # rejects those duplicates.
        records = feeder_config(self.table, metrics=["flows"])
        with self.assertRaisesRegex(ValueError, r"records metric.*\['baseline'\]"):
            check_paced_feeder_config(records, BOTH_MODES, 1, 600)
        with self.assertRaisesRegex(ValueError, r"query_prometheus_too"):
            check_paced_feeder_config(
                records, [{**SKETCHDB, "query_prometheus_too": True}], 1, 600
            )

    def test_unknown_metric_type(self):
        with open(self.table) as f:
            table = json.load(f)
        table["workloads"][0]["e2e"]["metrics"]["data"]["type"] = "histogram"
        with open(self.table, "w") as f:
            json.dump(table, f)
        with self.assertRaisesRegex(ValueError, "unknown type 'histogram'"):
            check_paced_feeder_config(feeder_config(self.table), [SKETCHDB], 1, 600)

    def test_sharded_ingest(self):
        with self.assertRaisesRegex(ValueError, "parallelism=1"):
            check_paced_feeder_config(feeder_config(self.table), BOTH_MODES, 2, 600)

    def test_feeder_ending_before_the_queries(self):
        with self.assertRaisesRegex(ValueError, "at least 601"):
            check_paced_feeder_config(feeder_config(self.table), BOTH_MODES, 1, 601)

    def test_missing_keys_rejected_with_the_other_exporters(self):
        params = OmegaConf.create(
            {
                "exporters": {
                    "only_start_if_queries_exist": False,
                    "exporter_list": {"paced_feeder": {"workload_table": "t"}},
                }
            }
        )
        config, reason = read_exporter_config(params)
        self.assertIsNone(config)
        self.assertIn("duration_secs", reason)


class RecordingProvider:
    def __init__(self, running_count):
        self.commands = []
        self.running_count = running_count

    def get_home_dir(self):
        return "/home/u"

    def is_remote(self):
        return True

    def execute_command(self, node_idx, cmd, **kwargs):
        self.commands.append((node_idx, cmd))
        returncode = 0
        if cmd.startswith("pgrep"):
            returncode = 0 if cmd.endswith(f"-qx {self.running_count}") else 1
        return subprocess.CompletedProcess(cmd, returncode)


class Args:
    def get_node_range(self, include_coordinator=True):
        return [0, 1, 2] if include_coordinator else [1, 2]

    def get_coordinator_node(self):
        return 0


class PacedFeederServiceStartTest(unittest.TestCase):
    def start(self, running_count):
        provider = RecordingProvider(running_count)
        service = PacedFeederService(provider, Args())
        with tempfile.TemporaryDirectory() as local_dir, mock.patch(
            "experiment_utils.services.paced_feeder.time.sleep"
        ), mock.patch(
            "experiment_utils.services.paced_feeder.sync.rsync_dataset_file",
            return_value="/exp/paced_feeder_output/table.json",
        ):
            service.start(
                config=feeder_config("/local/table.json"),
                sinks={"query_engine": QE_URL, "prometheus": PROM_URL},
                experiment_output_dir="/exp",
                local_experiment_dir=local_dir,
            )
        return provider.commands

    def test_one_process_per_sink_and_metric_spread_over_workers(self):
        commands = self.start(running_count=2)
        feeders = [(n, c) for n, c in commands if "--sink" in c]
        self.assertEqual(
            [
                (
                    n,
                    c.split(" --url ")[1].split()[0],
                    c.split(" --metric ")[1].split()[0],
                )
                for n, c in feeders
            ],
            [
                (1, QE_URL, "data"),
                (2, QE_URL, "data_total"),
                (1, PROM_URL, "data"),
                (2, PROM_URL, "data_total"),
            ],
        )
        # One event-time origin, so the sinks get the same timestamps.
        self.assertEqual(
            len({c.split(" --start-ms ")[1].split()[0] for _, c in feeders}), 1
        )
        self.assertTrue(
            all(
                "--workload-table /exp/paced_feeder_output/table.json" in c
                for _, c in feeders
            )
        )

    def test_dead_feeder_fails_the_experiment(self):
        # A wrong URL stops a feeder after its first tick; without this check
        # the run would carry on with no data.
        with self.assertRaisesRegex(RuntimeError, "expected 2 processes"):
            self.start(running_count=1)


class StartCommandProvider:
    def __init__(self):
        self.commands = []

    def get_home_dir(self):
        return "/home/u"

    def execute_command(self, node_idx, cmd, **kwargs):
        self.commands.append(cmd)
        # netstat finds nothing on the Prometheus port.
        return subprocess.CompletedProcess(cmd, 1)


class PrometheusRemoteWriteReceiverTest(unittest.TestCase):
    def bare_metal_command(self, receiver):
        provider = StartCommandProvider()
        PrometheusService(provider, 1, 0, remote_write_receiver=receiver).start("/exp")
        return provider.commands[-1]

    def test_bare_metal_flag(self):
        self.assertIn(
            constants.PROMETHEUS_REMOTE_WRITE_RECEIVER_FLAG,
            self.bare_metal_command(True),
        )
        self.assertNotIn(
            constants.PROMETHEUS_REMOTE_WRITE_RECEIVER_FLAG,
            self.bare_metal_command(False),
        )

    def test_containerized_flag(self):
        provider = StartCommandProvider()
        service = DockerPrometheusService(provider, 1, 0, remote_write_receiver=True)
        with mock.patch.object(service, "_force_cleanup_container"), mock.patch.object(
            service, "_wait_for_service_ready"
        ):
            service.start("/exp")
        self.assertIn(
            constants.PROMETHEUS_REMOTE_WRITE_RECEIVER_FLAG, provider.commands[-1]
        )

    def test_enabled_when_the_paced_feeder_is_configured(self):
        def service(exporter_list):
            cfg = OmegaConf.create(
                {
                    "experiment_params": {
                        "monitoring": {
                            "tool": "prometheus",
                            "deployment_mode": "bare_metal",
                        },
                        "exporters": {"exporter_list": exporter_list},
                    }
                }
            )
            return create_prometheus_service(cfg, StartCommandProvider(), 1, 0)

        self.assertTrue(service({"paced_feeder": {}}).remote_write_receiver)
        self.assertFalse(service({"fake_exporter": {}}).remote_write_receiver)


class PrometheusForwardingTest(unittest.TestCase):
    """In sketchdb mode Prometheus must not forward fed metrics to the query
    engine, which the feeder already writes to."""

    def remote_write_args(self, metrics):
        cfg = OmegaConf.create(
            {
                "streaming": {
                    "parallelism": 1,
                    "remote_write": {
                        "ip": "10.10.1.1",
                        "base_port": 8080,
                        "path": "/api/v1/write",
                    },
                },
                "experiment_params": {
                    "exporters": {
                        "only_start_if_queries_exist": False,
                        "exporter_list": {},
                    },
                    "query_groups": [],
                    "metrics": metrics,
                },
            }
        )
        args = mock.Mock(remote_write_url=None, remote_write_metric_names=None)
        with mock.patch("generate_prometheus_config.main"), mock.patch(
            "generate_victoriametrics_config.main"
        ):
            call_generate_prometheus_config(
                args,
                cfg,
                constants.SKETCHDB_EXPERIMENT_NAME,
                constants.SKETCHDB_EXPERIMENT_NAME,
            )
        return args

    def test_only_fed_metrics_means_no_remote_write(self):
        args = self.remote_write_args(
            [{"metric": "data", "labels": ["job"], "exporter": "paced_feeder"}]
        )
        self.assertIsNone(args.remote_write_url)

    def test_scraped_metrics_are_still_forwarded(self):
        args = self.remote_write_args(
            [
                {"metric": "data", "labels": ["job"], "exporter": "paced_feeder"},
                {"metric": "node_cpu", "labels": ["job"], "exporter": "node_exporter"},
            ]
        )
        self.assertEqual(args.remote_write_url, QE_URL)
        self.assertEqual(args.remote_write_metric_names, ["node_cpu"])


class PrometheusOutOfOrderWindowTest(unittest.TestCase):
    """The feeder sends a tick's requests concurrently, so Prometheus' receiver
    sees a series' samples out of order within a second and, without an
    out-of-order window, rejects them (400 "out of order sample"). Bare-metal
    and containerized Prometheus both read this generated prometheus.yml."""

    def storage(self, exporter_list):
        with tempfile.TemporaryDirectory() as out:
            prometheus_config = OmegaConf.create(
                {
                    "scrape_interval": "1s",
                    "evaluation_interval": "10s",
                    "recording_rules": {"interval": "5s"},
                }
            )
            args = GeneratePrometheusArgs(
                1, out, out, prometheus_config, "10.10.1.1", "10.10.1", 0
            )
            generate_prometheus_config.main(
                args,
                {
                    "exporters": {
                        "only_start_if_queries_exist": False,
                        "exporter_list": exporter_list,
                    },
                    "query_groups": [],
                    "metrics": [],
                },
            )
            with open(os.path.join(out, constants.PROMETHEUS_CONFIG_FILE)) as f:
                return yaml.safe_load(f).get("storage")

    def test_window_set_when_the_feeder_targets_prometheus(self):
        feeder = {"workload_table": "t", "metrics": [], "seed": 1, "duration_secs": 1}
        self.assertEqual(
            self.storage({"paced_feeder": feeder}),
            {"tsdb": {"out_of_order_time_window": "5s"}},
        )

    def test_no_window_without_the_feeder(self):
        self.assertIsNone(self.storage({}))


if __name__ == "__main__":
    unittest.main()
