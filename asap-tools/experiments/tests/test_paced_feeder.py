"""Tests for running the paced feeder from the e2e runner."""

import json
import os
import subprocess
import tempfile
import unittest
from unittest import mock

from omegaconf import OmegaConf

import constants
from experiment_utils.core import call_generate_prometheus_config, read_exporter_config
from experiment_utils.services import create_prometheus_service
from experiment_utils.services.docker_prometheus import DockerPrometheusService
from experiment_utils.services import paced_feeder
from experiment_utils.services.paced_feeder import (
    PACED_FEEDER_START_LEAD_SECS,
    PACED_FEEDER_STARTUP_CHECK_SECS,
    PacedFeederService,
    aligned_interval_lcm,
    backdated_start_ms,
    paced_feeder_min_duration_secs,
    paced_feeder_command,
    paced_feeder_sinks,
    set_data_start,
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


def check_paced_feeder_config(
    config,
    modes,
    parallelism,
    min_feed_secs,
    forward_unsupported_queries=False,
    auto_discover_labels=False,
):
    paced_feeder.check_paced_feeder_config(
        config,
        modes,
        parallelism,
        min_feed_secs,
        forward_unsupported_queries=forward_unsupported_queries,
        auto_discover_labels=auto_discover_labels,
    )


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
            "gauge",
            1700000000000,
            1700000005000,
            "/out/query_engine_data_stats.json",
        )
        self.assertEqual(
            cmd,
            "./target/release/paced_feeder --sink remote-write"
            f" --url {QE_URL} --workload-table /out/table.json --metric data"
            " --seed 42 --duration-secs 600 --start-ms 1700000000000"
            " --send-at-ms 1700000005000"
            " --stats-out /out/query_engine_data_stats.json",
        )

    def test_optional_flags_are_appended(self):
        cmd = paced_feeder_command(
            feeder_config("/t", samples_per_sec=20, extra_flags="--concurrency 4"),
            "/t",
            QE_URL,
            "data",
            "gauge",
            0,
            0,
            "/s",
        )
        self.assertTrue(cmd.endswith(" --samples-per-sec 20 --concurrency 4"))

    def test_each_metric_gets_its_own_kind_of_rate(self):
        # The feeder exits on --samples-per-sec for a records metric (and on
        # --records-per-sec for a gauge), which the startup check would only
        # report as a dead process.
        config = feeder_config("/t", samples_per_sec=20, records_per_sec=1000)

        def rate_flags(metric, metric_type):
            cmd = paced_feeder_command(
                config, "/t", QE_URL, metric, metric_type, 0, 0, "/s"
            )
            return [f for f in cmd.split() if f.endswith("-per-sec")]

        self.assertEqual(rate_flags("data", "gauge"), ["--samples-per-sec"])
        self.assertEqual(rate_flags("data_total", "counter"), ["--samples-per-sec"])
        self.assertEqual(rate_flags("flows", "records"), ["--records-per-sec"])


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

    def test_sharded_ingest_is_fine_without_sketchdb(self):
        check_paced_feeder_config(feeder_config(self.table), [BASELINE], 2, 600)

    def test_forwarding_to_a_prometheus_without_data_refused(self):
        # In sketchdb mode Prometheus gets no fed data, so a query ASAPQuery
        # can't answer would be forwarded there and come back empty.
        with self.assertRaisesRegex(ValueError, "forward_unsupported_queries"):
            check_paced_feeder_config(
                feeder_config(self.table),
                BOTH_MODES,
                1,
                600,
                forward_unsupported_queries=True,
            )
        check_paced_feeder_config(
            feeder_config(self.table),
            [{**SKETCHDB, "query_prometheus_too": True}, BASELINE],
            1,
            600,
            forward_unsupported_queries=True,
        )

    def test_label_discovery_refused(self):
        # It reads Prometheus before the feeder has sent anything.
        with self.assertRaisesRegex(ValueError, "auto_discover_labels"):
            check_paced_feeder_config(
                feeder_config(self.table),
                BOTH_MODES,
                1,
                600,
                auto_discover_labels=True,
            )

    def test_feeder_ending_before_the_queries(self):
        with self.assertRaisesRegex(ValueError, "at least 601"):
            check_paced_feeder_config(feeder_config(self.table), BOTH_MODES, 1, 601)

    def test_duration_covers_startup_wait_and_queries(self):
        # 60 s steady_state_wait and 533 s of queries: the feeder's startup
        # check comes on top, so 600 s is exactly enough and 599 s is not.
        min_secs = paced_feeder_min_duration_secs(60, 533)
        self.assertEqual(min_secs, PACED_FEEDER_STARTUP_CHECK_SECS + 60 + 533)
        self.assertEqual(min_secs, 600)
        check_paced_feeder_config(feeder_config(self.table), BOTH_MODES, 1, min_secs)
        with self.assertRaisesRegex(ValueError, "at least 600"):
            check_paced_feeder_config(
                feeder_config(self.table, duration_secs=599), BOTH_MODES, 1, min_secs
            )

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


def client_config(*intervals_secs, aligned=True):
    return {
        "query_groups": [
            {
                "id": i,
                "repetition_delay_ms": t * 1000,
                "client_options": {"repetitions": 2, "align_to_interval": aligned},
            }
            for i, t in enumerate(intervals_secs)
        ]
    }


class DataStartTest(unittest.TestCase):
    def test_backdates_to_the_last_boundary(self):
        # Sent from 10:02:23, stamped from 10:00:00.
        send_at = 1791885743000
        self.assertEqual(backdated_start_ms(send_at, 300), 1791885600000)
        self.assertEqual(backdated_start_ms(1791885600000, 300), 1791885600000)
        self.assertEqual(backdated_start_ms(send_at, 1), send_at)

    def test_origin_fits_every_aligned_interval(self):
        self.assertEqual(aligned_interval_lcm(client_config(1, 60, 300)), 300)
        self.assertEqual(aligned_interval_lcm(client_config(60, 90)), 180)
        self.assertEqual(aligned_interval_lcm(client_config(60, aligned=False)), 1)

    def test_aligned_groups_learn_the_data_start(self):
        config = client_config(60)
        config["query_groups"].append(
            {"id": 9, "repetition_delay_ms": 1000, "client_options": {}}
        )
        set_data_start(config, 1791885600000, 1791885743000)
        self.assertEqual(
            config["query_groups"][0]["client_options"],
            {
                "repetitions": 2,
                "align_to_interval": True,
                "data_start_time": 1791885600,
                "data_lag_secs": 143.0,
            },
        )
        self.assertEqual(config["query_groups"][1]["client_options"], {})


# Fake seconds each SSH prep step (binary check, table copy) takes.
PREP_SECS = 10


class RecordingProvider:
    def __init__(self, running_count, launch_secs=0):
        self.commands = []
        self.launches = 0
        self.running_count = running_count
        self.launch_secs = launch_secs
        self.clock = 1700000000.0

    def get_home_dir(self):
        return "/home/u"

    def is_remote(self):
        return True

    def execute_command_parallel(self, node_idxs, cmd, **kwargs):
        self.commands += [(n, cmd) for n in node_idxs]

    def execute_command(self, node_idx, cmd, **kwargs):
        if "--sink" in cmd:
            # One SSH launches every feeder of a node.
            self.launches += 1
            self.clock += self.launch_secs
            self.commands += [
                (node_idx, f"nohup {c.strip()}") for c in cmd.split("nohup ") if c
            ]
        else:
            self.commands.append((node_idx, cmd))
        if cmd.startswith("test -x"):
            self.clock += PREP_SECS
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
    def start(
        self,
        running_count,
        sinks=None,
        align_secs=1,
        launch_secs=0,
        **config_overrides,
    ):
        provider = RecordingProvider(running_count, launch_secs)
        self.provider = provider
        service = PacedFeederService(provider, Args())

        def rsync(*args):
            provider.clock += PREP_SECS
            return "/exp/paced_feeder_output/table.json"

        with tempfile.TemporaryDirectory() as local_dir, mock.patch(
            "experiment_utils.services.paced_feeder.time.sleep"
        ), mock.patch(
            "experiment_utils.services.paced_feeder.time.time",
            lambda: provider.clock,
        ), mock.patch(
            "experiment_utils.services.paced_feeder.sync.rsync_dataset_file",
            side_effect=rsync,
        ):
            self.started = service.start(
                config=feeder_config(write_table(local_dir), **config_overrides),
                sinks=sinks or {"query_engine": QE_URL, "prometheus": PROM_URL},
                experiment_output_dir="/exp",
                local_experiment_dir=local_dir,
                align_secs=align_secs,
            )
        return provider.commands

    @staticmethod
    def flag_values(commands, flag):
        return {
            int(c.split(f" {flag} ")[1].split()[0])
            for _, c in commands
            if "--sink" in c
        }

    def test_send_time_leads_the_launches_not_the_prep(self):
        # The SSH prep (2 nodes x binary check + table copy) takes 40 fake
        # seconds; the shared send time must still be the lead after it.
        commands = self.start(running_count=2)
        send_at_ms = (1700000000 + 4 * PREP_SECS + PACED_FEEDER_START_LEAD_SECS) * 1000
        self.assertEqual(self.flag_values(commands, "--send-at-ms"), {send_at_ms})
        self.assertEqual(self.flag_values(commands, "--start-ms"), {send_at_ms})
        self.assertEqual(self.started, (send_at_ms, send_at_ms))

    def test_data_is_backdated_to_the_aligned_boundary(self):
        # Sent from 1700000045 s, stamped from the last multiple of 300 s.
        commands = self.start(running_count=2, align_secs=300)
        self.assertEqual(self.flag_values(commands, "--start-ms"), {1699999800000})
        self.assertEqual(self.flag_values(commands, "--send-at-ms"), {1700000045000})
        self.assertEqual(self.started, (1699999800000, 1700000045000))

    def test_one_launch_per_node(self):
        self.start(running_count=2)
        self.assertEqual(self.provider.launches, 2)

    def test_launch_slower_than_the_lead_fails(self):
        # A feeder launched after its send time would trail the other sinks.
        with self.assertRaisesRegex(RuntimeError, "longer than the 5 s lead"):
            self.start(running_count=2, launch_secs=3)
        self.assertTrue(any("pkill" in c for _, c in self.provider.commands))

    def test_records_and_gauge_processes_get_their_rate_flags(self):
        commands = self.start(
            running_count=1,
            sinks={"query_engine": QE_URL},
            metrics=["data", "flows"],
            samples_per_sec=20,
            records_per_sec=1000,
        )
        rate_flags = {
            c.split(" --metric ")[1].split()[0]: [
                f for f in c.split() if f.endswith("-per-sec")
            ]
            for _, c in commands
            if "--sink" in c
        }
        self.assertEqual(
            rate_flags,
            {"data": ["--samples-per-sec"], "flows": ["--records-per-sec"]},
        )

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
                (1, PROM_URL, "data"),
                (2, QE_URL, "data_total"),
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

    def remote_write_args(self, metrics, fed=("data",)):
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
                        "exporter_list": {"paced_feeder": {"metrics": list(fed)}},
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

    def test_fed_metrics_come_from_the_feeder_block(self):
        # The feeder sends what its own block lists; a metric not tagged
        # paced_feeder in `metrics` would otherwise reach ASAPQuery twice.
        args = self.remote_write_args(
            [
                {"metric": "data", "labels": ["job"], "exporter": "node_exporter"},
                {"metric": "node_cpu", "labels": ["job"], "exporter": "node_exporter"},
            ]
        )
        self.assertEqual(args.remote_write_metric_names, ["node_cpu"])


if __name__ == "__main__":
    unittest.main()
