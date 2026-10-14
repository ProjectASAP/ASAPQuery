"""Tests for the query engine's spill config and end-of-run spill report."""

import json
import os
import subprocess
import tempfile
import unittest
from unittest.mock import Mock

from experiment_utils.services.query_engine import QueryEngineRustService

METRICS = """# HELP asap_store_spill_write_errors_total Evicted-window batches ...
# TYPE asap_store_spill_write_errors_total counter
asap_store_spill_write_errors_total {errors}
"""


def completed(stdout):
    return subprocess.CompletedProcess(args="", returncode=0, stdout=stdout, stderr="")


def engine_store(spill):
    service = QueryEngineRustService(Mock(), use_container=False, node_offset=0)
    config = service._build_engine_config(
        output_dir="/out/query_engine_output",
        data_ingestion_interval_ms=1000,
        log_level="INFO",
        streaming_engine="precompute",
        controller_config_dir="/ctl",
        backend={"type": "prometheus", "forward_unsupported_queries": True},
        http_port=8088,
        remote_write_port=8080,
        lock_strategy="per-key",
        profile_query_engine=False,
        ingest_json_config=None,
        spill=spill,
    )
    return config["store"]


class SpillConfigTest(unittest.TestCase):
    def test_spill_off_leaves_spill_dir_unset(self):
        self.assertEqual(engine_store(False), {"lock_strategy": "per-key"})

    def test_spill_on_writes_under_the_engine_output_dir(self):
        self.assertEqual(
            engine_store(True)["spill_dir"], "/out/query_engine_output/spill"
        )


class RecordSpillTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.provider = Mock()
        self.service = QueryEngineRustService(
            self.provider, use_container=False, node_offset=0
        )
        self.service.remote_spill_dir = "/out/query_engine_output/spill"
        self.service.local_spill_report = os.path.join(self.tmp.name, "report.json")
        self.service.http_port = 8088

    def tearDown(self):
        self.tmp.cleanup()

    def test_records_size_and_error_count(self):
        self.provider.execute_command.side_effect = [
            completed("4096\n"),
            completed(METRICS.format(errors="0")),
        ]
        self.service.record_spill()
        with open(self.service.local_spill_report) as f:
            self.assertEqual(json.load(f), {"spill_bytes": 4096, "write_errors": 0})

    def test_failed_writes_fail_the_run(self):
        self.provider.execute_command.side_effect = [
            completed("4096\n"),
            completed(METRICS.format(errors="2")),
        ]
        with self.assertRaisesRegex(RuntimeError, "2 spill writes failed"):
            self.service.record_spill()

    def test_missing_error_counter_fails_the_run(self):
        # An engine without spill wiring must not pass as "no errors".
        self.provider.execute_command.side_effect = [
            completed("4096\n"),
            completed("other_metric 1\n"),
        ]
        with self.assertRaisesRegex(ValueError, "missing"):
            self.service.record_spill()

    def test_spill_off_is_a_no_op(self):
        service = QueryEngineRustService(
            self.provider, use_container=False, node_offset=0
        )
        service.record_spill()
        self.provider.execute_command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
