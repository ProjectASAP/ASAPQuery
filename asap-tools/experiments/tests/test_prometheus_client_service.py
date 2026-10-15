"""Tests for the querier container's start command."""

import unittest
from unittest import mock

import constants
from experiment_utils.services.prometheus_client_service import QueryClientService


class Provider:
    def get_home_dir(self):
        return "/home/u"

    def get_node_ip(self, node_offset):
        return "10.0.0.1"


class StartContainerizedTest(unittest.TestCase):
    def start_cmd(self):
        service = QueryClientService(Provider(), use_container=True, node_offset=0)
        with mock.patch("utils.run_cmd") as run_cmd:
            service.start(
                constants.SKETCHDB_EXPERIMENT_NAME,
                "/out/sketchdb.yaml",
                "/out/inference_config.yaml",
                "/out",
                "client.log",
                export_cost_and_latency=False,
                profile_query_engine_pid=None,
                profile_prometheus_time=None,
                parallel=False,
                backend_protocol="prometheus",
            )
        run_cmd.assert_called_once()
        return run_cmd.call_args.args[0]

    def test_failed_compose_generation_stops_the_start(self):
        steps = self.start_cmd().split(" && ")
        self.assertEqual(
            [s.split()[0] for s in steps], ["cd", "mkdir", "python3", "docker"]
        )
        self.assertIn("generate_prometheus_client_compose.py", steps[2])
        self.assertTrue(steps[3].endswith("up --no-build -d"))
        for step in steps:
            self.assertNotIn(";", step)


if __name__ == "__main__":
    unittest.main()
