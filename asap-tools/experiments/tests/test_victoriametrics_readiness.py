"""Regression tests for VictoriaMetrics and query-engine readiness."""

import subprocess
import unittest
from unittest.mock import Mock, patch

from constants import BASELINE_EXPERIMENT_NAME, SKETCHDB_EXPERIMENT_NAME
from experiment_utils.services.base import DockerServiceBase
from experiment_utils.services.docker_victoriametrics import get_remote_write_urls
from experiment_utils.services.query_engine import QueryEngineRustService


class RemoteWriteConfigurationTest(unittest.TestCase):
    def test_sketchdb_uses_prometheus_remote_write_endpoint(self):
        self.assertEqual(
            get_remote_write_urls("10.10.1.1", SKETCHDB_EXPERIMENT_NAME),
            [
                "http://10.10.1.1:8428/api/v1/write",
                "http://10.10.1.1:8080/api/v1/write",
            ],
        )

    def test_baseline_only_writes_to_vmsingle(self):
        self.assertEqual(
            get_remote_write_urls("10.10.1.1", BASELINE_EXPERIMENT_NAME),
            ["http://10.10.1.1:8428/api/v1/write"],
        )

    def test_unknown_experiment_mode_fails_loudly(self):
        with self.assertRaises(ValueError):
            get_remote_write_urls("10.10.1.1", "unknown")


class FakeDockerService(DockerServiceBase):
    def start(self, **kwargs):
        pass

    def stop(self, **kwargs):
        pass

    def get_container_name(self):
        return "fake"

    def get_service_url(self):
        return "http://localhost:8428"

    def get_health_endpoint(self):
        return "/health"

    def is_healthy(self):
        return True


class ReadinessCheckTest(unittest.TestCase):
    def test_http_400_is_not_considered_ready(self):
        provider = Mock()
        provider.execute_command.return_value = subprocess.CompletedProcess(
            args="curl", returncode=22, stdout="", stderr=""
        )
        service = FakeDockerService(provider, num_nodes=1, node_offset=0)

        with patch("experiment_utils.services.base.time.sleep"):
            with self.assertRaises(RuntimeError):
                service._wait_for_service_ready(max_retries=1)

        command = provider.execute_command.call_args.kwargs["cmd"]
        self.assertIn("curl -fsS", command)

    def test_query_engine_requires_runtime_info_endpoint(self):
        provider = Mock()
        provider.execute_command.side_effect = [
            subprocess.CompletedProcess(
                args="docker inspect", returncode=0, stdout="true\n", stderr=""
            ),
            subprocess.CompletedProcess(
                args="curl", returncode=22, stdout="", stderr=""
            ),
        ]
        service = QueryEngineRustService(provider, use_container=True, node_offset=0)

        self.assertFalse(service.is_healthy())
        self.assertIn(
            "/api/v1/status/runtimeinfo",
            provider.execute_command.call_args_list[1].kwargs["cmd"],
        )


if __name__ == "__main__":
    unittest.main()
