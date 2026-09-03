"""Tests for optional planner discovery configuration."""

import tempfile
import unittest
from pathlib import Path

from experiment_utils.services.misc import ControllerService, DiscoveryBackend
from generate_controller_compose import generate_compose_file


class RecordingProvider:
    def __init__(self):
        self.commands = []

    def get_home_dir(self):
        return "/tmp"

    def execute_command(self, **kwargs):
        self.commands.append(kwargs["cmd"])


class OptionalPlannerDiscoveryTest(unittest.TestCase):
    def test_controller_service_omits_prometheus_url_when_discovery_disabled(self):
        provider = RecordingProvider()
        service = ControllerService(provider, use_container=True, node_offset=0)

        service._start_containerized(
            controller_input_file="/tmp/input.yaml",
            streaming_engine="precompute",
            controller_remote_output_dir="/tmp/output",
            punting=False,
            discovery_backend=None,
            data_ingestion_interval_ms=1000,
            query_language="promql",
        )

        self.assertEqual(len(provider.commands), 1)
        self.assertNotIn("--prometheus-url", provider.commands[0])

    def test_controller_service_preserves_explicit_discovery_url(self):
        provider = RecordingProvider()
        service = ControllerService(provider, use_container=True, node_offset=0)

        service._start_containerized(
            controller_input_file="/tmp/input.yaml",
            streaming_engine="precompute",
            controller_remote_output_dir="/tmp/output",
            punting=False,
            discovery_backend=DiscoveryBackend(
                type="prometheus", url="http://localhost:9090", database=None
            ),
            data_ingestion_interval_ms=1000,
            query_language="promql",
        )

        self.assertIn("--prometheus-url http://localhost:9090", provider.commands[0])

    def test_compose_generator_omits_discovery_url_when_not_provided(self):
        template_path = (
            Path(__file__).resolve().parents[3]
            / "asap-planner-rs/docker-compose.yml.j2"
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "controller-compose.yml"
            generate_compose_file(
                template_path=str(template_path),
                output_path=str(output_path),
                controller_dir="/tmp/planner",
                container_name="controller",
                input_config_path="/tmp/input.yaml",
                output_dir="/tmp/output",
                data_ingestion_interval_ms=1000,
                streaming_engine="precompute",
                punting=False,
                prometheus_url=None,
                query_language="promql",
            )
            compose = output_path.read_text()

        self.assertNotIn("--prometheus-url", compose)


if __name__ == "__main__":
    unittest.main()
