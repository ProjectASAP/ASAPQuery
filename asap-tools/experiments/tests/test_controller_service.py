"""Tests for optional planner discovery configuration."""

import tempfile
import unittest
from pathlib import Path

import yaml

from experiment_utils.services.misc import (
    ControllerService,
    DiscoveryBackend,
    MilpPlanner,
)
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


MILP = MilpPlanner(saturation_dir="/sat", workload_facts="/facts.yaml", w_cpu=0.5)


class MilpPlannerTest(unittest.TestCase):
    def start(self, use_container, milp):
        provider = RecordingProvider()
        ControllerService(provider, use_container, node_offset=0).start(
            controller_input_file="/tmp/input.yaml",
            streaming_engine="precompute",
            controller_remote_output_dir="/tmp/output",
            punting=False,
            discovery_backend=None,
            data_ingestion_interval_ms=1000,
            milp=milp,
        )
        return provider.commands[0]

    def test_bare_metal_passes_milp_flags(self):
        cmd = self.start(use_container=False, milp=MILP)
        self.assertIn(
            " --planner milp --workload-facts /facts.yaml"
            " --atomic-costs /sat/optimizer_cost/rqe_atomic_costs.json"
            " --saturation-dir /sat --w-cpu 0.5",
            cmd,
        )
        self.assertNotIn("--w-mem", cmd)

    def test_container_passes_milp_inputs_to_compose_generator(self):
        cmd = self.start(use_container=True, milp=MILP)
        self.assertIn(
            " --saturation-dir /sat --workload-facts /facts.yaml --w-cpu 0.5", cmd
        )

    def test_legacy_planner_has_no_milp_flags(self):
        for use_container in (False, True):
            cmd = self.start(use_container=use_container, milp=None)
            self.assertNotIn("--planner", cmd)
            self.assertNotIn("--saturation-dir", cmd)

    def render(self, **milp_kwargs):
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
                **milp_kwargs,
            )
            return yaml.safe_load(output_path.read_text())["services"]["controller"]

    def test_compose_mounts_milp_inputs_and_points_flags_at_them(self):
        controller = self.render(
            saturation_dir="/sat", workload_facts="/facts.yaml", w_mem=3.0
        )
        self.assertIn("/sat:/app/saturation:ro", controller["volumes"])
        self.assertIn(
            "/facts.yaml:/app/input/workload_facts.yaml:ro", controller["volumes"]
        )
        command = controller["command"]
        flags = dict(zip(command[::2], command[1::2]))
        self.assertEqual(flags["--planner"], "milp")
        self.assertEqual(flags["--workload-facts"], "/app/input/workload_facts.yaml")
        self.assertEqual(
            flags["--atomic-costs"],
            "/app/saturation/optimizer_cost/rqe_atomic_costs.json",
        )
        self.assertEqual(flags["--saturation-dir"], "/app/saturation")
        self.assertEqual(flags["--w-mem"], "3.0")
        self.assertNotIn("--w-cpu", flags)

    def test_compose_without_milp_is_unchanged(self):
        controller = self.render()
        self.assertEqual(len(controller["volumes"]), 2)
        self.assertNotIn("--planner", controller["command"])


if __name__ == "__main__":
    unittest.main()
