"""Regression tests for fake exporter service orchestration."""

import tempfile
import unittest
from unittest.mock import patch

from experiment_utils.services.fake_exporters import RustExporterService


class FakeExporterArgs:
    def get_node_range(self, include_coordinator=True):
        return [1, 2] if not include_coordinator else [0, 1, 2]

    def get_coordinator_node(self):
        return 0


class FakeExporterProvider:
    def get_home_dir(self):
        return "/tmp"


class RustExporterContainerBatchingTest(unittest.TestCase):
    def test_container_batches_are_sequential_per_node(self):
        """Keep Docker startup bounded per node while retaining cross-node fan-out."""
        service = RustExporterService(
            provider=FakeExporterProvider(),
            args=FakeExporterArgs(),
            use_container=True,
        )
        config = {
            "num_ports_per_server": 8,
            "dataset": "demo",
            "start_port": 50000,
            "synthetic_data_value_scale": 1,
            "num_labels": 1,
            "num_values_per_label": 2,
            "metric_type": "gauge",
        }
        rounds = []

        def record_round(_provider, node_commands, **kwargs):
            rounds.append((list(node_commands), kwargs))

        # Regression coverage for Roborev 139: submitting all per-node batches
        # together allows multiple batches to overlap on the same Docker daemon.
        with patch(
            "experiment_utils.services.fake_exporters."
            "execute_fake_exporter_commands_in_parallel",
            side_effect=record_round,
        ):
            with tempfile.TemporaryDirectory() as local_experiment_dir:
                service._start_containerized(
                    config=config,
                    experiment_output_dir="",
                    local_experiment_dir=local_experiment_dir,
                )

        self.assertEqual(len(rounds), 2)
        self.assertEqual(
            [
                [node_idx for node_idx, _ in node_commands]
                for node_commands, _ in rounds
            ],
            [[1, 2], [1, 2]],
        )
        self.assertEqual(
            [
                [command.count("docker run") for _, command in node_commands]
                for node_commands, _ in rounds
            ],
            [[5, 5], [3, 3]],
        )
        self.assertTrue(all(kwargs["wait"] for _, kwargs in rounds))


if __name__ == "__main__":
    unittest.main()
