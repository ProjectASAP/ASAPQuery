"""Tests for ClusterDataExporterService data-file validation and node checks."""

import subprocess
import unittest

from experiment_utils.services.cluster_data_exporter import ClusterDataExporterService


class RecordingProvider:
    """Answers every command with one matching file and records the commands."""

    def __init__(self):
        self.commands = []

    def execute_command(self, node_idx, cmd, cmd_dir, nohup, popen):
        self.commands.append(cmd)
        return subprocess.CompletedProcess([], 0, "1\n", "")


class ValidateAlibabaDataTest(unittest.TestCase):
    def _counted_pattern(self, data_type, data_year):
        provider = RecordingProvider()
        service = ClusterDataExporterService(provider, 0, "/traces")
        service._validate_alibaba_data(data_type, data_year)
        return [c for c in provider.commands if "wc -l" in c][0]

    def test_patterns_match_exporter_file_names(self):
        # The check used to look for MsResource_*.csv.gz for both years, a
        # name the exporter never reads, so valid data was rejected.
        cases = {
            ("node", 2021): "Node_*.csv.gz",
            ("node", 2022): "NodeMetrics_*.csv.gz",
            ("msresource", 2021): "MSResource_*.csv.gz",
            ("msresource", 2022): "MSMetrics_*.csv.gz",
        }
        for (data_type, data_year), pattern in cases.items():
            with self.subTest(data_type=data_type, data_year=data_year):
                self.assertIn(
                    f"/traces/{pattern}", self._counted_pattern(data_type, data_year)
                )

    def test_unknown_year_raises(self):
        service = ClusterDataExporterService(RecordingProvider(), 0, "/traces")
        with self.assertRaises(ValueError):
            service._validate_alibaba_data("msresource", 2020)


class DockerCommandTest(unittest.TestCase):
    def test_msresource_uses_exporter_cli_value(self):
        # The exporter's clap enum spells it ms-resource; passing the config
        # value through made the container exit before serving metrics.
        service = ClusterDataExporterService(RecordingProvider(), 0, "/traces")
        service.container_name = "cde"
        cmd = service._build_docker_command(
            {"provider": "alibaba", "data_type": "msresource", "data_year": 2022},
            port=40000,
            output_dir="/out",
        )
        self.assertIn("--data-type=ms-resource", cmd)


class NodeCountTest(unittest.TestCase):
    def test_more_than_one_worker_node_is_rejected(self):
        service = ClusterDataExporterService(RecordingProvider(), 0, "/traces")
        with self.assertRaises(AssertionError):
            service.start({"provider": "google"}, "/out", "/local", num_nodes=2)


if __name__ == "__main__":
    unittest.main()
