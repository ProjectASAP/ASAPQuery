"""
Prometheus Kafka Adapter service management for experiments.
"""

import os
import subprocess

import constants
from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider


class PrometheusKafkaAdapterService(BaseService):
    """Service for managing the Prometheus Kafka adapter."""

    def __init__(self, provider: InfrastructureProvider, node_offset: int):
        """
        Initialize Prometheus Kafka Adapter service.

        Args:
            provider: Infrastructure provider for node communication and management
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.node_offset = node_offset

    def start(self, flink_input_format: str, **kwargs) -> None:
        """
        Start the Prometheus Kafka adapter.

        Args:
            flink_input_format: Input format for Flink
            **kwargs: Additional configuration
        """
        installed_dir = os.path.join(
            self.provider.get_home_dir(), "code", "prometheus-kafka-adapter"
        )
        cmd = './run.sh {} \\"{}\\" {} {} > /dev/null 2>&1 &'.format(
            installed_dir,
            constants.KAFKA_BROKER,
            constants.FLINK_INPUT_TOPIC,
            flink_input_format,
        )
        cmd_dir = os.path.join(installed_dir, "installation")

        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=True,
            popen=False,
        )

    def stop(self, **kwargs) -> None:
        """
        Stop the Prometheus Kafka adapter.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        cmd = "pkill -f prometheus-kafka-adapter-musl"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

    def is_healthy(self) -> bool:
        """
        Check if Prometheus Kafka adapter is healthy.

        Returns:
            True if adapter process is running
        """
        try:
            cmd = "pgrep -f prometheus-kafka-adapter-musl"
            result = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
            assert isinstance(result, subprocess.CompletedProcess)
            return result.stdout.strip() != ""
        except Exception:
            return False
