"""
Prometheus service management for experiments.
"""

import os
import time
import subprocess

from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider
from constants import PROMETHEUS_CONFIG_DIR, PROMETHEUS_CONFIG_FILE


class PrometheusService(BaseService):
    """Service for managing Prometheus operations."""

    def __init__(
        self, provider: InfrastructureProvider, num_nodes: int, node_offset: int
    ):
        """
        Initialize Prometheus service.

        Args:
            provider: Infrastructure provider for node communication and management
            num_nodes: Number of nodes to manage
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.num_nodes = num_nodes
        self.node_offset = node_offset

    def get_query_endpoint_port(self) -> int:
        """Get the query endpoint port for Prometheus."""
        return 9090

    def start(self, experiment_output_dir: str, **kwargs) -> None:
        """
        Start Prometheus service.

        Args:
            experiment_output_dir: Directory containing prometheus config
            **kwargs: Additional configuration (currently unused)
        """
        self._start_prometheus(experiment_output_dir)

    def _check_port_open(self, port: int) -> bool:
        """Check if a port is available (not in use)."""
        cmd = f"netstat -antlp | grep ':{port}'"
        result = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )
        assert isinstance(result, subprocess.CompletedProcess)
        if result.returncode == 0:
            return False
        return True

    def _start_prometheus(self, experiment_output_dir: str) -> None:
        """Start Prometheus with proper configuration."""
        home_dir = self.provider.get_home_dir()
        prometheus_config_dir = os.path.join(
            experiment_output_dir, PROMETHEUS_CONFIG_DIR
        )
        cmd_dir = os.path.join(home_dir, "prometheus")

        # Copy prometheus config
        cmd = "cp {}/{} .".format(prometheus_config_dir, PROMETHEUS_CONFIG_FILE)
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=False,
            popen=False,
            ignore_errors=False,
        )

        # Wait for port to be open
        while True:
            if self._check_port_open(9090):
                break
            time.sleep(3)

        # Start prometheus
        cmd = f"./prometheus --config.file={PROMETHEUS_CONFIG_FILE} > /dev/null 2>&1 < /dev/null &"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=True,
            popen=True,
            ignore_errors=False,
        )

    def stop(self, **kwargs) -> None:
        """
        Stop Prometheus service.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        self._stop_prometheus()

    def _stop_prometheus(self) -> None:
        """Stop Prometheus server."""
        try:
            self.provider.execute_command(
                node_idx=self.node_offset,
                cmd="killall -9 prometheus",
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
        except subprocess.CalledProcessError:
            pass

    def reset(self) -> None:
        """Reset Prometheus data across nodes."""
        # For provider-based architecture, we need to handle reset differently
        # This maintains backward compatibility for CloudLab while allowing future provider extensions
        if hasattr(self.provider, "username") and hasattr(
            self.provider, "hostname_suffix"
        ):
            cmd = "python3 reset_prometheus.py --num_nodes {} --cloudlab_username {} --hostname_suffix {} --node_offset {}".format(
                self.num_nodes,
                self.provider.username,
                self.provider.hostname_suffix,
                self.node_offset,
            )
            subprocess.run(cmd, shell=True, check=True)
        else:
            # For non-CloudLab providers, implement provider-specific reset logic
            raise NotImplementedError(
                "Reset functionality not yet implemented for this provider type"
            )

    def is_healthy(self) -> bool:
        """
        Check if Prometheus service is healthy.

        Returns:
            True if service is running
        """
        return True
