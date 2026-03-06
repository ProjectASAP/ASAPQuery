"""
Prometheus target health monitoring service for experiments.
"""

import os
from typing import Optional

import constants
from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider


class PrometheusHealthMonitor(BaseService):
    """Service for monitoring Prometheus target health and scrape performance."""

    def __init__(self, provider: InfrastructureProvider, node_offset: int):
        """
        Initialize Prometheus health monitor.

        Args:
            provider: Infrastructure provider for node communication and management
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.node_offset = node_offset
        self.output_dir: Optional[str] = None
        self.monitor_script_path = None
        self.prometheus_url = "http://localhost:9090"

    def start(
        self,
        experiment_output_dir: str,
        **kwargs,
    ) -> None:
        """
        Start health monitoring for Prometheus targets.

        Args:
            experiment_output_dir: Directory for experiment output
            prometheus_url: URL of the Prometheus server to monitor
            **kwargs: Additional configuration
        """

        self.output_dir = os.path.join(experiment_output_dir, "prometheus_health")

        # Path to the monitoring script on the remote host
        self.monitor_script_path = os.path.join(
            self.provider.get_home_dir(),
            "code",
            "asap-tools",
            "experiments",
            "prometheus_health_monitor.py",
        )

        # Start the monitoring script on the remote host
        cmd = f"nohup python3 {self.monitor_script_path} --prometheus_url {self.prometheus_url} --output_dir {self.output_dir} --interval {constants.PROMETHEUS_HEALTH_POLLING_INTERVAL}"

        # Run in background with nohup
        cmd += " > {}/prometheus_health_monitor.out 2>&1 &".format(self.output_dir)

        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"mkdir -p {self.output_dir}; {cmd}",
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        print(f"Started Prometheus health monitoring for {self.prometheus_url}")

    def stop(self, **kwargs) -> None:
        """
        Stop health monitoring.

        Args:
            **kwargs: Additional configuration (currently unused)
        """

        # Kill the monitoring script on the remote host
        cmd = "pkill -f 'prometheus_health_monitor.py'"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

        print(f"Stopped Prometheus health monitoring for {self.prometheus_url}")

    def is_healthy(self) -> bool:
        """
        Check if the health monitor is healthy.

        Returns:
            True if monitoring process is running on remote host
        """

        try:
            cmd = "pgrep -f 'prometheus_health_monitor.py'"
            result = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
            return result.stdout.strip() != ""
        except Exception:
            return False
