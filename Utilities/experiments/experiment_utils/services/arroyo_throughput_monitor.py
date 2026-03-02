"""
Arroyo throughput monitoring service for experiments.
"""

import os
from typing import Optional

import constants
from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider


class ArroyoThroughputMonitor(BaseService):
    """Service for monitoring Arroyo pipeline throughput metrics."""

    def __init__(self, provider: InfrastructureProvider, node_offset: int):
        """
        Initialize Arroyo throughput monitor.

        Args:
            provider: Infrastructure provider for node communication and management
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.node_offset = node_offset
        self.pipeline_id: Optional[str] = None
        self.output_dir: Optional[str] = None
        self.monitor_script_path = None

    def start(self, pipeline_id: str, experiment_output_dir: str, **kwargs) -> None:
        """
        Start throughput monitoring for a specific pipeline.

        Args:
            pipeline_id: ID of the Arroyo pipeline to monitor
            experiment_output_dir: Directory for experiment output
            **kwargs: Additional configuration
        """
        self.pipeline_id = pipeline_id
        self.output_dir = os.path.join(experiment_output_dir, "arroyo_throughput")

        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"mkdir -p {self.output_dir}",
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        # Path to the existing monitoring script on the remote host
        self.monitor_script_path = os.path.join(
            self.provider.get_home_dir(),
            "code",
            "Utilities",
            "experiments",
            "arroyo_throughput_monitor.py",
        )

        # Start the monitoring script on the remote host
        cmd = f"python3 {self.monitor_script_path} --pipeline_id {pipeline_id} --output_dir {self.output_dir} --interval {constants.ARROYO_THROUGHPUT_POLLING_INTERVAL}"

        # Run in background with nohup
        cmd += " > {}/arroyo_throughput_monitor.out 2>&1 &".format(self.output_dir)

        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=True,
            popen=False,
        )

        print(f"Started Arroyo throughput monitoring for pipeline {pipeline_id}")

    def stop(self, **kwargs) -> None:
        """
        Stop throughput monitoring.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        # Kill the monitoring script on the remote host
        cmd = f"pkill -f 'arroyo_throughput_monitor.py.*{self.pipeline_id}'"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

        print(f"Stopped Arroyo throughput monitoring for pipeline {self.pipeline_id}")

    def is_healthy(self) -> bool:
        """
        Check if the throughput monitor is healthy.

        Returns:
            True if monitoring process is running on remote host
        """
        try:
            cmd = f"pgrep -f 'arroyo_throughput_monitor.py.*{self.pipeline_id}'"
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
