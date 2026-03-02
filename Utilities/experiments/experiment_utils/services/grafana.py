"""
Grafana service management for experiment infrastructure.

This module provides a Docker-based Grafana service for dashboard visualization
during experiments.
"""

import os
import subprocess

from .base import DockerServiceBase
from experiment_utils.providers.base import InfrastructureProvider


class GrafanaService(DockerServiceBase):
    """Docker-based Grafana service for experiment dashboards."""

    def __init__(
        self, provider: InfrastructureProvider, num_nodes: int, node_offset: int
    ):
        """
        Initialize Grafana service.

        Args:
            provider: Infrastructure provider for node communication and management
            num_nodes: Number of nodes to manage
            node_offset: Starting node index offset
        """
        super().__init__(provider, num_nodes, node_offset)
        self.container_name = "grafana-demo"

    def get_container_name(self) -> str:
        """Get the Docker container name."""
        return self.container_name

    def get_service_url(self) -> str:
        """Get Grafana URL for health checks."""
        return "http://localhost:3000"

    def get_health_endpoint(self) -> str:
        """Get Grafana health check endpoint."""
        return "/api/health"

    def start(self, admin_password: str = "admin", **kwargs) -> None:
        """
        Start Grafana in Docker container.

        Args:
            admin_password: Admin password for Grafana
            **kwargs: Additional configuration parameters
        """
        # Force cleanup any existing container
        self._force_cleanup_container()

        # Start Grafana container
        docker_cmd = [
            "docker",
            "run",
            "-d",
            "--name",
            self.container_name,
            # "-p",
            # "3000:3000",
            "-e",
            f"GF_SECURITY_ADMIN_PASSWORD={admin_password}",
            "--rm",  # Auto-cleanup when stopped
            "--network",
            "host",
            "grafana/grafana-oss",
        ]

        cmd = " ".join(docker_cmd)
        print(f"Starting Grafana container: {cmd}")

        result = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        if isinstance(result, subprocess.CompletedProcess) and result.returncode != 0:
            raise RuntimeError(f"Failed to start Grafana container: {result.stderr}")

        print(f"Grafana container {self.container_name} started successfully")

    def stop(self, **kwargs) -> None:
        """
        Stop Grafana container.

        Args:
            **kwargs: Additional configuration parameters
        """
        print(f"Stopping Grafana container: {self.container_name}")

        # Stop the container (will auto-remove due to --rm flag)
        stop_cmd = f"docker stop {self.container_name} 2>/dev/null || true"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=stop_cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

        print(f"Grafana container {self.container_name} stopped")

    def configure_dashboard(self, experiment_type: str, experiment_name: str) -> bool:
        """
        Configure Grafana with datasources and dashboard from experiment config.

        Args:
            experiment_type: Experiment type (e.g., 'cloud_demo', 'collapsable')
            experiment_name: Name of the experiment

        Returns:
            True if configuration succeeded, False otherwise
        """
        try:
            print("Configuring Grafana datasources and dashboard...")

            # Construct the command to call grafana_config.py with Python 3.11
            cmd = f"python3.11 grafana_config.py experiment_type={experiment_type} experiment.name={experiment_name} --configure"
            print(cmd)

            # Use the CloudLab home directory pattern like other services
            cmd_dir = os.path.join(
                self.provider.get_home_dir(), "code", "Utilities", "experiments"
            )

            print(f"Calling grafana_config.py: {cmd}")
            print(f"Working directory: {cmd_dir}")

            # Execute the command on the CloudLab node
            result = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=cmd_dir,
                nohup=False,
                popen=False,
            )

            if isinstance(result, subprocess.CompletedProcess):
                if result.returncode == 0:
                    print("✓ Grafana configuration completed successfully")
                    return True
                else:
                    print(
                        f"✗ Grafana configuration failed with exit code {result.returncode}"
                    )
                    if result.stderr:
                        print(f"Error output: {result.stderr}")
                    return False
            else:
                print("✗ Grafana configuration failed - unexpected result type")
                return False

        except Exception as e:
            print(f"Error configuring Grafana: {e}")
            return False

    def get_dashboard_url(self, experiment_name: str) -> str:
        """
        Get the URL for the experiment dashboard.

        Args:
            experiment_name: Name of the experiment

        Returns:
            Full URL to the dashboard
        """
        dashboard_uid = f"exp-{experiment_name}"
        return f"{self.get_service_url()}/d/{dashboard_uid}"
