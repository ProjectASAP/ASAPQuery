"""
Docker-based Prometheus service management for vertical scalability testing.
"""

import os

from .base import DockerServiceBase
from experiment_utils.providers.base import InfrastructureProvider
from constants import PROMETHEUS_CONFIG_DIR, PROMETHEUS_CONFIG_FILE


class DockerPrometheusService(DockerServiceBase):
    """Docker-based Prometheus service with resource constraints."""

    def __init__(
        self, provider: InfrastructureProvider, num_nodes: int, node_offset: int
    ):
        """
        Initialize Docker Prometheus service.

        Args:
            provider: Infrastructure provider for node communication and management
            num_nodes: Number of nodes to manage
            node_offset: Starting node index offset
        """
        super().__init__(provider, num_nodes, node_offset)
        self.container_name = "prometheus-container"

    def get_container_name(self) -> str:
        """Get the Docker container name."""
        return self.container_name

    def get_service_url(self) -> str:
        """Get Prometheus URL for queries."""
        return "http://localhost:9090"

    def get_query_endpoint_port(self) -> int:
        """Get the query endpoint port for Prometheus."""
        return 9090

    def get_health_endpoint(self) -> str:
        """Get Prometheus health check endpoint."""
        return "/-/ready"

    def start(
        self,
        experiment_output_dir: str,
        cpu_limit: float = None,
        memory_limit: str = None,
        **kwargs,
    ) -> None:
        """
        Start Prometheus in Docker container with optional resource limits.

        Args:
            experiment_output_dir: Directory containing prometheus config
            cpu_limit: Optional number of CPUs to allocate (e.g., 4.0)
            memory_limit: Optional memory limit (e.g., "8g")
            **kwargs: Additional configuration
        """
        # Stop and remove any existing container first
        self._force_cleanup_container()

        # Prepare volume mounts
        prometheus_config_dir = os.path.join(
            experiment_output_dir, PROMETHEUS_CONFIG_DIR
        )
        prometheus_data_dir = os.path.join(experiment_output_dir, "prometheus_data")

        # Create data directory on remote host with proper permissions
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"mkdir -p {prometheus_data_dir} && chmod 777 {prometheus_data_dir}",
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        # Build Docker command
        docker_cmd_parts = [
            f"docker run -d --name {self.container_name}",
        ]

        # Add resource limits if specified
        if cpu_limit is not None:
            docker_cmd_parts.append(f"--cpus={cpu_limit}")
        if memory_limit is not None:
            docker_cmd_parts.append(f"--memory={memory_limit}")

        docker_cmd_parts.extend(
            [
                "-p 9090:9090",
                f"-v {prometheus_config_dir}/{PROMETHEUS_CONFIG_FILE}:/etc/prometheus/{PROMETHEUS_CONFIG_FILE}:ro",
                f"-v {prometheus_data_dir}:/prometheus",
                "prom/prometheus:latest",
                f"--config.file=/etc/prometheus/{PROMETHEUS_CONFIG_FILE}",
                "--storage.tsdb.path=/prometheus",
                "--web.console.libraries=/etc/prometheus/console_libraries",
                "--web.console.templates=/etc/prometheus/consoles",
                "--web.enable-lifecycle",
            ]
        )

        docker_cmd = " ".join(docker_cmd_parts)

        # Run Docker container
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=docker_cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        # Wait for Prometheus to be ready
        self._wait_for_service_ready()

    def stop(self, **kwargs) -> None:
        """
        Stop and remove Prometheus Docker container.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        # Stop and remove container
        cmd = f"docker stop {self.container_name}; docker rm {self.container_name}"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

    def get_prometheus_url(self) -> str:
        """
        Get Prometheus URL for queries.

        Returns:
            Prometheus base URL
        """
        return self.get_service_url()
