"""
Base service class for experiment infrastructure management.
"""

import subprocess
import time
from abc import ABC, abstractmethod
from typing import Optional

from experiment_utils.providers.base import InfrastructureProvider


class BaseService(ABC):
    """Abstract base class for all services with common interface."""

    def __init__(self, provider: InfrastructureProvider):
        """
        Initialize base service.

        Args:
            provider: Infrastructure provider for node communication and management
        """
        self.provider = provider

        # Maintain backward compatibility properties
        if hasattr(provider, "username"):
            self.username = provider.username
        if hasattr(provider, "hostname_suffix"):
            self.hostname_suffix = provider.hostname_suffix

    @abstractmethod
    def start(self, **kwargs) -> None:
        """
        Start the service. Must be implemented by subclasses.

        Args:
            **kwargs: Service-specific configuration parameters
        """
        pass

    @abstractmethod
    def stop(self, **kwargs) -> None:
        """
        Stop the service. Must be implemented by subclasses.

        Args:
            **kwargs: Service-specific configuration parameters
        """
        pass

    def is_healthy(self) -> bool:
        """
        Check if service is healthy. Can be overridden by subclasses.

        Returns:
            True if service is running and healthy, False otherwise
        """
        return True

    def restart(self, **kwargs) -> None:
        """
        Restart the service. Default implementation stops then starts.

        Args:
            **kwargs: Service-specific configuration parameters
        """
        self.stop(**kwargs)
        self.start(**kwargs)

    def __str__(self) -> str:
        """String representation of the service."""
        return f"{self.__class__.__name__}()"

    def __repr__(self) -> str:
        """Detailed string representation of the service."""
        return f"{self.__class__.__name__}(provider={self.provider!r})"


class DockerServiceBase(BaseService):
    """Abstract base class for Docker-based services with common container management."""

    def __init__(
        self, provider: InfrastructureProvider, num_nodes: int, node_offset: int
    ):
        """
        Initialize Docker service base.

        Args:
            provider: Infrastructure provider for node communication and management
            num_nodes: Number of nodes to manage
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.num_nodes = num_nodes
        self.node_offset = node_offset

    @abstractmethod
    def get_container_name(self) -> str:
        """Get the Docker container name. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def get_service_url(self) -> str:
        """Get the service URL for health checks. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def get_health_endpoint(self) -> str:
        """Get the health check endpoint path. Must be implemented by subclasses."""
        pass

    def get_container_stats(self) -> Optional[dict]:
        """
        Get Docker container resource usage statistics.

        Returns:
            Dictionary with CPU and memory usage stats, or None if unavailable
        """
        container_name = self.get_container_name()
        try:
            cmd = f"docker stats {container_name} --no-stream --format 'table {{{{.CPUPerc}}}},{{{{.MemUsage}}}},{{{{.MemPerc}}}}'"
            result = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
            assert isinstance(result, subprocess.CompletedProcess)
            if result.returncode == 0 and result.stdout.strip():
                # Parse the output
                lines = result.stdout.strip().split("\n")
                if len(lines) > 1:  # Skip header line
                    stats_line = lines[1]
                    cpu_perc, mem_usage, mem_perc = stats_line.split(",")
                    return {
                        "cpu_percent": cpu_perc.strip(),
                        "memory_usage": mem_usage.strip(),
                        "memory_percent": mem_perc.strip(),
                    }
        except Exception:
            pass
        return None

    def reset(self) -> None:
        """Reset service data by removing Docker volumes and data."""
        # Stop container first
        self.stop()

        # Remove any lingering data directories
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd="docker volume prune -f",
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

    def is_healthy(self) -> bool:
        """
        Check if Docker container is healthy.

        Returns:
            True if container is running and healthy
        """
        container_name = self.get_container_name()
        try:
            # Check if container is running
            cmd = f"docker ps --filter name={container_name} --format '{{{{.Status}}}}'"
            result = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
            assert isinstance(result, subprocess.CompletedProcess)
            return result.returncode == 0 and "Up" in result.stdout
        except Exception:
            return False

    def _force_cleanup_container(self) -> None:
        """Force cleanup of any existing container with the same name."""
        container_name = self.get_container_name()
        # Kill and remove container if it exists, ignore errors
        cleanup_cmd = f"docker kill {container_name} 2>/dev/null || true; docker rm {container_name} 2>/dev/null || true"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cleanup_cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

    def _wait_for_service_ready(self, max_retries: int = 30) -> None:
        """Wait for service to be ready to accept connections."""
        service_url = self.get_service_url()
        health_endpoint = self.get_health_endpoint()

        for i in range(max_retries):
            if self.is_healthy():
                # Additional check - try to access service health endpoint
                try:
                    cmd = f"curl -s {service_url}{health_endpoint}"
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
                        print(
                            f"Docker container {self.get_container_name()} ready after {i+1} attempts"
                        )
                        return
                except Exception:
                    pass

            print(
                f"Waiting for Docker container {self.get_container_name()} to be ready... ({i+1}/{max_retries})"
            )
            time.sleep(5)

        raise RuntimeError(
            f"Docker container {self.get_container_name()} failed to become ready"
        )
