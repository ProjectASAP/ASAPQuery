"""
Cluster data exporter service management for experiments.

This module provides services for managing the cluster_data_exporter, which replays
Google and Alibaba cluster trace data as Prometheus metrics.
"""

import os
import time
from typing import Dict, Any, Optional

from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider


class ClusterDataExporterService(BaseService):
    """
    Service for managing cluster_data_exporter via Docker.

    This service manages a Docker-based exporter that replays cluster trace data
    from Google (2011) or Alibaba (2021/2022) datasets as Prometheus metrics.
    """

    DOCKER_IMAGE = "sketchdb-cluster-data-exporter:latest"
    CONTAINER_BASE_NAME = "cluster-data-exporter"

    def __init__(
        self,
        provider: InfrastructureProvider,
        node_offset: int,
        data_directory: str,
    ):
        """
        Initialize cluster data exporter service.

        Args:
            provider: Infrastructure provider for node communication
            node_offset: Starting node index offset
            data_directory: Path to directory containing cluster trace data
        """
        super().__init__(provider)
        self.node_offset = node_offset
        self.data_directory = data_directory
        self.container_name: Optional[str] = None
        self.port: Optional[int] = None
        self.provider_type: Optional[str] = None

    def start(
        self,
        config: Dict[str, Any],
        experiment_output_dir: str,
        local_experiment_dir: str,
        **kwargs,
    ) -> None:
        """
        Start cluster_data_exporter in Docker container.

        Args:
            config: Cluster data exporter configuration containing:
                - provider: "google" or "alibaba"
                - port: Port number for metrics endpoint
                - Provider-specific options (metrics, data_type, data_year, etc.)
            experiment_output_dir: Directory for experiment output
            local_experiment_dir: Local experiment directory for config dumps
            **kwargs: Additional configuration

        Raises:
            AssertionError: If num_nodes != 1
            ValueError: If data directory validation fails
        """
        # Get number of nodes from provider (assuming it has this info)
        num_nodes = kwargs.get("num_nodes", 1)

        # Assert that we have exactly 2 nodes
        assert num_nodes == 1, (
            f"cluster_data_exporter requires exactly 1 node (num_nodes==1), "
            f"got {num_nodes}"
        )

        # Extract configuration
        provider = config["provider"]
        port = config.get("port", 40000)
        self.provider_type = provider
        self.port = port

        # Validate data directory
        self._validate_data_directory(provider, config)

        # Create container name
        self.container_name = f"{self.CONTAINER_BASE_NAME}-{provider}-{port}"

        # Create output directory for cluster_data_exporter (similar to query engine)
        output_dir = os.path.join(experiment_output_dir, "cluster_data_exporter")

        # Create output directory command with proper permissions
        # The Docker container runs as a non-root user, so we need to ensure
        # the output directory is writable
        mkdir_cmd = f"mkdir -p {output_dir} && chmod 777 {output_dir}"
        self.provider.execute_command(
            node_idx=self.node_offset + 1,
            cmd=mkdir_cmd,
            cmd_dir="",
            nohup=False,
            popen=False,
        )

        # Build docker run command
        docker_cmd = self._build_docker_command(
            config=config,
            port=port,
            output_dir=output_dir,
        )

        # Log configuration to file
        os.makedirs(
            os.path.join(local_experiment_dir, "cluster_data_exporter_config"),
            exist_ok=True,
        )
        with open(
            os.path.join(
                local_experiment_dir,
                "cluster_data_exporter_config",
                f"config_{provider}.txt",
            ),
            "w",
        ) as f:
            f.write(f"provider: {provider}\n")
            f.write(f"port: {port}\n")
            f.write(f"data_directory: {self.data_directory}\n")
            f.write(f"container_name: {self.container_name}\n")
            f.write(f"docker_cmd: {docker_cmd}\n")
            for key, value in config.items():
                if key not in ["provider", "port"]:
                    f.write(f"{key}: {value}\n")

        # Stop any existing container with the same name
        stop_cmd = f"docker stop {self.container_name} 2>/dev/null || true; docker rm {self.container_name} 2>/dev/null || true"
        self.provider.execute_command(
            node_idx=self.node_offset + 1,
            cmd=stop_cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        print(f"Starting cluster_data_exporter on node {self.node_offset + 1}")
        print(f"  Provider: {provider}")
        print(f"  Port: {port}")
        print(f"  Container: {self.container_name}")

        # Start the container
        self.provider.execute_command(
            node_idx=self.node_offset + 1,
            cmd=docker_cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        # Wait for service to be ready
        target_node = self.node_offset + 1
        node_ip = self.provider.get_node_ip(target_node)
        print(
            f"Waiting for cluster_data_exporter to be ready at http://{node_ip}:{port}/metrics"
        )
        self._wait_for_health(target_node, node_ip, port, timeout=60)
        print("cluster_data_exporter is ready!")

    def stop(self, **kwargs) -> None:
        """
        Stop cluster_data_exporter container.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        if self.container_name is None:
            # Try to stop any cluster-data-exporter containers
            cmd = (
                f"docker ps -a --filter name={self.CONTAINER_BASE_NAME} "
                f"--format '{{{{.Names}}}}' | xargs -r docker stop; "
                f"docker ps -a --filter name={self.CONTAINER_BASE_NAME} "
                f"--format '{{{{.Names}}}}' | xargs -r docker rm"
            )
        else:
            cmd = f"docker stop {self.container_name} 2>/dev/null || true; docker rm {self.container_name} 2>/dev/null || true"

        print(f"Stopping cluster_data_exporter on node {self.node_offset + 1}")
        self.provider.execute_command(
            node_idx=self.node_offset + 1,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

    def _build_docker_command(
        self,
        config: Dict[str, Any],
        port: int,
        output_dir: str,
    ) -> str:
        """
        Build docker run command based on provider configuration.

        Args:
            config: Exporter configuration
            port: Port number
            output_dir: Output directory for logs and other data

        Returns:
            Docker run command string
        """
        provider = config["provider"]

        # Base docker run command with common options
        cmd_parts = [
            "docker run -d",
            f"--name {self.container_name}",
            f"-p {port}:{port}",
            f"-v {self.data_directory}:/data:ro",  # Read-only mount
            f"-v {output_dir}:/output",  # Output directory for logs
        ]

        # Add resource limits if specified
        if "memory_limit" in config:
            cmd_parts.append(f"--memory {config['memory_limit']}")
        if "cpu_limit" in config:
            cmd_parts.append(f"--cpus {config['cpu_limit']}")

        # Add restart policy
        cmd_parts.append("--restart no")

        # Add image name
        cmd_parts.append(self.DOCKER_IMAGE)

        # Add application arguments
        cmd_parts.append("--input-directory /data")
        cmd_parts.append(f"--port {port}")

        # Add logging configuration
        log_level = config.get("log_level", "INFO")
        cmd_parts.append(f"--log-level {log_level}")
        cmd_parts.append("--log-dir /output")

        cmd_parts.append(provider)

        # Add provider-specific arguments
        if provider == "google":
            if "metrics" in config:
                cmd_parts.append(f"--metrics={config['metrics']}")

            parts_mode = config.get("parts_mode", "all-parts")
            if parts_mode == "all-parts":
                cmd_parts.append("--all-parts")
            elif parts_mode == "part-index":
                part_index = config.get("part_index", 0)
                cmd_parts.append(f"--part-index={part_index}")

        elif provider == "alibaba":
            if "data_type" in config:
                cmd_parts.append(f"--data-type={config['data_type']}")
            if "data_year" in config:
                cmd_parts.append(f"--data-year={config['data_year']}")

            parts_mode = config.get("parts_mode", "all-parts")
            if parts_mode == "all-parts":
                cmd_parts.append("--all-parts")
            elif parts_mode == "part-index":
                part_index = config.get("part_index", 0)
                cmd_parts.append(f"--part-index={part_index}")

            # Add speedup parameter (default: 1 for real-time)
            speedup = config.get("speedup", 1)
            cmd_parts.append(f"--speedup={speedup}")

        # Build final command (no redirection needed, logs go to /output inside container)
        cmd = " ".join(cmd_parts)

        return cmd

    def _validate_data_directory(
        self,
        provider: str,
        config: Dict[str, Any],
    ) -> None:
        """
        Validate that data directory exists on remote node and contains required files.

        Args:
            provider: Provider type ("google" or "alibaba")
            config: Exporter configuration

        Raises:
            ValueError: If validation fails
        """
        # Determine which node to check - the cluster data exporter runs on node_offset + 1
        target_node = self.node_offset + 1
        print(
            f"Validating data directory on node {target_node} (node_offset={self.node_offset})"
        )

        # Check if directory exists on remote node
        check_dir_cmd = f"test -d {self.data_directory}"
        result = self.provider.execute_command(
            node_idx=target_node,
            cmd=check_dir_cmd,
            cmd_dir="",
            nohup=False,
            popen=False,
        )

        # execute_command returns the result; if directory doesn't exist, test will fail
        # We need to check the exit code to determine if directory exists
        # For now, we'll try to list the directory to verify it exists
        list_dir_cmd = f"ls -la {self.data_directory} 2>&1"
        result = self.provider.execute_command(
            node_idx=target_node,
            cmd=list_dir_cmd,
            cmd_dir="",
            nohup=False,
            popen=False,
        )

        # Handle CompletedProcess object
        if result is not None:
            output = result.stdout if hasattr(result, "stdout") else str(result)
            if "No such file or directory" in output:
                raise ValueError(
                    f"Data directory does not exist on remote node {target_node}: {self.data_directory}\n"
                    f"Please ensure the cluster trace data is available at this location."
                )

        # Provider-specific validation
        if provider == "google":
            self._validate_google_data()
        elif provider == "alibaba":
            data_type = config.get("data_type")
            data_year = config.get("data_year")
            if not data_type or not data_year:
                raise ValueError(
                    "Alibaba provider requires 'data_type' and 'data_year' in config"
                )
            self._validate_alibaba_data(data_type, data_year)
        else:
            raise ValueError(f"Unknown provider: {provider}")

    def _validate_google_data(self) -> None:
        """
        Validate Google cluster trace data files exist on remote node.

        Raises:
            ValueError: If required files are missing
        """
        # Check for at least one part file on remote node
        target_node = self.node_offset + 1

        # Normalize path to avoid double slashes
        data_dir = self.data_directory.rstrip("/")
        count_cmd = f"ls {data_dir}/part-*-of-00500.csv.gz 2>/dev/null | wc -l"

        result = self.provider.execute_command(
            node_idx=target_node,
            cmd=count_cmd,
            cmd_dir="",
            nohup=False,
            popen=False,
        )

        try:
            # Handle CompletedProcess object
            if hasattr(result, "stdout"):
                output = result.stdout.strip()
            else:
                output = result.strip() if result else ""

            num_files = int(output) if output else 0
        except (ValueError, AttributeError):
            num_files = 0

        if num_files == 0:
            raise ValueError(
                f"No Google trace data files found in {self.data_directory} on remote node {target_node}\n"
                f"Expected files matching pattern: part-*-of-00500.csv.gz"
            )

        print(f"Found {num_files} Google trace data files on remote node {target_node}")

    def _validate_alibaba_data(
        self,
        data_type: str,
        data_year: int,
    ) -> None:
        """
        Validate Alibaba cluster trace data files exist on remote node.

        Args:
            data_type: Data type ("node" or "msresource")
            data_year: Data year (2021 or 2022)

        Raises:
            ValueError: If required files are missing
        """
        # Determine expected file pattern based on data type and year
        if data_type == "node":
            if data_year == 2021 or data_year == 2022:
                pattern = "Node_*.csv.gz"
            else:
                raise ValueError(
                    f"Invalid data_year for Alibaba node data: {data_year}"
                )
        elif data_type == "msresource":
            if data_year == 2021 or data_year == 2022:
                pattern = "MsResource_*.csv.gz"
            else:
                raise ValueError(
                    f"Invalid data_year for Alibaba msresource data: {data_year}"
                )
        else:
            raise ValueError(f"Invalid data_type for Alibaba: {data_type}")

        # Check for data files on remote node
        target_node = self.node_offset + 1

        # Normalize path to avoid double slashes
        data_dir = self.data_directory.rstrip("/")
        count_cmd = f"ls {data_dir}/{pattern} 2>/dev/null | wc -l"

        # Debug: List what's actually in the directory
        debug_cmd = f"ls -la {data_dir}/ 2>&1 | head -20"
        debug_result = self.provider.execute_command(
            node_idx=target_node,
            cmd=debug_cmd,
            cmd_dir="",
            nohup=False,
            popen=False,
        )
        print(f"Directory contents on node {target_node}:")
        print(debug_result)

        result = self.provider.execute_command(
            node_idx=target_node,
            cmd=count_cmd,
            cmd_dir="",
            nohup=False,
            popen=False,
        )

        print(f"Raw result from count command: '{result}'")
        print(f"Result type: {type(result)}")

        try:
            # Handle CompletedProcess object
            if hasattr(result, "stdout"):
                output = result.stdout.strip()
            else:
                output = result.strip() if result else ""

            num_files = int(output) if output else 0
        except (ValueError, AttributeError) as e:
            print(f"Error parsing result: {e}")
            num_files = 0

        if num_files == 0:
            raise ValueError(
                f"No Alibaba {data_type} trace data files found in {self.data_directory} on remote node {target_node}\n"
                f"Expected files matching pattern: {pattern}\n"
                f"Checked with command: {count_cmd}"
            )

        print(
            f"Found {num_files} Alibaba {data_type} {data_year} trace data files on remote node"
        )

    def _wait_for_health(
        self,
        node_idx: int,
        node_ip: str,
        port: int,
        timeout: int = 60,
    ) -> None:
        """
        Wait for the exporter to be ready by polling the metrics endpoint via SSH.

        Args:
            node_idx: Index of the node running the exporter
            node_ip: IP address of the node
            port: Port number
            timeout: Maximum time to wait in seconds

        Raises:
            RuntimeError: If service doesn't become ready within timeout
        """
        url = f"http://{node_ip}:{port}/metrics"
        start_time = time.time()
        last_error = None

        while time.time() - start_time < timeout:
            # Run curl from the remote node to check health
            check_cmd = f"curl -s -o /dev/null -w '%{{http_code}}' {url}"
            result = self.provider.execute_command(
                node_idx=node_idx,
                cmd=check_cmd,
                cmd_dir="",
                nohup=False,
                popen=False,
            )

            try:
                # Extract HTTP status code from result
                if hasattr(result, "stdout"):
                    http_code = result.stdout.strip()
                else:
                    http_code = str(result).strip()

                if http_code == "200":
                    return
                last_error = f"HTTP {http_code}"
            except Exception as e:
                last_error = str(e)

            time.sleep(2)

        raise RuntimeError(
            f"cluster_data_exporter did not become ready within {timeout} seconds. "
            f"Last error: {last_error}"
        )


class DataExporterFactory:
    """Factory for creating data exporter services."""

    @staticmethod
    def create_data_exporter_service(
        exporter_type: str,
        provider: InfrastructureProvider,
        node_offset: int,
        data_directory: str,
    ) -> BaseService:
        """
        Create a data exporter service based on type.

        Args:
            exporter_type: Type of data exporter ("cluster_data", etc.)
            provider: Infrastructure provider for node communication
            node_offset: Starting node index offset
            data_directory: Path to data directory

        Returns:
            Appropriate data exporter service instance

        Raises:
            ValueError: If exporter_type is not supported
        """
        if exporter_type == "cluster_data":
            return ClusterDataExporterService(
                provider=provider,
                node_offset=node_offset,
                data_directory=data_directory,
            )
        else:
            raise ValueError(
                f"Invalid data exporter type: {exporter_type}. "
                f"Supported types are: 'cluster_data'"
            )
