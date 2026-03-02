"""
Docker-based VictoriaMetrics service management for vertical scalability testing.

Uses a 2-container architecture:
- vmsingle: Storage backend
- vmagent: Scraping and remote write agent
"""

import os
from typing import Optional
from jinja2 import Template

from .base import DockerServiceBase
from experiment_utils.providers.base import InfrastructureProvider
import constants
from constants import (
    PROMETHEUS_CONFIG_DIR,
    VMAGENT_SCRAPE_CONFIG_FILE,
    VMAGENT_REMOTE_WRITE_CONFIG_FILE,
    SKETCHDB_EXPERIMENT_NAME,
    BASELINE_EXPERIMENT_NAME,
)
import utils


class DockerVictoriaMetricsService(DockerServiceBase):
    """Docker-based VictoriaMetrics service with vmsingle + vmagent architecture."""

    def __init__(
        self, provider: InfrastructureProvider, num_nodes: int, node_offset: int
    ):
        """
        Initialize Docker VictoriaMetrics service.

        Args:
            provider: Infrastructure provider for node communication and management
            num_nodes: Number of nodes to manage
            node_offset: Starting node index offset
        """
        super().__init__(provider, num_nodes, node_offset)
        self.vmsingle_container_name = "victoriametrics-single"
        self.vmagent_container_name = "victoriametrics-agent"
        self.compose_file = None
        self.experiment_mode = None

    def get_container_name(self) -> str:
        """Get the Docker container name (returns vmsingle as primary)."""
        return self.vmsingle_container_name

    def get_service_url(self) -> str:
        """Get VictoriaMetrics URL for queries."""
        return "http://localhost:8428"

    def get_query_endpoint_port(self) -> int:
        """Get the query endpoint port for VictoriaMetrics."""
        return 8428

    def get_health_endpoint(self) -> str:
        """Get VictoriaMetrics health check endpoint."""
        return "/health"

    def start(
        self,
        experiment_output_dir: str,
        local_experiment_dir: str,
        experiment_mode: str,
        cpu_limit: Optional[float] = None,
        memory_limit: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
        Start VictoriaMetrics in Docker with vmsingle + vmagent architecture.

        Args:
            experiment_output_dir: Directory for data storage
            local_experiment_dir: Local experiment directory for file creation
            cpu_limit: Optional number of CPUs to allocate (e.g., 4.0)
            memory_limit: Optional memory limit (e.g., "8g")
            experiment_mode: Experiment mode (BASELINE_EXPERIMENT_NAME or SKETCHDB_EXPERIMENT_NAME)
            **kwargs: Additional configuration
        """
        # Stop and remove any existing containers first
        self._force_cleanup_containers()

        # Store experiment mode for remote write configuration
        self.experiment_mode = experiment_mode

        # Prepare directories
        vm_config_dir = os.path.join(experiment_output_dir, PROMETHEUS_CONFIG_DIR)
        vm_data_dir = os.path.join(experiment_output_dir, "victoriametrics_data")

        # Get current user ID and group ID for non-root container execution
        # NOTE: VictoriaMetrics Docker images run as root by default (no User directive),
        # while Prometheus images run as 'nobody' user by default. We need to explicitly
        # set the user for VM containers to avoid data files being owned by root.
        uid_result = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd="id -u",
            cmd_dir=None,
            nohup=False,
            popen=False,
        )
        gid_result = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd="id -g",
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        # Extract UID/GID from results
        import subprocess

        assert isinstance(
            uid_result, subprocess.CompletedProcess
        ), "Failed to get user ID"
        assert isinstance(
            gid_result, subprocess.CompletedProcess
        ), "Failed to get group ID"
        user_id = uid_result.stdout.strip()
        group_id = gid_result.stdout.strip()

        # Create directories on remote host with proper permissions and ownership
        # Create both the main vm_data_dir and vmagent-data subdirectory with correct ownership.
        # The chown is necessary because the directories might already exist from a previous run
        # with root ownership (before we added the user directive to docker-compose), and the
        # non-root container user won't have permission to write to root-owned directories.
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"mkdir -p {vm_config_dir} {vm_data_dir}/vmagent-data && chown -R {user_id}:{group_id} {vm_data_dir} && chmod 755 {vm_data_dir} && chmod 755 {vm_data_dir}/vmagent-data",
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        # Determine remote write URLs based on experiment mode
        node_ip = self.provider.get_node_ip(self.node_offset)
        if experiment_mode == BASELINE_EXPERIMENT_NAME:
            # Only write to vmsingle
            remote_write_urls = [f"http://{node_ip}:8428/api/v1/write"]
        elif experiment_mode == SKETCHDB_EXPERIMENT_NAME:
            # Write to both vmsingle AND queryengine
            remote_write_urls = [
                f"http://{node_ip}:8428/api/v1/write",
                f"http://{node_ip}:8080/receive",
            ]
        else:
            # Invalid experiment mode
            assert (
                False
            ), f"Invalid experiment_mode: {experiment_mode}. Must be '{BASELINE_EXPERIMENT_NAME}' or '{SKETCHDB_EXPERIMENT_NAME}'"

        # Convert resource limits to strings if specified
        if cpu_limit is not None:
            cpu_limit = str(cpu_limit)

        if memory_limit is not None:
            memory_limit = str(memory_limit)

        # Generate docker-compose file from template
        template_path = os.path.join(
            os.path.dirname(__file__), "docker-compose.victoriametrics.yml.j2"
        )
        with open(template_path, "r") as f:
            template = Template(f.read())

        compose_content = template.render(
            vmsingle_container_name=self.vmsingle_container_name,
            vmagent_container_name=self.vmagent_container_name,
            user_id=user_id,
            group_id=group_id,
            vm_config_dir=vm_config_dir,
            vm_data_dir=vm_data_dir,
            vmagent_scrape_config=VMAGENT_SCRAPE_CONFIG_FILE,
            vmagent_remote_write_config=VMAGENT_REMOTE_WRITE_CONFIG_FILE,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            node_ip=node_ip,
            remote_write_urls=remote_write_urls,
        )

        # Create compose file locally and rsync to remote
        local_compose_file = os.path.join(
            local_experiment_dir, "docker-compose.victoriametrics.yml"
        )
        os.makedirs(os.path.dirname(local_compose_file), exist_ok=True)
        with open(local_compose_file, "w") as f:
            f.write(compose_content)

        # Rsync to remote host
        remote_compose_file = os.path.join(
            experiment_output_dir, "docker-compose.victoriametrics.yml"
        )
        self.compose_file = remote_compose_file

        hostname = f"node{self.node_offset}.{self.provider.hostname_suffix}"
        rsync_cmd = 'rsync -azh -e "ssh {}" {} {}@{}:{}'.format(
            constants.SSH_OPTIONS,
            local_compose_file,
            self.provider.username,
            hostname,
            remote_compose_file,
        )
        utils.run_cmd_with_retry(rsync_cmd, popen=False, ignore_errors=False)

        # Start containers using docker-compose
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"docker compose -f {remote_compose_file} up -d",
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        # Wait for VictoriaMetrics to be ready
        self._wait_for_service_ready()

    def stop(self, **kwargs) -> None:
        """
        Stop and remove VictoriaMetrics Docker containers.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        if self.compose_file:
            # Stop using docker-compose
            self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=f"docker compose -f {self.compose_file} down",
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
            self.compose_file = None
        else:
            # Fallback: stop containers individually
            self._force_cleanup_containers()

    def reset(self, **kwargs) -> None:
        """
        Reset VictoriaMetrics by removing data.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        # Stop containers first
        self.stop(**kwargs)

        # Note: Data directory cleanup is handled by experiment teardown
        # which removes the entire experiment_output_dir

    def _force_cleanup_containers(self) -> None:
        """Force cleanup of both vmsingle and vmagent containers."""
        # Stop and remove vmsingle
        cmd = f"docker stop {self.vmsingle_container_name} && docker rm {self.vmsingle_container_name}"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

        # Stop and remove vmagent
        cmd = f"docker stop {self.vmagent_container_name} && docker rm {self.vmagent_container_name}"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

    def get_victoriametrics_url(self) -> str:
        """
        Get VictoriaMetrics URL for queries.

        Returns:
            VictoriaMetrics base URL
        """
        return self.get_service_url()
