"""
ClickHouse Docker service management for SQL experiment infrastructure.
"""

import os
import subprocess
from typing import Optional
from jinja2 import Template

from .base import DockerServiceBase
from experiment_utils.providers.base import InfrastructureProvider
import constants
import utils


class ClickHouseService(DockerServiceBase):
    """Manages a ClickHouse Docker container on a remote CloudLab node."""

    CONTAINER_NAME = "clickhouse-server"
    DEFAULT_HTTP_PORT = 8123
    DEFAULT_NATIVE_PORT = 9000
    DEFAULT_IMAGE_TAG = "latest"
    DEFAULT_DATABASE = "default"

    def __init__(
        self, provider: InfrastructureProvider, num_nodes: int, node_offset: int
    ):
        super().__init__(provider, num_nodes, node_offset)
        self.compose_file: Optional[str] = None
        self._http_port = self.DEFAULT_HTTP_PORT

    def get_container_name(self) -> str:
        return self.CONTAINER_NAME

    def get_service_url(self) -> str:
        return f"http://localhost:{self._http_port}"

    def get_health_endpoint(self) -> str:
        # ClickHouse HTTP interface responds with "Ok." on /ping when ready
        return "/ping"

    def get_http_port(self) -> int:
        return self._http_port

    def is_healthy(self) -> bool:
        """ClickHouse is ready only when /ping returns exactly 'Ok.'"""
        result = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"curl -s http://localhost:{self._http_port}/ping",
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )
        if not isinstance(result, subprocess.CompletedProcess):
            return False
        return result.returncode == 0 and result.stdout.strip() == "Ok."

    def start(
        self,
        experiment_output_dir: str,
        local_experiment_dir: str,
        http_port: int = DEFAULT_HTTP_PORT,
        native_port: int = DEFAULT_NATIVE_PORT,
        database: str = DEFAULT_DATABASE,
        image_tag: str = DEFAULT_IMAGE_TAG,
        cpu_limit: Optional[float] = None,
        memory_limit: Optional[str] = None,
        **kwargs,
    ) -> None:
        """
        Render the Jinja2 compose template, rsync it to the node, and start ClickHouse.

        Args:
            experiment_output_dir: Remote directory for data and config storage
            local_experiment_dir: Local directory to write the rendered compose file
            http_port: ClickHouse HTTP interface port (default 8123)
            native_port: ClickHouse native TCP port (default 9000)
            database: Default database name
            image_tag: Docker image tag for clickhouse/clickhouse-server
            cpu_limit: Optional CPU limit (e.g. 4.0)
            memory_limit: Optional memory limit (e.g. "8g")
        """
        self._http_port = http_port
        self._force_cleanup_container()

        data_dir = os.path.join(experiment_output_dir, "clickhouse_data")
        log_dir = os.path.join(experiment_output_dir, "clickhouse_logs")

        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"mkdir -p {data_dir} {log_dir} && chmod 777 {data_dir} {log_dir}",
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        template_path = os.path.join(
            os.path.dirname(__file__), "docker-compose.clickhouse.yml.j2"
        )
        with open(template_path, "r") as f:
            template = Template(f.read())

        compose_content = template.render(
            container_name=self.CONTAINER_NAME,
            image_tag=image_tag,
            database=database,
            data_dir=data_dir,
            log_dir=log_dir,
            cpu_limit=str(cpu_limit) if cpu_limit is not None else None,
            memory_limit=memory_limit,
        )

        local_compose_file = os.path.join(
            local_experiment_dir, "docker-compose.clickhouse.yml"
        )
        os.makedirs(os.path.dirname(local_compose_file), exist_ok=True)
        with open(local_compose_file, "w") as f:
            f.write(compose_content)

        remote_compose_file = os.path.join(
            experiment_output_dir, "docker-compose.clickhouse.yml"
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

        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"docker compose -f {remote_compose_file} up -d",
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        self._wait_for_service_ready()

    def stop(self, **kwargs) -> None:
        """Stop and remove the ClickHouse container."""
        if self.compose_file:
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
            self._force_cleanup_container()
