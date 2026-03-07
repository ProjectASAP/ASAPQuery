"""
Prometheus Client Service for running experiments
"""

import os
import subprocess
from typing import Optional

import utils
import constants
from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider


class PrometheusClientService(BaseService):
    def __init__(
        self,
        provider: InfrastructureProvider,
        use_container: bool,
        node_offset: int,
    ):
        super().__init__(provider)
        self.use_container = use_container
        self.node_offset = node_offset
        self.container_name = "sketchdb-prometheusclient"
        self.latency_exporter_socket_addr = (
            f"{self.provider.get_node_ip(node_offset)}:9150"
        )
        self.compose_file = None

    def start(
        self,
        experiment_mode,
        config_file,
        query_engine_config_file,
        output_dir,
        output_file,
        export_cost_and_latency,
        profile_query_engine_pid: Optional[int],
        profile_prometheus_time: Optional[int],
        parallel: bool,
        **kwargs,
    ):
        if self.use_container:
            return self._start_containerized(
                experiment_mode,
                config_file,
                query_engine_config_file,
                output_dir,
                output_file,
                export_cost_and_latency,
                profile_query_engine_pid,
                profile_prometheus_time,
                parallel,
            )
        else:
            return self._start_bare_metal(
                experiment_mode,
                config_file,
                query_engine_config_file,
                output_dir,
                output_file,
                export_cost_and_latency,
                profile_query_engine_pid,
                profile_prometheus_time,
                parallel,
            )

    def _start_containerized(
        self,
        experiment_mode: str,
        config_file: str,
        query_engine_config_file: str,
        output_dir: str,
        output_file: str,
        export_cost_and_latency: bool,
        profile_query_engine_pid: Optional[int],
        profile_prometheus_time: Optional[int],
        parallel: bool,
    ):
        prometheus_client_dir = os.path.join(
            self.provider.get_home_dir(),
            "code",
            "asap-tools",
            "queriers",
            "prometheus-client",
        )
        template_path = os.path.join(prometheus_client_dir, "docker-compose.yml.j2")
        remote_compose_file = os.path.join(
            output_dir, "prometheus-client-docker-compose.yml"
        )
        self.compose_file = remote_compose_file
        helper_script = os.path.join(
            self.provider.get_home_dir(),
            "code",
            "asap-tools",
            "experiments",
            "generate_prometheus_client_compose.py",
        )

        gen_compose_cmd = f"python3 {helper_script}"
        gen_compose_cmd += f" --template-path {template_path}"
        gen_compose_cmd += f" --compose-output-path {remote_compose_file}"
        gen_compose_cmd += f" --prometheusclient-dir {prometheus_client_dir}"
        gen_compose_cmd += f" --container-name {self.container_name}"
        gen_compose_cmd += f" --experiment-output-dir {output_dir}"
        gen_compose_cmd += f" --config-file {config_file}"
        gen_compose_cmd += f" --client-output-dir {output_dir}"
        gen_compose_cmd += f" --client-output-file {output_file}"
        gen_compose_cmd += (
            f" --prometheus-host {self.provider.get_node_ip(self.node_offset)}"
        )
        gen_compose_cmd += (
            f" --sketchdb-host {self.provider.get_node_ip(self.node_offset)}"
        )
        if parallel:
            gen_compose_cmd += " --parallel"

        if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
            assert query_engine_config_file is not None
            gen_compose_cmd += f" --align-query-time --server-for-alignment sketchdb --query-engine-config-file {query_engine_config_file}"

        if export_cost_and_latency:
            gen_compose_cmd += (
                f" --latency-exporter-socket-addr {self.latency_exporter_socket_addr}"
            )

        if profile_query_engine_pid is not None:
            gen_compose_cmd += f" --profile-query-engine-pid {profile_query_engine_pid}"

        if profile_prometheus_time is not None:
            gen_compose_cmd += f" --profile-prometheus-time {profile_prometheus_time}"

        cmd = f"mkdir -p {output_dir}; {gen_compose_cmd}; docker compose -f {remote_compose_file} up --no-build -d"
        try:
            utils.run_cmd(f"cd {prometheus_client_dir}; {cmd}", popen=False)
        except Exception as e:
            print(f"Failed to start PrometheusClient container: {e}")
            raise
        return

    def _start_bare_metal(
        self,
        experiment_mode: str,
        config_file: str,
        query_engine_config_file: str,
        output_dir: str,
        output_file: str,
        export_cost_and_latency: bool,
        profile_query_engine_pid: Optional[int],
        profile_prometheus_time: Optional[int],
        parallel: bool,
    ):
        cmd = "python3 -u main_prometheus_client.py --config_file {} --output_dir {} --output_file {}{}".format(
            config_file, output_dir, output_file, " --parallel" if parallel else ""
        )

        if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
            assert query_engine_config_file is not None
            cmd += " --align_query_time --server_for_alignment sketchdb --query_engine_config_file {}".format(
                query_engine_config_file
            )

        # TODO Update handling of config yaml so port:ip isn't hardcoded and always
        #      matches the IP:PORT for scrape target in the generated prometheus config
        if export_cost_and_latency:
            cmd += f" --export_latencies_for_prometheus {self.provider.get_node_ip(self.node_offset)}:9150"

        if profile_query_engine_pid is not None:
            cmd += " --profile_query_engine_pid {}".format(profile_query_engine_pid)

        if profile_prometheus_time is not None:
            cmd += " --profile_prometheus_time {}".format(profile_prometheus_time)

        cmd_dir = os.path.join(
            self.provider.get_home_dir(),
            "code",
            "asap-tools",
            "queriers",
            "prometheus-client",
        )
        utils.run_cmd(f"cd {cmd_dir}; {cmd}", popen=False)

        return

    def stop(self, **kwargs) -> None:
        if self.use_container:
            return self._stop_containerized()
        else:
            return self._stop_bare_metal()

    def _stop_containerized(self):
        """Stop PrometheusClient using containerized deployment."""
        try:
            if self.compose_file:
                cmd = f"docker compose -f {self.compose_file} down"
                if self.provider.hostname_suffix == "localhost":
                    utils.run_cmd(cmd, popen=False)
                    self.compose_file = None
                else:
                    self.provider.execute_command(
                        node_idx=self.node_offset,
                        cmd=cmd,
                        cmd_dir=None,
                        nohup=False,
                        popen=False,
                        ignore_errors=True,
                    )
            else:
                # Fallback: stop by container name on remote node
                cmd = f"docker stop {self.container_name}; docker rm {self.container_name}"
                if self.provider.hostname_suffix == "localhost":
                    utils.run_cmd(cmd, popen=False)
                else:
                    self.provider.execute_command(
                        node_idx=self.node_offset,
                        cmd=cmd,
                        cmd_dir=None,
                        nohup=False,
                        popen=False,
                        ignore_errors=True,
                    )
        except Exception as e:
            print(f"Error stopping PrometheusClient container: {e}")
        return

    def _stop_bare_metal(self):
        """Kill Prometheus client processes."""
        cmd = "pkill -f main_prometheus_client.py"
        if self.provider.hostname_suffix == "localhost":
            # If running on localhost, use pkill to stop the process (e.g. from remote_monitor)
            utils.run_cmd(cmd, popen=False)
        else:
            self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
        return

    def is_healthy(self) -> bool:
        """
        Check if prometheus client is healthy by checking if process is running.

        Returns:
            True if prometheus client process is running
        """

        if self.use_container:
            return self._is_healthy_containerized()
        else:
            return self._is_healthy_bare_metal()

    def _is_healthy_bare_metal(self) -> bool:
        """Check if PrometheusClient is healthy using bare metal deployment."""
        try:
            cmd = "pgrep -f main_prometheus_client.py"
            result = utils.run_cmd(cmd, popen=False)
            import subprocess

            assert isinstance(result, subprocess.CompletedProcess)
            return result.stdout.strip() != ""
        except Exception:
            return False

    def _is_healthy_containerized(self) -> bool:
        """Check if PrometheusClient is healthy using containerized deployment."""
        try:
            # Check if container is running
            result = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", self.container_name],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip() == "true"
        except subprocess.CalledProcessError:
            return False
        except Exception:
            return False
