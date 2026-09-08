"""
Miscellaneous service classes for smaller services.
"""

import os
import random
from dataclasses import dataclass
from typing import Optional

from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider


@dataclass
class DiscoveryBackend:
    """Backend connection for label/column auto-discovery by asap-planner.

    PromQL mode:  DiscoveryBackend(type="prometheus", url=<url>, database=None)
    SQL mode:     DiscoveryBackend(type="clickhouse", url=<url>, database=<db>)
    """

    type: str
    url: str
    database: Optional[str]


class DeathstarService(BaseService):
    """Service for managing DeathStar benchmark."""

    def __init__(self, provider: InfrastructureProvider, args):
        """
        Initialize DeathStar service.

        Args:
            provider: Infrastructure provider for node communication and management
            args: Experiment args object providing get_node_range() and get_coordinator_node()
        """
        super().__init__(provider)
        self.args = args

    def start(self, **kwargs) -> None:
        """
        Start DeathStar benchmark across nodes.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        cmd = "docker compose up -d"
        cmd_dir = (
            f"{self.provider.get_home_dir()}/benchmarks/DeathStarBench/socialNetwork"
        )
        self.provider.execute_command_parallel(
            node_idxs=self.args.get_node_range(include_coordinator=False),
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=False,
            popen=True,
            redirect=True,
            wait=True,
        )

    def stop(self, **kwargs) -> None:
        """
        Stop DeathStar benchmark across nodes.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        cmd = "docker compose down"
        cmd_dir = (
            f"{self.provider.get_home_dir()}/benchmarks/DeathStarBench/socialNetwork"
        )
        self.provider.execute_command_parallel(
            node_idxs=self.args.get_node_range(include_coordinator=False),
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=False,
            popen=True,
            wait=True,
            ignore_errors=True,
        )

    def run_workload(
        self,
        experiment_output_dir: str,
        local_experiment_dir: str,
        minimum_experiment_running_time: int,
        random_params: bool = False,
    ) -> None:
        """
        Run DeathStar benchmark workload across nodes.

        Args:
            experiment_output_dir: Directory for experiment output
            local_experiment_dir: Local experiment directory for config dumps
            minimum_experiment_running_time: Minimum time to run experiment
            random_params: Whether to use random parameters
        """
        cmd_dir = (
            f"{self.provider.get_home_dir()}/benchmarks/DeathStarBench/socialNetwork"
        )

        TOTAL_CONNECTIONS = 480
        TOTAL_REQUESTS = 1200

        connections = TOTAL_CONNECTIONS // self.args.num_nodes
        requests = TOTAL_REQUESTS // self.args.num_nodes
        output_file_template = (
            "{}/deathstar_logs/connections_{}_requests_{}_nodes_{}_ip_{}.txt"
        )

        ips = []
        output_files = []
        for i in self.args.get_node_range(include_coordinator=False):
            ips.append(self.provider.get_node_ip(i))
            output_files.append(
                output_file_template.format(
                    experiment_output_dir,
                    TOTAL_CONNECTIONS,
                    TOTAL_REQUESTS,
                    self.args.num_nodes,
                    i,
                )
            )

        if not random_params:
            cmd_template = "../wrk2/wrk -D exp -t 12 -c {} -d {} -L -s ./wrk2/scripts/social-network/compose-post.lua http://{}:8080/wrk2-api/post/compose -R {} > {} 2>&1 &"
            cmds = [
                cmd_template.format(
                    connections,
                    minimum_experiment_running_time,
                    ip,
                    requests,
                    output_file,
                )
                for ip, output_file in zip(ips, output_files)
            ]
        else:
            cmd_template = "../wrk2/wrk -D exp -t {} -c {} -d {} -L -s ./wrk2/scripts/social-network/compose-post.lua http://{}:8080/wrk2-api/post/compose -R {} -s ./wrk2/scripts/social-network/random-params.lua > {} 2>&1 &"
            cmds = []
            for ip, output_file in zip(ips, output_files):
                random_threads = random.randint(1, 12)
                random_duration = random.randint(
                    minimum_experiment_running_time, minimum_experiment_running_time * 2
                )
                cmds.append(
                    cmd_template.format(
                        random_threads,
                        connections,
                        random_duration,
                        ip,
                        requests,
                        output_file,
                    )
                )

        # Dump workload configuration to a file
        os.makedirs(
            os.path.join(local_experiment_dir, "deathstar_config"), exist_ok=True
        )
        with open(
            os.path.join(local_experiment_dir, "deathstar_config", "cmds.sh"), "w"
        ) as f:
            f.write("\n".join(cmds))

        cmds.insert(0, "mkdir -p {};".format(os.path.dirname(output_files[0])))
        final_cmd = " ".join(cmds)
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=final_cmd,
            cmd_dir=cmd_dir,
            nohup=True,
            popen=False,
        )


class ControllerService(BaseService):
    """Service for managing the controller."""

    def __init__(
        self,
        provider: InfrastructureProvider,
        use_container: bool,
        node_offset: int,
    ):
        """
        Initialize Controller service.

        Args:
            provider: Infrastructure provider for node communication and management
            use_container: Whether to use containerized deployment
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.use_container = use_container
        self.node_offset = node_offset
        self.compose_file = None
        self.container_name = "sketchdb-controller"

    def start(
        self,
        controller_input_file: str,
        streaming_engine: str,
        controller_remote_output_dir: str,
        punting: bool,
        discovery_backend: Optional[DiscoveryBackend],
        data_ingestion_interval_ms: int,
        query_language: str = "promql",
        **kwargs,
    ) -> None:
        """
        Start the controller (asap-planner).

        Args:
            controller_input_file: Path to controller input configuration
            streaming_engine: Type of streaming engine
            controller_remote_output_dir: Controller output directory
            punting: Enable query punting based on performance heuristics
            discovery_backend: Optional backend used for label/column auto-discovery.
                When omitted, the planner uses schema hints from its input config.
                PromQL mode: DiscoveryBackend(type="prometheus", url=<url>, database=None)
                SQL mode:    DiscoveryBackend(type="clickhouse", url=<url>, database=<db>)
            data_ingestion_interval_ms: Data ingestion interval in milliseconds (required for all modes)
            query_language: 'promql' (default) or 'sql'
            **kwargs: Additional configuration
        """
        if self.use_container:
            return self._start_containerized(
                controller_input_file,
                streaming_engine,
                controller_remote_output_dir,
                punting,
                discovery_backend,
                data_ingestion_interval_ms,
                query_language,
            )
        else:
            return self._start_bare_metal(
                controller_input_file,
                streaming_engine,
                controller_remote_output_dir,
                punting,
                discovery_backend,
                data_ingestion_interval_ms,
                query_language,
            )

    @staticmethod
    def _discovery_args(discovery_backend: Optional[DiscoveryBackend]) -> str:
        """Return planner CLI arguments for an optional discovery backend."""
        if discovery_backend is None:
            return ""
        if discovery_backend.type == "prometheus":
            return f" --prometheus-url {discovery_backend.url}"
        if discovery_backend.type == "clickhouse":
            args = f" --clickhouse-url {discovery_backend.url}"
            if discovery_backend.database:
                args += f" --clickhouse-database {discovery_backend.database}"
            return args
        raise ValueError(f"Unsupported discovery backend: {discovery_backend.type}")

    def _start_bare_metal(
        self,
        controller_input_file: str,
        streaming_engine: str,
        controller_remote_output_dir: str,
        punting: bool,
        discovery_backend: Optional[DiscoveryBackend],
        data_ingestion_interval_ms: int,
        query_language: str,
    ) -> None:
        controller_log = os.path.join(controller_remote_output_dir, "controller.log")
        # Force UTC so naive (no Z/offset) datetime-string time literals in SQL
        # queries parse identically here (parse_datetime, sqlpattern_parser.rs)
        # and in ClickHouse (whose container has no TZ override, so it defaults
        # to UTC) -- otherwise the two would silently disagree by the shell's
        # local UTC offset.
        cmd = (
            f"TZ=UTC ../target/release/asap-planner"
            f" --input_config {controller_input_file}"
            f" --output_dir {controller_remote_output_dir}"
            f" --streaming_engine {streaming_engine}"
            f" --query-language {query_language}"
        )
        cmd += f" --data-ingestion-interval-ms {data_ingestion_interval_ms}"
        cmd += self._discovery_args(discovery_backend)
        if punting:
            cmd += " --enable-punting"
        cmd += " -v"
        cmd += f" > {controller_log} 2>&1"
        cmd_dir = os.path.join(self.provider.get_home_dir(), "code", "asap-planner-rs")
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=False,
            popen=False,
            ignore_errors=False,
        )

    def _start_containerized(
        self,
        controller_input_file: str,
        streaming_engine: str,
        controller_remote_output_dir: str,
        punting: bool,
        discovery_backend: Optional[DiscoveryBackend],
        data_ingestion_interval_ms: int,
        query_language: str,
    ):
        controller_dir = os.path.join(
            self.provider.get_home_dir(), "code", "asap-planner-rs"
        )

        template_path = os.path.join(controller_dir, "docker-compose.yml.j2")
        remote_compose_file = os.path.join(
            controller_remote_output_dir, "controller-docker-compose.yml"
        )
        helper_script = os.path.join(
            self.provider.get_home_dir(),
            "code",
            "asap-tools",
            "experiments",
            "generate_controller_compose.py",
        )
        self.compose_file = remote_compose_file

        generate_cmd = f"python3 {helper_script}"
        generate_cmd += f" --template-path {template_path}"
        generate_cmd += f" --compose-output-path {remote_compose_file}"
        generate_cmd += f" --controller-dir {controller_dir}"
        generate_cmd += f" --container-name {self.container_name}"
        generate_cmd += f" --input-config-path {controller_input_file}"
        generate_cmd += f" --controller-output-dir {controller_remote_output_dir}"
        generate_cmd += f" --streaming-engine {streaming_engine}"
        generate_cmd += f" --query-language {query_language}"
        generate_cmd += f" --data-ingestion-interval-ms {data_ingestion_interval_ms}"
        generate_cmd += self._discovery_args(discovery_backend)
        if punting:
            generate_cmd += " --punting"

        controller_log = os.path.join(controller_remote_output_dir, "controller.log")
        cmd = (
            f"mkdir -p {controller_remote_output_dir}; {generate_cmd}; "
            f"docker compose -f {remote_compose_file} up --no-build > {controller_log} 2>&1"
        )
        try:
            self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=controller_dir,
                nohup=False,
                popen=False,
                ignore_errors=False,
            )
        except Exception as e:
            print(f"Failed to start Controller container: {e}")
            print(f"Check controller logs at: {controller_log}")
            raise

        return None

    def stop(self, **kwargs) -> None:
        """
        Stop the controller.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        if self.use_container:
            return self._stop_containerized()
        else:
            return self._stop_bare_metal()

    def _stop_containerized(self) -> None:
        """Stop Controller using containerized deployment."""
        try:
            if self.compose_file:
                # Stop using docker compose command on remote node
                cmd = f"docker compose -f {self.compose_file} down"
                self.provider.execute_command(
                    node_idx=self.node_offset,
                    cmd=cmd,
                    cmd_dir=None,
                    nohup=False,
                    popen=False,
                    ignore_errors=True,
                )
                self.compose_file = None
            else:
                # Fallback: stop by container name on remote node
                cmd = f"docker stop {self.container_name}; docker rm {self.container_name}"
                self.provider.execute_command(
                    node_idx=self.node_offset,
                    cmd=cmd,
                    cmd_dir=None,
                    nohup=False,
                    popen=False,
                    ignore_errors=True,
                )
        except Exception as e:
            print(f"Error stopping QueryEngine container: {e}")

    def _stop_bare_metal(self) -> None:
        # Controller typically runs to completion, no explicit stop needed for bare metal
        pass
