"""
Miscellaneous service classes for smaller services.
"""

import os
import random
import subprocess

import constants
from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider


class DeathstarService(BaseService):
    """Service for managing DeathStar benchmark."""

    def __init__(
        self, provider: InfrastructureProvider, num_nodes: int, node_offset: int
    ):
        """
        Initialize DeathStar service.

        Args:
            provider: Infrastructure provider for node communication and management
            num_nodes: Number of nodes to run DeathStar on
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.num_nodes = num_nodes
        self.node_offset = node_offset

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
            node_idxs=list(
                range(self.node_offset + 1, self.node_offset + self.num_nodes + 1)
            ),
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
            node_idxs=list(
                range(self.node_offset + 1, self.node_offset + self.num_nodes + 1)
            ),
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=False,
            popen=True,
            wait=True,
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

        connections = TOTAL_CONNECTIONS // self.num_nodes
        requests = TOTAL_REQUESTS // self.num_nodes
        output_file_template = (
            "{}/deathstar_logs/connections_{}_requests_{}_nodes_{}_ip_{}.txt"
        )

        ips = []
        output_files = []
        for i in range(self.node_offset + 1, self.node_offset + self.num_nodes + 1):
            ips.append(self.provider.get_node_ip(i))
            output_files.append(
                output_file_template.format(
                    experiment_output_dir,
                    TOTAL_CONNECTIONS,
                    TOTAL_REQUESTS,
                    self.num_nodes,
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
        prometheus_scrape_interval: int,
        streaming_engine: str,
        controller_remote_output_dir: str,
        punting: bool,
        **kwargs,
    ) -> None:
        """
        Start the controller.

        Args:
            controller_input_file: Path to controller input configuration
            prometheus_scrape_interval: Prometheus scraping interval
            streaming_engine: Type of streaming engine
            controller_remote_output_dir: Controller output directory
            punting: Enable query punting based on performance heuristics
            **kwargs: Additional configuration
        """
        if self.use_container:
            return self._start_containerized(
                controller_input_file,
                prometheus_scrape_interval,
                streaming_engine,
                controller_remote_output_dir,
                punting,
            )
        else:
            return self._start_bare_metal(
                controller_input_file,
                prometheus_scrape_interval,
                streaming_engine,
                controller_remote_output_dir,
                punting,
            )

    def _start_bare_metal(
        self,
        controller_input_file: str,
        prometheus_scrape_interval: int,
        streaming_engine: str,
        controller_remote_output_dir: str,
        punting: bool,
    ) -> None:
        cmd = "python3 main_controller.py --input_config {} --prometheus_scrape_interval {} --output_dir {} --streaming_engine {}".format(
            controller_input_file,
            prometheus_scrape_interval,
            controller_remote_output_dir,
            streaming_engine,
        )
        if punting:
            cmd += " --enable-punting"
        cmd_dir = os.path.join(self.provider.get_home_dir(), "code", "asap-planner")
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
        prometheus_scrape_interval: int,
        streaming_engine: str,
        controller_remote_output_dir: str,
        punting: bool,
    ):
        controller_dir = os.path.join(
            self.provider.get_home_dir(), "code", "asap-planner"
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
        generate_cmd += f" --prometheus-scrape-interval {prometheus_scrape_interval}"
        generate_cmd += f" --streaming-engine {streaming_engine}"
        if punting:
            generate_cmd += " --punting"

        cmd = f"mkdir -p {controller_remote_output_dir}; {generate_cmd}; docker compose -f {remote_compose_file} up --no-build -d"
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


class DumbKafkaConsumerService(BaseService):
    """Service for managing simple Kafka consumer."""

    def __init__(self, provider: InfrastructureProvider, node_offset: int):
        """
        Initialize Dumb Kafka Consumer service.

        Args:
            provider: Infrastructure provider for node communication and management
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.node_offset = node_offset

    def start(self, experiment_output_dir: str, **kwargs) -> None:
        """
        Start the dumb Kafka consumer.

        Args:
            experiment_output_dir: Directory for experiment output
            **kwargs: Additional configuration
        """
        cmd = "python3 -u dumb_kafka_consumer.py --kafka_topic {} --output_file {} > /dev/null 2>&1 &".format(
            constants.FLINK_OUTPUT_TOPIC,
            os.path.join(experiment_output_dir, "dumb_kafka_consumer_output.json"),
        )
        cmd_dir = os.path.join(
            self.provider.get_home_dir(), "code", "asap-tools", "experiments"
        )
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=True,
            popen=False,
            ignore_errors=False,
        )

    def stop(self, **kwargs) -> None:
        """
        Stop the dumb Kafka consumer.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        cmd = "pkill -f dumb_kafka_consumer.py"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

    def is_healthy(self) -> bool:
        """
        Check if consumer is healthy.

        Returns:
            True if consumer process is running
        """
        try:
            cmd = "pgrep -f dumb_kafka_consumer.py"
            result = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
            assert isinstance(result, subprocess.CompletedProcess)
            return result.stdout.strip() != ""
        except Exception:
            return False
