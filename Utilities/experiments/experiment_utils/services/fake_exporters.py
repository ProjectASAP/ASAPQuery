"""
Exporter service management for experiments.
"""

import os
from abc import abstractmethod
from typing import Tuple, List, Dict, Any

import constants
from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider


class BaseExporterService(BaseService):
    """Base class for exporter services."""

    FAKE_EXPORTER_BASE_CONTAINER_NAME = "sketchdb-fake-exporter"
    FAKE_EXPORTER_BASE_COMPOSE_FILENAME_PREFIX = "fake-exporter-compose"

    def __init__(
        self,
        provider: InfrastructureProvider,
        num_nodes: int,
        use_container: bool,
        node_offset: int,
    ):
        """
        Initialize base exporter service.

        Args:
            provider: Infrastructure provider for node communication and management
            num_nodes: Number of nodes to run exporters on
            use_container: Whether to use containerized deployment
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.num_nodes: int = num_nodes
        self.use_container: bool = use_container
        self.node_offset: int = node_offset
        self.container_names: List[str] = []
        self.compose_files: List[str] = []

    @abstractmethod
    def start(
        self,
        config: Dict[str, Any],
        experiment_output_dir: str,
        local_experiment_dir: str,
        **kwargs,
    ) -> None:
        """Start exporters with given configuration."""
        pass

    @abstractmethod
    def stop(self, **kwargs) -> None:
        """Stop all exporter processes."""
        pass

    @staticmethod
    def get_compose_and_container_names(port, language: str) -> Tuple[str, str]:
        """
        Returns a tuple with the fake exporter's compose file name and container name
        based on the port it will run on, with the compose file name as the 0th element
        and the container name as the 1st element
        """
        compose_name = f"{BaseExporterService.FAKE_EXPORTER_BASE_COMPOSE_FILENAME_PREFIX}-{port}-{language}.yml"
        container_name = (
            f"{BaseExporterService.FAKE_EXPORTER_BASE_CONTAINER_NAME}-{port}-{language}"
        )
        return (compose_name, container_name)


class PythonExporterService(BaseExporterService):
    """Service for managing Python fake exporters."""

    def start(
        self,
        config: Dict[str, Any],
        experiment_output_dir: str,
        local_experiment_dir: str,
        **kwargs,
    ) -> None:
        """
        Start Python fake exporters.

        Args:
            config: Exporter configuration
            experiment_output_dir: Directory for experiment output
            local_experiment_dir: Local experiment directory for config dumps
            **kwargs: Additional configuration
        """
        if self.use_container:
            self._start_containerized(
                config,
                experiment_output_dir,
                local_experiment_dir,
            )
        else:
            self._start_bare_metal(
                config,
                experiment_output_dir,
                local_experiment_dir,
            )

    def _start_bare_metal(
        self,
        config: Dict[str, Any],
        experiment_output_dir: str,
        local_experiment_dir: str,
        **kwargs,
    ) -> None:
        """
        Start Python fake exporters.

        Args:
            config: Exporter configuration
            experiment_output_dir: Directory for experiment output
            local_experiment_dir: Local experiment directory for config dumps
            **kwargs: Additional configuration
        """
        output_dir = os.path.join(experiment_output_dir, "fake_exporter_output")
        num_ports = config["num_ports_per_server"]
        dataset = config["dataset"]

        cmds = []
        for port in range(num_ports):
            cmd = "python3 fake_exporter.py --output_dir {} --port {} --valuescale {} --dataset {} --num_labels {} --num_values_per_label {} --metric_type {}".format(
                output_dir,
                port + config["start_port"],
                config["synthetic_data_value_scale"],
                dataset,
                config["num_labels"],
                config["num_values_per_label"],
                config["metric_type"],
            )
            cmds.append(cmd)

        cmd_dir = os.path.join(
            constants.CLOUDLAB_HOME_DIR,
            "code",
            "PrometheusExporters",
            "fake_exporter",
            "fake_exporter_python",
        )

        # Dump workload configuration to a file
        os.makedirs(
            os.path.join(local_experiment_dir, "fake_exporter_config"), exist_ok=True
        )
        with open(
            os.path.join(local_experiment_dir, "fake_exporter_config", "cmds.sh"), "w"
        ) as f:
            f.write("\n".join(cmds))

        # Run commands in parallel across nodes
        for cmd in cmds:
            self.provider.execute_command_parallel(
                node_idxs=list(
                    range(self.node_offset + 1, self.node_offset + self.num_nodes + 1)
                ),
                cmd=cmd,
                cmd_dir=cmd_dir,
                nohup=False,
                popen=True,
                redirect=True,
                wait=False,
            )

    def _start_containerized(
        self,
        config: Dict[str, Any],
        experiment_output_dir: str,
        local_experiment_dir: str,
        **kwargs,
    ) -> None:
        output_dir = os.path.join(experiment_output_dir, "fake_exporter_output")
        num_ports = config["num_ports_per_server"]
        dataset = config["dataset"]

        # Build docker run commands for each port
        docker_run_cmds: List[str] = []
        container_names: List[str] = []

        for port in range(num_ports):
            actual_port = port + config["start_port"]
            container_name = f"{BaseExporterService.FAKE_EXPORTER_BASE_CONTAINER_NAME}-{actual_port}-python"

            # Build docker run command with volume mounts for Python exporter
            docker_cmd = (
                f"docker run -d "
                f"--name {container_name} "
                f"-p {actual_port}:{actual_port} "
                f"-v {experiment_output_dir}:/app/output "
                f"-v {output_dir}:/app/exporter_output_dir "
                f"--restart unless-stopped "
                f"sketchdb-fake-exporter-python:latest "
                f"--output_dir /app/exporter_output_dir "
                f"--port {actual_port} "
                f"--valuescale {config['synthetic_data_value_scale']} "
                f"--dataset {dataset} "
                f"--num_labels {config['num_labels']} "
                f"--num_values_per_label {config['num_values_per_label']} "
                f"--metric_type {config['metric_type']}"
            )

            container_names.append(container_name)
            docker_run_cmds.append(docker_cmd)

        self.container_names = container_names

        # Dump commands to a file for reference
        os.makedirs(
            os.path.join(local_experiment_dir, "fake_exporter_config"), exist_ok=True
        )
        with open(
            os.path.join(
                local_experiment_dir, "fake_exporter_config", "docker_run_cmds.sh"
            ),
            "w",
        ) as f:
            f.write("\n".join(docker_run_cmds))

        # Create output directory first
        mkdir_cmd = f"mkdir -p {output_dir}"
        self.provider.execute_command_parallel(
            node_idxs=list(
                range(self.node_offset + 1, self.node_offset + self.num_nodes + 1)
            ),
            cmd=mkdir_cmd,
            cmd_dir="",
            nohup=False,
            popen=True,
            redirect=True,
            wait=True,
        )

        # Start containers in batches to avoid overwhelming Docker daemon
        BATCH_SIZE = 5
        for i in range(0, len(docker_run_cmds), BATCH_SIZE):
            batch = docker_run_cmds[i : i + BATCH_SIZE]
            # Combine docker run commands in batch into single SSH command
            batch_cmd = "; ".join(batch)

            self.provider.execute_command_parallel(
                node_idxs=list(
                    range(self.node_offset + 1, self.node_offset + self.num_nodes + 1)
                ),
                cmd=batch_cmd,
                cmd_dir="",
                nohup=False,
                popen=True,
                redirect=True,
                wait=True,  # Wait for batch to complete
            )

        return

    def stop(self, **kwargs) -> None:
        """
        Stop Python fake exporters across nodes.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        if self.use_container:
            self._stop_containerized()
        else:
            self._stop_bare_metal()

    def _stop_bare_metal(self, **kwargs) -> None:
        """
        Stop Python fake exporters across nodes.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        cmd = "pkill -f fake_exporter.py"
        self.provider.execute_command_parallel(
            node_idxs=list(
                range(self.node_offset + 1, self.node_offset + self.num_nodes + 1)
            ),
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=True,
            wait=True,
        )

    def _stop_containerized(self, **kwargs) -> None:
        """Stop fake exporters using containerized deployment."""
        try:
            if self.container_names is not None and len(self.container_names) > 0:
                # Stop and remove containers by name
                # Batch container names to avoid command line length issues
                BATCH_SIZE = 10
                for i in range(0, len(self.container_names), BATCH_SIZE):
                    batch = self.container_names[i : i + BATCH_SIZE]
                    container_list = " ".join(batch)
                    cmd = f"docker stop {container_list} 2>/dev/null || true; docker rm {container_list} 2>/dev/null || true"

                    self.provider.execute_command_parallel(
                        node_idxs=list(
                            range(
                                self.node_offset + 1,
                                self.node_offset + self.num_nodes + 1,
                            )
                        ),
                        cmd=cmd,
                        cmd_dir=None,
                        nohup=False,
                        popen=True,
                        wait=True,
                    )
            else:
                # Fallback: stop all containers matching the base name pattern
                cmd = f"docker ps -a --filter name={BaseExporterService.FAKE_EXPORTER_BASE_CONTAINER_NAME} --format '{{{{.Names}}}}' | xargs -r docker stop; docker ps -a --filter name={BaseExporterService.FAKE_EXPORTER_BASE_CONTAINER_NAME} --format '{{{{.Names}}}}' | xargs -r docker rm"
                self.provider.execute_command_parallel(
                    node_idxs=list(
                        range(
                            self.node_offset + 1,
                            self.node_offset + self.num_nodes + 1,
                        )
                    ),
                    cmd=cmd,
                    cmd_dir=None,
                    nohup=False,
                    popen=True,
                    wait=True,
                )
        except Exception as e:
            print(f"Error stopping fake exporter containers: {e}")


class RustExporterService(BaseExporterService):
    """Service for managing Rust fake exporters."""

    def start(
        self,
        config: Dict[str, Any],
        experiment_output_dir: str,
        local_experiment_dir: str,
        **kwargs,
    ) -> None:
        """
        Start Rust fake exporters.

        Args:
            config: Exporter configuration
            experiment_output_dir: Directory for experiment output
            local_experiment_dir: Local experiment directory for config dumps
            **kwargs: Additional configuration
        """
        if self.use_container:
            return self._start_containerized(
                config,
                experiment_output_dir,
                local_experiment_dir,
            )
        else:
            return self._start_bare_metal(
                config,
                experiment_output_dir,
                local_experiment_dir,
            )

    def _start_bare_metal(
        self,
        config: Dict[str, Any],
        experiment_output_dir: str,
        local_experiment_dir: str,
        **kwargs,
    ) -> None:
        """
        Start Rust fake exporters.

        Args:
            config: Exporter configuration
            experiment_output_dir: Directory for experiment output
            local_experiment_dir: Local experiment directory for config dumps
            **kwargs: Additional configuration
        """
        num_ports = config["num_ports_per_server"]
        dataset = config["dataset"]

        cmds = []
        for port in range(num_ports):
            cmd = "./target/release/fake_exporter --port {} --valuescale {} --dataset {} --num-labels {} --num-values-per-label {} --metric-type {}".format(
                port + config["start_port"],
                config["synthetic_data_value_scale"],
                dataset,
                config["num_labels"],
                config["num_values_per_label"],
                config["metric_type"],
            )
            cmds.append(cmd)

        cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/code/PrometheusExporters/fake_exporter/fake_exporter_rust/fake_exporter"

        # Dump workload configuration to a file
        os.makedirs(
            os.path.join(local_experiment_dir, "fake_exporter_config"), exist_ok=True
        )
        with open(
            os.path.join(local_experiment_dir, "fake_exporter_config", "cmds.sh"), "w"
        ) as f:
            f.write("\n".join(cmds))

        # Run commands in parallel across nodes
        for cmd in cmds:
            self.provider.execute_command_parallel(
                node_idxs=list(
                    range(self.node_offset + 1, self.node_offset + self.num_nodes + 1)
                ),
                cmd=cmd,
                cmd_dir=cmd_dir,
                nohup=False,
                popen=True,
                redirect=True,
                wait=False,
            )

        return

    def _start_containerized(
        self,
        config: Dict[str, Any],
        experiment_output_dir: str,
        local_experiment_dir: str,
        **kwargs,
    ) -> None:
        num_ports = config["num_ports_per_server"]
        dataset = config["dataset"]

        # Build docker run commands for each port
        docker_run_cmds: List[str] = []
        container_names: List[str] = []

        for port in range(num_ports):
            actual_port = port + config["start_port"]
            container_name = f"{BaseExporterService.FAKE_EXPORTER_BASE_CONTAINER_NAME}-{actual_port}-rust"

            # Build docker run command
            docker_cmd = (
                f"docker run -d "
                f"--name {container_name} "
                f"-p {actual_port}:{actual_port} "
                f"--restart unless-stopped "
                f"sketchdb-fake-exporter-rust:latest "
                f"--port {actual_port} "
                f"--valuescale {config['synthetic_data_value_scale']} "
                f"--dataset {dataset} "
                f"--num-labels {config['num_labels']} "
                f"--num-values-per-label {config['num_values_per_label']} "
                f"--metric-type {config['metric_type']}"
            )

            container_names.append(container_name)
            docker_run_cmds.append(docker_cmd)

        self.container_names = container_names

        # Dump commands to a file for reference
        os.makedirs(
            os.path.join(local_experiment_dir, "fake_exporter_config"), exist_ok=True
        )
        with open(
            os.path.join(
                local_experiment_dir, "fake_exporter_config", "docker_run_cmds.sh"
            ),
            "w",
        ) as f:
            f.write("\n".join(docker_run_cmds))

        # Start containers in batches to avoid overwhelming Docker daemon
        BATCH_SIZE = 5
        for i in range(0, len(docker_run_cmds), BATCH_SIZE):
            batch = docker_run_cmds[i : i + BATCH_SIZE]
            # Combine docker run commands in batch into single SSH command
            batch_cmd = "; ".join(batch)

            self.provider.execute_command_parallel(
                node_idxs=list(
                    range(self.node_offset + 1, self.node_offset + self.num_nodes + 1)
                ),
                cmd=batch_cmd,
                cmd_dir="",
                nohup=False,
                popen=True,
                redirect=True,
                wait=True,  # Wait for batch to complete
            )

        return

    def stop(self, **kwargs) -> None:
        """
        Stop Rust fake exporters across nodes.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        if self.use_container:
            return self._stop_containerized()
        else:
            return self._stop_bare_metal()

    def _stop_bare_metal(self, **kwargs) -> None:
        """
        Stop Python fake exporters across nodes.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        cmd = "pkill -f fake_exporter"
        self.provider.execute_command_parallel(
            node_idxs=list(
                range(self.node_offset + 1, self.node_offset + self.num_nodes + 1)
            ),
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=True,
            wait=True,
        )

    def _stop_containerized(self, **kwargs) -> None:
        """Stop fake exporters using containerized deployment."""
        try:
            if self.container_names is not None and len(self.container_names) > 0:
                # Stop and remove containers by name
                # Batch container names to avoid command line length issues
                BATCH_SIZE = 10
                for i in range(0, len(self.container_names), BATCH_SIZE):
                    batch = self.container_names[i : i + BATCH_SIZE]
                    container_list = " ".join(batch)
                    cmd = f"docker stop {container_list} 2>/dev/null || true; docker rm {container_list} 2>/dev/null || true"

                    self.provider.execute_command_parallel(
                        node_idxs=list(
                            range(
                                self.node_offset + 1,
                                self.node_offset + self.num_nodes + 1,
                            )
                        ),
                        cmd=cmd,
                        cmd_dir=None,
                        nohup=False,
                        popen=True,
                        wait=True,
                    )
            else:
                # Fallback: stop all containers matching the base name pattern
                cmd = f"docker ps -a --filter name={BaseExporterService.FAKE_EXPORTER_BASE_CONTAINER_NAME} --format '{{{{.Names}}}}' | xargs -r docker stop; docker ps -a --filter name={BaseExporterService.FAKE_EXPORTER_BASE_CONTAINER_NAME} --format '{{{{.Names}}}}' | xargs -r docker rm"
                self.provider.execute_command_parallel(
                    node_idxs=list(
                        range(
                            self.node_offset + 1,
                            self.node_offset + self.num_nodes + 1,
                        )
                    ),
                    cmd=cmd,
                    cmd_dir=None,
                    nohup=False,
                    popen=True,
                    wait=True,
                )
        except Exception as e:
            print(f"Error stopping fake exporter containers: {e}")


class AvalancheExporterService(BaseExporterService):
    """Service for managing Avalanche exporters via Docker."""

    def start(
        self,
        config: Dict[str, Any],
        experiment_output_dir: str,
        local_experiment_dir: str,
        **kwargs,
    ) -> None:
        """
        Start Avalanche exporter in Docker container.

        Args:
            config: Avalanche exporter configuration
            experiment_output_dir: Directory for experiment output
            local_experiment_dir: Local experiment directory for config dumps
            **kwargs: Additional configuration
        """
        # Default avalanche configuration
        cardinality = config.get("cardinality", 10000)
        ingestion_rate = config.get("ingestion_rate", 1000000)
        port = config.get("port", 9001)
        container_name = f"avalanche-exporter-{port}"

        # Stop any existing container
        # self._stop_avalanche_container(container_name)

        # Docker command for avalanche
        # Avalanche generates high-cardinality metrics for load testing
        docker_cmd = (
            f"docker run -d --name {container_name} "
            f"-p {port}:9001 "
            f"quay.io/freshtracks.io/avalanche:latest "
            f"--metric-count=1 "
            f"--series-count={cardinality} "
            f"--metricname-length=5 "
            f"--labelname-length=5 "
            f"--const-label=environment=test "
            f"--port=9001"
        )

        # Log configuration to file
        os.makedirs(
            os.path.join(local_experiment_dir, "avalanche_exporter_config"),
            exist_ok=True,
        )
        with open(
            os.path.join(
                local_experiment_dir, "avalanche_exporter_config", "config.txt"
            ),
            "w",
        ) as f:
            f.write(f"cardinality: {cardinality}\n")
            f.write(f"ingestion_rate: {ingestion_rate}\n")
            f.write(f"port: {port}\n")
            f.write(f"docker_cmd: {docker_cmd}\n")

        # Run on the first node (avalanche generates enough load from single instance)
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=docker_cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

    def stop(self, **kwargs) -> None:
        """
        Stop Avalanche exporter containers across nodes.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        # Stop avalanche containers (common naming pattern)
        cmd = "docker ps --filter name=avalanche-exporter --format '{{.Names}}' | xargs -r docker stop"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=True,
        )

        # Remove containers
        cmd = "docker ps -a --filter name=avalanche-exporter --format '{{.Names}}' | xargs -r docker rm"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=True,
        )

    # def _stop_avalanche_container(self, container_name: str) -> None:
    #     """Stop and remove a specific avalanche container."""
    #     cmd = f"docker stop {container_name}; docker rm {container_name}"
    #     utils.run_on_cloudlab_node(
    #         1,
    #         self.username,
    #         self.hostname_suffix,
    #         cmd,
    #         None,
    #         nohup=False,
    #         popen=False,
    #         ignore_errors=True,
    #     )


class ExporterServiceFactory:
    """Factory for creating appropriate exporter services."""

    @staticmethod
    def create_exporter_service(
        language: str,
        provider: "InfrastructureProvider",
        num_nodes: int,
        use_container: bool,
        node_offset: int,
    ) -> BaseExporterService:
        """
        Create an exporter service based on language.

        Args:
            language: Programming language ("python" or "rust")
            provider: Infrastructure provider for node communication and management
            num_nodes: Number of nodes
            use_container: Whether to use containerized deployment
            node_offset: Starting node index offset

        Returns:
            Appropriate exporter service instance

        Raises:
            ValueError: If language is not supported
        """
        if language == "python":
            return PythonExporterService(
                provider, num_nodes, use_container, node_offset
            )
        elif language == "rust":
            return RustExporterService(provider, num_nodes, use_container, node_offset)
        else:
            raise ValueError(
                f"Invalid fake exporter language: {language}. Supported languages are 'python' and 'rust'"
            )
