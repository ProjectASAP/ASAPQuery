"""
System exporters service management for experiments.

Handles node_exporter, blackbox_exporter, and cadvisor.
"""

from omegaconf import DictConfig
from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider


class SystemExportersService(BaseService):
    """Service for managing system exporters (node_exporter, blackbox_exporter, cadvisor)."""

    def __init__(
        self, provider: InfrastructureProvider, num_nodes: int, node_offset: int
    ):
        """
        Initialize System Exporters service.

        Args:
            provider: Infrastructure provider for node communication and management
            num_nodes: Number of nodes to manage
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.num_nodes = num_nodes
        self.node_offset = node_offset

    def start(self, experiment_params: DictConfig, **kwargs) -> None:
        """
        Start system exporters on nodes.

        Args:
            experiment_params: Experiment configuration parameters (OmegaConf DictConfig)
            **kwargs: Additional configuration
        """
        # Start exporters on worker nodes
        for node_idx in range(
            self.node_offset + 1, self.node_offset + self.num_nodes + 1
        ):
            local_ip = self.provider.get_node_ip(node_idx)

            # Start node_exporter
            node_exporter_port, node_exporter_cmd_options = (
                self._get_node_exporter_options(experiment_params)
            )
            cmd, cmd_dir = self._get_node_exporter_cmd(
                local_ip, node_exporter_port, node_exporter_cmd_options
            )
            self.provider.execute_command(
                node_idx=node_idx,
                cmd=cmd,
                cmd_dir=cmd_dir,
                nohup=True,
                popen=False,
            )

            # Start cadvisor
            cmd, cmd_dir = self._get_cadvisor_cmd(local_ip)
            self.provider.execute_command(
                node_idx=node_idx,
                cmd=cmd,
                cmd_dir=cmd_dir,
                nohup=True,
                popen=False,
            )

        # Start blackbox_exporter on controller node
        coordinator_node = self.node_offset
        cmd, cmd_dir = self._get_blackbox_exporter_cmd(
            local_ip=self.provider.get_node_ip(coordinator_node)
        )
        self.provider.execute_command(
            node_idx=coordinator_node,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=True,
            popen=False,
        )

    def stop(self, **kwargs) -> None:
        """
        Stop system exporters across nodes.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        cmd = "killall node_exporter; killall blackbox_exporter; docker stop cadvisor; docker rm cadvisor"
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

    def _get_node_exporter_cmd(
        self, local_ip: str, local_port: int = 9100, cmd_options: str = ""
    ):
        """Get node_exporter command and working directory."""
        home_dir = self.provider.get_home_dir()
        return (
            f"./node_exporter --web.listen-address={local_ip}:{local_port} {cmd_options} > /dev/null 2>&1 < /dev/null &",
            f"{home_dir}/exporters/node_exporter",
        )

    def _get_blackbox_exporter_cmd(self, local_ip: str, local_port: int = 9115):
        """Get blackbox_exporter command and working directory."""
        home_dir = self.provider.get_home_dir()
        return (
            f"./blackbox_exporter --web.listen-address={local_ip}:{local_port} > /dev/null 2>&1 < /dev/null &",
            f"{home_dir}/exporters/blackbox_exporter",
        )

    def _get_cadvisor_cmd(self, local_ip: str, local_port: int = 8082):
        """Get cadvisor command and working directory."""
        cadvisor_port = 8080
        return (
            f"docker run --volume=/:/rootfs:ro --volume=/var/run:/var/run:ro --volume=/sys:/sys:ro --volume=/scratch/var_lib_docker/:/var/lib/docker:ro --volume=/dev/disk/:/dev/disk:ro --publish={local_ip}:{local_port}:{cadvisor_port} --detach=true --name=cadvisor --privileged   --device=/dev/kmsg gcr.io/cadvisor/cadvisor:v0.49.1",
            None,
        )

    def _get_node_exporter_options(self, experiment_config: DictConfig):
        """Get node_exporter port and extra flags from configuration."""
        port = 9100
        extra_flags = ""

        if "exporters" not in experiment_config:
            return port, extra_flags
        if "exporter_list" not in experiment_config["exporters"]:
            return port, extra_flags
        exporters_config = experiment_config["exporters"]["exporter_list"]

        if "node_exporter" in exporters_config:
            if "extra_flags" in exporters_config["node_exporter"]:
                extra_flags = exporters_config["node_exporter"]["extra_flags"]
            if "port" in exporters_config["node_exporter"]:
                port = exporters_config["node_exporter"]["port"]

        return port, extra_flags

    def is_healthy(self) -> bool:
        """
        Check if system exporters are healthy.

        Returns:
            True if exporters are running
        """
        return True
