"""
Base infrastructure provider interface for experiment management.

This module defines the abstract interface that all infrastructure providers
must implement to support running experiments across different environments.
"""

from abc import ABC, abstractmethod
from typing import Union, Optional, List
import subprocess


class InfrastructureProvider(ABC):
    """
    Abstract base class for infrastructure providers.

    This interface defines the contract that all infrastructure providers
    (CloudLab, AWS, Kubernetes, Local) must implement.
    """

    @abstractmethod
    def execute_command(
        self,
        node_idx: int,
        cmd: str,
        cmd_dir: Optional[str] = None,
        nohup: bool = False,
        popen: bool = False,
        ignore_errors: bool = False,
        manual: bool = False,
    ) -> Union[subprocess.Popen, subprocess.CompletedProcess]:
        """
        Execute a command on the specified node.

        Args:
            node_idx: Index of the node to execute command on
            cmd: Command to execute
            cmd_dir: Working directory for command execution
            nohup: Whether to run command with nohup
            popen: Whether to return Popen object (True) or wait for completion (False)
            ignore_errors: Whether to ignore command execution errors
            manual: Whether to prompt user to manually run command

        Returns:
            Either a Popen object (if popen=True) or CompletedProcess (if popen=False)
        """
        pass

    @abstractmethod
    def execute_command_parallel(
        self,
        node_idxs: List[int],
        cmd: str,
        cmd_dir: Optional[str] = None,
        nohup: bool = False,
        popen: bool = True,
        redirect: bool = False,
        wait: bool = True,
    ) -> List[subprocess.Popen]:
        """
        Execute a command on multiple nodes in parallel.

        Args:
            node_idxs: List of node indices to execute command on
            cmd: Command to execute
            cmd_dir: Working directory for command execution
            nohup: Whether to run command with nohup
            popen: Must be True for parallel execution
            redirect: Whether to redirect output to /dev/null
            wait: Whether to wait for all commands to complete

        Returns:
            List of Popen objects for each node
        """
        pass

    @abstractmethod
    def get_node_address(self, node_idx: int) -> str:
        """
        Get the network address for the specified node.

        Args:
            node_idx: Index of the node

        Returns:
            Network address (hostname, IP, etc.) for the node
        """
        pass

    @abstractmethod
    def get_node_ip(self, node_idx: int) -> str:
        """
        Get the internal network IP for the specified node.

        This is used for service-to-service communication within the cluster.
        Different from get_node_address() which may return hostnames for SSH.

        Args:
            node_idx: Index of the node

        Returns:
            Internal IP address for the node
        """
        pass

    @abstractmethod
    def get_home_dir(self) -> str:
        """
        Get the home directory path for experiments.

        Returns:
            Path to the experiment home directory
        """
        pass

    @abstractmethod
    def get_query_log_file(self) -> str:
        """
        Get the path to the query log file.

        Returns:
            Path to the query log file
        """
        pass

    def get_provider_type(self) -> str:
        """
        Get the provider type identifier.

        Returns:
            String identifier for the provider type
        """
        return self.__class__.__name__.replace("Provider", "").lower()

    def __str__(self) -> str:
        """String representation of the provider."""
        return f"{self.__class__.__name__}()"

    def __repr__(self) -> str:
        """Detailed string representation of the provider."""
        return f"{self.__class__.__name__}()"
