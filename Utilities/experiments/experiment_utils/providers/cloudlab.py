"""
CloudLab infrastructure provider implementation.

This module provides the CloudLab-specific implementation of the infrastructure
provider interface, wrapping the existing SSH-based node communication logic.
"""

from typing import Union, Optional, List
import subprocess

from .base import InfrastructureProvider
import utils
import constants


class CloudLabProvider(InfrastructureProvider):
    """
    CloudLab infrastructure provider.

    This provider implements the infrastructure interface using SSH connections
    to CloudLab nodes, maintaining backward compatibility with existing code.
    """

    def __init__(self, username: str, hostname_suffix: str):
        """
        Initialize CloudLab provider.

        Args:
            username: CloudLab username for SSH connections
            hostname_suffix: CloudLab hostname suffix for node addressing
        """
        self.username = username
        self.hostname_suffix = hostname_suffix

    def execute_command(
        self,
        node_idx: int,
        cmd: str,
        cmd_dir: Optional[str] = None,
        nohup: bool = False,
        popen: bool = False,
        ignore_errors: bool = False,
        manual: bool = False,
    ) -> Union[subprocess.Popen, subprocess.CompletedProcess, None]:
        """
        Execute a command on the specified CloudLab node via SSH.

        Args:
            node_idx: Index of the CloudLab node (0-based)
            cmd: Command to execute
            cmd_dir: Working directory for command execution
            nohup: Whether to run command with nohup
            popen: Whether to return Popen object (True) or wait for completion (False)
            ignore_errors: Whether to ignore command execution errors
            manual: Whether to prompt user to manually run command

        Returns:
            Either a Popen object (if popen=True) or CompletedProcess (if popen=False)
        """
        return utils.run_on_cloudlab_node(
            node_idx=node_idx,
            username=self.username,
            hostname_suffix=self.hostname_suffix,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=nohup,
            popen=popen,
            ignore_errors=ignore_errors,
            manual=manual,
        )

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
        Execute a command on multiple CloudLab nodes in parallel via SSH.

        Args:
            node_idxs: List of CloudLab node indices to execute command on
            cmd: Command to execute
            cmd_dir: Working directory for command execution
            nohup: Whether to run command with nohup
            popen: Must be True for parallel execution
            redirect: Whether to redirect output to /dev/null
            wait: Whether to wait for all commands to complete

        Returns:
            List of Popen objects for each node
        """
        if wait:
            utils.run_on_cloudlab_nodes_in_parallel(
                node_idxs=node_idxs,
                username=self.username,
                hostname_suffix=self.hostname_suffix,
                cmd=cmd,
                cmd_dir=cmd_dir,
                nohup=nohup,
                popen=popen,
                redirect=redirect,
            )
            return []  # Original function doesn't return popen objects when waiting
        else:
            return utils.run_on_cloudlab_nodes_in_parallel_without_wait(
                node_idxs=node_idxs,
                username=self.username,
                hostname_suffix=self.hostname_suffix,
                cmd=cmd,
                cmd_dir=cmd_dir,
                nohup=nohup,
                popen=popen,
                redirect=redirect,
            )

    def get_node_address(self, node_idx: int) -> str:
        """
        Get the network address for the specified CloudLab node.

        Args:
            node_idx: Index of the CloudLab node

        Returns:
            CloudLab hostname in the format node{idx}.{hostname_suffix}
        """
        return f"node{node_idx}.{self.hostname_suffix}"

    def get_node_ip(self, node_idx: int) -> str:
        """
        Get the internal network IP for the specified CloudLab node.

        CloudLab nodes use the internal network 10.10.1.0/24 for
        inter-node communication.

        Args:
            node_idx: Index of the CloudLab node

        Returns:
            Internal IP in the format 10.10.1.{idx+1}
        """
        return f"10.10.1.{node_idx + 1}"

    def get_home_dir(self) -> str:
        """
        Get the CloudLab home directory path for experiments.

        Returns:
            CloudLab home directory path from constants
        """
        return constants.CLOUDLAB_HOME_DIR

    def get_query_log_file(self) -> str:
        """
        Get the path to the CloudLab query log file.

        Returns:
            CloudLab query log file path from constants
        """
        return constants.CLOUDLAB_QUERY_LOG_FILE

    def __repr__(self) -> str:
        """Detailed string representation of the CloudLab provider."""
        return f"CloudLabProvider(username='{self.username}', hostname_suffix='{self.hostname_suffix}')"
