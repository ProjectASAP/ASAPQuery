"""
CloudLab Local infrastructure provider implementation.

This module provides a local execution provider for CloudLab nodes that executes
commands locally (no SSH) but uses CloudLab paths and usernames. This is useful
for scripts running ON CloudLab nodes that need local execution.
"""

from typing import Union, Optional, List
import subprocess

from .base import InfrastructureProvider
import constants


class CloudLabLocalProvider(InfrastructureProvider):
    """
    CloudLab Local infrastructure provider.

    This provider implements the infrastructure interface for local execution
    on CloudLab nodes, using CloudLab paths and usernames but executing commands
    locally without SSH.
    """

    def __init__(
        self,
        username: str,
        use_cloudlab_ips: bool,
        cloudlab_home_dir: Optional[str] = None,
    ):
        """
        Initialize CloudLab Local provider.

        Args:
            username: CloudLab username (for compatibility)
            use_cloudlab_ips: If True, return CloudLab network IPs (10.10.1.x).
                            If False, return localhost (127.0.0.1).
            cloudlab_home_dir: CloudLab home directory path (defaults to constants.CLOUDLAB_HOME_DIR)
        """
        self.username = username
        self.use_cloudlab_ips = use_cloudlab_ips
        self.hostname_suffix = (
            "localhost"  # For compatibility with existing localhost checks
        )
        self.cloudlab_home_dir = cloudlab_home_dir or constants.CLOUDLAB_HOME_DIR

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
        Execute a command locally on the CloudLab node.

        Args:
            node_idx: Node index (ignored for local execution)
            cmd: Command to execute
            cmd_dir: Working directory for command execution
            nohup: Whether to run command with nohup
            popen: Whether to return Popen object (True) or wait for completion (False)
            ignore_errors: Whether to ignore command execution errors
            manual: Whether to prompt user to manually run command

        Returns:
            Either a Popen object (if popen=True) or CompletedProcess (if popen=False)
        """
        if manual:
            print(f"Please run manually: {cmd}")
            if cmd_dir:
                print(f"In directory: {cmd_dir}")
            return subprocess.CompletedProcess([], 0, "", "")

        # Build the command
        if nohup:
            cmd = f"nohup {cmd}"

        # Execute locally
        if popen:
            return subprocess.Popen(
                cmd,
                shell=True,
                cwd=cmd_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        else:
            try:
                result = subprocess.run(
                    cmd,
                    shell=True,
                    cwd=cmd_dir,
                    capture_output=True,
                    text=True,
                    check=not ignore_errors,
                )
                return result
            except subprocess.CalledProcessError as e:
                if ignore_errors:
                    return e
                raise

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
        Execute a command in parallel locally.

        Note: For CloudLab local provider, this executes the command once
        since there's only one local node.

        Args:
            node_idxs: List of node indices (ignored for local execution)
            cmd: Command to execute
            cmd_dir: Working directory for command execution
            nohup: Whether to run command with nohup
            popen: Must be True for parallel execution
            redirect: Whether to redirect output to /dev/null
            wait: Whether to wait for all commands to complete

        Returns:
            List containing single Popen object
        """
        if redirect:
            cmd += " > /dev/null 2>&1"

        # Execute once locally (use first node_idx if provided, otherwise 0)
        node_idx = node_idxs[0] if node_idxs else 0
        process = self.execute_command(
            node_idx=node_idx,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=nohup,
            popen=True,  # Always return Popen for parallel
        )

        processes = [process]

        if wait:
            for p in processes:
                p.wait()

        return processes

    def get_node_address(self, node_idx: int) -> str:
        """
        Get the network address for the local node.

        Args:
            node_idx: Node index (ignored for local execution)

        Returns:
            Always returns "localhost" for local execution
        """
        return "localhost"

    def get_node_ip(self, node_idx: int) -> str:
        """
        Get the internal network IP for the local node.

        Args:
            node_idx: Node index

        Returns:
            CloudLab network IP (10.10.1.{idx+1}) if use_cloudlab_ips=True,
            otherwise "127.0.0.1" for localhost
        """
        if self.use_cloudlab_ips:
            return f"10.10.1.{node_idx + 1}"
        return "127.0.0.1"

    def get_home_dir(self) -> str:
        """
        Get the CloudLab home directory path for experiments.

        Returns:
            Path to the CloudLab experiment home directory
        """
        return self.cloudlab_home_dir

    def get_query_log_file(self) -> str:
        """
        Get the path to the query log file.

        Returns:
            Path to the CloudLab query log file
        """
        return constants.CLOUDLAB_QUERY_LOG_FILE
