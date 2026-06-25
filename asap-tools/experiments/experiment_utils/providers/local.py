"""
Local infrastructure provider implementation.

This module provides a single-machine execution provider: no SSH, no
CloudLab paths/usernames. Everything runs as a local subprocess against a
user-configured home directory that mirrors CloudLab's deployment layout.
"""

import os
from typing import Union, Optional, List
import subprocess

from .base import InfrastructureProvider


class LocalProvider(InfrastructureProvider):
    """
    Local infrastructure provider for running experiments on a single dev
    machine, with no CloudLab account/cluster involved.
    """

    def __init__(self, home_dir: str):
        """
        Initialize Local provider.

        Args:
            home_dir: Local directory mirroring CloudLab's deployment layout
                      (prometheus/, code/arroyo/..., experiment_outputs/, etc.)
        """
        self.home_dir = home_dir

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
        """Execute a command locally (node_idx is ignored: there is only one node)."""
        if manual:
            print(f"Please run manually: {cmd}")
            if cmd_dir:
                print(f"In directory: {cmd_dir}")
            return subprocess.CompletedProcess([], 0, "", "")

        if nohup:
            cmd = f"nohup {cmd}"

        # subprocess treats cwd="" as an actual (invalid) path, unlike the
        # CloudLab SSH path's `if cmd_dir: cd {cmd_dir}` which is falsy-safe.
        cmd_dir = cmd_dir or None

        if popen:
            try:
                return subprocess.Popen(
                    cmd,
                    shell=True,
                    cwd=cmd_dir,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            except OSError as e:
                # execute_command_parallel has no ignore_errors knob (see
                # follow-up issue), so this can't be conditional yet. Mirrors
                # CloudLab's SSH transport: launching `ssh` locally never
                # fails even if the remote cmd_dir is missing — the failure
                # is invisible unless something later checks output/health.
                print(f"Warning: failed to launch '{cmd}' in {cmd_dir!r}: {e}")
                return None

        try:
            return subprocess.run(
                cmd,
                shell=True,
                cwd=cmd_dir,
                capture_output=True,
                text=True,
                check=not ignore_errors,
            )
        except (subprocess.CalledProcessError, OSError) as e:
            # Over SSH, a missing cmd_dir just makes the *remote* shell exit
            # nonzero, which ignore_errors already swallows via check=False.
            # Locally, a missing cmd_dir makes subprocess.run itself raise
            # OSError (e.g. FileNotFoundError) before the command ever runs —
            # same "best-effort cleanup, don't crash" intent, different
            # exception type, so it needs the same ignore_errors treatment.
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
        """Execute a command once locally (there is only one local node)."""
        if redirect:
            cmd += " > /dev/null 2>&1"

        process = self.execute_command(
            node_idx=node_idxs[0] if node_idxs else 0,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=nohup,
            popen=True,
        )
        processes = [process]
        if wait:
            for p in processes:
                if p is not None:
                    p.wait()
        return processes

    def is_remote(self) -> bool:
        return False

    def get_node_address(self, node_idx: int) -> str:
        return "localhost"

    def get_node_ip(self, node_idx: int) -> str:
        return "127.0.0.1"

    def get_home_dir(self) -> str:
        return self.home_dir

    def get_query_log_file(self) -> str:
        return os.path.join(self.home_dir, "prometheus", "queries.log")

    def __repr__(self) -> str:
        return f"LocalProvider(home_dir='{self.home_dir}')"
