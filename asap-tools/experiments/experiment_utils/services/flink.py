"""
Flink service management for experiments.
"""

import os
import subprocess
from typing import List, Optional, Tuple

import constants
from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider


class FlinkService(BaseService):
    """Service for managing Flink cluster and jobs."""

    def __init__(self, provider: InfrastructureProvider, node_offset: int):
        """
        Initialize Flink service.

        Args:
            provider: Infrastructure provider for node communication and management
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.node_offset = node_offset
        self.active_jobs = []

    def start(self, **kwargs) -> None:
        """
        Start Flink cluster if not already running.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        # Check if already running
        if self._is_cluster_running():
            return

        cmd = """
        if ! jps | grep -q StandaloneSessionClusterEntrypoint; then
            ./bin/start-cluster.sh
        fi
        """
        cmd_dir = os.path.join(self.provider.get_home_dir(), "flink")

        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=False,
            popen=False,
            manual=False,
        )

    def stop(self, **kwargs) -> None:
        """
        Stop Flink cluster and all jobs.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        # Stop all running jobs first
        self.stop_all_jobs()

        # Stop cluster
        cmd = "./bin/stop-cluster.sh"
        cmd_dir = os.path.join(self.provider.get_home_dir(), "flink")
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=False,
            popen=False,
            manual=False,
        )

        self.active_jobs.clear()

    def stop_all_jobs(self) -> None:
        """Stop all running Flink jobs."""
        flink_dir = os.path.join(constants.CLOUDLAB_HOME_DIR, "flink", "bin")
        flink_exe = "./flink"

        cmd = (
            flink_exe
            + r" list -r | grep RUNNING | awk '{print \$4}' | xargs -I {} "
            + flink_exe
            + r" cancel {}"
        )

        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=flink_dir,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

        self.active_jobs.clear()

    def stop_all_java_processes(self) -> None:
        """Stop all Java processes (useful for local Flink mode)."""
        cmd = "pkill -f java"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

        self.active_jobs.clear()

    def get_flink_pids(self, do_local_flink: bool = False) -> Optional[List[int]]:
        """
        Get PIDs of running Flink processes.

        Args:
            do_local_flink: Whether running in local mode

        Returns:
            List of PIDs or None if no processes found
        """
        keywords = ["sketch-0.1.jar"]
        if not do_local_flink:
            keywords.append("TaskManagerRunner")

        cmd = ";".join(
            "pgrep java -a | grep {} | cut -d ' ' -f1".format(keyword)
            for keyword in keywords
        )

        result = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

        assert isinstance(result, subprocess.CompletedProcess)
        if result.stdout == "":
            return None

        pids = result.stdout.split("\n")
        pids = [int(pid) for pid in pids if pid != ""]
        return pids

    def run_flinksketch(
        self,
        experiment_output_dir: str,
        flink_input_format: str,
        flink_output_format: str,
        enable_object_reuse: bool,
        do_local_flink: bool,
        controller_remote_output_dir: str,
        compress_json: bool,
    ) -> Tuple[Optional[str], Optional[subprocess.Popen]]:
        """
        Run FlinkSketch job.

        Args:
            experiment_output_dir: Directory for experiment output
            flink_input_format: Input data format
            flink_output_format: Output data format
            enable_object_reuse: Whether to enable object reuse optimization
            do_local_flink: Whether to run in local mode
            controller_remote_output_dir: Controller output directory
            compress_json: Whether to compress JSON output

        Returns:
            Tuple of (job_id, popen_process)
        """
        flink_exe = os.path.join(constants.CLOUDLAB_HOME_DIR, "flink", "bin", "flink")
        flinksketch_dir = os.path.join(
            constants.CLOUDLAB_HOME_DIR, "code", "FlinkSketch"
        )

        if do_local_flink:
            cmd_prefix = "java -cp {}/lib/*:./target/sketch-0.1.jar org.myorg.flink.DataStreamJob".format(
                os.path.join(constants.CLOUDLAB_HOME_DIR, "flink")
            )
        else:
            cmd_prefix = "{} run ./target/sketch-0.1.jar".format(flink_exe)

        # Original command with output redirection (commented out for stdout monitoring)
        # cmd = "{} --inputKafkaTopic {} --outputKafkaTopic {} --configFilePath {}/streaming_config.yaml --readFlowkey false --outputFormat {} --kafkaInputFormat {} --enableObjectReuse {} {} {} {} > {} 2>&1 &".format(
        #     cmd_prefix,
        #     constants.FLINK_INPUT_TOPIC,
        #     constants.FLINK_OUTPUT_TOPIC,
        #     controller_remote_output_dir,
        #     flink_output_format,
        #     flink_input_format,
        #     str(enable_object_reuse).lower(),
        #     "--compressJson true" if compress_json else "",
        #     "--logLevel DEBUG",
        #     "--skipKeyByIfPossible true",
        #     os.path.join(experiment_output_dir, "flinksketch.out"),
        # )

        # Command without output redirection to enable stdout monitoring
        cmd = "{} --inputKafkaTopic {} --outputKafkaTopic {} --configFilePath {}/streaming_config.yaml --readFlowkey false --outputFormat {} --kafkaInputFormat {} --enableObjectReuse {} {} {} {}".format(
            cmd_prefix,
            constants.FLINK_INPUT_TOPIC,
            constants.FLINK_OUTPUT_TOPIC,
            controller_remote_output_dir,
            flink_output_format,
            flink_input_format,
            str(enable_object_reuse).lower(),
            "--compressJson true" if compress_json else "",
            "--logLevel DEBUG",
            "--skipKeyByIfPossible true",
        )

        popen = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=flinksketch_dir,
            nohup=True,
            popen=True,
        )

        if do_local_flink:
            return None, popen
        else:
            assert popen is not None and popen.stdout is not None
            job_id = None
            for line in iter(popen.stdout.readline, b""):
                decoded_line = line.decode("utf-8").strip()
                if "Job has been submitted with JobID" in decoded_line:
                    job_id = decoded_line.split()[-1]
                    break

            if job_id is None:
                raise RuntimeError("Failed to retrieve JobID from Flink job submission")

            self.active_jobs.append(job_id)
            return job_id, popen

    def stop_flinksketch(
        self,
        job_id: Optional[str],
        popen: Optional[subprocess.Popen],
        flink_pids: Optional[List[int]],
        do_local_flink: bool,
    ) -> None:
        """
        Stop FlinkSketch job.

        Args:
            job_id: Flink job ID (for cluster mode)
            popen: Process handle
            flink_pids: Process IDs (for local mode)
            do_local_flink: Whether running in local mode
        """
        if do_local_flink:
            if flink_pids:
                cmd = ";".join(["kill -9 {}".format(pid) for pid in flink_pids])
                self.provider.execute_command(
                    node_idx=self.node_offset,
                    cmd=cmd,
                    cmd_dir=None,
                    nohup=False,
                    popen=False,
                    manual=False,
                )
        else:
            if job_id:
                flink_exe = os.path.join(
                    constants.CLOUDLAB_HOME_DIR, "flink", "bin", "flink"
                )
                cmd = "{} cancel {}".format(flink_exe, job_id)
                self.provider.execute_command(
                    node_idx=self.node_offset,
                    cmd=cmd,
                    cmd_dir=None,
                    nohup=False,
                    popen=False,
                    manual=False,
                )

                if job_id in self.active_jobs:
                    self.active_jobs.remove(job_id)

        if popen:
            popen.terminate()

    def is_healthy(self) -> bool:
        """
        Check if Flink cluster is healthy.

        Returns:
            True if cluster is running and responsive
        """
        return self._is_cluster_running()

    def _is_cluster_running(self) -> bool:
        """Check if Flink cluster is actually running."""
        try:
            cmd = "jps | grep -q StandaloneSessionClusterEntrypoint"
            result = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
            assert isinstance(result, subprocess.CompletedProcess)
            return result.returncode == 0
        except Exception:
            return False
