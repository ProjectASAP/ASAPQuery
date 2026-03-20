"""
Arroyo service management for experiments.
"""

import os
import time
import subprocess
from typing import List, Optional

import constants
from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider


class ArroyoService(BaseService):
    """Service for managing Arroyo cluster and pipelines."""

    def __init__(
        self,
        provider: InfrastructureProvider,
        use_container: bool,
        node_offset: int,
    ):
        """
        Initialize Arroyo service.

        Args:
            provider: Infrastructure provider for node communication and management
            use_container: Whether to use containerized deployment
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.use_container = use_container
        self.node_offset = node_offset
        self.container_name = "sketchdb-arroyo"
        self.active_pipelines = []

    def start(self, experiment_output_dir: str, **kwargs) -> None:
        """
        Start Arroyo cluster.

        Args:
            experiment_output_dir: Directory for experiment output
            **kwargs: Additional configuration
        """
        if self.use_container:
            self._start_containerized(experiment_output_dir, **kwargs)
        else:
            self._start_bare_metal(experiment_output_dir, **kwargs)

    def stop(self, **kwargs) -> None:
        """
        Stop Arroyo cluster and all pipelines.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        if self.use_container:
            self._stop_containerized(**kwargs)
        else:
            self._stop_bare_metal(**kwargs)

        self.active_pipelines.clear()

    def stop_all_jobs(self) -> None:
        """Stop all running Arroyo jobs."""
        cmd = "python3 delete_pipeline.py --all_pipelines"
        cmd_dir = os.path.join(
            self.provider.get_home_dir(), "code", "asap-summary-ingest"
        )
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

        self.active_pipelines.clear()

    def get_arroyo_pids(self) -> Optional[List[int]]:
        """
        Get PIDs of running Arroyo worker processes.

        Returns:
            List of PIDs or None if no processes found
        """
        if self.use_container:
            return self._get_arroyo_pids_containerized()
        else:
            return self._get_arroyo_pids_bare_metal()

    def run_arroyosketch(
        self,
        experiment_name: str,
        experiment_output_dir: str,
        flink_input_format: str,
        flink_output_format: str,
        controller_remote_output_dir: str,
        remote_write_ip: str,
        remote_write_base_port: int,
        remote_write_path: str,
        parallelism: int,
        use_kafka_ingest: bool = False,
        enable_optimized_remote_write: bool = False,
        avoid_long_ssh: bool = False,
        sketch_cms_impl: str = "legacy",
        sketch_kll_impl: str = "legacy",
        sketch_cmwh_impl: str = "legacy",
    ) -> str:
        """
        Run ArroyoSketch pipeline.

        Args:
            experiment_name: Name of the experiment
            experiment_output_dir: Directory for experiment output
            flink_input_format: Input data format
            flink_output_format: Output data format
            controller_remote_output_dir: Controller output directory
            use_kafka_ingest: If True, use Kafka as input source; if False, use Prometheus remote write
            remote_write_ip: IP address for Prometheus remote write endpoint
            remote_write_base_port: Base port for Prometheus remote write endpoint
            remote_write_path: Path for Prometheus remote write endpoint
            parallelism: Pipeline parallelism
            enable_optimized_remote_write: If True, use optimized Prometheus remote_write source (10-20x faster)
            avoid_long_ssh: If True, run command in background to avoid long SSH connections
            sketch_cms_impl: Count-Min Sketch backend (legacy|sketchlib). Must match QueryEngine.
            sketch_kll_impl: KLL Sketch backend (legacy|sketchlib). Must match QueryEngine.
            sketch_cmwh_impl: Count-Min-With-Heap backend (legacy|sketchlib). Must match QueryEngine.

        Returns:
            Pipeline ID

        Raises:
            RuntimeError: If cluster is not running or pipeline creation fails
        """
        arroyosketch_output_dir = os.path.join(
            experiment_output_dir, "arroyosketch_output"
        )

        if use_kafka_ingest:
            cmd = "python run_arroyosketch.py --source_type kafka --kafka_input_format {} --output_format {} --pipeline_name {} --config_file_path {}/streaming_config.yaml  --input_kafka_topic {} --output_kafka_topic {} --output_dir {} --sketch_cms_impl {} --sketch_kll_impl {} --sketch_cmwh_impl {}".format(
                flink_input_format,
                flink_output_format,
                experiment_name,
                controller_remote_output_dir,
                constants.FLINK_INPUT_TOPIC,
                constants.FLINK_OUTPUT_TOPIC,
                arroyosketch_output_dir,
                sketch_cms_impl,
                sketch_kll_impl,
                sketch_cmwh_impl,
            )
        else:
            # Build base command for Prometheus remote write
            cmd = "python run_arroyosketch.py --source_type prometheus_remote_write --prometheus_bind_ip {} --prometheus_base_port {} --prometheus_path {} --parallelism {} --output_format {} --pipeline_name {} --config_file_path {}/streaming_config.yaml --output_kafka_topic {} --output_dir {}".format(
                remote_write_ip,
                remote_write_base_port,
                remote_write_path,
                parallelism,
                flink_output_format,
                experiment_name,
                controller_remote_output_dir,
                constants.FLINK_OUTPUT_TOPIC,
                arroyosketch_output_dir,
            )
            # Add optimized source flag if enabled
            if enable_optimized_remote_write:
                cmd += " --prometheus_remote_write_source optimized"
        # Sketch impl mode - must match QueryEngine
        cmd += " --sketch_cms_impl {} --sketch_kll_impl {} --sketch_cmwh_impl {}".format(
            sketch_cms_impl, sketch_kll_impl, sketch_cmwh_impl
        )
        cmd_dir = os.path.join(
            constants.CLOUDLAB_HOME_DIR, "code", "asap-summary-ingest"
        )

        if avoid_long_ssh:
            # Run in background to avoid long SSH connection
            cmd = "mkdir -p {}; {}".format(arroyosketch_output_dir, cmd)
            cmd += " > {}/arroyosketch.out 2>&1 < /dev/null &".format(
                arroyosketch_output_dir
            )
            self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=cmd_dir,
                nohup=True,
                popen=False,
            )
            # Poll until process finishes
            self.wait_for_run_arroyosketch()

            # Read pipeline ID from file
            pipeline_id = self.read_pipeline_id_from_file(arroyosketch_output_dir)
        else:
            # Traditional synchronous execution
            ret = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=cmd_dir,
                nohup=False,
                popen=False,
            )
            assert isinstance(ret, subprocess.CompletedProcess)

            # Parse pipeline ID from stdout
            pipeline_id = None
            for line in ret.stdout.split("\n"):
                if "Pipeline created with ID" in line:
                    pipeline_id = line.strip().split(":")[-1].strip()
                    break

            if pipeline_id is None:
                raise RuntimeError(
                    "Failed to retrieve pipeline ID from Arroyo job submission"
                )

        self.active_pipelines.append(pipeline_id)
        return pipeline_id

    def stop_arroyosketch(self, pipeline_id: str) -> None:
        """
        Stop ArroyoSketch pipeline by deleting it.

        Args:
            pipeline_id: ID of the pipeline to stop
        """
        cmd = "python3 delete_pipeline.py --pipeline_id {}".format(pipeline_id)
        cmd_dir = os.path.join(
            constants.CLOUDLAB_HOME_DIR, "code", "asap-summary-ingest"
        )
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=False,
            popen=False,
            manual=False,
        )

        if pipeline_id in self.active_pipelines:
            self.active_pipelines.remove(pipeline_id)

    def wait_for_run_arroyosketch(self, polling_interval: int = 10) -> None:
        """
        Wait for run_arroyosketch.py process to complete.

        Args:
            polling_interval: Seconds between polling checks
        """
        print("Waiting for run_arroyosketch.py to complete...")
        while True:
            cmd = "pgrep -f run_arroyosketch.py"
            ret = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
            assert isinstance(ret, subprocess.CompletedProcess)
            if ret.stdout.strip() == "":
                print("run_arroyosketch.py has completed")
                break
            print(
                f"run_arroyosketch.py is still running. Will check again in {polling_interval} seconds."
            )
            time.sleep(polling_interval)

    def read_pipeline_id_from_file(self, arroyosketch_output_dir: str) -> str:
        """
        Read pipeline ID from file written by run_arroyosketch.py.

        Args:
            arroyosketch_output_dir: Directory containing pipeline_id.txt

        Returns:
            Pipeline ID string

        Raises:
            RuntimeError: If file doesn't exist or is empty
        """
        pipeline_id_file = os.path.join(arroyosketch_output_dir, "pipeline_id.txt")

        cmd = f"cat {pipeline_id_file}"
        ret = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

        assert isinstance(ret, subprocess.CompletedProcess)

        if ret.returncode != 0 or not ret.stdout.strip():
            raise RuntimeError(
                f"Failed to read pipeline ID from {pipeline_id_file}. "
                f"File may not exist or is empty. Return code: {ret.returncode}"
            )

        pipeline_id = ret.stdout.strip()
        print(f"Retrieved pipeline ID from file: {pipeline_id}")
        return pipeline_id

    def is_healthy(self) -> bool:
        """
        Check if Arroyo cluster is healthy.

        Returns:
            True if cluster is running
        """
        if self.use_container:
            return self._is_healthy_containerized()
        else:
            return self._is_healthy_bare_metal()

    def _start_bare_metal(self, experiment_output_dir: str, **kwargs) -> None:
        """Start Arroyo cluster using bare metal deployment (original implementation)."""
        arroyo_config_file_path = os.path.join(
            constants.CLOUDLAB_HOME_DIR, "code", "asap-summary-ingest", "config.yaml"
        )
        arroyo_bin_path = os.path.join(
            constants.CLOUDLAB_HOME_DIR, "code", "arroyo", "target", "release", "arroyo"
        )

        arroyo_output_file = os.path.join(experiment_output_dir, "arroyo_cluster.out")
        cmd = r"""bash -l -c \"nohup {} --config {} cluster > {} 2>&1 &\" """.format(
            arroyo_bin_path, arroyo_config_file_path, arroyo_output_file
        )

        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            manual=False,
        )

    def _start_containerized(self, experiment_output_dir: str, **kwargs) -> None:
        """Start Arroyo cluster using Docker container deployment."""
        arroyo_config_file_path = os.path.join(
            constants.CLOUDLAB_HOME_DIR, "code", "asap-summary-ingest", "config.yaml"
        )
        arroyo_output_file = os.path.join(experiment_output_dir, "arroyo_cluster.out")

        # Stop and remove existing container if it exists
        self._stop_containerized()

        # Use host networking to avoid port conflicts and access host Kafka service
        # Docker run command with config mount and host networking
        cmd = f"docker run --detach --pull never --name {self.container_name} --network host -v {arroyo_config_file_path}:/config.yaml {constants.ARROYO_IMAGE} --config /config.yaml cluster > {arroyo_output_file} 2>&1"

        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            manual=False,
        )

    def _stop_bare_metal(self, **kwargs) -> None:
        """Stop Arroyo cluster using bare metal deployment (original implementation)."""
        # Stop cluster
        # TODO: we should make this more robust. Arroyo processes sometimes do not get killed
        cmd = "pkill -SIGKILL -f 'arroyo.*cluster'"

        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )
        cmd = "pkill -SIGKILL -f 'arroyo.*worker'"

        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

    def _stop_containerized(self, **kwargs) -> None:
        """Stop Arroyo cluster using Docker container deployment."""
        try:
            # Stop and remove container
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
            print(f"Error stopping Arroyo container: {e}")

    def _get_arroyo_pids_bare_metal(self) -> Optional[List[int]]:
        """Get PIDs using bare metal deployment (original implementation)."""
        keywords = ["arroyo worker"]

        cmd = ";".join(
            r"ps aux | grep \"{}\" | grep -v grep | awk '{{print \$2}}'".format(keyword)
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

    def _get_arroyo_pids_containerized(self) -> Optional[List[int]]:
        """Get PIDs using Docker container deployment."""
        try:
            # Get container PID
            cmd = f"docker inspect --format='{{{{.State.Pid}}}}' {self.container_name}"
            result = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )

            assert isinstance(result, subprocess.CompletedProcess)
            if result.stdout.strip() and result.stdout.strip() != "0":
                return [int(result.stdout.strip())]
            return None
        except Exception:
            return None

    def _is_healthy_bare_metal(self) -> bool:
        """Check if Arroyo cluster is healthy using bare metal deployment."""
        try:
            cmd = "pgrep -f 'arroyo.*cluster'"
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

    def _is_healthy_containerized(self) -> bool:
        """Check if Arroyo cluster is healthy using Docker container deployment."""
        try:
            # Check if container is running
            cmd = f"docker inspect -f '{{{{.State.Running}}}}' {self.container_name}"
            result = self.provider.execute_command(
                node_idx=self.node_offset,
                cmd=cmd,
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
            assert isinstance(result, subprocess.CompletedProcess)
            return result.stdout.strip() == "true"
        except Exception:
            return False

    def get_monitoring_keyword(self) -> str:
        """
        Get the keyword to use for process monitoring.

        Returns:
            Container name if using containers, otherwise process name
        """
        if self.use_container:
            return self.container_name
        else:
            return "arroyo.*worker"
