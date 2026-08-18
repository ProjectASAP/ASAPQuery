"""
Remote monitor service management for experiments.
"""

import os
import re
import shlex
import time
import subprocess
from typing import List, Optional

import constants
from .base import BaseService
from experiment_utils.providers.base import InfrastructureProvider
from .query_engine import BaseQueryEngineService
from .arroyo import ArroyoService


class RemoteMonitorService(BaseService):
    """Service for managing remote monitor processes."""

    def __init__(self, provider: InfrastructureProvider, node_offset: int):
        """
        Initialize Remote Monitor service.

        Args:
            provider: Infrastructure provider for node communication and management
            node_offset: Starting node index offset
        """
        super().__init__(provider)
        self.node_offset = node_offset

    def start(
        self,
        controller_client_config: str,
        experiment_output_dir: str,
        experiment_mode: str,
        profile_query_engine: bool,
        profile_prometheus_time: Optional[int],
        profile_flink: bool,
        flink_pids: Optional[List[int]],
        profile_arroyo: bool,
        arroyo_pids: Optional[List[int]],
        manual_mode: bool,
        do_local_flink: bool,
        streaming_engine: str,
        query_engine_service: "BaseQueryEngineService",
        arroyo_service: Optional["ArroyoService"],
        controller_remote_output_dir: str,
        use_container_prometheus_client: bool,
        prometheus_client_parallel: bool,
        backend_protocol: str,
        pre_query_wait_seconds: int,
        monitor_interval_seconds: float,
        backend_tool: Optional[str] = None,
        timed_duration: Optional[int] = None,
    ) -> None:
        """
        Start remote monitor processes.

        Args:
            **kwargs: Additional configuration (currently unused)
            timed_duration: If provided, use timed mode instead of prometheus_client mode
            backend_tool: TSDB running on the node ("prometheus" or "victoriametrics").
                Not used when backend_protocol="clickhouse".
            pre_query_wait_seconds: In prometheus_client mode, seconds to wait
                (with the process monitor already running) before launching the
                query client. Used to capture the precompute/ingest phase in the
                same continuous monitor session as the query phase.
            monitor_interval_seconds: Seconds between CPU/memory samples.
        """
        # Determine execution mode
        use_timed_mode = timed_duration is not None

        # Determine which config file to look for based on monitoring tool.
        # Only relevant when backend_protocol != "clickhouse".
        if backend_tool == "victoriametrics":
            # first one is for vmagent
            # second one is for vmsingle
            # TODO: remove this hardcoding and instead query the service to get this
            config_keywords = [
                constants.VMAGENT_SCRAPE_CONFIG_FILE,
                "victoriametrics-single",
            ]
        else:
            config_keywords = [constants.PROMETHEUS_CONFIG_FILE]

        # Build the list of process keywords to monitor.
        if backend_protocol == "clickhouse":
            # ClickHouse/SQL experiments: use ps-friendly process names rather
            # than Docker container names. Host-network bare-metal ClickHouse
            # does not show up as ``clickhouse-server`` in ``ps``, and
            # ``docker inspect`` can fail in background SSH shells.
            if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
                if query_engine_service is not None:
                    keywords = [query_engine_service.get_monitoring_keyword()]
                else:
                    keywords = [constants.QUERY_ENGINE_RS_PROCESS_KEYWORD]
            else:
                keywords = ["clickhouse"]
        else:
            keywords = list(config_keywords)

            if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
                if query_engine_service is not None:
                    keywords.append(query_engine_service.get_monitoring_keyword())
                else:
                    keywords.append(constants.QUERY_ENGINE_RS_PROCESS_KEYWORD)

                if streaming_engine == "flink":
                    keywords.append("sketch-0.1.jar")
                    if not do_local_flink:
                        keywords.append(
                            "org.apache.flink.runtime.taskexecutor.TaskManagerRunner"
                        )
                elif streaming_engine == "arroyo":
                    if arroyo_service is not None:
                        keywords.append(arroyo_service.get_monitoring_keyword())
                    else:
                        keywords.append("arroyo.*worker")

        if use_timed_mode:
            # Build command for timed mode (skip_querying)
            cmd = (
                "python3 -u remote_monitor.py "
                "--execution_mode timed "
                "--experiment_mode {} "
                r"--keywords \"{}\" "
                "--config_file {} "
                "--experiment_output_dir {} "
                "--monitor_output_file {} "
                "--time_to_run {} "
                "--node_offset {} "
                "--streaming_engine {} "
                "--monitor_interval_seconds {} "
            ).format(
                experiment_mode,
                ",".join(keywords),
                os.path.join(
                    os.path.dirname(experiment_output_dir),
                    "controller_client_configs",
                    os.path.basename(controller_client_config),
                ),
                experiment_output_dir,
                "monitor_output.json",
                timed_duration,
                self.node_offset,
                streaming_engine,
                monitor_interval_seconds,
            )

            cmd_dir = os.path.join(
                self.provider.get_home_dir(), "code", "asap-tools", "experiments"
            )
            cmd += " > {}/remote_monitor.out 2>&1".format(experiment_output_dir)

            if manual_mode:
                input(
                    "In manual mode. Remote monitor is not going to be started. Press Enter to continue"
                )
                print(cmd_dir)
                print(cmd)
                input("In manual mode. Press Enter to teardown the experiment")
            else:
                # Timed mode always runs in background
                cmd += " < /dev/null &"
                self.provider.execute_command(
                    node_idx=self.node_offset,
                    cmd=cmd,
                    cmd_dir=cmd_dir,
                    nohup=True,
                    popen=False,
                )
            return

        # Original prometheus_client mode logic
        assert controller_remote_output_dir is not None

        cmd = (
            "python3 -u remote_monitor.py "
            "--execution_mode prometheus_client "
            "--experiment_mode {} "
            r"--keywords \"{}\" "
            "--config_file {} "
            "--experiment_output_dir {} "
            "--monitor_output_file {} "
            "--prometheus_client_output_file {} "
            "--node_offset {} "
            "--streaming_engine {} "
            "--monitor_interval_seconds {} "
        ).format(
            experiment_mode,
            ",".join(keywords),
            os.path.join(
                os.path.dirname(experiment_output_dir),
                "controller_client_configs",
                os.path.basename(controller_client_config),
            ),
            experiment_output_dir,
            "monitor_output.json",
            "prometheus_client_output.txt",
            self.node_offset,
            streaming_engine,
            monitor_interval_seconds,
        )

        if pre_query_wait_seconds > 0:
            cmd += " --pre_query_wait_seconds {}".format(pre_query_wait_seconds)

        # Add container flag if enabled
        if use_container_prometheus_client:
            cmd += " --use_container_prometheus_client"

        # Add parallel flag if enabled
        if prometheus_client_parallel:
            cmd += " --prometheus_client_parallel"

        if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
            cmd += " --query_engine_config_file {}".format(
                os.path.join(controller_remote_output_dir, "inference_config.yaml")
            )

            if profile_query_engine:
                cmd += " --profile_query_engine"

            if profile_flink and flink_pids:
                cmd += " --profile_flink_pids {}".format(",".join(map(str, flink_pids)))

            if profile_arroyo and arroyo_pids:
                cmd += " --profile_arroyo_pids {}".format(
                    ",".join(map(str, arroyo_pids))
                )

        if profile_prometheus_time is not None:
            cmd += " --profile_prometheus_time {}".format(profile_prometheus_time)

        cmd += " --backend_protocol {}".format(backend_protocol)

        cmd_dir = os.path.join(
            self.provider.get_home_dir(), "code", "asap-tools", "experiments"
        )

        cmd += " > {}/remote_monitor.out 2>&1".format(experiment_output_dir)

        if manual_mode:
            input(
                "In manual mode. Remote monitor is not going to be started. Press Enter to continue"
            )
            print(cmd_dir)
            print(cmd)
            input("In manual mode. Press Enter to teardown the experiment")
        else:
            if constants.AVOID_REMOTE_MONITOR_LONG_SSH:
                cmd += " < /dev/null &"
                self.provider.execute_command(
                    node_idx=self.node_offset,
                    cmd=cmd,
                    cmd_dir=cmd_dir,
                    nohup=True,
                    popen=False,
                )
            else:
                self.provider.execute_command(
                    node_idx=self.node_offset,
                    cmd=cmd,
                    cmd_dir=cmd_dir,
                    nohup=False,
                    popen=False,
                )

    def start_clickhouse_ingest_monitor(
        self,
        controller_client_config: str,
        experiment_output_dir: str,
        monitor_interval_seconds: float = 1.0,
        monitor_output_file: str = "monitor_output.json",
        manual_mode: bool = False,
    ) -> None:
        """Monitor ClickHouse CPU/memory during bulk JSONEachRow load.

        Runs ``remote_monitor.py`` in ``ingest`` mode until the stop file
        (``constants.INGEST_MONITOR_STOP_FILE``) is created, or SIGTERM/SIGINT,
        then writes ``monitor_output_file`` under ``remote_monitor_output/``.
        """
        keywords = ["clickhouse"]
        cmd = (
            "python3 -u remote_monitor.py "
            "--execution_mode ingest "
            "--experiment_mode {} "
            r"--keywords \"{}\" "
            "--config_file {} "
            "--experiment_output_dir {} "
            "--monitor_output_file {} "
            "--node_offset {} "
            "--streaming_engine {} "
            "--monitor_interval_seconds {} "
            "--backend_protocol clickhouse "
        ).format(
            constants.BASELINE_EXPERIMENT_NAME,
            ",".join(keywords),
            controller_client_config,
            experiment_output_dir,
            monitor_output_file,
            self.node_offset,
            "precompute",
            monitor_interval_seconds,
        )

        cmd_dir = os.path.join(
            self.provider.get_home_dir(), "code", "asap-tools", "experiments"
        )
        stop_file = os.path.join(
            experiment_output_dir, constants.INGEST_MONITOR_STOP_FILE
        )
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"rm -f {shlex.quote(stop_file)}",
            cmd_dir="",
            nohup=False,
            popen=False,
            ignore_errors=True,
        )
        cmd += " > {}/remote_monitor.out 2>&1".format(experiment_output_dir)

        if manual_mode:
            input(
                "In manual mode. ClickHouse ingest monitor will not start. Press Enter"
            )
            print(cmd_dir)
            print(cmd)
            return

        cmd += " < /dev/null &"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=cmd_dir,
            nohup=True,
            popen=False,
        )

    @staticmethod
    def _remote_monitor_pgrep_pattern(
        execution_mode: Optional[str] = None,
        experiment_output_dir: Optional[str] = None,
    ) -> str:
        """Build a ``pgrep -f`` regex that prefers mode/dir over a bare script name.

        Matching only ``remote_monitor.py`` can hit leftover processes from a
        prior crashed run. Prefer ``--execution_mode`` and the experiment output
        directory when known.
        """
        parts = ["remote_monitor\\.py"]
        if execution_mode:
            parts.append(f"--execution_mode {execution_mode}")
        if experiment_output_dir:
            # Escape regex metacharacters in the path (e.g. dots in hostnames).
            parts.append(re.escape(experiment_output_dir))
        return ".*".join(parts)

    def is_remote_monitor_running(
        self,
        execution_mode: Optional[str] = None,
        experiment_output_dir: Optional[str] = None,
    ) -> bool:
        pattern = self._remote_monitor_pgrep_pattern(
            execution_mode=execution_mode,
            experiment_output_dir=experiment_output_dir,
        )
        cmd = f"pgrep -f {shlex.quote(pattern)}"
        result = self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )
        assert isinstance(result, subprocess.CompletedProcess)
        return bool(result.stdout.strip())

    def wait_for_remote_monitor_start(
        self,
        timeout: int,
        polling_interval: int,
        execution_mode: Optional[str] = None,
        experiment_output_dir: Optional[str] = None,
    ) -> None:
        """Poll until a matching ``remote_monitor.py`` is running on the node."""
        start = time.time()
        while time.time() - start < timeout:
            if self.is_remote_monitor_running(
                execution_mode=execution_mode,
                experiment_output_dir=experiment_output_dir,
            ):
                return
            time.sleep(polling_interval)
        mode_desc = execution_mode or "any"
        raise RuntimeError(
            f"remote_monitor.py ({mode_desc}) did not start within "
            f"{timeout}s. Check remote_monitor.out under the experiment output dir."
        )

    def wait_for_remote_monitor_process_exit(
        self,
        polling_interval: int,
        execution_mode: Optional[str] = None,
        experiment_output_dir: Optional[str] = None,
    ) -> None:
        """Poll until matching ``remote_monitor.py`` processes have exited."""
        while self.is_remote_monitor_running(
            execution_mode=execution_mode,
            experiment_output_dir=experiment_output_dir,
        ):
            print(
                "Waiting for remote monitor to exit "
                f"(checking again in {polling_interval}s)..."
            )
            time.sleep(polling_interval)

    def signal_ingest_monitor_stop(self, experiment_output_dir: str) -> None:
        """Ask an ingest-mode ``remote_monitor.py`` to shut down gracefully."""
        stop_file = os.path.join(
            experiment_output_dir, constants.INGEST_MONITOR_STOP_FILE
        )
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"touch {shlex.quote(stop_file)}",
            cmd_dir="",
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

    def cleanup_ingest_monitor_stop_file(self, experiment_output_dir: str) -> None:
        stop_file = os.path.join(
            experiment_output_dir, constants.INGEST_MONITOR_STOP_FILE
        )
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=f"rm -f {shlex.quote(stop_file)}",
            cmd_dir="",
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

    def stop(self, **kwargs) -> None:
        """
        Stop remote monitor processes.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        self.kill_remote_monitor()

    def kill_remote_monitor(
        self,
        execution_mode: Optional[str] = None,
        experiment_output_dir: Optional[str] = None,
    ) -> None:
        """Kill matching remote monitor processes."""
        pattern = self._remote_monitor_pgrep_pattern(
            execution_mode=execution_mode,
            experiment_output_dir=experiment_output_dir,
        )
        cmd = f"pkill -f {shlex.quote(pattern)}"
        self.provider.execute_command(
            node_idx=self.node_offset,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

    def wait_for_remote_monitor_to_finish(
        self,
        minimum_experiment_running_time: int,
        polling_interval: int,
        execution_mode: Optional[str] = None,
        experiment_output_dir: Optional[str] = None,
    ) -> None:
        """
        Wait for remote monitor process to finish.

        Args:
            minimum_experiment_running_time: Minimum time to wait before polling
            polling_interval: Interval between polling checks
            execution_mode: If set, only match this ``--execution_mode``
            experiment_output_dir: Optional path to further disambiguate the process
        """
        print(
            "Waiting for {} seconds for remote monitor to finish".format(
                minimum_experiment_running_time
            )
        )
        time.sleep(minimum_experiment_running_time)
        print("Done waiting for remote monitor to finish. Will start polling")

        while self.is_remote_monitor_running(
            execution_mode=execution_mode,
            experiment_output_dir=experiment_output_dir,
        ):
            print(
                "Remote monitor is still running. Will check again in {} seconds".format(
                    polling_interval
                )
            )
            time.sleep(polling_interval)

    def is_healthy(self) -> bool:
        """
        Check if remote monitor service is healthy.

        Returns:
            True if remote monitor processes are manageable
        """
        return True
