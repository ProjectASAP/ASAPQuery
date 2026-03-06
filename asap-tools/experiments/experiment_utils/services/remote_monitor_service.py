"""
Remote monitor service management for experiments.
"""

import os
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
        arroyo_service: "ArroyoService",
        controller_remote_output_dir: str,
        use_container_prometheus_client: bool,
        prometheus_client_parallel: bool,
        monitoring_tool: str,
        timed_duration: Optional[int] = None,
    ) -> None:
        """
        Start remote monitor processes.

        Args:
            **kwargs: Additional configuration (currently unused)
            timed_duration: If provided, use timed mode instead of prometheus_client mode
            monitoring_tool: Monitoring tool being used ("prometheus" or "victoriametrics")
        """
        # Determine execution mode
        use_timed_mode = timed_duration is not None

        # Determine which config file to look for based on monitoring tool
        if monitoring_tool == "victoriametrics":
            # first one is for vmagent
            # second one is for vmsingle
            # TODO: remove this hardcoding and instead query the service to get this
            config_keywords = [
                constants.VMAGENT_SCRAPE_CONFIG_FILE,
                "victoriametrics-single",
            ]
        else:
            config_keywords = [constants.PROMETHEUS_CONFIG_FILE]

        if use_timed_mode:
            # Build command for timed mode (skip_querying)
            keywords = config_keywords

            if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
                if query_engine_service is not None:
                    keywords.append(query_engine_service.get_monitoring_keyword())
                else:
                    keywords.append(constants.QUERY_ENGINE_PROCESS_KEYWORD)

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

        keywords = config_keywords

        if experiment_mode == constants.SKETCHDB_EXPERIMENT_NAME:
            if query_engine_service is not None:
                keywords.append(query_engine_service.get_monitoring_keyword())
            else:
                keywords.append(constants.QUERY_ENGINE_PROCESS_KEYWORD)

            if streaming_engine == "flink":
                keywords.append("sketch-0.1.jar")  # flinksketch jar
                if not do_local_flink:
                    keywords.append(
                        "org.apache.flink.runtime.taskexecutor.TaskManagerRunner"
                    )
            elif streaming_engine == "arroyo":
                if arroyo_service is not None:
                    keywords.append(arroyo_service.get_monitoring_keyword())
                else:
                    keywords.append("arroyo.*worker")

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
        )

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

    def stop(self, **kwargs) -> None:
        """
        Stop remote monitor processes.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        self.kill_remote_monitor()

    def kill_remote_monitor(self) -> None:
        """Kill remote monitor processes."""
        cmd = "pkill -f remote_monitor.py"
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
        polling_interval: int = 10,
    ) -> None:
        """
        Wait for remote monitor process to finish.

        Args:
            minimum_experiment_running_time: Minimum time to wait before polling
            polling_interval: Interval between polling checks
        """
        print(
            "Waiting for {} seconds for remote monitor to finish".format(
                minimum_experiment_running_time
            )
        )
        time.sleep(minimum_experiment_running_time)
        print("Done waiting for remote monitor to finish. Will start polling")

        while True:
            cmd = "pgrep -f remote_monitor.py"
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
                break
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
