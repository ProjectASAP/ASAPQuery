import os
import json
import time
import argparse
import subprocess
import yaml
import signal
from loguru import logger

from typing import List
from experiment_utils.services.prometheus_client_service import PrometheusClientService
from experiment_utils.providers.cloudlab_local import CloudLabLocalProvider
from classes import process_monitor
from classes.query_cost import CostModelOption
from classes.QueryCostExporter import QueryCostExporterHook
from classes.ProcessMonitorHook import ProcessMonitorHook

import utils
import constants


def create_loggers(logging_dir, log_level):
    logger.remove(None)  # remove default loggers

    logger.add("{}/remote_monitor.log".format(logging_dir), filter="__main__")

    logger.add(  # add cost exporter logger
        "{}/query_cost_exporter.log".format(logging_dir),
        filter=lambda record: record["extra"].get("module") == "query_cost_exporter",
        level=log_level,
        enqueue=True,
    )


def get_pids(keyword) -> List[int]:
    # TODO: In the future, we should probably have a separate cmd line argument for Docker container keywords vs process keywords
    # First try to find Docker container by name/keyword
    docker_cmd = f"docker inspect --format='{{{{.State.Pid}}}}' {keyword} 2>/dev/null"
    result = subprocess.run(
        docker_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    if result.returncode == 0 and result.stdout.decode().strip():
        # Found Docker container, return its PID
        container_pid = result.stdout.decode().strip()
        if container_pid != "0":  # 0 means container is not running
            print(f"Found Docker container '{keyword}' with PID: {container_pid}")
            return [int(container_pid)]

    # Fallback to original process search for bare metal
    # cmd = f"ps aux | grep {keyword} | grep -v grep | awk '{{print $2}}'"
    # print(os.getpid())
    # cmd = "pgrep -f {} | grep -v {}".format(keyword, os.getpid())
    cmd = "ps aux | grep -v remote_monitor.py | grep -E \"{}\" | grep -v grep | awk '{{print $2}}'".format(
        keyword
    )
    # print("My PID:", os.getpid())
    print(cmd)
    # result = subprocess.run(['pgrep', '-f', keyword], stdout=subprocess.PIPE)
    result = subprocess.run(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    pids = result.stdout.decode().strip().split("\n")
    print(pids)
    pids = [pid for pid in pids if pid]
    pid_names = []
    for pid in pids:
        try:
            with open(f"/proc/{pid}/cmdline", "r") as f:
                cmdline = f.read().replace("\x00", " ").strip()
                pid_names.append((pid, cmdline))
        except FileNotFoundError:
            pid_names.append((pid, "Process not found"))
    print(pid_names)
    if len(pids) == 0:
        raise ValueError(f"No processes found for keyword {keyword}")
    return [int(pid) for pid in pids]


def start_profiling_flink_pids(flink_pids):
    asprof_bin = "/scratch/sketch_db_for_prometheus/asprof/bin/asprof"
    cmd = ";".join(["{} start {}".format(asprof_bin, pid) for pid in flink_pids])
    logger.debug("Starting profiling for flink pids with command: {}".format(cmd))
    utils.run_cmd(cmd, popen=False, ignore_errors=False)


def stop_profiling_flink_pids(flink_pids, experiment_output_dir, store: bool):
    asprof_bin = "/scratch/sketch_db_for_prometheus/asprof/bin/asprof"
    flink_profiles_dir = os.path.join(experiment_output_dir, "flink_profiles")
    os.makedirs(flink_profiles_dir, exist_ok=True)

    cmds = []
    for pid in flink_pids:
        if not store:
            cmd = "{} stop {} > /dev/null 2>&1".format(asprof_bin, pid)
            cmds.append(cmd)
        else:
            for format in ["flamegraph", "tree", "flat"]:
                cmd = "{} stop -o {} -f {} {}".format(
                    asprof_bin,
                    format,
                    os.path.join(flink_profiles_dir, "{}.{}".format(pid, format)),
                    pid,
                )
                cmds.append(cmd)

    logger.debug("Stopping profiling for flink pids with command: {}".format(cmds))
    utils.run_cmd(";".join(cmds), popen=False, ignore_errors=not store)


def start_profiling_arroyo_pids(arroyo_pids, experiment_output_dir):
    arroyo_flamegraph_pids = []

    arroyo_profiles_dir = os.path.join(experiment_output_dir, "arroyo_profiles")
    os.makedirs(arroyo_profiles_dir, exist_ok=True)

    flamegraph_bin = os.path.expanduser("~/.cargo/bin/flamegraph")

    for pid in arroyo_pids:
        output_file = os.path.join(
            arroyo_profiles_dir, "arroyo_worker_{}.svg".format(pid)
        )
        cmd = "{} -o {} --pid {} --no-inline".format(flamegraph_bin, output_file, pid)
        logger.debug("Starting flamegraph for PID {} with command: {}".format(pid, cmd))
        proc = subprocess.Popen(cmd, shell=True)
        arroyo_flamegraph_pids.append(proc.pid)

    logger.debug(
        "Started flamegraph processes with PIDs: {}".format(arroyo_flamegraph_pids)
    )
    return arroyo_flamegraph_pids


def stop_profiling_arroyo_pids(
    arroyo_flamegraph_pids, experiment_output_dir, store: bool
):
    if not store:
        for flamegraph_pid in arroyo_flamegraph_pids:
            try:
                os.kill(flamegraph_pid, signal.SIGTERM)
                logger.debug("Killed flamegraph process PID: {}".format(flamegraph_pid))
            except ProcessLookupError:
                logger.debug(
                    "Flamegraph process PID {} already terminated".format(
                        flamegraph_pid
                    )
                )
    else:
        for flamegraph_pid in arroyo_flamegraph_pids:
            try:
                os.kill(flamegraph_pid, signal.SIGTERM)
                logger.debug(
                    "Stopped flamegraph process PID: {}".format(flamegraph_pid)
                )
            except ProcessLookupError:
                logger.debug(
                    "Flamegraph process PID {} already terminated".format(
                        flamegraph_pid
                    )
                )

    logger.debug("Stopped profiling for arroyo pids")


# TODO Provide some way of specifying which hooks will be used
def get_process_monitor_hooks(
    export_cost: bool, provider, node_offset: int
) -> List[ProcessMonitorHook]:
    hooks = []
    # TODO Ideally the cost exporter should be configured by either the experiment
    #      config yaml or from the command line
    if export_cost:
        logger.debug("Cost exporter hook added to process monitor")
        monitors_and_models = {
            "memory_info": [
                CostModelOption.NO_TRANSFORM,
                CostModelOption.SUM,
                CostModelOption.ARITHMETIC_AVG,
            ],
            "cpu_percent": [
                CostModelOption.NO_TRANSFORM,
                CostModelOption.SUM,
                CostModelOption.ARITHMETIC_AVG,
            ],
        }
        cost_exporter_hook = QueryCostExporterHook(
            monitors_and_models, addr=provider.get_node_ip(node_offset), port=9151
        )
        hooks.append(cost_exporter_hook)

    return hooks


def check_args(args):
    if args.execution_mode == "timed" and not args.time_to_run:
        raise ValueError(
            "--time_to_run must be specified when execution_mode is 'timed'"
        )
    elif (
        args.execution_mode == "prometheus_client"
        and not args.prometheus_client_output_file
    ):
        raise ValueError(
            "--prometheus_client_output_file must be specified when execution_mode is 'prometheus_client'"
        )


def main(args):
    check_args(args)

    remote_monitor_output_dir = os.path.join(
        args.experiment_output_dir, "remote_monitor_output"
    )
    prometheus_client_output_dir = os.path.join(
        args.experiment_output_dir, "prometheus_client_output"
    )
    os.makedirs(remote_monitor_output_dir, exist_ok=True)
    os.makedirs(prometheus_client_output_dir, exist_ok=True)

    create_loggers(remote_monitor_output_dir, "DEBUG")

    # pid_keyword_map = {}
    pids = []
    keywords_expanded = []
    for keyword in args.keywords:
        keyword_pids = get_pids(keyword)
        pids.extend(keyword_pids)
        keywords_expanded.extend([keyword] * len(keyword_pids))
        # pid_keyword_map[pid] = keyword

    # pids = list(pid_keyword_map.keys())
    if not pids:
        logger.error("No matching processes found.")
        return

    profile_query_engine_pid = None
    if args.profile_query_engine:
        if (
            constants.QUERY_ENGINE_PY_PROCESS_KEYWORD in args.keywords
            or constants.QUERY_ENGINE_PY_CONTAINER_NAME in args.keywords
        ):
            query_engine_pids = get_pids(constants.QUERY_ENGINE_PY_PROCESS_KEYWORD)
            profile_query_engine_pid = query_engine_pids[
                0
            ]  # Take first PID for profiling
        elif (
            constants.QUERY_ENGINE_RS_PROCESS_KEYWORD in args.keywords
            or constants.QUERY_ENGINE_RS_CONTAINER_NAME in args.keywords
        ):
            raise NotImplementedError(
                "Profiling for Rust query engine is not implemented yet"
            )

    logger.debug("Starting process monitors")

    with open(args.config_file) as config_f:
        client_config = yaml.safe_load(config_f)

    export_cost_and_latency = False
    if (
        "export_cost_and_latency" in client_config
        and client_config["export_cost_and_latency"]
    ):
        export_cost_and_latency = True

    # Create provider for getting network IPs (use CloudLab IPs for Prometheus to scrape)
    ip_provider = CloudLabLocalProvider(username="user", use_cloudlab_ips=True)
    monitor_hooks = get_process_monitor_hooks(
        export_cost=export_cost_and_latency,
        provider=ip_provider,
        node_offset=args.node_offset,
    )

    monitor, control_pipe, monitor_pipe = process_monitor.start_monitor(
        pids,
        keywords_expanded,
        1,
        ["memory_info", "cpu_percent"],
        include_children=True,
        hooks=monitor_hooks,
    )

    if args.profile_flink_pids:
        logger.debug("Starting profiling for flink pids")
        logger.debug("Checking if profilers are already running. If so, stopping them.")
        stop_profiling_flink_pids(
            args.profile_flink_pids, args.experiment_output_dir, store=False
        )
        start_profiling_flink_pids(args.profile_flink_pids)

    arroyo_flamegraph_pids = None
    if args.profile_arroyo_pids:
        logger.debug("Starting profiling for arroyo pids")
        logger.debug("Checking if profilers are already running. If so, stopping them.")
        stop_profiling_arroyo_pids([], args.experiment_output_dir, store=False)
        arroyo_flamegraph_pids = start_profiling_arroyo_pids(
            args.profile_arroyo_pids, args.experiment_output_dir
        )

    if args.execution_mode == "prometheus_client":
        logger.debug("Starting prometheus client")
        # Create CloudLab local provider for local execution with CloudLab paths
        provider = CloudLabLocalProvider(username="user", use_cloudlab_ips=False)
        prometheus_client_service = PrometheusClientService(
            provider,
            use_container=args.use_container_prometheus_client,
            node_offset=args.node_offset,
        )
        prometheus_client_service.start(
            args.experiment_mode,
            args.config_file,
            args.query_engine_config_file,
            prometheus_client_output_dir,
            args.prometheus_client_output_file,
            export_cost_and_latency,
            profile_query_engine_pid,
            args.profile_prometheus_time,
            args.prometheus_client_parallel,
        )

        if prometheus_client_service.use_container:
            while prometheus_client_service.is_healthy():
                logger.debug(
                    "Waiting for prometheus client container to stop running..."
                )
                time.sleep(5)
            prometheus_client_service.stop()

        logger.debug("Finished prometheus client")

    elif args.execution_mode == "interactive":
        logger.debug("Waiting for user input to stop monitoring")
        input("Press Enter to stop monitoring...")
    elif args.execution_mode == "timed":
        logger.debug(f"Running for {args.time_to_run} seconds")
        time.sleep(args.time_to_run)

    if args.profile_flink_pids:
        logger.debug("Stopping profiling for flink pids")
        stop_profiling_flink_pids(
            args.profile_flink_pids, args.experiment_output_dir, store=True
        )

    if args.profile_arroyo_pids and arroyo_flamegraph_pids:
        logger.debug("Stopping profiling for arroyo pids")
        stop_profiling_arroyo_pids(
            arroyo_flamegraph_pids, args.experiment_output_dir, store=True
        )

    logger.debug("Stopping process monitors")
    monitor_info = process_monitor.stop_monitor(monitor, control_pipe, monitor_pipe)

    monitor_output_file = os.path.join(
        remote_monitor_output_dir, args.monitor_output_file
    )
    with open(monitor_output_file, "w") as f:
        json.dump(monitor_info, f)

    logger.debug("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--execution_mode",
        type=str,
        required=True,
        choices=["interactive", "timed", "prometheus_client"],
        help="Execution mode: interactive, timed, or prometheus_client",
    )
    parser.add_argument("--experiment_mode", type=str, required=True)
    parser.add_argument(
        "--keywords",
        type=str,
        required=True,
        help="List of comma separated keywords to search for processes",
    )
    parser.add_argument(
        "--experiment_output_dir",
        type=str,
        required=True,
        help="File to store monitor info",
    )
    parser.add_argument(
        "--config_file",
        type=str,
        required=True,
        help="File containing prometheus client configuration",
    )
    parser.add_argument(
        "--query_engine_config_file",
        type=str,
        required=False,
        help="File containing query engine configuration",
    )
    parser.add_argument(
        "--monitor_output_file",
        type=str,
        required=True,
        help="File to store monitor output",
    )
    parser.add_argument(
        "--prometheus_client_output_file",
        type=str,
        required=False,
        help="File to store prometheus client output",
    )
    parser.add_argument("--profile_query_engine", action="store_true")
    parser.add_argument("--profile_prometheus_time", type=int, required=False)
    parser.add_argument("--profile_flink_pids", type=str, required=False)
    parser.add_argument("--profile_arroyo_pids", type=str, required=False)
    parser.add_argument("--time_to_run", type=int, required=False)
    parser.add_argument(
        "--use_container_prometheus_client",
        action="store_true",
        help="Use containerized Prometheus client",
    )
    parser.add_argument(
        "--prometheus_client_parallel",
        action="store_true",
        help="Enable parallel execution in Prometheus client",
    )
    parser.add_argument(
        "--node_offset",
        type=int,
        required=True,
    )
    args = parser.parse_args()
    args.keywords = args.keywords.strip().split(",")
    if args.profile_flink_pids:
        args.profile_flink_pids = [
            int(pid) for pid in args.profile_flink_pids.split(",")
        ]
    if args.profile_arroyo_pids:
        args.profile_arroyo_pids = [
            int(pid) for pid in args.profile_arroyo_pids.split(",")
        ]
    main(args)
