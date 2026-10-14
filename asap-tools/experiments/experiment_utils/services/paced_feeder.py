"""
Paced feeder service: pushes a sketch-bench workload table's metrics in real
time over Prometheus remote write, one feeder process per (sink, metric).

Unlike the fake exporters, nothing is scraped: the feeder writes straight to
ASAPQuery's ingest and/or Prometheus' remote-write receiver. Every process gets
the same seed and table, so each system receives identical data.
"""

import functools
import json
import math
import os
import time
from typing import Any, Dict, List, Tuple

import constants
from .base import BaseService
from .fake_exporters import get_exporter_target_nodes
from experiment_utils import sync
from experiment_utils.providers.base import InfrastructureProvider

PACED_FEEDER_EXPORTER = "paced_feeder"
PACED_FEEDER_REQUIRED_KEYS = ("workload_table", "metrics", "seed", "duration_secs")
# Metric types Prometheus' remote-write receiver accepts.
PROMETHEUS_METRIC_TYPES = ("gauge", "counter")
# Streams of records, sent as one sample per value field (`http.latency`).
# Many records share a series and timestamp, so only ASAPQuery takes them.
RECORDS_METRIC_TYPE = "records"

PACED_FEEDER_BINARY = "./target/release/paced_feeder"
# The brackets keep pgrep/pkill from matching the shell that runs them.
PACED_FEEDER_PROCESS_PATTERN = "[t]arget/release/paced_feeder"
# Feeders are launched over SSH, one node after another, and all start at the
# first aligned time at least this long after the launches begin: a base plus,
# per node, enough for one SSH retry (run_cmd_with_retry waits 5 s).
PACED_FEEDER_START_LEAD_SECS = 5
PACED_FEEDER_LAUNCH_SECS_PER_NODE = 10
# A feeder stops after its first tick if every request fails (e.g. a wrong
# URL), so check the processes one tick after the shared start.
PACED_FEEDER_STARTUP_CHECK_SECS = 2
# Feeders wait for the next multiple of the aligned intervals' LCM before
# sending; refuse intervals that would make that wait longer than this.
PACED_FEEDER_MAX_ALIGN_SECS = 600
# ASAPQuery's default allowed lateness: a window closes this long after its
# end, so an aligned query must not be sent sooner.
QUERY_ENGINE_ALLOWED_LATENESS_SECS = 5

SINK_QUERY_ENGINE = "query_engine"
SINK_PROMETHEUS = "prometheus"


def paced_feeder_metric_names(experiment_params) -> List[str]:
    """Metrics the paced feeder sends, per its own config block."""
    exporter_list = experiment_params.get("exporters", {}).get("exporter_list", {})
    feeder = exporter_list.get(PACED_FEEDER_EXPORTER)
    return list(feeder["metrics"]) if feeder is not None else []


def aligned_start_ms(earliest_ms: int, align_secs: int) -> int:
    """The first multiple of `align_secs` at or after `earliest_ms`: every
    mode's data starts on the same boundary of each aligned query group's
    interval."""
    align_ms = align_secs * 1000
    return -(-earliest_ms // align_ms) * align_ms


def aligned_interval_lcm(client_config: Dict[str, Any]) -> int:
    """LCM, in seconds, of the repeat intervals of the client config's
    `align_to_interval` groups (1 if there are none), after checking that
    those groups can be aligned."""
    intervals = []
    for group in client_config["query_groups"]:
        options = group.get("client_options", {})
        if not options.get("align_to_interval"):
            continue
        interval_ms = group["repetition_delay_ms"]
        if interval_ms <= 0 or interval_ms % 1000 != 0:
            raise ValueError(
                f"query group {group['id']}: align_to_interval needs a "
                f"whole-second repetition_delay_ms, got {interval_ms}"
            )
        offset = options.get("query_time_offset", 0)
        if offset < QUERY_ENGINE_ALLOWED_LATENESS_SECS:
            raise ValueError(
                f"query group {group['id']}: align_to_interval needs "
                f"query_time_offset >= {QUERY_ENGINE_ALLOWED_LATENESS_SECS} "
                f"(ASAPQuery's allowed lateness), got {offset}"
            )
        intervals.append(interval_ms // 1000)
    # math.lcm needs Python 3.9.
    lcm = functools.reduce(lambda a, b: a * b // math.gcd(a, b), intervals, 1)
    if lcm > PACED_FEEDER_MAX_ALIGN_SECS:
        raise ValueError(
            f"aligned repeat intervals {sorted(set(intervals))} s have an LCM of "
            f"{lcm} s; the feeder would wait up to that long to start "
            f"(limit {PACED_FEEDER_MAX_ALIGN_SECS} s)"
        )
    return lcm


def set_data_start(client_config: Dict[str, Any], start_ms: int) -> None:
    """Tell the client's aligned groups when the data starts (see the
    client's query_time.py)."""
    for group in client_config["query_groups"]:
        options = group.get("client_options", {})
        if options.get("align_to_interval"):
            options["data_start_time"] = start_ms // 1000


def feeds_prometheus(mode: Dict[str, Any]) -> bool:
    """Whether the feeder sends to Prometheus in this experiment mode."""
    return mode["mode"] != constants.SKETCHDB_EXPERIMENT_NAME or mode.get(
        "query_prometheus_too", False
    )


def paced_feeder_min_duration_secs(
    steady_state_wait: float, query_running_secs: float
) -> float:
    """Seconds of data that last until the final query: queries end at most
    startup check + steady_state_wait + the query groups' running time after
    the data starts. Aligned groups need less (their last evaluation is at
    most the starting delay + running time into the data)."""
    return PACED_FEEDER_STARTUP_CHECK_SECS + steady_state_wait + query_running_secs


def table_metric_types(workload_table: str) -> Dict[str, str]:
    """Metric name -> type in the table's `workloads[0].e2e.metrics`."""
    with open(workload_table) as f:
        (workload,) = json.load(f)["workloads"]
    return {name: m["type"] for name, m in workload["e2e"]["metrics"].items()}


def check_paced_feeder_config(
    config: Dict[str, Any],
    modes: List[Dict[str, Any]],
    parallelism: int,
    min_feed_secs: float,
    forward_unsupported_queries: bool,
    auto_discover_labels: bool,
) -> None:
    """Fail before the experiment starts on a config the feeder would reject,
    one sending records to Prometheus, one that stops sending before the
    queries end, or one whose queries would read a Prometheus without the
    data."""
    prometheus_modes = [m["mode"] for m in modes if feeds_prometheus(m)]
    sketchdb_modes = [
        m for m in modes if m["mode"] == constants.SKETCHDB_EXPERIMENT_NAME
    ]
    if forward_unsupported_queries and not all(map(feeds_prometheus, sketchdb_modes)):
        raise ValueError(
            "sketchdb mode without query_prometheus_too gives Prometheus no "
            "paced_feeder data, so queries ASAPQuery forwards there would come "
            "back empty; set streaming.forward_unsupported_queries=false"
        )
    if auto_discover_labels:
        raise ValueError(
            "controller.auto_discover_labels reads labels from Prometheus before "
            "the paced_feeder starts; list the labels under experiment_params.metrics"
        )
    table_metrics = table_metric_types(config["workload_table"])
    for metric in config["metrics"]:
        if metric not in table_metrics:
            raise ValueError(
                f"paced_feeder metric {metric!r} is not in "
                f"{config['workload_table']} (has {sorted(table_metrics)})"
            )
        metric_type = table_metrics[metric]
        if metric_type == RECORDS_METRIC_TYPE:
            if prometheus_modes:
                raise ValueError(
                    f"paced_feeder metric {metric!r} is a records metric, which "
                    f"only ASAPQuery takes, but modes {prometheus_modes} feed "
                    "Prometheus (baseline or query_prometheus_too)"
                )
        elif metric_type not in PROMETHEUS_METRIC_TYPES:
            raise ValueError(
                f"paced_feeder metric {metric!r} has unknown type {metric_type!r}"
            )
    if sketchdb_modes and parallelism != 1:
        raise ValueError(
            "paced_feeder sends to one ingest port; set streaming.parallelism=1"
        )
    if config["duration_secs"] < min_feed_secs:
        raise ValueError(
            f"paced_feeder.duration_secs={config['duration_secs']} ends before the "
            f"queries do: needs at least {min_feed_secs} "
            "(feeder startup + flow.steady_state_wait + the query groups' "
            "running time)"
        )


def paced_feeder_sinks(
    mode: Dict[str, Any], query_engine_url: str, prometheus_url: str
) -> Dict[str, str]:
    """Sink name -> remote-write URL for one experiment mode.

    sketchdb feeds ASAPQuery's ingest, plus Prometheus when the mode also
    queries it; every other mode queries Prometheus only.
    """
    sinks = {}
    if mode["mode"] == constants.SKETCHDB_EXPERIMENT_NAME:
        sinks[SINK_QUERY_ENGINE] = query_engine_url
    if feeds_prometheus(mode):
        sinks[SINK_PROMETHEUS] = prometheus_url
    return sinks


def paced_feeder_command(
    config: Dict[str, Any],
    workload_table: str,
    url: str,
    metric: str,
    metric_type: str,
    start_ms: int,
    stats_out: str,
) -> str:
    cmd = (
        f"{PACED_FEEDER_BINARY} --sink remote-write --url {url}"
        f" --workload-table {workload_table} --metric {metric}"
        f" --seed {config['seed']} --duration-secs {config['duration_secs']}"
        f" --start-ms {start_ms} --stats-out {stats_out}"
    )
    # The feeder refuses the other kind's rate flag.
    if metric_type == RECORDS_METRIC_TYPE:
        if config.get("records_per_sec") is not None:
            cmd += f" --records-per-sec {config['records_per_sec']}"
    elif config.get("samples_per_sec") is not None:
        cmd += f" --samples-per-sec {config['samples_per_sec']}"
    if config.get("extra_flags"):
        cmd += f" {config['extra_flags']}"
    return cmd


class PacedFeederService(BaseService):
    """Runs paced_feeder processes bare-metal, spread over the worker nodes
    (the coordinator in local mode)."""

    def __init__(self, provider: InfrastructureProvider, args):
        super().__init__(provider)
        self.target_nodes = get_exporter_target_nodes(args)
        self.cmd_dir = os.path.join(
            provider.get_home_dir(),
            "code",
            "asap-tools",
            "data-sources",
            "paced-feeder",
        )

    def start(
        self,
        config: Dict[str, Any],
        sinks: Dict[str, str],
        experiment_output_dir: str,
        local_experiment_dir: str,
        align_secs: int = 1,
        **kwargs,
    ) -> int:
        """Launch the feeders; returns when their data starts, in epoch ms,
        once they are running."""
        output_dir = os.path.join(experiment_output_dir, "paced_feeder_output")
        metric_types = table_metric_types(config["workload_table"])
        jobs = [(sink, metric) for sink in sinks for metric in config["metrics"]]
        job_nodes = [
            self.target_nodes[i % len(self.target_nodes)] for i in range(len(jobs))
        ]
        used_nodes = sorted(set(job_nodes))

        for node_idx in used_nodes:
            self.provider.execute_command(
                node_idx=node_idx,
                cmd=f"test -x {PACED_FEEDER_BINARY}",
                cmd_dir=self.cmd_dir,
                nohup=False,
                popen=False,
                ignore_errors=False,
            )
            # Same path on every node; also creates output_dir.
            workload_table = sync.rsync_dataset_file(
                self.provider, config["workload_table"], output_dir, node_idx
            )

        # One shared start for every process of this mode, taken after the SSH
        # prep above so the lead covers only the launches.
        lead_secs = (
            PACED_FEEDER_START_LEAD_SECS
            + PACED_FEEDER_LAUNCH_SECS_PER_NODE * len(used_nodes)
        )
        start_ms = aligned_start_ms(
            (math.ceil(time.time()) + lead_secs) * 1000, align_secs
        )
        node_cmds: List[Tuple[int, str]] = []
        for node_idx, (sink, metric) in zip(job_nodes, jobs):
            name = f"{sink}_{metric}"
            cmd = paced_feeder_command(
                config,
                workload_table,
                sinks[sink],
                metric,
                metric_types[metric],
                start_ms,
                os.path.join(output_dir, f"{name}_stats.json"),
            )
            node_cmds.append(
                (
                    node_idx,
                    f"nohup {cmd} > {os.path.join(output_dir, name)}.out 2>&1 &",
                )
            )

        local_config_dir = os.path.join(local_experiment_dir, "paced_feeder_config")
        os.makedirs(local_config_dir, exist_ok=True)
        with open(os.path.join(local_config_dir, "cmds.sh"), "w") as f:
            f.write("\n".join(cmd for _, cmd in node_cmds) + "\n")

        for node_idx in used_nodes:
            self.provider.execute_command(
                node_idx=node_idx,
                cmd=" ".join(cmd for n, cmd in node_cmds if n == node_idx),
                cmd_dir=self.cmd_dir,
                nohup=False,
                popen=False,
                ignore_errors=False,
            )
        if time.time() * 1000 > start_ms:
            # A feeder launched after the start begins late, so its data
            # would trail the other sinks'.
            self.stop()
            raise RuntimeError(
                f"paced_feeder: launching on {len(used_nodes)} nodes took longer "
                f"than the {lead_secs} s lead"
            )

        time.sleep(start_ms / 1000 - time.time() + PACED_FEEDER_STARTUP_CHECK_SECS)
        for node_idx in used_nodes:
            expected = job_nodes.count(node_idx)
            # No $(...): the SSH command is double-quoted on this side.
            result = self.provider.execute_command(
                node_idx=node_idx,
                cmd=f"pgrep -fc '{PACED_FEEDER_PROCESS_PATTERN}' | grep -qx {expected}",
                cmd_dir=None,
                nohup=False,
                popen=False,
                ignore_errors=True,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"paced_feeder: expected {expected} processes running on "
                    f"node {node_idx}; see the .out files in {output_dir}"
                )
        return start_ms

    def stop(self, **kwargs) -> None:
        self.provider.execute_command_parallel(
            node_idxs=self.target_nodes,
            cmd=f"pkill -f '{PACED_FEEDER_PROCESS_PATTERN}'",
            cmd_dir=None,
            nohup=False,
            popen=True,
            wait=True,
            ignore_errors=True,
        )
