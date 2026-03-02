"""
Configuration validation and management utilities for experiments.
Contains functions for validating configs, generating controller configs, etc.
"""

import os
import copy
import yaml
from typing import List, Tuple

from omegaconf import DictConfig, OmegaConf

import constants


def validate_basic_config(
    cfg: DictConfig,
    required_params: List[Tuple[str, str]],
    script_name: str = "experiment",
):
    """
    Validate basic configuration parameters that must be provided via command line.

    Args:
        cfg: The configuration object to validate
        required_params: List of (param_path, description) tuples for required parameters
        script_name: Name of the script for error messages
    """
    missing_params = []
    for param_path, description in required_params:
        try:
            value = OmegaConf.select(cfg, param_path)
            if value is None or (isinstance(value, str) and value == "???"):
                missing_params.append((param_path, description))
        except Exception:
            missing_params.append((param_path, description))

    if missing_params:
        error_msg = "Required parameters must be provided via command line:\n\n"
        for param_path, description in missing_params:
            error_msg += f"  {param_path}: {description}\n"

        error_msg += "\nExample usage:\n"
        error_msg += f"python {script_name}.py \\\n"
        for param_path, _ in required_params[:4]:  # Show first 4 params as example
            if "experiment.name" in param_path:
                error_msg += f"  {param_path}=my_test \\\n"
            elif "cloudlab.num_nodes" in param_path:
                error_msg += f"  {param_path}=4 \\\n"
            elif "cloudlab.username" in param_path:
                error_msg += f"  {param_path}=myuser \\\n"
            elif "cloudlab.hostname_suffix" in param_path:
                error_msg += f"  {param_path}=myexp.cloudlab.us\n"

        raise ValueError(error_msg)


def validate_experiment_config(
    experiment_params: DictConfig, require_queries: bool = True
):
    """
    Validate the loaded experiment configuration structure.

    Args:
        experiment_params: The experiment parameters configuration
        require_queries: Whether to require query_groups to be non-empty (default: True)
    """
    # Check for skip_querying mode
    skip_querying = experiment_params.get("skip_querying", False)

    if skip_querying:
        # Require experiment_duration
        if not hasattr(experiment_params, "experiment_duration"):
            raise ValueError(
                "experiment_duration must be specified when skip_querying=True. "
                "Add it to your experiment config or as CLI override: experiment_duration=300"
            )

        # Validate no experiment mode has query_prometheus_too=True
        if hasattr(experiment_params, "experiment") and experiment_params.experiment:
            for mode in experiment_params.experiment:
                if mode.get("query_prometheus_too", False):
                    raise ValueError(
                        "query_prometheus_too must be False when skip_querying=True. "
                        "Cannot query Prometheus when queries are skipped."
                    )

        # Warn if query_groups is present
        if (
            hasattr(experiment_params, "query_groups")
            and experiment_params.query_groups
        ):
            print("-" * 60)
            print("WARNING: query_groups is present but will be IGNORED")
            print("         skip_querying=True means no queries will be executed")
            print("-" * 60)

        # Don't require queries for validation
        require_queries = False

    # Check for required sections
    required_sections = ["query_groups", "exporters", "metrics"]
    missing_sections = []

    for section in required_sections:
        if section not in experiment_params:
            missing_sections.append(section)

    if missing_sections:
        error_msg = f"Missing required sections in experiment config: {', '.join(missing_sections)}\n"
        error_msg += "Example sections that should be present:\n"
        error_msg += "- query_groups: List of query configurations\n"
        error_msg += "- exporters: Exporter configurations\n"
        error_msg += "- metrics: Metric definitions\n"
        raise ValueError(error_msg)

    # Validate query_groups structure (conditionally required)
    if require_queries and len(experiment_params.query_groups) == 0:
        raise ValueError(
            "At least one query group must be defined in experiment config"
        )

    for i, group in enumerate(experiment_params.query_groups):
        if "queries" not in group:
            raise ValueError(f"Query group {i} missing 'queries' field")
        if "client_options" not in group:
            raise ValueError(f"Query group {i} missing 'client_options' field")
        if "starting_delay" not in group.client_options:
            raise ValueError(
                f"Query group {i} missing 'client_options.starting_delay' field"
            )
        if "repetitions" not in group.client_options:
            raise ValueError(
                f"Query group {i} missing 'client_options.repetitions' field"
            )
        if "repetition_delay" not in group:
            raise ValueError(f"Query group {i} missing 'repetition_delay' field")

    # Validate exporters structure
    if "exporter_list" not in experiment_params.exporters:
        raise ValueError("Missing 'exporter_list' in exporters section")

    # Validate metrics structure
    if len(experiment_params.metrics) == 0:
        raise ValueError("At least one metric must be defined in experiment config")

    for i, metric in enumerate(experiment_params.metrics):
        if "metric" not in metric:
            raise ValueError(f"Metric {i} missing 'metric' field")
        if "exporter" not in metric:
            raise ValueError(f"Metric {i} missing 'exporter' field")

    # Cross-validate fake_exporter num_labels with metric labels
    if "fake_exporter" in experiment_params.exporters.exporter_list:
        fake_exporter_config = experiment_params.exporters.exporter_list.fake_exporter
        num_labels_in_config = fake_exporter_config.get("num_labels", 0)

        # Find metrics that use fake_exporter
        for i, metric in enumerate(experiment_params.metrics):
            if metric.exporter == "fake_exporter":
                if "labels" not in metric:
                    raise ValueError(
                        f"Metric {i} ('{metric.metric}') uses fake_exporter but has no 'labels' field"
                    )

                # Count labels excluding 'instance' and 'job'
                metric_labels = metric.labels
                non_system_labels = [
                    label for label in metric_labels if label not in ["instance", "job"]
                ]
                num_labels_in_metric = len(non_system_labels)

                if num_labels_in_metric != num_labels_in_config:
                    raise ValueError(
                        f"Metric {i} ('{metric.metric}'): fake_exporter num_labels mismatch. "
                        f"Exporter config specifies num_labels={num_labels_in_config}, "
                        f"but metric has {num_labels_in_metric} non-system labels {non_system_labels}. "
                        f"The num_labels in fake_exporter config should match the count of labels "
                        f"excluding 'instance' and 'job'."
                    )


def get_minimum_experiment_running_time(experiment_params: DictConfig) -> int:
    """Calculate minimum experiment running time from query groups or experiment_duration."""
    # Check for skip_querying mode
    skip_querying = experiment_params.get("skip_querying", False)

    if skip_querying:
        # Return experiment_duration directly
        if not hasattr(experiment_params, "experiment_duration"):
            raise ValueError(
                "experiment_duration must be specified when skip_querying=True"
            )
        experiment_duration = experiment_params.experiment_duration
        print("Skip querying mode enabled")
        print("Experiment duration:", experiment_duration)
        return experiment_duration

    # Original logic for calculating from query_groups
    query_groups = experiment_params.query_groups
    # if len(query_groups) != 1:
    #    raise ValueError("Only one query group is supported for now")

    experiment_running_time = 0
    for query_group in query_groups:
        query_group_starting_delay = query_group.client_options.starting_delay
        query_group_repetitions = query_group.client_options.repetitions
        query_group_reptition_delay = query_group.repetition_delay

        query_group_running_time = (
            query_group_starting_delay
            + query_group_repetitions * query_group_reptition_delay
        )
        experiment_running_time = max(experiment_running_time, query_group_running_time)

    # print("Starting delay:", starting_delay)
    # print("Repetitions:", repetitions)
    # print("Repetition delay:", reptition_delay)
    print("Total experiment running time:", experiment_running_time)

    return experiment_running_time


def generate_controller_client_configs(
    experiment_params: DictConfig,
    local_experiment_dir: str,
    aggregate_cleanup: DictConfig = None,
    sketch_parameters: DictConfig = None,
) -> Tuple[List[str], List[str]]:
    """Generate controller client configurations from experiment parameters."""
    # experiment_params is already loaded by Hydra
    experiment_config = OmegaConf.to_container(experiment_params, resolve=True)
    assert experiment_config is not None and isinstance(experiment_config, dict)

    # Add aggregate_cleanup configuration if provided
    if aggregate_cleanup is not None:
        cleanup_config = OmegaConf.to_container(aggregate_cleanup, resolve=True)
        experiment_config["aggregate_cleanup"] = cleanup_config

    # Add sketch_parameters configuration if provided
    if sketch_parameters is not None:
        sketch_params_config = OmegaConf.to_container(sketch_parameters, resolve=True)
        experiment_config["sketch_parameters"] = sketch_params_config

    output_dir = os.path.join(local_experiment_dir, "controller_client_configs")
    os.makedirs(output_dir, exist_ok=True)

    servers_config = experiment_config["servers"]
    experiment_modes = experiment_config["experiment"]
    experiment_to_server_config_map = {}

    for server_config in servers_config:
        server_name = server_config["name"]
        experiment_to_server_config_map[server_name] = server_config

    for experiment_mode in experiment_modes:
        controller_client_config = copy.deepcopy(experiment_config)
        del controller_client_config["experiment"]
        if "workloads" in controller_client_config:
            del controller_client_config["workloads"]
        controller_client_config["servers"] = [
            experiment_to_server_config_map[experiment_mode["server"]]
        ]

        if (
            experiment_mode["mode"] == constants.SKETCHDB_EXPERIMENT_NAME
            and "query_prometheus_too" in experiment_mode
            and experiment_mode["query_prometheus_too"]
        ):
            controller_client_config["servers"] = servers_config

        with open(
            os.path.join(output_dir, "{}.yaml".format(experiment_mode["mode"])), "w"
        ) as f:
            yaml.dump(controller_client_config, f)

    metrics_to_remote_write = [
        metric_config["metric"] for metric_config in experiment_config["metrics"]
    ]

    return [e["mode"] for e in experiment_modes], metrics_to_remote_write


def check_exporter_and_queries_exist(
    exporter_name: str, experiment_params: DictConfig
) -> bool:
    """Check if an exporter is configured and queries exist for it."""
    if "exporters" not in experiment_params:
        return False
    exporters_config = experiment_params.exporters
    if "exporter_list" not in exporters_config:
        return False

    if exporter_name not in exporters_config.exporter_list:
        return False

    if "only_start_if_queries_exist" not in experiment_params.exporters:
        flag = False
    else:
        flag = experiment_params.exporters.only_start_if_queries_exist

    if flag is False:
        return True

    if "query_groups" not in experiment_params:
        return False

    if "metrics" not in experiment_params:
        return False

    metric_exporter_names = [
        [metric_config.metric, metric_config.exporter]
        for metric_config in experiment_params.metrics
    ]

    query_groups = experiment_params.query_groups
    for group in query_groups:
        queries = group.queries
        for q in queries:
            for metric in metric_exporter_names:
                if (
                    metric[0] in q
                    and metric[0] + "_" not in q
                    and "_" + metric[0] not in q
                ) and metric[1] == exporter_name:
                    return True

    return False


def read_workloads_config(experiment_params: DictConfig):
    """Read and validate workloads configuration."""
    if "workloads" not in experiment_params:
        return None
    workloads_config = experiment_params.workloads
    if workloads_config is None:
        return None

    if "deathstar" in workloads_config:
        if any(key not in workloads_config.deathstar for key in ["use"]):
            return None

    return workloads_config


def get_prometheus_scrape_interval(prometheus_config):
    """Extract scrape interval from Prometheus configuration."""
    prometheus_scrape_interval_string = prometheus_config.scrape_interval
    # convert to seconds
    if prometheus_scrape_interval_string.endswith("s"):
        prometheus_scrape_interval = int(prometheus_scrape_interval_string[:-1])
    elif prometheus_scrape_interval_string.endswith("m"):
        prometheus_scrape_interval = int(prometheus_scrape_interval_string[:-1]) * 60
    else:
        raise ValueError(
            f"Invalid scrape interval string: {prometheus_scrape_interval_string}"
        )

    return prometheus_scrape_interval


class Args:
    """Helper class to convert Hydra config to argparse-like namespace for backward compatibility."""

    def __init__(self, cfg: DictConfig):
        # Experiment configuration
        self.experiment_name = cfg.experiment.name

        # CloudLab configuration
        self.num_nodes = cfg.cloudlab.num_nodes
        self.node_offset = cfg.cloudlab.node_offset
        self.cloudlab_username = cfg.cloudlab.username
        self.hostname_suffix = cfg.cloudlab.hostname_suffix

        # Logging and debugging
        self.log_level = cfg.logging.level

        # Profiling options
        self.profile_query_engine = cfg.profiling.query_engine
        self.profile_prometheus_time = cfg.profiling.prometheus_time
        self.profile_flink = cfg.profiling.flink
        self.profile_arroyo = cfg.profiling.arroyo

        # Throughput monitoring options
        self.throughput_arroyo = cfg.throughput.arroyo
        self.throughput_prometheus = cfg.throughput.prometheus

        # Health check monitoring options
        self.health_check_prometheus = cfg.health_check.prometheus

        # Manual mode options
        self.manual_query_engine = cfg.manual.query_engine
        self.manual_remote_monitor = cfg.manual.remote_monitor

        # Experiment flow options
        self.no_teardown = cfg.flow.no_teardown
        self.steady_state_wait = cfg.flow.steady_state_wait

        # Streaming engine configuration
        self.streaming_engine = cfg.streaming.engine
        self.parallelism = cfg.streaming.parallelism
        self.flink_input_format = cfg.streaming.flink_input_format
        self.flink_output_format = cfg.streaming.flink_output_format
        self.enable_object_reuse = cfg.streaming.enable_object_reuse
        self.do_local_flink = cfg.streaming.do_local_flink
        self.forward_unsupported_queries = cfg.streaming.forward_unsupported_queries
        self.use_kafka_ingest = cfg.streaming.use_kafka_ingest
        # Remote write configuration
        self.remote_write_ip = cfg.streaming.remote_write.ip
        self.remote_write_base_port = cfg.streaming.remote_write.base_port
        self.remote_write_path = cfg.streaming.remote_write.path

        # Fake exporter language
        self.fake_exporter_language = cfg.fake_exporter_language

        # Query engine language
        self.query_engine_language = cfg.query_engine_language

        # Query language (SQL vs PROMQL) - only used by Rust query engine
        self.query_language = cfg.query_language

        # Query engine options
        self.dump_precomputes = cfg.query_engine.dump_precomputes
        self.lock_strategy = cfg.query_engine.lock_strategy

        # Controller configuration
        self.controller_punting = cfg.controller.punting

        # Aggregate cleanup configuration
        # Valid policies: "circular_buffer", "read_based", "no_cleanup"
        self.cleanup_policy = cfg.aggregate_cleanup.policy

        # Container configuration
        self.use_container_query_engine = cfg.use_container.query_engine
        self.use_container_arroyo = cfg.use_container.arroyo
        self.use_container_controller = cfg.use_container.controller
        self.use_container_fake_exporter = cfg.use_container.fake_exporter
        self.use_container_prometheus_client = cfg.use_container.prometheus_client

        # Prometheus client configuration
        self.prometheus_client_parallel = cfg.prometheus_client.parallel

    def get_node_range(self, include_coordinator: bool = True) -> list:
        """
        Get the range of node indices for this experiment.

        Args:
            include_coordinator: If True, includes node0/coordinator in the range

        Returns:
            List of node indices starting from node_offset

        Example:
            With num_nodes=2 and node_offset=10:
            - get_node_range(True) returns [10, 11, 12] (coordinator + 2 workers)
            - get_node_range(False) returns [11, 12] (2 workers only)
        """
        if include_coordinator:
            return list(range(self.node_offset, self.node_offset + self.num_nodes + 1))
        else:
            return list(
                range(self.node_offset + 1, self.node_offset + self.num_nodes + 1)
            )

    def get_coordinator_node(self) -> int:
        """Get the coordinator node index (first node in the range)."""
        return self.node_offset


def validate_config(cfg: DictConfig, script_name: str = "experiment_run_e2e"):
    """
    Validate configuration parameters and experiment configuration.

    Args:
        cfg: The Hydra configuration object
        script_name: Name of the script for error messages
    """
    # Check for required parameters that must be provided via command line
    required_params = [
        ("experiment.name", "Human-readable experiment name"),
        ("cloudlab.num_nodes", "Number of CloudLab nodes to use"),
        ("cloudlab.username", "Your CloudLab username"),
        ("cloudlab.hostname_suffix", "CloudLab experiment hostname suffix"),
    ]

    # Use the existing validate_basic_config function
    validate_basic_config(cfg, required_params, script_name)

    # Validate no_teardown with experiment modes (if applicable)
    if (
        hasattr(cfg, "flow")
        and hasattr(cfg.flow, "no_teardown")
        and cfg.flow.no_teardown
    ):
        if (
            hasattr(cfg, "experiment_params")
            and hasattr(cfg.experiment_params, "experiment")
            and len(cfg.experiment_params.experiment) > 1
        ):
            raise ValueError(
                "--no_teardown can only be used with a single experiment mode"
            )

    # Validate aggregate cleanup policy
    valid_policies = ["circular_buffer", "read_based", "no_cleanup"]
    if hasattr(cfg, "aggregate_cleanup") and hasattr(cfg.aggregate_cleanup, "policy"):
        policy = cfg.aggregate_cleanup.policy
        if policy not in valid_policies:
            raise ValueError(
                f"Invalid aggregate_cleanup.policy: '{policy}'. "
                f"Valid options: {valid_policies}"
            )

        # Validate Python query engine only supports no_cleanup
        if (
            hasattr(cfg, "query_engine_language")
            and cfg.query_engine_language == "python"
            and policy != "no_cleanup"
        ):
            raise ValueError(
                f"aggregate_cleanup.policy='{policy}' is not supported by the Python query engine. "
                "Either use query_engine_language='rust' or set aggregate_cleanup.policy='no_cleanup'"
            )


def generate_and_copy_prometheus_config(
    num_nodes_in_experiment,
    local_experiment_dir,
    prometheus_config_output_dir,
    experiment_mode,
    cfg,
    prometheus_config,
    node_offset: int,
    sketchdb_experiment_name: str,
    provider=None,
):
    """
    Generate and copy Prometheus configuration for experiment.

    Args:
        num_nodes_in_experiment: Number of nodes in experiment
        local_experiment_dir: Local experiment directory
        prometheus_config_output_dir: Output directory for prometheus config files
        experiment_mode: Experiment mode
        cfg: Configuration object
        prometheus_config: Prometheus configuration
        sketchdb_experiment_name: SketchDB experiment name
        provider: Infrastructure provider
        node_offset: Starting node index offset
    """
    # Import here to avoid circular imports
    import experiment_utils

    # Get IP information from provider
    if provider is None:
        raise ValueError("provider parameter is required for IP configuration")

    prometheus_client_ip = provider.get_node_ip(node_offset)
    # Extract IP prefix from first node (e.g., "10.10.1.1" -> "10.10.1")
    node_ip_prefix = ".".join(prometheus_client_ip.split(".")[:-1])

    args = experiment_utils.GeneratePrometheusArgs(
        num_nodes_in_experiment,
        local_experiment_dir,
        prometheus_config_output_dir,
        prometheus_config,
        prometheus_client_ip,
        node_ip_prefix,
        node_offset,
    )

    experiment_utils.call_generate_prometheus_config(
        args, cfg, experiment_mode, sketchdb_experiment_name
    )
