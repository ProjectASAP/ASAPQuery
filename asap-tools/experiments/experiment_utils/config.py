"""
Configuration validation and management utilities for experiments.
Contains functions for validating configs, generating controller configs, etc.
"""

import os
import copy
import yaml
from typing import Any, Dict, List, Tuple

from omegaconf import DictConfig, ListConfig, OmegaConf

import constants
from experiment_utils.providers.factory import create_provider


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
                error_msg += f"  {param_path}=myexp.cloudlab.us \\\n"

        raise ValueError(error_msg)


def get_node_params(cfg: DictConfig) -> Tuple[int, int]:
    """Return (num_nodes, node_offset). Local mode is a single node and never
    touches cfg.providers.cloudlab (whose username/hostname_suffix are mandatory-missing
    markers that raise on mere attribute access if not provided)."""
    if hasattr(cfg.providers, "local"):
        return 0, 0
    return cfg.providers.cloudlab.num_nodes, cfg.providers.cloudlab.node_offset


def required_cloudlab_params(cfg: DictConfig) -> List[Tuple[str, str]]:
    """Return providers.cloudlab.* required-param tuples, or [] when running in local mode."""
    if hasattr(cfg.providers, "local"):
        return []
    return [
        ("providers.cloudlab.num_nodes", "Number of CloudLab nodes to use"),
        ("providers.cloudlab.username", "Your CloudLab username"),
        ("providers.cloudlab.hostname_suffix", "CloudLab experiment hostname suffix"),
    ]


def _is_clickhouse_experiment(experiment_params: DictConfig) -> bool:
    """Return True if experiment_params describes a ClickHouse (SQL) experiment."""
    return "dataset" in experiment_params


def _resolve_sql_file(sql_file: Any, mode: str, idx: int, group_name: str) -> str:
    """Resolve the sql_file path for one query group and mode.

    ``sql_file`` is a dict keyed by mode name (e.g.
    baseline/baseline_sketch/baseline_mv/...). sketchdb always resolves
    through baseline's entry — ASAP's SQL-mode query engine must see the
    exact same SQL text as baseline to pattern-match it against available
    sketches, so it never gets its own dict entry.
    """
    lookup_mode = (
        constants.BASELINE_EXPERIMENT_NAME
        if mode == constants.SKETCHDB_EXPERIMENT_NAME
        else mode
    )
    path = sql_file.get(lookup_mode)
    if not path:
        raise ValueError(
            f"Query group {idx} ({group_name!r}) missing sql_file entry for "
            f"mode {lookup_mode!r}"
        )
    return path


def _validate_clickhouse_experiment_config(experiment_params: DictConfig) -> None:
    """Validate experiment_params for a ClickHouse experiment."""
    # Validate dataset section
    if "dataset" not in experiment_params:
        raise ValueError(
            "ClickHouse experiments require a 'dataset' section in experiment config. "
            "Add dataset.name and dataset.local_data_file."
        )
    dataset = experiment_params.dataset
    valid_dataset_names = {"clickbench", "h2o", "custom"}
    dataset_name = dataset.get("name")
    if not dataset_name or dataset_name == "???":
        raise ValueError(
            "dataset.name is required. " f"Valid choices: {valid_dataset_names}"
        )
    if dataset_name not in valid_dataset_names:
        raise ValueError(
            f"dataset.name={dataset_name!r} is not valid. "
            f"Valid choices: {valid_dataset_names}"
        )

    local_data_file = dataset.get("local_data_file")
    if not local_data_file or local_data_file == "???":
        raise ValueError(
            "dataset.local_data_file is required. "
            "Provide the path to the JSON-lines data file on this machine."
        )
    if not os.path.exists(local_data_file):
        raise ValueError(
            f"dataset.local_data_file={local_data_file!r} does not exist. "
            "Run benchmark/prepare_data.py first to produce the JSON-lines file."
        )

    # Validate query_groups
    if "query_groups" not in experiment_params or not experiment_params.query_groups:
        raise ValueError(
            "At least one query group must be defined in experiment config."
        )
    if not experiment_params.get("experiment"):
        raise ValueError(
            "At least one mode must be defined in experiment_params.experiment."
        )
    experiment_modes = {m["mode"] for m in experiment_params.experiment}
    experiment_modes.discard(constants.SKETCHDB_EXPERIMENT_NAME)

    for i, group in enumerate(experiment_params.query_groups):
        sql_file = group.get("sql_file")
        if not sql_file or sql_file == "???":
            raise ValueError(
                f"Query group {i} missing 'sql_file'. "
                "Generate SQL files with benchmark/generate_queries.py first."
            )
        group_name = group.get("name", str(i))
        for mode in experiment_modes:
            path = _resolve_sql_file(sql_file, mode, i, group_name)
            if not os.path.exists(path):
                raise ValueError(
                    f"Query group {i} sql_file for mode {mode!r} "
                    f"({path!r}) does not exist."
                )


def validate_experiment_config(
    experiment_params: DictConfig, require_queries: bool = True
):
    """
    Validate the loaded experiment configuration structure.

    Args:
        experiment_params: The experiment parameters configuration
        require_queries: Whether to require query_groups to be non-empty (default: True)
    """
    # ClickHouse experiments have a different required structure
    if _is_clickhouse_experiment(experiment_params):
        _validate_clickhouse_experiment_config(experiment_params)
        return

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
        if "repetition_delay_ms" not in group:
            raise ValueError(f"Query group {i} missing 'repetition_delay_ms' field")

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
        query_group_reptition_delay = query_group.repetition_delay_ms / 1000

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

    # Fields accepted by ControllerConfig (deny_unknown_fields in asap-planner-rs).
    # Everything else (exporters, monitoring, servers, …) is experiment-only and
    # must not appear in the file passed to the controller binary.
    CONTROLLER_ALLOWED_KEYS = {
        "query_groups",
        "sketch_parameters",
        "aggregate_cleanup",
        "metrics",
        "existing_streaming_config",
    }

    for experiment_mode in experiment_modes:
        full_config = copy.deepcopy(experiment_config)
        del full_config["experiment"]
        if "workloads" in full_config:
            del full_config["workloads"]
        full_config["servers"] = [
            experiment_to_server_config_map[experiment_mode["server"]]
        ]

        if (
            experiment_mode["mode"] == constants.SKETCHDB_EXPERIMENT_NAME
            and "query_prometheus_too" in experiment_mode
            and experiment_mode["query_prometheus_too"]
        ):
            full_config["servers"] = servers_config

        # Full config — used by prometheus_client (needs "servers", etc.)
        with open(
            os.path.join(output_dir, "{}.yaml".format(experiment_mode["mode"])), "w"
        ) as f:
            yaml.dump(full_config, f)

        # Controller-only config — stripped to the fields ControllerConfig accepts.
        controller_only_config = {
            k: v for k, v in full_config.items() if k in CONTROLLER_ALLOWED_KEYS
        }
        with open(
            os.path.join(
                output_dir, "{}_controller_input.yaml".format(experiment_mode["mode"])
            ),
            "w",
        ) as f:
            yaml.dump(controller_only_config, f)

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


def read_sql_queries(cfg: DictConfig) -> List[Tuple[str, str]]:
    """Return list of (name, sql_file_path) pairs from a ClickHouse experiment config.

    Args:
        cfg: Top-level Hydra config (cfg.experiment_params.query_groups is used).

    Returns:
        List of (group_name, sql_file_path) tuples.
    """
    query_groups = cfg.experiment_params.query_groups
    result = []
    for i, group in enumerate(query_groups):
        name = group.get("name", str(i))
        sql_file = group.get("sql_file")
        if not sql_file:
            raise ValueError(f"Query group {i!r} ({name!r}) missing 'sql_file'")
        result.append((name, sql_file))
    return result


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


def get_prometheus_data_ingestion_interval_ms(prometheus_config):
    """Extract scrape interval from Prometheus configuration, returned in milliseconds."""
    s = prometheus_config.scrape_interval
    # ponytail: check ms before s — "100ms".endswith("s") is True and would misroute
    if s.endswith("ms"):
        return int(s[:-2])
    elif s.endswith("s"):
        return int(s[:-1]) * 1000
    elif s.endswith("m"):
        return int(s[:-1]) * 60 * 1000
    else:
        raise ValueError(f"Invalid scrape interval string: {s}")


class Args:
    """Helper class to convert Hydra config to argparse-like namespace for backward compatibility."""

    def __init__(self, cfg: DictConfig):
        # Experiment configuration
        self.experiment_name = cfg.experiment.name

        # CloudLab configuration (or local-mode equivalents)
        self.num_nodes, self.node_offset = get_node_params(cfg)
        if hasattr(cfg.providers, "local"):
            self.cloudlab_username = None
            self.hostname_suffix = None
        else:
            self.cloudlab_username = cfg.providers.cloudlab.username
            self.hostname_suffix = cfg.providers.cloudlab.hostname_suffix

        # Single source of truth for the infrastructure provider: build it here
        # (from cfg alone) so every node-dependent value derived from it —
        # remote_write_ip included — is computed in exactly one place instead
        # of being re-derived (and potentially forgotten) in each script.
        self.provider = create_provider(cfg)

        # Remote write IP (127.0.0.1 for local mode, CloudLab's 10.10.1.x
        # scheme otherwise). Written back onto cfg.streaming.remote_write.ip
        # too, since generate_prometheus_config reads it from cfg directly
        # rather than through this Args object.
        self.remote_write_ip = self.provider.get_node_ip(self.node_offset)
        cfg.streaming.remote_write.ip = self.remote_write_ip

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
        # Remote write configuration (self.remote_write_ip set above, near
        # provider construction)
        self.remote_write_base_port = cfg.streaming.remote_write.base_port
        self.remote_write_path = cfg.streaming.remote_write.path

        # Fake exporter language
        self.fake_exporter_language = cfg.fake_exporter_language

        self.backend = OmegaConf.to_container(cfg.backend, resolve=True)

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

    def to_dict(self) -> Dict[str, Any]:
        """JSON-serializable snapshot of this Args instance.

        `provider` holds a live InfrastructureProvider object, so it's
        swapped for its repr() (e.g. "CloudLabProvider(username='...', ...)")
        rather than dropped.
        """
        return {k: (repr(v) if k == "provider" else v) for k, v in vars(self).items()}


def validate_config(cfg: DictConfig, script_name: str = "experiment_run_e2e"):
    """
    Validate configuration parameters and experiment configuration.

    Args:
        cfg: The Hydra configuration object
        script_name: Name of the script for error messages
    """
    # Enforce exactly one provider (generic: works for any future provider)
    active_providers = list(cfg.providers.keys())
    if len(active_providers) == 0:
        raise ValueError(
            "No provider configured. Uncomment exactly one provider under "
            "'providers:' in config/config.yaml."
        )
    if len(active_providers) > 1:
        raise ValueError(
            f"Multiple providers configured: {active_providers}. "
            "Comment out all but one under 'providers:' in config/config.yaml."
        )

    # Check for required parameters that must be provided via command line
    required_params = [
        ("experiment.name", "Human-readable experiment name"),
    ] + required_cloudlab_params(cfg)

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

    # Profiling the Rust query engine requires bare-metal mode (debug symbols unavailable in container)
    if (
        hasattr(cfg, "profiling")
        and cfg.profiling.get("query_engine", False)
        and hasattr(cfg, "use_container")
        and cfg.use_container.get("query_engine", True)
    ):
        raise ValueError(
            "profiling.query_engine=true requires use_container.query_engine=false. "
            "Container builds discard debug symbols, making flamegraph output unreadable."
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

    # ClickHouse backend requires dataset config in experiment_params
    if (
        hasattr(cfg, "backend")
        and cfg.backend.get("type") == "clickhouse"
        and hasattr(cfg, "experiment_params")
        and "dataset" not in cfg.experiment_params
    ):
        raise ValueError(
            "backend.type=clickhouse requires experiment_params.dataset to be set. "
            "Use experiment_type=clickhouse or add a dataset section to your experiment config."
        )


def _load_sql_queries(sql_file: str) -> List[str]:
    """Read a SQL file and return individual statements, preserving comment lines."""
    with open(sql_file) as f:
        content = f.read()
    return [stmt.strip() for stmt in content.split(";") if stmt.strip()]


def generate_clickhouse_client_configs(
    query_groups: Any,
    local_experiment_dir: str,
    mode_server_urls: Dict[str, str],
    clickhouse_database: str = "default",
    clickhouse_user: str = "default",
    clickhouse_password: str = "",
) -> List[str]:
    """Generate prometheus-client config YAMLs for ClickHouse experiment modes.

    SQL queries are read from the ``sql_file`` paths in each query group and
    inlined into the YAML, so no separate SQL file rsync is required.

    For each mode in ``mode_server_urls`` a file is written to
    ``{local_experiment_dir}/controller_client_configs/{mode}.yaml`` — the same
    directory that ``rsync_controller_client_configs`` already syncs.

    Args:
        query_groups: Iterable of query-group dicts (or DictConfig/ListConfig).
            Each entry must have ``sql_file`` and may have ``client_options``
            (``starting_delay``, ``repetitions``) and ``repetition_delay_ms``.
        local_experiment_dir: Local directory under which
            ``controller_client_configs/`` is created.
        mode_server_urls: Mapping of mode name to ClickHouse server URL, e.g.
            ``{"baseline": "http://localhost:8123"}``.  One YAML file is
            written per entry.
        clickhouse_database: ClickHouse database name (default ``"default"``).
        clickhouse_user: ClickHouse user (default ``"default"``).
        clickhouse_password: ClickHouse password (default ``""``).

    Returns:
        List of mode names for which configs were generated.
    """
    output_dir = os.path.join(local_experiment_dir, "controller_client_configs")
    os.makedirs(output_dir, exist_ok=True)

    # Normalise OmegaConf containers to plain Python structures
    if isinstance(query_groups, (DictConfig, ListConfig)):
        query_groups_list: List[Dict] = OmegaConf.to_container(query_groups, resolve=True)  # type: ignore[assignment]
    else:
        query_groups_list = list(query_groups)

    modes = list(mode_server_urls.keys())
    for mode, url in mode_server_urls.items():
        # Each mode may target a different sql_file (e.g. baseline_mv queries a
        # materialized view, baseline_sketch swaps in a sketch function) — see
        # _resolve_sql_file for the sketchdb-falls-back-to-baseline rule.
        built_groups = []
        for idx, group in enumerate(query_groups_list):
            sql_file = group.get("sql_file")
            if not sql_file:
                name = group.get("name", str(idx))
                raise ValueError(f"Query group {idx!r} ({name!r}) missing 'sql_file'")
            sql_file = _resolve_sql_file(
                sql_file, mode, idx, group.get("name", str(idx))
            )

            queries = _load_sql_queries(sql_file)
            if not queries:
                raise ValueError(f"No SQL statements found in {sql_file!r}")

            client_opts = dict(group.get("client_options") or {})
            client_opts.setdefault("starting_delay", 0)
            client_opts.setdefault("repetitions", 1)

            built_groups.append(
                {
                    "id": idx,
                    "queries": queries,
                    "repetition_delay_ms": group.get("repetition_delay_ms", 0),
                    "client_options": client_opts,
                    "time_window_seconds": group.get("time_window_seconds"),
                }
            )

        config: Dict[str, Any] = {
            "servers": [
                {
                    "name": mode,
                    "url": url,
                    "protocol": "clickhouse",
                    "database": clickhouse_database,
                    "user": clickhouse_user,
                    "password": clickhouse_password,
                }
            ],
            "query_groups": built_groups,
        }
        config_path = os.path.join(output_dir, f"{mode}.yaml")
        with open(config_path, "w") as f:
            yaml.dump(config, f)

    return modes


def generate_sql_planner_input(
    query_groups: Any, dataset_cfg: Any, sketch_parameters: Any = None
) -> str:
    """Generate the YAML input file for asap-planner in SQL mode.

    The planner (``asap-planner --query-language sql``) reads a
    ``SQLControllerConfig`` YAML that contains:
      - ``tables``: schema of the tables being queried
      - ``query_groups``: SQL queries with controller options
      - ``sketch_parameters``: optional per-sketch-type overrides (e.g.
        ``DatasketchesKLL.K``), matching ``ControllerConfig``'s PromQL-mode
        field of the same name (``SketchParameterOverrides`` in
        asap-planner-rs's ``config/input.rs``).

    This function builds that YAML from the experiment config so the runner
    does not need a hand-authored planner input file.

    Args:
        query_groups: ListConfig of query group dicts.
            Each entry must have ``sql_file``, ``repetition_delay_ms``, and
            ``controller_options`` (``accuracy_sla``, ``latency_sla``).
        dataset_cfg: DictConfig with ``table``/``name``, and ``precompute``
            sub-config (``timestamp_col``, ``value_col``, ``label_cols``).
        sketch_parameters: Optional DictConfig/dict mirroring ``config.yaml``'s
            top-level ``sketch_parameters`` section (``CountMinSketch``,
            ``DatasketchesKLL``, etc.). When ``None``, the planner falls back
            to its own defaults.

    Returns:
        YAML string ready to write to disk and pass to asap-planner.
    """
    precompute_cfg = dataset_cfg.precompute
    table_name = str(dataset_cfg.get("table") or dataset_cfg.name)
    value_col = str(precompute_cfg.value_col)
    tables = [
        {
            "name": table_name,
            "time_column": str(precompute_cfg.timestamp_col),
            "value_columns": [value_col],
            "metadata_columns": list(precompute_cfg.label_cols),
        }
    ]

    if isinstance(query_groups, (DictConfig, ListConfig)):
        groups_list = OmegaConf.to_container(query_groups, resolve=True)
    else:
        groups_list = list(query_groups)

    planner_query_groups = []
    for idx, group in enumerate(groups_list):
        sql_file = group.get("sql_file")
        if not sql_file:
            raise ValueError(f"query_groups[{idx}] missing 'sql_file'")
        # sketchdb always plans against baseline's SQL text (see _resolve_sql_file).
        sql_file = _resolve_sql_file(
            sql_file,
            constants.SKETCHDB_EXPERIMENT_NAME,
            idx,
            group.get("name", str(idx)),
        )
        queries = _load_sql_queries(sql_file)
        if not queries:
            raise ValueError(f"No SQL statements found in {sql_file!r}")

        ctrl_opts = dict(group.get("controller_options") or {})
        planner_query_groups.append(
            {
                "id": idx + 1,
                "repetition_delay_ms": int(group.get("repetition_delay_ms", 0)),
                "queries": queries,
                "controller_options": {
                    "accuracy_sla": float(ctrl_opts.get("accuracy_sla", 0.95)),
                    "latency_sla": float(ctrl_opts.get("latency_sla", 100.0)),
                },
            }
        )

    planner_input = {
        "tables": tables,
        "query_groups": planner_query_groups,
        "aggregate_cleanup": {"policy": "read_based"},
    }
    if sketch_parameters is not None:
        if isinstance(sketch_parameters, (DictConfig, ListConfig)):
            sketch_parameters = OmegaConf.to_container(sketch_parameters, resolve=True)
        planner_input["sketch_parameters"] = sketch_parameters
    return yaml.dump(planner_input, default_flow_style=False, allow_unicode=True)


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
