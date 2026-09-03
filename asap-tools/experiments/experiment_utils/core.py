from typing import Dict, Tuple, Optional
from omegaconf import DictConfig, OmegaConf


def read_exporter_config(experiment_params: DictConfig) -> Tuple[Optional[Dict], str]:
    if "exporters" not in experiment_params:
        return None, "No exporters section in experiment config"
    exporters_config = experiment_params.exporters
    if "exporter_list" not in exporters_config:
        return None, "No exporter_list section in exporters config"
    if "only_start_if_queries_exist" not in exporters_config:
        return None, "No only_start_if_queries_exist section in exporters config"

    if "fake_exporter" in exporters_config.exporter_list:
        if any(
            key not in exporters_config.exporter_list.fake_exporter
            for key in [
                "num_ports_per_server",
                "dataset",
                "synthetic_data_value_scale",
                "start_port",
                "num_labels",
                "num_values_per_label",
                "metric_type",
            ]
        ):
            return None, "Missing keys in fake_exporter section"

    if "node_exporter" in exporters_config.exporter_list:
        if any(
            key not in exporters_config.exporter_list.node_exporter for key in ["port"]
        ):
            return None, "Missing keys in node_exporter section"

    if "avalanche" in exporters_config.exporter_list:
        # Validate avalanche exporter configuration
        avalanche_config = exporters_config.exporter_list.avalanche
        required_keys = ["cardinality", "ingestion_rate", "port"]
        missing_keys = [key for key in required_keys if key not in avalanche_config]
        if missing_keys:
            return None, f"Missing keys in avalanche section: {missing_keys}"

    if "cluster_data_exporter" in exporters_config.exporter_list:
        # Validate cluster_data_exporter configuration
        cde_config = exporters_config.exporter_list.cluster_data_exporter

        # Check required keys
        required_keys = ["provider", "port"]
        missing_keys = [key for key in required_keys if key not in cde_config]
        if missing_keys:
            return (
                None,
                f"Missing keys in cluster_data_exporter section: {missing_keys}",
            )

        # Validate provider
        provider = cde_config.provider
        if provider not in ["google", "alibaba"]:
            return (
                None,
                f"cluster_data_exporter provider must be 'google' or 'alibaba', got '{provider}'",
            )

        # Provider-specific validation
        if provider == "google":
            # Validate Google-specific configuration
            if "metrics" not in cde_config:
                return (
                    None,
                    "cluster_data_exporter with Google provider requires 'metrics'",
                )
            if "parts_mode" not in cde_config:
                return (
                    None,
                    "cluster_data_exporter with Google provider requires 'parts_mode'",
                )

            parts_mode = cde_config.parts_mode
            if parts_mode not in ["all-parts", "part-index"]:
                return (
                    None,
                    f"cluster_data_exporter parts_mode must be 'all-parts' or 'part-index', got '{parts_mode}'",
                )

            if parts_mode == "part-index" and "part_index" not in cde_config:
                return (
                    None,
                    "cluster_data_exporter with parts_mode='part-index' requires 'part_index'",
                )

        elif provider == "alibaba":
            # Validate Alibaba-specific configuration
            required_alibaba_keys = ["data_type", "data_year", "parts_mode"]
            missing_alibaba_keys = [
                key for key in required_alibaba_keys if key not in cde_config
            ]
            if missing_alibaba_keys:
                return (
                    None,
                    f"cluster_data_exporter with Alibaba provider missing keys: {missing_alibaba_keys}",
                )

            data_type = cde_config.data_type
            if data_type not in ["node", "msresource"]:
                return (
                    None,
                    f"cluster_data_exporter data_type must be 'node' or 'msresource', got '{data_type}'",
                )

            data_year = cde_config.data_year
            if data_year not in [2021, 2022]:
                return (
                    None,
                    f"cluster_data_exporter data_year must be 2021 or 2022, got {data_year}",
                )

            parts_mode = cde_config.parts_mode
            if parts_mode not in ["all-parts", "part-index"]:
                return (
                    None,
                    f"cluster_data_exporter parts_mode must be 'all-parts' or 'part-index', got '{parts_mode}'",
                )

            if parts_mode == "part-index" and "part_index" not in cde_config:
                return (
                    None,
                    "cluster_data_exporter with parts_mode='part-index' requires 'part_index'",
                )

    return exporters_config, ""


class GeneratePrometheusArgs:
    """Arguments class for generate_prometheus_config module."""

    def __init__(
        self,
        num_nodes,
        local_experiment_dir,
        output_dir,
        prometheus_config,
        prometheus_client_ip,
        node_ip_prefix,
        node_offset,
    ):
        self.num_nodes = num_nodes
        self.node_offset = node_offset
        self.output_dir = output_dir
        # self.copy_to_dir = os.path.join(local_experiment_dir, constants.PROMETHEUS_CONFIG_DIR)
        self.copy_to_dir = None
        self.rule_files = None
        self.remote_write_url = None
        self.remote_write_metric_names = None
        self.remote_write_base_port = None
        self.parallelism = None

        self.query_log_file = getattr(prometheus_config, "query_log_file", None)
        self.scrape_interval = prometheus_config.scrape_interval
        self.evaluation_interval = prometheus_config.evaluation_interval
        self.recording_rules_interval = prometheus_config.recording_rules.interval
        self.input_file = None

        self.prometheus_client_ip = prometheus_client_ip
        self.node_ip_prefix = node_ip_prefix


def call_generate_prometheus_config(
    args, cfg, experiment_mode=None, sketchdb_experiment_name=None
):
    """
    Helper function to call generate_prometheus_config with proper setup.

    Args:
        args: GeneratePrometheusArgs instance
        cfg: DictConfig containing master configuration
        experiment_mode: Optional experiment mode for remote write setup
        sketchdb_experiment_name: Optional name to check for remote write setup
    """
    import generate_prometheus_config
    import generate_victoriametrics_config

    # Set up remote write if this is a SketchDB experiment
    if experiment_mode == sketchdb_experiment_name and sketchdb_experiment_name:
        metrics_to_remote_write = get_metrics_to_remote_write(cfg.experiment_params)
        args.remote_write_metric_names = metrics_to_remote_write

        # Build remote_write_url from experiment params
        streaming_config = cfg.get("streaming", {})
        remote_write_config = streaming_config.get("remote_write", {})

        ip = remote_write_config["ip"]
        base_port = remote_write_config["base_port"]
        path = remote_write_config["path"]
        parallelism = streaming_config["parallelism"]

        args.remote_write_url = f"http://{ip}:{base_port}{path}"
        args.remote_write_base_port = base_port
        args.parallelism = parallelism

    # Call the function directly instead of subprocess
    generate_prometheus_config.main(args, OmegaConf.to_container(cfg.experiment_params))

    # Also generate VictoriaMetrics configs (will be used if tool=victoriametrics)
    generate_victoriametrics_config.main(
        args, OmegaConf.to_container(cfg.experiment_params)
    )


def get_metrics_to_remote_write(experiment_params: DictConfig):
    """
    Get list of metrics that should be written to remote write based on experiment configuration.

    Args:
        experiment_params: DictConfig containing experiment parameters

    Returns:
        List of metric names to be written to remote write
    """
    if "metrics" not in experiment_params:
        return []

    if "query_groups" not in experiment_params:
        return []

    if (
        "only_start_if_queries_exist" not in experiment_params.exporters
        or not experiment_params.exporters.only_start_if_queries_exist
    ):
        return [metric_config.metric for metric_config in experiment_params.metrics]

    total_queries = []
    query_groups = experiment_params.query_groups
    for group in query_groups:
        queries = group.queries
        total_queries.extend(queries)

    metrics_to_remote_write = []
    for metric_config in experiment_params.metrics:
        find = False
        for query in total_queries:
            if metric_config.metric in query:
                find = True
                break
        if find:
            metrics_to_remote_write.append(metric_config.metric)

    return metrics_to_remote_write
