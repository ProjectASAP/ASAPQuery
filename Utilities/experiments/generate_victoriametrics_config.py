import os
import yaml
import argparse
from omegaconf import DictConfig

import experiment_utils
from constants import (
    VMAGENT_SCRAPE_CONFIG_FILE,
    VMAGENT_REMOTE_WRITE_CONFIG_FILE,
)


def get_metrics_for_exporter(exporter_name, experiment_config):
    """Extract metrics for a specific exporter from experiment config."""
    if "metrics" not in experiment_config:
        return []

    metrics = []
    for metric_config in experiment_config["metrics"]:
        if metric_config["exporter"] == exporter_name:
            metrics.append(metric_config["metric"])

    return metrics


def add_metric_relabel_configs(scrape_config, exporter_name, experiment_config):
    """Add metric_relabel_configs to scrape_config to only keep required metrics."""
    metrics = get_metrics_for_exporter(exporter_name, experiment_config)
    if metrics:
        scrape_config["metric_relabel_configs"] = [
            {
                "source_labels": ["__name__"],
                "regex": "|".join(metrics),
                "action": "keep",
            }
        ]


def check_queries_exist_for_prometheus_config(exporter_name, experiment_config):
    """Check if queries exist for the given exporter."""
    if "only_start_if_queries_exist" not in experiment_config["exporters"]:
        flag = False
    else:
        flag = experiment_config["exporters"]["only_start_if_queries_exist"]

    if flag is False:
        return True

    if "query_groups" not in experiment_config:
        return False

    if "metrics" not in experiment_config:
        return False

    metric_exporter_names = [
        [metric_config["metric"], metric_config["exporter"]]
        for metric_config in experiment_config["metrics"]
    ]

    query_groups = experiment_config["query_groups"]
    for group in query_groups:
        queries = group["queries"]
        for q in queries:
            for metric in metric_exporter_names:
                # TODO: "does metric exist in query" condition should use promql AST
                if (
                    metric[0] in q
                    and metric[0] + "_" not in q
                    and "_" + metric[0] not in q
                ) and metric[1] == exporter_name:
                    return True

    return False


def create_base_vmagent_scrape_config(scrape_interval):
    """Create base vmagent scrape configuration."""
    config = {
        "global": {
            "scrape_interval": scrape_interval,
        },
        "scrape_configs": [],
    }
    return config


def create_vmagent_remote_write_relabel_config(remote_write_metric_names):
    """
    Create vmagent remote write relabeling configuration.

    This configuration filters which metrics get remote written based on the
    remote_write_metric_names list. Only metrics matching these names will be
    sent to remote write endpoints.

    Args:
        remote_write_metric_names: List of metric names to remote write, or None

    Returns:
        String containing YAML relabeling rules
    """
    if not remote_write_metric_names:
        return ""

    # Create a keep action that only allows specified metrics
    # This will be written as raw YAML text
    relabel_config = (
        f"- action: keep\n"
        f"  source_labels: [__name__]\n"
        f"  regex: {'|'.join(remote_write_metric_names)}\n"
    )
    return relabel_config


def main(args, experiment_config=None):
    """
    Generate VictoriaMetrics vmagent configuration files.

    Creates two files:
    1. vmagent_scrape.yml - Scraping configuration (similar to prometheus.yml)
    2. vmagent_remote_write.yml - Remote write relabeling rules
    """
    # Use provided experiment_config or read from file for backward compatibility
    if experiment_config is None:
        with open(args.experiment_config_file, "r") as experiment_config_f:
            experiment_config = yaml.safe_load(experiment_config_f)

    exporter_config, rejection_reason = experiment_utils.read_exporter_config(
        DictConfig(experiment_config)
    )

    if exporter_config is None:
        raise ValueError("Invalid exporter config: {}".format(rejection_reason))

    # Create vmagent scrape config
    if hasattr(args, "scrape_interval"):
        scrape_config = create_base_vmagent_scrape_config(
            scrape_interval=args.scrape_interval
        )
    else:
        # Fallback default
        scrape_config = create_base_vmagent_scrape_config(scrape_interval="15s")

    # Add scrape targets for node_exporter
    if "node_exporter" in exporter_config[
        "exporter_list"
    ] and check_queries_exist_for_prometheus_config("node_exporter", experiment_config):
        node_exporters = [("node_exporter", 9100)]
        for exporter, port in node_exporters:
            scrape_job = {
                "job_name": exporter,
                "static_configs": [
                    {
                        "targets": [
                            f"{args.node_ip_prefix}.{i + 1}:{port}"
                            for i in range(
                                args.node_offset + 1,
                                args.node_offset + args.num_nodes + 1,
                            )
                        ]
                    }
                ],
            }

            # Add metric_relabel_configs to only keep required metrics
            add_metric_relabel_configs(scrape_job, exporter, experiment_config)

            scrape_config["scrape_configs"].append(scrape_job)

    # Add scrape targets for blackbox_exporter
    if "blackbox_exporter" in exporter_config[
        "exporter_list"
    ] and check_queries_exist_for_prometheus_config(
        "blackbox_exporter", experiment_config
    ):
        blackbox_exporters = [("blackbox_exporter", 9115)]
        for exporter, port in blackbox_exporters:
            scrape_job = {
                "job_name": exporter,
                "static_configs": [
                    {
                        "targets": [
                            f"{args.node_ip_prefix}.{i + 1}:{port}"
                            for i in range(
                                args.node_offset + 1,
                                args.node_offset + args.num_nodes + 1,
                            )
                        ]
                    }
                ],
            }

            add_metric_relabel_configs(scrape_job, exporter, experiment_config)

            scrape_config["scrape_configs"].append(scrape_job)

    # Add scrape targets for cadvisor
    if "cadvisor" in exporter_config[
        "exporter_list"
    ] and check_queries_exist_for_prometheus_config("cadvisor", experiment_config):
        cadvisor_exporters = [("cadvisor", 8082)]
        for exporter, port in cadvisor_exporters:
            scrape_job = {
                "job_name": exporter,
                "static_configs": [
                    {
                        "targets": [
                            f"{args.node_ip_prefix}.{i + 1}:{port}"
                            for i in range(
                                args.node_offset + 1,
                                args.node_offset + args.num_nodes + 1,
                            )
                        ]
                    }
                ],
            }

            add_metric_relabel_configs(scrape_job, exporter, experiment_config)

            scrape_config["scrape_configs"].append(scrape_job)

    # Add scrape targets for fake_exporter
    if "fake_exporter" in exporter_config[
        "exporter_list"
    ] and check_queries_exist_for_prometheus_config("fake_exporter", experiment_config):
        fake_exporter_config = exporter_config["exporter_list"]["fake_exporter"]
        num_ports_per_server = fake_exporter_config["num_ports_per_server"]
        start_port = fake_exporter_config["start_port"]

        targets = []
        for i in range(args.node_offset + 1, args.node_offset + args.num_nodes + 1):
            for j in range(num_ports_per_server):
                targets.append(f"{args.node_ip_prefix}.{i + 1}:{start_port + j}")

        scrape_job = {
            "job_name": "fake_exporter",
            "static_configs": [{"targets": targets}],
        }

        add_metric_relabel_configs(scrape_job, "fake_exporter", experiment_config)

        scrape_config["scrape_configs"].append(scrape_job)

    # Add scrape targets for avalanche exporter
    if "avalanche" in exporter_config[
        "exporter_list"
    ] and check_queries_exist_for_prometheus_config("avalanche", experiment_config):
        avalanche_config = exporter_config["exporter_list"]["avalanche"]
        avalanche_port = avalanche_config.get("port", 9001)

        targets = []
        for i in range(args.node_offset + 1, args.node_offset + args.num_nodes + 1):
            targets.append(f"{args.node_ip_prefix}.{i + 1}:{avalanche_port}")

        scrape_job = {
            "job_name": "avalanche",
            "static_configs": [{"targets": targets}],
        }

        add_metric_relabel_configs(scrape_job, "avalanche", experiment_config)

        scrape_config["scrape_configs"].append(scrape_job)

    # Add scrape targets for cluster_data_exporter
    if "cluster_data_exporter" in exporter_config[
        "exporter_list"
    ] and check_queries_exist_for_prometheus_config(
        "cluster_data_exporter", experiment_config
    ):
        cluster_data_config = exporter_config["exporter_list"]["cluster_data_exporter"]
        cluster_data_port = cluster_data_config.get("port", 9010)

        targets = []
        for i in range(args.node_offset + 1, args.node_offset + args.num_nodes + 1):
            targets.append(f"{args.node_ip_prefix}.{i + 1}:{cluster_data_port}")

        scrape_job = {
            "job_name": "cluster_data_exporter",
            "static_configs": [{"targets": targets}],
        }

        add_metric_relabel_configs(
            scrape_job, "cluster_data_exporter", experiment_config
        )

        scrape_config["scrape_configs"].append(scrape_job)

    # Write vmagent scrape configuration
    scrape_output_path = os.path.join(args.output_dir, VMAGENT_SCRAPE_CONFIG_FILE)
    os.makedirs(os.path.dirname(scrape_output_path), exist_ok=True)
    with open(scrape_output_path, "w") as f:
        yaml.dump(scrape_config, f, default_flow_style=False, sort_keys=False)

    # Write vmagent remote write relabeling configuration
    # Get remote_write_metric_names from args if available
    remote_write_metric_names = getattr(args, "remote_write_metric_names", None)
    remote_write_relabel_config = create_vmagent_remote_write_relabel_config(
        remote_write_metric_names
    )
    remote_write_output_path = os.path.join(
        args.output_dir, VMAGENT_REMOTE_WRITE_CONFIG_FILE
    )
    with open(remote_write_output_path, "w") as f:
        # Write as plain text (YAML), not using yaml.dump since it's already formatted
        f.write(remote_write_relabel_config)

    print(f"VictoriaMetrics vmagent scrape config written to {scrape_output_path}")
    print(
        f"VictoriaMetrics vmagent remote write config written to {remote_write_output_path}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate VictoriaMetrics vmagent configuration"
    )
    parser.add_argument("--num-nodes", type=int, required=True)
    parser.add_argument("--output-dir", type=str, required=True)
    parser.add_argument("--node-ip-prefix", type=str, default="10.10.1")
    parser.add_argument("--node-offset", type=int, default=0)
    parser.add_argument("--scrape-interval", type=str, default="15s")
    parser.add_argument("--experiment-config-file", type=str, required=True)
    parser.add_argument("--remote-write", type=str, nargs="+", help="Remote write URLs")

    args = parser.parse_args()
    main(args)
