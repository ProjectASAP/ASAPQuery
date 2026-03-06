import os
import yaml
import shutil
import argparse
from omegaconf import DictConfig

import experiment_utils


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


# def read_exporter_config_for_prometheus_config(experiment_config_file) -> Tuple[Optional[Dict], str]:
#     with open(experiment_config_file, "r") as f:
#         experiment_config = yaml.safe_load(f)

#     if "exporters" not in experiment_config:
#         return None, "No exporters section in experiment config"
#     exporters_config = experiment_config["exporters"]
#     if "exporter_list" not in exporters_config:
#         return None, "No exporter_list section in exporters config"
#     if "only_start_if_queries_exist" not in exporters_config:
#         return None, "No only_start_if_queries_exist section in exporters config"

#     if "fake_exporter" in exporters_config["exporter_list"]:
#         if any(key not in exporters_config["exporter_list"]["fake_exporter"] for key in ["num_ports_per_server", "dataset", "synthetic_data_value_scale", "start_port", "num_labels", "num_values_per_label", "metric_type"]):
#             return None, "Missing keys in fake_exporter section"

#     if "node_exporter" in exporters_config["exporter_list"]:
#         if any(key not in exporters_config["exporter_list"]["node_exporter"] for key in ["port"]):
#             return None, "Missing keys in node_exporter section"

#     return exporters_config, ""


def create_base_prometheus_config(scrape_interval, evaluation_interval, query_log_file):
    """Create base Prometheus configuration with configurable parameters."""
    config = {
        "global": {
            "scrape_interval": scrape_interval,
            "evaluation_interval": evaluation_interval,
        },
        "alerting": {"alertmanagers": [{"static_configs": [{"targets": []}]}]},
    }

    if query_log_file:
        config["global"]["query_log_file"] = query_log_file

    return config


def generate_recording_rules_file(output_path, interval="5s"):
    """Generate recording rules file with configurable interval."""
    recording_rules = {
        "groups": [
            {
                "name": "RecordingRules",
                "interval": interval,
                "rules": [
                    {
                        "record": "node_top_3_cpu_usage_60s",
                        "expr": "topk(3, sum by (instance, mode) (sum_over_time(node_cpu_seconds_total[60s])))",
                        "labels": {"metric_type": "high_cpu_instances_modes"},
                    }
                ],
            }
        ]
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        yaml.dump(recording_rules, f)


def main(args, experiment_config=None):
    # Create base config using parameters instead of reading from template
    if hasattr(args, "scrape_interval") and hasattr(args, "evaluation_interval"):
        # New Hydra-based approach
        config = create_base_prometheus_config(
            scrape_interval=args.scrape_interval,
            evaluation_interval=args.evaluation_interval,
            query_log_file=args.query_log_file,
        )
    else:
        # Fallback to old template-based approach for backward compatibility
        with open(args.input_file, "r") as f:
            config = yaml.load(f, Loader=yaml.FullLoader)

    # Use provided experiment_config or read from file for backward compatibility
    if experiment_config is None:
        with open(args.experiment_config_file, "r") as experiment_config_f:
            experiment_config = yaml.safe_load(experiment_config_f)

    exporter_config, rejection_reason = experiment_utils.read_exporter_config(
        DictConfig(experiment_config)
    )

    if exporter_config is None:
        raise ValueError("Invalid exporter config: {}".format(rejection_reason))

    # add section for "scrape_configs"
    config["scrape_configs"] = []
    # add target for self-monitoring prometheus
    # config['scrape_configs'].append({
    #     'job_name': 'prometheus',
    #     'static_configs': [
    #         {
    #             'targets': ['localhost:9090']
    #         }
    #     ]
    # })

    # add targets for monitoring node_exporter, blackbox_exporter, and cadvisor
    # exporters = [('node_exporter', 9099), ('blackbox_exporter', 9115), ('cadvisor', 8082)]
    # exporters = [('node_exporter', 9100),  ('cadvisor', 8082)]

    if "node_exporter" in exporter_config[
        "exporter_list"
    ] and check_queries_exist_for_prometheus_config("node_exporter", experiment_config):
        node_exporters = [("node_exporter", 9100)]
        for exporter, port in node_exporters:
            scrape_config = {
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
            add_metric_relabel_configs(scrape_config, exporter, experiment_config)

            config["scrape_configs"].append(scrape_config)

    if "fake_exporter" in exporter_config[
        "exporter_list"
    ] and check_queries_exist_for_prometheus_config("fake_exporter", experiment_config):
        fake_exporters = [
            (
                "fake_exporter",
                [
                    port
                    + exporter_config["exporter_list"]["fake_exporter"]["start_port"]
                    for port in range(
                        exporter_config["exporter_list"]["fake_exporter"][
                            "num_ports_per_server"
                        ]
                    )
                ],
            )
        ]
        for exporter, ports in fake_exporters:
            targets = []
            for target_ip in range(
                args.node_offset + 1, args.node_offset + args.num_nodes + 1
            ):
                for port in ports:
                    targets.append(f"{args.node_ip_prefix}.{target_ip + 1}:{port}")

            scrape_config = {
                "job_name": exporter,
                "static_configs": [
                    {
                        "targets": targets,
                    }
                ],
            }

            # Add metric_relabel_configs to only keep required metrics
            add_metric_relabel_configs(scrape_config, exporter, experiment_config)

            config["scrape_configs"].append(scrape_config)

    if "avalanche" in exporter_config[
        "exporter_list"
    ] and check_queries_exist_for_prometheus_config("avalanche", experiment_config):
        # Avalanche exporters run on Docker containers with port 9001 exposed
        avalanche_config = exporter_config["exporter_list"]["avalanche"]
        num_exporters = avalanche_config.get("num_exporters", 1)
        start_port = avalanche_config.get("start_port", 9001)

        targets = []
        for i in range(num_exporters):
            port = start_port + i
            targets.append(f"localhost:{port}")

        scrape_config = {
            "job_name": "avalanche",
            "static_configs": [
                {
                    "targets": targets,
                }
            ],
        }

        # Add metric_relabel_configs to only keep required metrics
        add_metric_relabel_configs(scrape_config, "avalanche", experiment_config)

        config["scrape_configs"].append(scrape_config)

    if "cluster_data_exporter" in exporter_config[
        "exporter_list"
    ] and check_queries_exist_for_prometheus_config(
        "cluster_data_exporter", experiment_config
    ):
        # cluster_data_exporter runs on node after coordinator (node_offset + 1)
        cde_config = exporter_config["exporter_list"]["cluster_data_exporter"]
        port = cde_config.get("port", 40000)

        # Target is on the node after coordinator
        # Node indexing: node_offset + 1 = coordinator, node_offset + 2 = cluster data exporter node
        target_node_idx = args.node_offset + 2
        target = f"{args.node_ip_prefix}.{target_node_idx}:{port}"

        scrape_config = {
            "job_name": "cluster_data_exporter",
            "scrape_interval": "10s",  # Fast scrape interval for high-resolution cluster data
            "static_configs": [
                {
                    "targets": [target],
                }
            ],
        }

        # Add metric_relabel_configs to only keep required metrics
        add_metric_relabel_configs(
            scrape_config, "cluster_data_exporter", experiment_config
        )

        config["scrape_configs"].append(scrape_config)

    # TODO:
    #      - Make IP:PORT not hardcoded, should always match the IP:PORT
    #             with which the cost/latency exporters are instantiated
    if (
        "export_cost_and_latency" in experiment_config
        and experiment_config["export_cost_and_latency"]
    ):
        config["scrape_configs"].append(
            {
                "job_name": "query_latency_exporter",
                "static_configs": [{"targets": [f"{args.prometheus_client_ip}:9150"]}],
            }
        )
        config["scrape_configs"].append(
            {
                "job_name": "query_cost_exporter",
                "static_configs": [{"targets": [f"{args.prometheus_client_ip}:9151"]}],
            }
        )

    # add config for blackbox_exporter
    # config['scrape_configs'].append({
    #     'job_name': 'blackbox',
    #     'metrics_path': '/probe',
    #     'params': {
    #         'module': ['http_2xx']
    #     },
    #     'static_configs': [
    #         {
    #             'targets': [f"10.10.1.{i + 1}:9115" for i in range(1, args.num_nodes + 1)]
    #         }
    #     ],
    #     'relabel_configs': [
    #         {
    #             'source_labels': ['__address__'],
    #             'target_label': '__param_target',
    #         },
    #         {
    #             'source_labels': ['__param_target'],
    #             'target_label': 'instance',
    #         },
    #         {
    #             'target_label': '__address__',
    #             'replacement': '10.10.1.1:9115',
    #         }
    #     ]
    # })

    if not (hasattr(args, "scrape_interval") and hasattr(args, "evaluation_interval")):
        # Only update query_log_file if using template-based approach and if it's not None
        if args.query_log_file:
            config["global"]["query_log_file"] = args.query_log_file

    if args.rule_files:
        config["rule_files"] = args.rule_files

    if args.remote_write_url:
        parallelism = getattr(args, "parallelism")
        base_port = getattr(args, "remote_write_base_port")

        if parallelism > 1 and base_port is None:
            raise ValueError("remote_write_base_port required when parallelism > 1")

        config["remote_write"] = []
        for i in range(parallelism):
            if parallelism > 1:
                # Replace port in URL for sharding
                url_parts = args.remote_write_url.split(":")
                port_and_path = url_parts[-1]
                original_port = port_and_path.split("/")[0]
                new_port = base_port + i
                new_url = args.remote_write_url.replace(
                    f":{original_port}", f":{new_port}"
                )
            else:
                new_url = args.remote_write_url

            remote_write_config = {"url": new_url}

            # Add queue_config with batch_send_deadline set to scrape_interval
            scrape_interval = config["global"]["scrape_interval"]
            remote_write_config["queue_config"] = {
                "batch_send_deadline": scrape_interval,
                # "max_samples_per_send": 5000,
                # "capacity": 100000,
            }

            # Add metric filtering and sharding logic for multiple destinations
            if parallelism > 1:
                remote_write_config["write_relabel_configs"] = []

                # Add metric filtering first to reduce processing overhead
                if args.remote_write_metric_names:
                    remote_write_config["write_relabel_configs"].append(
                        {
                            "source_labels": ["__name__"],
                            "regex": "|".join(args.remote_write_metric_names),
                            "action": "keep",
                        }
                    )

                # Add improved sharding logic using metric name + common labels
                remote_write_config["write_relabel_configs"].extend(
                    [
                        {
                            "source_labels": ["__name__", "instance", "job"],
                            "separator": "|",
                            "target_label": "__tmp_hash_input",
                            "replacement": "${1}|${2}|${3}",
                        },
                        {
                            "source_labels": ["__tmp_hash_input"],
                            "modulus": parallelism,
                            "target_label": "__tmp_shard",
                            "action": "hashmod",
                        },
                        {
                            "source_labels": ["__tmp_shard"],
                            "regex": str(i),
                            "action": "keep",
                        },
                    ]
                )
            else:
                # For single destination, only add metric filtering if specified
                if args.remote_write_metric_names:
                    remote_write_config["write_relabel_configs"] = [
                        {
                            "source_labels": ["__name__"],
                            "regex": "|".join(args.remote_write_metric_names),
                            "action": "keep",
                        }
                    ]

            config["remote_write"].append(remote_write_config)

    # Write prometheus config to output_dir
    import constants

    output_file = os.path.join(args.output_dir, constants.PROMETHEUS_CONFIG_FILE)
    os.makedirs(args.output_dir, exist_ok=True)
    with open(output_file, "w") as f:
        yaml.dump(config, f)

    if args.copy_to_dir:
        os.makedirs(args.copy_to_dir, exist_ok=True)
        # copy output_file and rule_files to copy_to_dir
        shutil.copy(
            os.path.join(args.output_dir, constants.PROMETHEUS_CONFIG_FILE),
            args.copy_to_dir,
        )
        if args.rule_files:
            for rule_file in args.rule_files:
                dst_path = os.path.join(args.copy_to_dir, os.path.dirname(rule_file))
                os.makedirs(dst_path, exist_ok=True)

                # Handle both template-based and Hydra-based approaches
                if (
                    hasattr(args, "scrape_interval")
                    and hasattr(args, "evaluation_interval")
                    and args.input_file
                ):
                    # Template-based: copy from input_file directory
                    shutil.copy(
                        os.path.join(os.path.dirname(args.input_file), rule_file),
                        dst_path,
                    )
                else:
                    # Hydra-based: copy from current directory or generate dynamically
                    if os.path.exists(rule_file):
                        shutil.copy(rule_file, dst_path)
                    else:
                        # Generate recording rules with configurable interval
                        generate_recording_rules_file(
                            os.path.join(dst_path, os.path.basename(rule_file)),
                            args.recording_rules_interval or "5s",
                        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_nodes", type=int, required=True)
    parser.add_argument(
        "--input_file", type=str, required=False
    )  # Made optional for Hydra mode
    parser.add_argument("--output_dir", type=str, required=True)
    parser.add_argument("--query_log_file", type=str, required=False)
    parser.add_argument("--rule_files", nargs="+", required=False)
    parser.add_argument("--remote_write_url", type=str, required=False)
    parser.add_argument("--remote_write_metric_names", type=str, required=False)
    parser.add_argument(
        "--remote_write_base_port",
        type=int,
        required=False,
        help="Base port for remote_write sharding",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        required=False,
        help="Number of parallel remote_write destinations",
    )
    parser.add_argument("--copy_to_dir", type=str, required=False)
    parser.add_argument("--experiment_config_file", type=str, required=True)
    # New Hydra-compatible parameters
    parser.add_argument(
        "--scrape_interval",
        type=str,
        required=False,
        help="Prometheus scrape interval (e.g., '5s')",
    )
    parser.add_argument(
        "--evaluation_interval",
        type=str,
        required=False,
        help="Prometheus evaluation interval (e.g., '1s')",
    )
    parser.add_argument(
        "--recording_rules_interval",
        type=str,
        required=False,
        help="Recording rules evaluation interval (e.g., '5s')",
    )
    parser.add_argument(
        "--prometheus-client-ip",
        type=str,
        required=True,
        help="Prometheus client node IP address",
    )
    parser.add_argument(
        "--node-ip-prefix",
        type=str,
        required=True,
        help="Node IP prefix (e.g., 10.10.1 for CloudLab)",
    )
    parser.add_argument(
        "--node-offset",
        type=int,
        required=False,
        default=0,
        help="Node offset for CloudLab deployments (default: 0)",
    )

    args = parser.parse_args()
    if args.remote_write_metric_names:
        args.remote_write_metric_names = args.remote_write_metric_names.strip().split(
            ","
        )
    main(args)
