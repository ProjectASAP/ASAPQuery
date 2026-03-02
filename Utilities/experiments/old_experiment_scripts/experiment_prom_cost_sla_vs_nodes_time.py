import os
import time
import argparse
import subprocess

import utils
import constants
from experiment_utils.services import MonitoringService

NUM_MEASUREMENTS = 10
TIME_BETWEEN_MEASUREMENTS = 600


def generate_config(num_nodes_in_experiment):
    rule_files = ["blackbox-exporter.yml", "node-exporter.yml", "google-cadvisor.yml"]
    rule_files = [
        os.path.join("recording_rules", rule_file) for rule_file in rule_files
    ]
    cmd = "python3 generate_prometheus_config.py --num_nodes {} --input_file {} --output_file {} --rule_files {} --query_log_file {}".format(
        num_nodes_in_experiment,
        "prometheus_config/template.prometheus.yml",
        "prometheus_config/prometheus.yml",
        " ".join(rule_files),
        constants.CLOUDLAB_QUERY_LOG_FILE,
    )
    subprocess.run(cmd, shell=True, check=True)


def rsync_config(username, hostname_suffix):
    hostname = f"node0.{hostname_suffix}"
    cmd = 'rsync -azh -e "ssh -o StrictHostKeyChecking=no" ./prometheus_config {}@{}:{}'.format(
        username,
        hostname,
        os.path.join(constants.CLOUDLAB_HOME_DIR, "cloudlab_scripts"),
    )
    subprocess.run(cmd, shell=True, check=True)


def start_deathstar(num_nodes_in_experiment, username, hostname_suffix):
    cmd = "docker compose up -d"
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/benchmarks/DeathStarBench/socialNetwork"
    utils.run_on_cloudlab_nodes_in_parallel(
        range(1, num_nodes_in_experiment + 1),
        username,
        hostname_suffix,
        cmd,
        cmd_dir,
        nohup=False,
        popen=True,
        redirect=True,
    )


def start_monitoring(num_nodes, username, hostname_suffix, experiment_output_dir):
    monitoring_service = MonitoringService(username, hostname_suffix, num_nodes)
    # Create a minimal experiment config for system exporters
    experiment_params = {"exporters": {"exporter_list": {}}}
    monitoring_service.start(experiment_params, experiment_output_dir)


def calculate_cost(num_nodes_in_experiment, measurement_idx, username, hostname_suffix):
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/cloudlab_scripts"
    output_file_name = "cost_logs/retention_{}_interval_{}_nodes_{}_measurement_{}.txt"
    cmd_template = (
        "mkdir -p ./cost_logs; python3 calculate_cost.py --query_log_file ./prometheus/queries.log --retention_days {} > "
        + output_file_name
    )

    # retention = 30
    final_cmd = cmd_template.format(
        30, TIME_BETWEEN_MEASUREMENTS, num_nodes_in_experiment, measurement_idx
    )
    utils.run_on_cloudlab_node(
        0, username, hostname_suffix, final_cmd, cmd_dir, nohup=False, popen=False
    )
    # retention = 300
    final_cmd = cmd_template.format(
        300, TIME_BETWEEN_MEASUREMENTS, num_nodes_in_experiment, measurement_idx
    )
    utils.run_on_cloudlab_node(
        0, username, hostname_suffix, final_cmd, cmd_dir, nohup=False, popen=False
    )


def stop_monitoring(num_nodes, username, hostname_suffix):
    monitoring_service = MonitoringService(username, hostname_suffix, num_nodes)
    monitoring_service.stop()


def stop_deathstar(num_nodes_in_experiment, username, hostname_suffix):
    cmd = "docker compose down"
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/benchmarks/DeathStarBench/socialNetwork"
    utils.run_on_cloudlab_nodes_in_parallel(
        range(1, num_nodes_in_experiment + 1),
        username,
        hostname_suffix,
        cmd,
        cmd_dir,
        nohup=False,
        popen=True,
    )


def reset_prometheus(num_nodes, username, hostname_suffix):
    cmd = "python3 reset_prometheus.py --num_nodes {} --cloudlab_username {} --hostname_suffix {}".format(
        num_nodes, username, hostname_suffix
    )
    subprocess.run(cmd, shell=True, check=True)


def main(args):
    stop_monitoring(args.num_nodes, args.cloudlab_username, args.hostname_suffix)
    stop_deathstar(args.num_nodes, args.cloudlab_username, args.hostname_suffix)
    reset_prometheus(args.num_nodes, args.cloudlab_username, args.hostname_suffix)

    experiment_output_dir = f"{constants.CLOUDLAB_HOME_DIR}/cloudlab_scripts"

    for num_nodes_in_experiment in range(args.start_num_nodes, args.num_nodes + 2, 2):
        generate_config(num_nodes_in_experiment)
        rsync_config(args.cloudlab_username, args.hostname_suffix)
        start_deathstar(
            num_nodes_in_experiment, args.cloudlab_username, args.hostname_suffix
        )
        start_monitoring(
            args.num_nodes,
            args.cloudlab_username,
            args.hostname_suffix,
            experiment_output_dir,
        )
        for measurement_idx in range(NUM_MEASUREMENTS):
            time.sleep(TIME_BETWEEN_MEASUREMENTS)
            calculate_cost(
                num_nodes_in_experiment,
                measurement_idx,
                args.cloudlab_username,
                args.hostname_suffix,
            )
        stop_monitoring(args.num_nodes, args.cloudlab_username, args.hostname_suffix)
        stop_deathstar(
            num_nodes_in_experiment, args.cloudlab_username, args.hostname_suffix
        )
        reset_prometheus(args.num_nodes, args.cloudlab_username, args.hostname_suffix)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_nodes", type=int, required=True)
    parser.add_argument("--start_num_nodes", type=int, default=2)
    parser.add_argument("--cloudlab_username", type=str, required=True)
    parser.add_argument("--hostname_suffix", type=str, required=True)
    args = parser.parse_args()
    main(args)
