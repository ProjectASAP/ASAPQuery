import os
import argparse
import subprocess

import utils
import constants
from experiment_utils.services import MonitoringService


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


def run_deathstar_workload(
    num_nodes_in_experiment, username, hostname_suffix, experiment_output_dir
):
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/benchmarks/DeathStarBench/socialNetwork"
    cmd_template = "../wrk2/wrk -D exp -t 12 -c {} -d 300 -L -s ./wrk2/scripts/social-network/compose-post.lua http://{}:8080/wrk2-api/post/compose -R {} > {} 2>&1 &"

    TOTAL_CONNECTIONS = 480
    TOTAL_REQUESTS = 1200
    output_file_template = (
        "{}/deathstar_logs/connections_{}_requests_{}_nodes_{}_ip_{}.txt"
    )

    connections = TOTAL_CONNECTIONS // num_nodes_in_experiment
    requests = TOTAL_REQUESTS // num_nodes_in_experiment

    ips = []
    output_files = []
    for i in range(1, num_nodes_in_experiment + 1):
        ips.append(f"10.10.1.{i+1}")
        output_files.append(
            output_file_template.format(
                experiment_output_dir,
                TOTAL_CONNECTIONS,
                TOTAL_REQUESTS,
                num_nodes_in_experiment,
                i,
            )
        )

    cmds = [
        cmd_template.format(connections, ip, requests, output_file)
        for ip, output_file in zip(ips, output_files)
    ]
    cmds.insert(0, "mkdir -p {};".format(os.path.dirname(output_files[0])))
    cmds.append("wait")
    final_cmd = " ".join(cmds)
    utils.run_on_cloudlab_node(
        0, username, hostname_suffix, final_cmd, cmd_dir, nohup=False, popen=False
    )
    # input('Press enter after command has finished')


def calculate_cost(
    num_nodes_in_experiment, username, hostname_suffix, experiment_output_dir
):
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/cloudlab_scripts"
    output_file = experiment_output_dir + "/cost_logs/retention_30_nodes_{}.txt"
    cmd_template = (
        "mkdir -p {}; python3 calculate_cost.py --query_log_file ./prometheus/queries.log --retention_days {} > "
        + output_file
    )

    # retention = 30
    final_cmd = cmd_template.format(
        os.path.dirname(output_file), 30, num_nodes_in_experiment
    )
    utils.run_on_cloudlab_node(
        0, username, hostname_suffix, final_cmd, cmd_dir, nohup=False, popen=False
    )
    # retention = 300
    final_cmd = cmd_template.format(
        os.path.dirname(output_file), 300, num_nodes_in_experiment
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

    experiment_output_dir = (
        f"{constants.CLOUDLAB_HOME_DIR}/cloudlab_scripts/{args.experiment_name}"
    )
    utils.run_on_cloudlab_nodes_in_parallel(
        range(args.num_nodes + 1),
        args.cloudlab_username,
        args.hostname_suffix,
        f"mkdir -p {experiment_output_dir}",
        "",
        nohup=False,
        popen=True,
    )

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
        # this blocks until the workload is done
        run_deathstar_workload(
            num_nodes_in_experiment,
            args.cloudlab_username,
            args.hostname_suffix,
            experiment_output_dir,
        )
        calculate_cost(
            num_nodes_in_experiment,
            args.cloudlab_username,
            args.hostname_suffix,
            experiment_output_dir,
        )
        stop_monitoring(args.num_nodes, args.cloudlab_username, args.hostname_suffix)
        stop_deathstar(
            num_nodes_in_experiment, args.cloudlab_username, args.hostname_suffix
        )
        reset_prometheus(args.num_nodes, args.cloudlab_username, args.hostname_suffix)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment_name", type=str, required=True)
    parser.add_argument("--num_nodes", type=int, required=True)
    parser.add_argument("--start_num_nodes", type=int, default=2)
    parser.add_argument("--cloudlab_username", type=str, required=True)
    parser.add_argument("--hostname_suffix", type=str, required=True)
    args = parser.parse_args()
    main(args)
