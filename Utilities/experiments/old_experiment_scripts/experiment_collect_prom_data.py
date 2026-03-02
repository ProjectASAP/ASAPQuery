import os
import time
import random
import argparse
import subprocess

import utils
import constants
from experiment_utils.services import MonitoringService
from experiment_utils.providers.cloudlab import CloudLabProvider


def generate_and_copy_config(num_nodes_in_experiment, local_experiment_dir):
    # rule_files = ["custom_recording_rules.yml"]
    rule_files = ["blackbox-exporter.yml", "node-exporter.yml", "google-cadvisor.yml"]
    rule_files = [
        os.path.join("recording_rules", rule_file) for rule_file in rule_files
    ]
    cmd = "python3 generate_prometheus_config.py --num_nodes {} --input_file {} --output_file {} --rule_files {} --query_log_file {} --copy_to_dir {}".format(
        num_nodes_in_experiment,
        "prometheus_config/template.prometheus.yml",
        "prometheus_config/prometheus.yml",
        " ".join(rule_files),
        constants.CLOUDLAB_QUERY_LOG_FILE,
        os.path.join(local_experiment_dir, "prometheus_config"),
    )
    subprocess.run(cmd, shell=True, check=True)


def rsync_config(username, hostname_suffix, node_offset):
    hostname = f"node{node_offset}.{hostname_suffix}"
    cmd = 'rsync -azh -e "ssh -o StrictHostKeyChecking=no" ./prometheus_config {}@{}:{}'.format(
        username,
        hostname,
        os.path.join(constants.CLOUDLAB_HOME_DIR, "cloudlab_scripts"),
    )
    subprocess.run(cmd, shell=True, check=True)


def start_deathstar(num_nodes_in_experiment, username, hostname_suffix, node_offset):
    cmd = "docker compose up -d"
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/benchmarks/DeathStarBench/socialNetwork"
    utils.run_on_cloudlab_nodes_in_parallel(
        range(node_offset + 1, node_offset + num_nodes_in_experiment + 1),
        username,
        hostname_suffix,
        cmd,
        cmd_dir,
        nohup=False,
        popen=True,
        redirect=True,
    )


def start_monitoring(
    num_nodes, username, hostname_suffix, experiment_output_dir, node_offset
):
    provider = CloudLabProvider(username, hostname_suffix)
    monitoring_service = MonitoringService(provider, num_nodes, node_offset)
    # Create a minimal experiment config for system exporters
    experiment_params = {"exporters": {"exporter_list": {}}}
    monitoring_service.start(experiment_params, experiment_output_dir)


def run_deathstar_workload(
    num_nodes_in_experiment,
    username,
    hostname_suffix,
    experiment_output_dir,
    local_experiment_dir,
    node_offset,
    random_params=False,
):
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/benchmarks/DeathStarBench/socialNetwork"

    TOTAL_CONNECTIONS = 480
    TOTAL_REQUESTS = 1200
    DURATION = 3000

    connections = TOTAL_CONNECTIONS // num_nodes_in_experiment
    requests = TOTAL_REQUESTS // num_nodes_in_experiment
    output_file_template = (
        "{}/deathstar_logs/connections_{}_requests_{}_nodes_{}_ip_{}.txt"
    )

    ips = []
    output_files = []
    for i in range(node_offset + 1, node_offset + num_nodes_in_experiment + 1):
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

    if not random_params:
        cmd_template = "../wrk2/wrk -D exp -t 12 -c {} -d {} -L -s ./wrk2/scripts/social-network/compose-post.lua http://{}:8080/wrk2-api/post/compose -R {} > {} 2>&1 &"
        cmds = [
            cmd_template.format(connections, DURATION, ip, requests, output_file)
            for ip, output_file in zip(ips, output_files)
        ]
    else:
        cmd_template = "../wrk2/wrk -D exp -t {} -c {} -d {} -L -s ./wrk2/scripts/social-network/compose-post.lua http://{}:8080/wrk2-api/post/compose -R {} -s ./wrk2/scripts/social-network/random-params.lua > {} 2>&1 &"
        cmds = []
        for ip, output_file in zip(ips, output_files):
            random_threads = random.randint(1, 12)
            random_duration = random.randint(60, 600)
            cmds.append(
                cmd_template.format(
                    random_threads,
                    connections,
                    random_duration,
                    ip,
                    requests,
                    output_file,
                )
            )

    # dump workload configuration to a file
    os.makedirs(os.path.join(local_experiment_dir, "deathstar_config"), exist_ok=True)
    with open(
        os.path.join(local_experiment_dir, "deathstar_config", "cmds.sh"), "w"
    ) as f:
        f.write("\n".join(cmds))

    cmds.insert(0, "mkdir -p {};".format(os.path.dirname(output_files[0])))
    cmds.append("wait")
    final_cmd = " ".join(cmds)
    utils.run_on_cloudlab_node(
        node_offset,
        username,
        hostname_suffix,
        final_cmd,
        cmd_dir,
        nohup=False,
        popen=False,
    )


def stop_monitoring(num_nodes, username, hostname_suffix, node_offset):
    provider = CloudLabProvider(username, hostname_suffix)
    monitoring_service = MonitoringService(provider, num_nodes, node_offset)
    monitoring_service.stop()


def stop_deathstar(num_nodes_in_experiment, username, hostname_suffix, node_offset):
    cmd = "docker compose down"
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/benchmarks/DeathStarBench/socialNetwork"
    utils.run_on_cloudlab_nodes_in_parallel(
        range(node_offset + 1, node_offset + num_nodes_in_experiment + 1),
        username,
        hostname_suffix,
        cmd,
        cmd_dir,
        nohup=False,
        popen=True,
    )


def reset_prometheus(num_nodes, username, hostname_suffix, node_offset):
    cmd = "python3 reset_prometheus.py --num_nodes {} --cloudlab_username {} --hostname_suffix {} --node_offset {}".format(
        num_nodes, username, hostname_suffix, node_offset
    )
    subprocess.run(cmd, shell=True, check=True)


def export_prometheus_data(
    username, hostname_suffix, experiment_output_dir, node_offset
):
    cmd = "python3 export_prometheus_data.py --output_dir {}".format(
        os.path.join(experiment_output_dir, "exported_prometheus_data")
    )
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/cloudlab_scripts"
    utils.run_on_cloudlab_node(
        node_offset,
        username,
        hostname_suffix,
        cmd,
        cmd_dir,
        nohup=False,
        popen=False,
        manual=False,
    )


def copy_prometheus_data(username, hostname_suffix, experiment_name, node_offset):
    cmd = "python3 copy_prometheus_data.py --cloudlab_username {} --hostname_suffix {} --output_dir {} --node_offset {}".format(
        username,
        hostname_suffix,
        os.path.join(
            constants.LOCAL_EXPERIMENT_DIR, experiment_name, "raw_prometheus_data"
        ),
        node_offset,
    )
    subprocess.run(cmd, shell=True, check=True)


def rsync_experiment_data(username, hostname_suffix, experiment_name, node_offset):
    cmd = f'rsync -azh -e "ssh {constants.SSH_OPTIONS}" {username}@node{node_offset}.{hostname_suffix}:{constants.CLOUDLAB_HOME_DIR}/experiments/{experiment_name} {constants.LOCAL_EXPERIMENT_DIR}/'
    print(cmd)
    subprocess.run(cmd, shell=True, check=True)


def main(args):
    local_experiment_dir = os.path.join(
        constants.LOCAL_EXPERIMENT_DIR, args.experiment_name
    )
    os.makedirs(local_experiment_dir, exist_ok=True)

    stop_monitoring(
        args.num_nodes, args.cloudlab_username, args.hostname_suffix, args.node_offset
    )
    stop_deathstar(
        args.num_nodes, args.cloudlab_username, args.hostname_suffix, args.node_offset
    )
    reset_prometheus(
        args.num_nodes, args.cloudlab_username, args.hostname_suffix, args.node_offset
    )

    experiment_output_dir = (
        f"{constants.CLOUDLAB_HOME_DIR}/experiments/{args.experiment_name}"
    )
    utils.run_on_cloudlab_nodes_in_parallel(
        range(args.node_offset, args.node_offset + args.num_nodes + 1),
        args.cloudlab_username,
        args.hostname_suffix,
        f"mkdir -p {experiment_output_dir}",
        "",
        nohup=False,
        popen=True,
    )
    utils.run_on_cloudlab_node(
        args.node_offset,
        args.cloudlab_username,
        args.hostname_suffix,
        "mkdir -p {}".format(os.path.dirname(constants.CLOUDLAB_QUERY_LOG_FILE)),
        "",
        nohup=False,
        popen=False,
    )

    num_nodes_in_experiment = args.num_nodes

    generate_and_copy_config(num_nodes_in_experiment, local_experiment_dir)
    rsync_config(args.cloudlab_username, args.hostname_suffix, args.node_offset)
    start_deathstar(
        num_nodes_in_experiment,
        args.cloudlab_username,
        args.hostname_suffix,
        args.node_offset,
    )
    start_monitoring(
        args.num_nodes,
        args.cloudlab_username,
        args.hostname_suffix,
        experiment_output_dir,
        args.node_offset,
    )
    # this blocks until the workload is done
    run_deathstar_workload(
        num_nodes_in_experiment,
        args.cloudlab_username,
        args.hostname_suffix,
        experiment_output_dir,
        local_experiment_dir,
        args.node_offset,
        random_params=False,
    )
    # prometheus returns zero metrics if we don't wait
    time.sleep(60)
    copy_prometheus_data(
        args.cloudlab_username,
        args.hostname_suffix,
        args.experiment_name,
        args.node_offset,
    )
    export_prometheus_data(
        args.cloudlab_username,
        args.hostname_suffix,
        experiment_output_dir,
        args.node_offset,
    )
    stop_monitoring(
        args.num_nodes, args.cloudlab_username, args.hostname_suffix, args.node_offset
    )
    stop_deathstar(
        num_nodes_in_experiment,
        args.cloudlab_username,
        args.hostname_suffix,
        args.node_offset,
    )
    reset_prometheus(
        args.num_nodes, args.cloudlab_username, args.hostname_suffix, args.node_offset
    )
    rsync_experiment_data(
        args.cloudlab_username,
        args.hostname_suffix,
        args.experiment_name,
        args.node_offset,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment_name", type=str, required=True)
    parser.add_argument("--num_nodes", type=int, required=True)
    parser.add_argument("--cloudlab_username", type=str, required=True)
    parser.add_argument("--hostname_suffix", type=str, required=True)
    parser.add_argument("--node_offset", type=int, required=True)
    args = parser.parse_args()
    main(args)
