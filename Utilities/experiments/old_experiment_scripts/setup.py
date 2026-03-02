import argparse

import utils
import constants


def setup_dependencies():
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/cloudlab_scripts"
    cmd = (
        "./setup_dependencies.sh; sudo apt-get update; sudo apt-get install -y python3-pip; sudo pip3 install humanize numpy; sudo usermod -aG docker "
        + args.cloudlab_username
    )
    return cmd, cmd_dir


def setup_exporters():
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/cloudlab_scripts"
    cmd = f"./setup_exporters.sh {constants.CLOUDLAB_HOME_DIR}"
    return cmd, cmd_dir


def setup_benchmarks():
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/cloudlab_scripts"
    cmd = f"./setup_benchmarks.sh {constants.CLOUDLAB_HOME_DIR}"
    return cmd, cmd_dir


def setup_prometheus():
    cmd_dir = f"{constants.CLOUDLAB_HOME_DIR}/cloudlab_scripts"
    cmd = f"./setup_prometheus.sh {constants.CLOUDLAB_HOME_DIR}"
    return cmd, cmd_dir


def main(args):
    # TODO make this parallel
    for node_idx in range(args.num_nodes + 1):
        # local_ip = f"10.10.1.{node_idx + 1}"
        setup_functions = [setup_dependencies, setup_benchmarks, setup_exporters]

        if node_idx == 0:
            setup_functions.append(setup_prometheus)

        for setup_function in setup_functions:
            cmd, cmd_dir = setup_function()
            utils.run_on_cloudlab_node(
                node_idx,
                args.cloudlab_username,
                args.hostname_suffix,
                cmd,
                cmd_dir,
                nohup=False,
                popen=False,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_nodes", type=int, required=True)
    parser.add_argument("--cloudlab_username", type=str, required=True)
    parser.add_argument("--hostname_suffix", type=str, required=True)
    args = parser.parse_args()
    main(args)
