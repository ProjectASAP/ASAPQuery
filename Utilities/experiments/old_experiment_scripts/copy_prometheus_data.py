import os
import argparse
import subprocess

import constants


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)

    data_to_copy = [
        f"{constants.CLOUDLAB_HOME_DIR}/prometheus/data",
        f"{constants.CLOUDLAB_HOME_DIR}/prometheus/queries.log",
    ]
    for data in data_to_copy:
        cmd = f'rsync -azh -e "ssh {constants.SSH_OPTIONS}" {args.cloudlab_username}@node{args.node_offset}.{args.hostname_suffix}:{data} {args.output_dir}/'
        subprocess.run(cmd, shell=True, check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cloudlab_username", type=str, required=True)
    parser.add_argument("--hostname_suffix", type=str, required=True)
    parser.add_argument("--output_dir", type=str, required=True)
    parser.add_argument("--node_offset", type=int)
    args = parser.parse_args()
    main(args)
