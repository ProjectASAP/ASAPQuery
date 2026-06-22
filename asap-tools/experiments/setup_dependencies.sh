#!/bin/bash

# Installs the dependencies needed to run the experiment orchestrators
# (experiment_run_e2e.py, experiment_run_clickhouse.py, etc.) from the local
# machine. These scripts only drive remote CloudLab nodes over ssh/rsync; all
# component installation on the nodes themselves is handled by
# asap-tools/deploy_from_scratch.sh.

set -e

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")

sudo apt-get update
sudo apt-get install -y python3-pip rsync openssh-client
pip3 install --user -r "${THIS_DIR}/requirements.txt"

# promql_utilities (used by post_experiment/) isn't on PyPI, install local editable copy
pip3 install --user -e "${THIS_DIR}/../../asap-common/dependencies/py/promql_utilities"
