#!/bin/bash

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")

sudo apt-get install -y python3-pip
# TODO: change to virtualenv
pip3 install --user -r "${THIS_DIR}/requirements.txt"
(
  cd "${THIS_DIR}/../dependencies/py/promql_utilities" || exit
  pip3 install --user -e .
)
