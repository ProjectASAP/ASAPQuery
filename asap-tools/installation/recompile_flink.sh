#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <cloudlab_username> <hostname_suffix>"
    exit 1
fi

USERNAME=$1
HOSTNAME_SUFFIX=$2

THIS_DIR=$(dirname "$(readlink -f "$0")")

CMD="cd /scratch/sketch_db_for_prometheus/code/asap-tools/installation; "
CMD=$CMD"./setup_internal_components.sh FlinkSketch;"

echo "Running command: $CMD"

ssh -o StrictHostKeyChecking=no $USERNAME@"node0."$HOSTNAME_SUFFIX "$CMD" < /dev/null &

wait
