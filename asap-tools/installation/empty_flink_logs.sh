#!/bin/bash

if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    echo "Usage: $0 <cloudlab_username> <hostname_suffix> [<node_offset>]"
    exit 1
fi

USERNAME=$1
HOSTNAME_SUFFIX=$2
NODE_OFFSET=${3:-0}

THIS_DIR=$(dirname "$(readlink -f "$0")")

CMD="cd /scratch/sketch_db_for_prometheus/flink/log; "
CMD=$CMD"for file in *; do > \$file; done;"

echo "Running command: $CMD"

ssh -o StrictHostKeyChecking=no $USERNAME@"node$NODE_OFFSET."$HOSTNAME_SUFFIX "$CMD" < /dev/null &

wait
