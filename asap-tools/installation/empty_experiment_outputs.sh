#!/bin/bash

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
    echo "Usage: $0 <num_nodes> <cloudlab_username> <hostname_suffix> [<node_offset>]"
    exit 1
fi

NUM_NODES=$1
USERNAME=$2
HOSTNAME_SUFFIX=$3
NODE_OFFSET=${4:-0}

THIS_DIR=$(dirname "$(readlink -f "$0")")

OPTIONS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
CMD="cd /scratch/sketch_db_for_prometheus/experiment_outputs; "
CMD=$CMD"rm -rf *"

echo "Running command: $CMD"
for i in $(seq $NODE_OFFSET $(($NODE_OFFSET + $NUM_NODES - 1))); do
    ssh $OPTIONS "$USERNAME"@node"$i"."$HOSTNAME_SUFFIX" "$CMD" < /dev/null &
done

wait
