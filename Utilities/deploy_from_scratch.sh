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
(cd "${THIS_DIR}/cloudlab_setup/multi_node"; "./oneshot_setup.sh" "$NUM_NODES" "$USERNAME" "$HOSTNAME_SUFFIX" "$NODE_OFFSET") || exit 1
(cd "${THIS_DIR}/installation"; "./oneshot_setup.sh" "$NUM_NODES" "$USERNAME" "$HOSTNAME_SUFFIX" "$NODE_OFFSET") || exit 1
