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
source $THIS_DIR"/constants.sh"
source $THIS_DIR"/utils.sh"

SINGLE_NODE_DIR="$THIS_DIR/../single_node"

setup_nodes $NUM_NODES $USERNAME $HOSTNAME_PREFIX $HOSTNAME_SUFFIX $SINGLE_NODE_DIR"/setup_storage.sh" true $NODE_OFFSET
setup_nodes $NUM_NODES $USERNAME $HOSTNAME_PREFIX $HOSTNAME_SUFFIX $SINGLE_NODE_DIR"/rsync.sh" true $NODE_OFFSET
