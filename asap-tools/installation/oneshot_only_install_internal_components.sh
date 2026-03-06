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
source "$THIS_DIR/../cloudlab_setup/multi_node/utils.sh"

OPTIONS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
HOSTNAME_PREFIX="node"

# Create a wrapper script to execute the installation command
install_internal_cmd() {
    local username="$1"
    local hostname="$2"
    local cd_cmd="cd /scratch/sketch_db_for_prometheus/code/asap-tools/installation"
    local internal_cmd="./only_install_internal_components.sh all"

    ssh $OPTIONS $username@$hostname "$cd_cmd && $internal_cmd"
}

echo "Installing internal components on all nodes..."
setup_nodes $NUM_NODES $USERNAME $HOSTNAME_PREFIX $HOSTNAME_SUFFIX "install_internal_cmd" true $NODE_OFFSET

echo "Internal components installed on all nodes."
