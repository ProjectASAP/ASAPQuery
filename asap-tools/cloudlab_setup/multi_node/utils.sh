#!/bin/bash

setup_nodes() {
    local num_nodes=$1
    local username=$2
    local hostname_prefix=$3
    local hostname_suffix=$4
    local setup_command=$5
    local do_wait=$6
    local node_offset=${7:-0}

    for i in $(seq $node_offset $((node_offset + num_nodes - 1))); do
        local hostname="${hostname_prefix}${i}.${hostname_suffix}"
        echo "Setting up ${hostname}"
        ${setup_command} ${username} ${hostname} < /dev/null &
    done

    if [ "$do_wait" = true ]; then
        wait
    fi
}
