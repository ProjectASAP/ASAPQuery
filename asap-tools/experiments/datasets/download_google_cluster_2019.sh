#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <num_nodes> <cloudlab_username> <hostname_suffix>"
    exit 1
fi

num_nodes=$1
USERNAME=$2
HOSTNAME_SUFFIX=$3

CMD="cd /scratch/sketch_db_for_prometheus/code/asap-tools/experiments/datasets; "
CMD=$CMD"curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz; "
CMD=$CMD"tar -xf google-cloud-cli-linux-x86_64.tar.gz; "

for i in $(seq 1 $((num_nodes-1))); do
    echo "Downloading in node$i.$HOSTNAME_SUFFIX"
    CMD_i=$CMD"./google-cloud-sdk/bin/gsutil cp gs://clusterdata_2019_a/instance_usage-0000000000"$i"*.json.gz ./; "
    CMD_i=$CMD_i"gunzip instance_usage-*.json.gz; "
    ssh -o StrictHostKeyChecking=no $USERNAME@node$i.$HOSTNAME_SUFFIX "$CMD_i" < /dev/null &
done
