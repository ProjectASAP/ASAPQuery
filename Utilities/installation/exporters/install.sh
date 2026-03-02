#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <install_dir>"
  exit 1
fi

# node_exporter
NODE_EXPORTER_FILENAME="node_exporter-1.8.2.linux-amd64.tar.gz"
NODE_EXPORTER_URL="https://github.com/prometheus/node_exporter/releases/download/v1.8.2/"$NODE_EXPORTER_FILENAME
NODE_EXPORTER_DIRNAME="node_exporter"

# blackbox_exporter
BLACKBOX_EXPORTER_FILENAME="blackbox_exporter-0.25.0.linux-amd64.tar.gz"
BLACKBOX_EXPORTER_URL="https://github.com/prometheus/blackbox_exporter/releases/download/v0.25.0/"$BLACKBOX_EXPORTER_FILENAME
BLACKBOX_EXPORTER_DIRNAME="blackbox_exporter"

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
source "$THIS_DIR/../utils.sh"

INSTALL_DIR=$1

cd $INSTALL_DIR
mkdir -p exporters
cd exporters

wget $NODE_EXPORTER_URL
# rm -rf $NODE_EXPORTER_DIRNAME; mkdir $NODE_EXPORTER_DIRNAME
untar $NODE_EXPORTER_FILENAME $NODE_EXPORTER_DIRNAME

wget $BLACKBOX_EXPORTER_URL
# rm -rf $BLACKBOX_EXPORTER_DIRNAME; mkdir $BLACKBOX_EXPORTER_DIRNAME
untar $BLACKBOX_EXPORTER_FILENAME $BLACKBOX_EXPORTER_DIRNAME
