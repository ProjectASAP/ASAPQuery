#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <install_dir>"
    exit 1
fi

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
source "$THIS_DIR/../utils.sh"

# prometheus
PROMETHEUS_FILENAME="prometheus-2.53.2.linux-amd64.tar.gz"
PROMETHEUS_URL="https://github.com/prometheus/prometheus/releases/download/v2.53.2/"$PROMETHEUS_FILENAME
PROMETHEUS_DIRNAME="prometheus"

INSTALL_DIR=$1

cd $INSTALL_DIR

wget $PROMETHEUS_URL
# rm -rf $PROMETHEUS_DIRNAME; mkdir $PROMETHEUS_DIRNAME
untar $PROMETHEUS_FILENAME $PROMETHEUS_DIRNAME
