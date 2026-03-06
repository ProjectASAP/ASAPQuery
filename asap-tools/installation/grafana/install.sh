#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: $0 <install_dir>"
    exit 1
fi

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
source "$THIS_DIR/../utils.sh"

INSTALL_DIR=$1

GRAFANA_FILENAME="grafana-enterprise-11.2.2.linux-amd64.tar.gz"
GRAFANA_URL="https://dl.grafana.com/enterprise/release/"$GRAFANA_FILENAME
GRAFANA_DIRNAME="grafana"

cd $INSTALL_DIR
wget $GRAFANA_URL
untar $GRAFANA_FILENAME $GRAFANA_DIRNAME
