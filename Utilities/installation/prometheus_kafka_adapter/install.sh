#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 <path_to_prometheus_kafka_adapter>"
    exit 1
fi

DIR=$1

PROM_KAFKA_ADAPTER_REPO="https://github.com/Telefonica/prometheus-kafka-adapter.git"
PROM_KAFKA_ADAPTER_DIRNAME="prometheus-kafka-adapter"

cd $DIR
rm -rf $PROM_KAFKA_ADAPTER_DIRNAME
git clone $PROM_KAFKA_ADAPTER_REPO
cd $PROM_KAFKA_ADAPTER_DIRNAME

# sudo su shenanigans are required because
# (a) build-musl needs docker,
# (b) docker usermod -aG only takes effect in a new shell, and
# (c) I want to do this in a single SSH connection
sudo su - $USER -c 'pwd; cd '$PWD'; pwd; make build-musl'
