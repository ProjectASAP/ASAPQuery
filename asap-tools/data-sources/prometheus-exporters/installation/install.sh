#!/bin/bash

set -e

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
PROMETHEUS_EXPORTERS_DIR=$(dirname "$THIS_DIR")

echo "Building PrometheusExporters Docker images..."

# Build cluster data exporter
echo "Building Cluster Data Exporter..."
(
    cd "$PROMETHEUS_EXPORTERS_DIR/cluster_data_exporter/installation"
    ./install.sh
)

# Build fake exporter python
echo "Building Fake Exporter Python..."
(
    cd "$PROMETHEUS_EXPORTERS_DIR/fake_exporter/fake_exporter_python/installation"
    ./install.sh
)

# Build fake exporter rust
echo "Building Fake Exporter Rust..."
(
    cd "$PROMETHEUS_EXPORTERS_DIR/fake_exporter/fake_exporter_rust/fake_exporter/installation"
    ./install.sh
)

# Build fake kafka exporter
echo "Building Fake Kafka Exporter..."
(
    cd "$PROMETHEUS_EXPORTERS_DIR/fake_kafka_exporter/installation"
    ./install.sh
)

echo "All PrometheusExporters Docker images built successfully!"
