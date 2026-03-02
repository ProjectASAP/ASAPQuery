#!/bin/bash

set -e

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
PARENT_DIR=$(dirname "$THIS_DIR")

echo "Building Cluster Data Exporter Docker image..."
cd "$PARENT_DIR"
docker build . -f Dockerfile -t sketchdb-cluster-data-exporter:latest

echo "Cluster Data Exporter Docker image built successfully: sketchdb-cluster-data-exporter:latest"