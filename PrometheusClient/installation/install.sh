#!/bin/bash

set -e

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
PARENT_DIR=$(dirname "$THIS_DIR")

echo "Building PrometheusClient Docker image..."
cd "$PARENT_DIR"
docker build . -f Dockerfile -t sketchdb-prometheusclient:latest

echo "PrometheusClient Docker image built successfully: sketchdb-prometheusclient:latest"
