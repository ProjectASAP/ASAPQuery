#!/bin/bash

set -e

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
PARENT_DIR=$(dirname "$THIS_DIR")

echo "Building Fake Kafka Exporter Docker image..."
cd "$PARENT_DIR"
docker build . -f Dockerfile -t sketchdb-fake-kafka-exporter:latest

echo "Fake Kafka Exporter Docker image built successfully: sketchdb-fake-kafka-exporter:latest"
