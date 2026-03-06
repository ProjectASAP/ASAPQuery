#!/bin/bash

set -e

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
PARENT_DIR=$(dirname "$THIS_DIR")

echo "Building Fake Exporter Rust Docker image..."
cd "$PARENT_DIR"
docker build . -f Dockerfile -t sketchdb-fake-exporter-rust:latest

echo "Fake Exporter Rust Docker image built successfully: sketchdb-fake-exporter-rust:latest"