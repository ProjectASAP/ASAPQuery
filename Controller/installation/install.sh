#!/bin/bash

set -e

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
PARENT_DIR=$(dirname "$THIS_DIR")

echo "Building Controller Docker image..."
cd "$PARENT_DIR"
docker build . -f Dockerfile -t sketchdb-controller:latest

echo "Controller Docker image built successfully: sketchdb-controller:latest"
