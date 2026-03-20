#!/bin/bash

set -e

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
PARENT_DIR=$(dirname "$THIS_DIR")
WORKSPACE_DIR=$(dirname "$PARENT_DIR")

source "$HOME/.cargo/env"

echo "Building asap-planner-rs Rust binary..."
cd "$WORKSPACE_DIR"
cargo build --release -p asap_planner

echo "Building asap-planner-rs Docker image..."
docker build . -f asap-planner-rs/Dockerfile -t sketchdb-controller:latest

echo "asap-planner-rs Docker image built successfully: sketchdb-controller:latest"
