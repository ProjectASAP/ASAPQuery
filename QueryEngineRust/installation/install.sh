#!/bin/bash

set -e

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
PARENT_DIR=$(dirname "$THIS_DIR")

source "$HOME/.cargo/env"

echo "Building QueryEngine Rust binary..."
cd "$PARENT_DIR"
cargo build --release

echo "Building QueryEngine Rust Docker image..."
cd "$(dirname "$PARENT_DIR")"
docker build . -f QueryEngineRust/Dockerfile -t sketchdb-queryengine-rust:latest

echo "QueryEngine Rust Docker image built successfully: sketchdb-queryengine-rust:latest"
