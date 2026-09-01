#!/bin/bash

set -e

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
PARENT_DIR=$(dirname "$THIS_DIR")

source "$HOME/.cargo/env"

echo "Building QueryEngine Rust binary..."
cd "$PARENT_DIR"
cargo build --release -p query_engine_rust
cp ../target/release/query_engine_rust ../target/release/query_engine_rust_normal

echo "Building frame-pointer QueryEngine Rust binary..."
fp_rustflags="${RUSTFLAGS:-}"
fp_rustflags="${fp_rustflags:+$fp_rustflags }-C force-frame-pointers=yes"
RUSTFLAGS="$fp_rustflags" cargo build --release -p query_engine_rust
mv ../target/release/query_engine_rust ../target/release/query_engine_rust_fp
mv ../target/release/query_engine_rust_normal ../target/release/query_engine_rust

echo "Built normal and frame-pointer QueryEngine Rust binaries."

echo "Building QueryEngine Rust Docker image..."
cd "$(dirname "$PARENT_DIR")"
docker build . -f asap-query-engine/Dockerfile -t sketchdb-queryengine-rust:latest

echo "QueryEngine Rust Docker image built successfully: sketchdb-queryengine-rust:latest"
