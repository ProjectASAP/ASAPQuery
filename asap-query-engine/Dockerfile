# QueryEngine Rust/Dockerfile
# Multi-stage build for Rust application

FROM rust:1.89 AS builder

LABEL maintainer="SketchDB Team"
LABEL description="QueryEngine Rust service for SketchDB"

WORKDIR /code

# protobuf-compiler: prost-build (e.g., asap_sketchlib). cmake, libclang-dev:
# HiGHS, built through the asap_planner dependency.
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    cmake \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the asap-common directory
COPY asap-common ./asap-common

COPY Cargo.toml ./
COPY Cargo.lock ./
COPY asap-query-engine/Cargo.toml ./asap-query-engine/
COPY asap-adapters/Cargo.toml ./asap-adapters/
COPY asap-planner-rs/Cargo.toml ./asap-planner-rs/
COPY rqe-optimizer/Cargo.toml ./rqe-optimizer/
COPY rqe-optimizer/src ./rqe-optimizer/src

# Create dummy source files so Cargo can resolve all workspace members
# All explicit [[bin]] targets in asap-adapters/Cargo.toml must have stubs here for the dependency cache layer
RUN mkdir -p asap-query-engine/src asap-adapters/src/bin \
    && echo "pub fn placeholder() {}" > asap-query-engine/src/lib.rs \
    && echo "pub fn placeholder() {}" > asap-adapters/src/lib.rs \
    && echo "fn main() {}" > asap-adapters/src/main.rs \
    && echo "fn main() {}" > asap-adapters/src/bin/precompute_engine.rs \
    && echo "fn main() {}" > asap-adapters/src/bin/test_e2e_precompute.rs \
    && echo "fn main() {}" > asap-adapters/src/bin/bench_precompute_sketch.rs \
    && echo "fn main() {}" > asap-adapters/src/bin/bench_ingest_sender.rs \
    && echo "fn main() {}" > asap-adapters/src/bin/e2e_quickstart_resource_test.rs \
    && mkdir -p asap-planner-rs/src && echo "fn main() {}" > asap-planner-rs/src/main.rs \
    && echo "pub fn placeholder() {}" >> asap-planner-rs/src/lib.rs

# Build dependencies (this layer will be cached)
WORKDIR /code/asap-adapters
RUN cargo build --release && rm -rf src/ /code/asap-query-engine/src/

# Copy source code
COPY asap-query-engine/src /code/asap-query-engine/src
COPY asap-adapters/src ./src
COPY asap-planner-rs/src /code/asap-planner-rs/src

# Build the actual application
RUN touch src/main.rs src/lib.rs /code/asap-query-engine/src/lib.rs /code/asap-planner-rs/src/lib.rs \
    && cargo build --release

# Runtime stage with Ubuntu 24.04 (has newer glibc/libstdc++)
FROM ubuntu:24.04

WORKDIR /app

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/*

# Copy the built binary
COPY --from=builder /code/target/release/query_engine_rust /usr/local/bin/query_engine_rust

# Expose the HTTP server port
EXPOSE 8088

# Note: Running as root to match Python QueryEngine behavior
# This allows writing to mounted volumes without permission issues

# Use ENTRYPOINT to allow passing command line arguments
ENTRYPOINT ["query_engine_rust"]
