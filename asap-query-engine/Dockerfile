# QueryEngine Rust/Dockerfile
# Multi-stage build for Rust application

FROM rust:1.89 AS builder

LABEL maintainer="SketchDB Team"
LABEL description="QueryEngine Rust service for SketchDB"

WORKDIR /code

# Copy the asap-common directory
COPY asap-common ./asap-common

# Copy path dependencies of asap-query-engine
COPY asap-common/sketch-core ./asap-common/sketch-core

COPY Cargo.toml ./
COPY Cargo.lock ./
COPY asap-query-engine/Cargo.toml ./asap-query-engine/
COPY asap-planner-rs/Cargo.toml ./asap-planner-rs/

# Create dummy source files so Cargo can resolve all workspace members
RUN mkdir -p asap-query-engine/src && echo "fn main() {}" > asap-query-engine/src/main.rs && \
    mkdir -p asap-query-engine/benches && echo "fn main() {}" > asap-query-engine/benches/simple_map_store_benchmark.rs && \
    mkdir -p asap-planner-rs/src && echo "fn main() {}" > asap-planner-rs/src/main.rs && \
    echo "pub fn placeholder() {}" >> asap-planner-rs/src/lib.rs

# Build dependencies (this layer will be cached)
WORKDIR /code/asap-query-engine
RUN cargo build --release && rm -rf src/

# Copy source code
COPY asap-query-engine/src ./src

# Build the actual application
RUN touch src/main.rs && cargo build --release

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
