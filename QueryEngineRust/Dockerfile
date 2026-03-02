# QueryEngine Rust/Dockerfile
# Multi-stage build for Rust application

FROM rust:1.89 AS builder

LABEL maintainer="SketchDB Team"
LABEL description="QueryEngine Rust service for SketchDB"

WORKDIR /code

# Copy the CommonDependencies directory
COPY CommonDependencies ./CommonDependencies

# Copy path dependencies of QueryEngineRust
COPY sketch-core ./sketch-core

COPY Cargo.toml ./
COPY Cargo.lock ./
COPY QueryEngineRust/Cargo.toml ./QueryEngineRust/
# COPY QueryEngineRust/.cargo ./QueryEngineRust/.cargo

# Create a dummy main.rs to build dependencies
RUN mkdir -p QueryEngineRust/src && echo "fn main() {}" > QueryEngineRust/src/main.rs

# Build dependencies (this layer will be cached)
# Uses BuildKit secret mount to pass git credentials without baking into a layer
WORKDIR /code/QueryEngineRust
RUN cargo build --release && rm -rf src/
# RUN --mount=type=secret,id=git_token \
#     if [ -f /run/secrets/git_token ]; then \
#       git config --global url."https://x-access-token:$(cat /run/secrets/git_token)@github.com/".insteadOf "https://github.com/"; \
#     fi && \
#     cargo build --release && rm -rf src/

# Copy source code
COPY QueryEngineRust/src ./src

# Build the actual application
RUN touch src/main.rs && cargo build --release
# RUN --mount=type=secret,id=git_token \
#     if [ -f /run/secrets/git_token ]; then \
#       git config --global url."https://x-access-token:$(cat /run/secrets/git_token)@github.com/".insteadOf "https://github.com/"; \
#     fi && \
#     touch src/main.rs && cargo build --release

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
