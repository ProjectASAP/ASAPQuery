FROM rust:1.89 AS builder

LABEL maintainer="SketchDB Team"
LABEL description="ASAP Planner (Rust) for SketchDB"

WORKDIR /code

COPY asap-common ./asap-common
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY asap-query-engine/Cargo.toml ./asap-query-engine/
COPY asap-planner-rs/Cargo.toml ./asap-planner-rs/

# Create dummy source files so Cargo can resolve all workspace members
RUN mkdir -p asap-query-engine/src && echo "fn main() {}" > asap-query-engine/src/main.rs && \
    mkdir -p asap-planner-rs/src && echo "fn main() {}" > asap-planner-rs/src/main.rs && \
    echo "pub fn placeholder() {}" >> asap-planner-rs/src/lib.rs

# Build dependencies (this layer will be cached)
WORKDIR /code/asap-planner-rs
RUN cargo build --release && rm -rf src/

# Copy source code
COPY asap-planner-rs/src ./src

# Build the actual application
RUN touch src/main.rs && cargo build --release

FROM ubuntu:24.04

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app/input /app/output

COPY --from=builder /code/target/release/asap-planner /usr/local/bin/asap-planner

ENTRYPOINT ["asap-planner"]
