# Use the official Rust image as a parent image
FROM rust:1.82 AS builder

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./

# Copy the source code
COPY src ./src

# Build the application
RUN cargo build --release

# Set the entrypoint to the fake_kafka_exporter binary
ENTRYPOINT ["target/release/fake_kafka_exporter"]
