# Use the official Rust image as a build environment
FROM rust:latest AS builder

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./

# Copy the source code
COPY src ./src

# Build the application in release mode
RUN cargo build --release

# Use a minimal runtime image
FROM debian:bookworm-slim

# Install necessary runtime dependencies
RUN apt-get update && apt-get install -y \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -r -s /bin/false exporter

# Create the data and output directories that will be mounted as volumes
RUN mkdir -p /data /output && chown exporter:exporter /data /output

# Copy the binary from the builder stage
COPY --from=builder /usr/src/app/target/release/cluster_data_exporter /usr/local/bin/cluster_data_exporter

# Change to the non-root user
USER exporter

# Set the entrypoint to the binary
# All arguments including port and input directory must be provided via docker run or docker-compose
ENTRYPOINT ["cluster_data_exporter"]
