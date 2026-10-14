#!/bin/bash

set -e

THIS_DIR=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")

echo "Building paced_feeder..."
source "$HOME/.cargo/env"
cd "$(dirname "$THIS_DIR")"
cargo build --release

echo "paced_feeder built: $(pwd)/target/release/paced_feeder"
