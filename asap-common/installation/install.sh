#!/bin/bash

# Build script for SketchDB shared base image
# This script builds the base image that contains common dependencies

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"

# Image name and tag
IMAGE_NAME="sketchdb-base"
IMAGE_TAG="latest"
FULL_IMAGE_NAME="${IMAGE_NAME}:${IMAGE_TAG}"

echo "Building SketchDB base image: $FULL_IMAGE_NAME"
echo "Build context: $BASE_DIR"

# Build the base image
docker build \
    -t "$FULL_IMAGE_NAME" \
    -f "$SCRIPT_DIR/Dockerfile" \
    "$BASE_DIR"

echo "Successfully built base image: $FULL_IMAGE_NAME"

echo "Base image build complete!"
echo "Services can now use: FROM $FULL_IMAGE_NAME"
