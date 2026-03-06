# CommonDependencies/Dockerfile
# Shared base image for SketchDB services containing common dependencies and internal packages

FROM python:3.10-slim AS sketchdb-base

LABEL maintainer="SketchDB Team"
LABEL description="Shared base image with common dependencies for SketchDB services"

WORKDIR /app

# Install system dependencies needed across all services
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install promql_utilities (shared internal package)
COPY dependencies/py/promql_utilities /tmp/promql_utilities
RUN pip install --no-cache-dir /tmp/promql_utilities && rm -rf /tmp/promql_utilities

# Install common Python dependencies used across multiple services
COPY installation/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

# Create common directories
RUN mkdir -p /app/logs /app/config /app/outputs

# Set Python path to include app directory
#ENV PYTHONPATH=/app${PYTHONPATH:+:${PYTHONPATH}}
ENV PYTHONPATH=/app

# Default working directory for services
WORKDIR /app

# Default user (can be overridden by services if needed)
# RUN useradd -m -s /bin/bash sketchdb
# USER sketchdb

# Services will inherit from this base image
