# Deployment Guide

This guide covers the deployment scripts and infrastructure setup for CloudLab experiments.

## Overview

The deployment system consists of two main scripts:
- **`deploy_from_scratch.sh`**: Complete initial setup (~1 hour)
- **`deploy_changes.sh`**: Incremental updates (< 1 min)

All deployment scripts are **Linux-only** and have been tested on Ubuntu. They are not compatible with macOS.

## Table of Contents

- [Initial Deployment](#initial-deployment)
- [Incremental Deployment](#incremental-deployment)
- [Component Configuration](#component-configuration)
- [Deployment Architecture](#deployment-architecture)
- [What Gets Installed](#what-gets-installed)

## Initial Deployment

### deploy_from_scratch.sh

**Purpose:** Complete CloudLab cluster setup from scratch

**Time:** Approximately 1 hour

**Usage:**
```bash
cd $REPO_ROOT/asap-tools
./deploy_from_scratch.sh <num_nodes> <cloudlab_username> <hostname_suffix>

# Example:
./deploy_from_scratch.sh 10 myuser sketchdb.cloudmigration-PG0.utah.cloudlab.us
```

### What It Does

The script executes these phases in order:

#### Phase 1: Storage Setup
- Runs on all CloudLab nodes in parallel
- Configures `/scratch` volume using CloudLab's `mkextrafs.pl` script
- Creates directory structure: `/scratch/sketch_db_for_prometheus/`

#### Phase 2: Code Sync
- Runs on all CloudLab nodes in parallel
- Rsyncs all component repositories from local machine to `/scratch/sketch_db_for_prometheus/code/`
- Respects `.rsyncignore` files in each component

#### Phase 3: External Components Installation
- Runs on all CloudLab nodes in parallel
- Installs third-party software:
  - Common dependencies (Python 3.11, Rust, Go, Docker)
  - Prometheus v2.53.2
  - Kafka v3.8.0
  - Flink v1.20.0
  - Grafana
  - Benchmark tools and exporters

#### Phase 4: Internal Components Installation
- Runs on all CloudLab nodes in parallel
- Builds and installs project-specific code:
  - asap-common (base Docker image)
  - asap-query-engine (Rust binary + Docker image)
  - asap-planner-rs (Rust binary + Docker image)
  - Arroyo (Node.js frontend + Rust binary + Docker image)
  - asap-summary-ingest (Python scripts)
  - asap-tools/queriers/prometheus-client, asap-tools/data-sources/prometheus-exporters, asap-tools/execution-utilities, asap-tools/prometheus-benchmark

### Directory Structure Created

```
/scratch/sketch_db_for_prometheus/
├── code/                           # All component source code
│   ├── asap-tools/
│   ├── asap-query-engine/
│   ├── asap-planner-rs/
│   ├── asap-summary-ingest/
│   ├── arroyo/
│   ├── asap-common/
│   └── asap-tools/prometheus-benchmark/
├── prometheus/                     # Prometheus binaries
├── kafka/                          # Kafka installation
├── flink/                          # Flink installation
└── experiment_outputs/             # Experiment results
```

## Incremental Deployment

### deploy_changes.sh

**Purpose:** Deploy only changed code to CloudLab nodes

**Time:** Approximately 1 min

**Usage:**
```bash
cd $REPO_ROOT/asap-tools

# Make your code changes
vim asap-query-engine/src/main.rs

# Deploy changes
./deploy_changes.sh <num_nodes> <cloudlab_username> <hostname_suffix>

# Example:
./deploy_changes.sh 10 myuser sketchdb.cloudmigration-PG0.utah.cloudlab.us
```

### How It Works

1. **Intelligent Syncing:**
   - Runs rsync with `--itemize-changes` flag
   - Tracks which files actually changed
   - Parses rsync output to identify affected components

2. **Selective Installation:**
   - Only reinstalls components that had file changes
   - Skips Utilities (has no installation step)
   - Much faster than full rebuild

3. **Example Scenario:**
   ```bash
   # Edit asap-query-engine only
   vim asap-query-engine/src/query_executor.rs

   # Deploy changes
   ./deploy_changes.sh 10 myuser sketchdb.utah.cloudlab.us

   # Result: Only asap-query-engine is rebuilt and reinstalled
   # Takes ~5 minutes instead of 60 minutes
   ```

## Component Configuration

### components.conf

Located at: `asap-tools/components.conf`

This file defines which components get synced to CloudLab. Each component must be a directory in `$REPO_ROOT/`.

**Currently Enabled Components:**
```bash
asap-tools
asap-common
sketchlib-rust
asap-query-engine
asap-planner-rs
asap-summary-ingest
asap-quickstart
asap-tools/data-sources/prometheus-exporters
asap-tools/queriers/prometheus-client
asap-tools/execution-utilities
```

**Deprecated Components (commented out):**
```bash
# FlinkSketch               # Legacy Flink implementation
# QueryEngine               # Python version (replaced by Rust)
# prometheus-kafka-adapter  # Legacy Kafka adapter
```

### Component Structure

Each component directory can contain:
- `.rsyncignore`: Files/directories to exclude from sync (like `.gitignore`)
- `installation/install.sh`: Installation script
- `installation/setup_dependencies.sh`: Dependency setup (optional)

## Deployment Architecture

### Parallelization

All node operations run in parallel to maximize speed:
- Storage setup runs on all nodes simultaneously
- Rsync runs to all nodes simultaneously
- Installation runs on all nodes simultaneously

This is achieved using the `setup_nodes()` function in `cloudlab_setup/multi_node/utils.sh`.

### Node Addressing

Nodes are addressed as:
- Hostnames: `node0.<suffix>`, `node1.<suffix>`, ..., `node{N-1}.<suffix>`
- Internal IPs: `10.10.1.1`, `10.10.1.2`, ..., `10.10.1.N`
- Node 0 is always the **coordinator node**

### SSH Key Authentication

All scripts use SSH key-based authentication:
```bash
ssh <username>@node<idx>.<hostname_suffix>
```

Ensure your SSH keys are properly configured before deployment.

## What Gets Installed

### External Components

These are third-party software installed from source or packages:

#### 1. Common Dependencies
- **Python 3.11** with pip
- **Rust** (latest stable)
- **Go** (latest stable)
- **Docker & Docker Compose**
- Build tools: make, gcc, g++, cmake
- Libraries: libssl-dev, pkg-config, etc.
- Rust fake exporter (built from source)

#### 2. Prometheus v2.53.2
- Downloaded from official releases
- Extracted to `/scratch/prometheus/`
- Configured with custom scrape configs per experiment

#### 3. Kafka v3.8.0
- Downloaded from Apache mirrors
- Configured in KRaft mode (no ZooKeeper)
- Settings:
  - Log retention: 10 minutes
  - Max message size: 10 MB
  - Replication factor: 1

#### 4. Flink v1.20.0
- Downloaded from Apache mirrors
- Extracted to `/scratch/flink/`
- Configured for cluster mode

#### 5. Grafana
- Installed via setup scripts
- Used for visualization dashboards

#### 6. Benchmarks & Exporters
- Avalanche (high-cardinality load generator)
- prometheus-benchmark tools
- asprof (profiling tools)

### Internal Components

These are project-specific components built as Docker images or binaries:

#### 1. asap-common
**What:** Base Docker image with shared dependencies
**Build Process:**
```bash
cd asap-common
docker build -t sketchdb-base:latest .
```
**Purpose:** Shared base layer for other Docker images to reduce build time

#### 2. asap-query-engine
**What:** Rust-based query engine for executing PromQL queries over sketches
**Build Process:**
```bash
cd asap-query-engine
cargo build --release  # Compile Rust binary
docker build -t sketchdb-queryengine-rust:latest .  # Build Docker image
```
**Deployment:** Can run as Docker container or bare-metal binary

#### 3. asap-planner-rs
**What:** Rust-based service that generates sketch configurations based on query patterns
**Build Process:**
```bash
cd asap-planner-rs
cargo build --release -p asap_planner
docker build . -f Dockerfile -t sketchdb-controller:latest
```
**Deployment:** Docker container only

#### 4. arroyo
**What:** Streaming engine for processing metrics and building sketches
**Build Process:**
```bash
cd arroyo
# Install Node.js 18 via NVM
nvm install 18
# Install pnpm
npm install -g pnpm
# Build frontend
cd crates/arroyo-console
pnpm install
pnpm build
cd ../..
# Compile Rust binary
cargo build --release
# Build Docker image
docker build -t arroyo-full:latest .
# Install refinery CLI for migrations
cargo install refinery_cli
```
**Components:**
- PostgreSQL database (for metadata)
- Web console (Node.js/React frontend)
- Controller and workers (Rust binaries)

#### 5. asap-summary-ingest
**What:** Python scripts for deploying asap-summary-ingest pipelines
**Build Process:**
```bash
cd asap-summary-ingest
pip install -r requirements.txt  # jinja2 for templating
```
**Deployment:** Python scripts, no Docker image

#### 6. asap-tools/queriers/prometheus-client
**What:** Client for executing PromQL queries against Prometheus or SketchDB
**Build Process:** Language-specific (Python or Rust)

#### 7. asap-tools/data-sources/prometheus-exporters, asap-tools/execution-utilities, asap-tools/prometheus-benchmark
**What:** Various testing and monitoring tools
**Build Process:** Component-specific

## Deployment Scripts Reference

### Main Scripts

| Script | Location | Purpose |
|--------|----------|---------|
| `deploy_from_scratch.sh` | `asap-tools/` | Full deployment |
| `deploy_changes.sh` | `asap-tools/` | Incremental updates |
| `oneshot_setup.sh` | `cloudlab_setup/multi_node/` | Storage + rsync |
| `oneshot_rsync_and_selective_install_internal.sh` | `cloudlab_setup/multi_node/` | Incremental sync + install |

### Utility Scripts

| Script | Location | Purpose |
|--------|----------|---------|
| `setup_storage.sh` | `cloudlab_setup/single_node/` | Configure /scratch volume |
| `rsync.sh` | `cloudlab_setup/single_node/` | Rsync code to node |
| `rsync_and_selective_install_internal.sh` | `cloudlab_setup/single_node/` | Rsync + detect changes |
| `install_components.sh` | `installation/` | Generic component installer |
| `install_external_components.sh` | `installation/` | Install external software |
| `setup_internal_components.sh` | `installation/` | Install internal components |
| `only_install_internal_components.sh` | `installation/` | Reinstall specific components |

### Helper Scripts

| Script | Location | Purpose |
|--------|----------|---------|
| `shared_utils.sh` | `asap-tools/` | Shared functions (rsync, component loading) |
| `utils.sh` | `cloudlab_setup/multi_node/` | Parallel execution helper |
| `constants.sh` | `cloudlab_setup/multi_node/` | Hostname configuration |
| `constants.sh` | `cloudlab_setup/single_node/` | Remote path configuration |

## Troubleshooting Deployment

### Issue: deploy_from_scratch.sh times out

**Cause:** CloudLab nodes may have slow internet or package downloads are failing

**Solutions:**
- Check CloudLab node internet connectivity: `ssh user@node0.suffix && ping google.com`
- Try deployment again (some downloads may have cached)
- Check for specific error messages in terminal output

### Issue: deploy_changes.sh doesn't rebuild component

**Cause:** Rsync didn't detect changes (file timestamps unchanged)

**Solutions:**
```bash
# Force rebuild by touching files
cd $REPO_ROOT/asap-query-engine
touch src/main.rs

# Run deploy_changes again
cd $REPO_ROOT/asap-tools
./deploy_changes.sh 10 myuser sketchdb.utah.cloudlab.us
```

OR

Run `deploy_from_scratch.sh`

## Best Practices

1. **Always use deploy_changes.sh for iterative development**
   - Much faster than full deployment
   - Only rebuilds what changed

2. **Verify node accessibility before deployment**
   ```bash
   ssh myuser@node0.sketchdb.utah.cloudlab.us echo "OK"
   ```

3. **Keep components.conf up to date**
   - Only include components you actually need
   - Comment out deprecated components

4. **Use .rsyncignore files**
   - Exclude build artifacts: `target/`, `node_modules/`, `.git/`
   - Reduces sync time and storage usage

5. **Monitor disk usage**
   ```bash
   # Check disk usage on all nodes
   for i in {0..9}; do
     ssh user@node$i.suffix df -h /scratch
   done
   ```

6. **Clean up after experiments**
   - Delete old experiment outputs
   - Prune Docker images regularly

## Known Problems

1. Modifying `asap-common` and then using `deploy_changes.sh` will only rebuild the `sketchdb-base` Docker image, not all the images that depend on this.
Workaround: Run `deploy_from_scratch.sh`

## Advanced Usage

### Skipping Component Installation

To sync code without reinstalling:

```bash
cd cloudlab_setup/multi_node
./oneshot_only_rsync.sh <num_nodes> <username> <suffix>
```
