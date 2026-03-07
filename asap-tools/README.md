# Experiment Framework

A framework for running end-to-end distributed systems experiments on CloudLab, comparing SketchDB (sketch-based query engine) against traditional Prometheus.

**Platform Support:** Linux only (tested on Ubuntu). Not compatible with macOS.

## Quick Start

### Prerequisites

- **Local Machine:**
  - Ubuntu
  - Python 3.8+
  - Local clone of `ASAPQuery` repo (at `$REPO_ROOT`)

- **CloudLab:**
  - Active experiment with N nodes
  - Nodes named: `node0.<suffix>`, `node1.<suffix>`, ..., `node{N-1}.<suffix>`

### Initial Setup (from scratch)

This takes approximately 1 hour and sets up all infrastructure on CloudLab nodes.

```bash
cd $$REPO_ROOT/asap-tools
./deploy_from_scratch.sh <num_nodes> <cloudlab_username> <hostname_suffix>

# Example:
./deploy_from_scratch.sh 10 myuser sketchdb.cloudmigration-PG0.utah.cloudlab.us
```

**Note**: The output of this script is dumped to stdout/stderr and not logged to any file. Redirect output if you wish to.

**What it does:**
- Configures storage on all CloudLab nodes
- Rsyncs all code from local machine to CloudLab
- Installs external components (Prometheus, Kafka, Flink, Docker, etc.)
- Builds internal components (Docker images, Rust binaries)

### Running an Experiment

**Important:** Must be run from the `asap-tools/experiments/` directory.

```bash
cd $$REPO_ROOT/asap-tools/experiments

python experiment_run_e2e.py \
  experiment_type=<experiment_type> \
  experiment.name=<experiment_name> \
  cloudlab.num_nodes=<num_worker_nodes> \
  cloudlab.username=<your_username> \
  cloudlab.hostname_suffix=<cloudlab_suffix>
```

#### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `experiment_type` | Which experiment configuration to use | `simple_config`, `cloud_demo` |
| `experiment.name` | Unique name for this experiment run | `my_test_run` |
| `cloudlab.num_nodes` | Number of worker nodes (not including coordinator) | `9` (for 10-node cluster) |
| `cloudlab.username` | Your CloudLab username | `myuser` |
| `cloudlab.hostname_suffix` | CloudLab experiment hostname suffix | `sketchdb.utah.cloudlab.us` |

**Note**: For a cloudlab cluster of `N` nodes, specify `cloudlab.num_nodes=N-1`

#### Example: Simple Test Run

```bash
cd experiments

python experiment_run_e2e.py \
  experiment_type=simple_config \
  experiment.name=my_first_test \
  cloudlab.num_nodes=9 \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=sketchdb.cloudmigration-PG0.utah.cloudlab.us
```

**Results:** Automatically rsynced to `$REPO_ROOT/experiment_outputs/my_first_test/`

### Deploying Code Changes

After making code changes locally, use `deploy_changes.sh` to sync and rebuild only what changed:

```bash
# Make your changes
vim asap-query-engine/src/main.rs

# Deploy changes
cd $REPO_ROOT/asap-tools
./deploy_changes.sh <num_nodes> <cloudlab_username> <hostname_suffix>

# Example:
./deploy_changes.sh 10 myuser sketchdb.cloudmigration-PG0.utah.cloudlab.us
```

## Common Usage Patterns

### Comparing cost/latency for ASAP vs Prometheus

In your experiment config yaml file (e.g. `$REPO_DIR/asap-tools/experiments/config/experiment_type/simple_config.yaml`), set:

```yaml
experiments:
- mode: sketchdb
- mode: prometheus
```

With this config, 2 experiments are run independently. In the first experiment, `asap-tools/queriers/prometheus-client` only sends queries to ASAP. After this experiment finishes, the infra is torn down. Then the second experiment is set up and `asap-tools/queriers/prometheus-client` sends queries only to Prometheus directly. In the second experiment (i.e. when `mode=prometheus`), none of ASAP's components are set up (apart from `asap-tools/queriers/prometheus-client`).

Post-experiment analysis:
- Use `compare_costs.py` and `compare_latencies.py` from `$REPO_DIR/asap-tools/experiments/post_experiments/`.
- `run_compare_latencies.sh` is an easy wrapper around `compare_latencies.py`

### Comparing query accuracy for ASAP vs Prometheus

```yaml
experiments:
- mode: sketchdb
  query_prometheus_too: true
# DO NOT NEED mode: prometheus
```

With this config, only one experiment is run. In the same experiment, `PrometheusClient` sends a query to ASAP and then immediately after that, sends a query to Prometheus too.

Post-experiment analysis:
- Use `calculate_fidelity.py` from `$REPO_DIR/asap-tools/experiments/post_experiments/`.
- `run_calculate_fidelity.sh` is an easy wrapper around `calculate_fidelity.py`

### Debugging with Verbose Logging

```bash
python experiment_run_e2e.py \
  experiment_type=simple_config \
  experiment.name=debug_test \
  cloudlab.num_nodes=9 \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=sketchdb.cloudmigration-PG0.utah.cloudlab.us \
  logging.level=DEBUG \
  flow.no_teardown=true
```

- `logging.level=DEBUG`: Enables verbose output from all services
- `flow.no_teardown=true`: Keeps services running for inspection after experiment

### Running a service as a baremetal process

```bash
# Disable Docker for that service
python experiment_run_e2e.py ... \
  use_container.query_engine=false \
  use_container.arroyo=false
```

### Running Parallel Experiments on a single Cloudlab cluster

Use `node_offset` to run multiple experiments on same cluster:

```bash
# Experiment 1: Uses nodes 0-9
./deploy_from_scratch.sh 10 myuser sketchdb.utah.cloudlab.us

# Experiment 2: Uses nodes 10-19
cd experiments
python experiment_run_e2e.py ... cloudlab.node_offset=10 cloudlab.num_nodes=10
```

## Documentation

- **[Deployment Guide](docs/deployment.md)** - Detailed deployment scripts documentation
- **[Configuration Reference](docs/configuration.md)** - Complete configuration parameter reference
- **[Experiment Types](docs/experiment_types.md)** - Detailed documentation of all experiment configurations
- **[Usage Guide](docs/usage.md)** - Usage patterns and workflows
- **[Architecture](docs/architecture.md)** - System architecture and extension points
- **[Troubleshooting](docs/troubleshooting.md)** - Common issues and debugging tips

## Quick Troubleshooting

**Checking service health**
```bash
# Check service logs
ssh user@node0.suffix
docker ps  # See running containers
docker logs <container_name> -f  # Check logs
```

See [docs/troubleshooting.md](docs/troubleshooting.md) for more detailed debugging help.



## Project Structure

```
asap-tools/
├── deploy_from_scratch.sh       # Initial CloudLab setup
├── deploy_changes.sh            # Incremental code deployment
├── components.conf              # Components to sync
├── cloudlab_setup/              # CloudLab infrastructure setup scripts
├── installation/                # Component installation scripts
├── experiments/
│   ├── experiment_run_e2e.py    # Main experiment orchestrator
│   ├── config/                  # Hydra configuration files
│   │   ├── config.yaml          # Base configuration
│   │   └── experiment_type/     # Experiment-specific configs
│   └── experiment_utils/        # Utilities and services
└── docs/                        # Documentation
```
