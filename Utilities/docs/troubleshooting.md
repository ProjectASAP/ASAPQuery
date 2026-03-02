# Troubleshooting Guide

Common issues and debugging strategies for the experiment framework.

## Table of Contents

- [Deployment Issues](#deployment-issues)
- [Experiment Execution Issues](#experiment-execution-issues)
- [Service Issues](#service-issues)
- [Data Issues](#data-issues)
- [Performance Issues](#performance-issues)
- [Debugging Strategies](#debugging-strategies)
- [Common Error Messages](#common-error-messages)

## Deployment Issues

### Issue: Docker permission errors

**Symptoms:**
- Running `deploy_changes.sh` or an experiment script results in `ERROR: permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock`

**Solutions:**
- Run `sudo usermod -aG docker <cloudlab_username>` on each cloudlab node
- Quick bash command for cloudlab cluster with `N` nodes

```bash
for i in {0..N-1}; do ssh -o StrictHostKeyChecking=no <cloudlab_username>@node"$i".<cloudlab_suffix> "sudo usermod -aG docker <cloudlab_username>"; done;
```

## Experiment Execution Issues

### Issue: Looping over "Waiting for X seconds for remote monitor to finish"

**Symptoms:**
- `experiment_run_e2e.py` script continuously shows "Waiting for X seconds for remote monitor to finish" for multiple minutes (e.g. > 5 min)

**Possible Causes:**
- Could be a variety of causes, but usually indicates something wrong with `PrometheusClient`, `QueryEngineRust`, or `remote_monitor.py`

**Solutions:**
Look at log files for the above components on the cloudlab nodes. You can also check `docker logs`

### Issue: Hydra configuration validation errors

**Symptoms:**
- "Missing required parameter" errors
- "Invalid configuration" errors
- Hydra fails to compose config

**Solutions:**

```bash
# 1. Check all required parameters are provided
python experiment_run_e2e.py \
  experiment_type=simple_config_fake_ports_2_card_20 \
  experiment.name=test \
  cloudlab.num_nodes=9 \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=sketchdb.utah.cloudlab.us

# 2. Validate experiment type exists
ls experiments/config/experiment_type/<your_type>.yaml

# 3. Check config syntax
python -c "import yaml; yaml.safe_load(open('experiments/config/experiment_type/simple_config_fake_ports_2_card_20.yaml'))"

# 4. Use --cfg job to see composed config
python experiment_run_e2e.py \
  experiment_type=simple_config_fake_ports_2_card_20 \
  experiment.name=test \
  cloudlab.num_nodes=9 \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=sketchdb.utah.cloudlab.us \
  --cfg job
```

## Service Issues

### Issue: Kafka message size limit exceeded

**Symptoms:**
- Errors about message size exceeding limits
- Failed to send large precomputes over Kafka
- Messages like "RecordTooLargeException" or "MessageSizeTooLargeException"

**Solutions:**

Kafka has three places where message size limits need to be configured (see [PR #154](https://github.com/ProjectASAP/asap-internal/pull/154)):

1. **Kafka broker configuration** (`Utilities/installation/kafka/install.sh`):
   - Set `message.max.bytes` to desired limit (e.g., `20971520` for 20MB)
   - Set `replica.fetch.max.bytes` to the same limit

2. **Kafka topic configuration** (`Utilities/experiments/experiment_utils/services/kafka.py`):
   - Update `max.message.bytes` in the topic creation command

3. **Arroyo connection profile** (`ArroyoSketch/templates/json/connection_profile.j2`):
   - Add connection properties with `message.max.bytes` and `batch.size`

**Example values:**
- 4MB: `4194304`
- 20MB: `20971520` (default, sufficient for large precomputes like 3x65536 CountMinSketch)
- 100MB: `104857600`

After making changes, redeploy Kafka and restart affected services.

## Debugging Strategies

### Strategy 1: Enable Maximum Logging and keep infrastructure running after experiment

```bash
python experiment_run_e2e.py \
  experiment_type=simple_config_fake_ports_2_card_20 \
  experiment.name=debug \
  cloudlab.num_nodes=9 \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=sketchdb.utah.cloudlab.us \
  logging.level=DEBUG \
  flow.no_teardown=true
```

### Strategy 2: Step-by-Step Manual Execution

```bash
# Run experiment setup, then manually control execution

python experiment_run_e2e.py \
  experiment_type=simple_config_fake_ports_2_card_20 \
  experiment.name=manual_test \
  cloudlab.num_nodes=9 \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=sketchdb.utah.cloudlab.us \
  manual.query_engine=true \
  manual.remote_monitor=true \
  flow.no_teardown=true

# Script will pause at key points
# SSH to nodes and inspect/test manually
ssh myuser@node0.suffix

# Check each service
docker ps
curl http://localhost:9090/-/ready
curl http://localhost:8088/health

# Manually run queries
curl 'http://localhost:8088/api/v1/query?query=sum_over_time(fake_metric_total[1m])'

# When satisfied, continue or kill experiment
```
