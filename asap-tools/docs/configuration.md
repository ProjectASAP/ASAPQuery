# Configuration Reference

Complete reference for all configuration parameters in the experiment framework.

## Table of Contents

- [Configuration System Overview](#configuration-system-overview)
- [Required Parameters](#required-parameters)
- [Experiment Configuration](#experiment-configuration)
- [CloudLab Configuration](#cloudlab-configuration)
- [Logging and Debugging](#logging-and-debugging)
- [Profiling Options](#profiling-options)
- [Monitoring Options](#monitoring-options)
- [Manual Mode Options](#manual-mode-options)
- [Experiment Flow Control](#experiment-flow-control)
- [Streaming Engine Configuration](#streaming-engine-configuration)
- [Prometheus Configuration](#prometheus-configuration)
- [Language Selection](#language-selection)
- [Query Engine Options](#query-engine-options)
- [Prometheus Client Configuration](#prometheus-client-configuration)
- [Container Deployment Settings](#container-deployment-settings)
- [Grafana Configuration](#grafana-configuration)
- [Configuration Composition](#configuration-composition)

## Configuration System Overview

The experiment framework uses [Hydra](https://hydra.cc/) for hierarchical configuration management.

**Configuration Files:**
- `experiments/config/config.yaml` - Base configuration with defaults
- `experiments/config/experiment_type/*.yaml` - Experiment-specific configurations
- Command-line arguments - Override any parameter

**Configuration Composition:**
```
config.yaml (base)
  ↓
+ experiment_type/*.yaml (merged into experiment_params)
  ↓
+ Command-line overrides
  ↓
= Final configuration
```

## Required Parameters

These parameters **must** be specified (marked with `???` in config):

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `experiment.name` | string | **Required** - Unique name for this experiment run | `my_test_run` |
| `cloudlab.num_nodes` | int | **Required** - Number of CloudLab nodes (not including coordinator) | `9` |
| `cloudlab.username` | string | **Required** - Your CloudLab username | `myuser` |
| `cloudlab.hostname_suffix` | string | **Required** - CloudLab experiment hostname suffix | `sketchdb.utah.cloudlab.us` |
| `experiment_type` | string | **Required** - Which experiment configuration to use | `simple_config` |

**Usage:**
```bash
python experiment_run_e2e.py \
  experiment_type=simple_config \
  experiment.name=my_test \
  cloudlab.num_nodes=9 \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=sketchdb.utah.cloudlab.us
```

## Experiment Configuration

### experiment.name

**Type:** `string`
**Required:** Yes
**Category:** Deployment
**Description:** Human-readable name for this experiment run. Used for creating output directories.

**Example:**
```bash
experiment.name=vertical_scalability_test_jan_2025
```

**Output location:** `$REPO_ROOT/experiment_outputs/<experiment.name>/`

## CloudLab Configuration

### cloudlab.num_nodes

**Type:** `int`
**Required:** Yes
**Category:** Deployment
**Description:** Number of CloudLab worker nodes to use. Node 0 is always the coordinator, so specify the number of workers (total_nodes - 1).

**Example:**
```bash
cloudlab.num_nodes=9  # Uses nodes 1-9 as workers (10 nodes total)
```

### cloudlab.node_offset

**Type:** `int`
**Default:** `0`
**Category:** Deployment
**Description:** Starting node index. Allows running multiple experiments in parallel on the same cluster.

**Example:**
```bash
# Experiment 1: Uses nodes 0-9
cloudlab.node_offset=0
cloudlab.num_nodes=9

# Experiment 2: Uses nodes 10-19
cloudlab.node_offset=10
cloudlab.num_nodes=9
```

### cloudlab.username

**Type:** `string`
**Required:** Yes
**Category:** Deployment
**Description:** Your CloudLab username for SSH access.

**Example:**
```bash
cloudlab.username=myuser
```

### cloudlab.hostname_suffix

**Type:** `string`
**Required:** Yes
**Category:** Deployment
**Description:** CloudLab experiment hostname suffix (part after `node<N>.`).

**Example:**
```bash
cloudlab.hostname_suffix=sketchdb.cloudmigration-PG0.utah.cloudlab.us
# Results in nodes: node0.sketchdb.cloudmigration-PG0.utah.cloudlab.us, etc.
```

## Logging and Debugging

### logging.level

**Type:** `string`
**Default:** `"INFO"`
**Category:** Debugging
**Options:** `"DEBUG"`, `"INFO"`, `"WARNING"`, `"ERROR"`
**Description:** Log verbosity level. Use `DEBUG` for detailed debugging output.

**Example:**
```bash
logging.level=DEBUG  # Verbose output for debugging
```

**When to use:**
- `DEBUG`: Debugging issues, want detailed logs
- `INFO`: Normal operation (default)
- `WARNING`: Only important warnings
- `ERROR`: Only errors

## Profiling Options

All profiling options are in the `profiling` section and are **monitoring** features. Profiling captures CPU and memory usage patterns to identify performance bottlenecks.

### profiling.query_engine

**Type:** `bool`
**Default:** `false`
**Category:** Monitoring
**Description:** Enable CPU and memory profiling of the query engine process using `py-spy`.
**Captures:** Function call stacks, CPU time per function, heap allocations
**Note:** (TODO) I believe this only works for the older Python based QueryEngine, not QueryEngineRust

**Example:**
```bash
profiling.query_engine=true
```

**Output:** Profiling data saved to experiment output directory.

### profiling.prometheus_time

**Type:** `int` (optional)
**Default:** `null`
**Category:** Monitoring
**Description:** Time-limited Prometheus profiling in seconds. If set, profiles Prometheus for this duration.
**Captures:** CPU profiles of Prometheus server process

**Example:**
```bash
profiling.prometheus_time=300  # Profile for 5 minutes
```

### profiling.flink

**Type:** `bool`
**Default:** `false`
**Category:** Monitoring
**Description:** Enable profiling of Flink worker processes.
**Captures:** CPU and memory profiles of Flink TaskManager JVMs

**Example:**
```bash
profiling.flink=true
```

**Only applies when:** `streaming.engine=flink`

### profiling.arroyo

**Type:** `bool`
**Default:** `false`
**Category:** Monitoring
**Description:** Enable profiling of Arroyo worker processes.
**Captures:** CPU profiles of Arroyo pipeline workers

**Example:**
```bash
profiling.arroyo=true
```

**Only applies when:** `streaming.engine=arroyo`

## Monitoring Options

These options collect performance metrics during the experiment to track system behavior over time.

### throughput.arroyo

**Type:** `bool`
**Default:** `false`
**Category:** Monitoring
**Description:** Track Arroyo pipeline throughput during experiment by polling Arroyo metrics endpoint.
**Captures:** Metrics/second ingested, sketches/second produced, pipeline lag

**Example:**
```bash
throughput.arroyo=true
```

**Output:** Throughput metrics saved to experiment output directory.

### throughput.prometheus

**Type:** `bool`
**Default:** `false`
**Category:** Monitoring
**Description:** Track Prometheus ingestion rate during experiment by querying internal Prometheus metrics.
**Captures:** Samples/second ingested, active time series count, scrape duration

**Example:**
```bash
throughput.prometheus=true
```

**Output:** Throughput metrics saved to experiment output directory.

### health_check.prometheus

**Type:** `bool`
**Default:** `false`
**Category:** Monitoring
**Description:** Monitor Prometheus target health and scrape duration throughout experiment.
**Captures:** Target health status (up/down), scrape duration per target, failed scrape counts

**Example:**
```bash
health_check.prometheus=true
```

**Output:** Health check data saved to experiment output directory.

## Manual Mode Options

Manual mode options are for **debugging** purposes.

### manual.query_engine

**Type:** `bool`
**Default:** `false`
**Category:** Debugging
**Description:** Don't auto-start query engine. Allows manual startup for debugging.

**Example:**
```bash
manual.query_engine=true
```

**When to use:** Debugging query engine startup issues or want to manually configure before starting.

### manual.remote_monitor

**Type:** `bool`
**Default:** `false`
**Category:** Debugging
**Description:** Prompt before running queries. Useful for inspecting system state before query execution.

**Example:**
```bash
manual.remote_monitor=true
```

**When to use:** Want to manually verify services are healthy before starting queries.

## Experiment Flow Control

All flow control options are in the `flow` section.

### flow.no_teardown

**Type:** `bool`
**Default:** `false`
**Category:** Debugging
**Description:** Skip teardown phase, keep all services running after experiment. Useful for debugging.

**Limitation:** Only works with single experiment mode (e.g., only "sketchdb" or only "prometheus").

**Example:**
```bash
flow.no_teardown=true
```

**When to use:**
- Want to inspect running services after experiment
- Need to manually test queries
- Debugging service issues

**After experiment:**
```bash
# SSH to coordinator and inspect
ssh user@node0.suffix
docker ps
docker logs sketchdb-queryengine-rust
curl http://localhost:8088/api/v1/query?query=...
```

### flow.replace_query_engine_with_dumb_consumer

**Type:** `bool`
**Default:** `false`
**Category:** Experimental
**Description:** Replace query engine with simple Kafka consumer for testing sketch output without query processing.

**Example:**
```bash
flow.replace_query_engine_with_dumb_consumer=true
```

**When to use:** Testing streaming engine output in isolation.

### flow.steady_state_wait

**Type:** `int`
**Default:** `60`
**Category:** Experimental
**Description:** Seconds to wait for system stabilization before starting queries. Allows metrics to accumulate and sketches to warm up.

**Example:**
```bash
flow.steady_state_wait=120  # Wait 2 minutes
```

**When to use:**
- Queries need longer warmup period
- High-cardinality metrics need more time to stabilize

## Streaming Engine Configuration

All streaming engine options are in the `streaming` section.

### streaming.engine

**Type:** `string`
**Default:** `"arroyo"`
**Category:** Deployment
**Options:** `"flink"`, `"arroyo"`
**Description:** Streaming engine to use. Arroyo is recommended for production.

**Example:**
```bash
streaming.engine=arroyo  # Use Arroyo (recommended)
streaming.engine=flink   # Use Flink
```

### streaming.parallelism

**Type:** `int`
**Default:** `1`
**Category:** Experimental
**Description:** Parallelism level for streaming pipelines. Higher values enable more parallel processing.

**Example:**
```bash
streaming.parallelism=4  # 4 parallel workers
```

**When to use:** High-throughput scenarios requiring more parallelism.

### streaming.flink_input_format

**Type:** `string`
**Default:** `"json"`
**Category:** Experimental
**Options:** `"json"`, `"avro-json"`, `"avro-binary"`
**Description:** Data format for streaming input from Kafka.

**Example:**
```bash
streaming.flink_input_format=json  # Use JSON (human-readable)
streaming.flink_input_format=avro-binary  # Use Avro (efficient)
```

### streaming.flink_output_format

**Type:** `string`
**Default:** `"json"`
**Category:** Experimental
**Options:** `"json"`, `"byte"`
**Description:** Data format for streaming output to Kafka.

**Example:**
```bash
streaming.flink_output_format=json  # Use JSON
streaming.flink_output_format=byte  # Use binary
```

### streaming.enable_object_reuse

**Type:** `bool`
**Default:** `false`
**Category:** Experimental
**Description:** Flink optimization - reuse objects to reduce GC pressure.

**Example:**
```bash
streaming.enable_object_reuse=true
```

**When to use:** High-throughput Flink jobs with GC issues.
**Only applies when:** `streaming.engine=flink`

### streaming.do_local_flink

**Type:** `bool`
**Default:** `false`
**Category:** Debugging
**Description:** Run Flink in local mode (single JVM) for faster iteration during development.

**Example:**
```bash
streaming.do_local_flink=true
```

**When to use:** Developing Flink jobs, want faster startup.
**Only applies when:** `streaming.engine=flink`

### streaming.forward_unsupported_queries

**Type:** `bool`
**Default:** `false`
**Category:** Experimental
**Description:** Forward unsupported queries to Prometheus instead of returning error.

**Example:**
```bash
streaming.forward_unsupported_queries=true
```

### streaming.use_kafka_ingest

**Type:** `bool`
**Default:** `false`
**Category:** Deployment
**Description:** Use Kafka for metric ingestion (legacy). Default is Prometheus remote write API (recommended).

**Example:**
```bash
streaming.use_kafka_ingest=false  # Use remote write (recommended)
streaming.use_kafka_ingest=true   # Use Kafka (legacy)
```

**Data flow comparison:**
- Remote write: `Exporters → Prometheus → RemoteWrite API → Arroyo`
- Kafka: `Exporters → Prometheus → KafkaAdapter → Kafka → Flink/Arroyo`

### streaming.remote_write.ip

**Type:** `string`
**Default:** `"${remote_write_ip:${cloudlab.node_offset}}"`
**Category:** Deployment
**Description:** IP address for Prometheus remote write endpoint. Uses resolver to compute `10.10.1.{offset+1}`.

**Example:**
```bash
# Automatically computed based on node_offset
# node_offset=0 → 10.10.1.1
# node_offset=5 → 10.10.1.6
```

### streaming.remote_write.base_port

**Type:** `int`
**Default:** `8080`
**Category:** Deployment
**Description:** Base port for remote write API. Multiple parallel instances increment from this.

**Example:**
```bash
streaming.remote_write.base_port=8080
streaming.parallelism=4
# Results in ports: 8080, 8081, 8082, 8083
```

### streaming.remote_write.path

**Type:** `string`
**Default:** `"/receive"`
**Category:** Deployment
**Description:** HTTP path for remote write endpoint.

**Example:**
```bash
streaming.remote_write.path=/receive
# Full URL: http://10.10.1.1:8080/receive
```

## Prometheus Configuration

All Prometheus options are in the `prometheus` section.

### prometheus.scrape_interval

**Type:** `string`
**Default:** `"10s"`
**Category:** Experimental
**Description:** How frequently Prometheus scrapes targets. Affects data resolution and freshness.

**Example:**
```bash
prometheus.scrape_interval=5s   # Scrape every 5 seconds
prometheus.scrape_interval=30s  # Scrape every 30 seconds
```

**Impact:** Lower interval = more data points, higher resource usage.

### prometheus.evaluation_interval

**Type:** `string`
**Default:** `"10s"`
**Category:** Experimental
**Description:** How frequently Prometheus evaluates recording rules.

**Example:**
```bash
prometheus.evaluation_interval=10s
```

### prometheus.recording_rules.interval

**Type:** `string`
**Default:** `"5s"`
**Category:** Experimental
**Description:** Interval for recording rule evaluation.

**Example:**
```bash
prometheus.recording_rules.interval=5s
```

**Note:** `prometheus.query_log_file` is disabled by default to avoid Docker permission issues.

## Language Selection

### fake_exporter_language

**Type:** `string`
**Default:** `"python"`
**Category:** Deployment
**Options:** `"python"`, `"rust"`
**Description:** Language for fake metric exporters. Rust is faster for high-cardinality scenarios.

**Example:**
```bash
fake_exporter_language=rust  # Use Rust exporter (faster)
fake_exporter_language=python  # Use Python exporter
```

**When to use:**
- Python: Development, lower cardinality
- Rust: Production, high cardinality, high throughput

### query_engine_language

**Type:** `string`
**Default:** `"rust"`
**Category:** Deployment
**Options:** `"python"`, `"rust"`
**Description:** Query engine implementation. Rust is production-ready and recommended.

**Example:**
```bash
query_engine_language=rust    # Use Rust (recommended)
query_engine_language=python  # Use Python (legacy)
```

### query_language

**Type:** `string`
**Default:** `"PROMQL"`
**Category:** Experimental
**Options:** `"SQL"`, `"PROMQL"`
**Description:** Query language used by Rust query engine.

**Example:**
```bash
query_language=PROMQL  # Use PromQL (default)
query_language=SQL     # Use SQL (experimental)
```

**Note:** Only applies when `query_engine_language=rust`.

## Query Engine Options

### query_engine.dump_precomputes

**Type:** `bool`
**Default:** `false`
**Category:** Debugging
**Description:** Dump precomputed sketch values to files (Rust query engine only). Useful for debugging sketch accuracy.

**Example:**
```bash
query_engine.dump_precomputes=true
```

**Output:** Precomputed values saved to experiment output directory.
**Only applies when:** `query_engine_language=rust`

## Prometheus Client Configuration

### prometheus_client.parallel

**Type:** `bool`
**Default:** `false`
**Category:** Experimental
**Description:** Enable parallel query execution in Prometheus client. Can improve throughput but may overwhelm target.

**Example:**
```bash
prometheus_client.parallel=true
```

**When to use:** Testing query engine scalability under parallel load.

## Container Deployment Settings

All container deployment settings are in the `use_container` section and are **deployment** options.

### use_container.query_engine

**Type:** `bool`
**Default:** `true`
**Category:** Deployment
**Description:** Deploy query engine as Docker container vs bare-metal binary.

**Example:**
```bash
use_container.query_engine=false  # Run as bare-metal binary
```

**When to use false:** Debugging query engine, want direct access to binary.

### use_container.arroyo

**Type:** `bool`
**Default:** `true`
**Category:** Deployment
**Description:** Deploy Arroyo as Docker container vs bare-metal.

**Example:**
```bash
use_container.arroyo=false  # Run bare-metal
```

### use_container.controller

**Type:** `bool`
**Default:** `true`
**Category:** Deployment
**Description:** Deploy controller as Docker container vs bare-metal.

**Example:**
```bash
use_container.controller=false
```

### use_container.fake_exporter

**Type:** `bool`
**Default:** `true`
**Category:** Deployment
**Description:** Deploy fake exporters as Docker containers vs bare-metal.

**Example:**
```bash
use_container.fake_exporter=false
```

### use_container.prometheus_client

**Type:** `bool`
**Default:** `true`
**Category:** Deployment
**Description:** Deploy Prometheus client as Docker container vs bare-metal.

**Example:**
```bash
use_container.prometheus_client=false
```

### use_container.grafana

**Type:** `bool`
**Default:** `true`
**Category:** Deployment
**Description:** Deploy Grafana as Docker container vs bare-metal.

**Example:**
```bash
use_container.grafana=false
```

**General guidance:** Setting to `false` can help debug issues by running services directly without container overhead.

## Grafana Configuration

All Grafana options are in the `grafana` section.

### grafana.host

**Type:** `string`
**Default:** `"localhost:3000"`
**Category:** Deployment
**Description:** Grafana server address.

**Example:**
```bash
grafana.host=localhost:3000
```

### grafana.user

**Type:** `string`
**Default:** `"admin"`
**Category:** Deployment
**Description:** Grafana admin username.

**Example:**
```bash
grafana.user=admin
```

### grafana.password

**Type:** `string`
**Default:** `"admin"`
**Category:** Deployment
**Description:** Grafana admin password.

**Example:**
```bash
grafana.password=admin
```

## Configuration Composition

### Hydra Defaults

The base `config.yaml` includes:

```yaml
defaults:
  - _self_
  - experiment_type: ???  # REQUIRED: Must specify experiment type
```

This means:
1. Load `config.yaml` first
2. Load specified `experiment_type/*.yaml` and merge into `experiment_params`
3. Apply command-line overrides

### Custom Resolvers

Two custom OmegaConf resolvers are registered:

#### local_experiment_dir

Returns `constants.LOCAL_EXPERIMENT_DIR`.

**Usage in config:**
```yaml
some_path: ${local_experiment_dir:}/my_subdir
```

#### remote_write_ip

Computes IP based on node_offset: `10.10.1.{offset+1}`.

**Usage in config:**
```yaml
remote_write_ip: ${remote_write_ip:${cloudlab.node_offset}}
```

**Examples:**
- `node_offset=0` → `10.10.1.1`
- `node_offset=5` → `10.10.1.6`

### Command-Line Override Examples

```bash
# Override single parameter
python experiment_run_e2e.py ... logging.level=DEBUG

# Override nested parameter
python experiment_run_e2e.py ... streaming.engine=flink

# Override deeply nested parameter
python experiment_run_e2e.py ... experiment_params.query_groups.0.client_options.repetitions=20

# Override multiple parameters
python experiment_run_e2e.py ... \
  logging.level=DEBUG \
  flow.no_teardown=true \
  streaming.parallelism=4
```

### Using Config Groups

Load additional config groups:

```bash
# Apply overrides from docs/overrides/arroyo.yaml
python experiment_run_e2e.py ... +overrides=arroyo
```

## Parameter Dependencies and Constraints

Some parameters have important relationships that affect correctness and performance.

### starting_delay (Query Groups)

**Relationship:** `starting_delay >= query_time_offset + max_lookback_window + buffer`

**Why:** The system needs enough time to accumulate metrics data for the lookback window and account for freshness delay.

**Example:**
```yaml
query_groups:
  - queries:
      - sum_over_time(fake_metric_total[1m])  # 60s lookback
    client_options:
      query_time_offset: 10  # 10s freshness delay
      starting_delay: 70     # >= 10 + 60 = 70s minimum
```

### repetition_delay (Query Groups)

**Relationship:** `repetition_delay` should evenly divide the lookback window for good coverage

**Example:**
```yaml
query_groups:
  - queries:
      - sum_over_time(fake_metric_total[1m])  # 60s lookback
    repetition_delay: 10  # Query every 10s
```

**Good values:**
- Lookback `[1m]`: Use `repetition_delay: 10` or `15` (divides 60)
- Lookback `[5m]`: Use `repetition_delay: 30` or `60` (divides 300)

### num_labels and metrics.labels (Exporter Configuration)

**Relationship:** `len(metrics.labels) >= exporter.num_labels + 2`

**Why:** Exporter generates `num_labels` custom labels, plus `instance` and `job` are always added.

**Example:**
```yaml
exporters:
  exporter_list:
    fake_exporter:
      num_labels: 3  # Generates label_0, label_1, label_2

metrics:
  - metric: fake_metric_total
    labels: ['instance', 'job', 'label_0', 'label_1', 'label_2']
    # Total: 5 labels (2 automatic + 3 generated) ✓
```

## Configuration Validation

The framework validates configurations at runtime using `experiment_utils/config.py`.

**Required sections in experiment_params:**

1. **query_groups** - At least one query group with:
   - `queries`: List of PromQL queries
   - `client_options.repetitions`: Number of repetitions
   - `client_options.starting_delay`: Warmup period
   - `controller_options.accuracy_sla`: Accuracy threshold
   - `controller_options.latency_sla`: Latency threshold

2. **exporters** - Must have:
   - `exporter_list`: Dictionary of exporter configs
   - `only_start_if_queries_exist`: Boolean flag

3. **metrics** - At least one metric with:
   - `metric`: Metric name
   - `labels`: List of label names
   - `exporter`: Which exporter produces this metric

**Validation errors will stop the experiment before it starts.**
