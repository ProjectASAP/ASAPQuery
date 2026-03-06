# Configuration Parameters Reference

This document provides a comprehensive reference for all configuration parameters available in the Hydra-based experiment framework.

## Table of Contents
1. [Core Infrastructure Parameters](#core-infrastructure-parameters)
2. [Logging and Debugging](#logging-and-debugging)
3. [Profiling Parameters](#profiling-parameters)
4. [Manual Mode Parameters](#manual-mode-parameters)
5. [Experiment Flow Control](#experiment-flow-control)
6. [Streaming Engine Configuration](#streaming-engine-configuration)
7. [Prometheus Configuration](#prometheus-configuration)
8. [Experiment-Specific Parameters](#experiment-specific-parameters)
9. [Experiment Parameters (from experiment_type configs)](#experiment-parameters)
10. [Parameter Validation and Constraints](#parameter-validation-and-constraints)
11. [Configuration Schema Issues](#configuration-schema-issues)
12. [Usage Examples](#usage-examples)

---

## Core Infrastructure Parameters

### Required Parameters
These parameters must be provided for all experiment scripts:

#### `experiment.name` (string, required)
- **Description**: Human-readable experiment name used for organizing outputs
- **Usage**: Creates output directories and experiment identification
- **Example**: `"performance_test_2024"`
- **Validation**: Cannot be `???` or empty
- **Used by**: All experiment scripts

#### `cloudlab.num_nodes` (int, required)
- **Description**: Number of CloudLab nodes to allocate for the experiment
- **Range**: 1-50 (recommended)
- **Example**: `4`
- **Validation**: Must be positive integer
- **Used by**: All experiment scripts

#### `cloudlab.username` (string, required)
- **Description**: Your CloudLab username for SSH access
- **Example**: `"myuser"`
- **Validation**: Must be valid CloudLab username
- **Used by**: All experiment scripts

#### `cloudlab.hostname_suffix` (string, required)
- **Description**: CloudLab experiment hostname suffix
- **Example**: `"myexp.cloudlab.us"`
- **Validation**: Must be valid hostname format
- **Used by**: All experiment scripts

#### `prometheus.local_config_dir` (string, conditionally required)
- **Description**: Path to local Prometheus configuration directory
- **Example**: `"/path/to/prometheus/config"`
- **Validation**: Directory must exist and be readable
- **Required by**: `experiment_run_e2e.py`, `experiment_run_e2e_no_queryengine.py`
- **Not required by**: `experiment_run_exporters_and_prometheus.py`, `experiment_run_empty_flink.py`

---

## Logging and Debugging

#### `logging.level` (string, optional)
- **Description**: Logging level for the experiment
- **Default**: `"INFO"`
- **Choices**: `"DEBUG"`, `"INFO"`, `"WARNING"`, `"ERROR"`
- **Example**: `"DEBUG"`
- **Usage**: Controls verbosity of experiment output

---

## Profiling Parameters

#### `profiling.query_engine` (boolean, optional)
- **Description**: Enable profiling for the query engine component
- **Default**: `false`
- **Example**: `true`
- **Usage**: Enables performance profiling of query processing

#### `profiling.prometheus_time` (int, optional)
- **Description**: Duration in seconds to profile Prometheus
- **Default**: `null` (disabled)
- **Range**: 1-3600 seconds
- **Example**: `60`
- **Usage**: Profiles Prometheus for specified duration

#### `profiling.flink` (boolean, optional)
- **Description**: Enable profiling for Flink streaming engine
- **Default**: `false`
- **Example**: `true`
- **Usage**: Enables Flink JVM profiling

#### `profiling.arroyo` (boolean, optional)
- **Description**: Enable profiling for Arroyo streaming engine
- **Default**: `false`
- **Example**: `true`
- **Usage**: Enables Arroyo profiling when using Arroyo engine

---

## Manual Mode Parameters

#### `manual.query_engine` (boolean, optional)
- **Description**: Run query engine in manual mode (requires user intervention)
- **Default**: `false`
- **Example**: `true`
- **Usage**: Pauses for manual query engine setup

#### `manual.remote_monitor` (boolean, optional)
- **Description**: Enable remote monitoring in manual mode
- **Default**: `false`
- **Example**: `true`
- **Usage**: Allows manual control of remote monitoring setup

---

## Experiment Flow Control

#### `flow.no_teardown` (boolean, optional)
- **Description**: Skip teardown after experiment completion
- **Default**: `false`
- **Example**: `true`
- **Constraint**: Can only be used with single experiment mode
- **Usage**: Leaves services running for debugging

#### `flow.steady_state_wait` (int, optional)
- **Description**: Time in seconds to wait for system steady state
- **Default**: `300`
- **Range**: 0-3600 seconds
- **Example**: `60`
- **Usage**: Allows system to stabilize before starting measurements

---

## Streaming Engine Configuration

#### `streaming.engine` (string, optional)
- **Description**: Which streaming engine to use
- **Default**: `"flink"`
- **Choices**: `"flink"`, `"arroyo"`
- **Example**: `"arroyo"`
- **Usage**: Selects streaming processing framework

#### `streaming.flink_input_format` (string, optional)
- **Description**: Input data format for Flink
- **Default**: `"json"`
- **Choices**: `"json"`, `"avro-json"`, `"avro-binary"`
- **Example**: `"avro-json"`
- **Usage**: Controls Flink data deserialization

#### `streaming.flink_output_format` (string, optional)
- **Description**: Output data format for Flink
- **Default**: `"json"`
- **Choices**: `"json"`, `"byte"`
- **Example**: `"byte"`
- **Usage**: Controls Flink data serialization

#### `streaming.enable_object_reuse` (boolean, optional)
- **Description**: Enable object reuse optimization in streaming engine
- **Default**: `false`
- **Example**: `true`
- **Usage**: Performance optimization for high-throughput scenarios

#### `streaming.do_local_flink` (boolean, optional)
- **Description**: Run Flink locally instead of on CloudLab cluster
- **Default**: `false`
- **Example**: `true`
- **Usage**: Development mode for local testing

#### `streaming.forward_unsupported_queries` (boolean, optional)
- **Description**: Forward unsupported queries to Prometheus
- **Default**: `false`
- **Example**: `true`
- **Usage**: Fallback mechanism for complex queries

---

## Prometheus Configuration

#### `prometheus.scrape_interval` (string, optional)
- **Description**: How frequently Prometheus scrapes targets
- **Default**: `"5s"`
- **Format**: Time duration with unit (s, m, h)
- **Example**: `"10s"`
- **Usage**: Controls monitoring granularity

#### `prometheus.evaluation_interval` (string, optional)
- **Description**: How frequently Prometheus evaluates rules
- **Default**: `"1s"`
- **Format**: Time duration with unit
- **Example**: `"5s"`
- **Usage**: Controls rule evaluation frequency

#### `prometheus.query_log_file` (string, optional)
- **Description**: Path to Prometheus query log file
- **Default**: `"/scratch/sketch_db_for_prometheus/prometheus/queries.log"`
- **Example**: `"/custom/path/queries.log"`
- **Usage**: Logs all queries for analysis

#### `prometheus.recording_rules.interval` (string, optional)
- **Description**: How frequently to evaluate recording rules
- **Default**: `"5s"`
- **Format**: Time duration with unit
- **Example**: `"10s"`
- **Usage**: Controls pre-computed metric updates

---

## Monitoring Configuration

### `monitoring.tool` (string, required in experiment_type configs)
- **Description**: Which monitoring/TSDB tool to use for metrics collection
- **Choices**: `"prometheus"`, `"victoriametrics"`
- **Example**: `"prometheus"`
- **Location**: Specified in experiment_type config files (e.g., `cloud_demo.yaml`)
- **Usage**: Determines which time-series database service to deploy

### `monitoring.deployment_mode` (string, required in experiment_type configs)
- **Description**: How to deploy the monitoring tool
- **Choices**: `"bare_metal"`, `"containerized"`
- **Example**: `"containerized"`
- **Location**: Specified in experiment_type config files
- **Usage**: Determines deployment strategy for the monitoring service
- **Constraints**:
  - VictoriaMetrics only supports `containerized` mode
  - `bare_metal` mode only available for Prometheus

### `monitoring.resource_limits` (dict, optional)
- **Description**: Resource constraints for containerized monitoring deployments
- **Required when**: Only applicable when `deployment_mode: containerized`
- **Location**: Specified in experiment_type config files
- **Validation**: Will raise error if specified with `deployment_mode: bare_metal`
- **Example**:
  ```yaml
  resource_limits:
    cpu_limit: 4.0
    memory_limit: 8g
  ```

### `monitoring.resource_limits.cpu_limit` (float, optional)
- **Description**: Number of CPU cores to allocate to the monitoring container
- **Range**: 0.5-64.0 (depending on host)
- **Example**: `4.0`
- **Usage**: Limits CPU usage via Docker `--cpus` flag
- **Use case**: Vertical scalability testing

### `monitoring.resource_limits.memory_limit` (string, optional)
- **Description**: Memory limit for the monitoring container
- **Format**: Integer with unit suffix (k, m, g)
- **Example**: `"8g"`, `"4096m"`
- **Usage**: Limits memory usage via Docker `--memory` flag
- **Use case**: Vertical scalability testing

---

## Migration from Old Configuration

### Deprecated: `docker_resources`

The old `docker_resources` configuration is **deprecated and no longer supported**.

**Old format (NO LONGER VALID):**
```yaml
docker_resources:
  cpu_limit: 2.0
  memory_limit: 2g
  tool: prometheus
```

**New format:**
```yaml
monitoring:
  tool: prometheus
  deployment_mode: containerized
  resource_limits:
    cpu_limit: 2.0
    memory_limit: 2g
```

**Error handling:** If an old config with `docker_resources` is used, the system will raise a clear error message directing users to update their configuration.

---

## Monitoring Configuration Examples

### Example 1: Standard Bare-Metal Prometheus (Most Common)
```yaml
monitoring:
  tool: prometheus
  deployment_mode: bare_metal
```

### Example 2: Containerized Prometheus Without Resource Limits
```yaml
monitoring:
  tool: prometheus
  deployment_mode: containerized
```

### Example 3: Containerized Prometheus With Resource Limits (Vertical Scalability)
```yaml
monitoring:
  tool: prometheus
  deployment_mode: containerized
  resource_limits:
    cpu_limit: 2.0
    memory_limit: 4g
```

### Example 4: Containerized VictoriaMetrics With Resource Limits
```yaml
monitoring:
  tool: victoriametrics
  deployment_mode: containerized
  resource_limits:
    cpu_limit: 4.0
    memory_limit: 8g
```

---

#### `fake_exporter_language` (string, optional)
- **Description**: Language implementation for fake metric exporter
- **Default**: `"python"`
- **Choices**: `"python"`, `"rust"`
- **Example**: `"rust"`
- **Usage**: Selects fake exporter implementation

---

## Experiment-Specific Parameters

### For experiment_run_sketchdboffline.py

#### `experiment_variants.sketchdboffline.experiment_dir` (string, required)
- **Description**: Path to experiment data directory for offline analysis
- **Example**: `"/path/to/experiment/data"`
- **Validation**: Directory must exist and be readable

#### `experiment_variants.sketchdboffline.labels` (list, optional)
- **Description**: List of labels to include in analysis
- **Default**: `["label_0", "label_1", "label_2", "instance", "job"]`
- **Example**: `["instance", "job", "label_0"]`

#### `experiment_variants.sketchdboffline.groupby` (list, required)
- **Description**: List of labels to group by in aggregation
- **Example**: `["label_0", "instance"]`
- **Validation**: Must be subset of available labels

#### `experiment_variants.sketchdboffline.aggregation` (string, required)
- **Description**: Aggregation function to apply
- **Choices**: `"sum"`, `"avg"`, `"count"`, `"min"`, `"max"`
- **Example**: `"sum"`

### For experiment_run_flink_with_different_num_aggregations.py

#### `experiment_variants.flink_aggregations.aggregation_id` (int, required)
- **Description**: ID of the aggregation query to duplicate for testing
- **Range**: 0 to number of queries - 1
- **Example**: `0`

#### `experiment_variants.flink_aggregations.min_aggregations` (int, required)
- **Description**: Minimum number of aggregations to test
- **Range**: 1-100
- **Example**: `1`

#### `experiment_variants.flink_aggregations.max_aggregations` (int, required)
- **Description**: Maximum number of aggregations to test
- **Range**: min_aggregations to 1000
- **Example**: `10`
- **Constraint**: Must be >= min_aggregations

#### `experiment_variants.flink_aggregations.profile_duration` (int, optional)
- **Description**: Seconds to run Flink before starting profiling
- **Default**: `300`
- **Range**: 60-3600 seconds
- **Example**: `120`

#### `experiment_variants.flink_aggregations.config` (string, required)
- **Description**: Path to base configuration file for aggregation testing
- **Example**: `"/path/to/base_config.yaml"`
- **Validation**: File must exist and be valid YAML

---

## Experiment Parameters

These parameters come from the `experiment_type` config group and are prefixed with `experiment_params.`:

### Experiment Mode Configuration

#### `experiment_params.experiment` (list, required)
- **Description**: List of experiment modes to run
- **Structure**: Each item has `mode` and optional `query_prometheus_too`
- **Example**:
  ```yaml
  experiment:
    - mode: sketchdb
      query_prometheus_too: true
    - mode: prometheus
  ```
- **Choices for mode**: `"sketchdb"`, `"prometheus"`
- **SCHEMA ISSUE**: Inconsistent use of `query_prometheus_too` parameter across configs

### Server Configuration

#### `experiment_params.servers` (list, required)
- **Description**: List of server endpoints for the experiment
- **Structure**: Each item has `name` and `url`
- **Example**:
  ```yaml
  servers:
    - name: prometheus
      url: http://localhost:9090
    - name: sketchdb
      url: http://localhost:8088
  ```
- **SCHEMA ISSUE**: Identical across all configs - should be in shared defaults

### Workload Configuration

#### `experiment_params.workloads` (dict, optional)
- **Description**: External workload configurations
- **Structure**: Each workload has configuration options
- **Example**:
  ```yaml
  workloads:
    deathstar:
      use: true
  ```
- **SCHEMA ISSUE**: Inconsistent commenting patterns across configs

### Exporter Configuration

#### `experiment_params.exporters.only_start_if_queries_exist` (boolean, optional)
- **Description**: Only start exporters if queries reference their metrics
- **Default**: `true`
- **Example**: `false`
- **Usage**: Optimization to avoid unnecessary metric collection

#### Node Exporter Parameters

#### `experiment_params.exporters.exporter_list.node_exporter.port` (int, optional)
- **Description**: Port for node exporter service
- **Default**: `9100`
- **Range**: 1024-65535
- **Example**: `9200`

#### `experiment_params.exporters.exporter_list.node_exporter.extra_flags` (string, optional)
- **Description**: Additional command line flags for node exporter
- **Default**: `"--collector.disable-defaults --collector.cpu"`
- **Example**: `"--collector.disable-defaults --collector.cpu --collector.memory"`

#### Fake Exporter Parameters

#### `experiment_params.exporters.exporter_list.fake_exporter.num_ports_per_server` (int, optional)
- **Description**: Number of fake exporter instances per server
- **Range**: 1-20
- **Example**: `5`
- **SCHEMA ISSUE**: Values vary widely (1-10) across configs without clear pattern

#### `experiment_params.exporters.exporter_list.fake_exporter.start_port` (int, optional)
- **Description**: Starting port number for fake exporters
- **Default**: `50000`
- **Range**: 1024-65535
- **Example**: `51000`

#### `experiment_params.exporters.exporter_list.fake_exporter.dataset` (string, optional)
- **Description**: Distribution pattern for synthetic data generation
- **Default**: `"zipf"`
- **Choices**: `"zipf"`, `"uniform"`, `"normal"`
- **Example**: `"uniform"`

#### `experiment_params.exporters.exporter_list.fake_exporter.synthetic_data_value_scale` (int, optional)
- **Description**: Maximum value for synthetic data (range: [0, value_scale])
- **Default**: `10000`
- **Range**: 1-1000000
- **Example**: `50000`

#### `experiment_params.exporters.exporter_list.fake_exporter.num_labels` (int, optional)
- **Description**: Number of labels per metric
- **Range**: 1-10
- **Example**: `4`
- **SCHEMA ISSUE**: Inconsistent values (2 vs 3) across configs

#### `experiment_params.exporters.exporter_list.fake_exporter.num_values_per_label` (int, optional)
- **Description**: Number of unique values per label (cardinality)
- **Range**: 1-100
- **Example**: `15`
- **SCHEMA ISSUE**: Highly variable (2-20) across configs

#### `experiment_params.exporters.exporter_list.fake_exporter.metric_type` (string, optional)
- **Description**: Type of Prometheus metric to generate
- **Default**: `"counter"`
- **Choices**: `"counter"`, `"gauge"`
- **Example**: `"gauge"`
- **Note**: Counter metrics have `_total` suffix, gauge metrics don't

### Query Group Configuration

#### `experiment_params.query_groups` (list, required)
- **Description**: List of query group configurations defining the experimental workload
- **Validation**: At least one query group must be defined
- **Structure**: Each group contains queries, timing, and client/controller options

#### `experiment_params.query_groups[].id` (int, required)
- **Description**: Unique identifier for the query group
- **Example**: `1`
- **Usage**: Used for result organization and debugging

#### `experiment_params.query_groups[].queries` (list, required)
- **Description**: List of PromQL queries to execute
- **Example**:
  ```yaml
  queries:
    - 'sum_over_time(fake_metric_total[10m])'
    - 'increase(fake_metric_total[10m])'
    - 'sum by (instance, job) (sum_over_time(fake_metric_total[10m]))'
  ```
- **Validation**: Must be valid PromQL syntax
- **SCHEMA ISSUE**: Inconsistent time ranges ([1m] vs [10m]) across configs

#### `experiment_params.query_groups[].repetition_delay` (int, required)
- **Description**: Delay in seconds between query repetitions
- **Range**: 1-3600 seconds
- **Example**: `10`
- **Usage**: Controls query load and timing

#### `experiment_params.query_groups[].client_options.repetitions` (int, required)
- **Description**: Number of times to repeat each query in the group
- **Range**: 1-1000
- **Example**: `30`
- **SCHEMA ISSUE**: Values vary widely (3-100) across configs

#### `experiment_params.query_groups[].client_options.query_time_offset` (int, optional)
- **Description**: Time offset in seconds for queries to account for freshness delay
- **Default**: `10`
- **Range**: 0-300 seconds
- **Example**: `15`
- **Usage**: Ensures data is available for lookback queries

#### `experiment_params.query_groups[].client_options.starting_delay` (int, required)
- **Description**: Initial delay in seconds before starting query execution
- **Example**: `610`
- **Usage**: Allows system initialization and data collection
- **SCHEMA ISSUE**: Two main patterns (70 for 1m queries, 610 for 10m queries)

#### `experiment_params.query_groups[].controller_options.accuracy_sla` (float, optional)
- **Description**: Accuracy SLA for query results (0.0-1.0)
- **Default**: `0.99`
- **Range**: 0.0-1.0
- **Example**: `0.95`
- **Usage**: Quality assurance for approximate query results

#### `experiment_params.query_groups[].controller_options.latency_sla` (float, optional)
- **Description**: Latency SLA in seconds
- **Default**: `1`
- **Range**: 0.1-60.0 seconds
- **Example**: `2`
- **Usage**: Performance requirement for query response time

### Metrics Configuration

#### `experiment_params.metrics` (list, required)
- **Description**: List of metric definitions that will be collected
- **Validation**: At least one metric must be defined
- **Structure**: Each metric specifies name, labels, and source exporter

#### `experiment_params.metrics[].metric` (string, required)
- **Description**: Name of the Prometheus metric
- **Example**: `"fake_metric_total"`, `"node_cpu_seconds_total"`
- **Usage**: Must match actual metric names from exporters

#### `experiment_params.metrics[].labels` (list, required)
- **Description**: List of label names for the metric
- **Example**: `['instance', 'job', 'label_0', 'label_1', 'label_2']`
- **Usage**: Defines metric dimensionality and grouping options

#### `experiment_params.metrics[].exporter` (string, required)
- **Description**: Name of the exporter that provides this metric
- **Example**: `"fake_exporter"`, `"node_exporter"`
- **Validation**: Must match an exporter defined in exporter_list

---

## Parameter Validation and Constraints

### Current Validation Gaps

1. **Missing Value Range Validation**:
   - No validation for numeric parameter ranges
   - No validation for time format strings
   - No validation for port number ranges

2. **Missing Type Validation**:
   - No validation that numeric parameters are actually numbers
   - No validation of boolean parameter formats
   - No validation of list/dict structures

3. **Missing Cross-Parameter Validation**:
   - No validation that total experiment time is reasonable
   - No validation that port ranges don't overlap
   - No validation that queries reference defined metrics

4. **Missing File/Path Validation**:
   - Limited validation of file existence
   - No validation of file permissions
   - No validation of directory writability

### Recommended Validation Schema

```python
# Recommended validation rules
VALIDATION_RULES = {
    "cloudlab.num_nodes": {"type": int, "min": 1, "max": 50},
    "prometheus.scrape_interval": {"type": str, "pattern": r"^\d+[smh]$"},
    "experiment_params.exporters.exporter_list.fake_exporter.start_port": {
        "type": int, "min": 1024, "max": 65535
    },
    "experiment_params.query_groups[].repetition_delay": {
        "type": int, "min": 1, "max": 3600
    },
    "experiment_params.metrics[].exporter": {
        "type": str, "must_exist_in": "experiment_params.exporters.exporter_list"
    }
}
```

---

## Configuration Schema Issues

### Major Inconsistencies Found

1. **Experiment Mode Structure**:
   - Inconsistent use of `query_prometheus_too` parameter
   - Some configs have only one mode, others have both
   - Different commenting patterns

2. **Fake Exporter Configuration**:
   - `num_ports_per_server`: Values vary from 1-10 without clear pattern
   - `num_labels`: Some use 2, others use 3
   - `num_values_per_label`: Highly variable (2-20)
   - `metric_type`: Most use counter, some use gauge

3. **Query Group Timing**:
   - Two distinct patterns: 70s delay for 1m queries, 610s for 10m queries
   - `repetitions` vary widely (3-100) across similar configs
   - No clear relationship between timing parameters

4. **Server Configuration Redundancy**:
   - Identical server configurations in all 24 files
   - Should be moved to shared defaults

5. **Workload Configuration**:
   - Inconsistent commenting of deathstar workload
   - No clear indication of when workload should be enabled

### Recommendations for Schema Standardization

1. **Create Base Templates**:
   - `base_fake_exporter.yaml` with standard parameters
   - `base_node_exporter.yaml` for real metrics
   - `base_timing.yaml` for standard timing patterns

2. **Implement Parameter Inheritance**:
   - Extract common configurations to shared defaults
   - Use Hydra composition to reduce duplication

3. **Add Schema Validation**:
   - Implement comprehensive parameter validation
   - Add cross-parameter consistency checks
   - Validate business logic constraints

4. **Standardize Naming Conventions**:
   - Use consistent parameter names across all configs
   - Follow clear naming patterns for related parameters

---

## Usage Examples

### Basic Experiment Execution
```bash
python experiment_run_e2e.py \
  experiment.name=my_test \
  experiment_type=cloud_demo \
  cloudlab.num_nodes=4 \
  cloudlab.username=myuser \
  cloudlab.hostname_suffix=myexp.cloudlab.us \
  prometheus.local_config_dir=/path/to/prometheus/config
```

### Parameter Override Examples
```bash
# Override query timing
python experiment_run_e2e.py \
  experiment_type=cloud_demo \
  experiment_params.query_groups.0.repetition_delay=30 \
  experiment_params.query_groups.0.client_options.repetitions=50 \
  [required params...]

# Override exporter configuration
python experiment_run_e2e.py \
  experiment_type=cloud_demo \
  experiment_params.exporters.exporter_list.fake_exporter.num_ports_per_server=5 \
  experiment_params.exporters.exporter_list.fake_exporter.metric_type=gauge \
  [required params...]

# Override streaming engine
python experiment_run_e2e.py \
  experiment_type=cloud_demo \
  streaming.engine=arroyo \
  streaming.enable_object_reuse=true \
  [required params...]
```

### Development Mode Example
```bash
python experiment_run_e2e.py \
  experiment_type=cloud_demo \
  cloudlab.num_nodes=2 \
  logging.level=DEBUG \
  flow.steady_state_wait=60 \
  streaming.do_local_flink=true \
  [required params...]
```

### Grid Search Examples
```bash
# Test different repetition delays
for delay in 10 30 60; do
  python experiment_run_e2e.py \
    experiment.name=delay_${delay} \
    experiment_type=cloud_demo \
    experiment_params.query_groups.0.repetition_delay=${delay} \
    [required params...]
done

# Test different node counts
for nodes in 4 8 16; do
  python experiment_run_e2e.py \
    experiment.name=nodes_${nodes} \
    experiment_type=cloud_demo \
    cloudlab.num_nodes=${nodes} \
    [required params...]
done
```

---

This reference provides comprehensive documentation of all available configuration parameters, their constraints, and current schema issues. Use this as a guide for parameter selection and to understand the current state of the configuration system.
