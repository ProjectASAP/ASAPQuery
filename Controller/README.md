# Controller

The Controller is ASAP's auto-configuration service that determines optimal sketch parameters based on query workload and SLAs.

## Purpose

Given a workload of PromQL queries, the Controller:
1. Analyzes each query to determine which sketch algorithm to use
2. Computes sketch parameters (size, accuracy) based on SLAs
3. Generates `streaming_config.yaml` for ArroyoSketch
4. Generates `inference_config.yaml` for QueryEngine

This automation eliminates manual configuration and ensures sketches meet performance targets.

## How It Works

### Input: controller-config.yaml

The user provides a configuration file describing:
- **Queries** to accelerate
- **Metrics** metadata (labels, cardinality estimates)
- **SLAs** (accuracy, latency targets) (**CURRENTLY IGNORED**)

**Example:**
```yaml
query_groups:
  - id: 1
    queries:
      - "quantile by (job) (0.99, http_request_duration_seconds)"
      - "sum by (job) (rate(http_requests_total[5m]))"
    client_options:
      repetitions: 10
      starting_delay: 60
    controller_options:
      accuracy_sla: 0.99  # 99% accuracy
      latency_sla: 1.0    # 1 second max latency

metrics:
  - metric: "http_request_duration_seconds"
    labels: ["job", "instance", "method", "status"]
    cardinality:
      job: 10
      instance: 100
      method: 5
      status: 4
  - metric: "http_requests_total"
    labels: ["job", "instance", "method", "status"]
```

### Process: Analyze and Configure

1. **Parse queries** (`utils/parse_query.py`)
   - Extract query type (quantile, sum, avg, etc.)
   - Identify aggregation labels
   - Determine time range

2. **Select sketch algorithm** (`utils/logics.py::decide_sketch_type()`)
   - Quantile queries → DDSketch or KLL
   - Sum/count queries → Simple aggregation
   - Consider query patterns and SLAs

3. **Compute sketch parameters** (`utils/logics.py`)
   - Calculate sketch size based on accuracy SLA
   - Determine merge strategy for aggregations
   - Set up windowing parameters

4. **Generate configs**
   - `streaming_config.yaml` → Describes which sketches to build
   - `inference_config.yaml` → Describes how to query sketches

### Output Files

**streaming_config.yaml** (for ArroyoSketch):
```yaml
sketches:
  - metric: "http_request_duration_seconds"
    sketch_type: "ddsketch"
    parameters:
      alpha: 0.01  # 1% relative error
      max_num_bins: 2048
    aggregation:
      - "job"
    window: "1h"
```

**inference_config.yaml** (for QueryEngine):
```yaml
sketches:
  - metric: "http_request_duration_seconds"
    sketch_type: "ddsketch"
    labels: ["job"]
    kafka_topic: "sketches"
```

## Key Files

**TODO**

## Configuration Schema

### controller-config.yaml

```yaml
query_groups:
  - id: <int>                          # Unique group ID
    queries:                           # List of PromQL queries
      - "<promql_query>"
    client_options:                    # Query execution options
      repetitions: <int>               # How many times to run
      starting_delay: <int>            # Delay before first run (seconds)
      repetition_delay: <int>          # Delay between runs (seconds)
      query_time_offset: <int>         # Time offset for queries (seconds)
    controller_options:
      accuracy_sla: <float>            # 0.0-1.0 (default: 0.99)
      latency_sla: <float>             # Seconds (default: 1.0)
      sketch_type: <str>               # Optional: force specific sketch
      custom_sketch_params: <dict>     # Optional: override params

metrics:
  - metric: "<metric_name>"            # Prometheus metric name
    labels: [<label_names>]            # List of label names
    cardinality:                       # Optional: estimated cardinalities
      <label_name>: <int>
```
