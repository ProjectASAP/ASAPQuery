# PromSketch Integration — Multi-Path Ingestion Architecture

## 1. Overview

QueryEngine supports two parallel data ingestion paths:

1. **Precomputed pipeline**: A Kafka topic carrying pre-aggregated sketch buckets is consumed by `KafkaConsumer`, stored in `SimpleMapStore`, and served through the standard query path.
2. **Raw sample pipeline (Prometheus Remote Write)**: A standalone HTTP endpoint (`/api/v1/write`) accepts standard Prometheus remote write requests (Snappy-compressed protobuf). Decoded samples are inserted into `PromSketchStore` (which maintains live EHUniv, EHKLL, and USampling sketch instances per series) and served through the sketch query path.

When a query arrives, the engine tries the sketch path first, then falls through to the precomputed path, and finally (optionally) to a remote Prometheus server.

## 2. Data Flow Diagram

```
Raw Samples Path (Prometheus Remote Write):
  Prometheus / Agent --> POST /api/v1/write --> PrometheusRemoteWriteServer --> PromSketchStore
                          (Snappy + protobuf)          decode & insert
                                                                               |
                                                                         sketch_insert()
                                                                   (EHUniv, EHKLL, USampling)

Precomputed Path:
  Prometheus --> PrecomputeEngine --> Kafka [precomputed] --> KafkaConsumer --> SimpleMapStore

Query Path:
  HTTP Request --> SimpleEngine
                     |-- (1) handle_sketch_query_promql() --> PromSketchStore.eval_matching()
                     |-- (2) precomputed pipeline (SimpleMapStore)
                     +-- (3) fallback --> Prometheus server
```

## 3. Query Routing

When a PromQL query arrives, `SimpleEngine` dispatches it as follows:

1. **PromSketch path** — `handle_sketch_query_promql()` parses the query (AST first, regex fallback for custom functions). If the function name is in `promsketch_func_map` and the `PromSketchStore` has matching series data, results are returned immediately.
2. **Precomputed path** — If the sketch path returns `None` (function not sketch-backed, no store configured, or no matching series), the query falls through to `SimpleMapStore`.
3. **Prometheus fallback** — If `--forward-unsupported-queries` is set and the precomputed path also misses, the query is forwarded to the remote Prometheus server.

### Sketch-Backed Functions (13 total)

These functions are routed to `PromSketchStore` first, with fallthrough to precomputed on miss:

| Function                | Sketch Type | Standard PromQL? | Description                                      |
|-------------------------|-------------|-------------------|--------------------------------------------------|
| `entropy_over_time`     | EHUniv      | No (custom)       | Shannon entropy of the sample distribution        |
| `distinct_over_time`    | EHUniv      | No (custom)       | Estimated number of distinct values               |
| `l1_over_time`          | EHUniv      | No (custom)       | L1 norm of the value vector                       |
| `l2_over_time`          | EHUniv      | No (custom)       | L2 norm of the value vector                       |
| `quantile_over_time`    | EHKLL       | Yes               | Approximate quantile (e.g., p50, p99)             |
| `min_over_time`         | EHKLL       | Yes               | Minimum value over the range                      |
| `max_over_time`         | EHKLL       | Yes               | Maximum value over the range                      |
| `avg_over_time`         | USampling   | Yes               | Average of sampled values                         |
| `count_over_time`       | USampling   | Yes               | Count of sampled data points                      |
| `sum_over_time`         | USampling   | Yes               | Sum of sampled values                             |
| `sum2_over_time`        | USampling   | No (custom)       | Sum of squared values                             |
| `stddev_over_time`      | USampling   | Yes               | Standard deviation over the range                 |
| `stdvar_over_time`      | USampling   | Yes               | Variance over the range                           |

### Non-Sketch Functions

These functions always go directly to the precomputed pipeline (not in `promsketch_func_map`):

| Function    | Description                          |
|-------------|--------------------------------------|
| `rate`      | Per-second rate of increase          |
| `increase`  | Total increase over the range        |

## 4. Configuration Reference

### CLI Arguments

| Argument                            | Description                                                       | Default         |
|-------------------------------------|-------------------------------------------------------------------|-----------------|
| `--enable-prometheus-remote-write`  | Enable the Prometheus remote write ingest endpoint                | `false`         |
| `--prometheus-remote-write-port`    | Port for the Prometheus remote write HTTP server                  | `9090`          |
| `--auto-init-sketches`              | Auto-initialize all 3 sketch types for every new series           | `true`          |
| `--promsketch-config`               | Path to a sketch configuration YAML file (optional)               | (none)          |

### Sketch Config YAML

All fields are optional; defaults are shown below.

```yaml
eh_univ:
  k: 50                # EH buckets for UnivMon
  time_window: 1000000 # milliseconds

eh_kll:
  k: 50                # EH buckets for KLL
  kll_k: 256           # KLL accuracy parameter
  time_window: 1000000

sampling:
  sample_rate: 0.2     # fraction of data points to sample
  time_window: 1000000
```

## 5. Deployment Checklist

### Start QueryEngine with remote write enabled:

```bash
./query_engine \
  --enable-prometheus-remote-write \
  --prometheus-remote-write-port 9090 \
  --promsketch-config promsketch_config.yaml   # optional
```

### Configure Prometheus (or any remote write sender) to write to the endpoint:

```yaml
# prometheus.yml
remote_write:
  - url: "http://<query-engine-host>:9090/api/v1/write"
```

### Verify ingestion

Check logs for `"Received N samples"` messages.

### Verify queries

```bash
curl 'http://localhost:8088/api/v1/query?query=quantile_over_time(0.5,metric[1m])&time=...'
```

### Monitor

Use the `/metrics` endpoint for Prometheus counters (see section 6).

## 6. `/metrics` Endpoint

Exposed at `GET /metrics` in Prometheus exposition format. Key metrics:

| Metric                                          | Type      | Description                                           |
|-------------------------------------------------|-----------|-------------------------------------------------------|
| `promsketch_series_total`                       | Gauge     | Number of live series currently tracked               |
| `promsketch_samples_ingested_total`             | Counter   | Total raw samples ingested                            |
| `promsketch_ingest_errors_total`                | Counter   | Total ingestion errors (parse failures, etc.)         |
| `promsketch_ingest_batch_duration_seconds`      | Histogram | Time spent processing each ingestion batch            |
| `promsketch_sketch_queries_total{result="hit"}` | Counter   | Sketch queries that returned data                     |
| `promsketch_sketch_queries_total{result="miss"}`| Counter   | Sketch queries that fell through (no matching series) |
| `promsketch_sketch_query_duration_seconds`      | Histogram | End-to-end latency of sketch query evaluation         |
