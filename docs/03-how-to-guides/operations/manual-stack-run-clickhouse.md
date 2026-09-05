# Running the ASAP Stack Manually (Clickhouse)

This guide covers running the ASAP stack manually with Clickhouse for development and debugging. For Prometheus, see [manual-stack-run-prometheus.md](manual-stack-run-prometheus.md). For automated experiments, use the experiment framework in `asap-tools/experiments/`.

## Prerequisites

- asap-query-engine built at `~/code/asap-query-engine/target/release/query_engine_rust`
- Clickhouse installed and accessible

## Directory Structure

```
~/code/
├── asap-query-engine/      # Query interception layer
└── asap-tools/experiments/ # Automated experiment framework
```

## Config Files

Two config files are needed for ASAP mode. The examples below use a data file with records shaped like:

```json
{"time": 1234567890000, "hostname": "hostname_0", "datacenter": "datacenter_0", "cpu_usage": 45.2, "memory_usage": 72.1}
```

**inference_config.yaml** - Defines metrics and queries for the QueryEngine:
```yaml
tables:
  - name: metrics_table
    time_column: time
    metadata_columns: [hostname, datacenter]
    value_columns: [cpu_usage, memory_usage]
cleanup_policy:
  name: read_based
queries:
- aggregations:
  - aggregation_id: 1
    read_count_threshold: 1
  query: |
    SELECT datacenter, quantile(0.99)(cpu_usage) as p99
    FROM metrics_table
    WHERE time BETWEEN DATEADD(s, -11, NOW()) AND DATEADD(s, -10, NOW())
    GROUP BY datacenter
```

**streaming_config.yaml** - Defines streaming aggregations for the precompute engine:
```yaml
aggregations:
- aggregationId: 1
  aggregationType: DatasketchesKLL
  aggregationSubType: ''
  labels:
    grouping: [datacenter]
    rollup: [hostname]
    aggregated: []
  table_name: metrics_table
  value_column: cpu_usage
  parameters:
    K: 20
  tumblingWindowSize: 1
  windowSizeMs: 1000
  windowType: tumbling
  spatialFilter: ''
tables:
  - name: metrics_table
    time_column: time
    metadata_columns: [hostname, datacenter]
    value_columns: [cpu_usage, memory_usage]
```

---

## Baseline Mode (Clickhouse Only)

Baseline mode runs queries directly against Clickhouse without ASAP's precompute layer.

### 1. Start Clickhouse

Install timezone data (required for Clickhouse):
```bash
apt-get install -y tzdata && ln -sf /usr/share/zoneinfo/UTC /etc/localtime
```

Start the Clickhouse server:
```bash
clickhouse-server
```

### 2. Create the Table and Load Data

```sql
CREATE TABLE metrics_table (
    time DateTime64(3),
    hostname String,
    datacenter String,
    cpu_usage Float64,
    memory_usage Float64
) ENGINE = MergeTree()
ORDER BY (datacenter, hostname, time);
```

Generate a data file of newline-delimited JSON records shaped like the example above (e.g. `data.jsonl`), then bulk-load it directly over ClickHouse's HTTP interface:

```bash
curl -s 'http://localhost:8123/?query=INSERT%20INTO%20metrics_table%20FORMAT%20JSONEachRow' \
    --data-binary @data.jsonl
```

### 3. Query Clickhouse

Query via HTTP protocol (we can use clickhouse-client, but ASAP only supports HTTP protocol for now so use HTTP).
The query parameter in the request is a URL-encoded form of a SQL query. See https://www.urlencoder.org/.

SQL query:
```sql
SELECT datacenter, quantile(0.99)(cpu_usage) as p99
FROM metrics_table
WHERE time BETWEEN DATEADD(s, -11, NOW()) AND DATEADD(s, -10, NOW())
GROUP BY datacenter
```

URL-encoded request:
```bash
curl 'http://localhost:8123/?query=SELECT%20datacenter%2C%20quantile%280.99%29%28cpu_usage%29%20as%20p99%0AFROM%20metrics_table%0AWHERE%20time%20BETWEEN%20DATEADD%28s%2C%20-11%2C%20NOW%28%29%29%20AND%20DATEADD%28s%2C%20-10%2C%20NOW%28%29%29%0AGROUP%20BY%20datacenter'
```

---

## ASAP Mode

ASAP mode runs QueryEngineRust's precompute engine to build sketches, with Clickhouse configured as the fallback backend for queries the sketches can't answer. The precompute engine reads the same data file directly (no message broker in between).

### 1. Start Clickhouse

Same as baseline (including timezone setup), and create `metrics_table` and load `data.jsonl` as in the Baseline section.

```bash
clickhouse-server
```

### 2. Start QueryEngineRust

Write `engine_config.yaml` (see `asap-query-engine/examples/engine_config.yaml` for the full schema):

```yaml
output_dir: "./output"
log_level: "INFO"
data_ingestion_interval_ms: 1000
streaming_engine: "precompute"
http_server:
  port: 8088
backend:
  type: "clickhouse"
  url: "http://localhost:8123"
  database: "default"
  forward_unsupported_queries: true
store:
  lock_strategy: "per-key"
ingest:
  type: "json"
  path: "/path/to/data.jsonl"
  metric_name: "metrics_table"
  value_col: "cpu_usage"
  label_cols: ["hostname", "datacenter"]
  timestamp_col: "time"
  timestamp_unit: "seconds"
inference_config: "/path/to/inference_config.yaml"
streaming_config: "/path/to/streaming_config.yaml"
```

```bash
cd ~/code/asap-query-engine
./target/release/query_engine_rust --config-file engine_config.yaml
```

QueryEngine now listens on port 8088 and intercepts SQL queries, replaying `data.jsonl` through the precompute engine to build sketches.

### 3. Query via QueryEngine

Direct queries to QueryEngineRust instead of Clickhouse, using the Clickhouse HTTP protocol.
The query parameter in the request is a URL-encoded form of a SQL query. See https://www.urlencoder.org/.

SQL query:
```sql
SELECT datacenter, quantile(0.99)(cpu_usage) as p99
FROM metrics_table
WHERE time BETWEEN DATEADD(s, -11, NOW()) AND DATEADD(s, -10, NOW())
GROUP BY datacenter
```

URL-encoded request:
```bash
curl 'http://localhost:8088/clickhouse/query?query=SELECT%20datacenter%2C%20quantile%280.99%29%28cpu_usage%29%20as%20p99%0AFROM%20metrics_table%0AWHERE%20time%20BETWEEN%20DATEADD%28s%2C%20-11%2C%20NOW%28%29%29%20AND%20DATEADD%28s%2C%20-10%2C%20NOW%28%29%29%0AGROUP%20BY%20datacenter'
```
