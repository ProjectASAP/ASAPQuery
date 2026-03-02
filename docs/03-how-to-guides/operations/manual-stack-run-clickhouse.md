# Running the ASAP Stack Manually (Clickhouse)

This guide covers running the ASAP stack manually with Clickhouse for development and debugging. For Prometheus, see [manual-stack-run-prometheus.md](manual-stack-run-prometheus.md). For automated experiments, use the experiment framework in `Utilities/experiments/`.

## Prerequisites

- Kafka installed
- Arroyo built at `~/code/arroyo/target/release/arroyo`
- QueryEngineRust built at `~/code/QueryEngineRust/target/release/query_engine_rust`
- PrometheusExporters built (fake_kafka_exporter)
- Clickhouse installed and accessible

## Directory Structure

```
~/code/
├── ArroyoSketch/           # Pipeline configuration scripts
│   ├── config.yaml         # Arroyo cluster config
│   └── run_arroyosketch.py # Creates sources, sinks, and pipelines
├── QueryEngineRust/        # Query interception layer
├── PrometheusExporters/    # Data generators
│   └── fake_kafka_exporter/
└── Utilities/experiments/  # Automated experiment framework
```

## Config Files

Two config files are needed for ASAP mode. These must match the schema produced by fake_kafka_exporter.

**Example fake_kafka_exporter invocation:**
```bash
./target/release/fake_kafka_exporter \
    --kafka-topic raw_data_topic \
    --metadata-columns hostname,datacenter \
    --num-values-per-metadata-column 10,5 \
    --value-columns cpu_usage,memory_usage
```

This produces records like:
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

**streaming_config.yaml** - Defines streaming aggregations for Arroyo:
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
  windowSize: 1
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

Baseline mode runs queries directly against Clickhouse without ASAP's streaming layer.

### 1. Start Kafka

```bash
cd ~/kafka
./bin/kafka-server-start.sh ./config/kraft/server.properties
```

Wait for Kafka to be ready, then create topics:
```bash
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic raw_data_topic --partitions 1 --replication-factor 1
```

### 2. Start Data Exporter

The fake_kafka_exporter generates synthetic data and writes directly to Kafka:

```bash
cd ~/code/PrometheusExporters/fake_kafka_exporter
./target/release/fake_kafka_exporter \
    --kafka-topic raw_data_topic \
    --metadata-columns hostname,datacenter \
    --num-values-per-metadata-column 10,5 \
    --value-columns cpu_usage,memory_usage \
    --frequency 1
```

### 3. Start Clickhouse

Install timezone data (required for Clickhouse):
```bash
apt-get install -y tzdata && ln -sf /usr/share/zoneinfo/UTC /etc/localtime
```

Start the Clickhouse server:
```bash
clickhouse-server
```

### 4. Configure Clickhouse to Ingest from Kafka

Create a Kafka engine table to consume from the raw data topic. Run these commands in `clickhouse-client`:

```sql
CREATE TABLE kafka_table (
    time DateTime64(3),
    hostname String,
    datacenter String,
    cpu_usage Float64,
    memory_usage Float64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'localhost:9092',
    kafka_topic_list = 'raw_data_topic',
    kafka_group_name = 'clickhouse_consumer',
    kafka_format = 'JSONEachRow';

CREATE TABLE metrics_table (
    time DateTime64(3),
    hostname String,
    datacenter String,
    cpu_usage Float64,
    memory_usage Float64
) ENGINE = MergeTree()
ORDER BY (datacenter, hostname, time);

CREATE MATERIALIZED VIEW cpu_usage_mv TO metrics_table AS
SELECT * FROM kafka_table;
```

### 5. Query Clickhouse

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

ASAP mode adds Arroyo (streaming aggregation) and QueryEngineRust (query interception) to accelerate queries using sketches. Data flows through Kafka to both Arroyo (for sketches) and Clickhouse (for fallback queries).

### 1. Start Kafka

```bash
cd ~/kafka
./bin/kafka-server-start.sh ./config/kraft/server.properties
```

Wait for Kafka to be ready, then create topics:
```bash
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic raw_data_topic --partitions 1 --replication-factor 1

./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic sketch_topic --partitions 1 --replication-factor 1 \
    --config max.message.bytes=20971520
```

### 2. Start Data Exporter

Same as baseline - exporter writes to Kafka:

```bash
cd ~/code/PrometheusExporters/fake_kafka_exporter
./target/release/fake_kafka_exporter \
    --kafka-topic raw_data_topic \
    --metadata-columns hostname,datacenter \
    --num-values-per-metadata-column 10,5 \
    --value-columns cpu_usage,memory_usage \
    --frequency 1
```

### 3. Start Clickhouse

Same as baseline (including timezone setup). Start the server and configure Kafka ingestion:
```bash
clickhouse-server
```

Then create the tables as shown in the Baseline section (step 4).

### 4. Start Arroyo Cluster

```bash
cd ~/code/arroyo
./target/release/arroyo --config ~/code/ArroyoSketch/config.yaml cluster
```

Arroyo API runs at `http://localhost:5115`. Verify with:
```bash
curl http://localhost:5115/api/v1/pipelines
```

### 5. Configure ArroyoSketch Pipeline

Run `run_arroyosketch.py` to create Arroyo sources, sinks, and pipeline. For Clickhouse, always use Kafka source:

```bash
cd ~/code/ArroyoSketch
python run_arroyosketch.py \
    --source_type kafka \
    --kafka_input_format json \
    --input_kafka_topic raw_data_topic \
    --output_format json \
    --pipeline_name my_pipeline \
    --config_file_path /path/to/streaming_config.yaml \
    --output_kafka_topic sketch_topic \
    --output_dir ./outputs \
    --parallelism 1 \
    --query_language sql
```

The script outputs the pipeline ID. Verify the pipeline is running:
```bash
curl http://localhost:5115/api/v1/pipelines
```

### 6. Start QueryEngineRust

Check main.rs. It may be hardcoded to initialize the Prometheus-HTTP adapter. Change it the Clickhouse-HTTP adapter in the source code and recompile. We will make this configurable by a command line option in the future.

Replace
```rust
let adapter_config = AdapterConfig::prometheus_promql(
    args.prometheus_server.clone(),
    args.forward_unsupported_queries,
);
```

with

```rust
let adapter_config = AdapterConfig::clickhouse_sql(
    "http://localhost:8123".to_string(), // ClickHouse server URL
    "default".to_string(),               // Database name
    true,                                // Always forward (fallback for every query)
);
```

Recompile with `cargo build --release`.

```bash
cd ~/code/QueryEngineRust
./target/release/query_engine_rust \
    --kafka-topic sketch_topic \
    --input-format json \
    --config /path/to/inference_config.yaml \
    --streaming-config /path/to/streaming_config.yaml \
    --http-port 8088 \
    --delete-existing-db \
    --log-level info \
    --output-dir ./output \
    --streaming-engine arroyo \
    --query-language SQL \
    --lock-strategy per-key
    --prometheus-scrape-interval 1 \ # this should not be required for Clickhouse, but currently is required
```

QueryEngine now listens on port 8088 and intercepts SQL queries.

### 7. Query via QueryEngine

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
