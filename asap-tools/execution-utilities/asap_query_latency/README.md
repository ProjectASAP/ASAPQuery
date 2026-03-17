# ASAP Benchmark: ClickBench `hits` Dataset

Measures ASAP query latency (KLL sketch) against 50 fixed-timestamp QUANTILE 0.95 queries over 1M ClickBench rows.

## Data Flow

```
hits.json.gz
    └─> data_exporter (sort by EventTime → Kafka hits topic)
                              |
             ┌────────────────┴────────────────┐
             ↓                                  ↓
    Arroyo (KLL sketch, 1-sec windows)   ClickHouse (Kafka engine)
             |
       sketch_topic (Kafka)
             |
      QueryEngineRust :8088
             |
      run_benchmark.py → asap_results.csv
```

---

## Prerequisites

```bash
export INSTALL_DIR=/scratch/sketch_db_for_prometheus
```

### Build binaries (one-time)

```bash
# QueryEngineRust
cd ~/asap-internal/QueryEngineRust && cargo build --release

# data_exporter
cd ~/asap-internal/ExecutionUtilities/clickhouse-benchmark-pipeline/data_exporter
cargo build --release
```

### Download and chunk ClickBench data (one-time)

```bash
cd ~/asap-internal/ExecutionUtilities/clickhouse-benchmark-pipeline
python3 clickbench_importer/download_data.py

DATA_DIR=~/asap-internal/ExecutionUtilities/clickhouse-benchmark-pipeline/clickbench_importer/data
cd $DATA_DIR
mv hits.json.gz hits_full.json.gz
zcat hits_full.json.gz | head -n 1000000 | gzip > hits.json.gz
```

### Python dependencies (one-time)

```bash
pip3 install --user requests matplotlib numpy
pip3 install --user ~/asap-internal/CommonDependencies/dependencies/py/promql_utilities/
```

---

## Run the Pipeline

**Order matters**: create the Arroyo pipeline before producing data (connection table uses `offset: latest`).

### Step 1 — Start Kafka

```bash
~/asap-internal/Utilities/installation/kafka/run.sh $INSTALL_DIR/kafka
```

### Step 2 — Create Kafka topics

```bash
KAFKA=$INSTALL_DIR/kafka/bin
$KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic hits --partitions 1 --replication-factor 1
$KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic sketch_topic --partitions 1 --replication-factor 1 \
    --config max.message.bytes=20971520
```

### Step 3 — Start ClickHouse

```bash
~/asap-internal/Utilities/installation/clickhouse/run.sh $INSTALL_DIR
```

### Step 4 — Init ClickHouse tables

```bash
cd ~/asap-internal/ExecutionUtilities/clickhouse-benchmark-pipeline
CLICKHOUSE_BIN=$INSTALL_DIR/clickhouse bash scripts/init_clickhouse.sh
```

### Step 5 — Start Arroyo cluster

```bash
~/asap-internal/arroyo/target/release/arroyo \
    --config ~/asap-internal/ArroyoSketch/config.yaml cluster \
    > /tmp/arroyo.log 2>&1 &
```

### Step 6 — Create ArroyoSketch pipeline

```bash
cd ~/asap-internal/ArroyoSketch
python3 run_arroyosketch.py \
    --source_type kafka \
    --kafka_input_format json \
    --input_kafka_topic hits \
    --output_format json \
    --pipeline_name asap_hits_pipeline \
    --config_file_path ~/asap-internal/ExecutionUtilities/asap_query_latency/streaming_config.yaml \
    --output_kafka_topic sketch_topic \
    --output_dir ./outputs \
    --parallelism 1 \
    --query_language sql
```

### Step 7 — Start QueryEngineRust

```bash
cd ~/asap-internal/QueryEngineRust
nohup ./target/release/query_engine_rust \
    --kafka-topic sketch_topic --input-format json \
    --config ~/asap-internal/ExecutionUtilities/asap_query_latency/inference_config.yaml \
    --streaming-config ~/asap-internal/ExecutionUtilities/asap_query_latency/streaming_config.yaml \
    --http-port 8088 --delete-existing-db --log-level DEBUG \
    --output-dir ./output --streaming-engine arroyo \
    --query-language SQL --lock-strategy per-key \
    --prometheus-scrape-interval 1 > /tmp/query_engine.log 2>&1 &
```

### Step 8 — Produce ClickBench data

`data_exporter` reads all 1M records, transforms them (RFC3339 EventTime, stringify integer fields), sorts by EventTime, then sends to Kafka with event-time Kafka timestamps. This enables Arroyo's watermark to advance correctly for event-time windowing.

```bash
cd ~/asap-internal/ExecutionUtilities/clickhouse-benchmark-pipeline
TOTAL_RECORDS=1000000 DATA_MODE=clickbench bash scripts/generate_data.sh
```

Wait for completion before running the benchmark.

### Step 9 — Run ASAP benchmark

```bash
cd ~/asap-internal/ExecutionUtilities/asap_query_latency
./run_benchmark.py \
    --mode asap \
    --sql-file asap_quantile_queries.sql \
    --output asap_results.csv
```

### Step 10 — Run baseline benchmark (ClickHouse)

Wait for Step 8 data ingestion to complete, then verify ClickHouse has all rows:

```bash
$INSTALL_DIR/clickhouse client --query "SELECT count(*) FROM hits"
# Should return 1000000
```

Run the benchmark:

```bash
cd ~/asap-internal/ExecutionUtilities/asap_query_latency
./run_benchmark.py \
    --mode baseline \
    --sql-file clickhouse_quantile_queries.sql \
    --output baseline_results.csv \
    --clickhouse-url http://localhost:8123
```

---

## Reset (re-run from scratch)

```bash
# Kill workers and Arroyo cluster
pkill -f "arroyo"; pkill -f "query_engine_rust"; pkill -f "data_exporter"
sleep 2

# Stop Kafka and ClickHouse
pkill -f "kafka-server-start.sh"; pkill -f "clickhouse server"
sleep 2

# Clear Arroyo checkpoint state (REQUIRED — old watermark causes all windows to be skipped)
rm -rf /tmp/arroyo/

# Delete Kafka topics
KAFKA=$INSTALL_DIR/kafka/bin
$KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic hits
$KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic sketch_topic

# Delete old Arroyo pipeline
cd ~/asap-internal/ArroyoSketch
python3 delete_pipeline.py --all_pipelines

# Clear ClickHouse table
$INSTALL_DIR/clickhouse client --query "TRUNCATE TABLE hits"
```

Then repeat Steps 1–9.
