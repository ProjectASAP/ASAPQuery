# ASAP Generalized Benchmark Pipeline

Measures ASAP query latency (KLL sketch) against ClickHouse baseline for
arbitrary datasets. Supports ClickBench and H2O groupby out of the box.

## Architecture

```
data_file → prepare_data.py → arroyo_file.json
                                    ↓
                       export_to_arroyo.py (file source)
                                    ↓
                         sketch_topic (Kafka)
                                    ↓
                        QueryEngineRust :8088
                                    ↓
data_file → export_to_database.py  run_benchmark.py → results/
                ↓
          ClickHouse :8123 (baseline)
```

**Key difference from the old pipeline:** Arroyo reads directly from a local
file (`single_file_custom` connector) rather than from a Kafka input topic.
Kafka is still required for the **sketch output** topic (`sketch_topic`).

---

## Prerequisites

```bash
export INSTALL_DIR=/scratch/sketch_db_for_prometheus
pip3 install --user -r requirements.txt

# Build binaries (one-time)
cd ~/ASAPQuery/asap-query-engine && cargo build --release
```

---

## ClickBench + ClickHouse End-to-End Example

### Step 1 — Download dataset

```bash
cd ~/ASAPQuery/asap-tools/execution-utilities/benchmark
python download_dataset.py --dataset clickbench --output-dir ./data
```

Optionally limit to 1M rows:

```bash
cd ./data
mv hits.json.gz hits_full.json.gz
zcat hits_full.json.gz | head -n 1000000 | gzip > hits.json.gz
```

### Step 2 — Prepare data for Arroyo file source

The Arroyo file source requires RFC3339 timestamps and string metadata columns.
This step converts the raw ClickBench JSON:

```bash
python prepare_data.py \
    --dataset clickbench \
    --input ./data/hits.json.gz \
    --output ./data/hits_arroyo.json \
    --max-rows 1000000
```

This produces `hits_arroyo.json` with:
- `EventTime` converted from `"2013-07-14 20:38:47"` → `"2013-07-14T20:38:47Z"`
- `RegionID`, `OS`, `UserAgent`, `TraficSourceID` as strings
- Records sorted by `EventTime`

### Step 3 — Start infrastructure

```bash
# Kafka
~/ASAPQuery/asap-tools/installation/kafka/run.sh $INSTALL_DIR/kafka

# Create sketch output topic
KAFKA=$INSTALL_DIR/kafka/bin
$KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic sketch_topic --partitions 1 --replication-factor 1 \
    --config max.message.bytes=20971520

# ClickHouse
~/ASAPQuery/asap-tools/installation/clickhouse/run.sh $INSTALL_DIR
```

### Step 4 — Start Arroyo cluster

```bash
~/ASAPQuery/asap-summary-ingest/target/release/arroyo \
    --config ~/ASAPQuery/asap-summary-ingest/config.yaml cluster \
    > /tmp/arroyo.log 2>&1 &
```

### Step 5 — Launch Arroyo sketch pipeline (file source)

```bash
python export_to_arroyo.py \
    --streaming-config ./configs/clickbench_streaming.yaml \
    --source-type file \
    --input-file ./data/hits_arroyo.json \
    --file-format json \
    --ts-format rfc3339 \
    --pipeline-name clickbench_pipeline \
    --arroyosketch-dir ~/ASAPQuery/asap-summary-ingest \
    --output-dir ./arroyo_outputs
```

### Step 6 — Start QueryEngineRust

```bash
cd ~/ASAPQuery/asap-query-engine
nohup ./target/release/query_engine_rust \
    --kafka-topic sketch_topic --input-format json \
    --config ~/ASAPQuery/asap-tools/execution-utilities/benchmark/configs/clickbench_inference.yaml \
    --streaming-config ~/ASAPQuery/asap-tools/execution-utilities/benchmark/configs/clickbench_streaming.yaml \
    --http-port 8088 --delete-existing-db --log-level DEBUG \
    --output-dir ./output --streaming-engine arroyo \
    --query-language SQL --lock-strategy per-key \
    --prometheus-scrape-interval 1 > /tmp/query_engine.log 2>&1 &
```

### Step 7 — Load data into ClickHouse (baseline)

```bash
cd ~/ASAPQuery/asap-tools/execution-utilities/benchmark
python export_to_database.py \
    --dataset clickbench \
    --file-path ./data/hits.json.gz \
    --clickhouse-url "http://localhost:8123/" \
    --init-sql-file ./configs/clickbench_hits_init.sql
```

Verify: `$INSTALL_DIR/clickhouse client --query "SELECT count(*) FROM hits"`

### Step 8 — Generate SQL query files

```bash
python generate_queries.py \
    --table-name hits \
    --ts-column EventTime \
    --value-column ResolutionWidth \
    --group-by-columns RegionID,OS,UserAgent,TraficSourceID \
    --window-size 10 \
    --num-queries 50 \
    --ts-format datetime \
    --window-form dateadd \
    --auto-detect-timestamps \
    --data-file ./data/hits_arroyo.json \
    --data-file-format json \
    --output-prefix ./queries/clickbench
```

This writes `queries/clickbench_asap.sql` and `queries/clickbench_clickhouse.sql`.

### Step 9 — Run benchmark

```bash
python run_benchmark.py \
    --mode both \
    --asap-sql-file ./queries/clickbench_asap.sql \
    --baseline-sql-file ./queries/clickbench_clickhouse.sql \
    --output-dir ./results \
    --output-prefix clickbench
```

Results: `results/clickbench_asap.csv`, `results/clickbench_baseline.csv`,
`results/clickbench_comparison.png`.

---

## H2O GroupBy End-to-End Example

### Step 1 — Download dataset

```bash
python download_dataset.py --dataset h2o --output-dir ./data
```

### Step 2 — Prepare data for Arroyo file source

```bash
python prepare_data.py \
    --dataset h2o \
    --input ./data/G1_1e7_1e2_0_0.csv \
    --output ./data/h2o_arroyo.json \
    --max-rows 1000000
```

### Steps 3–4 — Start infrastructure and Arroyo (same as ClickBench)

### Step 5 — Launch Arroyo sketch pipeline

```bash
python export_to_arroyo.py \
    --streaming-config ./configs/h2o_streaming.yaml \
    --source-type file \
    --input-file ./data/h2o_arroyo.json \
    --file-format json \
    --ts-format rfc3339 \
    --pipeline-name h2o_pipeline \
    --arroyosketch-dir ~/ASAPQuery/asap-summary-ingest \
    --output-dir ./arroyo_outputs
```

### Step 6 — Start QueryEngineRust

```bash
cd ~/ASAPQuery/asap-query-engine
nohup ./target/release/query_engine_rust \
    --kafka-topic sketch_topic --input-format json \
    --config ~/ASAPQuery/asap-tools/execution-utilities/benchmark/configs/h2o_inference.yaml \
    --streaming-config ~/ASAPQuery/asap-tools/execution-utilities/benchmark/configs/h2o_streaming.yaml \
    --http-port 8088 --delete-existing-db --log-level DEBUG \
    --output-dir ./output --streaming-engine arroyo \
    --query-language SQL --lock-strategy per-key \
    --prometheus-scrape-interval 1 > /tmp/query_engine.log 2>&1 &
```

### Step 7 — Load data into ClickHouse (baseline)

```bash
python export_to_database.py \
    --dataset h2o \
    --file-path ./data/G1_1e7_1e2_0_0.csv \
    --init-sql-file ./configs/h2o_init.sql \
    --max-rows 1000000
```

### Step 8 — Generate SQL query files

```bash
python generate_queries.py \
    --table-name h2o_groupby \
    --ts-column timestamp \
    --value-column v1 \
    --group-by-columns id1,id2 \
    --window-size 10 \
    --num-queries 50 \
    --ts-format iso \
    --auto-detect-timestamps \
    --data-file ./data/h2o_arroyo.json \
    --data-file-format json \
    --output-prefix ./queries/h2o
```

### Step 9 — Run benchmark

```bash
python run_benchmark.py \
    --mode both \
    --asap-sql-file ./queries/h2o_asap.sql \
    --baseline-sql-file ./queries/h2o_clickhouse.sql \
    --output-dir ./results \
    --output-prefix h2o
```

---

## Custom Dataset

```bash
# 1. Download (any HTTP URL)
python download_dataset.py --dataset custom \
    --custom-url https://example.com/mydata.json.gz \
    --output-dir ./data

# 2. Prepare (edit prepare_data.py for your schema, or skip if already RFC3339)

# 3. Export to Arroyo
python export_to_arroyo.py \
    --streaming-config ./configs/my_streaming.yaml \
    --source-type file \
    --input-file ./data/mydata.json \
    --file-format json \
    --ts-format rfc3339 \
    --pipeline-name my_pipeline \
    --arroyosketch-dir ~/ASAPQuery/asap-summary-ingest

# 4. Export to ClickHouse
python export_to_database.py \
    --dataset custom \
    --file-path ./data/mydata.json \
    --init-sql-file ./configs/my_init.sql \
    --table-name my_table

# 5. Generate queries
python generate_queries.py \
    --table-name my_table \
    --ts-column event_time \
    --value-column metric_value \
    --group-by-columns region,host \
    --window-size 10 \
    --num-queries 50 \
    --auto-detect-timestamps \
    --data-file ./data/mydata.json \
    --output-prefix ./queries/my_dataset

# 6. Run benchmark
python run_benchmark.py \
    --mode both \
    --asap-sql-file ./queries/my_dataset_asap.sql \
    --baseline-sql-file ./queries/my_dataset_clickhouse.sql \
    --output-dir ./results
```

---

## Reset

```bash
pkill -f "arroyo"; pkill -f "query_engine_rust"
sleep 2
pkill -f "kafka-server-start.sh"; pkill -f "clickhouse server"
sleep 2
rm -rf /tmp/arroyo/

KAFKA=$INSTALL_DIR/kafka/bin
$KAFKA/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic sketch_topic

cd ~/ASAPQuery/asap-summary-ingest
python3 delete_pipeline.py --all_pipelines

$INSTALL_DIR/clickhouse client --query "TRUNCATE TABLE hits"
# or for H2O: $INSTALL_DIR/clickhouse client --query "TRUNCATE TABLE h2o_groupby"
```

---

## Files

| File | Purpose |
|------|---------|
| `download_dataset.py` | Download ClickBench, H2O, or custom datasets |
| `prepare_data.py` | Convert raw data to Arroyo file source format (RFC3339, string columns) |
| `export_to_arroyo.py` | Launch Arroyo sketch pipeline (file or kafka source) |
| `export_to_database.py` | Load data into ClickHouse for baseline |
| `generate_queries.py` | Generate paired ASAP + ClickHouse SQL query files |
| `run_benchmark.py` | Run queries and produce CSV results + plots |
| `configs/` | Dataset-specific streaming/inference YAML and ClickHouse init SQL |
