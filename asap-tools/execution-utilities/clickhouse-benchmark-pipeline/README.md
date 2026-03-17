# ClickHouse Benchmark Pipeline

Benchmark ClickHouse with 43 ClickBench queries using Kafka data ingestion.

## Prerequisites

Install and run Kafka and ClickHouse natively (no Docker):

```bash
# Install Kafka (one-time)
cd /path/to/Utilities/installation/kafka
./install.sh /path/to/Utilities/installation/kafka

# Install ClickHouse (one-time)
cd /path/to/Utilities/installation/clickhouse
./install.sh /path/to/Utilities/installation/clickhouse
```

## Usage

```bash
# 1. Start Kafka (in a terminal)
cd /path/to/Utilities/installation/kafka
./run.sh kafka/

# 2. Create Kafka topic
kafka/bin/kafka-topics.sh --create --topic hits --bootstrap-server localhost:9092

# 3. Start ClickHouse (in another terminal)
cd /path/to/Utilities/installation/clickhouse
./run.sh /path/to/Utilities/installation/clickhouse

# 4. Initialize ClickHouse tables
./scripts/init_clickhouse.sh

# 5. Generate data (choose one mode)
DATA_MODE=clickbench TOTAL_RECORDS=100000 ./scripts/generate_data.sh
DATA_MODE=fake TOTAL_RECORDS=100000 ./scripts/generate_data.sh

# 6. Check data
./scripts/check_data.sh

# 7. Run benchmark
./scripts/run_benchmark.sh
```

## Data Modes

| Mode | Description |
|------|-------------|
| `fake` | Synthetic data |
| `clickbench` | Real ClickBench dataset (~100M rows) |

## Configuration

Edit `config.env` to change defaults. Environment variables override config values.

# Elasticsearch Benchmark Pipeline

## Prerequisites

Follow instructions to install Elasticsearch:
```bash
cd /path/to/Utilities/installation/elastic
```

## Configuration

Edit `config.env` to update the `ES_API_KEY` field after installing Elasticsearch locally.

## Usage

```bash
# 1. Load H2O CSV data into Elastic
./scripts/init_elastic.sh

# 2. Check data
./scripts/check_elastic_data.sh

# 3. Run benchmark
./scripts/run_benchmark.sh h2o_elastic
```
