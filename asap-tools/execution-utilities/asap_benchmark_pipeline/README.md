# ASAP H2O Benchmark Pipeline

Benchmarks ASAP (KLL sketch-based) query serving against a ClickHouse baseline using the [H2O groupby dataset](https://h2oai.github.io/db-benchmark/) (10M rows, 100 groups).

**ASAP mode** streams data through Kafka into Arroyo, which builds KLL sketches per tumbling window per group. The QueryEngine (QE) ingests these sketches and serves approximate quantile queries directly from them.

**Baseline mode** loads the same data into a ClickHouse MergeTree table and runs equivalent quantile queries using ClickHouse's native `quantile()` function.

Each benchmark run produces a CSV of per-query latencies and a latency plot (`.png`).

## Prerequisites

- Kafka and ClickHouse installed (see `../../installation/`)
- Arroyo binary built (`arroyo/target/release/arroyo`)
- QueryEngine binary built (`target/release/query_engine_rust`)
- Python 3 with `requests`, `kafka-python`, `gdown`, `matplotlib`

## Usage

### Full pipeline (recommended)

```bash
# Run baseline benchmark (starts infra, loads data, runs queries)
./run_pipeline.sh --mode baseline --load-data --output baseline_results.csv

# Clean up between runs for fair comparison
./cleanup.sh

# Run ASAP benchmark
./run_pipeline.sh --mode asap --load-data --output asap_results.csv

# Run both back-to-back and generate comparison plot
./run_pipeline.sh --mode both --load-data
```

### Options

```
--mode [asap|baseline|both]  Execution mode (default: asap)
--load-data                  Download and load the H2O dataset
--output [FILE]              Output CSV file
--skip-infra                 Skip starting Kafka/ClickHouse (already running)
--max-rows [N]               Limit rows loaded (0 = all, default: all)
```

### Cleanup

`cleanup.sh` kills all processes (QE, Arroyo, Kafka, ClickHouse), clears Kafka topics, drops the ClickHouse table, and flushes OS page caches to ensure identical starting conditions between runs.

```bash
./cleanup.sh           # full cleanup (requires sudo for cache drop)
./cleanup.sh --no-sudo # skip OS cache clearing
```

### Benchmark only (infra already running, data already loaded)

```bash
python3 run_benchmark.py --mode baseline --output baseline_results.csv
python3 run_benchmark.py --mode asap --output asap_results.csv
```

### Comparison plot

After running both modes, generate a side-by-side comparison:

```bash
python3 plot_latency.py
```

## Configuration

| File | Purpose |
|------|---------|
| `streaming_config.yaml` | Arroyo sketch pipeline config (window size, aggregation type, grouping) |
| `inference_config.yaml` | QE query-to-sketch mapping (must match `streaming_config.yaml` window size) |
| `h2o_init.sql` | ClickHouse table schema |
| `asap_quantile_queries.sql` | Queries for ASAP mode (QUANTILE syntax) |
| `clickhouse_quantile_queries.sql` | Queries for baseline mode (quantile() syntax) |

### Changing window size

To benchmark with a different tumbling window size (e.g., 120s):

1. Set `tumblingWindowSize` and `windowSize` in `streaming_config.yaml`
2. Set the DATEADD offset in `inference_config.yaml` to match (e.g., `-120`)
3. Regenerate query files with matching window boundaries

## Output

Each run produces:
- `<output>.csv` — per-query latencies, row counts, and result previews
- `<output>.png` — bar chart of latency by query execution order
- `latency_comparison.png` — side-by-side comparison (from `plot_latency.py`)
