# ASAP Query Latency Benchmark

Benchmark ASAP query performance with ClickBench hits dataset. Outputs CSV with query latency and results.

## Quick Start

0. /scripts/init_elastic.sh

### 1. Setup ASAP Stack

Follow `/docs/03-how-to-guides/operations/manual-stack-run-clickhouse.md` to start:
- Kafka
- ClickHouse
- Arroyo cluster
- QueryEngineRust (port 8088)

### 2. Run Benchmark

```bash
# Run ASAP benchmark
./run_benchmark.py --load-data --output asap_results.csv

# Default mode is ASAP (queries go through QueryEngine on port 8088)
```

**Output:** CSV with 33 queries, latencies, and results

### Optional: Baseline Comparison

```bash
# Run without ASAP (direct ClickHouse) for comparison
./run_benchmark.py --mode baseline --output baseline_results.csv
```

## Files

- `asap_test_queries.sql` - 33 SQL queries (Spatial, Temporal, SpatioTemporal, Nested)
- `run_benchmark.py` - Benchmark runner
- `inference_config.yaml` - ASAP configuration
- `asap_results.csv` - Benchmark output
