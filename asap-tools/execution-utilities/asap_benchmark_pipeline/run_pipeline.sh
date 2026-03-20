#!/bin/bash

# Full pipeline script: starts Kafka, ClickHouse, Arroyo, and QueryEngine,
# then runs the benchmark.
#
# Usage:
#   ASAP mode (full from scratch):
#     ./run_pipeline.sh --mode asap --load-data --output asap_results_run1.csv
#
#   ASAP mode (infra already running, data already loaded):
#     ./run_pipeline.sh --mode asap --skip-infra --output asap_results_run2.csv
#
#   Baseline mode (full from scratch):
#     ./run_pipeline.sh --mode baseline --load-data --output baseline_results.csv
#
#   Baseline mode (ClickHouse already running):
#     ./run_pipeline.sh --mode baseline --skip-infra --output baseline_results.csv

set -euo pipefail

# ==========================================
# 1. DYNAMIC PATH RESOLUTION
# ==========================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." &>/dev/null && pwd)"

KAFKA_INSTALL_DIR="$ROOT_DIR/Utilities/installation/kafka"
CLICKHOUSE_INSTALL_DIR="$ROOT_DIR/Utilities/installation/clickhouse"
KAFKA_DIR="$KAFKA_INSTALL_DIR/kafka"
CLICKHOUSE_DIR="$CLICKHOUSE_INSTALL_DIR/clickhouse"

# ==========================================
# 2. ARGUMENT PARSING
# ==========================================
MODE="asap"
LOAD_DATA=0
OUTPUT_FILE="asap_results_run1.csv"
SKIP_INFRA=0

print_usage() {
    echo "Usage: ./run_pipeline.sh [OPTIONS]"
    echo "Options:"
    echo "  --mode [asap|baseline]   Execution mode (default: asap)"
    echo "  --load-data              Stream H2O dataset into ClickHouse/Kafka"
    echo "  --output [FILE]          Output CSV file (default: asap_results_run1.csv)"
    echo "  --skip-infra             Skip starting Kafka/ClickHouse (assume already running)"
    echo "  --help                   Show this message"
}

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --mode)       MODE="$2"; shift ;;
        --load-data)  LOAD_DATA=1 ;;
        --output)     OUTPUT_FILE="$2"; shift ;;
        --skip-infra) SKIP_INFRA=1 ;;
        --help)       print_usage; exit 0 ;;
        *) echo "Unknown parameter: $1"; print_usage; exit 1 ;;
    esac
    shift
done

# ==========================================
# 3. HELPER FUNCTIONS
# ==========================================

# Wait for a URL to return HTTP 200. Args: name url [max_seconds]
wait_for_url() {
    local name="$1"
    local url="$2"
    local max_seconds="${3:-120}"
    local elapsed=0
    echo "Waiting for $name..."
    while ! curl -sf "$url" >/dev/null 2>&1; do
        sleep 2
        elapsed=$((elapsed + 2))
        if [ "$elapsed" -ge "$max_seconds" ]; then
            echo "ERROR: $name did not become ready within ${max_seconds}s"
            echo "Check logs for details"
            exit 1
        fi
    done
    echo "$name is ready"
}

wait_for_kafka() {
    local max_seconds="${1:-120}"
    local elapsed=0
    echo "Waiting for Kafka..."
    while ! "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
        sleep 2
        elapsed=$((elapsed + 2))
        if [ "$elapsed" -ge "$max_seconds" ]; then
            echo "ERROR: Kafka did not become ready within ${max_seconds}s"
            echo "Check /tmp/kafka.log for details"
            exit 1
        fi
    done
    echo "Kafka is ready"
}

wait_for_arroyo_pipeline_running() {
    local max_seconds="${1:-300}"
    local elapsed=0
    echo "Waiting for Arroyo pipeline 'asap_h2o_pipeline' to reach RUNNING state..."
    echo "(This may take up to ${max_seconds}s while Arroyo compiles Rust UDFs)"
    while true; do
        state=$(curl -sf "http://localhost:5115/api/v1/pipelines" 2>/dev/null | \
            python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for p in data.get('data', []):
        if p.get('name') == 'asap_h2o_pipeline':
            # Arroyo returns null/None for state when actively running
            state = p.get('state')
            action = p.get('action', '')
            stop = p.get('stop', '')
            if state is None and stop == 'none':
                print('running')
            else:
                print(str(state).lower() if state else 'unknown')
            sys.exit(0)
    print('not_found')
except Exception:
    print('error')
" 2>/dev/null || echo "error")

        if [ "$state" = "running" ]; then
            echo "Pipeline is RUNNING"
            return 0
        fi

        echo "  Pipeline state: $state (elapsed: ${elapsed}s)"
        sleep 5
        elapsed=$((elapsed + 5))

        if [ "$elapsed" -ge "$max_seconds" ]; then
            echo "ERROR: Pipeline did not reach RUNNING state within ${max_seconds}s"
            echo "Check /tmp/arroyo.log for details"
            exit 1
        fi
    done
}

wait_for_data_loaded() {
    local min_rows="${1:-9000000}"
    echo "Waiting for ClickHouse h2o_groupby to have at least $min_rows rows..."
    while true; do
        count=$(curl -sf "http://localhost:8123/" -d "SELECT count(*) FROM h2o_groupby" 2>/dev/null | tr -d '[:space:]' || echo "0")
        if [ -n "$count" ] && [ "$count" -ge "$min_rows" ] 2>/dev/null; then
            echo "Data ready: $count rows"
            return 0
        fi
        echo "  Rows in h2o_groupby: ${count:-0}"
        sleep 5
    done
}

ensure_extracted() {
    local zip_file="$1"
    local target_dir="$2"
    if [ ! -d "$target_dir" ]; then
        echo "Extracting $(basename "$zip_file")..."
        unzip -q "$zip_file" -d "$(dirname "$target_dir")"
        echo "Extracted to $target_dir"
    fi
}

# Start Kafka only if it isn't already responding
start_kafka_if_needed() {
    if "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
        echo "Kafka already running, skipping start"
        return 0
    fi
    echo "Starting Kafka..."
    nohup bash "$KAFKA_INSTALL_DIR/run.sh" "$KAFKA_DIR" >/tmp/kafka.log 2>&1 &
    wait_for_kafka 120
}

# Start ClickHouse only if it isn't already responding
start_clickhouse_if_needed() {
    if curl -sf "http://localhost:8123/ping" >/dev/null 2>&1; then
        echo "ClickHouse already running, skipping start"
        return 0
    fi
    echo "Starting ClickHouse..."
    nohup bash "$CLICKHOUSE_INSTALL_DIR/run.sh" "$CLICKHOUSE_DIR" >/tmp/clickhouse.log 2>&1 &
    wait_for_url "ClickHouse" "http://localhost:8123/ping" 120
}

init_clickhouse_tables() {
    echo "Initializing ClickHouse tables..."
    python3 - <<'PYEOF'
import requests

with open("h2o_init.sql") as f:
    content = f.read()

statements = [s.strip() for s in content.split(";") if s.strip()]
for sql in statements:
    r = requests.post("http://localhost:8123/", data=sql)
    if not r.ok:
        print(f"  WARN: {r.text.strip()[:120]} | SQL: {sql[:60]}")
    else:
        print(f"  OK: {sql[:60]}")
PYEOF
}

cleanup_background_jobs() {
    echo "Cleaning up ASAP background processes..."
    pkill -f "arroyo.*cluster" || true
    pkill -f "query_engine_rust" || true
    sleep 2
}

# ==========================================
# 4. BASELINE MODE
# ==========================================
if [ "$MODE" = "baseline" ]; then
    echo "RUNNING IN BASELINE MODE"

    if [ "$SKIP_INFRA" -eq 0 ]; then
        ensure_extracted "$KAFKA_INSTALL_DIR/kafka.zip" "$KAFKA_DIR"
        ensure_extracted "$CLICKHOUSE_INSTALL_DIR/clickhouse.zip" "$CLICKHOUSE_DIR"
        start_kafka_if_needed
        start_clickhouse_if_needed
    fi

    if [ "$LOAD_DATA" -eq 1 ]; then
        cd "$SCRIPT_DIR"
        init_clickhouse_tables
    fi

    CMD="python3 run_benchmark.py --mode baseline --output $OUTPUT_FILE"
    [ "$LOAD_DATA" -eq 1 ] && CMD="$CMD --load-data"

    echo "Executing: $CMD"
    eval "$CMD"
    echo "Baseline run complete!"

# ==========================================
# 5. ASAP MODE
# ==========================================
elif [ "$MODE" = "asap" ]; then
    echo "RUNNING IN ASAP MODE"

    # Clean up any stale processes from previous runs
    cleanup_background_jobs

    if [ "$SKIP_INFRA" -eq 0 ]; then
        ensure_extracted "$KAFKA_INSTALL_DIR/kafka.zip" "$KAFKA_DIR"
        ensure_extracted "$CLICKHOUSE_INSTALL_DIR/clickhouse.zip" "$CLICKHOUSE_DIR"
        start_kafka_if_needed
        start_clickhouse_if_needed
    fi

    # Initialize ClickHouse tables only when loading fresh data
    # (h2o_init.sql drops and recreates tables, which would wipe existing data)
    if [ "$LOAD_DATA" -eq 1 ]; then
        cd "$SCRIPT_DIR"
        init_clickhouse_tables
    fi

    # Start Arroyo cluster
    echo "Starting Arroyo cluster..."
    cd "$ROOT_DIR/arroyo"
    nohup ./target/release/arroyo --config "$ROOT_DIR/ArroyoSketch/config.yaml" cluster \
        >/tmp/arroyo.log 2>&1 &

    wait_for_url "Arroyo API" "http://localhost:5115/api/v1/pipelines" 60

    # Submit Arroyo pipeline
    echo "Submitting Arroyo pipeline..."
    cd "$ROOT_DIR/ArroyoSketch"
    python3 run_arroyosketch.py \
        --source_type kafka \
        --kafka_input_format json \
        --input_kafka_topic h2o_groupby \
        --output_format json \
        --pipeline_name asap_h2o_pipeline \
        --config_file_path "$SCRIPT_DIR/streaming_config.yaml" \
        --output_kafka_topic sketch_topic \
        --output_dir ./outputs \
        --parallelism 1 \
        --query_language sql

    # Poll until pipeline is RUNNING (Arroyo compiles Rust UDFs, takes ~1-3 minutes)
    wait_for_arroyo_pipeline_running 300

    # Wait for Arroyo's Kafka source worker to fully initialize and assign partitions.
    # load_h2o_data re-runs h2o_init.sql (DROP/CREATE h2o_groupby_queue), which causes a
    # brief Kafka metadata disruption. If this races with Arroyo's initial partition assignment,
    # the worker sees 0 partitions and goes permanently idle. A short sleep avoids the race.
    echo "Waiting 20s for Arroyo worker to initialize Kafka partition assignment..."
    sleep 20

    # Load data through Kafka so Arroyo builds sketches AND MergeTree is populated
    cd "$SCRIPT_DIR"
    if [ "$LOAD_DATA" -eq 1 ]; then
        echo "Loading data through Kafka (ASAP mode)..."
        python3 run_benchmark.py --mode asap --load-data

        # Wait for MergeTree to reflect the data (materialized view consumes from Kafka)
        wait_for_data_loaded 9000000

        # Send a flush record to advance Arroyo's watermark past the last window.
        # This ensures the final 120s tumbling window is closed and its sketch is emitted.
        echo "Sending watermark flush record to Kafka..."
        FLUSH_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
        curl -sf "http://localhost:8123/?query=INSERT%20INTO%20h2o_groupby_queue%20FORMAT%20JSONEachRow" \
            --data-raw "{\"timestamp\":\"${FLUSH_TS}\",\"id1\":\"flush\",\"id2\":\"flush\",\"id3\":\"flush\",\"id4\":0,\"id5\":0,\"id6\":0,\"v1\":0,\"v2\":0,\"v3\":0.0}" \
            || echo "Warning: flush record insert failed (non-fatal)"

        # Give Arroyo additional time to close and flush the final sketch windows
        echo "Waiting 30s for Arroyo to flush all sketch windows..."
        sleep 30
    else
        echo "Skipping data load (--load-data not provided)"
    fi

    # Start QueryEngine
    echo "Starting QueryEngine..."
    cd "$ROOT_DIR/QueryEngineRust"
    nohup ./target/release/query_engine_rust \
        --kafka-topic sketch_topic \
        --input-format json \
        --config "$SCRIPT_DIR/inference_config.yaml" \
        --streaming-config "$SCRIPT_DIR/streaming_config.yaml" \
        --http-port 8088 \
        --delete-existing-db \
        --log-level info \
        --output-dir ./output \
        --streaming-engine arroyo \
        --query-language SQL \
        --lock-strategy per-key \
        --prometheus-scrape-interval 1 >/tmp/query_engine.log 2>&1 &

    # Poll until QueryEngine HTTP server is accepting connections
    wait_for_url "QueryEngine" "http://localhost:8088/clickhouse/query?query=SELECT+1" 60

    # Run benchmark against the sketches built during the data load above.
    # Uses asap_mode_queries.sql (default for asap mode) with QUANTILE(0.95, v1) and NOW()-based
    # 600s windows that contain the recently-closed 120s tumbling sketch windows.
    echo "Executing benchmark queries against existing sketches..."
    cd "$SCRIPT_DIR"
    python3 run_benchmark.py --mode asap --output "$OUTPUT_FILE"

    echo "ASAP run complete! Results: $OUTPUT_FILE"

else
    echo "Invalid mode: $MODE. Use 'asap' or 'baseline'."
    exit 1
fi
