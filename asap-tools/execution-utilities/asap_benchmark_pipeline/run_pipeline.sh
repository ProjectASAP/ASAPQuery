#!/bin/bash
#
# Full benchmark pipeline for H2O groupby: ASAP vs ClickHouse baseline.
#
# Usage:
#   # ASAP mode (full from scratch):
#   ./run_pipeline.sh --mode asap --load-data --output asap_results.csv
#
#   # Baseline mode (full from scratch):
#   ./run_pipeline.sh --mode baseline --load-data --output baseline_results.csv
#
#   # Both modes back-to-back:
#   ./run_pipeline.sh --mode both --load-data
#
#   # Skip infrastructure startup (already running):
#   ./run_pipeline.sh --mode asap --skip-infra --output asap_results.csv
#
# Environment variables (override defaults):
#   KAFKA_INSTALL_DIR   Path to kafka installation dir (contains run.sh + kafka/)
#   CLICKHOUSE_INSTALL_DIR  Path to clickhouse installation dir
#   ARROYO_BIN          Path to arroyo binary
#   ARROYO_CONFIG       Path to arroyo config.yaml
#   QE_BIN              Path to query_engine_rust binary
#   ARROYOSKETCH_DIR    Path to asap-summary-ingest directory

set -euo pipefail

# ==========================================
# 1. PATH RESOLUTION
# ==========================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
TOOLS_DIR="$(cd "$SCRIPT_DIR/../.." &>/dev/null && pwd)"
ROOT_DIR="$(cd "$TOOLS_DIR/.." &>/dev/null && pwd)"

KAFKA_INSTALL_DIR="${KAFKA_INSTALL_DIR:-$TOOLS_DIR/installation/kafka}"
CLICKHOUSE_INSTALL_DIR="${CLICKHOUSE_INSTALL_DIR:-$TOOLS_DIR/installation/clickhouse}"
KAFKA_DIR="$KAFKA_INSTALL_DIR/kafka"
CLICKHOUSE_DIR="$CLICKHOUSE_INSTALL_DIR/clickhouse"
ARROYO_BIN="${ARROYO_BIN:-$ROOT_DIR/arroyo/target/release/arroyo}"
ARROYO_CONFIG="${ARROYO_CONFIG:-$ROOT_DIR/asap-summary-ingest/config.yaml}"
QE_BIN="${QE_BIN:-$ROOT_DIR/target/release/query_engine_rust}"
ARROYOSKETCH_DIR="${ARROYOSKETCH_DIR:-$ROOT_DIR/asap-summary-ingest}"

# ==========================================
# 2. ARGUMENT PARSING
# ==========================================
MODE="asap"
LOAD_DATA=0
OUTPUT_FILE=""
SKIP_INFRA=0
MAX_ROWS=0

print_usage() {
    echo "Usage: ./run_pipeline.sh [OPTIONS]"
    echo "Options:"
    echo "  --mode [asap|baseline|both]  Execution mode (default: asap)"
    echo "  --load-data                  Load H2O dataset"
    echo "  --output [FILE]              Output CSV file"
    echo "  --skip-infra                 Skip starting infrastructure"
    echo "  --max-rows [N]               Max rows to load (0 = all)"
    echo "  --help                       Show this message"
}

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --mode)       MODE="$2"; shift ;;
        --load-data)  LOAD_DATA=1 ;;
        --output)     OUTPUT_FILE="$2"; shift ;;
        --skip-infra) SKIP_INFRA=1 ;;
        --max-rows)   MAX_ROWS="$2"; shift ;;
        --help)       print_usage; exit 0 ;;
        *) echo "Unknown parameter: $1"; print_usage; exit 1 ;;
    esac
    shift
done

# ==========================================
# 3. HELPER FUNCTIONS
# ==========================================

wait_for_url() {
    local name="$1" url="$2" max_seconds="${3:-120}" elapsed=0
    echo "Waiting for $name..."
    while ! curl -sf "$url" >/dev/null 2>&1; do
        sleep 2; elapsed=$((elapsed + 2))
        if [ "$elapsed" -ge "$max_seconds" ]; then
            echo "ERROR: $name did not become ready within ${max_seconds}s"
            exit 1
        fi
    done
    echo "$name is ready"
}

wait_for_kafka() {
    local max_seconds="${1:-120}" elapsed=0
    echo "Waiting for Kafka..."
    while ! "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
        sleep 2; elapsed=$((elapsed + 2))
        if [ "$elapsed" -ge "$max_seconds" ]; then
            echo "ERROR: Kafka did not become ready within ${max_seconds}s"
            exit 1
        fi
    done
    echo "Kafka is ready"
}

wait_for_arroyo_pipeline_running() {
    local max_seconds="${1:-300}" elapsed=0
    echo "Waiting for Arroyo pipeline to reach RUNNING state..."
    while true; do
        state=$(curl -sf "http://localhost:5115/api/v1/pipelines" 2>/dev/null | \
            python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for p in data.get('data', []):
        if p.get('name') == 'asap_h2o_pipeline':
            state = p.get('state')
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
        sleep 5; elapsed=$((elapsed + 5))
        if [ "$elapsed" -ge "$max_seconds" ]; then
            echo "ERROR: Pipeline did not reach RUNNING state within ${max_seconds}s"
            exit 1
        fi
    done
}

start_kafka_if_needed() {
    if "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
        echo "Kafka already running"
        return 0
    fi
    echo "Starting Kafka..."
    nohup bash "$KAFKA_INSTALL_DIR/run.sh" "$KAFKA_DIR" >/tmp/kafka.log 2>&1 &
    wait_for_kafka 120
}

start_clickhouse_if_needed() {
    if curl -sf "http://localhost:8123/ping" >/dev/null 2>&1; then
        echo "ClickHouse already running"
        return 0
    fi
    echo "Starting ClickHouse..."
    nohup bash "$CLICKHOUSE_INSTALL_DIR/run.sh" "$CLICKHOUSE_DIR" >/tmp/clickhouse.log 2>&1 &
    wait_for_url "ClickHouse" "http://localhost:8123/ping" 120
}

purge_kafka_topic() {
    local topic="$1" max_wait=30 elapsed=0
    echo "Purging Kafka topic '$topic'..."
    "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 \
        --delete --topic "$topic" 2>/dev/null || true
    while "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 \
            --list 2>/dev/null | grep -qx "$topic"; do
        sleep 1; elapsed=$((elapsed + 1))
        if [ "$elapsed" -ge "$max_wait" ]; then
            echo "  WARN: topic '$topic' still exists after ${max_wait}s"
            break
        fi
    done
    "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 \
        --create --topic "$topic" --partitions 1 --replication-factor 1 \
        --config retention.ms=-1 \
        2>/dev/null || true
    echo "  '$topic' reset"
}

get_sketch_topic_offset() {
    "$KAFKA_DIR/bin/kafka-get-offsets.sh" --bootstrap-server localhost:9092 \
        --topic sketch_topic 2>/dev/null | cut -d: -f3 || echo "0"
}

wait_for_new_sketches() {
    local initial_offset="$1" max_seconds="${2:-600}" elapsed=0
    echo "Waiting for sketches (baseline offset: $initial_offset)..."
    while true; do
        current_offset=$(get_sketch_topic_offset)
        if [ -n "$current_offset" ] && [ "$current_offset" -gt "$initial_offset" ] 2>/dev/null; then
            echo "Sketches emitted (offset: $current_offset, +$((current_offset - initial_offset)) new)"
            return 0
        fi
        echo "  sketch_topic offset: ${current_offset} (elapsed: ${elapsed}s) — sending flush nudge..."
        FLUSH_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
        echo "{\"timestamp\":\"${FLUSH_TS}\",\"id1\":\"flush\",\"id2\":\"flush\",\"id3\":\"flush\",\"id4\":0,\"id5\":0,\"id6\":0,\"v1\":0,\"v2\":0,\"v3\":0.0}" | \
            "$KAFKA_DIR/bin/kafka-console-producer.sh" --bootstrap-server localhost:9092 --topic h2o_groupby \
            >/dev/null 2>&1 || true
        sleep 30; elapsed=$((elapsed + 30))
        if [ "$elapsed" -ge "$max_seconds" ]; then
            echo "ERROR: No sketches after ${max_seconds}s"
            exit 1
        fi
    done
}

cleanup_background_jobs() {
    echo "Cleaning up background processes..."
    pkill -f "arroyo.*cluster" 2>/dev/null || true
    pkill -f "query_engine_rust" 2>/dev/null || true
    sleep 2
}

# ==========================================
# 4. BASELINE MODE
# ==========================================
run_baseline() {
    local output="${1:-baseline_results.csv}"
    echo "========================================"
    echo "RUNNING BASELINE MODE"
    echo "========================================"

    if [ "$SKIP_INFRA" -eq 0 ]; then
        start_clickhouse_if_needed
    fi

    if [ "$LOAD_DATA" -eq 1 ]; then
        cd "$SCRIPT_DIR"
        python3 run_benchmark.py --mode baseline --load-data --no-benchmark --max-rows "$MAX_ROWS"
    fi

    cd "$SCRIPT_DIR"
    python3 run_benchmark.py --mode baseline --output "$output"
    echo "Baseline complete: $output"
}

# ==========================================
# 5. ASAP MODE
# ==========================================
run_asap() {
    local output="${1:-asap_results.csv}"
    echo "========================================"
    echo "RUNNING ASAP MODE"
    echo "========================================"

    cleanup_background_jobs

    if [ "$SKIP_INFRA" -eq 0 ]; then
        start_kafka_if_needed
        start_clickhouse_if_needed
    fi

    # --- Purge Kafka topics for clean slate ---
    purge_kafka_topic h2o_groupby
    purge_kafka_topic sketch_topic

    # --- Load data into ClickHouse (direct) for data presence ---
    if [ "$LOAD_DATA" -eq 1 ]; then
        cd "$SCRIPT_DIR"
        python3 run_benchmark.py --mode asap --load-data --no-benchmark --max-rows "$MAX_ROWS"
    fi

    # --- Start Arroyo ---
    echo "Starting Arroyo cluster..."
    cd "$(dirname "$ARROYO_BIN")"
    nohup "$ARROYO_BIN" --config "$ARROYO_CONFIG" cluster >/tmp/arroyo.log 2>&1 &
    wait_for_url "Arroyo API" "http://localhost:5115/api/v1/pipelines" 60

    # --- Submit pipeline ---
    echo "Submitting Arroyo pipeline..."
    cd "$ARROYOSKETCH_DIR"
    python3 run_arroyosketch.py \
        --source_type kafka \
        --kafka_input_format json \
        --input_kafka_topic h2o_groupby \
        --output_format json \
        --pipeline_name asap_h2o_pipeline \
        --config_file_path "$SCRIPT_DIR/streaming_config.yaml" \
        --output_kafka_topic sketch_topic \
        --output_dir "$SCRIPT_DIR/outputs" \
        --parallelism 1 \
        --query_language sql

    wait_for_arroyo_pipeline_running 300

    echo "Waiting 20s for Arroyo worker initialization..."
    sleep 20

    INITIAL_SKETCH_OFFSET=$(get_sketch_topic_offset)

    # --- Stream data through Kafka for Arroyo ---
    if [ "$LOAD_DATA" -eq 1 ]; then
        cd "$SCRIPT_DIR"
        echo "Streaming data to Kafka for sketch generation..."
        python3 run_benchmark.py --load-kafka --no-benchmark --max-rows "$MAX_ROWS"

        # Wait for sketches
        wait_for_new_sketches "$INITIAL_SKETCH_OFFSET" 600
    fi

    # --- Reset consumer group offsets ---
    echo "Resetting query-engine-rust consumer group offsets..."
    "$KAFKA_DIR/bin/kafka-consumer-groups.sh" --bootstrap-server localhost:9092 \
        --group query-engine-rust --topic sketch_topic \
        --reset-offsets --to-earliest --execute 2>/dev/null || true

    # --- Start QueryEngine ---
    echo "Starting QueryEngine..."
    cd "$(dirname "$QE_BIN")"
    nohup env TZ=UTC "$QE_BIN" \
        --kafka-topic sketch_topic \
        --input-format json \
        --config "$SCRIPT_DIR/inference_config.yaml" \
        --streaming-config "$SCRIPT_DIR/streaming_config.yaml" \
        --http-port 8088 \
        --delete-existing-db \
        --log-level info \
        --output-dir "$SCRIPT_DIR/output" \
        --streaming-engine arroyo \
        --query-language SQL \
        --lock-strategy per-key \
        --prometheus-scrape-interval 1 >/tmp/query_engine.log 2>&1 &

    wait_for_url "QueryEngine" "http://localhost:8088/clickhouse/query?query=SELECT+1" 60

    echo "Waiting 60s for QueryEngine to ingest sketches..."
    sleep 60

    # --- Run benchmark ---
    cd "$SCRIPT_DIR"
    python3 run_benchmark.py --mode asap --output "$output"
    echo "ASAP complete: $output"
}

# ==========================================
# 6. DISPATCH
# ==========================================
case "$MODE" in
    baseline)
        run_baseline "${OUTPUT_FILE:-baseline_results.csv}"
        ;;
    asap)
        run_asap "${OUTPUT_FILE:-asap_results.csv}"
        ;;
    both)
        run_baseline "baseline_results.csv"
        run_asap "asap_results.csv"
        echo ""
        echo "========================================"
        echo "Generating comparison plot..."
        echo "========================================"
        cd "$SCRIPT_DIR"
        python3 plot_latency.py
        ;;
    *)
        echo "Invalid mode: $MODE. Use 'asap', 'baseline', or 'both'."
        exit 1
        ;;
esac
