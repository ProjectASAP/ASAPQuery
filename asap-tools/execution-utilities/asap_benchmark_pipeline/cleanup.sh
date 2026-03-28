#!/bin/bash
#
# Clean up all benchmark processes, Kafka state, ClickHouse data, and OS caches
# so that the next run_pipeline.sh invocation starts from identical conditions.
#
# Usage:
#   ./cleanup.sh           # full cleanup (requires sudo for cache drop)
#   ./cleanup.sh --no-sudo # skip OS cache clearing (no sudo required)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
TOOLS_DIR="$(cd "$SCRIPT_DIR/../.." &>/dev/null && pwd)"

KAFKA_INSTALL_DIR="${KAFKA_INSTALL_DIR:-$TOOLS_DIR/installation/kafka}"
CLICKHOUSE_INSTALL_DIR="${CLICKHOUSE_INSTALL_DIR:-$TOOLS_DIR/installation/clickhouse}"
KAFKA_DIR="$KAFKA_INSTALL_DIR/kafka"
CLICKHOUSE_DIR="$CLICKHOUSE_INSTALL_DIR/clickhouse"

NO_SUDO=0
for arg in "$@"; do
    case "$arg" in
        --no-sudo) NO_SUDO=1 ;;
        --help) echo "Usage: ./cleanup.sh [--no-sudo]"; exit 0 ;;
    esac
done

# ==========================================
# 1. Kill application processes
# ==========================================
echo "Stopping application processes..."
pkill -f "query_engine_rust" 2>/dev/null || true
pkill -f "arroyo.*cluster" 2>/dev/null || true
sleep 2
# Force-kill any stragglers
pkill -9 -f "query_engine_rust" 2>/dev/null || true
pkill -9 -f "arroyo.*cluster" 2>/dev/null || true

# ==========================================
# 2. Stop Kafka
# ==========================================
echo "Stopping Kafka..."
if [ -x "$KAFKA_DIR/bin/kafka-server-stop.sh" ]; then
    "$KAFKA_DIR/bin/kafka-server-stop.sh" 2>/dev/null || true
fi
if [ -x "$KAFKA_DIR/bin/zookeeper-server-stop.sh" ]; then
    "$KAFKA_DIR/bin/zookeeper-server-stop.sh" 2>/dev/null || true
fi
sleep 2
pkill -f "kafka\.Kafka" 2>/dev/null || true
pkill -f "QuorumPeerMain" 2>/dev/null || true
pkill -9 -f "kafka\.Kafka" 2>/dev/null || true
pkill -9 -f "QuorumPeerMain" 2>/dev/null || true

# Clean Kafka data directories (topics, consumer group offsets, logs)
if [ -d "$KAFKA_DIR" ]; then
    echo "Clearing Kafka data..."
    rm -rf "$KAFKA_DIR/data" "$KAFKA_DIR/logs" /tmp/kafka-logs /tmp/zookeeper 2>/dev/null || true
fi

# ==========================================
# 3. Stop ClickHouse and clear its data
# ==========================================
echo "Stopping ClickHouse..."
if curl -sf "http://localhost:8123/ping" >/dev/null 2>&1; then
    # Drop the table before stopping so next run starts clean
    curl -sf "http://localhost:8123" -d "DROP TABLE IF EXISTS h2o_groupby" 2>/dev/null || true
fi
pkill -f "clickhouse-server" 2>/dev/null || true
pkill -f "clickhouse server" 2>/dev/null || true
sleep 2
pkill -9 -f "clickhouse-server" 2>/dev/null || true
pkill -9 -f "clickhouse server" 2>/dev/null || true

# Clear ClickHouse data directory
if [ -d "$CLICKHOUSE_DIR" ]; then
    echo "Clearing ClickHouse data..."
    rm -rf "$CLICKHOUSE_DIR/data" "$CLICKHOUSE_DIR/store" "$CLICKHOUSE_DIR/metadata" 2>/dev/null || true
fi

# ==========================================
# 4. Clear QE output directories
# ==========================================
echo "Clearing QE output..."
rm -rf "$SCRIPT_DIR/output" "$SCRIPT_DIR/outputs" 2>/dev/null || true

# ==========================================
# 5. Drop OS page cache and dentries/inodes
# ==========================================
if [ "$NO_SUDO" -eq 0 ]; then
    echo "Dropping OS page cache, dentries, and inodes..."
    sync
    sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    echo "OS caches cleared"
else
    echo "Skipping OS cache clearing (--no-sudo)"
fi

echo "Cleanup complete. Next run_pipeline.sh will start from a clean state."
