#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."

# Load config
set -a
source "${PROJECT_DIR}/config.env"
set +a

# --- CONFIGURATION ---
CH_INSTALL_DIR="${PROJECT_DIR}/../../Utilities/installation/clickhouse/clickhouse"
CLICKHOUSE_BIN="${CH_INSTALL_DIR}/clickhouse"

# Set Defaults if config variables are missing
CH_HOST="${CLICKHOUSE_HOST:-127.0.0.1}"
CH_PORT="${CLICKHOUSE_PORT:-9000}"

# Check binary
if [ ! -f "${CLICKHOUSE_BIN}" ]; then
    echo "Error: ClickHouse binary not found at ${CLICKHOUSE_BIN}"
    exit 1
fi

# Select Mode
if [ "${DATA_MODE}" == "fake" ] || [ "${DATA_MODE}" == "clickbench" ]; then
    echo "Mode: ClickBench (Real or Fake)"
    SQL_FILE="${PROJECT_DIR}/clickhouse/schema.sql"
elif [ "${DATA_MODE}" == "h2o" ]; then
    echo "Mode: H2O Benchmark"
    SQL_FILE="${PROJECT_DIR}/clickhouse/h2o_init.sql"
else
    echo "Error: Unknown DATA_MODE '${DATA_MODE}'"
    exit 1
fi

echo "Initializing ClickHouse tables using ${SQL_FILE}..."
echo "Connecting to ${CH_HOST}:${CH_PORT}..."

# Execute SQL (Using --flag=value syntax to prevent parsing errors)
"${CLICKHOUSE_BIN}" client \
    --host="${CH_HOST}" \
    --port="${CH_PORT}" \
    --multiquery < "${SQL_FILE}"

echo "ClickHouse initialization complete."