#!/bin/bash

if [ $# -ne 1 ]; then
  echo "Usage: $0 <path to clickhouse directory>"
  exit 1
fi

CLICKHOUSE_DIR=$1

# Load config from benchmark pipeline
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/../../../ExecutionUtilities/clickhouse-benchmark-pipeline/config.env"
if [ -f "$CONFIG_FILE" ]; then
  set -a
  source "$CONFIG_FILE"
  set +a
fi

export TZ=UTC

cd "$CLICKHOUSE_DIR" || exit
./clickhouse server -- --http_port "${CLICKHOUSE_HTTP_PORT:-8123}" --tcp_port 9000
