#!/bin/bash
# Check data ingestion status

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."

# Load config
set -a
source "${PROJECT_DIR}/config.env"
set +a

echo "=== ClickHouse Data Status ==="
echo ""

# Count total rows
echo "Total rows in hits table:"
curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/" \
    --data-binary "SELECT count() FROM hits FORMAT Pretty"
echo ""

# Check recent data
echo "Most recent records:"
curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/" \
    --data-binary "SELECT EventTime, CounterID, UserID, URL FROM hits ORDER BY EventTime DESC LIMIT 5 FORMAT Pretty"
echo ""

# Table size
echo "Table size:"
curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/" \
    --data-binary "SELECT formatReadableSize(sum(bytes)) as size, count() as parts FROM system.parts WHERE table = 'hits' AND active FORMAT Pretty"
echo ""

# Kafka consumer lag (if available)
echo "Kafka consumer status:"
curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/" \
    --data-binary "SELECT * FROM system.kafka_consumers FORMAT Pretty" 2>/dev/null || echo "No Kafka consumer info available"

echo "=== H2O Data Status ==="
curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/" \
    --data-binary "SELECT count() as h2o_rows FROM h2o_groupby FORMAT Pretty"
echo ""
