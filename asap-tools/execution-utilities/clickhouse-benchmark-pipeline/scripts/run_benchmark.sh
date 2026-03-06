#!/bin/bash
# ClickBench Benchmark Runner
# Reads queries from benchmark_queries.sql and reports timing

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."

# Load config
set -a
source "${PROJECT_DIR}/config.env"
set +a

OUTPUT_DIR="${PROJECT_DIR}/${BENCHMARK_RESULTS_DIR}"

mkdir -p "$OUTPUT_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)

if [ "$1" == "h2o" ]; then
    QUERIES_FILE="${PROJECT_DIR}/h2o_queries.sql"
    RESULTS_FILE="${OUTPUT_DIR}/benchmark_h2o_${TIMESTAMP}.csv"
    DATABASE_TYPE="clickhouse"
    echo "Running H2O Benchmark on ClickHouse..."
elif [ "$1" == "h2o_elastic" ]; then
    QUERIES_FILE="${PROJECT_DIR}/h2o_elastic_queries.sql"
    RESULTS_FILE="${OUTPUT_DIR}/benchmark_h2o_elastic_${TIMESTAMP}.csv"
    DATABASE_TYPE="elasticsearch"
    echo "Running H2O Benchmark on Elasticsearch..."
elif [ "$1" == "clickbench" ]; then
    QUERIES_FILE="${PROJECT_DIR}/${BENCHMARK_QUERIES_FILE}"
    RESULTS_FILE="${OUTPUT_DIR}/benchmark_clickbench_${TIMESTAMP}.csv"
    DATABASE_TYPE="clickhouse"
    echo "Running ClickBench Benchmark..."
else
    echo "Error: invalid benchmark name '$1'"
    exit 1
fi

echo "query_num,query_time_ms,rows_read,bytes_read" > "$RESULTS_FILE"

run_clickhouse_query() {
    local query="$1"
    local query_num="$2"

    echo "Running Q${query_num}..."

    RESULT=$(curl -s "http://${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/" \
        --data-binary "$query FORMAT JSON" 2>/dev/null || echo '{}')

    ELAPSED=$(echo "$RESULT" | grep -o '"elapsed": *[0-9.]*' | head -1 | sed 's/.*: *//')
    ROWS_READ=$(echo "$RESULT" | grep -o '"rows_read": *[0-9]*' | head -1 | sed 's/.*: *//')
    BYTES_READ=$(echo "$RESULT" | grep -o '"bytes_read": *[0-9]*' | head -1 | sed 's/.*: *//')

    if [ -n "$ELAPSED" ]; then
        TIME_MS=$(echo "$ELAPSED * 1000" | bc 2>/dev/null || echo "0")
    else
        TIME_MS="0"
    fi

    echo "${query_num},${TIME_MS:-0},${ROWS_READ:-0},${BYTES_READ:-0}" >> "$RESULTS_FILE"
    echo "  Q${query_num}: ${TIME_MS:-0}ms (${ROWS_READ:-0} rows, ${BYTES_READ:-0} bytes)"
}

run_elasticsearch_sql_query() {
    local sql_query="$1"
    local query_num="$2"

    echo "Running Q${query_num}..."

    START=$(date +%s%N)

    RESULT=$(jq -n --arg q "$sql_query" '{"query": $q}' | \
        curl -s -X POST "http://${ES_HOST}:${ES_PORT}/_sql?format=json" \
            -H "Authorization: ApiKey ${ES_API_KEY}" \
            -H "Content-Type: application/json" \
            -d @-)

    END=$(date +%s%N)

    TIME_MS=$(( (END - START) / 1000000 ))

    ROWS_READ=$(echo "$RESULT" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(len(d.get('rows', [])))
except:
    print(0)
")

    echo "${query_num},${TIME_MS},${ROWS_READ},0" >> "$RESULTS_FILE"
    echo "  Q${query_num}: ${TIME_MS}ms (${ROWS_READ} rows)"
}

echo "Reading queries from: $QUERIES_FILE"
echo "Results will be saved to: $RESULTS_FILE"
echo ""

# Read queries from file (skip comments and empty lines)
QUERY_NUM=0
while IFS= read -r line; do
    # Skip empty lines and comments
    [[ -z "$line" || "$line" =~ ^[[:space:]]*-- ]] && continue

    # Remove trailing semicolon and whitespace
    query="${line%;}"
    query="${query%"${query##*[![:space:]]}"}"

    QUERY_NUM=$((QUERY_NUM + 1))

    if [ "$DATABASE_TYPE" == "elasticsearch" ]; then
        run_elasticsearch_sql_query "$query" "$QUERY_NUM"
    else
        run_clickhouse_query "$query" "$QUERY_NUM"
    fi
done < "$QUERIES_FILE"

echo ""
echo "Benchmark complete!"
echo "Results saved to: $RESULTS_FILE"

echo ""
echo "=== Summary ==="
TOTAL_TIME=$(awk -F',' 'NR>1 {sum+=$2} END {print sum}' "$RESULTS_FILE")
echo "Total queries: $QUERY_NUM"
echo "Total query time: ${TOTAL_TIME}ms"
if [ "$QUERY_NUM" -gt 0 ]; then
    AVG_TIME=$((TOTAL_TIME / QUERY_NUM))
    echo "Average query time: ${AVG_TIME}ms"
fi
