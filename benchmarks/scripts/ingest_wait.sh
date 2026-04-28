#!/usr/bin/env bash
set -euo pipefail

# ingest_wait.sh — waits for the query engine to accumulate sketches before
# benchmarking. With the precompute engine, the query engine begins computing
# immediately on startup, so we just sleep and then verify data is present.

QE_URL="http://localhost:8088/api/v1/query"
ACCUMULATE_SLEEP=90     # seconds for sketches to accumulate

# ── 1. Allow sketches to accumulate ─────────────────────────────────────────
echo "[ingest_wait] Sleeping ${ACCUMULATE_SLEEP}s for sketches to accumulate ..."
sleep "${ACCUMULATE_SLEEP}"

# ── 2. Verify query engine has data ─────────────────────────────────────────
echo "[ingest_wait] Verifying query engine has data ..."
response=$(curl -sf --max-time 10 \
  "${QE_URL}?query=avg%28sensor_reading%29" 2>/dev/null || true)

if [ -z "${response}" ]; then
  echo "[ingest_wait] ERROR: Query engine returned empty response." >&2
  exit 1
fi

result_count=$(echo "${response}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
result = data.get('data', {}).get('result', [])
print(len(result))
" 2>/dev/null || echo "0")

if [ "${result_count}" -eq 0 ]; then
  echo "[ingest_wait] ERROR: Query engine has no data yet (result array is empty)." >&2
  exit 1
fi

echo "[ingest_wait] Query engine has data (${result_count} result entries). Ready for benchmarking."
