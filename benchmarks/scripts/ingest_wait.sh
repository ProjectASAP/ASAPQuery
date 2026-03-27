#!/usr/bin/env bash
set -euo pipefail

# ingest_wait.sh — waits for the asap-demo Arroyo pipeline to reach RUNNING
# state, then sleeps to allow sketches to accumulate before verifying that the
# query engine has ingested data.

ARROYO_URL="http://localhost:5115/api/v1/pipelines"
QE_URL="http://localhost:8088/api/v1/query"
PIPELINE_NAME="asap-demo"
MAX_PIPELINE_WAIT=600   # seconds — Arroyo must compile Rust UDFs; allow extra time
ACCUMULATE_SLEEP=90     # seconds after pipeline is running
SLEEP=5

# ── 1. Wait for asap-demo pipeline to reach RUNNING ─────────────────────────
echo "[ingest_wait] Waiting for Arroyo pipeline '${PIPELINE_NAME}' to reach RUNNING state ..."
elapsed=0
while true; do
  state=$(curl -sf --max-time 10 "${ARROYO_URL}" 2>/dev/null \
    | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    # 'data' key may be null when no pipelines exist; use 'or []' to handle that
    pipelines = data if isinstance(data, list) else (data.get('data') or [])
    for p in pipelines:
        name = str(p.get('name') or p.get('id') or '')
        # Normalise hyphens/underscores so 'asap-demo' matches 'asap_demo'
        if '${PIPELINE_NAME}'.replace('-', '_') in name.replace('-', '_'):
            state = p.get('state')
            stop  = p.get('stop', '')
            # Arroyo signals a running pipeline via state=null and stop='none'
            if state is None and stop == 'none':
                print('Running')
            elif state is not None:
                print(str(state))
            else:
                print('stopped')
            break
    else:
        # No matching pipeline found yet — print nothing so caller retries
        pass
except Exception:
    pass
" 2>/dev/null || true)

  if [ "${state}" = "Running" ] || [ "${state}" = "RUNNING" ] || [ "${state}" = "running" ]; then
    echo "[ingest_wait] Pipeline '${PIPELINE_NAME}' is RUNNING (${elapsed}s elapsed)"
    break
  fi

  if [ "${elapsed}" -ge "${MAX_PIPELINE_WAIT}" ]; then
    echo "[ingest_wait] ERROR: Pipeline '${PIPELINE_NAME}' did not reach RUNNING within ${MAX_PIPELINE_WAIT}s (last state: '${state:-unknown}')" >&2
    # Dump pipeline list for diagnosis
    echo "[ingest_wait] Current Arroyo pipeline list:" >&2
    curl -sf --max-time 10 "${ARROYO_URL}" 2>/dev/null | python3 -m json.tool 2>/dev/null >&2 || true
    exit 1
  fi

  echo "[ingest_wait] Pipeline state: '${state:-not found}' — retrying in ${SLEEP}s (${elapsed}s elapsed) ..."
  sleep "${SLEEP}"
  elapsed=$(( elapsed + SLEEP ))
done

# ── 2. Allow sketches to accumulate ─────────────────────────────────────────
echo "[ingest_wait] Pipeline running. Sleeping ${ACCUMULATE_SLEEP}s for sketches to accumulate ..."
sleep "${ACCUMULATE_SLEEP}"

# ── 3. Verify query engine has data ─────────────────────────────────────────
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
