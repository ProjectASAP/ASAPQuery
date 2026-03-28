#!/usr/bin/env bash
set -euo pipefail

# wait_for_stack.sh — polls each service until healthy or times out.
# Used in CI after `docker compose up -d --build`.

MAX_WAIT=180   # seconds per service
SLEEP=5

wait_for_url() {
  local name="$1"
  local url="$2"
  local elapsed=0

  echo "[wait_for_stack] Waiting for ${name} at ${url} ..."
  while true; do
    if curl -sf --max-time 5 "${url}" > /dev/null 2>&1; then
      echo "[wait_for_stack] ${name} is healthy (${elapsed}s elapsed)"
      return 0
    fi
    if [ "${elapsed}" -ge "${MAX_WAIT}" ]; then
      echo "[wait_for_stack] ERROR: ${name} did not become healthy within ${MAX_WAIT}s" >&2
      return 1
    fi
    sleep "${SLEEP}"
    elapsed=$(( elapsed + SLEEP ))
  done
}

wait_for_url "Prometheus"   "http://localhost:9090/-/healthy"
wait_for_url "Arroyo API"   "http://localhost:5115/api/v1/pipelines"
wait_for_url "QueryEngine"  "http://localhost:8088/api/v1/query?query=vector(1)"

echo "[wait_for_stack] All services are healthy."
