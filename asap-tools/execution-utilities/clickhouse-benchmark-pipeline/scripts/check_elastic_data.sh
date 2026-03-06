#!/bin/bash
# Check Elasticsearch data ingestion status for H2O benchmark

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."

# Load config
set -a
source "${PROJECT_DIR}/config.env"
set +a

AUTH=(-H "Authorization: ApiKey ${ES_API_KEY}")
BASE="http://${ES_HOST}:${ES_PORT}"

echo "=== Elasticsearch Data Status ==="
echo ""

# Check connection
if ! curl -s "${AUTH[@]}" "${BASE}/" > /dev/null; then
    echo "Error: Cannot connect to Elasticsearch at ${ES_HOST}:${ES_PORT}"
    exit 1
fi

# Total document count
echo "Total documents in ${ES_INDEX_NAME} index:"
curl -s "${AUTH[@]}" "${BASE}/${ES_INDEX_NAME}/_count" | \
    python3 -c "import sys, json; print(json.load(sys.stdin)['count'])"
echo ""

# Index stats
echo "Index statistics:"
curl -s "${AUTH[@]}" "${BASE}/${ES_INDEX_NAME}/_stats" | \
    python3 -c "
import sys, json
stats = json.load(sys.stdin)
idx_stats = stats['indices']['${ES_INDEX_NAME}']['primaries']
print(f\"  Total size: {idx_stats['store']['size_in_bytes'] / (1024*1024*1024):.2f} GB\")
print(f\"  Document count: {idx_stats['docs']['count']:,}\")
print(f\"  Deleted docs: {idx_stats['docs']['deleted']:,}\")
"
echo ""

# Sample documents
echo "Sample documents (first 5 by timestamp):"
curl -s "${AUTH[@]}" -X POST "${BASE}/${ES_INDEX_NAME}/_search" \
    -H 'Content-Type: application/json' \
    -d '{
      "size": 5,
      "sort": [{"timestamp": "asc"}],
      "_source": ["timestamp", "id1", "id2", "id3", "v1", "v2", "v3"]
    }' | \
    python3 -c "
import sys, json
from datetime import datetime
results = json.load(sys.stdin)
for hit in results['hits']['hits']:
    doc = hit['_source']
    ts = datetime.fromtimestamp(doc['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    print(f\"  {ts} | id1={doc['id1']} id2={doc['id2']} id3={doc['id3']} | v1={doc['v1']} v2={doc['v2']} v3={doc['v3']}\")
"
echo ""

# Aggregation test
echo "Sample aggregation (count by id1):"
curl -s "${AUTH[@]}" -X POST "${BASE}/${ES_INDEX_NAME}/_search" \
    -H 'Content-Type: application/json' \
    -d '{
      "size": 0,
      "aggs": {
        "by_id1": {
          "terms": {"field": "id1", "size": 5}
        }
      }
    }' | \
    python3 -c "
import sys, json
results = json.load(sys.stdin)
for bucket in results['aggregations']['by_id1']['buckets']:
    print(f\"  {bucket['key']}: {bucket['doc_count']:,} documents\")
"
echo ""