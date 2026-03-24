#!/usr/bin/env python3
"""Benchmark queries against Elasticsearch using the SQL API.
Outputs CSV with query ID, latency, and results.

Equivalent to run_benchmark.py but targeting Elasticsearch SQL endpoint.
Timestamps in the h2o_benchmark index are epoch_millis starting from 2024-01-01T00:00:00Z,
incrementing by 1 second per row (as written by the Rust data exporter).
"""

import argparse
import csv
import re
import time
from pathlib import Path
from typing import List, Tuple, Optional

import requests


ASAP_PORT = 8088


def extract_queries_from_sql(sql_file: Path) -> List[Tuple[str, str]]:
    """Extract query ID and SQL from a .sql file.
    Matches comments like: -- T001: description
    followed by a SELECT statement ending in ;
    """
    queries = []
    with open(sql_file, "r") as f:
        content = f.read()

    pattern = r"-- ([A-Za-z0-9_]+):[^\n]*\n(SELECT[^;]+;)"
    matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)

    for query_id, sql in matches:
        queries.append((query_id, sql.strip()))

    return queries


def run_query(
    query: str,
    elastic_host: str,
    elastic_port: int,
    api_key: Optional[str],
    timeout: int = 30,
    fetch_size: int = 1000,
) -> Tuple[float, Optional[list], Optional[str]]:
    """
    Run a query against Elasticsearch SQL API.
    Returns (latency_ms, rows, error).

    Uses POST /_sql?format=json which is the standard ES SQL endpoint.
    """
    url = f"http://{elastic_host}:{elastic_port}/_sql?format=json"

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"ApiKey {api_key}"

    body = {
        "query": query.strip().rstrip(";"),
        "fetch_size": fetch_size,
    }

    try:
        start_time = time.time()
        response = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=timeout,
        )
        latency_ms = (time.time() - start_time) * 1000

        # if response.status_code == 200:
        #     data = response.json()
        #     print(f"DEBUG: {json.dumps(data)[:500]}")

        if response.status_code == 200:
            data = response.json()
            # Elasticsearch SQL format
            if "rows" in data:
                rows = data["rows"]
            # Elasticsearch hits format (ASAP sketch hit)
            elif "hits" in data and "hits" in data.get("hits", {}):
                rows = data["hits"]["hits"]
            # Prometheus/ASAP format
            elif "data" in data and "result" in data.get("data", {}):
                rows = data["data"]["result"]
            else:
                rows = []
            return latency_ms, rows, None
        else:
            error = f"HTTP {response.status_code}: {response.text[:200]}"
            return latency_ms, None, error

    except requests.Timeout:
        return timeout * 1000, None, "Timeout"
    except Exception as e:
        return 0, None, str(e)


def get_query_pattern(query_id: str) -> str:
    """Categorize query by ID prefix, mirroring the ClickHouse benchmark script."""
    if query_id.startswith("ST"):
        return "SpatioTemporal"
    elif query_id.startswith("S"):
        return "Spatial"
    elif query_id.startswith("T"):
        return "Temporal"
    elif query_id.startswith("N"):
        return "Nested"
    elif query_id.startswith("D"):
        return "Dated"
    elif query_id.startswith("L"):
        return "LongRange"
    elif query_id.startswith("Q"):
        return "Aggregate"
    else:
        return "Unknown"


def run_benchmark(
    sql_file: Path,
    elastic_host: str,
    elastic_port: int,
    api_key: Optional[str],
    output_csv: Path,
    mode: str = "baseline",
    query_filter: Optional[List[str]] = None,
    timeout: int = 30,
):
    """Run all queries and save results to CSV."""
    # Override port for asap mode
    if mode == "asap":
        elastic_port = ASAP_PORT

    print(f"\nRunning Elasticsearch benchmark in {mode} mode...")
    print(f"Endpoint: http://{elastic_host}:{elastic_port}/_sql")
    print(f"Output: {output_csv}")

    queries = extract_queries_from_sql(sql_file)

    if query_filter:
        queries = [(qid, sql) for qid, sql in queries if qid in query_filter]

    print(f"Found {len(queries)} queries\n")

    with open(output_csv, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "query_id",
                "query_pattern",
                "latency_ms",
                "result_rows",
                "result_preview",
                "error",
                "mode",
            ]
        )

        for query_id, sql in queries:
            print(f"Running {query_id}...", end=" ", flush=True)

            pattern = get_query_pattern(query_id)

            latency_ms, rows, error = run_query(
                sql,
                elastic_host,
                elastic_port,
                api_key,
                timeout=timeout,
            )

            if error:
                print(f"✗ {error}")
                writer.writerow(
                    [query_id, pattern, f"{latency_ms:.2f}", 0, "", error, mode]
                )
            else:
                num_rows = len(rows) if rows else 0
                # Preview: first row as a short string
                preview = str(rows[0])[:100] if rows else ""
                print(f"✓ {latency_ms:.2f}ms ({num_rows} rows)")
                writer.writerow(
                    [
                        query_id,
                        pattern,
                        f"{latency_ms:.2f}",
                        num_rows,
                        preview,
                        "",
                        mode,
                    ]
                )

            # Small delay between queries to avoid hammering the cluster
            time.sleep(0.1)

    print(f"\n✓ Results saved to {output_csv}")


def check_connection(
    elastic_host: str, elastic_port: int, api_key: Optional[str], mode: str = "baseline"
) -> bool:
    """Verify Elasticsearch is reachable and the SQL plugin is available."""
    if mode == "asap":
        elastic_port = ASAP_PORT

    url = f"http://{elastic_host}:{elastic_port}/_sql?format=json"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"ApiKey {api_key}"

    try:
        response = requests.post(
            url,
            headers=headers,
            json={"query": "SELECT 1"},
            timeout=5,
        )
        if response.status_code == 200:
            print(f"✓ Connected to Elasticsearch at {elastic_host}:{elastic_port}")
            return True
        else:
            print(
                f"✗ Elasticsearch SQL returned HTTP {response.status_code}: {response.text[:200]}"
            )
            return False
    except Exception as e:
        print(f"✗ Could not connect to Elasticsearch: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark queries against Elasticsearch SQL API"
    )
    parser.add_argument(
        "--elastic-host",
        default="localhost",
        help="Elasticsearch host (default: localhost)",
    )
    parser.add_argument(
        "--elastic-port",
        type=int,
        default=9200,
        help="Elasticsearch port (default: 9200, overridden to 8088 when --mode asap)",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Elasticsearch API key (optional)",
    )
    parser.add_argument(
        "--sql-file",
        required=True,
        help="Path to .sql file containing queries",
    )
    parser.add_argument(
        "--output",
        default="elastic_results.csv",
        help="Output CSV file (default: elastic_results.csv)",
    )
    parser.add_argument(
        "--mode",
        default="baseline",
        help="Label for the 'mode' column in output CSV (default: baseline). Use 'asap' to target port 8088.",
    )
    parser.add_argument(
        "--filter",
        default=None,
        help="Comma-separated query IDs to run (e.g. T000,T001,Q1)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Per-query timeout in seconds (default: 30)",
    )

    args = parser.parse_args()

    # Verify connection first
    # if not check_connection(args.elastic_host, args.elastic_port, args.api_key, args.mode):
    #     return 1

    sql_file = Path(args.sql_file)
    if not sql_file.exists():
        print(f"✗ SQL file not found: {sql_file}")
        return 1

    query_filter = [q.strip() for q in args.filter.split(",")] if args.filter else None

    run_benchmark(
        sql_file=sql_file,
        elastic_host=args.elastic_host,
        elastic_port=args.elastic_port,
        api_key=args.api_key,
        output_csv=Path(args.output),
        mode=args.mode,
        query_filter=query_filter,
        timeout=args.timeout,
    )

    return 0


if __name__ == "__main__":
    exit(main())
