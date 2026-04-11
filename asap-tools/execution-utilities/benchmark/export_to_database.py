#!/usr/bin/env python3
"""
Load a dataset into ClickHouse or Elasticsearch for baseline comparison.

Supports ClickBench (hits.json.gz), H2O groupby CSV, or a custom table.

Usage:
    # ClickBench to Clickhouse
    python export_to_database.py \\
        --dataset clickbench --database clickhouse \\
        --file-path ./data/hits.json.gz \\
        --init-sql-file ../clickhouse-benchmark-pipeline/clickhouse/clickbench_init.sql

    # H2O to Clickhouse
    python export_to_database.py \\
        --dataset h2o --database clickhouse \\
        --file-path ./data/G1_1e7_1e2_0_0.csv \\
        --init-sql-file ../asap_benchmark_pipeline/h2o_init.sql

    # H2O to Elasticsearch
    python export_to_database.py \\
        --dataset h2o --database elasticsearch \\
        --file-path ./data/G1_1e7_1e2_0_0.csv \\
        --es-host localhost \\
        --es-port 9200 \\
        --es-index h2o_benchmark \\
        --es-api-key your_api_key_here \\
        --es-bulk-size 5000

    # Custom JSON to ClickHouse
    python export_to_database.py \\
        --dataset custom --database clickhouse \\
        --file-path ./data/mydata.json \\
        --table-name mytable \\
        --ts-column event_time \\
        --ts-assignment passthrough
"""

import argparse
import os
import sys
from datetime import datetime, timezone

import requests

DEFAULT_CLICKHOUSE_URL = "http://localhost:8123/"
H2O_BATCH_SIZE = 50_000
H2O_ROWS_PER_SECOND = 1000
H2O_BASE_EPOCH = 1704067200  # 2024-01-01T00:00:00Z

# Valid (dataset, database) combinations tested so far
VALID_COMBINATIONS = {
    ("clickbench", "clickhouse"),
    ("h2o", "clickhouse"),
    ("h2o", "elasticsearch"),
    ("custom", "clickhouse"),
}


def _exec_clickhouse_sql(clickhouse_url: str, sql: str, label: str = ""):
    """Execute a SQL statement via the ClickHouse HTTP API."""
    r = requests.post(clickhouse_url, data=sql.encode())
    if not r.ok:
        print(f"  WARN [{label}]: {r.text.strip()[:200]}")
    else:
        short = sql.strip()[:80].replace("\n", " ")
        print(f"  OK: {short}")


def run_init_sql(clickhouse_url: str, init_sql_file: str):
    """Execute DDL statements from a SQL file."""
    print(f"Running init SQL from {init_sql_file}...")
    with open(init_sql_file) as f:
        content = f.read()
    stmts = [s.strip() for s in content.split(";") if s.strip()]
    for stmt in stmts:
        _exec_clickhouse_sql(clickhouse_url, stmt, label=stmt[:40])


def check_row_count(clickhouse_url: str, table_name: str) -> int:
    r = requests.post(clickhouse_url, data=f"SELECT count(*) FROM {table_name}")
    if r.ok:
        return int(r.text.strip())
    return 0


def load_clickbench(
    clickhouse_url: str,
    file_path: str,
    init_sql_file: str = None,
    skip_table_init: bool = False,
    skip_if_loaded: bool = False,
    max_rows: int = 0,
):
    """Load hits.json.gz into ClickHouse via HTTP INSERT."""
    if not skip_table_init and init_sql_file:
        run_init_sql(clickhouse_url, init_sql_file)

    if skip_if_loaded:
        count = check_row_count(clickhouse_url, "hits")
        if count > 0:
            print(f"Data already loaded ({count:,} rows). Skipping.")
            return True

    if not os.path.exists(file_path):
        print(f"ERROR: Data file not found: {file_path}")
        return False

    print(f"Loading ClickBench data from {file_path}...")

    def _row_stream():
        with gzip.open(file_path, "rt") as f:
            for i, line in enumerate(f):
                if max_rows > 0 and i >= max_rows:
                    break
                yield line.encode()

    url = clickhouse_url.rstrip("/") + "/?query=INSERT+INTO+hits+FORMAT+JSONEachRow"
    r = requests.post(url, data=_row_stream(), stream=True)
    if not r.ok:
        print(f"ERROR: ClickHouse insert failed: {r.text[:200]}")
        return False

    count = check_row_count(clickhouse_url, "hits")
    print(f"Loaded {count:,} rows into ClickHouse (hits)")
    return True


def _flush_h2o_batch(clickhouse_url: str, rows: list):
    """Flush a batch of H2O rows to ClickHouse via HTTP INSERT."""
    sql = "INSERT INTO h2o_groupby VALUES " + ",".join(rows)
    r = requests.post(clickhouse_url, data=sql.encode())
    if not r.ok:
        raise RuntimeError(f"ClickHouse insert failed: {r.text[:200]}")


def load_h2o_clickhouse(
    clickhouse_url: str,
    file_path: str,
    init_sql_file: str = None,
    skip_table_init: bool = False,
    skip_if_loaded: bool = False,
    max_rows: int = 0,
):
    """Load H2O groupby CSV into ClickHouse with synthetic timestamps.

    Timestamps are assigned at H2O_ROWS_PER_SECOND rows/sec starting from
    H2O_BASE_EPOCH (2024-01-01T00:00:00Z).
    Adapted from asap_benchmark_pipeline/run_benchmark.py:load_h2o_data_clickhouse().
    """
    if not skip_table_init and init_sql_file:
        run_init_sql(clickhouse_url, init_sql_file)

    if skip_if_loaded:
        count = check_row_count(clickhouse_url, "h2o_groupby")
        if count > 0:
            print(f"Data already loaded ({count:,} rows). Skipping.")
            return True

    if not os.path.exists(file_path):
        print(f"ERROR: Data file not found: {file_path}")
        return False

    print(f"Inserting H2O data from {file_path} into ClickHouse...")
    batch: list = []
    total = 0

    with open(file_path, "r", encoding="utf-8") as f:
        f.readline()  # skip header
        for i, line in enumerate(f):
            if max_rows > 0 and i >= max_rows:
                break
            parts = line.rstrip("\n").split(",")
            abs_sec = H2O_BASE_EPOCH + i // H2O_ROWS_PER_SECOND
            ts = datetime.fromtimestamp(abs_sec, tz=timezone.utc)
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

            batch.append(
                f"('{ts_str}','{parts[0]}','{parts[1]}','{parts[2]}',"
                f"{parts[3]},{parts[4]},{parts[5]},"
                f"{parts[6]},{parts[7]},{parts[8]})"
            )

            if len(batch) >= H2O_BATCH_SIZE:
                _flush_h2o_batch(clickhouse_url, batch)
                total += len(batch)
                batch = []
                if total % 500_000 == 0:
                    print(f"  Inserted {total:,} rows...")

    if batch:
        _flush_h2o_batch(clickhouse_url, batch)
        total += len(batch)

    print(f"Loaded {total:,} rows into ClickHouse (h2o_groupby)")
    return True

def load_h2o_elasticsearch(
    es_host: str,
    es_port: int,
    index_name: str,
    file_path: str,
    api_key: str = None,
    skip_if_loaded: bool = False,
    max_rows: int = 0,
):
    """Load H2O groupby CSV into Elasticsearch with synthetic timestamps."""
    try:
        from elasticsearch import Elasticsearch, helpers
    except ImportError:
        print("ERROR: elasticsearch-py not installed. Run: pip install elasticsearch")
        return False

    auth = {"api_key": api_key} if api_key else {}
    es = Elasticsearch(f"http://{es_host}:{es_port}", **auth)

    if not es.ping():
        print(f"ERROR: Cannot connect to Elasticsearch at {es_host}:{es_port}")
        return False

    if skip_if_loaded and es.indices.exists(index=index_name):
        count = es.count(index=index_name)["count"]
        if count > 0:
            print(f"Data already loaded ({count:,} rows). Skipping.")
            return True

    if es.indices.exists(index=index_name):
        print(f"Deleting existing index: {index_name}")
        es.indices.delete(index=index_name)

    print(f"Creating index: {index_name}")
    es.indices.create(index=index_name, body={
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "refresh_interval": "30s",
        },
        "mappings": {
            "properties": {
                "timestamp": {"type": "date", "format": "epoch_millis"},
                "id1": {"type": "keyword"},
                "id2": {"type": "keyword"},
                "id3": {"type": "keyword"},
                "id4": {"type": "long"},
                "id5": {"type": "long"},
                "id6": {"type": "long"},
                "v1": {"type": "long"},
                "v2": {"type": "long"},
                "v3": {"type": "double"},
            }
        },
    })

    if not os.path.exists(file_path):
        print(f"ERROR: Data file not found: {file_path}")
        return False

    print(f"Importing H2O data from {file_path} into Elasticsearch ({index_name})...")

    base_timestamp_ms = 1704067200000  # 2024-01-01T00:00:00Z in millis

    def generate_docs():
        with open(file_path, "r", encoding="utf-8") as f:
            f.readline()  # skip header
            for row_num, line in enumerate(f):
                if max_rows > 0 and row_num >= max_rows:
                    break
                parts = line.rstrip("\n").split(",")
                if len(parts) < 9:
                    continue
                yield {
                    "_index": index_name,
                    "_source": {
                        "timestamp": base_timestamp_ms + row_num * 10,
                        "id1": parts[0],
                        "id2": parts[1],
                        "id3": parts[2],
                        "id4": int(parts[3] or 0),
                        "id5": int(parts[4] or 0),
                        "id6": int(parts[5] or 0),
                        "v1":  int(parts[6] or 0),
                        "v2":  int(parts[7] or 0),
                        "v3":  float(parts[8] or 0.0),
                    },
                }

    total = 0
    errors = 0
    for ok, _ in helpers.streaming_bulk(
        es, generate_docs(), chunk_size=H2O_BATCH_SIZE, raise_on_error=False
    ):
        if ok:
            total += 1
        else:
            errors += 1
        if total % 500_000 == 0 and total > 0:
            print(f"  Indexed {total:,} documents...")

    print(f"Indexed {total:,} documents ({errors} errors)")
    print("Refreshing index...")
    es.indices.refresh(index=index_name)
    print(f"✓ Import complete! Index: {index_name}")
    return True

def load_custom(
    clickhouse_url: str,
    file_path: str,
    table_name: str,
    ts_column: str,
    ts_assignment: str = "passthrough",
    init_sql_file: str = None,
    skip_table_init: bool = False,
    skip_if_loaded: bool = False,
    max_rows: int = 0,
):
    """Load a custom JSON or CSV file into ClickHouse.

    For JSON files: uses INSERT FORMAT JSONEachRow via clickhouse-client.
    ts_assignment='synthetic' is only supported for CSV (same logic as H2O).
    """
    if not skip_table_init and init_sql_file:
        run_init_sql(clickhouse_url, init_sql_file)

    if skip_if_loaded:
        count = check_row_count(clickhouse_url, table_name)
        if count > 0:
            print(f"Data already loaded ({count:,} rows). Skipping.")
            return True

    if not os.path.exists(file_path):
        print(f"ERROR: Data file not found: {file_path}")
        return False

    path_lower = file_path.lower()
    url = (
        clickhouse_url.rstrip("/")
        + f"/?query=INSERT+INTO+{table_name}+FORMAT+JSONEachRow"
    )

    def _stream_gzip():
        with gzip.open(file_path, "rt") as f:
            for i, line in enumerate(f):
                if max_rows > 0 and i >= max_rows:
                    break
                yield line.encode()

    def _stream_plain():
        with open(file_path, "r") as f:
            for i, line in enumerate(f):
                if max_rows > 0 and i >= max_rows:
                    break
                yield line.encode()

    if path_lower.endswith(".json.gz") or path_lower.endswith(".jsonl.gz"):
        print(f"Loading {file_path} into ClickHouse ({table_name})...")
        r = requests.post(url, data=_stream_gzip(), stream=True)
        if not r.ok:
            print(f"ERROR: ClickHouse insert failed: {r.text[:200]}")
            return False
    elif path_lower.endswith(".json") or path_lower.endswith(".jsonl"):
        print(f"Loading {file_path} into ClickHouse ({table_name})...")
        r = requests.post(url, data=_stream_plain(), stream=True)
        if not r.ok:
            print(f"ERROR: ClickHouse insert failed: {r.text[:200]}")
            return False
    else:
        print(
            f"ERROR: Unsupported file format for {file_path}. Use --dataset h2o for CSV."
        )
        return False

    count = check_row_count(clickhouse_url, table_name)
    print(f"Loaded {count:,} rows into ClickHouse ({table_name})")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Load a dataset into ClickHouse or Elasticsearch for baseline comparison",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dataset",
        choices=["clickbench", "h2o", "custom"],
        required=True,
        help="Dataset type",
    )
    parser.add_argument(
        "--database",
        choices=["clickhouse", "elasticsearch"],
        required=True,
        help="Target database",
    )
    parser.add_argument(
        "--file-path",
        required=True,
        help="Path to the source data file",
    )
    parser.add_argument(
        "--clickhouse-url",
        default=DEFAULT_CLICKHOUSE_URL,
        help=f"ClickHouse HTTP URL (default: {DEFAULT_CLICKHOUSE_URL})",
    )
    parser.add_argument(
        "--init-sql-file",
        default=None,
        help="DDL SQL file to run before loading (CREATE TABLE ...)",
    )
    parser.add_argument(
        "--table-name",
        default=None,
        help="Target table name (required for --dataset custom)",
    )
    parser.add_argument(
        "--ts-column",
        default=None,
        help="Timestamp column name (for --dataset custom)",
    )
    parser.add_argument(
        "--ts-assignment",
        choices=["synthetic", "passthrough"],
        default="passthrough",
        help="How to assign timestamps for custom CSV data (default: passthrough)",
    )
    parser.add_argument(
        "--skip-table-init",
        action="store_true",
        help="Skip CREATE TABLE (assume tables already exist)",
    )
    parser.add_argument(
        "--skip-if-loaded",
        action="store_true",
        help="Skip insert if the table already has rows",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Maximum rows to load (0 = all)",
    )

    # Elasticsearch-specific flags
    es_group = parser.add_argument_group("Elasticsearch options (--database elasticsearch)")
    es_group.add_argument("--es-host", default="localhost", help="Elasticsearch host")
    es_group.add_argument("--es-port", type=int, default=9200, help="Elasticsearch port")
    es_group.add_argument("--es-index", default="h2o_benchmark", help="Elasticsearch index name")
    es_group.add_argument("--es-api-key", default=None, help="Elasticsearch API key")
    es_group.add_argument("--es-bulk-size", type=int, default=5000, help="Bulk insert batch size")

    args = parser.parse_args()

    # Validate (dataset, database) combination
    combo = (args.dataset, args.database)
    if combo not in VALID_COMBINATIONS:
        valid = ", ".join(f"({d}/{db})" for d, db in sorted(VALID_COMBINATIONS))
        parser.error(
            f"--dataset {args.dataset} is not supported with --database {args.database}. "
            f"Valid combinations: {valid}"
        )

    if args.dataset == "custom" and not args.table_name:
        parser.error("--table-name is required when --dataset custom")

    success = False
    if args.dataset == "clickbench":
        success = load_clickbench(
            args.clickhouse_url,
            args.file_path,
            init_sql_file=args.init_sql_file,
            skip_table_init=args.skip_table_init,
            skip_if_loaded=args.skip_if_loaded,
            max_rows=args.max_rows,
        )
    elif args.dataset == "h2o":
        if args.database == "elasticsearch":
            success = load_h2o_elasticsearch(
                es_host=args.es_host,
                es_port=args.es_port,
                index_name=args.es_index,
                file_path=args.file_path,
                api_key=args.es_api_key,
                skip_if_loaded=args.skip_if_loaded,
                max_rows=args.max_rows,
            )
        else:
            success = load_h2o_clickhouse(
                args.clickhouse_url,
                args.file_path,
                init_sql_file=args.init_sql_file,
                skip_table_init=args.skip_table_init,
                skip_if_loaded=args.skip_if_loaded,
                max_rows=args.max_rows,
            )
    elif args.dataset == "custom":
        success = load_custom(
            args.clickhouse_url,
            args.file_path,
            table_name=args.table_name,
            ts_column=args.ts_column,
            ts_assignment=args.ts_assignment,
            init_sql_file=args.init_sql_file,
            skip_table_init=args.skip_table_init,
            skip_if_loaded=args.skip_if_loaded,
            max_rows=args.max_rows,
        )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
