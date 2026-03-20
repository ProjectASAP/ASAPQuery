from typing import Tuple, List, Optional
from pathlib import Path
from datetime import datetime, timedelta
import re
import argparse
import os
import gdown
import requests
import urllib.parse
import time
import csv
import matplotlib.pyplot as plt

def extract_queries_from_sql(sql_file: Path) -> List[Tuple[str, str]]:
    """Extract query ID and SQL from asap_h2o_queries.sql"""
    queries = []
    with open(sql_file, "r") as f:
        content = f.read()

    pattern = r"-- ([A-Za-z0-9_]+):[^\n]*\n(SELECT[^;]+;)"
    matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)

    for query_id, sql in matches:
        sql = sql.strip()
        queries.append((query_id, sql))

    return queries

def data_loaded(clickhouse_url: str):
    try:
        response = requests.post(clickhouse_url, data="SELECT count(*) FROM h2o_groupby")
        if response.status_code != 200:
            return False
        count = int(response.text.strip())
        if count > 0:
            print(f"✓ Data already loaded ({count:,} rows)")
        return count > 0
    except:
        return False

def stream_csv_with_timestamps(filename: str):
    """
    Generator that reads the CSV and prepends a timestamp column.
    Starts at 1971-01-01 00:00:00 and increments by 10s every 100 rows.
    """
    start_time = datetime(1971, 1, 1, 0, 0, 0)
    
    with open(filename, 'r', encoding='utf-8') as f:
        header = f.readline().strip()
        yield f"timestamp,{header}\n".encode('utf-8')
        
        chunk = bytearray()
        row_count = 0
        ts_bytes = b""
        
        for line in f:
            if row_count % 100 == 0:
                delta_seconds = (row_count // 100) * 10
                current_time = start_time + timedelta(seconds=delta_seconds)
                ts_str = current_time.strftime('%Y-%m-%dT%H:%M:%SZ') + ','
                ts_bytes = ts_str.encode('utf-8')
            
            chunk.extend(ts_bytes)
            chunk.extend(line.encode('utf-8'))
            row_count += 1
            
            if len(chunk) > 65536:
                yield bytes(chunk)
                chunk = bytearray()
        
        if chunk:
            yield bytes(chunk)

def load_h2o_data(clickhouse_url: str, mode: str):
    # 1. SETUP TABLES
    try:
        with open("h2o_init.sql", 'r') as f:
            file_content = f.read()
    except FileNotFoundError:
        print("✗ Error: h2o_init.sql not found.")
        return False

    statements = [s.strip() for s in file_content.split(';') if s.strip()]
    print(f"Executing {len(statements)} setup statements...")

    try:
        for sql in statements:
            response = requests.post(clickhouse_url, data=sql)
            response.raise_for_status()
    except Exception as e:
        print(f"Error executing statement: {e}")
        return False
        
    print("✓ Created h2o_groupby tables and views")
    
    if data_loaded(clickhouse_url):
        return True

    # 2. DOWNLOAD DATA
    FILE_ID = "15SVQjQ2QehzYDLoDonio4aP7xqdMiNyi"
    FILENAME = "G1_1e7_1e2_0_0.csv" 
    
    if os.path.exists(FILENAME) and os.path.getsize(FILENAME) > 100 * 1024 * 1024:
        print(f"File {FILENAME} already exists. Skipping download.")
    else:
        print(f"Downloading H2O dataset (ID: {FILE_ID}) using gdown...")
        url = f"https://drive.google.com/uc?id={FILE_ID}"
        gdown.download(url, FILENAME, quiet=False)

    # 3. INSERT DATA VIA HTTP
    if mode == "asap":
        print("Publishing data to Kafka via ClickHouse HTTP (ASAP mode)...")
        insert_query = "INSERT INTO h2o_groupby_queue FORMAT CSVWithNames"
    else:
        print("Inserting data directly into ClickHouse MergeTree (Baseline mode)...")
        insert_query = "INSERT INTO h2o_groupby FORMAT CSVWithNames"
    
    url = f"{clickhouse_url.rstrip('/')}/"
    params = {"query": insert_query}
    
    try:
        response = requests.post(url, params=params, data=stream_csv_with_timestamps(FILENAME))
        if response.status_code != 200:
            print(f"✗ Error loading data: {response.text}")
            return False
    except Exception as e:
        print(f"✗ Exception during data load: {e}")
        return False

    if mode == "asap":
        print("Waiting for materialized view to consume all rows from Kafka...")
        prev_count = -1
        stable_rounds = 0
        while stable_rounds < 3:
            time.sleep(5)
            response = requests.post(clickhouse_url, data="SELECT count(*) FROM h2o_groupby")
            count = int(response.text.strip())
            print(f"  h2o_groupby row count: {count:,}")
            if count == prev_count:
                stable_rounds += 1
            else:
                stable_rounds = 0
            prev_count = count
    else:
        response = requests.post(clickhouse_url, data="SELECT count(*) FROM h2o_groupby")
        count = int(response.text.strip())

    print(f"✓ Loaded {count:,} rows")

    return True

def run_query(query: str, endpoint_url: str, session: requests.Session, timeout: int = 30) -> Tuple[float, Optional[str], Optional[str]]:
    encoded_query = urllib.parse.quote(query)
    
    if "?" in endpoint_url:
        url = f"{endpoint_url}&query={encoded_query}"
    else:
        url = f"{endpoint_url}?query={encoded_query}"

    try:
        start_time = time.time()
        response = session.get(url, timeout=timeout)
        latency_ms = (time.time() - start_time) * 1000

        if response.status_code == 200:
            return latency_ms, response.text.strip(), None
        else:
            return latency_ms, None, f"HTTP {response.status_code}: {response.text}"

    except requests.Timeout:
        return timeout * 1000, None, "Timeout"
    except Exception as e:
        return 0, None, str(e)

def run_benchmark(sql_file: Path, endpoint_url: str, output_csv: Path, mode: str, load_data: bool, query_filter: Optional[List[str]] = None):
    print(f"\nRunning benchmark in {mode} mode...")
    print(f"Endpoint: {endpoint_url}")
    print(f"Output: {output_csv}")

    queries = extract_queries_from_sql(sql_file)
    if query_filter:
        queries = [(qid, sql) for qid, sql in queries if qid in query_filter]
    print(f"Found {len(queries)} queries")

    session = requests.Session()

    # Lists to store plotting data
    plot_query_ids = []
    plot_latencies = []

    with open(output_csv, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["query_id", "latency_ms", "result_rows", "result_preview", "error", "mode"])

        for query_id, sql in queries:
            print(f"Running {query_id}...", end=" ", flush=True)
            
            latency_ms, result, error = run_query(sql, endpoint_url, session)

            if error:
                print(f"✗ {error}")
                writer.writerow([query_id, latency_ms, 0, "", error, mode])
                # Append 0 for failed queries on the plot to show they failed
                plot_query_ids.append(query_id)
                plot_latencies.append(0.0)
            else:
                result_lines = result.strip().split("\n") if result else []
                num_rows = len(result_lines)
                preview = result[:100].replace("\n", " | ") if result else ""
                print(f"✓ {latency_ms:.2f}ms ({num_rows} rows)")
                writer.writerow([query_id, f"{latency_ms:.2f}", num_rows, preview, "", mode])
                
                plot_query_ids.append(query_id)
                plot_latencies.append(latency_ms)
            
            time.sleep(0.1)

    print(f"\n✓ Results saved to {output_csv}")

    # --- Plotting Code ---
    if plot_latencies:
        plt.figure(figsize=(10, 6))
        
        # Give ASAP and Baseline distinct colors
        bar_color = '#1f77b4' if mode == 'baseline' else '#ff7f0e'
        
        # Create a numerical X-axis (1, 2, 3...)
        execution_order = list(range(1, len(plot_latencies) + 1))
        
        plt.bar(execution_order, plot_latencies, color=bar_color, edgecolor='black')
        
        plt.xlabel("Query Execution Order", fontsize=12, fontweight='bold')
        plt.ylabel("Latency (ms)", fontsize=12, fontweight='bold')
        
        # Set tick marks at every 10 on the X axis
        max_order = len(execution_order)
        plt.xticks(range(0, max_order + 1, 10))

        # Build dynamic title based on parameters
        load_text = "With Data Loading" if load_data else "Without Data Loading"
        plt.title(f"Query Latency - {mode.upper()} Mode ({load_text})", fontsize=14, fontweight='bold')
        
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()

        # Save plot to the same directory as the output CSV, replacing the extension with .png
        plot_output = output_csv.with_suffix(".png")
        plt.savefig(plot_output)
        print(f"✓ Graph successfully saved to {plot_output}")


def main():
    parser = argparse.ArgumentParser(description="Benchmark ASAP queries on H2o data")
    parser.add_argument("--mode", choices=["baseline", "asap"], default="asap", help="ASAP mode (default) or Baseline (ClickHouse only)")
    parser.add_argument("--load-data", action="store_true", help="Load H2o data into ClickHouse")
    parser.add_argument("--clickhouse-url", default="http://localhost:8123", help="ClickHouse server URL")
    parser.add_argument("--asap-url", default="http://localhost:8088/clickhouse/query", help="ASAP QueryEngine URL")
    parser.add_argument("--output", default="asap_results.csv", help="Output CSV file")
    parser.add_argument("--sql-file", default=None, help="SQL file to use (default: asap_h2o_queries.sql)")
    parser.add_argument("--filter", default=None, help="Comma-separated query IDs to run (e.g. T5,T6)")

    args = parser.parse_args()

    output_path = Path(args.output)
    if output_path.exists() and output_path.is_dir():
        print(f"Error: Output {output_path} is a directory. Please specify a file path (e.g., results.csv)")
        return 1

    if args.sql_file:
        sql_file = Path(args.sql_file)
    elif args.mode == "asap":
        sql_file = Path(__file__).parent / "asap_mode_queries.sql"
    else:
        sql_file = Path(__file__).parent / "asap_h2o_queries.sql"

    if args.load_data:
        if not load_h2o_data(args.clickhouse_url, args.mode):
            print("Failed to load data")
            return 1
    
    endpoint = args.clickhouse_url if args.mode == "baseline" else args.asap_url
    query_filter = [q.strip() for q in args.filter.split(",")] if args.filter else None

    # Notice we pass args.load_data so the plotting logic knows whether data was loaded
    run_benchmark(sql_file, endpoint, output_path, args.mode, args.load_data, query_filter)
    return 0

if __name__ == "__main__":
    exit(main())