#!/usr/bin/env python3
"""
Benchmark ASAP queries against ClickBench hits dataset.
Outputs CSV with query ID, latency, and results.
"""

import argparse
import csv
import re
import time
import urllib.parse
from pathlib import Path
from typing import List, Tuple, Optional
import subprocess
import requests


def extract_queries_from_sql(sql_file: Path) -> List[Tuple[str, str]]:
    """Extract query ID and SQL from asap_test_queries.sql"""
    queries = []
    with open(sql_file, "r") as f:
        content = f.read()

    # Pattern to match: -- S1: description\nSELECT ... ; or -- T5_W0: description\nSELECT ... ;
    pattern = r"-- ([A-Za-z0-9_]+):[^\n]*\n(SELECT[^;]+;)"
    matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)

    for query_id, sql in matches:
        # Clean up SQL
        sql = sql.strip()
        queries.append((query_id, sql))

    return queries


def load_clickbench_data(clickhouse_url: str):
    """Load ClickBench hits.json.gz data into ClickHouse"""
    print("Setting up ClickHouse tables...")

    # Create hits table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS hits (
        EventTime DateTime,
        EventDate Date,
        WatchID UInt64,
        JavaEnable UInt8,
        Title String,
        GoodEvent UInt16,
        CounterID UInt32,
        ClientIP Int32,
        RegionID UInt32,
        UserID String,
        CounterClass Int8,
        OS UInt8,
        UserAgent UInt8,
        URL String,
        Referer String,
        IsRefresh UInt8,
        RefererCategoryID UInt16,
        RefererRegionID UInt32,
        URLCategoryID UInt16,
        URLRegionID UInt32,
        ResolutionWidth UInt16,
        ResolutionHeight UInt16,
        ResolutionDepth UInt8,
        FlashMajor UInt8,
        FlashMinor UInt8,
        FlashMinor2 String,
        NetMajor UInt8,
        NetMinor UInt8,
        UserAgentMajor UInt16,
        UserAgentMinor String,
        CookieEnable UInt8,
        JavascriptEnable UInt8,
        IsMobile UInt8,
        MobilePhone UInt8,
        MobilePhoneModel String,
        Params String,
        IPNetworkID UInt32,
        TraficSourceID Int8,
        SearchEngineID UInt16,
        SearchPhrase String,
        AdvEngineID UInt8,
        IsArtifical UInt8,
        WindowClientWidth UInt16,
        WindowClientHeight UInt16,
        ClientTimeZone Int16,
        ClientEventTime DateTime,
        SilverlightVersion1 UInt8,
        SilverlightVersion2 UInt8,
        SilverlightVersion3 UInt32,
        SilverlightVersion4 UInt16,
        PageCharset String,
        CodeVersion UInt32,
        IsLink UInt8,
        IsDownload UInt8,
        IsNotBounce UInt8,
        FUniqID String,
        OriginalURL String,
        HID UInt32,
        IsOldCounter UInt8,
        IsEvent UInt8,
        IsParameter UInt8,
        DontCountHits UInt8,
        WithHash UInt8,
        HitColor String,
        LocalEventTime DateTime,
        Age UInt8,
        Sex UInt8,
        Income UInt8,
        Interests UInt16,
        Robotness UInt8,
        RemoteIP Int32,
        WindowName Int32,
        OpenerName Int32,
        HistoryLength Int16,
        BrowserLanguage String,
        BrowserCountry String,
        SocialNetwork String,
        SocialAction String,
        HTTPError UInt16,
        SendTiming UInt32,
        DNSTiming UInt32,
        ConnectTiming UInt32,
        ResponseStartTiming UInt32,
        ResponseEndTiming UInt32,
        FetchTiming UInt32,
        SocialSourceNetworkID UInt8,
        SocialSourcePage String,
        ParamPrice String,
        ParamOrderID String,
        ParamCurrency String,
        ParamCurrencyID UInt16,
        OpenstatServiceName String,
        OpenstatCampaignID String,
        OpenstatAdID String,
        OpenstatSourceID String,
        UTMSource String,
        UTMMedium String,
        UTMCampaign String,
        UTMContent String,
        UTMTerm String,
        FromTag String,
        HasGCLID UInt8,
        RefererHash Int64,
        URLHash Int64,
        CLID UInt32
    ) ENGINE = MergeTree()
    ORDER BY (RegionID, OS, UserAgent, TraficSourceID, EventTime);
    """

    try:
        response = requests.post(clickhouse_url, data=create_table_sql)
        response.raise_for_status()
        print("✓ Created hits table")
    except Exception as e:
        print(f"Error creating table: {e}")
        return False

    # Check if data already loaded
    count_sql = "SELECT count(*) FROM hits"
    response = requests.post(clickhouse_url, data=count_sql)
    count = int(response.text.strip())

    if count > 0:
        print(f"✓ Data already loaded ({count:,} rows)")
        return True

    # Load data from hits.json.gz
    data_file = Path(
        "/users/STWang/asap-internal/ExecutionUtilities/clickhouse-benchmark-pipeline/clickbench_importer/data/hits.json.gz"
    )
    if not data_file.exists():
        print(f"✗ Data file not found: {data_file}")
        return False

    print(f"Loading data from {data_file}...")
    print("This may take a few minutes...")

    # Use clickhouse-client to load data
    cmd = f"zcat {data_file} | clickhouse-client --query='INSERT INTO hits FORMAT JSONEachRow'"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"✗ Error loading data: {result.stderr}")
        return False

    # Verify
    response = requests.post(clickhouse_url, data=count_sql)
    count = int(response.text.strip())
    print(f"✓ Loaded {count:,} rows")

    return True


def run_query(
    query: str, endpoint_url: str, timeout: int = 30, debug: bool = False
) -> Tuple[float, Optional[str], Optional[str]]:
    """
    Run a query and return (latency_ms, result, error).
    debug=True prints per-request HTTP status. For detailed latency decomposition,
    run QueryEngineRust with --log-level DEBUG.
    """
    encoded_query = urllib.parse.quote(query)
    separator = "&" if "?" in endpoint_url else "?"
    url = f"{endpoint_url}{separator}query={encoded_query}"

    try:
        start_time = time.time()
        response = requests.get(url, timeout=timeout)
        latency_ms = (time.time() - start_time) * 1000

        if debug:
            status = response.status_code
            # Without --forward-unsupported-queries: 200 = ASAP served, else error
            source = "ASAP sketch" if status == 200 else f"error (HTTP {status})"
            print(f"  [{source}] {latency_ms:.2f}ms  status={status}")

        if response.status_code == 200:
            return latency_ms, response.text.strip(), None
        else:
            return latency_ms, None, f"HTTP {response.status_code}: {response.text}"

    except requests.Timeout:
        return timeout * 1000, None, "Timeout"
    except Exception as e:
        return 0, None, str(e)


def run_benchmark(
    sql_file: Path,
    endpoint_url: str,
    output_csv: Path,
    mode: str = "baseline",
    query_filter: Optional[List[str]] = None,
    debug: bool = False,
):
    """Run all queries and save results to CSV"""

    print(f"\nRunning benchmark in {mode} mode...")
    print(f"Endpoint: {endpoint_url}")
    print(f"Output: {output_csv}")
    if debug:
        print(
            "Debug mode: per-query HTTP status shown. For latency decomposition, run QueryEngineRust with --log-level DEBUG."
        )

    # Extract queries
    queries = extract_queries_from_sql(sql_file)
    if query_filter:
        queries = [(qid, sql) for qid, sql in queries if qid in query_filter]
    print(f"Found {len(queries)} queries")

    # Prepare CSV
    with open(output_csv, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "query_id",
                "query_pattern",
                "latency_ms",
                "result_rows",
                "result_full",
                "error",
                "mode",
            ]
        )

        latencies_ok: List[float] = []

        for query_id, sql in queries:
            print(f"Running {query_id}...", end=" " if not debug else "\n")

            # Determine query pattern
            if query_id.startswith("S"):
                pattern = "Spatial"
            elif query_id.startswith("T"):
                pattern = "Temporal"
            elif query_id.startswith("ST"):
                pattern = "SpatioTemporal"
            elif query_id.startswith("N"):
                pattern = "Nested"
            elif query_id.startswith("D"):
                pattern = "Dated"
            elif query_id.startswith("L"):
                pattern = "LongRange"
            else:
                pattern = "Unknown"

            # Run query
            latency_ms, result, error = run_query(sql, endpoint_url, debug=debug)

            if error:
                print(f"✗ {error}")
                writer.writerow([query_id, pattern, latency_ms, 0, "", error, mode])
            else:
                # Count result rows
                result_lines = result.strip().split("\n") if result else []
                num_rows = len(result_lines)

                # Full result (replace newlines with | for CSV compatibility)
                preview = result.replace("\n", " | ") if result else ""

                latencies_ok.append(latency_ms)
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

            # Small delay between queries
            time.sleep(0.1)

    print(f"\n✓ Results saved to {output_csv}")

    if latencies_ok:
        latencies_ok.sort()
        n = len(latencies_ok)
        avg = sum(latencies_ok) / n
        p50 = latencies_ok[int(n * 0.50)]
        p95 = latencies_ok[int(n * 0.95)]
        print(f"\nLatency summary ({n} successful queries):")
        print(
            f"  min={latencies_ok[0]:.2f}ms  avg={avg:.2f}ms  p50={p50:.2f}ms  p95={p95:.2f}ms  max={latencies_ok[-1]:.2f}ms"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark ASAP queries on ClickBench data"
    )
    parser.add_argument(
        "--mode",
        choices=["baseline", "asap"],
        default="asap",
        help="ASAP mode (default) or Baseline (ClickHouse only)",
    )
    parser.add_argument(
        "--load-data", action="store_true", help="Load ClickBench data into ClickHouse"
    )
    parser.add_argument(
        "--clickhouse-url",
        default="http://localhost:8123/?session_timezone=UTC",
        help="ClickHouse server URL",
    )
    parser.add_argument(
        "--asap-url",
        default="http://localhost:8088/clickhouse/query",
        help="ASAP QueryEngine URL",
    )
    parser.add_argument(
        "--output",
        default="asap_results.csv",
        help="Output CSV file (default: asap_results.csv)",
    )
    parser.add_argument(
        "--sql-file",
        default=None,
        help="SQL file to use (default: asap_test_queries.sql)",
    )
    parser.add_argument(
        "--filter",
        default=None,
        help="Comma-separated query IDs to run (e.g. T5,T6)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show per-query HTTP status and latency summary. For full latency decomposition, run QueryEngineRust with --log-level DEBUG.",
    )

    args = parser.parse_args()

    sql_file = (
        Path(args.sql_file)
        if args.sql_file
        else Path(__file__).parent / "asap_test_queries.sql"
    )

    # Load data if requested
    if args.load_data:
        if not load_clickbench_data(args.clickhouse_url):
            print("Failed to load data")
            return 1

    # Determine endpoint
    if args.mode == "baseline":
        endpoint = args.clickhouse_url
    else:
        endpoint = args.asap_url

    # Parse filter
    query_filter = [q.strip() for q in args.filter.split(",")] if args.filter else None

    # Run benchmark
    run_benchmark(
        sql_file, endpoint, Path(args.output), args.mode, query_filter, args.debug
    )

    return 0


if __name__ == "__main__":
    exit(main())
