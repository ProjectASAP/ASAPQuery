#!/usr/bin/env python3
"""
Benchmark ASAP vs ClickHouse baseline on the H2O groupby dataset.
Outputs CSV with query ID, latency, and results.
"""

import argparse
import csv
import json
import os
import re
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import gdown
import matplotlib.pyplot as plt
import requests
from kafka import KafkaProducer


# ---------------------------------------------------------------------------
# Query extraction
# ---------------------------------------------------------------------------


def extract_queries_from_sql(sql_file: Path) -> List[Tuple[str, str]]:
    """Extract (query_id, sql) pairs from an annotated SQL file."""
    with open(sql_file) as f:
        content = f.read()
    pattern = r"-- ([A-Za-z0-9_]+):[^\n]*\n(SELECT[^;]+;)"
    return [
        (qid, sql.strip())
        for qid, sql in re.findall(pattern, content, re.DOTALL | re.IGNORECASE)
    ]


# ---------------------------------------------------------------------------
# Data loading — ClickHouse direct
# ---------------------------------------------------------------------------


def load_h2o_data_clickhouse(
    clickhouse_url: str, skip_table_init: bool = False, max_rows: int = 0
):
    """Load H2O CSV into ClickHouse MergeTree (baseline path)."""

    if not skip_table_init:
        print("Initializing ClickHouse tables...")
        with open(Path(__file__).parent / "h2o_init.sql") as f:
            stmts = [s.strip() for s in f.read().split(";") if s.strip()]
        for sql in stmts:
            r = requests.post(clickhouse_url, data=sql)
            if not r.ok:
                print(f"  WARN: {r.text.strip()[:120]}")
            else:
                print(f"  OK: {sql[:60]}")

    # Check if already loaded
    r = requests.post(clickhouse_url, data="SELECT count(*) FROM h2o_groupby")
    count = int(r.text.strip())
    if count > 0:
        print(f"Data already loaded ({count:,} rows)")
        return True

    csv_path = _download_h2o_csv()

    print("Inserting data into ClickHouse...")
    data_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)

    batch_size = 50_000
    batch = []
    total = 0

    with open(csv_path, "r", encoding="utf-8") as f:
        f.readline()  # skip header
        for i, line in enumerate(f):
            if max_rows > 0 and i >= max_rows:
                break
            parts = line.rstrip("\n").split(",")
            abs_sec = 1704067200 + i // 1000
            ms = i % 1000
            ts = datetime.fromtimestamp(abs_sec, tz=timezone.utc)
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

            batch.append(
                f"('{ts_str}','{parts[0]}','{parts[1]}','{parts[2]}',"
                f"{parts[3]},{parts[4]},{parts[5]},"
                f"{parts[6]},{parts[7]},{parts[8]})"
            )

            if len(batch) >= batch_size:
                _flush_batch(clickhouse_url, batch)
                total += len(batch)
                batch = []
                if total % 500_000 == 0:
                    print(f"  Inserted {total:,} rows...")

    if batch:
        _flush_batch(clickhouse_url, batch)
        total += len(batch)

    print(f"Loaded {total:,} rows into ClickHouse")
    return True


def _flush_batch(clickhouse_url: str, rows: list):
    sql = "INSERT INTO h2o_groupby VALUES " + ",".join(rows)
    r = requests.post(clickhouse_url, data=sql)
    if not r.ok:
        raise RuntimeError(f"ClickHouse insert failed: {r.text[:200]}")


# ---------------------------------------------------------------------------
# Data loading — Kafka (for Arroyo sketch pipeline)
# ---------------------------------------------------------------------------


def produce_h2o_to_kafka(topic: str = "h2o_groupby", max_rows: int = 0):
    """Stream H2O CSV rows into Kafka with fixed 2024-01-01 message timestamps."""
    csv_path = _download_h2o_csv()

    data_ts = 1704067200  # 2024-01-01T00:00:00Z
    ms_suffixes = [f".{ms:03d}Z" for ms in range(1000)]
    sec_prefix_cache: dict = {}

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        linger_ms=10,
        batch_size=1048576,
    )

    with open(csv_path, "r", encoding="utf-8") as f:
        f.readline()  # skip header
        for i, line in enumerate(f):
            if max_rows > 0 and i >= max_rows:
                break
            parts = line.rstrip("\n").split(",")
            abs_sec = data_ts + i // 1000
            ms = i % 1000
            if abs_sec not in sec_prefix_cache:
                sec_prefix_cache[abs_sec] = datetime.fromtimestamp(
                    abs_sec, tz=timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%S")
            ts_str = sec_prefix_cache[abs_sec] + ms_suffixes[ms]

            msg = (
                f'{{"timestamp":"{ts_str}",'
                f'"id1":"{parts[0]}","id2":"{parts[1]}","id3":"{parts[2]}",'
                f'"id4":{parts[3]},"id5":{parts[4]},"id6":{parts[5]},'
                f'"v1":{parts[6]},"v2":{parts[7]},"v3":{parts[8]}}}'
            ).encode("utf-8")

            producer.send(topic, value=msg, timestamp_ms=data_ts * 1000 + i)

            if i % 500_000 == 499_999:
                print(f"  Produced {i + 1:,} rows...")

    producer.flush()
    producer.close()
    print(f"Produced {i + 1:,} rows to Kafka topic '{topic}'")


def _download_h2o_csv() -> str:
    FILE_ID = "15SVQjQ2QehzYDLoDonio4aP7xqdMiNyi"
    FILENAME = str(Path(__file__).parent / "G1_1e7_1e2_0_0.csv")
    if os.path.exists(FILENAME) and os.path.getsize(FILENAME) > 100 * 1024 * 1024:
        print(f"File {FILENAME} already exists, skipping download.")
        return FILENAME
    print(f"Downloading H2O dataset via gdown...")
    url = f"https://drive.google.com/uc?id={FILE_ID}"
    gdown.download(url, FILENAME, quiet=False)
    return FILENAME


# ---------------------------------------------------------------------------
# Pipeline latency measurement
# ---------------------------------------------------------------------------


def measure_pipeline_latency(
    kafka_topic: str = "h2o_groupby",
    asap_url: str = "http://localhost:8088/clickhouse/query",
    num_trials: int = 3,
) -> float:
    """Measure end-to-end pipeline latency: data into Kafka → query result from QE.

    Sends a fresh 10-second window of data, then polls the QE until the query
    for that window returns results.  Returns the median latency across trials.
    """
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        linger_ms=0,
        batch_size=16384,
    )
    session = requests.Session()

    # Use a timestamp far enough in the future to avoid collision with test data
    base_epoch = 1704200000  # 2024-01-02T12:53:20Z

    latencies = []
    for trial in range(num_trials):
        window_start = base_epoch + trial * 100  # space trials apart
        window_end = window_start + 10
        query_ts = datetime.fromtimestamp(window_end, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # Build 10,000 rows (100 id1 × 100 id2) for this 10-second window
        rows = []
        for sec_offset in range(10):
            ts = window_start + sec_offset
            ts_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            for i1 in range(1, 101):
                id1 = f"id{i1:03d}"
                id2 = f"id{(i1 * 7 + sec_offset) % 100 + 1:03d}"
                msg = (
                    f'{{"timestamp":"{ts_str}",'
                    f'"id1":"{id1}","id2":"{id2}","id3":"x",'
                    f'"id4":1,"id5":1,"id6":1,'
                    f'"v1":{i1 % 5 + 1},"v2":1,"v3":1.0}}'
                ).encode()
                rows.append((msg, ts * 1000))

        # Send all rows and record send-complete time
        for msg, ts_ms in rows:
            producer.send(kafka_topic, value=msg, timestamp_ms=ts_ms)
        producer.flush()
        send_done = time.time()
        print(f"  Trial {trial+1}: sent {len(rows)} rows for window ending {query_ts}")

        # Poll QE until query for this window succeeds
        query = (
            f"SELECT QUANTILE(0.95, v1) FROM h2o_groupby "
            f"WHERE timestamp BETWEEN DATEADD(s, -10, '{query_ts}') AND '{query_ts}' "
            f"GROUP BY id1, id2"
        )
        encoded = urllib.parse.quote(query)
        url = f"{asap_url}?query={encoded}"

        timeout = 120
        while time.time() - send_done < timeout:
            try:
                r = session.get(url, timeout=5)
                if r.status_code == 200 and r.text.strip():
                    latency_s = time.time() - send_done
                    latencies.append(latency_s)
                    print(f"    → result available after {latency_s:.2f}s")
                    break
            except Exception:
                pass
            time.sleep(0.5)
        else:
            print(f"    → TIMEOUT after {timeout}s")

    producer.close()

    if not latencies:
        print("WARNING: Could not measure pipeline latency")
        return 0.0

    latencies.sort()
    median = latencies[len(latencies) // 2]
    print(
        f"\nPipeline latency (data→query): median={median:.2f}s across {len(latencies)} trials"
    )
    return median * 1000  # return ms


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------


def run_query(
    query: str, endpoint_url: str, session: requests.Session, timeout: int = 30
) -> Tuple[float, Optional[str], Optional[str]]:
    encoded_query = urllib.parse.quote(query)
    separator = "&" if "?" in endpoint_url else "?"
    url = f"{endpoint_url}{separator}query={encoded_query}"

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


def run_benchmark(
    sql_file: Path,
    endpoint_url: str,
    output_csv: Path,
    mode: str = "baseline",
    query_filter: Optional[List[str]] = None,
    pipeline_overhead_ms: float = 0.0,
):
    print(f"\nRunning benchmark in {mode} mode...")
    print(f"Endpoint: {endpoint_url}")
    print(f"Output: {output_csv}")
    if pipeline_overhead_ms > 0:
        print(f"Pipeline overhead per query: {pipeline_overhead_ms:.2f}ms")

    queries = extract_queries_from_sql(sql_file)
    if query_filter:
        queries = [(qid, sql) for qid, sql in queries if qid in query_filter]
    print(f"Found {len(queries)} queries")

    session = requests.Session()
    serving_latencies: List[float] = []
    total_latencies: List[float] = []
    plot_latencies: List[float] = []

    with open(output_csv, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "query_id",
                "latency_ms",
                "serving_ms",
                "pipeline_ms",
                "result_rows",
                "result_preview",
                "error",
                "mode",
            ]
        )

        for query_id, sql in queries:
            print(f"Running {query_id}...", end=" ", flush=True)
            serving_ms, result, error = run_query(sql, endpoint_url, session)

            if error:
                print(f"ERROR {error}")
                writer.writerow(
                    [query_id, serving_ms, serving_ms, 0, 0, "", error, mode]
                )
                plot_latencies.append(0.0)
            else:
                total_ms = serving_ms + pipeline_overhead_ms
                result_lines = result.strip().split("\n") if result else []
                num_rows = len(result_lines)
                preview = result[:100].replace("\n", " | ") if result else ""
                serving_latencies.append(serving_ms)
                total_latencies.append(total_ms)
                plot_latencies.append(total_ms)
                if pipeline_overhead_ms > 0:
                    print(
                        f"{total_ms:.2f}ms (serving={serving_ms:.2f}ms + pipeline={pipeline_overhead_ms:.2f}ms, {num_rows} rows)"
                    )
                else:
                    print(f"{total_ms:.2f}ms ({num_rows} rows)")
                writer.writerow(
                    [
                        query_id,
                        f"{total_ms:.2f}",
                        f"{serving_ms:.2f}",
                        f"{pipeline_overhead_ms:.2f}",
                        num_rows,
                        preview,
                        "",
                        mode,
                    ]
                )

            time.sleep(0.1)

    print(f"\nResults saved to {output_csv}")

    if total_latencies:
        total_latencies.sort()
        serving_latencies.sort()
        n = len(total_latencies)

        def stats(arr):
            return (
                arr[0],
                sum(arr) / len(arr),
                arr[int(len(arr) * 0.5)],
                arr[int(len(arr) * 0.95)],
                arr[-1],
            )

        t_min, t_avg, t_p50, t_p95, t_max = stats(total_latencies)
        print(f"\nTotal latency summary ({n} successful queries):")
        print(
            f"  min={t_min:.2f}ms  avg={t_avg:.2f}ms  p50={t_p50:.2f}ms  p95={t_p95:.2f}ms  max={t_max:.2f}ms"
        )
        if pipeline_overhead_ms > 0:
            s_min, s_avg, s_p50, s_p95, s_max = stats(serving_latencies)
            print(
                f"  (serving only: min={s_min:.2f}ms  avg={s_avg:.2f}ms  p50={s_p50:.2f}ms)"
            )
            print(f"  (pipeline overhead: {pipeline_overhead_ms:.2f}ms per query)")

    if plot_latencies:
        plt.figure(figsize=(10, 6))
        bar_color = "#1f77b4" if mode == "baseline" else "#ff7f0e"
        execution_order = list(range(1, len(plot_latencies) + 1))
        plt.bar(execution_order, plot_latencies, color=bar_color, edgecolor="black")
        plt.xlabel("Query Execution Order", fontsize=12, fontweight="bold")
        plt.ylabel("Latency (ms)", fontsize=12, fontweight="bold")
        max_order = len(execution_order)
        tick_step = max(1, max_order // 20) * 5
        plt.xticks(range(0, max_order + 1, tick_step))
        plt.title(
            f"Query Latency - {mode.upper()} Mode", fontsize=14, fontweight="bold"
        )
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        plt.tight_layout()
        plot_output = output_csv.with_suffix(".png")
        plt.savefig(plot_output)
        plt.close()
        print(f"Plot saved to {plot_output}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark ASAP vs ClickHouse on H2O groupby data"
    )
    parser.add_argument("--mode", choices=["baseline", "asap"], default="asap")
    parser.add_argument("--load-data", action="store_true", help="Load H2O data")
    parser.add_argument("--clickhouse-url", default="http://localhost:8123")
    parser.add_argument("--asap-url", default="http://localhost:8088/clickhouse/query")
    parser.add_argument("--output", default="asap_results.csv")
    parser.add_argument("--sql-file", default=None)
    parser.add_argument("--filter", default=None, help="Comma-separated query IDs")
    parser.add_argument("--no-benchmark", action="store_true", help="Load data only")
    parser.add_argument("--skip-table-init", action="store_true")
    parser.add_argument(
        "--load-kafka",
        action="store_true",
        help="Stream data to Kafka (for Arroyo sketch pipeline)",
    )
    parser.add_argument(
        "--max-rows", type=int, default=0, help="Max rows to load (0 = all)"
    )

    args = parser.parse_args()

    if args.load_data:
        if not load_h2o_data_clickhouse(
            args.clickhouse_url,
            skip_table_init=args.skip_table_init,
            max_rows=args.max_rows,
        ):
            print("Failed to load data")
            return 1

    if args.load_kafka:
        produce_h2o_to_kafka(max_rows=args.max_rows)

    if args.no_benchmark:
        return 0

    if args.sql_file:
        sql_file = Path(args.sql_file)
    elif args.mode == "baseline":
        sql_file = Path(__file__).parent / "clickhouse_quantile_queries.sql"
    else:
        sql_file = Path(__file__).parent / "asap_quantile_queries.sql"

    endpoint = args.clickhouse_url if args.mode == "baseline" else args.asap_url
    query_filter = [q.strip() for q in args.filter.split(",")] if args.filter else None

    pipeline_overhead_ms = 0.0
    if args.mode == "asap":
        print("\nMeasuring pipeline latency (data → Kafka → Arroyo → QE → query)...")
        pipeline_overhead_ms = measure_pipeline_latency(asap_url=args.asap_url)

    run_benchmark(
        sql_file,
        endpoint,
        Path(args.output),
        args.mode,
        query_filter,
        pipeline_overhead_ms,
    )
    return 0


if __name__ == "__main__":
    exit(main())
