#!/usr/bin/env python3
"""
Load an H2O GroupBy CSV file into ClickHouse with synthetic timestamps.

Timestamps are assigned at ROWS_PER_SECOND rows/sec starting from BASE_EPOCH
(2024-01-01T00:00:00Z), matching the logic in benchmark/export_to_database.py.

Uses only Python stdlib (urllib) so it runs on any Python 3 installation
without extra packages.

Usage:
    python3 h2o_clickhouse_loader.py \\
        --data-file /path/to/G1_1e7_1e2_0_0.csv \\
        --url http://localhost:8123/ \\
        --batch-size 50000 \\
        --max-rows 0
"""

import argparse
import urllib.request
from datetime import datetime, timezone

BASE_EPOCH = 1_704_067_200  # 2024-01-01T00:00:00Z
ROWS_PER_SECOND = 1_000


def flush_batch(url: str, rows: list) -> None:
    sql = "INSERT INTO h2o_groupby VALUES " + ",".join(rows)
    req = urllib.request.Request(url, data=sql.encode("utf-8"))
    with urllib.request.urlopen(req) as resp:
        body = resp.read()
        if resp.status != 200:
            raise RuntimeError(
                "ClickHouse insert failed: "
                + body[:200].decode("utf-8", errors="replace")
            )


def load(data_file: str, url: str, batch_size: int, max_rows: int) -> None:
    batch = []
    total = 0
    with open(data_file, "r", encoding="utf-8") as f:
        f.readline()  # skip CSV header
        for i, line in enumerate(f):
            if max_rows > 0 and i >= max_rows:
                break
            parts = line.rstrip("\n").split(",")
            abs_sec = BASE_EPOCH + i // ROWS_PER_SECOND
            ts = datetime.fromtimestamp(abs_sec, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            row = (
                "('"
                + ts
                + "','"
                + parts[0]
                + "','"
                + parts[1]
                + "','"
                + parts[2]
                + "',"
                + parts[3].strip()
                + ","
                + parts[4].strip()
                + ","
                + parts[5].strip()
                + ","
                + parts[6].strip()
                + ","
                + parts[7].strip()
                + ","
                + parts[8].strip()
                + ")"
            )
            batch.append(row)
            if len(batch) >= batch_size:
                flush_batch(url, batch)
                total += len(batch)
                batch = []
                if total % 500_000 == 0:
                    print(f"  Inserted {total:,} rows...")

    if batch:
        flush_batch(url, batch)
        total += len(batch)

    print(f"H2O load complete: {total:,} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-file", required=True, help="Path to H2O CSV file")
    parser.add_argument(
        "--url", default="http://localhost:8123/", help="ClickHouse HTTP URL"
    )
    parser.add_argument(
        "--batch-size", type=int, default=50_000, help="INSERT batch size"
    )
    parser.add_argument(
        "--max-rows", type=int, default=0, help="Max rows to load (0 = all)"
    )
    args = parser.parse_args()
    load(args.data_file, args.url, args.batch_size, args.max_rows)


if __name__ == "__main__":
    main()
