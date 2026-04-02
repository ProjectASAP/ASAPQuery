#!/usr/bin/env python3
"""
Prepare data files for use with the Arroyo file source.

The Arroyo file source (single_file_custom connector) requires:
  - JSON-lines format
  - Timestamps in RFC3339 format (e.g. "2013-07-14T20:38:47Z")
  - Metadata columns (GROUP BY columns) as strings
  - Value columns as floats

This script converts raw downloaded datasets into the right format.

Usage:
    # ClickBench: convert hits.json.gz → hits_arroyo.json
    python prepare_data.py --dataset clickbench \\
        --input ./data/hits.json.gz \\
        --output ./data/hits_arroyo.json \\
        [--max-rows 1000000]

    # H2O: convert G1_1e7_1e2_0_0.csv → h2o_arroyo.json (adds synthetic timestamps)
    python prepare_data.py --dataset h2o \\
        --input ./data/G1_1e7_1e2_0_0.csv \\
        --output ./data/h2o_arroyo.json \\
        [--max-rows 1000000]
"""

import argparse
import gzip
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Synthetic timestamp base for H2O (2024-01-01T00:00:00Z)
H2O_BASE_EPOCH = 1704067200
H2O_ROWS_PER_SECOND = 1000

# ClickBench columns needed by Arroyo (must match streaming_config.yaml)
CB_TIMESTAMP_FIELD = "EventTime"
CB_VALUE_FIELDS = ["ResolutionWidth"]
CB_METADATA_FIELDS = ["RegionID", "OS", "UserAgent", "TraficSourceID"]
CB_KEEP_FIELDS = [CB_TIMESTAMP_FIELD] + CB_VALUE_FIELDS + CB_METADATA_FIELDS

# H2O columns
H2O_TIMESTAMP_FIELD = "timestamp"
H2O_METADATA_FIELDS = ["id1", "id2"]
H2O_VALUE_FIELDS = ["v1"]


def _parse_clickbench_ts(ts_str: str) -> str:
    """Convert 'YYYY-MM-DD HH:MM:SS' → 'YYYY-MM-DDTHH:MM:SSZ' (RFC3339)."""
    try:
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return ts_str  # already RFC3339 or unknown format


def prepare_clickbench(input_path: str, output_path: str, max_rows: int = 0):
    """Convert hits.json.gz to Arroyo-compatible JSON.

    - Converts EventTime to RFC3339
    - Stringifies integer metadata columns (RegionID, OS, UserAgent, TraficSourceID)
    - Sorts by EventTime (required for Arroyo event-time watermarks)
    - Writes only the fields needed by the streaming config
    """
    print(f"Reading {input_path}...")
    records = []

    opener = gzip.open if input_path.endswith(".gz") else open
    with opener(input_path, "rt") as f:
        for i, line in enumerate(f):
            if max_rows > 0 and i >= max_rows:
                break
            if i % 100_000 == 0 and i > 0:
                print(f"  Read {i:,} rows...", end="\r")
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            ts = _parse_clickbench_ts(str(obj.get(CB_TIMESTAMP_FIELD, "")))
            record = {CB_TIMESTAMP_FIELD: ts}
            for col in CB_VALUE_FIELDS:
                record[col] = float(obj.get(col, 0))
            for col in CB_METADATA_FIELDS:
                record[col] = str(obj.get(col, ""))
            records.append(record)

    print(f"\nSorting {len(records):,} records by {CB_TIMESTAMP_FIELD}...")
    records.sort(key=lambda r: r[CB_TIMESTAMP_FIELD])

    print(f"Writing to {output_path}...")
    with open(output_path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    print(f"Done. {len(records):,} records written.")
    if records:
        print(f"  Time range: {records[0][CB_TIMESTAMP_FIELD]} – {records[-1][CB_TIMESTAMP_FIELD]}")


def prepare_h2o(input_path: str, output_path: str, max_rows: int = 0):
    """Convert H2O CSV to Arroyo-compatible JSON with synthetic timestamps.

    - Adds synthetic RFC3339 timestamps at H2O_ROWS_PER_SECOND rows/sec
      starting from 2024-01-01T00:00:00Z
    - Converts id4, id5, id6 to strings (metadata columns are expected as strings)
    """
    print(f"Reading {input_path}...")
    count = 0

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(output_path, "w") as fout:

        header = fin.readline().strip()
        cols = header.split(",")
        id_idx = {c: i for i, c in enumerate(cols)}

        for i, line in enumerate(fin):
            if max_rows > 0 and i >= max_rows:
                break
            if i % 100_000 == 0 and i > 0:
                print(f"  Written {i:,} rows...", end="\r")

            parts = line.rstrip("\n").split(",")
            abs_sec = H2O_BASE_EPOCH + i // H2O_ROWS_PER_SECOND
            ms = i % H2O_ROWS_PER_SECOND
            ts = datetime.fromtimestamp(abs_sec, tz=timezone.utc)
            ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S") + f".{ms:03d}Z"

            record = {
                H2O_TIMESTAMP_FIELD: ts_str,
                "id1": parts[id_idx["id1"]],
                "id2": parts[id_idx["id2"]],
                "id3": parts[id_idx["id3"]],
                "id4": int(parts[id_idx["id4"]]),
                "id5": int(parts[id_idx["id5"]]),
                "id6": int(parts[id_idx["id6"]]),
                "v1": float(parts[id_idx["v1"]]),
                "v2": float(parts[id_idx["v2"]]),
                "v3": float(parts[id_idx["v3"]]),
            }
            fout.write(json.dumps(record) + "\n")
            count += 1

    print(f"\nDone. {count:,} records written to {output_path}.")
    first_ts = datetime.fromtimestamp(H2O_BASE_EPOCH, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    last_ts = datetime.fromtimestamp(H2O_BASE_EPOCH + count // H2O_ROWS_PER_SECOND, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"  Time range: {first_ts} – {last_ts}")


def main():
    parser = argparse.ArgumentParser(
        description="Prepare dataset files for Arroyo file source",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dataset",
        choices=["clickbench", "h2o"],
        required=True,
        help="Dataset type to prepare",
    )
    parser.add_argument("--input", required=True, help="Path to raw input file")
    parser.add_argument("--output", required=True, help="Path to write prepared JSON file")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Max rows to process (0 = all, default: 0)",
    )
    args = parser.parse_args()

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)

    if args.dataset == "clickbench":
        prepare_clickbench(args.input, args.output, args.max_rows)
    else:
        prepare_h2o(args.input, args.output, args.max_rows)


if __name__ == "__main__":
    main()
