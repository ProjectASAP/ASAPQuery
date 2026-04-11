#!/usr/bin/env python3
"""
Generate a SQL query file for benchmarking ASAP and ClickHouse.

Each query uses database-style quantile(q)(col) syntax, compatible with both
QueryEngineRust and ClickHouse. Queries target fixed time windows and match the
annotation format `-- T{NNN}: description` expected by run_benchmark.py.

Output:
  {prefix}.sql             quantile(q)(col) database-style syntax, compatible with both
                           QueryEngineRust and ClickHouse baseline

Usage:
    # Auto-detect timestamps from data file → ./queries/clickbench.sql
    python generate_queries.py \\
        --table-name hits \\
        --ts-column EventTime \\
        --value-column ResolutionWidth \\
        --group-by-columns RegionID,OS,UserAgent,TraficSourceID \\
        --window-size 10 \\
        --num-queries 50 \\
        --auto-detect-timestamps \\
        --data-file ./data/hits.json.gz \\
        --data-file-format json.gz \\
        --output-prefix ./queries/clickbench

    # Explicit timestamp file (one ISO timestamp per line) → ./queries/h2o.sql
    python generate_queries.py \\
        --table-name h2o_groupby \\
        --ts-column timestamp \\
        --value-column v1 \\
        --group-by-columns id1,id2 \\
        --window-size 10 \\
        --num-queries 50 \\
        --timestamps-file ./my_timestamps.txt \\
        --output-prefix ./queries/h2o
"""

import argparse
import gzip
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional


SAMPLE_SIZE = 10_000  # rows to read for timestamp auto-detection


def _parse_timestamp(value: str) -> Optional[datetime]:
    """Try to parse a timestamp string in common formats."""
    value = str(value).strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    # Try unix seconds/millis (numeric string)
    try:
        v = float(value)
        if v > 1e12:  # millis
            return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
        return datetime.fromtimestamp(v, tz=timezone.utc)
    except ValueError:
        pass
    return None


def _read_timestamps_from_json(
    file_path: str, ts_column: str, compressed: bool
) -> List[datetime]:
    """Read up to SAMPLE_SIZE timestamps from a JSON-lines file."""
    timestamps = []
    opener = gzip.open if compressed else open
    mode = "rt" if compressed else "r"
    with opener(file_path, mode) as f:
        for i, line in enumerate(f):
            if i >= SAMPLE_SIZE:
                break
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                val = obj.get(ts_column)
                if val is not None:
                    ts = _parse_timestamp(val)
                    if ts:
                        timestamps.append(ts)
            except (json.JSONDecodeError, KeyError):
                continue
    return timestamps


def _read_timestamps_from_csv(file_path: str, ts_column: str) -> List[datetime]:
    """Read up to SAMPLE_SIZE timestamps from a CSV file."""
    import csv

    timestamps = []
    with open(file_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        if ts_column not in (reader.fieldnames or []):
            print(
                f"WARNING: Column '{ts_column}' not found in CSV. "
                f"Available: {reader.fieldnames}"
            )
            return []
        for i, row in enumerate(reader):
            if i >= SAMPLE_SIZE:
                break
            ts = _parse_timestamp(row[ts_column])
            if ts:
                timestamps.append(ts)
    return timestamps


def detect_timestamps(data_file: str, data_file_format: str, ts_column: str) -> tuple:
    """Return (min_ts, max_ts) from a sample of the data file."""
    fmt = data_file_format.lower()
    if fmt in ("json.gz", "jsonl.gz"):
        timestamps = _read_timestamps_from_json(data_file, ts_column, compressed=True)
    elif fmt in ("json", "jsonl"):
        timestamps = _read_timestamps_from_json(data_file, ts_column, compressed=False)
    elif fmt == "csv":
        timestamps = _read_timestamps_from_csv(data_file, ts_column)
    else:
        print(f"ERROR: Unsupported data file format: {data_file_format}")
        sys.exit(1)

    if not timestamps:
        print(
            f"ERROR: No '{ts_column}' timestamps found in the first {SAMPLE_SIZE} "
            f"rows of {data_file}"
        )
        sys.exit(1)

    return min(timestamps), max(timestamps)


def _snap_to_window_boundary(ts: datetime, window_size: int) -> datetime:
    """Round a timestamp up to the next window boundary (epoch-aligned).

    Arroyo tumbling windows are aligned to epoch multiples of window_size.
    Querying at a non-boundary timestamp will miss the sketch.
    """
    epoch_sec = int(ts.timestamp())
    remainder = epoch_sec % window_size
    if remainder == 0:
        return ts
    snapped = epoch_sec + (window_size - remainder)
    return datetime.fromtimestamp(snapped, tz=timezone.utc)


def generate_window_ends(
    min_ts: datetime,
    max_ts: datetime,
    window_size: int,
    stride: int,
    num_queries: int,
) -> List[datetime]:
    """Generate evenly-spaced window-end timestamps within [min_ts, max_ts].

    Timestamps are snapped to epoch-aligned window boundaries so that
    Arroyo's tumbling window sketches can be found by QueryEngineRust.
    """
    # First valid window-end: snap to next boundary after min_ts + window_size
    earliest = min_ts + timedelta(seconds=window_size)
    start = _snap_to_window_boundary(earliest, window_size)
    if start >= max_ts:
        print(
            f"WARNING: window_size ({window_size}s) exceeds the data time range "
            f"({(max_ts - min_ts).total_seconds():.0f}s). Using max_ts as only endpoint."
        )
        return [max_ts]

    ends = []
    current = start
    while current <= max_ts and len(ends) < num_queries:
        ends.append(current)
        current += timedelta(seconds=stride)

    return ends


def format_ts(ts: datetime, ts_format: str) -> str:
    """Format a timestamp for SQL injection."""
    if ts_format == "iso":
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:  # datetime
        return ts.strftime("%Y-%m-%d %H:%M:%S")


def generate_sql_file(
    table_name: str,
    ts_column: str,
    value_column: str,
    group_by_columns: List[str],
    quantile: float,
    window_size: int,
    window_ends: List[datetime],
    ts_format: str,
    window_form: str,
    output_prefix: str,
):
    """Write a single SQL file compatible with both ASAP and ClickHouse."""
    group_by_clause = ", ".join(group_by_columns)
    percentile = quantile * 100
    # Strip trailing zero: 95.0 -> 95, 99.5 -> 99.5
    percentile_str = f"{percentile:.1f}".rstrip("0").rstrip(".")

    ch_lines = []
    es_lines = []

    for i, end_ts in enumerate(window_ends):
        end_str = format_ts(end_ts, ts_format)
        start_ts = end_ts - timedelta(seconds=window_size)
        start_str = format_ts(start_ts, ts_format)
        label = f"T{i:03d}"
        desc = f"quantile window ending at {end_str}"

        if window_form == "dateadd":
            where_clause = f"{ts_column} BETWEEN DATEADD(s, -{window_size}, '{end_str}') AND '{end_str}'"
        else:
            where_clause = f"{ts_column} BETWEEN '{start_str}' AND '{end_str}'"

        # Elasticsearch uses DATEADD + CAST form
        es_where = (
            f"{ts_column} BETWEEN DATEADD('s', -{window_size}, CAST('{end_str}' AS DATETIME)) "
            f"AND CAST('{end_str}' AS DATETIME)"
        )

        ch_sql = (
            f"-- {label}: {desc}\n"
            f"SELECT quantile({quantile})({value_column}) FROM {table_name} "
            f"WHERE {where_clause} GROUP BY {group_by_clause};"
        )

        asap_lines.append(asap_sql)
        ch_lines.append(ch_sql)
        es_lines.append(
            f"-- {label}: {desc}\n"
            f"SELECT PERCENTILE({value_column}, {percentile_str}) FROM {table_name} "
            f"WHERE {es_where} GROUP BY {group_by_clause};"
        )

    ch_file = f"{output_prefix}_clickhouse.sql"
    es_file = f"{output_prefix}_elasticsearch.sql"

    Path(asap_file).parent.mkdir(parents=True, exist_ok=True)

    with open(ch_file, "w") as f:
        f.write("\n".join(ch_lines) + "\n")

    with open(es_file, "w") as f:
        f.write("\n".join(es_lines) + "\n")

    print(f"Generated {len(window_ends)} queries:")
    print(f"  ClickHouse:    {ch_file}")
    print(f"  Elasticsearch: {es_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate paired ASAP + ClickHouse SQL query files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    # Table/column config
    parser.add_argument("--table-name", required=True)
    parser.add_argument("--ts-column", required=True, help="Timestamp column name")
    parser.add_argument(
        "--value-column", required=True, help="Column to compute quantile on"
    )
    parser.add_argument(
        "--group-by-columns",
        required=True,
        help="Comma-separated GROUP BY columns",
    )
    # Query parameters
    parser.add_argument("--quantile", type=float, default=0.95)
    parser.add_argument(
        "--window-size", type=int, default=10, help="Window size in seconds"
    )
    parser.add_argument("--num-queries", type=int, default=50)
    parser.add_argument(
        "--ts-format",
        choices=["iso", "datetime"],
        default="iso",
        help="Timestamp format in SQL: iso='YYYY-MM-DDTHH:MM:SSZ', datetime='YYYY-MM-DD HH:MM:SS' (default: iso)",
    )
    parser.add_argument(
        "--window-form",
        choices=["explicit", "dateadd"],
        default="explicit",
        help="SQL window form: explicit='BETWEEN start AND end', dateadd='BETWEEN DATEADD(s,-N,end) AND end' (default: explicit)",
    )
    parser.add_argument(
        "--output-prefix",
        required=True,
        help="Output file prefix (e.g. ./queries/clickbench → clickbench.sql)",
    )
    # Timestamp sources (mutually exclusive)
    ts_group = parser.add_mutually_exclusive_group(required=True)
    ts_group.add_argument(
        "--auto-detect-timestamps",
        action="store_true",
        help="Scan data file to determine time range",
    )
    ts_group.add_argument(
        "--timestamps-file",
        default=None,
        help="File with explicit window-end timestamps (one ISO timestamp per line)",
    )
    # Auto-detect options
    parser.add_argument(
        "--data-file",
        default=None,
        help="Path to data file (required with --auto-detect-timestamps)",
    )
    parser.add_argument(
        "--data-file-format",
        choices=["json", "jsonl", "json.gz", "jsonl.gz", "csv"],
        default="json",
        help="Data file format (default: json)",
    )
    parser.add_argument(
        "--stride-seconds",
        type=int,
        default=None,
        help="Spacing between window-end timestamps (default: window-size * 3)",
    )

    args = parser.parse_args()

    if args.auto_detect_timestamps and not args.data_file:
        parser.error("--data-file is required when --auto-detect-timestamps is set")

    group_by_columns = [c.strip() for c in args.group_by_columns.split(",")]
    stride = args.stride_seconds if args.stride_seconds else args.window_size * 3

    # Determine window-end timestamps
    if args.timestamps_file:
        window_ends = []
        with open(args.timestamps_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                ts = _parse_timestamp(line)
                if ts:
                    window_ends.append(ts)
                else:
                    print(f"WARNING: Could not parse timestamp: {line!r}")
        if not window_ends:
            print("ERROR: No valid timestamps found in --timestamps-file")
            sys.exit(1)
        window_ends = window_ends[: args.num_queries]
        print(
            f"Using {len(window_ends)} timestamps from {args.timestamps_file} "
            f"({window_ends[0]} – {window_ends[-1]})"
        )
    else:
        print(f"Scanning {args.data_file} for timestamp range...")
        min_ts, max_ts = detect_timestamps(
            args.data_file, args.data_file_format, args.ts_column
        )
        print(f"  Detected range: {min_ts} – {max_ts}")
        window_ends = generate_window_ends(
            min_ts, max_ts, args.window_size, stride, args.num_queries
        )
        print(
            f"  Generated {len(window_ends)} window endpoints "
            f"(stride={stride}s, window={args.window_size}s)"
        )

    generate_sql_file(
        table_name=args.table_name,
        ts_column=args.ts_column,
        value_column=args.value_column,
        group_by_columns=group_by_columns,
        quantile=args.quantile,
        window_size=args.window_size,
        window_ends=window_ends,
        ts_format=args.ts_format,
        window_form=args.window_form,
        output_prefix=args.output_prefix,
    )


if __name__ == "__main__":
    main()
