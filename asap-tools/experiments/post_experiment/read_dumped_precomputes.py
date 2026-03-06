#!/usr/bin/env python3
"""
Script to read QueryEngineRust --dump-precomputes output files.
Files are in MessagePack format with length-prefixed records.
Rust structs are serialized as arrays/lists.
"""

import sys
import struct
import msgpack
from pathlib import Path
from typing import List, Any
import datetime


class PrecomputeDump:
    """Represents the PrecomputeDump struct from Rust."""

    def __init__(self, data: List[Any]):
        if len(data) != 4:
            raise ValueError(f"Expected 4 fields in PrecomputeDump, got {len(data)}")

        self.timestamp = data[0]  # u64
        self.metadata = PrecomputedOutput(data[1])  # PrecomputedOutput
        self.accumulator_type = data[2]  # String
        self.accumulator_data_bytes = data[3]  # Vec<u8>


class PrecomputedOutput:
    """Represents the PrecomputedOutput struct from Rust."""

    def __init__(self, data: List[Any]):
        if len(data) != 4:
            raise ValueError(f"Expected 4 fields in PrecomputedOutput, got {len(data)}")

        self.start_timestamp = data[0]  # u64 (milliseconds)
        self.end_timestamp = data[1]  # u64 (milliseconds)
        self.key = (
            KeyByLabelValues(data[2]) if data[2] is not None else None
        )  # Option<KeyByLabelValues>
        self.aggregation_id = data[3]  # u64


class KeyByLabelValues:
    """Represents the KeyByLabelValues struct from Rust."""

    def __init__(self, data: List[Any]):
        if len(data) != 1:
            raise ValueError(f"Expected 1 field in KeyByLabelValues, got {len(data)}")

        self.label_values = data[0]  # Vec<String>

    def __str__(self):
        return f"KeyByLabelValues({self.label_values})"


def read_precompute_dump(file_path: str) -> List[PrecomputeDump]:
    """
    Read a precompute dump file and return list of PrecomputeDump records.
    """
    records = []

    with open(file_path, "rb") as f:
        while True:
            # Read 4-byte length prefix
            length_bytes = f.read(4)
            if len(length_bytes) < 4:
                break  # EOF

            length = struct.unpack("<I", length_bytes)[0]  # little-endian uint32

            # Read MessagePack data
            data = f.read(length)
            if len(data) < length:
                print("Warning: Incomplete record at end of file")
                break

            # Deserialize MessagePack
            try:
                raw_record = msgpack.unpackb(data, raw=False)
                record = PrecomputeDump(raw_record)
                records.append(record)
            except Exception as e:
                print(f"Error deserializing record: {e}")
                print(f"Raw data: {raw_record}")
                continue

    return records


def format_timestamp_ms(ts_ms: int) -> str:
    """Convert Unix timestamp in milliseconds to readable format."""
    return datetime.datetime.fromtimestamp(ts_ms / 1000).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )[:-3]


def format_timestamp_s(ts_s: int) -> str:
    """Convert Unix timestamp in seconds to readable format."""
    return datetime.datetime.fromtimestamp(ts_s).strftime("%Y-%m-%d %H:%M:%S")


def print_record_summary(record: PrecomputeDump, index: int):
    """Print a summary of a single record."""
    print(f"\n--- Record {index + 1} ---")
    print(f"Dump timestamp: {format_timestamp_s(record.timestamp)}")

    print(
        f"Precompute period: {format_timestamp_ms(record.metadata.start_timestamp)} to {format_timestamp_ms(record.metadata.end_timestamp)}"
    )
    print(f"Aggregation ID: {record.metadata.aggregation_id}")

    if record.metadata.key:
        print(f"Key labels: {record.metadata.key.label_values}")
    else:
        print("Key: None (global aggregation)")

    print(f"Accumulator type: {record.accumulator_type}")
    print(f"Accumulator data size: {len(record.accumulator_data_bytes)} bytes")
    print(f"Accumulator data (first 20 bytes): {record.accumulator_data_bytes[:20]}")


def analyze_accumulator_data(record: PrecomputeDump):
    """Attempt basic analysis of accumulator data."""
    data = record.accumulator_data_bytes

    if record.accumulator_type == "DatasketchesKLLAccumulator":
        print("  KLL Sketch data analysis:")
        print(f"    Total bytes: {len(data)}")
        if len(data) >= 8:
            # KLL sketches often start with specific headers
            print(f"    First 8 bytes (hex): {' '.join(f'{b:02x}' for b in data[:8])}")

    elif "HyperLogLog" in record.accumulator_type:
        print("  HyperLogLog data analysis:")
        print(f"    Total bytes: {len(data)}")

    elif "CountMin" in record.accumulator_type:
        print("  Count-Min Sketch data analysis:")
        print(f"    Total bytes: {len(data)}")


def main():
    if len(sys.argv) != 2:
        print("Usage: python read_precomputes.py <precompute_dump_file.msgpack>")
        print("Example: python read_precomputes.py precomputes_1757889445.msgpack")
        sys.exit(1)

    file_path = sys.argv[1]

    if not Path(file_path).exists():
        print(f"Error: File {file_path} not found")
        sys.exit(1)

    print(f"Reading precompute dump file: {file_path}")

    try:
        records = read_precompute_dump(file_path)
        print(f"\nFound {len(records)} records")

        # Group by accumulator type
        type_counts = {}
        for record in records:
            type_counts[record.accumulator_type] = (
                type_counts.get(record.accumulator_type, 0) + 1
            )

        print("\nAccumulator types found:")
        for acc_type, count in type_counts.items():
            print(f"  {acc_type}: {count} records")

        # Show details for first few records
        max_detailed = min(5, len(records))
        for i in range(max_detailed):
            record = records[i]
            print_record_summary(record, i)
            analyze_accumulator_data(record)

        if len(records) > max_detailed:
            print(f"\n... ({len(records) - max_detailed} more records)")

    except Exception as e:
        print(f"Error reading file: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
