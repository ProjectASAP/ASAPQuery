#!/usr/bin/env python3
"""Materialize a reproducible Google task-usage experiment scenario."""

import argparse
import csv
import gzip
import hashlib
import io
import json
from pathlib import Path


START_TIME = 0
END_TIME = 1
JOB_ID = 2
TASK_INDEX = 3
MACHINE_ID = 4
AGGREGATION_TYPE = 18
REPLAY_OFFSET_US = 600_000_000


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def gzip_payload_sha256(path):
    """Hash the decompressed CSV, so dataset identity ignores gzip metadata."""
    digest = hashlib.sha256()
    with gzip.open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def materialize(spec, spec_directory, output_directory):
    source = (spec_directory / spec["source_file"]).resolve()
    start_us, end_us = spec["source_time_range_us"]
    labels = spec["grouping_labels"]
    if labels != ["job_id", "task_index", "machine_id"]:
        raise ValueError(
            "Google v1 supports exactly job_id, task_index, machine_id grouping"
        )

    output_directory.mkdir(parents=True, exist_ok=True)
    output_data = output_directory / source.name
    series = set()
    seen_samples = set()
    rows = 0
    with gzip.open(source, "rt", newline="") as input_file, gzip.GzipFile(
        output_data, "wb", mtime=0
    ) as compressed_output, io.TextIOWrapper(
        compressed_output, newline=""
    ) as output_file:
        reader = csv.reader(input_file)
        writer = csv.writer(output_file, lineterminator="\n")
        for row in reader:
            if len(row) <= AGGREGATION_TYPE:
                raise ValueError(f"malformed source row with {len(row)} columns")
            row_start, row_end = int(row[START_TIME]), int(row[END_TIME])
            aggregation_type = row[AGGREGATION_TYPE] or "0"
            if not (
                row_start >= start_us and row_end <= end_us and aggregation_type == "0"
            ):
                continue
            group = (row[JOB_ID], row[TASK_INDEX], row[MACHINE_ID])
            sample_key = (row_start, group)
            if sample_key in seen_samples:
                raise ValueError(f"duplicate exported sample at {sample_key}")
            seen_samples.add(sample_key)
            if spec.get("rebase_to_offset", False):
                row[START_TIME] = str(row_start - start_us + REPLAY_OFFSET_US)
                row[END_TIME] = str(row_end - start_us + REPLAY_OFFSET_US)
            writer.writerow(row)
            series.add(group)
            rows += 1

    inventory = output_directory / "series_inventory.csv"
    with inventory.open("w", newline="") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(["metric", *labels])
        for group in sorted(series):
            writer.writerow([spec["exported_metric"], *group])

    manifest = {
        "schema_version": 1,
        "scenario_spec": spec,
        "source_sha256": sha256(source),
        "output_sha256": sha256(output_data),
        "output_payload_sha256": gzip_payload_sha256(output_data),
        "records_loaded": rows,
        "grouping_state_count": len(series),
        "arrival_rate_hz": rows / ((end_us - start_us) / 1_000_000),
        "duplicate_policy": "fail",
    }
    (output_directory / "scenario_manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )
    return manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    spec = json.loads(args.spec.read_text())
    materialize(spec, args.spec.parent, args.output)


if __name__ == "__main__":
    main()
