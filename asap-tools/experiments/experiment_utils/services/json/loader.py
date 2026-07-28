#!/usr/bin/env python3
"""
Load a JSON-lines file into ClickHouse in bounded-size batches.

Uses ``docker exec … clickhouse-client`` on the experiment node so multi-GB
files stream without loading the whole payload into curl/HTTP memory.

Usage:
    python3 loader.py \\
        --data-file /path/to/netflow.jsonl \\
        --table netflow_table \\
        --batch-size 100000 \\
        --max-rows 0
"""

import argparse
import gzip
import json
import subprocess
import sys


DEFAULT_CONTAINER = "clickhouse-server"
PROGRESS_ROW_INTERVAL = 500_000


def _open_data_file(path: str):
    lower = path.lower()
    if lower.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def _validate_line(line: str, line_no: int) -> str:
    stripped = line.strip()
    if not stripped:
        return ""
    if "\0" in stripped:
        preview = stripped[:80].replace("\0", "\\0")
        raise RuntimeError(
            f"Null byte in JSON line {line_no} (file may be corrupt — "
            f"regenerate on a native Linux filesystem, not WSL /mnt/c): {preview!r}"
        )
    try:
        json.loads(stripped)
    except json.JSONDecodeError as exc:
        preview = stripped[:120]
        raise RuntimeError(
            f"Invalid JSON on line {line_no}: {exc}. Preview: {preview!r}"
        ) from exc
    return stripped


def flush_batch(
    table: str,
    lines: list[str],
    container: str,
) -> None:
    body = "\n".join(lines).encode("utf-8")
    result = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            container,
            "clickhouse-client",
            "--query",
            f"INSERT INTO {table} FORMAT JSONEachRow",
        ],
        input=body,
        capture_output=True,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or b"").decode(
            "utf-8", errors="replace"
        )[:500]
        raise RuntimeError(f"ClickHouse insert failed: {detail}")


def load(
    data_file: str,
    table: str,
    batch_size: int,
    max_rows: int,
    container: str = DEFAULT_CONTAINER,
) -> None:
    batch: list[str] = []
    total = 0
    file_line_no = 0
    next_progress_report = PROGRESS_ROW_INTERVAL

    def flush(lines: list[str]) -> None:
        nonlocal total
        if not lines:
            return
        if max_rows > 0:
            lines = lines[: max_rows - total]
        if not lines:
            return
        flush_batch(table, lines, container)
        total += len(lines)

    with _open_data_file(data_file) as fin:
        for raw_line in fin:
            file_line_no += 1
            if max_rows > 0 and total >= max_rows:
                break
            stripped = _validate_line(raw_line, file_line_no)
            if not stripped:
                continue
            batch.append(stripped)
            if len(batch) >= batch_size:
                flush(batch)
                batch = []
                if total >= next_progress_report:
                    print(f"  Inserted {total:,} rows...", flush=True)
                    next_progress_report = total + PROGRESS_ROW_INTERVAL

    flush(batch)
    print(f"JSON load complete: {total:,} rows into {table!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-file", required=True, help="Path to JSON-lines file")
    parser.add_argument("--table", required=True, help="Target table name")
    parser.add_argument(
        "--batch-size", type=int, default=100_000, help="Rows per INSERT batch"
    )
    parser.add_argument(
        "--max-rows", type=int, default=0, help="Max rows to load (0 = all)"
    )
    parser.add_argument(
        "--container",
        default=DEFAULT_CONTAINER,
        help=f"ClickHouse docker container name (default: {DEFAULT_CONTAINER})",
    )
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be >= 1")
    try:
        load(args.data_file, args.table, args.batch_size, args.max_rows, args.container)
    except (RuntimeError, OSError) as exc:
        # The caller surfaces only the head of stderr, so print the message
        # itself rather than letting a traceback bury it.
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
