#!/usr/bin/env python3
"""
Launch an Arroyo sketch pipeline from a local file source.

Arroyo reads directly from a local JSON/Parquet file and writes sketches to
a Kafka topic (default: sketch_topic) for consumption by QueryEngineRust.

Usage:
    python export_to_arroyo.py \\
        --streaming-config configs/clickbench_streaming.yaml \\
        --input-file ./data/hits_arroyo.json \\
        --file-format json \\
        --ts-format rfc3339 \\
        --pipeline-name clickbench_pipeline \\
        --arroyosketch-dir ~/ASAPQuery/asap-summary-ingest
"""

import argparse
import os
import subprocess
import sys
import time

import requests

DEFAULT_ARROYO_URL = "http://localhost:5115/api/v1"
DEFAULT_OUTPUT_KAFKA_TOPIC = "sketch_topic"
DEFAULT_PARALLELISM = 1
DEFAULT_WAIT_TIMEOUT = 300


def wait_for_pipeline_running(
    pipeline_name: str,
    arroyo_url: str = DEFAULT_ARROYO_URL,
    timeout: int = DEFAULT_WAIT_TIMEOUT,
) -> bool:
    """Poll the Arroyo API until the named pipeline reaches RUNNING state."""
    print(f"Waiting for pipeline '{pipeline_name}' to reach RUNNING state...")
    elapsed = 0
    while True:
        state = "error"
        try:
            r = requests.get(f"{arroyo_url}/pipelines", timeout=5)
            if r.ok:
                data = r.json()
                for p in data.get("data", []):
                    if p.get("name") == pipeline_name:
                        s = p.get("state")
                        stop = p.get("stop", "")
                        if s is None and stop == "none":
                            state = "running"
                        else:
                            state = str(s).lower() if s else "unknown"
                        break
                else:
                    state = "not_found"
        except Exception:
            state = "error"

        if state == "running":
            print(f"Pipeline '{pipeline_name}' is RUNNING")
            return True

        print(f"  Pipeline state: {state} (elapsed: {elapsed}s)")
        time.sleep(5)
        elapsed += 5
        if elapsed >= timeout:
            print(f"ERROR: Pipeline did not reach RUNNING state within {timeout}s")
            return False


def build_arroyosketch_cmd(args, arroyosketch_script: str) -> list:
    """Build the run_arroyosketch.py command from our CLI arguments."""
    return [
        sys.executable,
        arroyosketch_script,
        "--source_type",
        "file",
        "--output_format",
        "json",
        "--pipeline_name",
        args.pipeline_name,
        "--config_file_path",
        os.path.abspath(args.streaming_config),
        "--output_kafka_topic",
        args.output_kafka_topic,
        "--output_dir",
        os.path.abspath(args.output_dir),
        "--parallelism",
        str(args.parallelism),
        "--query_language",
        "sql",
        "--input_file_path",
        os.path.abspath(args.input_file),
        "--file_format",
        args.file_format,
        "--ts_format",
        args.ts_format,
    ]


def main():
    parser = argparse.ArgumentParser(
        description="Launch Arroyo sketch pipeline from a local file source",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--streaming-config",
        required=True,
        help="Path to streaming_config.yaml",
    )
    parser.add_argument(
        "--input-file",
        required=True,
        help="Path to input data file (JSON or Parquet)",
    )
    parser.add_argument(
        "--file-format",
        choices=["json", "parquet"],
        default="json",
        help="File format (default: json)",
    )
    parser.add_argument(
        "--ts-format",
        choices=["unix_millis", "unix_seconds", "rfc3339"],
        default="rfc3339",
        help="Timestamp format in the data file (default: rfc3339)",
    )
    parser.add_argument(
        "--output-kafka-topic",
        default=DEFAULT_OUTPUT_KAFKA_TOPIC,
        help=f"Kafka topic for sketch output (default: {DEFAULT_OUTPUT_KAFKA_TOPIC})",
    )
    parser.add_argument(
        "--pipeline-name",
        required=True,
        help="Arroyo pipeline name",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=DEFAULT_PARALLELISM,
        help=f"Arroyo pipeline parallelism (default: {DEFAULT_PARALLELISM})",
    )
    parser.add_argument(
        "--arroyosketch-dir",
        required=True,
        help="Path to asap-summary-ingest/ directory (contains run_arroyosketch.py)",
    )
    parser.add_argument(
        "--arroyo-url",
        default=DEFAULT_ARROYO_URL,
        help=f"Arroyo API base URL (default: {DEFAULT_ARROYO_URL})",
    )
    parser.add_argument(
        "--output-dir",
        default="./arroyo_outputs",
        help="Directory for Arroyo pipeline output artifacts (default: ./arroyo_outputs)",
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Do not wait for pipeline to reach RUNNING state",
    )
    parser.add_argument(
        "--wait-timeout",
        type=int,
        default=DEFAULT_WAIT_TIMEOUT,
        help=f"Seconds to wait for RUNNING state (default: {DEFAULT_WAIT_TIMEOUT})",
    )

    args = parser.parse_args()

    arroyosketch_script = os.path.join(
        os.path.abspath(args.arroyosketch_dir), "run_arroyosketch.py"
    )
    if not os.path.exists(arroyosketch_script):
        print(f"ERROR: run_arroyosketch.py not found at {arroyosketch_script}")
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    cmd = build_arroyosketch_cmd(args, arroyosketch_script)
    print(f"Launching Arroyo pipeline '{args.pipeline_name}'...")
    print(f"Command: {' '.join(cmd)}")

    result = subprocess.run(cmd, cwd=os.path.abspath(args.arroyosketch_dir))
    if result.returncode != 0:
        print(f"ERROR: run_arroyosketch.py exited with code {result.returncode}")
        sys.exit(result.returncode)

    if not args.no_wait:
        success = wait_for_pipeline_running(
            args.pipeline_name,
            arroyo_url=args.arroyo_url,
            timeout=args.wait_timeout,
        )
        if not success:
            sys.exit(1)

    print("Done.")


if __name__ == "__main__":
    main()
