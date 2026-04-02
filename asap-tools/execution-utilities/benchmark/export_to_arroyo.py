#!/usr/bin/env python3
"""
Launch an Arroyo sketch pipeline against a dataset.

Supports two source modes:
  file   (default): Arroyo reads directly from a local JSON/Parquet file.
                    No Kafka input topic is required.
  kafka:            Arroyo reads from a Kafka topic (legacy path).

In both cases the sketch output is written to a Kafka topic (default:
sketch_topic) for consumption by QueryEngineRust.

Usage:
    # File source (recommended)
    python export_to_arroyo.py \\
        --streaming-config configs/clickbench_streaming.yaml \\
        --source-type file \\
        --input-file ./data/hits.json.gz \\
        --file-format json \\
        --ts-format rfc3339 \\
        --pipeline-name clickbench_pipeline \\
        --arroyosketch-dir ~/ASAPQuery/asap-summary-ingest

    # Kafka source (legacy)
    python export_to_arroyo.py \\
        --streaming-config configs/h2o_streaming.yaml \\
        --source-type kafka \\
        --input-kafka-topic h2o_groupby \\
        --pipeline-name h2o_pipeline \\
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
    """Poll the Arroyo API until the named pipeline reaches RUNNING state.

    Translated from asap_benchmark_pipeline/run_pipeline.sh lines 107-141.
    A pipeline is considered running when its 'state' field is None and
    'stop' is 'none' (Arroyo's representation of a healthy running pipeline).
    """
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
            print(
                f"ERROR: Pipeline did not reach RUNNING state within {timeout}s"
            )
            return False


def build_arroyosketch_cmd(args, arroyosketch_script: str) -> list:
    """Build the run_arroyosketch.py command from our CLI arguments."""
    cmd = [
        sys.executable,
        arroyosketch_script,
        "--source_type", args.source_type,
        "--output_format", "json",
        "--pipeline_name", args.pipeline_name,
        "--config_file_path", os.path.abspath(args.streaming_config),
        "--output_kafka_topic", args.output_kafka_topic,
        "--output_dir", os.path.abspath(args.output_dir),
        "--parallelism", str(args.parallelism),
        "--query_language", "sql",
    ]

    if args.source_type == "file":
        cmd += [
            "--input_file_path", os.path.abspath(args.input_file),
            "--file_format", args.file_format,
            "--ts_format", args.ts_format,
        ]
    elif args.source_type == "kafka":
        cmd += [
            "--kafka_input_format", "json",
            "--input_kafka_topic", args.input_kafka_topic,
        ]

    return cmd


def main():
    parser = argparse.ArgumentParser(
        description="Launch Arroyo sketch pipeline (file or kafka source)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--streaming-config",
        required=True,
        help="Path to streaming_config.yaml",
    )
    parser.add_argument(
        "--source-type",
        choices=["file", "kafka"],
        default="file",
        help="Data source type (default: file)",
    )
    # File source args
    parser.add_argument(
        "--input-file",
        default=None,
        help="Path to input data file (required for --source-type file)",
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
    # Kafka source args
    parser.add_argument(
        "--input-kafka-topic",
        default=None,
        help="Kafka topic to read from (required for --source-type kafka)",
    )
    # Common args
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
        "--wait-for-pipeline",
        action="store_true",
        default=True,
        help="Poll until pipeline reaches RUNNING state (default: True)",
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

    # Validate source-specific required args
    if args.source_type == "file" and not args.input_file:
        parser.error("--input-file is required when --source-type file")
    if args.source_type == "kafka" and not args.input_kafka_topic:
        parser.error("--input-kafka-topic is required when --source-type kafka")

    arroyosketch_script = os.path.join(
        os.path.abspath(args.arroyosketch_dir), "run_arroyosketch.py"
    )
    if not os.path.exists(arroyosketch_script):
        print(f"ERROR: run_arroyosketch.py not found at {arroyosketch_script}")
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    cmd = build_arroyosketch_cmd(args, arroyosketch_script)
    print(f"Launching Arroyo pipeline '{args.pipeline_name}' ({args.source_type} source)...")
    print(f"Command: {' '.join(cmd)}")

    result = subprocess.run(cmd)
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
