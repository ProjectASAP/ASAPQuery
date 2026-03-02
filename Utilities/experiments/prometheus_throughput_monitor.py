#!/usr/bin/env python3
"""
Prometheus throughput monitoring script using /metrics endpoint.

This version fetches metrics directly from Prometheus /metrics endpoint instead of
using the API. This approach works even when Prometheus is not configured to scrape
itself, which is the case in our setup by design.

For the old API-based approach, see old_prometheus_throughput_monitor.py
"""

import argparse
import json
import os
import re
import time
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional


# Metrics to collect for Prometheus throughput monitoring
THROUGHPUT_METRICS = [
    "prometheus_tsdb_samples_appended_total",
    "prometheus_tsdb_head_samples_appended_total",
    "prometheus_tsdb_symbol_table_size_bytes_total",
    "prometheus_remote_storage_samples_total",
    "prometheus_remote_storage_bytes_total",
    "prometheus_remote_storage_succeeded_samples_total",
    "prometheus_remote_storage_failed_samples_total",
]


def fetch_metrics_endpoint(prometheus_url: str) -> Optional[str]:
    """
    Fetch raw metrics text from Prometheus /metrics endpoint.

    Args:
        prometheus_url: Base URL of Prometheus server

    Returns:
        Raw metrics text or None if failed
    """
    try:
        response = requests.get(f"{prometheus_url}/metrics", timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching /metrics endpoint: {e}")
        return None


def parse_prometheus_metrics(
    metrics_text: str, target_metrics: List[str]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse Prometheus exposition format text to extract specific metrics.

    Args:
        metrics_text: Raw metrics text from /metrics endpoint
        target_metrics: List of metric names to extract

    Returns:
        Dictionary mapping metric names to list of parsed entries
    """
    parsed_metrics = {}

    for metric_name in target_metrics:
        parsed_metrics[metric_name] = []

        # Regex to match metric lines: metric_name{labels} value
        # Handle both with and without labels
        pattern = rf"^{re.escape(metric_name)}(?:\{{([^}}]*)\}})?\s+([^\s#]+)"

        for line in metrics_text.split("\n"):
            line = line.strip()
            if line.startswith("#") or not line:
                continue

            match = re.match(pattern, line)
            if match:
                labels_str = match.group(1) or ""
                value_str = match.group(2)

                # Parse labels
                labels = {}
                if labels_str:
                    # Parse label pairs: key="value",key2="value2"
                    label_pairs = re.findall(r'(\w+)="([^"]*)"', labels_str)
                    labels = dict(label_pairs)

                # Parse value (handle scientific notation)
                try:
                    value = float(value_str) if value_str != "NaN" else None
                except (ValueError, TypeError):
                    value = None

                parsed_metrics[metric_name].append({"value": value, "labels": labels})

    return parsed_metrics


def process_metric_result(
    metric_name: str, parsed_entries: List[Dict[str, Any]], timestamp: str
) -> List[Dict[str, Any]]:
    """
    Process parsed metric entries into our output format.

    Args:
        metric_name: Name of the metric
        parsed_entries: List of parsed metric entries
        timestamp: ISO timestamp when the query was made

    Returns:
        List of processed metric entries
    """
    if not parsed_entries:
        return [
            {"timestamp": timestamp, "value": None, "labels": {}, "error": "no_data"}
        ]

    processed_entries = []
    for entry in parsed_entries:
        processed_entries.append(
            {"timestamp": timestamp, "value": entry["value"], "labels": entry["labels"]}
        )

    return processed_entries


def append_metrics_to_file(metrics_data: Dict[str, Any], output_file: str) -> None:
    """
    Append metrics data to the output JSON file.

    Args:
        metrics_data: Dictionary containing timestamp and metrics data
        output_file: Path to the output file
    """
    try:
        # Load existing data if file exists
        if os.path.exists(output_file):
            with open(output_file, "r") as f:
                data = json.load(f)
        else:
            data = {
                "collection_start": metrics_data["collection_start"],
                "prometheus_url": metrics_data["prometheus_url"],
                "monitoring_approach": "metrics_endpoint",
                "measurements": [],
            }

        # Add new measurement
        measurement = {
            "timestamp": metrics_data["timestamp"],
            "metrics": metrics_data["metrics"],
        }
        data["measurements"].append(measurement)

        # Keep measurements sorted by timestamp
        data["measurements"].sort(key=lambda x: x["timestamp"])

        # Write back to file
        with open(output_file, "w") as f:
            json.dump(data, f, indent=2)

    except Exception as e:
        print(f"Error appending metrics to file: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Monitor Prometheus throughput metrics via /metrics endpoint"
    )
    parser.add_argument(
        "--prometheus_url",
        default="http://localhost:9090",
        help="Prometheus server URL",
    )
    parser.add_argument(
        "--output_dir", required=True, help="Output directory for metrics"
    )
    parser.add_argument(
        "--interval", type=int, required=True, help="Polling interval in seconds"
    )

    args = parser.parse_args()

    # Ensure Prometheus URL doesn't end with /
    prometheus_url = args.prometheus_url.rstrip("/")

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Output file for all metrics
    output_file = os.path.join(args.output_dir, "prometheus_throughput_metrics.json")

    collection_start = datetime.now().isoformat()
    print("Starting Prometheus throughput monitoring (/metrics endpoint approach)")
    print(f"Prometheus URL: {prometheus_url}")
    print(f"Output file: {output_file}")
    print(f"Metrics to collect: {THROUGHPUT_METRICS}")

    try:
        while True:
            timestamp = datetime.now().isoformat()

            # Fetch raw metrics text
            metrics_text = fetch_metrics_endpoint(prometheus_url)

            # Collect all metrics for this timestamp
            metrics_data = {}

            if metrics_text is None:
                # If /metrics endpoint unavailable, record null values
                for metric_name in THROUGHPUT_METRICS:
                    metrics_data[metric_name] = [
                        {
                            "timestamp": timestamp,
                            "value": None,
                            "labels": {},
                            "error": "metrics_endpoint_unavailable",
                        }
                    ]
            else:
                # Parse metrics from text
                parsed_metrics = parse_prometheus_metrics(
                    metrics_text, THROUGHPUT_METRICS
                )

                for metric_name in THROUGHPUT_METRICS:
                    parsed_entries = parsed_metrics.get(metric_name, [])
                    processed_entries = process_metric_result(
                        metric_name, parsed_entries, timestamp
                    )
                    metrics_data[metric_name] = processed_entries

            # Package the data for storage
            measurement_data = {
                "collection_start": collection_start,
                "prometheus_url": prometheus_url,
                "timestamp": timestamp,
                "metrics": metrics_data,
            }

            # Append to file
            append_metrics_to_file(measurement_data, output_file)

            # Wait for next iteration
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("Monitoring stopped")


if __name__ == "__main__":
    main()
