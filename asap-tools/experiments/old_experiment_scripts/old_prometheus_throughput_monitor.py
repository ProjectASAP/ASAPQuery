#!/usr/bin/env python3
"""
Standalone Prometheus throughput monitoring script.
Runs on the CloudLab host to monitor Prometheus throughput metrics.
"""

import argparse
import json
import os
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
]


def query_prometheus_metric(api_url: str, metric_name: str) -> Optional[Dict[str, Any]]:
    """
    Query a single metric from Prometheus API.

    Args:
        api_url: Prometheus API base URL
        metric_name: Name of the metric to query

    Returns:
        Dictionary with metric data or None if failed
    """
    try:
        response = requests.get(
            f"{api_url}/query", params={"query": metric_name}, timeout=10
        )
        response.raise_for_status()
        data = response.json()

        if data["status"] != "success":
            print(f"Warning: Prometheus query failed for {metric_name}: {data}")
            return None

        return data["data"]
    except Exception as e:
        print(f"Error querying metric {metric_name}: {e}")
        return None


def process_metric_result(
    metric_name: str, result_data: Optional[Dict[str, Any]], timestamp: str
) -> List[Dict[str, Any]]:
    """
    Process the result from a Prometheus query into our format.

    Args:
        metric_name: Name of the metric
        result_data: Raw result from Prometheus API
        timestamp: ISO timestamp when the query was made

    Returns:
        List of processed metric entries
    """
    if result_data is None:
        return [
            {
                "timestamp": timestamp,
                "value": None,
                "labels": {},
                "error": "query_failed",
            }
        ]

    processed_entries = []
    result_list = result_data.get("result", [])

    if not result_list:
        # No data returned for this metric
        processed_entries.append(
            {"timestamp": timestamp, "value": None, "labels": {}, "error": "no_data"}
        )
    else:
        for series in result_list:
            metric_labels = series.get("metric", {})
            value_data = series.get("value", [])

            if len(value_data) >= 2:
                # value_data is [timestamp, value_string]
                try:
                    value = float(value_data[1]) if value_data[1] != "NaN" else None
                except (ValueError, TypeError):
                    value = None
            else:
                value = None

            processed_entries.append(
                {"timestamp": timestamp, "value": value, "labels": metric_labels}
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
        description="Monitor Prometheus throughput metrics"
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
        "--interval", type=int, default=1, help="Polling interval in seconds"
    )

    args = parser.parse_args()

    # Ensure Prometheus URL doesn't end with /
    prometheus_url = args.prometheus_url.rstrip("/")
    api_url = f"{prometheus_url}/api/v1"

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Output file for all metrics
    output_file = os.path.join(args.output_dir, "prometheus_throughput_metrics.json")

    collection_start = datetime.now().isoformat()
    print("Starting Prometheus throughput monitoring")
    print(f"Prometheus URL: {prometheus_url}")
    print(f"Output file: {output_file}")
    print(f"Metrics to collect: {THROUGHPUT_METRICS}")

    try:
        while True:
            timestamp = datetime.now().isoformat()

            # Collect all metrics for this timestamp
            metrics_data = {}

            for metric_name in THROUGHPUT_METRICS:
                result_data = query_prometheus_metric(api_url, metric_name)
                processed_entries = process_metric_result(
                    metric_name, result_data, timestamp
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
