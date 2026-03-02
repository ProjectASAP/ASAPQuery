#!/usr/bin/env python3
"""
Prometheus target health monitoring script using /api/v1/targets endpoint.

This script monitors the health and scrape performance of all Prometheus targets.
It queries the /api/v1/targets API endpoint to collect:
- Target up/down status
- Last scrape duration
- Last scrape errors
- Scrape URLs
- Target labels (job, instance, etc.)

This is particularly useful for high-cardinality, low-scrape-interval experiments
to ensure Prometheus is successfully scraping all configured targets without missing
any due to performance constraints.

Related to issues #97 and #108.
"""

import argparse
import json
import os
import time
import requests
from datetime import datetime
from typing import Dict, Any, Optional


def fetch_targets_api(prometheus_url: str) -> Optional[Dict[str, Any]]:
    """
    Fetch target health information from Prometheus /api/v1/targets endpoint.

    Args:
        prometheus_url: Base URL of Prometheus server

    Returns:
        JSON response containing activeTargets and droppedTargets, or None if failed
    """
    try:
        response = requests.get(f"{prometheus_url}/api/v1/targets", timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "success":
            print(f"API returned non-success status: {data}")
            return None

        return data.get("data", {})
    except Exception as e:
        print(f"Error fetching /api/v1/targets endpoint: {e}")
        return None


def process_targets_data(
    targets_data: Optional[Dict[str, Any]], timestamp: str
) -> Dict[str, Any]:
    """
    Process targets API response into a structured format for monitoring.

    Args:
        targets_data: Response from /api/v1/targets endpoint
        timestamp: ISO timestamp when the query was made

    Returns:
        Dictionary containing processed target information
    """
    if targets_data is None:
        return {
            "timestamp": timestamp,
            "active_targets": [],
            "dropped_targets": [],
            "error": "api_unavailable",
        }

    active_targets = []
    for target in targets_data.get("activeTargets", []):
        # Extract labels
        labels = target.get("labels", {})

        # Extract health information
        health = target.get("health", "unknown")
        last_error = target.get("lastError", "")
        scrape_url = target.get("scrapeUrl", "")

        # Extract scrape performance metrics
        # lastScrapeDuration is in seconds (e.g., "0.003994474s")
        if "lastScrapeDuration" in target:
            last_scrape_duration = float(target["lastScrapeDuration"])
        else:
            last_scrape_duration = None

        # Last scrape timestamp
        last_scrape = target.get("lastScrape", "")

        # Discovered labels (before relabeling)
        discovered_labels = target.get("discoveredLabels", {})

        active_targets.append(
            {
                "labels": labels,
                "health": health,
                "scrape_url": scrape_url,
                "last_scrape": last_scrape,
                "last_scrape_duration": last_scrape_duration,
                "last_error": last_error,
                "discovered_labels": discovered_labels,
            }
        )

    # Process dropped targets (targets that were discovered but filtered out)
    dropped_targets = []
    for target in targets_data.get("droppedTargets", []):
        discovered_labels = target.get("discoveredLabels", {})
        dropped_targets.append(
            {
                "discovered_labels": discovered_labels,
            }
        )

    return {
        "timestamp": timestamp,
        "active_targets": active_targets,
        "dropped_targets": dropped_targets,
        "active_count": len(active_targets),
        "dropped_count": len(dropped_targets),
    }


def append_health_data_to_file(health_data: Dict[str, Any], output_file: str) -> None:
    """
    Append health monitoring data to the output JSON file.

    Args:
        health_data: Dictionary containing timestamp and target health data
        output_file: Path to the output file
    """
    try:
        # Load existing data if file exists
        if os.path.exists(output_file):
            with open(output_file, "r") as f:
                data = json.load(f)
        else:
            data = {
                "collection_start": health_data["collection_start"],
                "prometheus_url": health_data["prometheus_url"],
                "monitoring_type": "target_health",
                "measurements": [],
            }

        # Add new measurement
        measurement = {
            "timestamp": health_data["timestamp"],
            "active_targets": health_data["active_targets"],
            "dropped_targets": health_data["dropped_targets"],
            "active_count": health_data["active_count"],
            "dropped_count": health_data["dropped_count"],
        }

        # Add error field if present
        if "error" in health_data:
            measurement["error"] = health_data["error"]

        data["measurements"].append(measurement)

        # Keep measurements sorted by timestamp
        data["measurements"].sort(key=lambda x: x["timestamp"])

        # Write back to file
        with open(output_file, "w") as f:
            json.dump(data, f, indent=2)

    except Exception as e:
        print(f"Error appending health data to file: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Monitor Prometheus target health via /api/v1/targets endpoint"
    )
    parser.add_argument(
        "--prometheus_url",
        required=True,
        help="Prometheus server URL",
    )
    parser.add_argument(
        "--output_dir",
        required=True,
        help="Output directory for health monitoring data",
    )
    parser.add_argument(
        "--interval", type=int, default=5, help="Polling interval in seconds"
    )

    args = parser.parse_args()

    # Ensure Prometheus URL doesn't end with /
    prometheus_url = args.prometheus_url.rstrip("/")

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Output file for health monitoring data
    output_file = os.path.join(args.output_dir, "prometheus_target_health.json")

    collection_start = datetime.now().isoformat()
    print("Starting Prometheus target health monitoring (/api/v1/targets approach)")
    print(f"Prometheus URL: {prometheus_url}")
    print(f"Output file: {output_file}")
    print(f"Polling interval: {args.interval}s")

    try:
        while True:
            timestamp = datetime.now().isoformat()

            # Fetch target health data from API
            targets_data = fetch_targets_api(prometheus_url)

            # Process the target data
            processed_data = process_targets_data(targets_data, timestamp)

            # Add metadata for storage
            health_data = {
                "collection_start": collection_start,
                "prometheus_url": prometheus_url,
                **processed_data,
            }

            # Append to file
            append_health_data_to_file(health_data, output_file)

            # Print summary
            if "error" not in processed_data:
                print(
                    f"[{timestamp}] Active targets: {processed_data['active_count']}, "
                    f"Dropped targets: {processed_data['dropped_count']}"
                )

                # Count unhealthy targets
                unhealthy = [
                    t for t in processed_data["active_targets"] if t["health"] != "up"
                ]
                if unhealthy:
                    print(f"  WARNING: {len(unhealthy)} unhealthy targets!")
                    for target in unhealthy:
                        print(
                            f"    - {target['labels'].get('job', 'unknown')}/"
                            f"{target['labels'].get('instance', 'unknown')}: "
                            f"{target['health']} - {target['last_error']}"
                        )

            # Wait for next iteration
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nMonitoring stopped")


if __name__ == "__main__":
    main()
