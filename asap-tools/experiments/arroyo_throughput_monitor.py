#!/usr/bin/env python3
"""
Standalone Arroyo throughput monitoring script.
Runs on the CloudLab host to monitor Arroyo pipeline metrics.
"""

import argparse
import json
import os
import time
import requests
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(description="Monitor Arroyo pipeline throughput")
    parser.add_argument("--pipeline_id", required=True, help="Pipeline ID to monitor")
    parser.add_argument(
        "--output_dir", required=True, help="Output directory for metrics"
    )
    parser.add_argument(
        "--interval", type=int, default=1, help="Polling interval in seconds"
    )
    parser.add_argument(
        "--api_url", default="http://localhost:5115", help="Arroyo API base URL"
    )

    args = parser.parse_args()

    args.api_url = (
        args.api_url + "/api" if not args.api_url.endswith("/api") else args.api_url
    )

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    metrics_to_collect = ["bytes_recv", "bytes_sent", "messages_recv", "messages_sent"]

    print(f"Starting throughput monitoring for pipeline {args.pipeline_id}")

    # Get job IDs for this pipeline
    job_ids = get_pipeline_job_ids(args.api_url, args.pipeline_id)
    if not job_ids:
        print(f"Warning: No jobs found for pipeline {args.pipeline_id}")
        return

    print(f"Found jobs: {job_ids}")

    # Dump pipeline graph for understanding node indices
    dump_pipeline_graph(args.api_url, args.pipeline_id, args.output_dir)

    # Single file for all timestamps
    output_file = os.path.join(
        args.output_dir, f"throughput_metrics_{args.pipeline_id}.json"
    )

    try:
        while True:
            timestamp = datetime.now().isoformat()
            all_jobs_metrics = {
                "timestamp": timestamp,
                "pipeline_id": args.pipeline_id,
                "jobs": [],
            }

            for job_id in sorted(job_ids):  # Sort job IDs for predictable order
                try:
                    # Get metrics for this job
                    url = f"{args.api_url}/v1/pipelines/{args.pipeline_id}/jobs/{job_id}/operator_metric_groups"
                    response = requests.get(url, timeout=10)
                    response.raise_for_status()

                    metrics_data = response.json()

                    # Process metrics for this job
                    processed_metrics = process_metrics(
                        metrics_data,
                        timestamp,
                        args.pipeline_id,
                        job_id,
                        metrics_to_collect,
                    )
                    all_jobs_metrics["jobs"].append(processed_metrics)

                except Exception as e:
                    print(f"Error collecting metrics for job {job_id}: {e}")
                    # Still add the job with error info
                    all_jobs_metrics["jobs"].append(
                        {
                            "timestamp": timestamp,
                            "pipeline_id": args.pipeline_id,
                            "job_id": job_id,
                            "error": str(e),
                            "metrics": {},
                        }
                    )

            # Append to single file
            append_metrics_to_file(all_jobs_metrics, output_file)

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("Monitoring stopped")


def get_pipeline_job_ids(api_url, pipeline_id):
    """Get all job IDs for the pipeline."""
    try:
        url = f"{api_url}/v1/pipelines/{pipeline_id}/jobs"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        pipeline_data = response.json()
        jobs = pipeline_data.get("data", {})
        job_ids = [job.get("id") for job in jobs if job.get("id")]

        return job_ids

    except Exception as e:
        print(f"Error getting job IDs for pipeline {pipeline_id}: {e}")
        return []


def dump_pipeline_graph(api_url, pipeline_id, output_dir):
    """Dump the pipeline graph to understand node indices."""
    try:
        url = f"{api_url}/v1/pipelines/{pipeline_id}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        pipeline_data = response.json()

        # Create a clean structure for the pipeline graph
        graph_info = {
            "pipeline_id": pipeline_id,
            "pipeline_name": pipeline_data.get("name"),
            "created_at": pipeline_data.get("createdAt"),
            "action": pipeline_data.get("action"),
            "action_in_progress": pipeline_data.get("actionInProgress"),
            "stop": pipeline_data.get("stop"),
            "query": pipeline_data.get("query"),
            "preview": pipeline_data.get("preview"),
            "checkpoint_interval_micros": pipeline_data.get("checkpointIntervalMicros"),
            "nodes": {},
            "edges": [],
        }

        # Extract node information from the graph
        graph = pipeline_data.get("graph", {})
        nodes = graph.get("nodes", [])

        for node in nodes:
            node_id = node.get("nodeId")
            if node_id is not None:
                graph_info["nodes"][str(node_id)] = {
                    "node_id": node_id,
                    "operator": node.get("operator"),
                    "description": node.get("description", ""),
                    "parallelism": node.get("parallelism", 1),
                }

        # Extract edge information to understand data flow
        edges = graph.get("edges", [])
        for edge in edges:
            edge_info = {
                "src_id": edge.get("srcId"),
                "dest_id": edge.get("destId"),
                "edge_type": edge.get("edgeType"),
                "key_type": edge.get("keyType"),
                "value_type": edge.get("valueType"),
            }
            graph_info["edges"].append(edge_info)

        # Extract UDF information if present
        udfs = pipeline_data.get("udfs", [])
        if udfs:
            graph_info["udfs"] = []
            for udf in udfs:
                udf_info = {
                    "definition": udf.get("definition"),
                    "language": udf.get("language"),
                }
                graph_info["udfs"].append(udf_info)

        # Save pipeline graph info
        graph_file = os.path.join(output_dir, f"pipeline_graph_{pipeline_id}.json")
        with open(graph_file, "w") as f:
            json.dump(graph_info, f, indent=2)

        print(f"Pipeline graph saved to {graph_file}")

    except Exception as e:
        print(f"Error dumping pipeline graph: {e}")


def process_metrics(metrics_data, timestamp, pipeline_id, job_id, metrics_to_collect):
    """Process raw metrics data into a structured format."""
    processed = {
        "timestamp": timestamp,
        "pipeline_id": pipeline_id,
        "job_id": job_id,
        "metrics": {},
    }

    try:
        data = metrics_data.get("data", [])

        for node_data in data:
            node_id = node_data.get("nodeId")
            metric_groups = node_data.get("metricGroups", [])

            for metric_group in metric_groups:
                metric_name = metric_group.get("name")

                if metric_name in metrics_to_collect:
                    if metric_name not in processed["metrics"]:
                        processed["metrics"][metric_name] = []

                    subtasks = metric_group.get("subtasks", [])
                    for subtask in subtasks:
                        subtask_index = subtask.get("index")
                        metrics = subtask.get("metrics", [])

                        if metrics:
                            # Get the latest metric value
                            latest_metric = max(metrics, key=lambda m: m.get("time", 0))
                            processed["metrics"][metric_name].append(
                                {
                                    "node_id": node_id,
                                    "subtask_index": subtask_index,
                                    "value": latest_metric.get("value", 0),
                                    "time": latest_metric.get("time", 0),
                                }
                            )

        # Sort metrics within each metric type for predictable order
        for metric_name in processed["metrics"]:
            processed["metrics"][metric_name].sort(
                key=lambda x: (x["node_id"], x["subtask_index"])
            )

    except Exception as e:
        print(f"Error processing metrics: {e}")
        processed["error"] = str(e)

    return processed


def append_metrics_to_file(metrics_entry, output_file):
    """Append metrics entry to a single JSON file containing all timestamps."""
    try:
        # Load existing data if file exists
        if os.path.exists(output_file):
            with open(output_file, "r") as f:
                data = json.load(f)
        else:
            data = {"pipeline_id": metrics_entry["pipeline_id"], "measurements": []}

        # Add new measurement
        data["measurements"].append(metrics_entry)

        # Keep measurements sorted by timestamp
        data["measurements"].sort(key=lambda x: x["timestamp"])

        # Write back to file
        with open(output_file, "w") as f:
            json.dump(data, f, indent=2)

    except Exception as e:
        print(f"Error appending metrics to file: {e}")


if __name__ == "__main__":
    main()
