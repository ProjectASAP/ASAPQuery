import json
import requests
import argparse
import humanize
import numpy as np

BYTES_PER_SAMPLE = 1


def scrape_prometheus_metrics(prometheus_url):
    response = requests.get(f"{prometheus_url}/metrics")
    if response.status_code == 200:
        return response.text
    return None


def get_prometheus_metrics(scraped_text, metric_names):
    result = {}
    for line in scraped_text.split("\n"):
        for metric in metric_names:
            if line.startswith(f"{metric} "):
                value = line.split(" ")[1]
                value = float(value)
                result[metric] = value
    return result


def get_prometheus_storage_metrics(prometheus_url):
    metrics = [
        "prometheus_tsdb_storage_blocks_bytes",
        "prometheus_tsdb_wal_storage_size_bytes",
        "prometheus_remote_storage_samples_in_total",
    ]
    result = {}
    response = requests.get(f"{prometheus_url}/metrics")
    if response.status_code == 200:
        for line in response.text.split("\n"):
            for metric in metrics:
                if line.startswith(f"{metric} "):
                    value = line.split(" ")[1]
                    if "e" in value:
                        value = float(value)
                    else:
                        value = int(value)
                    result[metric] = value
    return result


# Function to parse Prometheus query log file and calculate total query evaluation time
def calculate_total_evaluation_time(log_file_path):
    total_time_ms = 0

    try:
        with open(log_file_path, "r") as log_file:
            for line in log_file:
                line = json.loads(line)
                total_time_ms += line["stats"]["timings"]["execTotalTime"]
        return total_time_ms
    except FileNotFoundError:
        print(f"Log file {log_file_path} not found.")
        return None


def get_ingestion_cost_per_tier(num_samples, tier_num_samples, tier_cost_per_sample):
    if num_samples <= tier_num_samples:
        return num_samples * tier_cost_per_sample, 0
    else:
        return tier_num_samples * tier_cost_per_sample, num_samples - tier_num_samples


def get_ingestion_cost(num_samples):
    cost = 0

    tier_cost, num_samples = get_ingestion_cost_per_tier(num_samples, 2e9, 0.9 / 10e6)
    cost += tier_cost
    if num_samples == 0:
        return cost
    tier_cost, num_samples = get_ingestion_cost_per_tier(
        num_samples, 250e9, 0.35 / 10e6
    )
    cost += tier_cost
    if num_samples == 0:
        return cost
    tier_cost, num_samples = get_ingestion_cost_per_tier(
        num_samples, np.inf, 0.16 / 10e6
    )
    cost += tier_cost

    return cost


def get_query_cost(query_samples_processed):
    return query_samples_processed * 0.1 / 1e9


def get_storage_cost(total_samples, retention_days):
    total_bytes = total_samples * BYTES_PER_SAMPLE
    return 0.03 * total_bytes / 1e9 * retention_days / 30


def get_dollar_cost(storage_values, query_values, retention_days):
    ingestion_cost = get_ingestion_cost(
        storage_values["prometheus_remote_storage_samples_in_total"]
    )
    query_cost = get_query_cost(query_values["prometheus_engine_query_samples_total"])
    storage_cost = get_storage_cost(
        storage_values["prometheus_remote_storage_samples_in_total"], retention_days
    )

    print("Ingestion cost:", ingestion_cost)
    print("Query cost:", query_cost)
    print("Storage cost:", storage_cost)

    return ingestion_cost + query_cost + storage_cost


def get_resource_cost(storage_values, query_values, retention_days):
    result = {}
    result["query_cpu_seconds"] = query_values[
        "prometheus_rule_group_duration_seconds_sum"
    ]
    return result


def print_metrics(metric_type, metrics):
    print(f"{metric_type} metrics:")
    for metric, value in metrics.items():
        if "byte" in metric:
            print(f"{metric}: {humanize.naturalsize(value, gnu=True)}")
        else:
            print(f"{metric}: {humanize.intcomma(value)}")


def main(args):
    scraped_prometheus_metrics = scrape_prometheus_metrics(args.prometheus_url)

    storage_metrics = [
        "prometheus_tsdb_storage_blocks_bytes",
        "prometheus_tsdb_wal_storage_size_bytes",
        "prometheus_remote_storage_samples_in_total",
    ]
    query_metrics = [
        "prometheus_engine_query_samples_total",
        "prometheus_rule_group_duration_seconds_sum",
    ]

    storage_values = get_prometheus_metrics(scraped_prometheus_metrics, storage_metrics)
    query_values = get_prometheus_metrics(scraped_prometheus_metrics, query_metrics)
    query_values["total_exec_time_from_query_log"] = calculate_total_evaluation_time(
        args.query_log_file
    )

    print_metrics("Storage", storage_values)
    print_metrics("Query", query_values)

    dollar_cost = get_dollar_cost(
        storage_values, query_values, retention_days=args.retention_days
    )
    resource_cost = get_resource_cost(
        storage_values, query_values, retention_days=args.retention_days
    )

    print(f"Dollar cost: {dollar_cost}")
    print(f"Resource cost: {resource_cost}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate cost based on Prometheus metrics and query log."
    )
    parser.add_argument(
        "--prometheus_url",
        help="URL of the Prometheus server",
        default="http://localhost:9090",
    )
    parser.add_argument(
        "--query_log_file", help="Path to the Prometheus query log file", required=True
    )
    parser.add_argument(
        "--retention_days",
        help="Number of days to retain data in Prometheus",
        required=True,
        type=int,
    )
    args = parser.parse_args()
    main(args)
