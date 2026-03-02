import json
import math
import argparse
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import os

import pyarrow.parquet as pq
from pyarrow import Table


class PrometheusExporter:
    def __init__(self, base_url="http://localhost:9090", max_workers=4):
        """Initialize the Prometheus exporter with the base URL of your Prometheus instance"""
        self.base_url = base_url.rstrip("/")
        self.api_endpoint = f"{self.base_url}/api/v1"
        self.max_workers = max_workers

        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def get_all_metric_names(self):
        """Get all available metric names from Prometheus"""
        response = requests.get(f"{self.api_endpoint}/label/__name__/values")
        response.raise_for_status()
        return response.json()["data"]

    def get_series_metadata(self, metric_name):
        """Get metadata for a specific metric series"""
        response = requests.get(
            f"{self.api_endpoint}/series", params={"match[]": metric_name}
        )
        response.raise_for_status()
        return response.json()["data"]

    def get_metric_start_time(self, metric_name):
        """Get the earliest timestamp for a metric"""
        query = f"first_over_time({metric_name}[10y])"
        response = requests.get(f"{self.api_endpoint}/query", params={"query": query})
        response.raise_for_status()
        data = response.json()["data"]["result"]
        if data:
            return min(result["value"][0] for result in data)
        return None

    def get_record_key(self, record):
        return json.dumps(record, sort_keys=True)

    def export_metric_data(self, metric_name, start_time, end_time, chunk_size=3600):
        """Export data for a specific metric with chunking for large datasets, using a generator to reduce memory usage"""
        seen_records = set()
        current_start = start_time

        while current_start < end_time:
            current_end = min(current_start + chunk_size, end_time)
            current_chunk_size = math.ceil(current_end - current_start)

            query = f"{metric_name}[{current_chunk_size}s]"
            params = {"query": query, "time": current_end}

            try:
                response = requests.get(f"{self.api_endpoint}/query", params=params)
                response.raise_for_status()
                data = response.json()["data"]["result"]

                for series in data:
                    metric_labels = series["metric"]
                    values = series["values"] if "values" in series else []

                    for value in values:
                        record = {
                            "timestamp": value[0],
                            "value": float(value[1]) if value[1] != "NaN" else None,
                            "metric_name": metric_name,
                            **metric_labels,
                        }

                        record_key = self.get_record_key(record)

                        if record_key not in seen_records:
                            seen_records.add(record_key)
                            yield record  # Yield records one by one instead of accumulating

            except Exception as e:
                self.logger.error(f"Error fetching data for {metric_name}: {e}")

            current_start = current_end

    def export_all_metrics(self, output_dir, start_time, end_time, metrics, formats):
        """Export all available metrics to separate files using streaming to reduce memory usage"""

        start_timestamp = start_time.timestamp()
        end_timestamp = end_time.timestamp()

        # Get all metric names
        metric_names = self.get_all_metric_names()
        metric_names = [name for name in metric_names if name in metrics]
        self.logger.info(f"Found {len(metric_names)} metrics to export")

        def export_metric(metric_name):
            try:
                safe_name = metric_name.replace(":", "_").replace("/", "_")

                # Define output paths based on selected formats
                paths = {}
                if "csv" in formats:
                    paths["csv"] = f"{output_dir}/{safe_name}.csv"
                if "json" in formats:
                    paths["json"] = f"{output_dir}/{safe_name}.json"
                if "parquet" in formats:
                    paths["parquet"] = f"{output_dir}/{safe_name}.parquet"

                record_count = 0
                first_batch = True
                parquet_writer = None
                parquet_schema = None

                # Stream records directly to files
                for batch in self._get_batched_records(
                    metric_name, start_timestamp, end_timestamp
                ):
                    if not batch:
                        continue

                    df = pd.DataFrame(batch)

                    # Handle CSV export
                    if "csv" in formats:
                        if first_batch:
                            df.to_csv(paths["csv"], index=False, mode="w")
                        else:
                            df.to_csv(paths["csv"], index=False, mode="a", header=False)

                    # Handle JSON export
                    if "json" in formats:
                        if first_batch:
                            # Initialize JSON file with array opening and first batch
                            with open(paths["json"], "w") as f:
                                f.write("[\n")
                                f.write(
                                    ",\n".join(json.dumps(record) for record in batch)
                                )
                        else:
                            # Append to JSON with proper formatting
                            with open(paths["json"], "a") as f:
                                f.write(",\n")
                                f.write(
                                    ",\n".join(json.dumps(record) for record in batch)
                                )

                    # Handle Parquet export incrementally
                    if "parquet" in formats:
                        table = Table.from_pandas(df)
                        if first_batch:
                            parquet_schema = table.schema
                            parquet_writer = pq.ParquetWriter(
                                paths["parquet"], parquet_schema, compression="snappy"
                            )
                        else:
                            if table.schema != parquet_schema:
                                raise ValueError(
                                    f"Schema mismatch for {metric_name}: "
                                    f"expected {parquet_schema}, got {table.schema}"
                                )
                        assert (
                            parquet_writer is not None
                        ), "Parquet writer should be initialized"
                        parquet_writer.write_table(table)

                    record_count += len(batch)
                    first_batch = False

                # Close the JSON array if any records were written
                if record_count > 0:
                    if "json" in formats:
                        with open(paths["json"], "a") as f:
                            f.write("\n]")

                    # Close the parquet writer
                    if "parquet" in formats and parquet_writer:
                        parquet_writer.close()

                    self.logger.info(f"Exported {metric_name} ({record_count} records)")

                return record_count
            except Exception as e:
                self.logger.error(f"Failed to export {metric_name}: {e}")
                return 0

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = list(executor.map(export_metric, metric_names))

        total_records = sum(results)
        self.logger.info(f"Export complete. Total records: {total_records}")

        # Create export summary
        summary = {
            "export_time": datetime.now().isoformat(),
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "total_metrics": len(metric_names),
            "total_records": total_records,
            "prometheus_url": self.base_url,
            "export_formats": formats,
        }

        with open(f"{output_dir}/export_summary.json", "w") as f:
            json.dump(summary, f, indent=2)

    def _get_batched_records(
        self, metric_name, start_timestamp, end_timestamp, batch_size=1000
    ):
        """Helper method to batch records from the generator for efficient CSV writing"""
        batch = []
        for record in self.export_metric_data(
            metric_name, start_timestamp, end_timestamp
        ):
            batch.append(record)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:  # Don't forget the last batch
            yield batch


def main(args):
    # Configuration
    START_TIME = datetime.now() - timedelta(days=7)  # Last 7 days
    END_TIME = datetime.now()

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    # Initialize exporter
    exporter = PrometheusExporter(args.url)

    # Start export
    print(f"Starting export from {START_TIME} to {END_TIME}")
    print(f"Output directory: {args.output_dir}")
    print(f"Export formats: {args.formats}")

    try:
        exporter.export_all_metrics(
            output_dir=args.output_dir,
            start_time=START_TIME,
            end_time=END_TIME,
            metrics=args.metric_names,
            formats=args.formats,
        )
        print("Export completed successfully!")

    except Exception as e:
        print(f"Error during export: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export Prometheus metrics to CSV and JSON files"
    )
    parser.add_argument(
        "--url", default="http://localhost:9090", help="URL of the Prometheus server"
    )
    parser.add_argument(
        "--output_dir", required=True, help="Output directory for exported files"
    )
    parser.add_argument(
        "--metric_names",
        type=str,
        required=False,
        help="Comma-separated list of metrics to export",
    )
    parser.add_argument(
        "--formats",
        type=str,
        required=True,
        help="Comma-separated list of export formats (csv,json,parquet)",
    )
    args = parser.parse_args()
    if args.metric_names:
        args.metric_names = args.metric_names.split(",")
    args.formats = args.formats.split(",")
    args.formats = [fmt.strip().lower() for fmt in args.formats]
    if "parquet" in args.formats:
        raise NotImplementedError(
            "Parquet export is not tested yet. Please use csv and/or json."
        )
    main(args)
