import argparse
from http.server import HTTPServer, BaseHTTPRequestHandler
import csv

import logging
from typing import Dict, Optional, List
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()

        metrics = self.server.get_metrics()  # type: ignore
        self.wfile.write(metrics.encode("utf-8"))


# class CSVMetricsExporter:
#     def __init__(self, csv_path, timestamp_column, metric_column):
#         self.df = pd.read_csv(csv_path)
#         print("CSV loaded")
#         self.timestamp_column = timestamp_column
#         self.metric_column = metric_column
#         self.label_columns = [
#             col
#             for col in self.df.columns
#             if col not in [timestamp_column, metric_column]
#         ]

#         # Convert timestamp strings to Unix timestamps (milliseconds)
#         self.df[timestamp_column] = (
#             pd.to_datetime(self.df[timestamp_column], unit="ns").astype(int) // 10**6
#         )
#         self.current_index = 0

#         # Preprocess data by grouping rows by timestamp
#         self.metrics_by_timestamp = {}
#         self.lengths_by_timestamp = {}
#         # grouped = self.df.groupby(timestamp_column)
#         # print("Grouping done")
#         # # TODO: this is horribly slow, fix
#         # for group_idx, (timestamp, group) in enumerate(grouped):
#         #     self.metrics_by_timestamp[timestamp] = self._format_metrics(group)
#         #     self.lengths_by_timestamp[timestamp] = len(group)
#         #     # if group_idx % 500 == 0:
#         #     print(f"Group {group_idx} done")
#         self.metrics_by_timestamp = (
#             self.df.groupby(timestamp_column).apply(self._format_metrics).to_dict()
#         )
#         self.lengths_by_timestamp = self.df[timestamp_column].value_counts().to_dict()
#         print("Metrics done")

#     def _format_metrics(self, group):
#         output = []
#         timestamp = int(group.iloc[0][self.timestamp_column])  # type: ignore
#         metric_name = f"csv_{self.metric_column}"
#         # Add TYPE header only for first occurrence
#         output.append(f"# TYPE {metric_name} gauge")
#         for _, row in group.iterrows():
#             value = row[self.metric_column]
#             labels = ",".join(
#                 [f'{label}="{row[label]}"' for label in self.label_columns]
#             )
#             output.append(f"{metric_name}{{{labels}}} {value} {timestamp}")
#         return "\n".join(output)

#     def get_metrics(self):
#         if self.current_index >= len(self.df):
#             return ""

#         current_timestamp = self.df.iloc[self.current_index][self.timestamp_column]
#         metrics = self.metrics_by_timestamp[current_timestamp]
#         self.current_index += self.lengths_by_timestamp[current_timestamp]
#         return metrics


# class ChunkedCSVReader:
#     def __init__(self, csv_path: str):
#         self.csv_path = csv_path
#         self.file = None
#         self.reader = None
#         self.fieldnames = None

#     def __enter__(self):
#         self.file = open(self.csv_path, "r")
#         self.reader = csv.DictReader(self.file)
#         self.fieldnames = self.reader.fieldnames
#         return self

#     def __exit__(self, exc_type, exc_val, exc_tb):
#         if self.file:
#             self.file.close()


class CSVMetricsExporterNoPandas:
    def __init__(self, csv_path: str, timestamp_column: str, metric_column: str):
        self.csv_path = csv_path
        self.timestamp_column = timestamp_column
        self.metric_column = metric_column
        self.label_columns: List[str] = []
        self.current_timestamp: Optional[int] = None
        self.current_chunk: List[Dict] = []
        self.file_size = Path(csv_path).stat().st_size
        self.bytes_processed = 0

        # Open file and initialize reader
        self.file = open(csv_path, "r")
        self.reader = csv.DictReader(self.file)
        assert self.reader.fieldnames is not None

        # Initialize label columns
        self.label_columns = [
            col
            for col in self.reader.fieldnames
            if col not in [timestamp_column, metric_column]
        ]
        logger.info(f"Initialized with {len(self.label_columns)} label columns")

    def __del__(self):
        """Cleanup when object is destroyed"""
        try:
            if hasattr(self, "file") and self.file:
                self.file.close()
        except Exception as e:
            logger.error(f"Error closing file: {str(e)}")

    def reset_file(self):
        """Reset file to beginning and reinitialize reader"""
        try:
            self.file.seek(0)
            self.reader = csv.DictReader(self.file)
            self.bytes_processed = 0
            self.current_timestamp = None
            self.current_chunk = []
            logger.info("File reset to beginning")
        except Exception as e:
            logger.error(f"Error resetting file: {str(e)}")
            # If there's an error, try to reopen the file
            self.file = open(self.csv_path, "r")
            self.reader = csv.DictReader(self.file)
            self.bytes_processed = 0

    def read_next_chunk(self) -> Optional[List[Dict]]:
        """Read the next chunk of rows with the same millisecond timestamp"""
        if not self.current_chunk:
            try:
                # Read until we find a new timestamp
                for row in self.reader:
                    self.bytes_processed += 1
                    timestamp = int(
                        int(row[self.timestamp_column]) // 10**6
                    )  # ns to ms

                    if self.current_timestamp is None:
                        self.current_timestamp = timestamp
                        self.current_chunk.append(row)
                    elif timestamp == self.current_timestamp:
                        self.current_chunk.append(row)
                    else:
                        # Found a new timestamp, save it for next time
                        self.current_timestamp = timestamp
                        self.current_chunk.append(row)
                        break

                progress = (self.bytes_processed / self.file_size) * 100
                logger.info(f"Progress: {progress:.2f}% processed")

                if self.current_chunk:
                    return self.current_chunk
                else:
                    # End of file reached
                    self.reset_file()
                    return None

            except Exception as e:
                logger.error(f"Error reading chunk: {str(e)}")
                # Try to recover by resetting the file
                self.reset_file()
                return None

        return self.current_chunk

    def _format_metrics(self, group: List[Dict], timestamp: int) -> str:
        """Format metrics in Prometheus exposition format"""
        output = []
        metric_name = f"csv_{self.metric_column}"
        output.append(f"# TYPE {metric_name} gauge")

        for row in group[:10]:
            value = row[self.metric_column]
            labels = ",".join(
                [f'{label}="{row[label]}"' for label in self.label_columns]
            )
            output.append(f"{metric_name}{{{labels}}} {value} {timestamp}")

        return "\n".join(output)

    def get_metrics(self) -> str:
        """Get metrics for the current timestamp chunk"""
        chunk = self.read_next_chunk()
        if not chunk:
            return ""

        metrics = self._format_metrics(chunk, self.current_timestamp or 0)
        # Clear the chunk after processing
        self.current_chunk = []
        return metrics


def run_server(
    port: int, csv_path: str, timestamp_column: str, metric_column: str
) -> None:
    exporter = CSVMetricsExporterNoPandas(csv_path, timestamp_column, metric_column)

    class MetricsServer(HTTPServer):
        def get_metrics(self):
            return exporter.get_metrics()

    server = MetricsServer(("", port), MetricsHandler)
    logger.info(f"Ready to serve metrics on port {port}")
    server.serve_forever()


# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


# class TimeBasedCSVExporter:
#     def __init__(
#         self, csv_path: str, metric_column, timestamp_column, update_interval: int = 60
#     ):
#         self.csv_path = csv_path
#         self.update_interval = update_interval
#         self.metrics: Dict[str, Gauge] = {}
#         self.current_ms: Optional[int] = None
#         self.csv_iterator = None
#         self.metric_column = metric_column
#         self.timestamp_column = timestamp_column

#     def ns_to_ms(self, ns: int) -> int:
#         """Convert nanosecond timestamp to milliseconds"""
#         return ns // 1_000_000

#     def get_next_batch(self) -> Optional[pd.DataFrame]:
#         """Read rows until timestamp changes by 1ms"""
#         if self.csv_iterator is None:
#             # Initialize CSV reader with appropriate timestamp parsing
#             try:
#                 self.csv_iterator = pd.read_csv(
#                     self.csv_path,
#                     iterator=True,
#                     dtype={
#                         self.timestamp_column: "int64"
#                     },  # Ensure timestamp is read as int64
#                 )
#             except Exception as e:
#                 logger.error(f"Error initializing CSV reader: {str(e)}")
#                 raise

#         rows = []
#         try:
#             while True:
#                 # Read one row at a time
#                 row = next(self.csv_iterator)

#                 # Convert timestamp to ms
#                 row_ms = self.ns_to_ms(row[self.timestamp_column].iloc[0])

#                 if self.current_ms is None:
#                     # First batch
#                     self.current_ms = row_ms
#                     rows.append(row)
#                 elif row_ms == self.current_ms:
#                     # Same millisecond, add to batch
#                     rows.append(row)
#                 else:
#                     # New millisecond reached
#                     # Save this row's ms for next batch
#                     self.current_ms = row_ms
#                     # Return concatenated batch
#                     result = pd.concat(rows, ignore_index=True)
#                     # Start new batch with current row
#                     rows = [row]
#                     return result

#         except StopIteration:
#             # End of file reached
#             if rows:
#                 # Return final batch if any rows accumulated
#                 return pd.concat(rows, ignore_index=True)
#             self.csv_iterator = None
#             self.current_ms = None
#             return None
#         except Exception as e:
#             logger.error(f"Error reading batch: {str(e)}")
#             raise

#     def create_metric(self, metric_name: str, label_names: list) -> None:
#         """Create a Prometheus metric if it doesn't exist"""
#         if metric_name not in self.metrics:
#             self.metrics[metric_name] = Gauge(
#                 metric_name, f"Metric imported from CSV: {metric_name}", label_names
#             )

#     def process_batch(self, batch: pd.DataFrame) -> None:
#         """Process a batch of rows with the same millisecond timestamp"""
#         try:
#             # Get label columns (excluding special columns)
#             label_columns = [
#                 col
#                 for col in batch.columns
#                 if col not in [self.timestamp_column, self.metric_column]
#             ]

#             # Process each row in the batch
#             for _, row in batch.iterrows():
#                 metric_name = row[self.metric_column]

#                 # Create metric if it doesn't exist
#                 self.create_metric(metric_name, label_columns)

#                 # Extract labels
#                 labels = {col: row[col] for col in label_columns}

#                 # Update metric value with timestamp in milliseconds
#                 # Set timestamp explicitly using the current batch timestamp
#                 self.metrics[metric_name].labels(**labels).set(row["value"])

#         except Exception as e:
#             logger.error(f"Error processing batch: {str(e)}")
#             raise

#     def update_metrics(self) -> bool:
#         """Read and update metrics from CSV in timestamp-based batches"""
#         try:
#             batch = self.get_next_batch()
#             if batch is not None:
#                 self.process_batch(batch)
#                 # Force garbage collection after batch processing
#                 gc.collect()
#                 logger.info(f"Processed batch for timestamp {self.current_ms}ms")
#                 return True
#             return False

#         except Exception as e:
#             logger.error(f"Error updating metrics: {str(e)}")
#             return False

#     def run(self, port: int = 8000) -> None:
#         """Start the exporter"""
#         start_http_server(port)
#         logger.info(f"Metrics server started on port {port}")

#         while True:
#             start_time = time.time()

#             # Process all available batches
#             while self.update_metrics():
#                 pass

#             # Calculate sleep time
#             elapsed = time.time() - start_time
#             sleep_time = max(0, self.update_interval - elapsed)

#             logger.info(
#                 f"Update cycle took {elapsed:.2f}s, sleeping for {sleep_time:.2f}s"
#             )
#             time.sleep(sleep_time)


def main(args):
    # exporter = TimeBasedCSVExporter(
    #     args.input_file, args.metric_column, args.timestamp_column
    # )
    # exporter.run()

    run_server(
        args.http_port, args.input_file, args.timestamp_column, args.metric_column
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", type=str, required=True)
    parser.add_argument("--timestamp_column", type=str, required=True)
    parser.add_argument("--metric_column", type=str, required=True)
    parser.add_argument("--http_port", default=8000)
    args = parser.parse_args()
    main(args)
