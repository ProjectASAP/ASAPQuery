from loguru import logger
from prometheus_client import Gauge, start_http_server


class QueryLatencyExporter:

    @staticmethod
    def _IP_valid(addr):
        """
        Verifies that a given ip address is of the correct type and is a "valid"
        IP address for running the exporter. At the moment, this function considers
        any properly formatted IP address as valid
        """
        if addr is None:
            raise TypeError("IP address cannot be None")
        elif not isinstance(addr, str):
            raise TypeError("IP address must be a string")
        elif addr == "localhost":
            return

        addr_nums = addr.split(sep=".")
        if len(addr_nums) != 4:
            raise ValueError("Improperly formatted IPv4 address")
        for num_str in addr_nums:
            if int(num_str) < 0 or int(num_str) > 255:
                raise ValueError("Improperly formatted IPv4 address")
        return

    @staticmethod
    def _port_valid(port):
        """
        Verifies that a given ip address is of the correct type and is a "valid"
        IP address for running the exporter. At the moment, this function considers
        any properly formatted IP address as valid
        """
        if port is None:
            raise TypeError("Port cannot be None")
        elif not isinstance(port, int):
            raise TypeError("Port must be an integer")
        elif port < 0 or port > 65535:
            raise ValueError("Improperly formatted port")

        return

    def __init__(self, addr: str, port: int):
        self.logger = logger.bind(module="query_latency_exporter")
        self.port = port
        self.addr = addr

        self.http_server = None
        self.server_thread = None

        try:
            QueryLatencyExporter._IP_valid(self.addr)
            QueryLatencyExporter._port_valid(self.port)
        except (TypeError, ValueError) as e:
            self.logger.error(f"Failed to create QueryLatencyExporter: {str(e)}")
            raise e

        self.latencies_metric = Gauge(
            "query_latencies", "Query latencies", labelnames=["query_index", "server"]
        )
        self.cumulative_latencies_metric = Gauge(
            "cumulative_query_latencies",
            "Query cumulative latencies",
            labelnames=["query_index", "server"],
        )
        self.logger.info("QueryLatencyExporter object created")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown()

    def launch(self):
        """
        Launches the exporter's http_server and server thread for exporting metrics
        to be scraped by Prometheus
        """
        if self.addr is None:
            self.logger.error("Launch failed: Exporter IP address is None")
            raise RuntimeError("Latency exporter failed to launch: exporter IP is None")
        elif self.port is None:
            self.logger.error("Launch failed: Exporter port is None")
            raise RuntimeError(
                "Latency exporter failed to launch: exporter port is None"
            )

        self.logger.info(f"Launching latency exporter at {self.addr}: {self.port}")

        try:
            self.http_server, self.server_thread = start_http_server(
                addr=self.addr, port=self.port
            )
        except Exception as e:
            self.logger.error(f"Failed to start http server due to exception: {str(e)}")
            e.add_note("Latency exporter failed to launch")
            raise e

        self.logger.info(f"Exporter successfully started at {self.addr}: {self.port}")
        print(f"Exporter running at {self.addr}: {self.port}")

        return

    def shutdown(self):
        """
        Cleans up all resources associated with the exporter, mainly the
        http_server and corresponding server thread
        """
        print("Shutting down latency exporter server and joining server thread...")

        self.logger.info("Shutting down server...")
        if self.http_server is not None:
            try:
                self.http_server.shutdown()
            except Exception as e:
                self.logger.error(f"Error shutting down http_server: {str(e)}")
                e.add_note("Attempt to shutdown exporter http_server failed.")
                raise e
            self.logger.info("Shut down server successfully")
        else:
            self.logger.error("Exporter http_server is None")
            raise RuntimeError("Exporter http_server is None")

        self.logger.info("Joining server thread...")
        if self.server_thread is not None:
            try:
                self.server_thread.join()
            except Exception as e:
                self.logger.error(f"Error joining server thread: {str(e)}")
                e.add_note("Attempt to join exporter's server thread failed.")
                raise e
            self.logger.info("Joined server thread successfully")
        else:
            self.logger.error("Exporter server thread is None")
            raise RuntimeError("Exporter server thread is None")

        print("Exporter shut down successfully")
        return

    def export_repetition(self, repetition_idx: int, result):
        """
        Exports a single repetition result for all queries
        """
        if not isinstance(repetition_idx, int):
            self.logger.error("Given non-integer repetition_idx")
            raise TypeError("Repetition index must be an integer")

        self.logger.trace(f"Updating metrics for repetition no.{repetition_idx}")

        if result is None:
            self.logger.error("Repetition result is None")
            raise TypeError("Repetition result is None")

        for server_name in result:
            for query_idx in result[server_name]:
                query_result_across_time = result[server_name][query_idx]
                query_rep_result = query_result_across_time.query_results[
                    repetition_idx
                ]
                latency = query_rep_result.latency
                cumulative_latency = query_rep_result.cumulative_latency

                if latency is not None:
                    self.latencies_metric.labels(
                        query_index=str(query_idx), server=server_name
                    ).set(latency)

                if cumulative_latency is not None:
                    self.cumulative_latencies_metric.labels(
                        query_index=str(query_idx), server=server_name
                    ).set(cumulative_latency)

        return
