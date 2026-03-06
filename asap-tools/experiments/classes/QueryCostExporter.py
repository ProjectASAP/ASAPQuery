from prometheus_client import start_http_server, Gauge
from loguru import logger
from typing import Dict, List, Tuple
from classes.ProcessMonitorHook import ProcessMonitorHook, ProcessMetricSnapshot
import classes.query_cost as query_cost
from classes.query_cost import CostModelOption, CostModel


class QueryCostExporterHook(ProcessMonitorHook):
    """
    Wrapper class for the QueryCostExporter
    """

    def __init__(
        self,
        monitor_to_models_map: Dict[str, List[CostModelOption]],
        addr: str,
        port: int,
    ):
        self.port = port
        self.addr = addr
        self.monitor_to_models_map = monitor_to_models_map
        self.exporter = None

    def init(self):
        """
        Instantiates the cost exporter and launches it for exporting
        """
        self.exporter = QueryCostExporter(
            self.monitor_to_models_map, self.addr, self.port
        )
        self.exporter.launch()

    def update(self, val):
        """
        Updates exporter metrics using the given value
        """
        if self.exporter is not None:
            self.exporter.export_recent_measurement(val)
        else:
            raise RuntimeError(
                "Exporter is None, remember to call init() before using this hook"
            )

    def close(self):
        """
        Shuts down the cost exporter
        """
        if self.exporter is not None:
            self.exporter.shutdown()
        else:
            raise RuntimeError(
                "Error closing hook, exporter is None. Did you remember to call init()?"
            )


class QueryCostExporter:

    @staticmethod
    def _IP_valid(addr):
        """
        Verifies that a given ip address is of the correct type and is a "valid"
        IP address for running the exporter. At the moment, this function considers
        any properly formatted IP address as valid.
        """
        if not isinstance(addr, str):
            raise TypeError("IP address must be a string")

        if addr == "localhost":
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
        Verifies that a given port is of the correct type and is a "valid"
        port to be used by the exporter. At the moment, this function considers
        any properly formatted port as valid
        """
        if not isinstance(port, int):
            raise TypeError("Port must be an integer")
        if port < 0 or port > 65535:
            raise ValueError("Improperly formatted port")

        return

    @staticmethod
    def _monitor_to_models_map_valid(monitor_to_models_map):
        """
        Verifies that the monitor_to_models_map given during object creation
        is valid, e.g. is a dictionary with valid keys and values
        """
        # Check map itself (Correct type, non-empty)
        if monitor_to_models_map is None:
            raise TypeError("Monitor to cost models map is None.")
        elif not isinstance(monitor_to_models_map, dict):
            raise TypeError("Monitor to cost models map must be a dictionary.")
        elif len(monitor_to_models_map) == 0:
            raise ValueError("Monitor to cost models map must not be empty.")

        # Check key-value pairs (Correct types, each monitor has at least one cost model)
        for monitor in monitor_to_models_map:
            if not isinstance(monitor, str):
                raise TypeError("Monitor names in the map must be given as strings.")

            cost_models = monitor_to_models_map[monitor]

            if cost_models is None:
                raise TypeError(f"Cost model list for {monitor} is None.")
            elif not isinstance(cost_models, list):
                raise TypeError(
                    f"Cost models for {monitor} must be given as a list of CostModelOption."
                )
            elif len(cost_models) == 0:
                raise ValueError(f"Cost model list for {monitor} is empty")

            for model in cost_models:
                if not isinstance(model, type(CostModelOption.NO_TRANSFORM)):
                    raise TypeError(
                        f"List of cost models for {monitor} contains one or more element that is not a CostModelOption"
                    )

    # NOTE: Implementation only uses prometheus Gauges
    @staticmethod
    def _create_prom_metric(
        monitor_metric_name: str, cost_model: CostModelOption, metric_labels: List[str]
    ) -> Gauge:
        """
        Creates a single prometheus metric for a single monitor (e.g. cpu_percent) and
        one of the cost functions applied to it. The name of the metric as seen by prometheus
        will be "<monitor_metric_name>_<cost model enumeration name>", e.g.
        "cpu_percent_NO_TRANSFORM"
        """
        prom_metric_name = "{}_{}".format(monitor_metric_name, cost_model.name)
        prom_description = "{}({})".format(cost_model.name, monitor_metric_name)

        return Gauge(prom_metric_name, prom_description, metric_labels)

    # NOTE Only uses prometheus gauges for metrics at the moment
    @staticmethod
    def _init_prom_metrics(
        monitor_to_models_map,
    ) -> Dict[str, List[Tuple[CostModel, Gauge]]]:
        """
        Creates a dictionary which maps the name of a monitor to a list of tuples,
        where each tuple contains a cost model object as the first element
        and the corresponding prometheus metric as the second element,
        e.g. Dict = {"cpu_percent": [(cost_model, Gauge), ...]}
        """
        prometheus_metrics = {}

        for monitor_metric in monitor_to_models_map:
            models_and_prom_metrics = []
            for cost_model_option in monitor_to_models_map[monitor_metric]:
                cost_model = query_cost.create_model(cost_model_option)
                prom_metric = QueryCostExporter._create_prom_metric(
                    monitor_metric, cost_model_option, ["keyword", "PID"]
                )
                model_and_prom_metric = (cost_model, prom_metric)
                models_and_prom_metrics.append(model_and_prom_metric)

            prometheus_metrics[monitor_metric] = models_and_prom_metrics

        return prometheus_metrics

    def __init__(
        self,
        monitor_to_models_map: Dict[str, List[CostModelOption]],
        addr: str,
        port: int,
    ):
        self.logger = logger.bind(module="query_cost_exporter")

        self.port = port
        self.addr = addr
        self.monitor_to_models_map = monitor_to_models_map

        self.http_server = None
        self.server_thread = None

        # Verify input parameters
        try:
            QueryCostExporter._IP_valid(self.addr)
            QueryCostExporter._port_valid(self.port)
            QueryCostExporter._monitor_to_models_map_valid(self.monitor_to_models_map)
        except (TypeError, ValueError) as e:
            self.logger.error(f"Failed to create QueryCostExporter: {str(e)}")
            e.add_note("Failed to create QueryCostExporter object")
            raise e

        self.prometheus_metrics_map = QueryCostExporter._init_prom_metrics(
            self.monitor_to_models_map
        )
        self.logger.info("QueryCostExporter object created")

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
            raise RuntimeError("Cost exporter failed to launch: exporter IP is None")

        if self.port is None:
            self.logger.error("Launch failed: Exporter port is None")
            raise RuntimeError("Cost exporter failed to launch: exporter port is None")

        self.logger.info(f"Launching cost exporter at {self.addr}:{self.port}...")

        try:
            self.http_server, self.server_thread = start_http_server(
                addr=self.addr, port=self.port
            )
        except Exception as e:
            self.logger.error(f"Failed to start http server due to exception: {str(e)}")
            e.add_note("Cost exporter failed to launch")
            raise e

        self.logger.info(f"Exporter successfully started at {self.addr}:{self.port}")
        print(f"Exporter running at {self.addr}:{self.port}")

        return

    def shutdown(self):
        """
        Cleans up all resources associated with the exporter, mainly the
        http_server and corresponding server thread
        """
        print("Shutting down cost exporter server and joining server thread...")

        self.logger.info("Shutting down server...")
        if self.http_server is not None:
            try:
                self.http_server.shutdown()
            except Exception as e:
                self.logger.error(f"Error shutting down http_server: {str(e)}")
                e.add_note("Attempt to shutdown cost exporter http_server failed.")
                raise e
            self.logger.info("Shut down server successfully")
        else:
            self.logger.error("Exporter http_server is None")
            raise RuntimeError("Cost exporter http_server is None")

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
            raise RuntimeError("Cost exporter server thread is None")

        print("Exporter shut down successfully")
        return

    # NOTE: This function is blocking. Exporting the new information requires
    #       the calling thread to perform all cost modelling calculations,
    #       so be wary when using cost models which take substantial time to
    #       compute
    def export_recent_measurement(self, iteration_info: List[ProcessMetricSnapshot]):
        """
        Takes a list of snapshots for every process and monitor from the most
        recent iteration in process_monitor
        """
        if iteration_info is None:
            raise TypeError("Failed to export iteration, iteration_info is None")
        elif not isinstance(iteration_info, list):
            raise TypeError("iteration_info must be a list of ProcessMetricSnapshots")

        for snapshot in iteration_info:
            self.export_snapshot(snapshot)

    # NOTE: Function logic currently assumes all prometheus metrics are Gauges
    # NOTE: This function is blocking. Since this function makes the necessary
    #       calls to compute costs, beware of cost models which take a while to
    #       compute
    def export_snapshot(self, snapshot: ProcessMetricSnapshot):
        """
        Updates all prometheus metrics corresponding to the given monitor. The
        function applies the corresponding cost function to the given value
        before exporting
        """
        if snapshot is None:
            self.logger.error("Exporter given None snapshot")
            raise TypeError("Attempt to export a None snapshot")
        elif not isinstance(snapshot, ProcessMetricSnapshot):
            self.logger.error("Wrong argument")
            raise TypeError(
                "export_snapshot() argument must be a ProcessMetricSnapshot"
            )

        pid = snapshot.pid
        keyword = snapshot.keyword
        monitor_name = snapshot.monitor_name
        measurement = snapshot.value
        self.logger.trace(
            f"Updating for pid={pid}, keyword={keyword}, monitor_name={monitor_name}, measurement={measurement}"
        )

        if monitor_name in self.prometheus_metrics_map:
            metric_list = self.prometheus_metrics_map[monitor_name]
            for cost_model, prometheus_metric in metric_list:
                # NOTE: For a computation like a sum, the cost is being computed
                #       using every measurement, i.e. across all PIDs and keywords,
                #       so PID and keyword labels are meaningless in these cases.
                cost = cost_model.compute(measurement)
                if cost is not None and prometheus_metric is not None:
                    prometheus_metric.labels(keyword=keyword, PID=pid).set(cost)

        return
