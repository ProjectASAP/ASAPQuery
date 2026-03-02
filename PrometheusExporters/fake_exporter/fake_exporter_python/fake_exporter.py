import argparse
import itertools
import os
import time
from typing import List

import numpy
import numpy as np
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY, CounterMetricFamily, GaugeMetricFamily
from prometheus_client.registry import Collector


class CustomCollector(Collector):
    def __init__(
        self, scale, dataset, num_labels, num_values_per_label: List[int], metric_type
    ):
        self.scale = scale
        # self.timeseries_id_start = timeseries_id_start
        self.dataset = dataset
        self.rng = numpy.random.default_rng(0)
        self.total_samples = 0
        self.const_1M = 1000000
        self.const_2M = 2000000
        self.const_3M = 3000000

        self.metric_type = metric_type

        self.uniform_counter = 0
        self.dynamic_counter = 0
        self.zipf_counter = 0
        self.normal_counter = 0

        self.num_labels: int = num_labels
        self.labels = [f"label_{i}" for i in range(self.num_labels)]
        self.num_values_per_label: List[int]
        self.values_per_label: List[List[str]] = []
        self.label_value_combinations: List[List[str]] = []

        self.label_value_combinations = self.compute_labels(
            num_labels, num_values_per_label
        )

        # print("values_per_label")
        # [print(sublist) for sublist in self.values_per_label]
        # print("label_value_combinations")
        # [print(sublist) for sublist in self.label_value_combinations]
        # assert False

    def compute_labels(
        self, num_labels: int, num_values_per_label: List[int]
    ) -> List[List[str]]:
        if len(num_values_per_label) == 1:
            self.num_values_per_label = [
                num_values_per_label[0] for _ in range(num_labels)
            ]
        else:
            if len(num_values_per_label) != num_labels:
                raise ValueError(
                    "Number of num_values_per_label must be equal to num_labels"
                )
            self.num_values_per_label = num_values_per_label

        num_timeseries = np.prod(self.num_values_per_label)

        for label_idx in range(self.num_labels):
            values = [
                f"value_{label_idx}_value_{value_idx}"
                for value_idx in range(self.num_values_per_label[label_idx])
            ]
            self.values_per_label.append(values)

        label_value_combinations = list(itertools.product(*self.values_per_label))
        assert len(label_value_combinations) == num_timeseries

        # convert from list[tuple[str]] to list[list[str]]
        rv: List[List[str]] = [
            list(label_value_combination)
            for label_value_combination in label_value_combinations
        ]
        return rv

    def get_uniform_value_gauge(self):
        value = -1
        while value < 0 or value > self.scale:
            # value = numpy.random.uniform() * self.scale
            value = self.rng.uniform(0, self.scale)
        return value

    def get_normal_value_gauge(self):
        value = -1
        while value < 0 or value > self.scale:
            value = self.rng.normal(loc=self.scale / 2, scale=self.scale)
        return value

    def get_zipf_value_gauge(self):
        value = -1
        while value < 0 or value > self.scale:
            # value = numpy.random.zipf(1.01)
            value = self.rng.zipf(1.01)
        return value

    def get_dynamic_value_gauge(self):
        value = -1
        while value < 0 or value > self.scale:
            if self.total_samples < self.const_1M:
                # value = numpy.random.zipf(1.01)
                value = self.rng.zipf(1.01)
            elif self.total_samples < self.const_2M:
                # value = numpy.random.uniform() * self.scale
                value = self.rng.uniform(0, self.scale)
            else:
                value = self.rng.normal(loc=self.scale / 2, scale=self.scale)
        self.total_samples = (self.total_samples + 1) % self.const_3M
        return value

    def get_uniform_value_counter(self):
        value = -1
        while value < 0 or value > self.scale:
            # value = numpy.random.uniform() * self.scale
            value = self.rng.uniform(0, self.scale)
        self.uniform_counter += value
        return self.uniform_counter

    def get_normal_value_counter(self):
        value = -1
        while value < 0 or value > self.scale:
            value = self.rng.normal(loc=self.scale / 2, scale=self.scale)
        self.normal_counter += value
        return self.normal_counter

    def get_zipf_value_counter(self):
        value = -1
        while value < 0 or value > self.scale:
            # value = numpy.random.zipf(1.01)
            value = self.rng.zipf(1.01)
        self.zipf_counter += value
        return self.zipf_counter

    def get_dynamic_value_counter(self):
        value = -1
        while value < 0 or value > self.scale:
            if self.total_samples < self.const_1M:
                # value = numpy.random.zipf(1.01)
                value = self.rng.zipf(1.01)
            elif self.total_samples < self.const_2M:
                # value = numpy.random.uniform() * self.scale
                value = self.rng.uniform(0, self.scale)
            else:
                value = self.rng.normal(loc=self.scale / 2, scale=self.scale)
        self.total_samples = (self.total_samples + 1) % self.const_3M
        self.dynamic_counter += value
        return self.dynamic_counter

    def collect(self):
        if self.metric_type == "counter":
            fake_metric = CounterMetricFamily(
                "fake_metric",
                "Generating fake time series data with {} dataset".format(self.dataset),
                labels=self.labels,
            )
        elif self.metric_type == "gauge":
            fake_metric = GaugeMetricFamily(
                "fake_metric",
                "Generating fake time series data with {} dataset".format(self.dataset),
                labels=self.labels,
            )
        else:
            fake_metric = GaugeMetricFamily(
                "fake_metric",
                "Generating fake time series data with {} dataset".format(self.dataset),
                labels=self.labels,
            )

        for label_value_combination in self.label_value_combinations:
            if self.metric_type == "counter":
                if self.dataset == "uniform":
                    value = self.get_uniform_value_counter()
                elif self.dataset == "normal":
                    value = self.get_normal_value_counter()
                elif self.dataset == "zipf":
                    value = self.get_zipf_value_counter()
                elif self.dataset == "dynamic":
                    value = self.get_dynamic_value_counter()
                else:
                    value = self.get_dynamic_value_counter()
            else:  # gauge
                if self.dataset == "uniform":
                    value = self.get_uniform_value_gauge()
                elif self.dataset == "normal":
                    value = self.get_normal_value_gauge()
                elif self.dataset == "zipf":
                    value = self.get_zipf_value_gauge()
                elif self.dataset == "dynamic":
                    value = self.get_dynamic_value_gauge()
                else:
                    value = self.get_dynamic_value_gauge()

            # labels = [f"label_value_{i}" for d in range(self.num_labels)]
            # fake_metric.add_metric(labels, value=value)
            fake_metric.add_metric(label_value_combination, value)

        yield fake_metric


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)

    metric_collector = CustomCollector(
        args.valuescale,
        args.dataset,
        args.num_labels,
        args.num_values_per_label,
        args.metric_type,
    )
    REGISTRY.register(metric_collector)
    start_http_server(port=args.port)
    print("Fake exporter started on port {}".format(args.port))
    while True:
        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", type=str, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--valuescale", type=int, required=True)
    # parser.add_argument("--start_instanceid", type=int, required=True)
    # parser.add_argument("--batchsize", type=int, required=True)
    parser.add_argument("--dataset", type=str, required=True)
    parser.add_argument("--num_labels", type=int, required=True)
    parser.add_argument("--num_values_per_label", type=str, required=True)
    parser.add_argument("--metric_type", type=str, required=True)
    args = parser.parse_args()

    args.num_values_per_label = [int(i) for i in args.num_values_per_label.split(",")]

    # if (
    #     args.port is None
    #     or args.valuescale is None
    #     or args.start_instanceid is None
    #     or args.batchsize is None
    #     or args.dataset is None
    # ):
    #     print("Fake exporter missing argument")
    #     sys.exit(0)
    main(args)
