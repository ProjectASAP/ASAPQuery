"""Tests for the planner input written from experiment parameters."""

import os
import tempfile
import unittest

import yaml
from omegaconf import OmegaConf

from experiment_utils.config import generate_controller_client_configs


def _controller_input(experiment_params):
    with tempfile.TemporaryDirectory() as tmp:
        generate_controller_client_configs(OmegaConf.create(experiment_params), tmp)
        path = os.path.join(
            tmp, "controller_client_configs", "sketchdb_controller_input.yaml"
        )
        with open(path) as f:
            return yaml.safe_load(f)


BASE = {
    "servers": [{"name": "sketchdb", "url": "http://localhost:8088"}],
    "experiment": [{"mode": "sketchdb", "server": "sketchdb"}],
    "metrics": [{"metric": "m", "labels": ["job"], "exporter": "fake"}],
}


class ControllerInputTest(unittest.TestCase):
    def test_client_options_are_stripped_from_query_groups(self):
        group = {
            "id": 1,
            "queries": ["sum(m)"],
            "repetition_delay_ms": 60000,
            "client_options": {"repetitions": 1},
            "controller_options": {"accuracy_sla": 0.99},
        }
        planner_input = _controller_input({**BASE, "query_groups": [group]})
        self.assertNotIn("client_options", planner_input["query_groups"][0])
        self.assertIn("controller_options", planner_input["query_groups"][0])

    def test_missing_query_groups_stays_missing(self):
        # An empty list would parse and plan nothing; a missing key fails loudly.
        self.assertNotIn("query_groups", _controller_input(BASE))


if __name__ == "__main__":
    unittest.main()
