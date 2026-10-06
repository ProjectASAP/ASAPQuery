"""Tests for generating the SQL planner input."""

import os
import tempfile
import unittest

import yaml
from omegaconf import OmegaConf

from experiment_utils.config import DEFAULT_LATENCY_SLA_MS, generate_sql_planner_input


class ControllerOptionKeysTest(unittest.TestCase):
    def test_unknown_controller_option_is_rejected(self):
        # A stale `latency_sla` used to be dropped here, so the planner ran
        # with no latency limit and never saw the bad key.
        groups = [
            {
                "sql_file": "unused.sql",
                "repetition_delay_ms": 60000,
                "controller_options": {"accuracy_sla": 0.99, "latency_sla": 1},
            }
        ]
        dataset_cfg = OmegaConf.create(
            {
                "name": "t",
                "precompute": {
                    "timestamp_col": "ts",
                    "value_col": "v",
                    "label_cols": ["job"],
                },
            }
        )
        with self.assertRaisesRegex(ValueError, "latency_sla"):
            generate_sql_planner_input(groups, dataset_cfg)


class LatencySlaMsTest(unittest.TestCase):
    def _planner_latency(self, controller_options):
        with tempfile.TemporaryDirectory() as tmp:
            sql_path = os.path.join(tmp, "q.sql")
            with open(sql_path, "w") as f:
                f.write("SELECT 1;")
            groups = [
                {
                    "sql_file": {"baseline": sql_path},
                    "repetition_delay_ms": 60000,
                    "controller_options": controller_options,
                }
            ]
            dataset_cfg = OmegaConf.create(
                {
                    "name": "t",
                    "precompute": {
                        "timestamp_col": "ts",
                        "value_col": "v",
                        "label_cols": ["job"],
                    },
                }
            )
            planner_input = yaml.safe_load(
                generate_sql_planner_input(groups, dataset_cfg)
            )
        return planner_input["query_groups"][0]["controller_options"].get(
            "latency_sla_ms"
        )

    def test_omitted_latency_sla_ms_uses_default(self):
        self.assertEqual(
            self._planner_latency({"accuracy_sla": 0.99}),
            float(DEFAULT_LATENCY_SLA_MS),
        )

    def test_explicit_latency_sla_ms_is_kept(self):
        self.assertEqual(
            self._planner_latency({"accuracy_sla": 0.99, "latency_sla_ms": 250}),
            250.0,
        )

    def test_null_latency_sla_ms_means_no_limit(self):
        self.assertIsNone(
            self._planner_latency({"accuracy_sla": 0.99, "latency_sla_ms": None})
        )


if __name__ == "__main__":
    unittest.main()
