"""Tests for generating the SQL planner input."""

import unittest

from omegaconf import OmegaConf

from experiment_utils.config import generate_sql_planner_input


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


if __name__ == "__main__":
    unittest.main()
