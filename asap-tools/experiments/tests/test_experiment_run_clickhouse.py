"""Regression tests for ClickHouse experiment controller wiring."""

import unittest
from unittest.mock import Mock

from omegaconf import OmegaConf

from experiment_run_clickhouse import _start_clickhouse_controller


class ClickHouseControllerConfigTest(unittest.TestCase):
    def test_controller_receives_configured_punting_setting(self):
        controller_service = Mock()
        cfg = OmegaConf.create({"controller": {"punting": True}})

        _start_clickhouse_controller(
            controller_service,
            cfg,
            controller_input_file="planner_input.yaml",
            streaming_engine="precompute",
        )

        controller_service.start.assert_called_once_with(
            punting=True,
            controller_input_file="planner_input.yaml",
            streaming_engine="precompute",
        )


if __name__ == "__main__":
    unittest.main()
