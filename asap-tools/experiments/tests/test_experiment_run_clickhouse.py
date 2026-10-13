"""Regression tests for ClickHouse experiment controller wiring."""

import unittest
from unittest.mock import Mock

from omegaconf import OmegaConf

from experiment_run_clickhouse import _ingest_mode, _start_clickhouse_controller


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


class IngestModeTest(unittest.TestCase):
    def test_baseline_when_present_else_first_mode(self):
        self.assertEqual(_ingest_mode(["baseline", "sketchdb"]), "baseline")
        self.assertEqual(_ingest_mode(["sketchdb", "baseline"]), "baseline")
        # A baseline_mv-only run monitors ingest under its own mode.
        self.assertEqual(_ingest_mode(["baseline_mv"]), "baseline_mv")


if __name__ == "__main__":
    unittest.main()
