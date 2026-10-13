"""Regression tests for ClickHouse experiment controller wiring."""

import unittest
from unittest.mock import Mock, patch

from omegaconf import OmegaConf

from experiment_run_clickhouse import (
    _deploy_prebuilt_clickhouse_controller_output,
    _start_clickhouse_controller,
)


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

    @patch("experiment_run_clickhouse.sync.rsync_streaming_configs")
    @patch("experiment_run_clickhouse.config.copy_prebuilt_controller_output")
    def test_prebuilt_output_is_validated_and_deployed(
        self, copy_prebuilt_controller_output, rsync_streaming_configs
    ):
        provider = Mock()

        _deploy_prebuilt_clickhouse_controller_output(
            provider,
            "/plans/frozen",
            "/tmp/local-controller",
            "/remote/controller",
            "/tmp/client.yaml",
            1000,
            2,
        )

        copy_prebuilt_controller_output.assert_called_once_with(
            "/plans/frozen",
            "/tmp/local-controller",
            "/tmp/client.yaml",
            1000,
            query_language="sql",
        )
        rsync_streaming_configs.assert_called_once_with(
            provider, "/tmp/local-controller", "/remote/controller", 2
        )


if __name__ == "__main__":
    unittest.main()
