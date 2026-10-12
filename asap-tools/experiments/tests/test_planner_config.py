"""Tests for planner.* -> --planner milp inputs."""

import unittest

from omegaconf import OmegaConf

from experiment_utils.config import milp_planner, require_legacy_planner
from experiment_utils.services.misc import MilpPlanner


def make_cfg(**overrides):
    cfg = OmegaConf.create(
        {
            "controller": {"punting": False, "auto_discover_labels": False},
            "planner": {
                "use_optimizer": True,
                "saturation_dir": "/sat",
                "workload_facts": "/facts.yaml",
                "w_cpu": None,
                "w_mem": 2.0,
            },
        }
    )
    return OmegaConf.merge(cfg, OmegaConf.create(overrides))


class MilpPlannerConfigTest(unittest.TestCase):
    def test_builds_inputs_when_enabled(self):
        self.assertEqual(
            milp_planner(make_cfg()),
            MilpPlanner(saturation_dir="/sat", workload_facts="/facts.yaml", w_mem=2.0),
        )

    def test_none_when_disabled(self):
        cfg = make_cfg(planner={"use_optimizer": False, "saturation_dir": None})
        self.assertIsNone(milp_planner(cfg))

    def test_rejects_each_conflict(self):
        cases = {
            "planner.saturation_dir is unset": {"planner": {"saturation_dir": None}},
            "planner.workload_facts is unset": {"planner": {"workload_facts": None}},
            "controller.punting=true": {"controller": {"punting": True}},
            "controller.auto_discover_labels=true": {
                "controller": {"auto_discover_labels": True}
            },
            "windowing is set": {"windowing": {"type": "sliding"}},
        }
        for message, overrides in cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                milp_planner(make_cfg(**overrides))

    def test_legacy_only_scripts_reject_optimizer(self):
        with self.assertRaisesRegex(ValueError, "experiment_run_clickhouse"):
            require_legacy_planner(make_cfg(), "experiment_run_clickhouse")
        require_legacy_planner(
            make_cfg(planner={"use_optimizer": False}), "experiment_run_clickhouse"
        )


if __name__ == "__main__":
    unittest.main()
