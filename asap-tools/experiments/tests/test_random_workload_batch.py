"""Tests for random_workload_batch.py's job planning."""

import unittest

import random_workload_batch as batch


class BatchPlanningTest(unittest.TestCase):
    def test_sets_name_their_workloads(self):
        self.assertEqual(
            batch.parse_set("seed-862-x5:01,03-04"),
            ("seed-862-x5", ["workload-01", "workload-03", "workload-04"]),
        )

    def test_retries_get_a_new_experiment_name(self):
        # A rerun under the old name hits the earlier run's ClickHouse data
        # directory, which the runner can't chmod.
        job = {"set": "seed-862-long", "workload": "workload-07", "arm": "ch"}
        names = []
        for attempts in (1, 2):
            names.append(batch.experiment_name({**job, "attempts": attempts}))
        self.assertEqual(names, ["s862long_w07_ch", "s862long_w07_ch_r2"])

    def test_each_job_runs_on_its_slots_two_nodes(self):
        job = {"set": "seed-862", "workload": "workload-01", "arm": "prom"}

        class Args:
            username, hostname_suffix = "u", "s"

        cmd = batch.command(job, "s862_w01_prom", 3, Args)
        self.assertIn("providers.cloudlab.num_nodes=1", cmd)
        self.assertIn("providers.cloudlab.node_offset=6", cmd)
        self.assertIn("flow.skip_copy_prometheus_data=true", cmd)
        self.assertIn(
            "experiment_params.experiment=[{mode:baseline,server:prometheus}]", cmd
        )


if __name__ == "__main__":
    unittest.main()
