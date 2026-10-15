"""Tests for random_workload_batch.py's job planning."""

import tempfile
import unittest
from pathlib import Path
from unittest import mock

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

    def test_a_tagged_variant_rerun_names_and_configs_apart(self):
        # Reruns must not reuse the original runs' names, whose outputs they
        # would overwrite.
        with tempfile.TemporaryDirectory() as tmp:
            sets = Path(tmp) / "seed-862-long"
            (sets / "workload-04").mkdir(parents=True)
            (sets / "workload-04" / "clickhouse.yaml").write_text(
                "query_groups: [{client_options: {starting_delay: 600}}]"
            )
            (sets / "workload-04-sla10").mkdir()
            (sets / "workload-04-sla10" / "e2e.yaml").write_text("")
            with mock.patch.object(batch, "CONFIG_DIR", Path(tmp)):
                [job] = batch.make_jobs(
                    [("seed-862-long", ["workload-04"])], ("asap",), "sla10", "sla10"
                )
        job["attempts"] = 1
        self.assertEqual(batch.experiment_name(job), "s862long_w04_asap_sla10")

        class Args:
            username, hostname_suffix = "u", "s"

        self.assertIn(
            "experiment_type=random_workload/seed-862-long/workload-04-sla10/e2e",
            batch.command(job, "n", 0, Args),
        )

    def test_a_live_orphan_keeps_its_slot(self):
        # A resumed driver once cleaned and reused the slots of runs it
        # had left going, wrecking them.
        jobs = [
            {"status": "orphaned", "names": ["a"], "slot": 9, "attempts": 1},
            {"status": "orphaned", "names": ["b"], "slot": 10, "attempts": 1},
            {"status": "queued", "names": [], "attempts": 0},
        ]
        with mock.patch.object(batch, "succeeded", return_value=False):
            busy = batch.settle_orphans(jobs, is_alive=lambda name: name == "a")
        self.assertEqual(busy, {9})
        self.assertEqual([j["status"] for j in jobs], ["orphaned", "queued", "queued"])

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
        # A pooled config's frontend=rqe needs the sketchdb mode left out here.
        self.assertIn("query_engine.frontend=promql", cmd)


if __name__ == "__main__":
    unittest.main()
