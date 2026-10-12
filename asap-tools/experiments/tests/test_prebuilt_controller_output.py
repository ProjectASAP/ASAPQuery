"""Tests for running e2e experiments on prebuilt planner configs."""

import os
import tempfile
import unittest

from experiment_utils.config import copy_prebuilt_controller_output


class CopyPrebuiltControllerOutputTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.src = os.path.join(self.tmp.name, "src")
        self.dest = os.path.join(self.tmp.name, "dest")
        os.makedirs(self.src)
        for name in ("streaming_config.yaml", "inference_config.yaml"):
            with open(os.path.join(self.src, name), "w") as f:
                f.write(name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_replaces_stale_output(self):
        os.makedirs(self.dest)
        with open(os.path.join(self.dest, "controller.log"), "w") as f:
            f.write("stale")
        copy_prebuilt_controller_output(self.src, self.dest)
        self.assertEqual(
            sorted(os.listdir(self.dest)),
            ["inference_config.yaml", "streaming_config.yaml"],
        )

    def test_refuses_missing_config(self):
        os.remove(os.path.join(self.src, "inference_config.yaml"))
        with self.assertRaisesRegex(ValueError, "inference_config.yaml"):
            copy_prebuilt_controller_output(self.src, self.dest)

    def test_refuses_own_output_dir(self):
        with self.assertRaisesRegex(ValueError, "own output dir"):
            copy_prebuilt_controller_output(self.src, self.src)
        self.assertTrue(os.path.isfile(os.path.join(self.src, "streaming_config.yaml")))


if __name__ == "__main__":
    unittest.main()
