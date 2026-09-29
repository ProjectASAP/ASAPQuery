"""Tests for ProcessMonitor behavior when monitored processes exit mid-run."""

import multiprocessing
import subprocess
import threading
import time
import unittest
from collections import namedtuple
from unittest import mock

import psutil

import constants
from classes import process_monitor

MONITORS = ["memory_info", "cpu_percent"]
INTERVAL_SECONDS = 0.05
_Mem = namedtuple("_Mem", ["rss"])


def _fake_process(pid, children=()):
    p = mock.Mock()
    p.pid = pid
    p.as_dict.return_value = {"memory_info": _Mem(rss=1), "cpu_percent": 0.0}
    p.children.return_value = list(children)
    return p


class SeedProcessExitTest(unittest.TestCase):
    def test_seed_exit_returns_aligned_samples_with_exit_marker(self):
        # A seed pid dying used to crash the sampler and lose every pid's samples.
        survivor = subprocess.Popen(["sleep", "60"])
        victim = subprocess.Popen(["sleep", "60"])
        self.addCleanup(survivor.kill)
        self.addCleanup(victim.kill)

        monitor, control_pipe, monitor_pipe = process_monitor.start_monitor(
            [survivor.pid, victim.pid],
            ["survivor", "victim"],
            INTERVAL_SECONDS,
            MONITORS,
            include_children=True,
            hooks=None,
        )
        time.sleep(INTERVAL_SECONDS * 5)
        victim.kill()
        victim.wait()

        self.assertTrue(control_pipe.poll(5), "sampler should send data unprompted")
        info = process_monitor.stop_monitor(
            monitor, control_pipe, monitor_pipe, timeout=5
        )

        self.assertIsNotNone(info)
        n = len(info[survivor.pid]["cpu_percent"])
        self.assertGreater(n, 0)
        # The survivor is sampled before the victim in each round, so this also
        # checks that the partial round was rolled back.
        for entry in info.values():
            self.assertEqual(len(entry["cpu_percent"]), n)
            self.assertEqual(len(entry["memory_info"]), n)
        self.assertEqual(
            info[victim.pid][constants.PROCESS_MONITOR_EXITED_AT_SAMPLE_KEY], n
        )
        self.assertNotIn(
            constants.PROCESS_MONITOR_EXITED_AT_SAMPLE_KEY, info[survivor.pid]
        )


class ChildProcessExitTest(unittest.TestCase):
    def test_child_exit_does_not_stop_monitor(self):
        dead_child = _fake_process(2)
        dead_child.as_dict.side_effect = psutil.NoSuchProcess(2)
        seed = _fake_process(1, children=[dead_child])

        with mock.patch.object(psutil, "Process", return_value=seed):
            control_pipe, monitor_pipe = multiprocessing.Pipe()
            monitor = process_monitor.MyMonitor(
                [1],
                ["seed"],
                monitor_pipe,
                INTERVAL_SECONDS,
                MONITORS,
                hooks=None,
                include_children=True,
            )
        # Run in a thread so the mocks stay in effect.
        thread = threading.Thread(target=monitor.run)
        thread.start()
        self.assertEqual(control_pipe.recv(), "ready")

        # A crash closes the pipe (poll -> True); a surviving monitor stays quiet until stopped.
        self.assertFalse(control_pipe.poll(INTERVAL_SECONDS * 5))
        control_pipe.send("stop")
        self.assertTrue(control_pipe.poll(5))
        info = control_pipe.recv()
        thread.join(5)

        self.assertGreater(len(info[1]["cpu_percent"]), 1)
        self.assertNotIn(constants.PROCESS_MONITOR_EXITED_AT_SAMPLE_KEY, info[1])
        self.assertEqual(info[2]["cpu_percent"], [])


if __name__ == "__main__":
    unittest.main()
