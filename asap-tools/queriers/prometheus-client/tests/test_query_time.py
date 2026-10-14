"""Tests for evaluation times aligned to a query group's repeat interval."""

import unittest

from classes.query_time import (
    aligned_eval_time,
    first_aligned_eval_time,
    secs_until_send,
)

# 2026-10-13 10:00:00 UTC, a multiple of 300 s.
START = 1791885600


class FirstAlignedEvalTimeTest(unittest.TestCase):
    def test_first_multiple_after_the_starting_delay(self) -> None:
        self.assertEqual(first_aligned_eval_time(START, 60000, 120), START + 120)
        self.assertEqual(first_aligned_eval_time(START, 60000, 121), START + 180)
        self.assertEqual(first_aligned_eval_time(START, 1000, 120), START + 120)

    def test_first_window_is_always_full(self) -> None:
        # A starting delay shorter than the interval would evaluate a window
        # that began before the data did.
        self.assertEqual(first_aligned_eval_time(START, 300000, 120), START + 300)
        self.assertEqual(first_aligned_eval_time(START, 60000, 0), START + 60)

    def test_start_off_the_interval_rejected(self) -> None:
        # Eval times off the interval would miss ASAPQuery's aligned windows.
        with self.assertRaisesRegex(ValueError, "not a multiple"):
            first_aligned_eval_time(START + 60, 300000, 0)

    def test_sub_second_interval_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "whole-second"):
            first_aligned_eval_time(START, 1500, 0)


class ScheduleTest(unittest.TestCase):
    def test_repetitions_step_by_the_interval(self) -> None:
        self.assertEqual(
            [aligned_eval_time(START, 60000, k) for k in range(3)],
            [START, START + 60, START + 120],
        )

    def test_waits_for_the_offset_after_the_eval_time(self) -> None:
        self.assertEqual(secs_until_send(START + 60, 10, START + 65), 5)

    def test_late_repetition_runs_at_once(self) -> None:
        # A slow repetition makes the next one late; it keeps its eval time
        # rather than skipping it.
        self.assertEqual(secs_until_send(START + 60, 10, START + 75), 0)

    def test_negative_offset_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "query_time_offset"):
            secs_until_send(START, -1, START)


if __name__ == "__main__":
    unittest.main()
