"""Tests for evaluation times aligned to a query group's repeat interval."""

import unittest

from classes.query_time import (
    aligned_eval_time,
    first_aligned_eval_time,
    secs_until_send,
)

# 2026-10-13 12:00:00 UTC, a multiple of 60 s.
MINUTE = 1791892800


class FirstAlignedEvalTimeTest(unittest.TestCase):
    def test_floors_to_the_interval_after_the_offset(self) -> None:
        # Started at 12:00:35 with a 10 s offset: 12:00:25 floors to 12:00:00.
        self.assertEqual(first_aligned_eval_time(MINUTE + 35.4, 60000, 10), MINUTE)
        # Started at 12:00:05: the 12:00:00 window has not settled yet.
        self.assertEqual(first_aligned_eval_time(MINUTE + 5, 60000, 10), MINUTE - 60)

    def test_one_second_interval(self) -> None:
        self.assertEqual(first_aligned_eval_time(MINUTE + 7.9, 1000, 2), MINUTE + 5)

    def test_sub_second_interval_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "whole-second"):
            first_aligned_eval_time(MINUTE, 1500, 0)

    def test_negative_offset_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "query_time_offset"):
            first_aligned_eval_time(MINUTE, 60000, -1)


class ScheduleTest(unittest.TestCase):
    def test_repetitions_step_by_the_interval(self) -> None:
        self.assertEqual(
            [aligned_eval_time(MINUTE, 60000, k) for k in range(3)],
            [MINUTE, MINUTE + 60, MINUTE + 120],
        )

    def test_waits_until_offset_after_the_eval_time(self) -> None:
        self.assertEqual(secs_until_send(MINUTE + 60, 10, MINUTE + 65), 5)

    def test_late_repetition_runs_at_once(self) -> None:
        # A slow repetition makes the next one late; it keeps its eval time
        # rather than skipping it.
        self.assertEqual(secs_until_send(MINUTE + 60, 10, MINUTE + 75), 0)


if __name__ == "__main__":
    unittest.main()
