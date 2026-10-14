"""Evaluation times aligned to a query group's repeat interval.

With `client_options.align_to_interval`, the data starts at event time S
(`client_options.data_start_time`, epoch seconds, a multiple of every aligned
group's interval) and runs L seconds behind the wall clock
(`client_options.data_lag_secs`). Repetition k of a group with repeat interval
T (`repetition_delay_ms`), starting delay D (`starting_delay`) and settle
offset O (`query_time_offset`, default 0) evaluates at

    t_k = S + (max(1, ceil(D / T)) + k) * T

and is sent at wall-clock time t_k + L + O, once its data has arrived. Every
evaluation time is a multiple of T, matching ASAPQuery's epoch-aligned windows,
and repetition k covers the same samples in every run regardless of when the
run started. For example, T = 60 s, D = 120 s, O = 10 s, and data stamped from
10:00:00 but sent from 10:02:23 (L = 143 s): repetitions evaluate at 10:02:00,
10:03:00, ... and are sent at 10:04:33, 10:05:33, ...
"""

import math

MS_PER_SEC = 1000


def first_aligned_eval_time(
    data_start_time: int, interval_ms: int, starting_delay_secs: float
) -> int:
    """The first multiple of the interval at least `starting_delay_secs` (and
    one full interval) after the data starts."""
    if interval_ms <= 0 or interval_ms % MS_PER_SEC != 0:
        raise ValueError(
            f"align_to_interval needs a whole-second repetition_delay_ms, got {interval_ms}"
        )
    interval_secs = interval_ms // MS_PER_SEC
    if data_start_time % interval_secs != 0:
        raise ValueError(
            f"data_start_time {data_start_time} is not a multiple of the "
            f"{interval_secs} s interval"
        )
    intervals = max(1, math.ceil(starting_delay_secs / interval_secs))
    return data_start_time + intervals * interval_secs


def aligned_eval_time(first_eval_time: int, interval_ms: int, repetition: int) -> int:
    return first_eval_time + repetition * (interval_ms // MS_PER_SEC)


def secs_until_send(
    eval_time: int, data_lag_secs: float, offset_secs: float, now_secs: float
) -> float:
    """How long to wait before evaluating at `eval_time`; 0 when already late."""
    if offset_secs < 0:
        raise ValueError(f"query_time_offset must be >= 0, got {offset_secs}")
    return max(0.0, eval_time + data_lag_secs + offset_secs - now_secs)
