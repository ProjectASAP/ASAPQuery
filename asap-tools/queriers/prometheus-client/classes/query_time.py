"""Evaluation times aligned to a query group's repeat interval.

With `client_options.align_to_interval`, repetition k of a group with repeat
interval T (`repetition_delay_ms`) and settle offset O
(`client_options.query_time_offset`, seconds, default 0) evaluates at

    t_k = floor((start - O) / T) * T + k * T

and is sent at wall-clock time t_k + O. Every evaluation time is a multiple of
T, so all systems answer the same windows (e.g. a 60 s query at whole minutes),
and O gives the data of the window ending at t_k time to arrive first. For
example, T = 60 s and O = 10 s, started at 12:00:35: repetitions evaluate at
12:00:00, 12:01:00, ... and are sent at 12:00:35 (late, so at once),
12:01:10, 12:02:10, ...
"""

MS_PER_SEC = 1000


def first_aligned_eval_time(
    now_secs: float, interval_ms: int, offset_secs: float
) -> int:
    """The latest multiple of the interval at least `offset_secs` before now."""
    if interval_ms <= 0 or interval_ms % MS_PER_SEC != 0:
        raise ValueError(
            f"align_to_interval needs a whole-second repetition_delay_ms, got {interval_ms}"
        )
    if offset_secs < 0:
        raise ValueError(f"query_time_offset must be >= 0, got {offset_secs}")
    interval_secs = interval_ms // MS_PER_SEC
    return int((now_secs - offset_secs) // interval_secs) * interval_secs


def aligned_eval_time(first_eval_time: int, interval_ms: int, repetition: int) -> int:
    return first_eval_time + repetition * (interval_ms // MS_PER_SEC)


def secs_until_send(eval_time: int, offset_secs: float, now_secs: float) -> float:
    """How long to wait before evaluating at `eval_time`; 0 when already late."""
    return max(0.0, eval_time + offset_secs - now_secs)
