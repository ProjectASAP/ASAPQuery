# Rebased Google task-usage replay input

This is the executable replay counterpart of the adjacent
`google_task_usage_262_3m_agg0` calibration input. It retains exactly the
same 20,051 rows, values, and label columns. Only CSV columns 1 and 2
(`start_time`, `end_time`, in microseconds) are transformed:

```
rebased_time_us = original_time_us - 1313535000000 + 600000000
```

The Google exporter computes replay time as
`(start_time_us / 1_000_000 - 600) * 1` for this validation scenario.
Rebasing therefore emits the first sample immediately and retains the original
180-second relative schedule. Without this temporary input transformation, the
source interval's original time origin would delay first output by roughly 15
days.

The atomic-cost profile remains tied to the unmodified calibration input and
its original source-time window. This file is solely a replay-clock adapter;
it is not a new dataset or a substitute for native exporter time filtering.

Validation:

- rows: 20,051
- rebased start-time range: 600000000–779000000 microseconds
- SHA-256: `744ce369e5dd638b800cc5eb202e8140a34166b3a8e301d583041ee640268d5e`
