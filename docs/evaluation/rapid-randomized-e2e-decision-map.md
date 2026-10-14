# Rapid randomized E2E evaluation: decision map

## #1: Paper claim

Type: Discuss

### Question

What claim must the fast experiment support?

### Answer

Across randomly generated supported query mixes, ASAPQuery has lower resource
cost and/or latency at bounded fidelity error than exact baselines, after the
same logical history is loaded. This is a rapid randomized E2E result, not a
claim to reproduce the full 24-hour online workload.

## #2: Workload sampling

Blocked by: #1

Type: Discuss

### Question

Which supported templates may contribute to a workload, and how will ten
queries be sampled so each workload is representative rather than accidentally
dominated by cheap or similar queries?

### Answer

The rapid result uses classic PromQL templates only, so ASAPQuery, Prometheus,
VictoriaMetrics, ClickHouse, and ClickHouse + exact MVs can all participate.
Multi-grouping SQL templates are a separate ASAPQuery-versus-ClickHouse result
if time permits. Sample queries uniformly from the eligible query-string pool;
the larger quantile-over-time family may therefore appear more often. Restrict
windowed queries to 15 m and 1 h, so each workload needs at most 1 h of logical
history rather than a 24 h fill. For each workload, sample ten query instances
without replacement from the eligible query-string pool; generate ten
independent workloads with recorded seeds 0–9. Parameterizations such as
different quantiles are distinct eligible query strings and may co-occur, but
an exact query string appears at most once in a workload.

## #3: Measurement protocol

Blocked by: #1

Type: Discuss

### Question

What common logical history, backfill, query repetitions, and monitored phase
give each random workload a credible cost-latency-fidelity comparison within the
paper deadline?

### Answer

Each of the ten query sets receives its own ASAPQuery plan and fresh run. A
union plan for all sampled queries would charge a ten-query workload for
precomputes it does not use. Instant templates repeat every second; windowed
templates repeat every minute, matching their source workload cadence. Measure
each workload for 15 minutes: report median latency and CPU/memory, retain raw
latencies and maxima, and do not claim a robust per-query p99 for minute-cadence
queries. The required systems are ASAPQuery, Prometheus, and raw ClickHouse;
VictoriaMetrics and ClickHouse + exact MVs are optional additions and do not
gate the rapid result.

## #5: Baseline history reuse

Blocked by: #3

Type: Discuss

### Question

May a baseline bulk-load the common 1 h history once and run all ten
phase-separated query workloads against it, or must every workload use a fresh
baseline process and reload? The former is faster; the latter removes cache and
physical-layout carryover.

### Answer

Each baseline bulk-loads the common 1 h history once, then runs the ten query
sets as separately monitored phases with query statistics reset. Randomize and
record workload order independently per baseline to mitigate cache carryover.

## #4: Aggregation and reporting

Blocked by: #2, #3, #5

Type: Discuss

### Question

How will results over ten sampled workloads be summarized and shown, including
unsupported baseline queries and fidelity metrics for each query family?

### Answer

Use the percentage of ASAPQuery evaluations meeting their template's
predeclared accuracy target as the common fidelity headline. Retain
family-specific errors alongside it: rank/relative error for quantiles,
precision for top-k, and relative error or exact match for sums, rates, and
instant aggregates.
