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

## #3: Measurement protocol

Blocked by: #1

Type: Discuss

### Question

What common logical history, backfill, query repetitions, and monitored phase
give each random workload a credible cost-latency-fidelity comparison within the
paper deadline?

## #4: Aggregation and reporting

Blocked by: #2, #3

Type: Discuss

### Question

How will results over ten sampled workloads be summarized and shown, including
unsupported baseline queries and fidelity metrics for each query family?
