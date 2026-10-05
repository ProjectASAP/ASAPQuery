# RQE optimizer status

The v1 design is in `rqe_sketch_deployment_v1.md`. This file tracks what is
implemented, what is next, and what is intentionally deferred.

## Done in v1

- Sliding deployments use `(capability, configuration, labels, x, y)`, where
  `x` is window size and `y` is slide.
- Eligibility enforces capability, labels, alignment, and the RQE's measured
  `accuracy_metric` constraint.
- Candidate generation uses legal windows and subset gcds of query intervals.
  A deployment may serve multiple compatible RQEs.
- Candidate dominance safely removes a deployment only when another covers all
  of its RQEs with no worse query memory, ingest CPU, or per-RQE latency.
- The analytical model combines measured per-operation costs with label-set
  cardinality and arrival rate to score ingest CPU, query CPU, merge CPU,
  total CPU, query memory, and per-RQE latency.
- The Pareto vector is `(peak_query_memory, TCO_cpu, {latency_i})`.
- Eager enumeration remains available for small inputs. Streaming enumeration
  retains only the incremental Pareto frontier.
- `small_problem --candidates-only` reports generated and retained candidate
  counts, per-RQE eligibility, and the Cartesian mapping-space size.
- Streaming mode reports periodic throughput and frontier-size progress, and
  can print initial example mappings.
- `scripts/export_rqe_optimizer_costs.sh` exports the 18-row measured cost
  table used by `small_problem`, without changing ASAPQuery's exporter.
- The MILP uses HiGHS to minimize TCO without enumerating mappings. It accepts
  optional peak-memory and per-RQE latency bounds and returns a normal mapping.
- `small_problem --milp` accepts repeatable per-RQE latency limits and includes
  1-hour, 6-hour, and 1-day quantile RQEs to exercise those constraints.
- A regression test confirms the MILP matches exhaustive minimum-TCO search on
  a tiny workload with shared deployment activation costs.
- Retained memory (`(x + max S) / y` instances per active deployment) is
  scored, and `milp::minimize_cost` minimizes the hourly price on one EC2
  machine family in fractional instances. Prices are a committed snapshot from
  `scripts/fetch_ec2_pricing.py`; `small_problem --milp --machine-family NAME`
  uses it.

## Next

- Add a hard `--max-mappings` limit to streaming experiments.
- Validate accuracy after merging `S / x` sketch instances. V1 currently uses
  single-instance measurements, which do not establish final query accuracy.
- Add repeated MILP solves for Pareto exploration across memory and latency
  budgets, or through normalized weighted objectives.

## Explicitly deferred

- Query-result sharing across RQEs.
- Merge buffers and concurrent-query memory.
- RQE churn, replanning, and migration cost.
- Precomputed rollups; v1 merges selected base instances at query time.
- A policy for choosing one mapping from the reported Pareto frontier.
- Additional capabilities beyond frequency, quantile, cardinality, and top-k.
