# RQE optimizer status

The v1 design is in `rqe_sketch_deployment_v1.md`. This file tracks what is
implemented, what is next, and what is intentionally deferred.

## Done in v1

- Sliding deployments use `(capability, configuration, metric, spatial_filter, G, x, y)`,
  where `x` is window size and `y` is slide.
- Workload facts are per metric: full label set, scrape interval, and
  cardinality per label set. The arrival rate is derived from them, and
  `validate_facts` reports bad facts up front.
- Eligibility enforces capability, metric, spatial filter, grouping (equal,
  or a subset for a family that merges across groups: a roll-up), alignment, and the RQE's
  measured accuracy, by each family's own metric, against the RAQE's SLA.
- Candidate generation uses legal windows and subset gcds of query intervals,
  keeping only multiples of the scrape interval. A deployment may serve
  multiple compatible RQEs.
- Candidate dominance safely removes a deployment only when another at the
  same grouping covers all of its RQEs with no worse ingest CPU and memory,
  compaction (per window and per slide), per-RQE query-job CPU, query memory
  (merge and output), or stored memory.
- The analytical model scores four phases, each with CPU and memory: ingest
  (open windows), merge, query, and storage (closed windows, `(S − x)/y + 1`
  for the longest lookback), plus per-RQE latency.
- The Pareto vector is `(CPU, memory, {latency_i})`.
- Eager enumeration remains available for small inputs. Streaming enumeration
  retains only the incremental Pareto frontier.
- `small_problem --candidates-only` reports generated and retained candidate
  counts, per-RQE eligibility, and the Cartesian mapping-space size.
- Streaming mode reports periodic throughput and frontier-size progress, and
  can print initial example mappings.
- `scripts/study_saturation.py --phase optimizer-cost` measures the cost
  table used by `small_problem` (#174), without changing ASAPQuery's
  exporter.
- The MILP uses HiGHS to minimize `Objective::AUCCost { w_cpu, w_mem }`,
  `w_cpu × CPU + w_mem × memory GiB` (default `(1, 0)`), without enumerating
  mappings. It accepts optional per-RQE latency bounds and returns a normal
  mapping.
- `small_problem --milp` takes `--w-cpu`/`--w-mem` and repeatable per-RQE
  latency limits, and includes 1-hour, 6-hour, and 1-day quantile RQEs to
  exercise those constraints.
- Regression tests confirm the MILP matches exhaustive search on tiny
  workloads with shared deployments, at several weightings.

## Next

- Add a hard `--max-mappings` limit to streaming experiments.
- Validate accuracy after merging `S / x` sketch instances. V1 currently uses
  single-instance measurements, which do not establish final query accuracy.
- Add repeated MILP solves for Pareto exploration across latency budgets or
  weights.
- Set `w_cpu`/`w_mem` from real prices.

## Explicitly deferred

- Query-result sharing across RQEs.
- Real query concurrency in the snapshot objective, whose memory assumes every
  query runs at once (cost by use holds query memory only while it runs).
- Bursty load; CPU is a mean over time.
- Admitting Hydra ([rqe_optimizer_hydra.md](rqe_optimizer_hydra.md)).
- Key labels `K` (PerGroup + PerKey sizes).
- RQE churn, replanning, and migration cost.
- Temporal pre-merging; v1 merges selected base instances at query time.
- A policy for choosing one mapping from the reported Pareto frontier.
- Additional capabilities beyond frequency, quantile, cardinality, and top-k.
