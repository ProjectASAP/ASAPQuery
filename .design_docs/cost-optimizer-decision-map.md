# Cost-based optimizer decision map

Goal: turn a PromQL workload with query frequencies and dataset-conditioned
sketch measurements into an offline deployment plan, then collect evidence that
the selected plan improves the stated objective without violating accuracy.

## Progress log

- 2026-09-07: Located the existing offline planner entry point:
  `asap-optimizer-cli`. `Controller::generate()` still takes the hardcoded
  planner path, so production wiring is intentionally not the first milestone.
- 2026-09-07: Verified a wire-contract gap: sketch-bench emits a versioned,
  workload-profiled atomic-cost document, while ASAPQuery currently loads a
  legacy flat entry array. A profile-aware loader and explicit selector are the
  first integration change.
- 2026-09-07: Distinguished grouping labels from sketch keys. Grouping labels
  determine how many sketch instances the planner deploys; sketch-key
  properties belong to a measured benchmark profile.
- 2026-09-07: Chosen first vertical slice: KLL backing PromQL
  `quantile_over_time`. The next unresolved concrete inputs are a source
  metric, KLL input value/key column, trace time slice, and grouping labels.
- 2026-09-07: Candidate external workload specs already present in
  `sketch-bench`: Alibaba microservices CPU (`cpu_utilization`, grouped by
  `msname`, one-minute slice); Google task CPU (`cpu_rate`, grouped by
  `machine_id`, three-minute slice); and Datadog/BOOM (`target`, scalar,
  one-minute slice). Existing ASAPQuery Prometheus replay configurations also
  exist for Alibaba node CPU and Google CPU. Recommended first case: Google
  CPU, because it has both a sketch-bench workload spec and a matching
  Prometheus replay configuration with explicit labels.
- 2026-09-07: Verified the selected Google trace is available locally:
  `google-cluster-data/ClusterData2011/clusterdata-2011-2/task_usage/part-00262-of-00500.csv.gz`
  (92 MB). The external loader supports numeric f64 grouped workloads, and the
  selected spec uses `cpu_rate`, grouped by `machine_id`, over a three-minute
  time slice. `kll-percall` is the correct benchmark variant because the
  ASAPQuery runtime invokes KLL `quantile()` per query. Remaining prerequisites:
  (1) a matching `quantile_over_time` workload and series-inventory CSV,
  (2) profile-aware atomic-cost loading/selection in ASAPQuery, and
  (3) explicit accuracy, arrival-rate, and exact-baseline assumptions.
- 2026-09-07: Inspected sketch-bench PR #124. It is the correct producer-side
  contract: a versioned document, one profile per exact external workload,
  required `query_accuracy`, and no cross-profile merging. Decision: do not
  add an ASAPQuery-specific `logical_metric` field to PR #124. Its
  `value_column` identifies the physical trace column measured; a Prometheus
  metric name is a query-engine identity that may rename or transform that
  column. ASAPQuery owns the explicit mapping from a queried metric to a
  benchmark profile. Do not add KLL key columns for this slice: KLL consumes
  numeric values and keyed external workloads are intentionally unsupported.
  ASAPQuery should mirror the versioned types, select exactly one external
  profile from an explicit selector, and pass only its entries to the existing
  candidate resolver.
- 2026-09-07: Corrected an earlier naming example after inspecting collector
  code. `google_mean_cpu_usage_rate_0` is an older experiment-specific name;
  the existing evaluation mapper emits `google_cluster_2019_cpu_rate` from
  raw `cpu_rate`, and creates a `_q_kll` alias only to route a second copy of
  the same samples through KLL. These are naming/routing conventions, not
  distinct source values. For the first slice, use one canonical raw-stream
  identity throughout (recommended Prometheus-safe name:
  `google_task_usage_cpu_rate`), and do not require a separate logical-metric
  field in the sketch-bench document.
- 2026-09-07: Validated the PR-123/PR-124 dependency analysis. PR 123 changes
  the underlying benchmark-record schema; PR 124 is the atomic-cost consumer
  break and must be handled via its independent document schema version. Two
  implementation corrections: `query_accuracy` is a required map of named
  metrics, not one `f64`; and workload selection can happen once at the
  ASAPQuery load boundary, returning the selected profile's existing flat
  `AtomicCostTable`. This avoids threading workload identity through every
  candidate resolver while still rejecting zero or ambiguous matches before
  any candidate lookup.
- 2026-09-07: Implemented the ASAPQuery consumer seam on branch
  `feat/profiled-atomic-cost-loader`. It mirrors PR 124's versioned document,
  external workload, required accuracy map, strict schema validation, and
  exact single-profile selection. The two offline optimizer CLIs now require a
  JSON `profiles[].workload` selector whenever `--atomic-costs` is supplied;
  selected entries preserve the existing flat resolver interface. Focused
  loader and full planner tests pass (216 tests plus doc-tests); the latter was
  run outside the sandbox because its existing ClickHouse mock opens a local
  listener.
- 2026-09-07: Review follow-up: centralized CLI profile loading behind one
  optimizer module interface and added explicit zero-match coverage. Synthetic
  profile descriptions remain opaque JSON because sketch-bench's data-generator
  schema is independently versioned; document, profile, entry, and external
  workload fields remain strict at this consumer boundary.

## #1: What identifies a benchmark cost profile?

Type: Discuss

### Question

Which dataset properties are expected to change insert, merge, query, memory,
or accuracy enough that they must select a distinct atomic-cost profile? Decide
the first paper-scale profile matrix and the semantics for choosing a profile at
planning time.

### Answer

Open. `sketch-bench` already emits `AtomicCostDocument { schema_version,
profiles: [{ workload, entries }] }`; the optimizer currently reads the older
flat entry array and therefore has no profile-selection rule. The available
observability corpus is under `../benchmarks/metrics_observability/data` from
the workspace root (Datadog/BOOM and Alibaba traces). Candidate framing from
the user: `(dataset_name, metric_name, keying/aggregated-label names,
time-range)`. The remaining decision is whether labels and range identify an
atomic measurement or instead parameterize the structural cost formula.

Clarification: **grouping labels** are the PromQL `GROUP BY` labels and
partition the metric stream into separate sketch instances. A **sketch key**
is the value or label tuple inserted into keyed sketches such as HLL, CMS, and
Hydra. Grouping labels and their observed distinct-group count are planner
context; the sketch-key distribution/cardinality (and, where relevant, encoded
key size) belong in the benchmark profile.

For the first vertical slice, scope this decision to one KLL
`quantile_over_time` workload. Do not define the full cross-dataset matrix yet.

## #2: What is the minimum credible empirical planning loop?

Blocked by: #1
Type: Prototype

### Question

What end-to-end experiment should prove that measured costs, rather than
hand-tuned constants, change a planner decision appropriately for a fixed
PromQL workload and series inventory?

### Answer

Open. The existing offline `asap-optimizer-cli` is the intended harness after
the document-loader/profile-selection gap is closed.

## #3: What feasibility evidence constrains optimization?

Blocked by: #1, #2
Type: Discuss

### Question

For each query/sketch/config/dataset case, which accuracy metric and threshold
make a candidate eligible, and how will exact-query cost and arrival rate be
measured rather than assumed?

### Answer

Open. `sketch-bench` already retains capability-specific `query_accuracy`, but
the ASAPQuery greedy optimizer does not use it; arrival rate (`rho`) and exact
query cost are currently placeholders.
