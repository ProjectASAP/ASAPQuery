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

2026-09-07 next prerequisite: the current Google benchmark workload groups
KLL input by `machine_id`, whereas ordinary PromQL `quantile_over_time` is
evaluated per input time series. The raw Google task-usage identity also
contains `job_id` and `task_index`; the planner's KLL model is marked
non-subpopulation-aware and currently scales by query grouping count. Before a
KLL sweep, choose and implement the computational unit consistently in the
benchmark, series inventory, and planner: (a) one sketch per complete source
series (the recommended standard-PromQL interpretation), (b) an explicitly
defined pooled/global ASAP operation, or (c) an explicitly defined per-machine
operation. Do not treat a per-machine benchmark as evidence for a per-series
query without this alignment.

2026-09-08 scope decision: for the first optimizer experiment, treat every
spatial filter as having selectivity 1.0. Thus each candidate uses the metric's
full arrival rate for ingest costing; no selectivity estimator is needed yet.
Filters can still be retained syntactically for query/config identity, but do
not reduce estimated arrival rate or instance count in this slice. A later
extension can estimate selectivity from the series/sample inventory and use
`arrival_rate_hz * selectivity` for the affected configuration.

2026-09-07 current-state evidence: the 2011 Google OTLP mapper exports
`google_cluster_2011_cpu_rate` and identifies an uncapped series by the full
attribute tuple `(zone, rack, host, service, task)`. `zone` and `rack` are
deterministic functions of `machine_id`; `host` is `machine_id`, `service` is
`job_id`, and `task` is `task_index`, so the independent source identity is
effectively `(machine_id, job_id, task_index)`. With a positive cardinality
cap, all three are instead projected to a common hashed cell. The current
sketch-bench Google workload is grouped only by `machine_id`, `cpu_rate`, and
a fixed three-minute window. The current planner loads an inventory of full
label tuples, but `SeriesDataset::profile` returns `1` for no query grouping
labels and otherwise returns the number of distinct requested groups. That
count is copied into every candidate; KLL is non-subpopulation-aware, so it
multiplies KLL memory and query/merge CPU by this count. Crucially, the
canonical `build_query_requirements_promql` helper assigns **all metric-schema
labels** to an OnlyTemporal query such as `quantile_over_time`; it therefore
does count one KLL per exported series when the inventory/schema are complete.
The `1` case applies to a query whose result has an empty grouping (for example
a spatial aggregate with no `by (...)`), not to a plain temporal quantile.

2026-09-08 correction: prior notes incorrectly claimed that a plain
`quantile_over_time` creates empty `QueryRequirements.grouping_labels` and
therefore one KLL. In ASAPQuery-only scope this is false:
`asap_types::build_query_requirements_promql` detects the absence of a spatial
aggregation and preserves all labels from `PromQLSchema`. Collector behavior
is out of scope for this research loop. The remaining issue is only scenario
alignment: the current sketch-bench profile is per machine, whereas a planner
inventory/schema may describe per-series machine/job/task KLLs. Use matching
synthetic metric projections and benchmark `group_columns` for each study.

2026-09-08 reduced TODO after correction: no ASAPQuery partition-key model is
needed for the initial standard temporal-KLL slice. Select one synthetic metric
scenario; make its ASAPQuery schema and unique-series inventory match it;
benchmark the identical raw grouping in sketch-bench; export/select that
profile; and run the optimizer. The remaining planner work is profile plumbing
already implemented on `feat/profiled-atomic-cost-loader` plus a small
reproducible experiment fixture. Filter selectivity remains fixed at 1.0.

2026-09-08 implementation split: a first measured-cost KLL run requires no
additional ASAPQuery optimizer algorithm change after the profiled-cost-loader
branch lands. It needs experiment infrastructure only: a scenario-matching
series inventory and workload YAML in ASAPQuery, an external Google KLL sweep
and atomic-cost export in sketch-bench, plus a selector and reproducible runner.
One subsequent, meaningful ASAPQuery code slice remains for a constrained
optimizer: `ControllerOptions.accuracy_sla` is parsed but not propagated to an
AQE or compared with the selected entry's `query_accuracy["mean_rank_err"]`.
Its semantics must be fixed explicitly (recommended: maximum acceptable mean
rank error, e.g. 0.02) before adding that feasibility filter. Existing
untracked `asap-tools/experiments/datasets/quantile_demo` artifacts are user
work and are out of scope for this experiment.

2026-09-08 implemented KLL feasibility slice: `controller_options` now accepts
optional `max_mean_rank_error` (a fraction: `0.02` is 2%). The AQE extractor
propagates it and uses the smallest limit when identical AQEs are deduplicated.
The greedy optimizer rejects a `DatasketchesKLL` candidate unless its selected
atomic-cost row contains finite `query_accuracy.mean_rank_err` at or below the
limit; EXACT remains feasible. Focused optimizer tests cover strictest-limit
deduplication and selection of a more expensive KLL configuration when the
cheaper one exceeds the 2% bound.

2026-09-09 proposed first experiment (discussion, do not run yet): fixed
Google-2011 task-usage CPU trace slice, one synthetic per-machine metric
(`host <- machine_id`), one 3-minute `quantile_over_time(0.99, metric[3m])`
query, selectivity 1.0, and KLL `k in {200,500}`. sketch-bench must measure
insert, merge, per-call quantile, memory, and mean rank error for exactly that
raw grouping/window; ASAPQuery consumes that one selected profile, the matching
unique-host inventory, a measured arrival rate (`records_loaded / 180s`), and
the query repetition rate. Plumbing passes only if both K values resolve as
real costs and the plan is reproducible. The paper-facing decision test should
evaluate a predeclared sweep of rank-error limits: each KLL candidate is
eligible iff its measured error is at most the limit, and the selected plan
must equal the minimum predicted cost among eligible candidates. A separate
hold-out/replay measurement is required before claiming that the atomic model
predicts real end-to-end plan cost; do not call the first slice that validation.

2026-09-09 agreed provisional control for Experiment 1: temporarily set
ASAPQuery's `EXACT_QUERY_CPU_SECS` to a documented high value on the experiment
branch to force the optimizer to compare feasible KLL candidates, then restore
its original `1e-3` value after the experiment. Do not claim an
exact-vs-approximate result while this forced baseline is active. The
runner/output must label it `forced_exact_baseline`; replacing it with a
measured raw-query baseline is required for a later end-to-end comparison.

2026-09-09 Experiment 1 run (artifacts retained in
`sketch-bench/output/cost_optimizer_experiments/google_task_usage_cpu_per_machine/2026-09-09/`):
the fixed Google task-usage CPU slice loaded 20,051 rows from the declared
three-minute window and produced 6,481 distinct `machine_id` values. The
scenario exports one synthetic metric `google_task_cpu_rate` with `host <-
machine_id`; its headered unique-series inventory, workload YAML, selector,
raw JSONL passes, flattened records, cost document, planner YAMLs, and planner
logs are all retained there. The profile has two real KLL entries:

| K | mean rank error | memory / instance | selected under forced exact=100 |
|---|---:|---:|---|
| 200 | 0.00128963 (0.129%) | 6,400 B | limits 0.005, 0.01, 0.02, 0.05 |
| 500 | 0.00063105 (0.063%) | 16,000 B | exploratory limit 0.001 |

The four predeclared limits are all looser than K=200's measured error, so
they correctly select K=200. The 0.001 result is explicitly exploratory (it
was chosen after observing the measurements) and verifies the intended
feasibility switch: K=200 is rejected and K=500 is selected. Exact was forced
to 100 CPU-seconds/query only for those diagnostic runs and restored to
`1e-3`; a smaller forced value of 1.0 left EXACT cheaper because the KLL plan
holds 6,481 instances, and that run is also retained. None of these results is
an exact-vs-approximate claim.

The run exposed two plumbing findings. First, accuracy and throughput records
both carried incidental timing metadata, and the old generic flattener kept
the first one it encountered. The preserved accuracy-first `atomic_costs.json`
therefore initially had zero profiles (one-sample accuracy wall-time versus
five throughput/CPU samples). This is now fixed in sketch-bench: flattened
fields have strict primary-pass ownership—accuracy contributes only query
accuracy, throughput contributes cost timing/resources, and latency contributes
only insert latency. Only representable `(operation, pass)` pairs are accepted;
unknown/contradictory primary fields, query/merge/prepare latency, and other
unrepresentable pairs fail loudly rather than being silently dropped. A
representable pass must also contain its required primary result field.
Reflattening the
same preserved accuracy-first raw report now yields
`atomic_costs_strict_accuracy_first.json` with one profile/two entries, so the
export order is no longer a correctness condition. Second, an empirical profile
previously allowed unmeasured HydraKLL candidates
to use a flat stub and win. ASAPQuery now drops HydraKLL when a nonempty
empirical table is present, and treats it as infeasible under a rank-error
limit; focused regression tests cover both cases.

2026-09-07 design refinement: a benchmark need not expose every raw trace
column. A paper experiment may define a **synthetic metric scenario** as a
chosen label projection of a trace (for example, a machine-only metric or a
full machine/job/task metric), provided the projection is recorded and used
consistently. The scenario, not the raw CSV alone, must bind (1) the mapper's
exported metric name and retained labels, (2) the unique-series inventory fed
to the planner, (3) the sketch-bench `group_columns` that identify physical
sketch instances, and (4) the selected atomic-cost workload profile. For the
first ungrouped standard-PromQL KLL temporal quantile, #3 is the number of
exported source series; for an intentionally pooled or per-machine synthetic
metric it is the corresponding scenario-defined instance count. This makes
controlled label-projection experiments credible instead of accidental schema
drift.

## #2: What is the minimum credible empirical planning loop?

Blocked by: #1
Type: Prototype

### Question

What end-to-end experiment should prove that measured costs, rather than
hand-tuned constants, change a planner decision appropriately for a fixed
PromQL workload and series inventory?

### Answer

The existing offline `asap-optimizer-cli` is the selection harness; the
Hydra-based `asap-tools/experiments/experiment_run_e2e.py` is sufficient as
the execution harness. It already materializes a controller input from
`experiment_params`, passes `windowing` and `sketch_parameters` overrides,
and preserves its resolved Hydra config and controller/client output. It does
*not* read an atomic-cost profile or invoke the offline optimizer, so the
validation protocol must run the optimizer offline to choose/cost candidates,
then run the selected K values in E2E via `sketch_parameters.DatasketchesKLL.K`.

Provisional validation experiment (not yet run): test whether the profile
predicts the relative end-to-end cost of `K=200` versus `K=500`, not yet an
exact-versus-approximate win. Use one Google `task_usage` part and one
`quantile_over_time(0.99, google_mean_cpu_usage_rate_0[3m])` workload, with
one-minute tumbling sketches so the query merges three windows. Train the
profile on a declared source-time training interval; use a disjoint source-time
holdout interval for E2E replay. Execute both forced K values in randomized
or alternating repeated trials. Compare the optimizer's predicted ordering
with observed query-engine CPU rate and steady-state memory; also report query
latency and accuracy against the Prometheus baseline as secondary outcomes.

Before running, close the scenario-alignment gaps: the runtime Google exporter
exposes `job_id`, `task_index`, and `machine_id`, and metric suffix `_0`
filters `aggregation_type=0`. The sketch-bench workload/profile must use the
same complete series key and filter, not the current machine-only unfiltered
profile. The E2E exporter currently selects a part but exposes no source-time
window control in its Hydra configuration; add that filter (preferred), or
explicitly pre-slice the input, before calling the replay a temporal holdout.
Record the replay speed/arrival rate as well. Without those alignments, E2E
would be a useful smoke test but not validation of the measured profile.

2026-09-09 source-file check: Google `task_usage/part-00262-of-00500.csv.gz`
spans 5,265 source seconds (about 87.75 minutes), while the first profile used
only 180 seconds. Selecting `part_index: 262` in `experiment_run_e2e.py`
therefore does not by itself replay the same calibration data. Even the
same-data consistency experiment needs either exporter source-time bounds or
a materialized pre-sliced copy of that interval.

2026-09-09 temporary same-data shortcut: materialized
`asap-tools/experiments/datasets/cost_optimizer_validation/google_task_usage_262_3m_agg0/part-00262-of-00500.csv.gz`
from that exact interval, retaining rows with missing/zero `aggregation_type`
to match the exporter’s `_0` convention. It has 20,051 rows, 6,481 machines,
and 10,379 distinct `(job_id, task_index, machine_id)` source series. Its
adjacent README records the predicate and SHA-256. This enables the first E2E
consistency run without changing the exporter; native source-time/filter
configuration remains the follow-up architectural work.

2026-09-09 CloudLab execution preparation: copied that exact derived file to
`node1` (the cluster-data-exporter host) at
`/scratch/sketch_db_for_prometheus/experiment_inputs/cost_optimizer_validation/google_task_usage_262_3m_agg0/`.
The remote SHA-256 is `13dd00844558627365848f5252d9f86fe9c302afe875ae2fd702016b4acdf70f`,
matching the local README. The runner's `num_nodes=1` denotes one worker plus
the node-0 coordinator; its cluster-data exporter requires exactly this one
worker and runs on `node_offset + 1` (node1). Do not set `num_nodes=2`: the
service fails fast. The dedicated local Hydra scenario is
`cost_optimizer_validation_google`; it runs the 3-minute temporal quantile
with one-minute tumbling sketches and preserves K as a CLI override.

2026-09-09 aligned atomic-cost profile: reran the KLL `k={200,500}` accuracy
and cost passes serially over the derived file with
`group_columns=[job_id, task_index, machine_id]`. The valid raw, flattened,
and document artifacts are in
`sketch-bench/output/cost_optimizer_experiments/google_task_usage_full_series/2026-09-09/final_serial/`.
The document has one profile/two entries and no skips. The earlier
`strict_rebuild/` artifacts are intentionally retained: they demonstrate that
the strict reducer rejected a duplicate entry caused by an overlapping local
benchmark invocation, rather than accepting ambiguous measurements.

2026-09-09 E2E implementation and outcome: the temporary replay input was
also rebased (both Google timestamp columns) to the exporter's fixed 600-second
epoch. The rebased copy has the same 20,051 values/labels and SHA-256
`744ce369e5dd638b800cc5eb202e8140a34166b3a8e301d583041ee640268d5e`; without
rebasing, its original 2011 timestamps would make the fixed-offset exporter
wait about fifteen days. The dedicated local `experiment_run_e2e.py` scenario
uses the full source-series labels `[instance, job, job_id, task_index,
machine_id]`, an 18-second sliding window and 1-second slide (the offline
optimizer's chosen K=200 geometry), and the runtime exporter at 1x rather than
the former 10x dilation. After CloudLab Docker access was enabled on both
nodes, the local runner completed both K=200 and K=500 runs in approximately
200 seconds each (190-second replay/warmup plus ten query repetitions), in
both SketchDB and Prometheus-baseline modes. The controller-resolved streaming
configs explicitly record K=200 and K=500 respectively. Raw client results,
latencies, resolved configs, controller logs, and monitor output are retained
under `experiment_outputs/cost_optimizer_validation_k{200,500}_1x_20260909/`.
Earlier retry directories are retained as failed preflight evidence (first
node1 Docker access, then node0 controller Docker access); the interrupted
10x run was superseded by the 1x runs.

2026-09-09 first E2E reduction (same-data consistency, not holdout): SketchDB
completed ten queries in each run. Mean client latency was 169.25 ms for K=200
and 170.61 ms for K=500; their independent Prometheus baseline runs averaged
114.54 ms and 112.12 ms. Query-engine monitor peak RSS was 2,754,297,856 B
(2.57 GiB) for K=200 versus 6,360,596,480 B (5.92 GiB) for K=500; mean sampled
query-engine CPU was 18.50% versus 23.43%. Thus this short run is consistent
with the predicted direction that K=200 is the cheaper feasible option in
memory/CPU, while latency is too close and too sparsely sampled to support a
latency-ordering claim. Each SketchDB result had a baseline result with the
same repetition and label set; baseline produced 251 (K=200) and 288 (K=500)
additional rows because the independently timed replays advanced through the
source stream at slightly different rates. On the matched rows the median and
95th-percentile absolute value differences were zero for both K values
(maximum 0.0091102). This is only a replay/result-path smoke check: values
from independently timed query executions do not establish KLL rank error.
The 2% eligibility claim remains supported by the paired sketch-bench profiles
(mean rank errors: K=200 0.158583%, K=500 0.07025%). A reproducible reduction
must retain the matched-key rule and report unmatched baseline rows rather than
silently treating them as zero error.

The reducer is `asap-tools/experiments/analyze_cost_optimizer_validation.py`.
It has been run over both retained directories and wrote
`experiment_outputs/cost_optimizer_validation_1x_summary.json`; it is the
reproducible source for the figures above.

2026-09-09 optimizer-to-E2E wiring assessment: no new bridge is required for
this single-AQE KLL validation. `experiment_run_e2e.py` already forwards its
global `sketch_parameters` and `windowing` Hydra objects into the controller
input; overriding `DatasketchesKLL.K`, the window size, and the slide exactly
recreates this selected KLL deployment. The offline CLI is nevertheless not
general E2E wiring: it prints a human-readable `OptimizerSolution`, while E2E
then invokes the ordinary controller generator. A multi-AQE optimizer result
may contain distinct K values/window geometries and AQE-to-aggregation
assignments, none representable by those global overrides. The next genuine
product/plumbing milestone is a machine-readable deployment artifact from the
optimizer plus a controller/E2E input mode that consumes it. Do this only after
the paper's single-AQE evidence is stable; it is not a prerequisite for the
current controlled experiment.

2026-09-10 generalization decisions: retain `aggregation_type=0` and assume
no spatial filters/selectivity for the present study. Add synthetic metric
projections and additional query workloads; different cardinality projections
are desirable but not a required first expansion. Evaluate disjoint trace
intervals, but do not frame the immediate objective as prediction on unseen
data. Replace the exporter’s temporary replay-clock treatment with native time
range logic. Use the optimizer for broad scenario/query/SLA sweeps, reserving
E2E for a small number of selected-plan evidence runs rather than repeating
every optimizer simulation.

2026-09-10 proposed architecture: introduce a dataset-wrangler module whose
single scenario specification materializes an exporter-ready dataset, complete
series inventory, and immutable manifest. The same manifest is passed to
sketch-bench and E2E, preventing label/time/filter drift. The benchmark export
is an `AtomicCostDocument` containing one `AtomicCostProfile` per exact
wrangled workload and measurement environment; each `AtomicCostEntry` is one
sketch/configuration’s insert, merge, query, memory, and accuracy observation.
Time range belongs in the profile/workload identity, not in a free-standing
entry. The optimizer should consume selected profiles plus an explicit backend
exact-cost profile and emit a machine-readable deployment plan, which E2E can
deploy without manually translating K/window overrides.

2026-09-10 implementation update: the first executable wrangler materializes
the Google task-usage validation scenario, applies the source interval and
`aggregation_type = 0` rule, validates duplicate exported samples, rebases the
replay timestamps, and writes the derived CSV, series inventory, and scenario
manifest. The manifest records both the compressed artifact hash and the
canonical decompressed-payload hash. The payload hash, not gzip metadata, is
the dataset identity that a future `AtomicCostProfile` must cite. Thus an
`AtomicCostDocument` remains a collection of measurements, not a dataset
container: `scenario spec -> wrangled CSV + ScenarioManifest -> sketch-bench
-> AtomicCostDocument`.

2026-09-10 profile-provenance implementation: `AtomicCostDocument` schema v2
adds a required profile-level `scenario` identity: the canonical wrangled CSV
payload hash, exported metric, grouping labels, and original source-time
range. `approxbench atomic-costs` now requires the wrangler's scenario
manifest and rejects malformed provenance. ASAPQuery's `--atomic-cost-profile`
selector must contain both `workload` and `scenario`; a workload-only match is
not eligible. This is the first enforced end-to-end guard against using a
benchmark profile measured on a different derived dataset.

2026-09-10 cost-model correction to make before claiming a calibrated
objective: expose a resource vector rather than immediately collapsing values
into one scalar: resident state memory (bytes), ingest CPU rate (CPU-s/s),
query CPU rate (CPU-s/s), and optional query working-memory constraint. Only
combine these with declared unit-bearing weights (for example, $/(byte-s) and
$/CPU-s), or state a multi-objective/constraint policy. Do not charge temporary
query memory as a per-query scalar without a measured lifetime. Keep KLL mean
rank error as the sole eligibility constraint for now. Measure the actual raw
Prometheus/exact query path rather than use a forced exact constant.

2026-09-10 policy decision: minimize actual deployment cost subject to the
mean-rank-error SLA and a latency SLA. For the stated 16-vCPU, 21-GB instance
at $0.638/hour, one provisioned instance costs $459.36 per 30-day month. Do
not manufacture a per-workload dollar saving from lower CPU/RAM use when two
plans both fit on one fixed-price node: their standalone provisioned price is
identical. Instead estimate aggregate workload CPU demand and resident memory,
compute required instances as the maximum of the CPU- and memory-capacity
ceilings, and price that integer capacity. Savings arise when a lower-resource
plan permits more workload packing or one fewer instance. The latency SLA
requires a separately defined end-to-end latency estimate/measurement; atomic
CPU time alone is not a latency prediction.

2026-09-10 first cost-model scope refinement: use CPU seconds only; defer
deployment-price/memory policy. For an unfiltered query, approximate query
read fanout by `N_G`, the number of distinct grouping-label tuples (not the
cardinality of individual labels unless there is exactly one grouping label).
For KLL, one grouping tuple has one KLL and `F=N_G`. For a CMS grouped by
`label_0` whose key is `(label_1,label_2)`, one CMS exists per `label_0`
value and `F=N_G=cardinality(label_0)`; key labels affect the measured CMS
profile/accuracy but do not multiply the number of CMS instances. Define
request CPU/latency conservatively as `F * (query_cpu + (windows_read-1) *
merge_cpu)` and optimize total CPU rate `ingest_cpu_rate + Σ frequency *
request_cpu`, subject to error and this atomic-CPU latency SLA. The current
planner violates this intended CMS rule: its `subpopulation_aware` branch
uses the hardcoded `SUBPOPULATION_COUNT=1`. Replace that placeholder with
explicit grouping-state count and query-fanout fields before adding CMS
experiments.

2026-09-10 terminology decision: `N_G` is the cardinality of the **distinct
tuples** formed by the labels that partition/deploy sketches, calculated from
the wrangled series inventory. It is not the product of individual label
cardinalities and does not include key labels. A true global partition has
`N_G=1`; an ordinary temporal query whose planner preserves all source labels
uses the cardinality of the full exported-series tuple. Retire
`subpopulation_aware` from the public cost-model vocabulary unless the runtime
actually uses a distinct physical multi-group container model. Deployment
price is deferred. Do not use a burstable CPU-credit instance such as t2.nano
as the reference for a CPU-seconds/latency model: its sustained CPU capacity
is not represented by its one nominal vCPU.

2026-09-10 implementation: `accuracy_sla` and `latency_sla` are now the sole
public optimizer feasibility inputs. The optimizer derives KLL's internal
mean-rank-error limit as `1 - accuracy_sla`, and derives an optional maximum
whole-request atomic CPU limit from positive `latency_sla`. Candidate request
CPU is rejected when it exceeds that limit. CPU-only selection is enabled by
zeroing the legacy memory weights. CMS now scales query work by its explicit
grouping-state count rather than the old hardcoded-one `subpopulation_aware`
branch; ingest CPU remains based on total input arrival rate and therefore
does not multiply by grouping count. Focused tests cover public-SLA conversion,
KLL accuracy conversion, and CMS query scaling.

## #3: What feasibility evidence constrains optimization?

Blocked by: #1, #2
Type: Discuss

### Question

For each query/sketch/config/dataset case, which accuracy metric and threshold
make a candidate eligible, and how will exact-query cost and arrival rate be
measured rather than assumed?

### Answer

`sketch-bench` retains capability-specific `query_accuracy`, and the greedy
optimizer now enforces `query_accuracy.mean_rank_err` for KLL when
`max_mean_rank_error` is supplied. Arrival rate (`rho`) and exact-query cost
remain placeholders for an end-to-end comparison. After the K-versus-K
holdout, measure the actual raw/exact query path for the same scenario and
replace the temporary diagnostic exact baseline; then test whether the
optimizer selects exact or approximate according to observed cost under the
same accuracy limit.
