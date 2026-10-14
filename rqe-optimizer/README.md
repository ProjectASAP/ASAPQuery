# rqe-optimizer: data, results, and how to use them

`rqe-optimizer` plans sketch deployments for a workload of RQEs (repeating
query expressions): it generates candidate deployments, prices them, and
solves a MILP for the cheapest plan that meets every RQE's accuracy target.
This page covers the data it reads and the results committed here: what each
file is, how to read it, and how to rerun or reuse it. The models themselves
are in `docs/rqe_optimizer_cost_model.md`, `docs/rqe_optimizer_candidates.md`
and `docs/rqe_optimizer_hydra.md`. The evaluation plan is
ProjectASAP/ASAPQuery#777 (`docs/evaluation/autosketch-vs-planner.md`).

## Layout

| Path | What it is | Status |
|---|---|---|
| `src/` | the optimizer: `candidates.rs` (candidates, roll-ups, Hydra), `usage.rs` (cost by use, batch latency), `milp.rs` (`minimize_usage_cost`), `autosketch.rs` (the AutoSketch baseline), `saturation.rs` (accuracy lookup) | |
| `examples/autosketch_vs_asap.rs` | the §6.3 runner: one workload in, one result JSON out | |
| `examples/workload_scenarios.rs` | how the optimal plan moves as one knob of a workload changes (types, groupings, windows, SLAs) | |
| `examples/small_problem.rs` | a toy instance solved end to end (brute force or MILP) | |
| `results/autosketch-vs-asap-hydra/` | **the current §6.3 results** (2026-10-11; classic and all rerun 2026-10-13 without templates 7 and 9; all rerun 2026-10-14 on the denser grid, #213): roll-ups, the roll-up ablation, Hydra candidates, the classic, multi-grouping and mixed + multi-grouping workloads, and each plan | current |
| `results/e2e-rate10/` | the classic, multi-grouping and all workloads (r = m = 1) re-planned at 10 samples/s per series, for the end-to-end run (on the grid before #213's denser one) | current (e2e input) |
| `results/autosketch-vs-asap-inputs/` | the accuracy curves every run reads (#138's study), and #138's tables and raw records | current (shared input) |
| `results/autosketch-vs-asap-synthetic/`, `results/autosketch-vs-asap-synthetic-sla/` | the 2026-10-08 results (#138): before roll-ups and the KLL footprint fix | superseded by `-hydra` |
| `data/autosketch-eval/table.json` | the trace workloads (Alibaba 2022, Google 2011, BOOM) | not yet rerun on cost by use |

Use `results/autosketch-vs-asap-hydra/` for anything new. Its `README.md` has
the full analysis; this page explains the files.

## Reading the results

Each workload is one file, `synthetic-templates<set>-shared<r>[-metrics<m>]-tp95.json`:

| Name part | Meaning |
|---|---|
| `templatesclassic` | the mixed set: 8 PromQL templates on one metric (42 RQEs); templates 7 and 9 (`sum by (job) (rate(...))`, `topk(32, sum by (label_0) (rate(...)))`) are dropped, as ASAPQuery would need per-series counter deltas to serve them |
| `templatesmultigroup` | the multi-grouping templates alone: distinct counts and p99s over several groupings of `http` and `flows`, including p99 `GROUP BY CUBE(region, service, endpoint)` (62 RQEs) |
| `templatesall` | classic + multi-grouping (104 RQEs) |
| `shared8` | 8 users query the same streams with their own windows, quantiles and repeat intervals |
| `metrics8`, `metrics16` | 8 or 16 independent copies of the workload, each on its own metric |
| `tp95` | every RQE must meet 95% accuracy in its sketch's own metric |

Start with the summaries and figures:

- `summary_synthetic.md`: every method's cheapest plan and every frontier
  point, per workload and weight setting.
- `summary_sla.md`: each method's cost at each batch-latency SLA.
- `fig_frontier.png`, `fig_cost_vs_sla.png`, `fig_planning_time.png`,
  `fig_rollup_ablation.png` (the README describes each).

### A result file

| Key | Meaning |
|---|---|
| `workload`, `rqes`, `streams` | the workload, its RQE count and its streams |
| `weights` | the weight settings: `cpu` (`w_cpu = 1`, cost in vCPU) and `fargate` ($/vCPU-hour and $/GB-hour, cost in $/hour) |
| `results` | version 1, no SLA: each method's cheapest plan (`bound_ms: null`) and its frontier points (`bound_ms`: the latency bound it was solved under) |
| `sla_results` | version 2: each method's cheapest plan at each SLA (`sla_ms`); `infeasible` when no plan meets it; AutoSketch's has `meets_sla` |
| `frontier` | each method's tightest feasible bound and AutoSketch's latency |
| `autosketch` | AutoSketch's search: probes, `search_secs`, `benchmark_secs_measured` (its benchmark runs, timed) |
| `dropped_unservable`, `sanity_violations` | RQEs no method could serve, and failed checks (ASAP ≤ no roll-ups ≤ PerQuery, ASAP ≤ AutoSketch); both are empty in every committed run except one solver-tolerance frontier check in `autosketch-vs-asap-targets` (see its README) |
| `notes` | what the runner assumed |

Every entry of `results` and `sla_results` is one plan:

| Field | Meaning |
|---|---|
| `method` | `asap`, `asap-norollup` (the roll-up ablation), `perquery` (no sharing), `autosketch` |
| `weights` | `cpu` or `fargate` |
| `objective` | the plan's cost by use, in the weights' unit |
| `cpu`, `cpu_parts` | mean vCPUs, split into ingest, compaction, query |
| `gib`, `gib_parts` | mean GiB, split into ingest, storage, compaction, query |
| `latency_ms`, `median_latency_ms` | batch latency (the longest compaction + query chain) and the median over RQEs |
| `active_deployments` | deployments the plan runs |
| `rolled_up_rqes`, `hydra_rqes` | RQEs served by merging a finer deployment's groups, and by a Hydra grid |
| `planning_secs`, `milp_solve_secs` | planning time |

The cheapest plan of each method:

```python
import json
d = json.load(open("synthetic-templatesmultigroup-shared1-tp95.json"))
for r in d["results"]:
    if r["weights"] == "cpu" and r.get("bound_ms") is None:
        print(r["method"], r["objective"], r["latency_ms"], r["active_deployments"])
```

Every method at the same latency (the paper table's comparison): take each
method's cheapest plan with `latency_ms <= d["frontier"]["autosketch_latency_ms"]`.

### Plans

The result files leave out which deployment serves each RQE (the runner's
`--no-chosen`). The plans are in `plans/<table>.plans.json.gz`. Each file
lists, for every method and weight setting, the cheapest plan
(`kind: "cheapest"`) and the plan at each feasible SLA (`kind: "sla"`); each
plan's `chosen` has one entry per RQE:

| Field | Meaning |
|---|---|
| `rqe` | the RQE's id: `<dataset>/<query_id>/<kind>/<range>` |
| `sketch`, `params` | the sketch and its config |
| `window_secs`, `slide_secs` | the deployment's window and slide |
| `merged_shards` | the windows a query merges (lookback / window) |
| `deployment` | the deployment's index: RQEs with one index share one deployment |
| `deployment_grouping` | only when it differs from the RQE's: the finer grouping it rolls up from, or a Hydra grid's schema |
| `ingest_workers`, `latency_ms` | ingest workers, and the RQE's compaction + query chain |
| `asap_accuracy`, `accuracy_source` | estimated error, and whether it was measured or from theory |
| `covers_share` | the RQE's coverage rule, if any |

```sh
zcat plans/synthetic-templatesmultigroup-shared1.plans.json.gz | jq '.plans[]
  | select(.kind == "cheapest" and .method == "asap" and .weights == "cpu") | .chosen'
```

To turn a plan into deployments, group `chosen` by `deployment`: each group is
one sketch (`sketch`, `params`) over the stream, kept per group of its
grouping, in windows of `window_secs` that start every `slide_secs`, serving
the listed RQEs. A deployment is tumbling when the two are equal and sliding
otherwise (the classic set's cheapest ASAP plan at CPU weights has a 300 s
window with a 60 s slide).

## The inputs

### Workload tables

`results/autosketch-vs-asap-hydra/inputs/tables/*.json`, from
`scripts/export_autosketch_eval_table.py --synthetic`. They are built as RQEs
directly; no PromQL is parsed. `plan.tsv` lists one run per table. A table
has `workloads[0].rqes` (one entry per RQE) and `workloads[0].schemas` (each
multi-grouping metric's labels, cardinalities and skew). An RQE:

| Field | Meaning |
|---|---|
| `id`, `query_id`, `range` | its name, and its lookback as written (`5m`) |
| `capability` | `quantile`, `cardinality`, `topk`, `sum`, `rate` |
| `stream`, `metric` | the data it reads (`http/latency`); RQEs on one stream can share deployments |
| `grouping` | its group-by labels (multi-grouping RQEs; others group per series or by job) |
| `lookback_secs`, `interval_secs` | lookback `S` and repeat interval `T` |
| `families` | the sketches that may serve it, each with its accuracy `target` and the data shape it is read at (`grid_param`: Zipf θ or Pareto a; `grid_K`: distinct keys) |
| `label_set` | groups, samples/s of the stream, keys per window |
| `min_N`, `max_N` | items per group per window (mean group) |
| `covers_share`, `covered_min_N`, `covered_max_N` | the coverage rule (only groups holding at least this share of records, plus the largest; `null`: every group) and the items of its smallest and largest covered group, where accuracy is read |

Every table also has `workloads[0].e2e`, for running a plan end to end
(sketch-bench#204). The runner does not read it, so the plans are unchanged.

| Field | Meaning |
|---|---|
| `e2e.queries` | classic: one entry per PromQL query and repeat interval: `promql`, `interval_secs`, and the `rqes` it reads (template 10's ratio reads two). Multi-grouping (no PromQL): one entry per distinct query and interval: canonical ASAPQuery `sql` over `NOW()` (the planner floors and replaces it), plus `table`, `value`, `capability`, `grouping`, `quantile` (p99: 0.99), `window_secs`, `interval_secs`, and the `rqes` it reads (sorted; templates 14, 15 and 17 share some groupings) |
| `e2e.metrics` | the metrics to generate. Classic: gauge `data` (sums, quantiles, top-k by sum) and counter `data_total` (rates), with `labels` in the `schemas` format (`label_0` is `child_of: job`, 1,000 per job, so 1e4 series), samples/s per series, and each stream's value shape. Multi-grouping: `type: "records"` tables `http` and `flows` (`data_1_http` for metric `data_1/http` in `metrics=m` tables) with the schema's `labels`, `records_per_sec` and `values` (shape, `scaled_by`, `anomaly`, `burst`) |

### Accuracy and cost

| File | Rows | Used for |
|---|---|---|
| `autosketch-vs-asap-inputs/saturation/out_grid_1e7_cost/saturation_curve.csv` | error vs. items `n`, per sketch config and data shape (`dist`, `param`, `cardinality`), seed mean | a deployment's accuracy at its items per instance |
| `.../out_grid_1e7_cost/saturation_merge_curve.csv` | the same after merging `m` shards | KLL and top-k when a query merges windows |
| `.../out_1e9/` | points past 1e7 items | large windows |
| `autosketch-vs-asap-hydra/inputs/saturation/optimizer_cost/rqe_atomic_costs.json` | one row per config: `insert_cpu_secs`, `merge_cpu_secs`, `query_cpu_secs` (per operation), `mem_bytes_per_instance`, and `measured_at` (items, data shape; for Hydra, its `dataset` and `schema_width`) | every plan's cost |
| `autosketch-vs-asap-hydra/inputs/saturation/hydra_saturation.csv` | Hydra error per (variant, config `R`×`W`, dataset, grouping, records, seed): mean, p50, p90, max, and max over groups holding ≥ 1% / 5% (`err_max_cov_*`) | Hydra grids' accuracy |

The raw study records are in `autosketch-vs-asap-inputs/raw/`.

## How to use them

**Reproduce the committed results.** Build, assemble one saturation directory
(the curves plus this run's cost table and Hydra study), and run every row of
`plan.tsv`; the exact commands are in
`results/autosketch-vs-asap-hydra/README.md`, "Reproduce". Costs are
deterministic; planning times depend on the machine.

**Run a new workload.** Write a table in the format above (add templates to
`SCHEMA_TEMPLATES` in `scripts/export_autosketch_eval_table.py`, or write the
JSON yourself) and run:

```sh
target/release/examples/autosketch_vs_asap synthetic --table MY_TABLE.json \
    --target p95 --runs 1 --saturation-dir SAT --out MY_RESULT.json
```

Leave out `--no-chosen` to keep each plan's `chosen`; add `--weights cpu` or
`--slas-ms 1000` to run less. A new data shape (θ, K, a) or sketch config not
on the measured grid needs a new saturation study
(`scripts/study_saturation.py`); new windows, intervals, groupings or
queries on the same shapes do not.

**See how the plan reacts to one change.** `examples/workload_scenarios.rs`
steps one knob of a base workload and prints the deployments chosen:

```sh
cargo run --release -p rqe-optimizer --example workload_scenarios -- \
    --saturation-dir SAT [--sweep NAME]
```

**Run plans end to end.** The plans give each deployment's sketch, config,
grouping and window; ASAPQuery's planner (`asap-planner-rs`, the MILP port)
produces the same kind of plan from PromQL or SQL. Hydra grids are not in
ASAPQuery's deployable families, and no committed plan uses one.

## Caveats

- Costs and latencies are estimates from per-operation measurements, not
  measured end-to-end runs.
- CPU is elastic and billed by use (an idealized autoscaler), so cost is a
  lower bound for provisioned machines.
- A merged top-k heap of `m · k` is assumed to keep the true top `k`.
- The trace workloads (`data/autosketch-eval/`) have not been rerun on cost
  by use.
