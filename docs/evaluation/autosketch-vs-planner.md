# Evaluation plan: AutoSketch vs. the ASAPQuery planner (paper §6.3)

Status: plan, no results yet. Open decisions are listed in §9 with a
recommended answer each; nothing in sketch-bench is implemented until they are
settled.

## 1. Question

Given the same measured sketch costs and the same workload of repeating query
expressions (RQEs), how much cheaper is the plan from ASAPQuery's planner than
the plans from AutoSketch, and how long does each take to plan?

AutoSketch ([NSDI '24](https://www.usenix.org/system/files/nsdi24-sun.pdf),
Algorithm 4) differs from ASAPQuery's planner in four ways that matter here:

| | AutoSketch | ASAPQuery planner |
| --- | --- | --- |
| Unit of optimization | One query at a time | The whole batch of RQEs, jointly |
| Repetition over time | Not modeled | Lookback `S` and repeat interval `T` drive window/slide choice |
| Sharing | None: each query gets its own sketch | One deployment may serve several compatible RQEs |
| Constraints | Accuracy only | Accuracy and per-RQE query latency |
| Objective | Resource use (memory) | Weighted CPU + memory cost from EC2 prices |

So the comparison invokes AutoSketch **once per RQE** (one QE at one repeat
interval) and sums the results; the planner is invoked once for the batch.

## 2. What already exists

| Piece | Where | Status |
| --- | --- | --- |
| AutoSketch Algorithm 4 adaptation (LHS seeds, feasibility-directed width/depth neighbor search, pruning) | ASAPQuery-backend `data_plane/examples/autosketch_comparison.rs` ([#547](https://github.com/ProjectASAP/ASAPQuery-backend/pull/547)) | Merged. CMS/Count Sketch/Bloom only; hardcoded CMS grid; executes sketches to measure accuracy. |
| Earlier protocol (E1–E3, end-to-end execution) | ASAPQuery-backend `docs/evaluation/autosketch-comparison.md` ([#545](https://github.com/ProjectASAP/ASAPQuery-backend/pull/545)) | Merged. Execution-based; this plan is planner-level and uses estimated costs instead. |
| Top-K dashboard comparison | ASAPQuery-backend [#602](https://github.com/ProjectASAP/ASAPQuery-backend/pull/602) | Closed, not merged. |
| RQE deployment MILP (HiGHS): candidates `(capability, config, labels, x, y)`, sharing, latency bounds, minimum-CPU objective | sketch-bench `rqe-optimizer/` ([#129](https://github.com/ProjectASAP/sketch-bench/pull/129)) | Merged 2026-10-04. **This is the planner we evaluate for now.** |
| Measured per-operation costs (`AtomicCostEntry`: memory/instance, insert/merge/query CPU, accuracy) | sketch-bench `scripts/export_rqe_optimizer_costs.sh` | Merged; 18 rows, 2 configs per sketch variant. |
| Moving the MILP into ASAPQuery's planner | ASAPQuery `asap-planner-rs/src/optimizer/` (Milind; related: [#776](https://github.com/ProjectASAP/ASAPQuery/pull/776), [#725](https://github.com/ProjectASAP/ASAPQuery/pull/725)) | In progress. The evaluation does not wait for it (§8, PR 5). |

No AutoSketch implementation exists in sketch-bench or in this repository.

## 3. Methods compared

All methods read the same `AtomicCostTable`, the same RQEs and the same
label-set cardinalities and arrival rates, and are scored by the same cost
function (§4).

1. **ASAP** — sketch-bench `rqe-optimizer` MILP over the whole batch, with
   accuracy and latency constraints, minimizing the §4 cost for one machine
   family.
2. **AutoSketch-Adapted** — Algorithm 4 run independently per RQE:
   - search space: the measured configs of the RQE's capability families
     (`Capability::families()`);
   - constraint: the RQE's accuracy tolerance, read from the measured table;
   - objective: per-instance memory (AutoSketch's register-memory objective),
     tie-break on insert CPU;
   - window adapter: one sliding sketch per query, `x = S`, `y = T` (if
     `S % T != 0`, `y = gcd(S, T)`), so each evaluation reads one instance and
     merges nothing;
   - no sharing: every RQE gets its own deployment, even when two RQEs pick an
     identical one, so ingest and memory are paid per RQE;
   - latency is ignored during search, then checked after.
3. **PerQuery-CostAware** (ablation) — the ASAP MILP solved on each RQE alone,
   without latency bounds, and the results summed. It uses the same objective
   and window choices as ASAP but no batching or sharing. ASAP vs. this ablation
   isolates the batch/sharing benefit; this ablation vs. AutoSketch-Adapted
   isolates the objective/window benefit.

AutoSketch-Adapted is a planner baseline, not a reproduction of the P4
compiler; stage/page/ALU constraints are dropped, and its accuracy oracle is
the sketch-bench table instead of online sketch execution (§9 Q3).

## 4. Cost model

Disk is excluded (agreed with Milind). Units: CPU in vCPU (CPU-seconds per
second), memory in GiB.

**CPU** — already in `rqe_optimizer::objectives::score`:

```text
CPU = Σ_active D  λ(ℓ_D) · (x_D / y_D) · insert_cpu_D                      (ingest)
    + Σ_r  card(ℓ_r) · (query_cpu_D(r) + (S_r / x_D(r) − 1) · merge_cpu_D(r)) / T_r   (query + merge)
```

**Memory** — new. Today's objective tracks peak per-query memory, not retained
state. Retained state of an active deployment `D` holds `x/y` open instances
plus the closed instances needed by the longest lookback it serves:

```text
Mem_D = card(ℓ_D) · mem_bytes_per_instance_D · (x_D + max_{r→D} S_r) / y_D
Mem   = Σ_active D  Mem_D
```

In the MILP the `max` is linear: `Mem_D ≥ coef(r, D) · z_{r,D}` for each
eligible `r`.

**Price per machine family** — for family `f` with `vCPU_f`, `GiB_f` and
on-demand `price_f` ($/hour), the plan needs a fractional instance count
`n_f ≥ CPU / vCPU_f` and `n_f ≥ Mem / GiB_f`; cost is `price_f · n_f`. This
adds one continuous variable and two constraints, and needs no arbitrary split
of an instance's price between CPU and memory. Families:

| Family | Example instance | Role |
| --- | --- | --- |
| Compute-optimized | c7i.xlarge | cheap CPU, scarce memory |
| General purpose | m7i.xlarge | balanced |
| Memory-optimized | r7i.xlarge | cheap memory, scarce CPU |

Storage-optimized families are dropped with disk. Prices are fetched once from
the AWS Pricing API (us-east-1, Linux, on-demand), committed as
`rqe-optimizer/data/ec2-pricing-<date>.json` with the query used, and never
edited by hand. Since `n_f` is fractional, instance size within a family does
not change the result.

ASAP is solved once per family. AutoSketch-Adapted's plan does not depend on
the family; it is scored under each family's cost.

**Latency** — per-RQE estimate already in sketch-bench:
`card(ℓ) · (query_cpu + (S/x − 1) · merge_cpu)`.

## 5. Constraints

**Accuracy target 95%**, mapped per capability to the metrics the cost table
already records:

| Capability | Metric | Constraint |
| --- | --- | --- |
| Freq | relative error | ≤ 0.05 |
| Quantile | rank error | ≤ 0.05 |
| Cardinality | relative error | ≤ 0.05 |
| TopK | precision@k | ≥ 0.95 |

**Latency** — per-RQE limit `L_r = α · min latency over r's eligible
deployments`, swept over `α ∈ {1.5, 2, 5, ∞}`. Using a multiple of the
fastest option keeps every point feasible for ASAP and makes the bound bind.
AutoSketch-Adapted ignores it; its violations are counted and reported, and its
cost is shown for those points but marked as infeasible.

## 6. Workloads

| ID | Description | Purpose |
| --- | --- | --- |
| W0 | `small_problem`'s 8 RQEs (freq, quantile, cardinality, top-k; 1h–1d lookbacks; 60s/300s intervals), tolerances moved to §5 | Readable worked example; one table in the paper |
| W1 | Seeded synthetic batches, `N ∈ {8, 32, 128, 512, 2048}` RQEs. Lookbacks {5m, 15m, 1h, 6h, 1d}, intervals {10s, 60s, 300s}, 4 capabilities, 3 label sets with fixed cardinality/rate. Knob: fraction of RQEs drawn from shared (capability, labels) cohorts, {0, 0.5, 1}. | Planning-time scaling; cost vs. shareability |
| W2 (optional) | RQEs from the Google cluster-trace query sets ([#746](https://github.com/ProjectASAP/ASAPQuery/pull/746)), with label cardinalities and rates measured from the trace | Realistic mix |

The cost table used for the evaluation needs a wider grid than today's two
configs per variant, otherwise Algorithm 4's neighbor search has nothing to
search: CMS/Count Sketch depth {2..8} × width {256..8192}, KLL k
{50..800}, DD α {0.005..0.05}, HLL precision {10..16}. Data: Zipf, as in the
existing export script.

## 7. Metrics and figures

Reported per (workload, method, machine family, α), median of 10 runs for
timings:

- **Planning time.** AutoSketch: sum of per-RQE search wall time, plus the
  number of accuracy probes. ASAP: candidate generation + dominance pruning +
  MILP solve. Offline sketch-bench profiling is shared by both and reported
  separately, once.
- **Total cost** ($/hour) and its breakdown: ingest CPU, query + merge CPU,
  memory, and which resource binds `n_f`.
- **Latency SLA violations** per method.
- **Estimated accuracy** per RQE (all methods meet it on single-instance
  measurements by construction).
- Active deployments and total sketch instances.

Figures:

1. Total cost by method, grouped by machine family (W0 and W1 at N = 128).
2. Planning time vs. N, log–log (W1).
3. Cost vs. shareability (W1).
4. Cost vs. latency limit α (W0, W1).

## 8. Who implements what, in which PR

| # | Repo | Change | Owner |
| --- | --- | --- | --- |
| this | ASAPQuery | This plan (`docs/evaluation/autosketch-vs-planner.md`) | Zeying |
| 1 | sketch-bench | `rqe-optimizer`: retained-memory term in `objectives.rs`; `milp::minimize_cost` with per-family price (§4); committed EC2 pricing JSON; dominance pruning also compares retained memory, so it cannot drop a candidate that is cheaper under the new objective. Tests: brute-force agreement on the tiny workload, as `#129` already does for CPU. | Zeying, coordinated with Milind since he is porting `milp.rs` |
| 2 | sketch-bench | Wider evaluation grid in `scripts/export_rqe_optimizer_costs.sh` (or a sibling script) and the resulting committed table | Zeying |
| 3 | sketch-bench | `rqe-optimizer/src/autosketch.rs`: Algorithm 4 ported from ASAPQuery-backend `autosketch_comparison.rs`, generalized from the CMS width/depth grid to each variant's measured parameter axes; one dedicated `Deployment` per RQE. Tests: picks the smallest feasible config on a grid; never shares; its window adapter output is eligible under `candidates::is_eligible`. | Zeying |
| 4 | sketch-bench | `rqe-optimizer/examples/autosketch_vs_asap.rs` (W0/W1 generators, all three methods, JSON output) and `scripts/plot_autosketch_vs_asap.py`; committed results and figures | Zeying |
| 5 | ASAPQuery | After the MILP lands in `asap-planner-rs`: port PR 1's objective there and rerun PR 4 against it, so the paper reports the planner that ships | Zeying + Milind |

PRs 1 and 2 are independent; 3 depends on 2 for a meaningful grid only; 4
depends on 1–3.

## 9. Open decisions

**Q1. AutoSketch's objective.** Recommended: per-instance memory (faithful to
the paper), plus PerQuery-CostAware as the strong ablation. Alternative: give
AutoSketch the §4 cost per query directly, which removes the objective
difference but no longer resembles AutoSketch.

**Q2. "Once per repeating time".** Recommended: one AutoSketch call per RQE
(QE × repeat interval), reused for every evaluation, since the chosen config
would not change between evaluations. Alternative: one call per evaluation over
a horizon `H`, i.e. planning time `Σ_r (H / T_r) · t_r`, which inflates
AutoSketch's planning time without changing its plan.

**Q3. AutoSketch accuracy probes.** Recommended: table lookup, the same
evidence ASAP uses, so the plans differ only by algorithm. AutoSketch's
planning time then excludes sketch execution; report its probe count so the
cost of online probing can be stated. Alternative: execute each probe, which
needs sketch-bench in the loop and makes planning-time comparisons dominated by
profiling.

**Q4. AutoSketch window adapter.** Recommended: `x = S, y = T`, one sliding
sketch per query. Alternative: tumbling `x = y = gcd(S, T)` with merges at
query time, as a sensitivity run.

**Q5. Memory model.** Recommended: retained state as in §4. Alternative: keep
today's peak per-query memory, which undercounts state for long lookbacks.

**Q6. Machine-family cost.** Recommended: fractional-instance `max` model (§4).
Alternative: fixed linear weights per family, which need an arbitrary
CPU/memory split of each instance's price.

## 10. Known limitations

- **Merged accuracy is not validated.** The cost table measures single
  instances. ASAP plans often merge `S/x` instances, while AutoSketch-Adapted
  merges none, so this gap affects ASAP only. Before the paper claims accuracy
  parity, replay at least W0's chosen plans in sketch-bench and report
  post-merge error (`docs/rqe_optimizer_TODO.md`, "Next").
- Costs and latencies are estimates from per-operation measurements, not
  end-to-end executions. The execution-based comparison is ASAPQuery-backend
  #545/#547.
- AutoSketch-Adapted's deployments (`x = S`, `y = gcd(S, T)`) are in the ASAP
  candidate set (`candidates.rs` generates every divisor of `S` as a window and
  `gcd(x, T)` as a slide). So when its plan meets the latency bounds, the ASAP
  MILP can choose the same deployments and pay for shared ones once; ASAP's
  cost is never higher. The result to report is the size of the gap and where it comes
  from, not that a gap exists.
