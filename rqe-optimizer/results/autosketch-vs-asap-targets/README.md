# AutoSketch vs. ASAP across accuracy targets (90%, 95%, 99%)

The §6.3 evaluation (`../autosketch-vs-asap-hydra/`) holds every RQE to 95%
accuracy. This sweep reruns the same 12 workloads at 90% and 99%. The level
applies in each sketch family's own metric:

| Accuracy target | Quantile: KLL rank error, DDSketch relative value error | Distinct count: HLL relative error | Top-k: precision@k |
|---|---|---|---|
| 90% (`acc90`) | ≤ 0.10 | ≤ 0.10 | ≥ 0.90 |
| 95% (`acc95`; the committed runs' `p95`) | ≤ 0.05 | ≤ 0.05 | ≥ 0.95 |
| 99% (`acc99`) | ≤ 0.01 | ≤ 0.01 | ≥ 0.99 |

Exact accumulators (sum, increase) have zero error at every level. Everything
else is unchanged: the same tables, cost table, curves and Hydra study, and
the same runner (`--target acc90` / `acc99`). The 95% results are
`../autosketch-vs-asap-hydra/synthetic-*-tp95.json`.

Every run: 0 RQEs dropped and 0 sanity violations.

## Results

Each method's cheapest plan (no latency bound), CPU only, in vCPU. The
`summary_targets.md` file also has the Fargate weights and every scaled workload.

| Workload | Target | ASAP | ASAP (no roll-ups) | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|
| mixed (50 RQEs) | 90% | 3.54 | 3.54 | 10.5 | 3,716 (1.0 s) |
| | 95% | 3.54 | 3.54 | 10.5 | 3,716 (1.0 s) |
| | 99% | 3.85 | 3.85 | 10.9 | 3,757 (1.0 s) |
| multi-grouping (62 RQEs) | 90% | 0.221 | 0.551 | 1.07 | 10.2 (0.49 s) |
| | 95% | 0.221 | 0.551 | 1.07 | 10.2 (0.49 s) |
| | 99% | 0.627 | 0.979 | 1.49 | 9.94 (1.57 s) |
| mixed + multi-grouping (112 RQEs) | 90% | 3.76 | 4.09 | 11.6 | 3,726 (1.0 s) |
| | 95% | 3.76 | 4.09 | 11.6 | 3,726 (1.0 s) |
| | 99% | 4.47 | 4.82 | 12.3 | 3,767 (1.57 s) |
| multi-grouping, 16 metrics (992 RQEs) | 95% | 3.53 | 8.81 | 17.1 | 163 (0.49 s) |
| | 99% | 10.0 | 15.7 | 23.8 | 159 (1.57 s) |

**90% costs the same as 95%, for every method and workload.** At 95% the
plans already use the cheapest measured config of each sketch that can
serve them. On mixed + multi-grouping, for example, the plan uses HLL
`lg_k = 12` (the smallest measured), DDSketch α = 0.05 and 0.02, and top-k
3 × 1024. The measured grid has nothing cheaper for a looser target to
switch to. Cost is also mostly ingest (CPU per sample), which barely depends
on sketch size. A sweep below 95% would need smaller configs measured first
(e.g. HLL `lg_k` 10, DDSketch α 0.1).

**99% costs more, most where sketches dominate the cost.**
- On the mixed set, ASAP's cost rises 9% (3.54 → 3.85 vCPU). Its cost is
  mostly exact accumulators and per-series ingest, which the target doesn't
  touch.
- On multi-grouping, which is all sketches, ASAP's cost rises 2.8× (0.221 →
  0.627 vCPU, and 3.53 → 10.0 at 16 metrics). HLL moves to `lg_k` 14 and 16,
  and quantiles to DDSketch α = 0.02.
- ASAP stays the cheapest at every target: 2.4× below PerQuery on
  multi-grouping and 2.8× on mixed, and 16× (multi-grouping) to ~1,000× (mixed)
  below AutoSketch.

**Roll-ups still help at 99%, but less.** On multi-grouping they save 36%
(0.979 → 0.627 vCPU) at 99%, against 60% at 90% and 95%, and ASAP rolls up
42 of the 62 RQEs, against 52.

**AutoSketch's plan gets slower at 99%.** Its batch latency rises from 0.49 or
1.0 s to 1.57 s. Its multi-grouping cost falls slightly (10.2 → 9.94 vCPU):
it minimizes memory, not cost, so its cost need not rise with the target.

## Figures

- `fig_cost_vs_target.png`: each method's cheapest plan vs. the accuracy
  target, per workload (mixed, multi-grouping, mixed + multi-grouping;
  columns) and weight setting (rows: CPU in vCPU, Fargate in $/hour).
- `fig_cost_vs_target_at_autosketch_latency.png`: the same, with ASAP and
  PerQuery held to AutoSketch's batch latency at each target.
- `summary_targets.md`: every workload, target and weight setting: each
  method's cost, ASAP's latency, deployments and roll-ups, RQEs dropped.

## Reproduce

```sh
cargo build --release -p rqe-optimizer --examples
H=rqe-optimizer/results/autosketch-vs-asap-hydra
D=rqe-optimizer/results/autosketch-vs-asap-targets
SAT=$(mktemp -d); cp -r rqe-optimizer/results/autosketch-vs-asap-inputs/saturation/out_* $SAT/
cp -r $H/inputs/saturation/* $SAT/
for acc in acc90 acc99; do
  cut -f1 $H/inputs/tables/plan.tsv | while read t; do
    target/release/examples/autosketch_vs_asap synthetic --table $H/inputs/tables/$t \
        --target $acc --runs 3 --no-chosen --saturation-dir $SAT --out $D/${t%.json}-t$acc.json
  done
done
python3 scripts/plot_autosketch_vs_asap_targets.py $D $H/synthetic-*-tp95.json $D/synthetic-*-tacc*.json
```

Runs: 2026-10-11, one Xeon E5-2683 v3 node (56 cores), 6 runs at a time.
Planning times are not compared here: the runs shared the machine.
AutoSketch's benchmark time was not re-measured for the new targets' probes.
