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
else is the 95% runs': the same tables (sketch-bench#210's, without classic templates 7
and 9, with sketch-bench#215's one-scrape instant RQEs), the cost table, curves and Hydra
study with sketch-bench#213's denser grid, and
the same runner (`--target acc90` / `acc99`). The 95% results are
`../autosketch-vs-asap-hydra/synthetic-*-tp95.json`.

Every run: 0 RQEs dropped, 0 sanity violations.

## Results

Each method's cheapest plan (no latency bound), CPU only, in vCPU. The
`summary_targets.md` file also has the Fargate weights and every scaled workload.

| Workload | Target | ASAP | ASAP (no roll-ups) | PerQuery | AutoSketch (latency) |
|---|---|---|---|---|---|
| mixed (42 RQEs) | 90% | 2.71 | 2.71 | 7.67 | 2,701 (1.0 s) |
| | 95% | 2.71 | 2.71 | 7.67 | 2,701 (1.0 s) |
| | 99% | 2.90 | 2.90 | 7.95 | 2,741 (1.0 s) |
| multi-grouping (62 RQEs) | 90% | 0.220 | 0.550 | 1.07 | 9.65 (0.49 s) |
| | 95% | 0.220 | 0.550 | 1.07 | 9.65 (0.49 s) |
| | 99% | 0.627 | 0.979 | 1.49 | 9.94 (1.6 s) |
| mixed + multi-grouping (104 RQEs) | 90% | 2.93 | 3.26 | 8.74 | 2,711 (1.0 s) |
| | 95% | 2.93 | 3.26 | 8.74 | 2,711 (1.0 s) |
| | 99% | 3.53 | 3.88 | 9.44 | 2,751 (1.6 s) |
| multi-grouping, 16 metrics (992 RQEs) | 95% | 3.52 | 8.80 | 17.1 | 154 (0.49 s) |
| | 99% | 10.0 | 15.7 | 23.8 | 159 (1.6 s) |

**90% costs the same as 95%, for every method and workload, even with the
denser grid.** HLL, which multi-grouping is mostly made of, still bottoms out
at `lg_k = 12` (asap_sketchlib builds 12, 14 and 16 only). DDSketch α = 0.1,
the new coarsest config, reads 0.048 relative error, so it meets 95% as well
as 90%. Cost is also mostly ingest (CPU per sample), which barely depends on
sketch size (KLL ≈ 30 ns and DDSketch ≈ 19 ns per item at every config).

**99% costs more, most where sketches dominate the cost.**
- On the mixed set, ASAP's cost rises 7% (2.71 → 2.90 vCPU). Its cost is
  mostly exact accumulators and per-series ingest, which the target doesn't
  touch.
- On multi-grouping, which is all sketches, it rises 2.8× (0.220 → 0.627
  vCPU, and 3.52 → 10.0 at 16 metrics). HLL moves to `lg_k` 14 and 16.
- ASAP stays the cheapest at every target: 2.4× below PerQuery on
  multi-grouping and 2.7× on mixed at 99%, and 16× (multi-grouping) to
  ~1,000× (mixed) below AutoSketch.

**Roll-ups still help at 99%, but less.** On multi-grouping they save 36%
(0.979 → 0.627 vCPU) at 99%, against 60% at 90% and 95%, and ASAP rolls up
42 of the 62 RQEs, against 52.

**AutoSketch's plan gets slower at 99%.** Its batch latency rises from 0.49 or
1.0 s to 1.6 s on the workloads with multi-grouping templates. Its
multi-grouping cost barely moves (9.65 → 9.94 vCPU): it minimizes memory, not
cost.

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
python3 rqe-optimizer/scripts/plot_autosketch_vs_asap_targets.py $D $H/synthetic-*-tp95.json $D/synthetic-*-tacc*.json
```

Runs: 2026-10-14, the 4 CloudLab nodes clnode109/155/167/178 (Xeon E5-2683
v3, 56 cores), each running its share serially, with the 95% runs. The
classic and all tables were rerun with sketch-bench#215's one-scrape instant RQEs
(2026-10-14, sketch-bench main at acc2db1, serially on a 16-core desktop);
the multi-grouping runs are unchanged. Planning
times are not compared here. AutoSketch's benchmark time was not measured for
the 90% and 99% probes.
