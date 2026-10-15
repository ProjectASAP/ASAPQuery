# ASAPQuery accelerated random E2E handoff

*Updated 2026-10-15 (session `2578ec80-4c3a-4ede-9be0-ad5eca70b852`, peer name `asapquery-random-e2e-11`).*

## Repositories and working copies

- **ASAPQuery worktree:** `.../code/ASAPQuery-random-e2e`. Don't use the shared `.../code/ASAPQuery`.
  - **Merged:** #879 (the pilot) and #902 (ClickHouse run and CPU-measurement fixes).
  - **Merged:** #903 (the long and x5 variants and the VictoriaMetrics pin).
- **sketch-bench worktree:** `.../code/sketch-bench-random-e2e`, detached at `main` (`a3753f2`).
  - #226 (`--random-manifest` exporter) and #228 (RQE `expr` trees) are merged.
  - Don't use `.../code/sketch-bench`: another session has uncommitted work there.
- **RQE = repeating query expression.**

## Design (`.design_docs/accelerated-random-workload-eval.md`)

- **Workloads:** seed 862, 10 workloads × 10 queries. The five-system pilot uses each workload's 7 `common-promql` queries: 2 sums, 3 quantiles, 2 top-k.
- **Labels:** `region:4 → service:5 → job:10 → label_0:50`, giving 10,000 series at 10 samples/s.
- **Quantile class:** 100s lookback (each series has 1,000 samples), repeat 1s.
- **Rendering:** the manifest is the source of truth. PromQL and ClickHouse SQL are both rendered from the RQEs (`random_workloads.rqe_query` → `sketch_bench_workloads.Query`). PromQL is never translated to SQL.
- **Quantile semantics today:** the average of per-series quantiles on every system. ASAPQuery uses a per-series sketch and a query-time avg (`--planner saved`, per-series avg support).
- **Variants** (`random_workloads.py --variant`) keep every random choice, so results pair query for query with the base:
  - `long`: lookbacks ×3, max 60 min; quantiles 300s.
  - `x5`: `label_0` 250, so 50k series and 500k samples/s.
- **Archive:** each frozen set lives in `random_workloads/seed-862[-long|-x5]/` with `workload-NN.json` and `workload-NN-pilot/` (eval table, plans, ASAPQuery planner configs). Tests check every archive against its manifest.

## Run configs and runners

- **Render:** `random_workload_artifacts.py --manifest <set>/workload-NN.json --out <tmp> --pilot-dir <set>/workload-NN-pilot` writes `config/experiment_type/random_workload/<set>/workload-NN/` (git-ignored).

  | Config | Runner | Systems |
  |---|---|---|
  | `e2e.yaml` | `experiment_run_e2e.py` | ASAPQuery `sketchdb` + Prometheus `baseline`. To run one, override `'experiment_params.experiment=[{mode:sketchdb,server:sketchdb}]'` |
  | `victoriametrics.yaml` | `experiment_run_e2e.py` | VictoriaMetrics |
  | `clickhouse.yaml`, `clickhouse_mv.yaml` | `experiment_run_clickhouse.py` | Raw ClickHouse, ClickHouse+MV |

- **Every arm:**
  - feeds the eval table's `data` metric with seed 862;
  - warms up for the longest lookback + 10s (1,210s base, 3,610s long);
  - then runs a 10-minute query schedule aligned to each interval.
- **Run end:** runs end when their queries finish. The feed's duration (warm-up + 10× the query phase) only bounds a hung run. Feeders stop on SIGTERM and still write their stats.
- **Nodes:** each run takes 2 nodes (`providers.cloudlab.num_nodes=1`, `node_offset=2k`). The database is on the first node, the feeder on the second, for both runners.
- **Pins:**
  - ClickHouse 23.8: 26.6+ crashes with an illegal instruction on the CloudLab E5-2450 nodes (no AVX2).
  - VictoriaMetrics v1.154.0, with `-search.latencyOffset=0s`. The result cache is on.
  - Prometheus is 2.53.2, bare metal.
- **Analysis:** `post_experiment/multi_experiment/compare_arms.py --reference <arm> --arm name=<exp_dir>/<mode> ...` reports:
  - latency, and the share answered within the interval;
  - agreement (relative error or precision@k);
  - CPU-seconds as whole run = warm-up + queries, plus peak memory.

## Workload 01 pilot results (base, seed 862)

- **Exact baselines agree:** Prometheus, raw ClickHouse and ClickHouse+MV agree to about 1e-15 on every shared window.
- **Latency:**
  - The 1s-interval quantiles run at p50 0.33–0.63s on ASAPQuery, 0.79s on ClickHouse+MV, about 1.05s on VictoriaMetrics, 2.5s on raw ClickHouse and 3.5s on Prometheus.
  - ASAPQuery and ClickHouse+MV keep up with every interval; Prometheus and raw ClickHouse fall behind.
  - On 10–20m windows, Prometheus takes 8–16s and ASAPQuery 0.3–0.4s.
- **Accuracy:** ASAPQuery's quantiles are off by about 2%. Its sums and top-k are exact apart from the window boundary: Prometheus 2.53 includes the sample at t−w.
- **CPU-s (warm-up + queries):** ASAPQuery 2,717 = 1,684 + 1,033; Prometheus 11,249; VictoriaMetrics 10,817.
  - The two ClickHouse numbers predate #902: they include the feeder's CPU, and raw ClickHouse was cut off.

## Tonight's plan (pending launch)

- **Capacity:** 42 nodes = 21 slots, about 8 hours.
- **Jobs:**
  - base: 10 workloads × 5 arms;
  - long: 10 × 5;
  - x5 probe: workload 01 × 5.
  - That's 105 jobs, about 145 slot-hours.
- **Still to do before launch:**
  - the driver script (longest-first queue, retry once, `compare_arms` per workload with ClickHouse+MV as the reference);
  - `flow.skip_copy_prometheus_data=true`;
  - a hardware check of the 42 nodes;
  - deploying this branch.
- **Risks:**
  - ASAPQuery's x5 memory is an estimated 12 GB on 15 GB nodes.
  - Prometheus and raw ClickHouse may hit the 10× bound on long and x5.

## Next

1. **Pooled quantiles** (your intent: `quantile by (G)` over 100s). Wait for ASAPQuery #887–#890, the RQE-tree query route. Then:
   - generate pooled quantile RQEs at grouping G, with the ClickHouse SQL pooled too;
   - update the exporter tree for the quantiles;
   - run ASAPQuery with `--frontend rqe`, while Prometheus and VictoriaMetrics keep the `avg by` workaround.
   - The fallback is ASAPQuery's SQL arm (#899–#901), which needs a planner fix (gauges as SQL tables) and two runs.
2. **Accelerated backfill:** removes the 20–60 minute warm-up of every run.
3. **The SQL-only cardinality arm.**
4. **Held review comments on sketch-bench #226** about the quantile shape, phi and window checks, and splitting the shared stream. They get resolved by the pooled rework.
