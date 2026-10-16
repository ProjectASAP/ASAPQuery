# ASAPQuery accelerated random E2E handoff

*Updated 2026-10-15 ~09:30 (session `2578ec80-4c3a-4ede-9be0-ad5eca70b852`, peer name `asapquery-random-e2e-11`).*

## Repositories and branches

- **ASAPQuery worktree:** `.../code/ASAPQuery-random-e2e`. Don't use the shared `.../code/ASAPQuery`.
  - **Merged:** #879 (pilot), #902 (ClickHouse runs and clean CPU), #903 (long and x5 variants, VictoriaMetrics pin).
  - **Open:** `codex/random-e2e-batch`, the batch driver and analysis scripts, not yet in a PR. #904 (this doc) is a draft.
- **sketch-bench worktree:** `.../code/sketch-bench-random-e2e`, detached at `main` (`a3753f2`).
  - #226 (`--random-manifest`) and #228 (`expr` trees) are merged.
- **Cluster:** CloudLab `scratch4.cloudmigration-PG0.apt.emulab.net`, `node0`–`node41`, all Xeon E5-2450: 8 cores / 16 threads, 15 GB RAM, no AVX2.
  - A run uses 2 nodes: `num_nodes=1`, `node_offset=2k`.
  - Deploy with `asap-tools/deploy_changes.sh 42 milindsr <suffix>` from **one** checkout. It compares timestamps, so alternating checkouts rebuilds everything.
- **RQE = repeating query expression.**

## Design (`.design_docs/accelerated-random-workload-eval.md`)

- **Sets:** seed 862, 10 workloads; the pilot runs each workload's 7 `common-promql` queries.
  - `base`: 10,000 series at 10 samples/s = 100k samples/s. Quantiles over 100s every 1s, other windows 1–20 min.
  - `long` (`--variant long`): every lookback ×3, max 60 min.
  - `x5` (`--variant x5`): 50,000 series, 500k samples/s.
  - The variants keep every random choice, so they pair query for query with the base.
- **Quantile semantics today:** the average of per-series quantiles, on every system. The user's intent is a pooled quantile per group; see Next.
- **Rendering:** PromQL and ClickHouse SQL are both rendered from the RQEs (`random_workloads.rqe_query` → `sketch_bench_workloads.Query`). ASAPQuery uses `--planner saved` plans from sketch-bench `autosketch_vs_asap`, archived in `random_workloads/<set>/workload-NN-pilot/`.
- **Arms:**
  - ASAPQuery `sketchdb` and Prometheus 2.53.2 run through `experiment_run_e2e.py` (`e2e.yaml`, one mode each).
  - VictoriaMetrics v1.154.0 also runs through `experiment_run_e2e.py`, with `latencyOffset=0s` and the cache on.
  - Raw ClickHouse and ClickHouse+MV (exact views) run through `experiment_run_clickhouse.py`, pinned to ClickHouse 23.8: 26.6+ needs AVX2.
  - Every run warms up for the longest lookback + 10s, then runs a 10-minute aligned query schedule. It ends when its queries finish. The feeder runs on the second node and stops on SIGTERM.

## Tools

- **Driver:** `asap-tools/experiments/random_workload_batch.py` runs one job per arm per workload on two-node slots, longest first.
  - It cleans each slot first and retries once under a new name.
  - Its state is in `experiment_outputs/<batch>/state.json`. It resumes, leaving runs still in flight alone (`--skip-slots`).
  - **Gap:** it counts a job done if it recorded results; it doesn't check validity.
- **Per-workload comparison:** `post_experiment/multi_experiment/compare_arms.py` reports latency, agreement and CPU (warm-up + queries).
- **Collation:** `collate_batch.py <batch> --csv` writes one row per run: CPU, memory, latency p50/p95/sum, failed queries, ingest lag, and **valid** (no failed queries, answers agreeing with Prometheus). It prints ratios over valid pairs.
  - It needs the feeder stats copied back from each run's feeder node: `<mode>/paced_feeder_output/*_stats.json`. They aren't synced automatically yet.
- **Plot:** `plot_cost_latency.py collated.csv out.png` draws p95 latency against total CPU, log–log, one panel per set, with invalid runs hollow.

## Overnight batch `experiment_outputs/overnight-1015/`

- **Coverage:** base 01–10 and long 01–10, all 5 arms. x5 was cancelled; its raw ClickHouse couldn't keep up, with ingest 9 minutes behind.
- **Outputs:**
  - `runs_table.md` and `collated.csv`: per-run status and numbers;
  - `cost_latency.png`: the scatter plot;
  - `summaries/`: per-workload comparisons.
- **ASAPQuery's improvement, long (valid pairs, geometric mean):**

  | vs | Pairs | CPU | p50 | p95 | Latency sum |
  |---|---|---|---|---|---|
  | Prometheus | 6 | 5.6× | 17× | 13× | 16× |
  | VictoriaMetrics | 6 | 3.1× | 2.8× | 3.1× | 3.6× |
  | Raw ClickHouse | 5 | 12× | 12× | 14× | 15× |
  | ClickHouse+MV | 1 | 3.7× | 2.1× | 3.4× | 4.1× |

  Base workload 01: 5.4×, 4.4×, 17× and 3.2× in CPU versus Prometheus, VictoriaMetrics, ClickHouse and ClickHouse+MV. Base 02–10 still need collating once their last jobs finish.
- **Problems found in long:**
  1. **ASAPQuery failed 22–35% of queries in workloads 04, 06, 08 and 09.** Confirmed from the stats:
     - **Too late:** the query arrived more than ~15s after its evaluation time, and the circular buffer had already evicted the windows it needed (`retention_slack_ms=21000`).
     - **Too early:** ingest lag exceeded the send delay, so the windows weren't closed yet.
     - With store advance between 0 and 15s, 1 of 22,084 queries failed.
     - The root cause is load: the per-series DDSketch (30s window sliding 1s) updates 30 windows per sample.
  2. **ClickHouse+MV gave wrong sums (2–36% low) in 9 of 10 long workloads.** It saturates the node, ingest falls 3–6 minutes behind, and its fast on-time queries read incomplete windows.
  3. **Raw ClickHouse, long 02:** returned empty results (bad run on node6–7).
  4. **The optimizer's predictions are far off:** for long 04 it predicts 41 ms, 0.19 CPU and 1.0 GiB, against p50 0.6s, ~4.5 cores and 5.5 GiB measured. So its latency SLAs at 100 ms and above never change the plan.
- **Raw ClickHouse slowness:** its table is `ORDER BY (labels…, ts)`, which can't prune by time, so every query reads all stored rows. A local test with 60M rows confirmed full scans; `ORDER BY ts` reads only the window.

## One-off runs in flight (long workload, results ~10:30–11:40)

| Name | Nodes | What |
|---|---|---|
| `s862long_w01_ch_tdigest`, `s862long_w01_chmv_tdigest` | node22–23, node26–27 | `quantileTDigest` instead of exact quantiles (`config/.../seed-862-long/workload-01-approx/`) |
| `s862long_w01_ch_timesort`, `s862long_w01_chmv_timesort` | node28–29, node30–31 | Base table `ORDER BY ts` (`workload-01-timesort/`) |
| `s862long_w04_asap_sla20`, `s862long_w04_asap_sla10` | node12–13, node18–19 | ASAPQuery plans with 20 ms / 10 ms SLAs: quantile windows 60s / 100s, sliding 1s (`experiment_outputs/sla-trial/`) |

## Next

1. Finish base 02–10. Then copy back the feeder stats, re-run `collate_batch.py` and `plot_cost_latency.py` for the 10-workload base scatter.
2. Decide on raw ClickHouse's schema (`ORDER BY ts`?) and ClickHouse+MV's ingest problem from the one-off results. Rerun those arms if needed.
3. **ASAPQuery's keep-up:** a cheaper per-series quantile plan (the cost model under-prices ingest) and/or more retention slack. Check the SLA trials.
4. **Driver:** copy feeder stats back, and validate runs (agreement, ingest lag) before marking them done.
5. **Pooled quantiles:** use the RQE-tree route once ASAPQuery #887–#890 merge. #906 now plans gauges as SQL tables, as a fallback.
6. Then: accelerated backfill, the SQL-only cardinality arm, and the held #226 review comments.
