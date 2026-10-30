# Mapping the Google cluster trace onto the e2e dashboard workload

All numbers below come from scripts in `scripts/`. Their Parquet intermediates have been deleted; rebuild them with `scripts/to_parquet.py` and then `scripts/steps.py` (about 8 min).

## 1. Datasets found

Only one Google dataset is local: **ClusterData 2011-2** (one cell of about 12.5k machines, May 2011), complete, at
`benchmarks/metrics_observability/data/google-cluster-data/ClusterData2011/clusterdata-2011-2/` (42 GB, gzip CSV, no header; columns are in `schema.csv`).

| table | parts | size |
|---|---|---|
| task_usage | 500 | 40 GB, 1,232,799,308 rows |
| task_events | 500 | 1.6 GB |
| job_events | 500 | 84 MB |
| task_constraints | 500 | 149 MB |
| machine_attributes / machine_events | 1 / 1 | 137 MB / 344 KB |

- There is no 2019 trace and no Google microservices trace locally.
  - `README-google-microservices.md` is a deployment guide for the Online Boutique demo, not a dataset.
  - `asap-tools/experiments/datasets/download_google_cluster_2019.sh` fetches 2019 `instance_usage` from GCS on cluster nodes.
- **Time span.** `start_time` runs from 600 s to 2,506,200 s, which is 29.0 days.
  - Each part covers about 5,010 s (83.5 min) and the parts are ordered by start time.
  - Part `p` covers roughly `[600 + 5011·p, …)` s.
- **Resolution.** Each row is one measurement window, normally 300 s.
  - 88% of rows are 300-s windows aligned on both ends. The rest are cut short by task start, stop or migration.
  - The step index is `start_time // 300 s`. Every one of the 8,352 steps (2..8353) is present.
- **task_usage columns:**
  - keys: start/end time (µs), job ID, task index, machine ID;
  - CPU rate (normalized cores); canonical, assigned and max memory; unmapped and total page cache;
  - disk I/O time, local disk, max CPU, max disk I/O, CPI, MAI;
  - sample portion, aggregation type, sampled CPU.
- **Entity counts:**
  - 1 cluster;
  - 933 users (925 appear in task_usage);
  - 672,074 jobs, of which 654,676 appear in task_usage;
  - 39,730 logical job names;
  - 24,281,242 tasks, keyed by (job, task index);
  - 12,580 machines.
- **Concurrency.** At a typical step, a median of 4,122 jobs and 126,749 tasks are reporting.
- **Duplicates.** Rows per (job, task, step) after bucketing: 1 row for 93.2% of cells, 2–5 rows for 6.4%, and up to more than 30 rows for the rest. These come from restarts and migrations within a step.

## 2. Hierarchy and mapping

- **Natural hierarchy:** cluster (1) → user (933) → job (collection) → task index.
  - Machine is orthogonal: tasks migrate between machines.
  - The schedulable class, scheduling class (0–3), is a per-job attribute.
- **Requirement:** 200 jobs, each with 50 tasks that report at every step of the replayed window.

  | window | strict (every step) | ≥99% of steps, gaps forward-filled |
  |---|---|---|
  | best single day | 253 (day 13); 224 (day 0) | — |
  | best 3 days | **227 (days 26–28)** | — |
  | best 4 / 7 days | 197 / 132 | 208 (days 11–17) |
  | 14 / 29 days | 41 / 5 | 181 / 142 |

  For the `half` variant (25 tasks per job), 7 days strict gives 215 jobs (days 3–9).
- **Which levels can be real:**
  - **job = job ID** and **label_0 = task index** are both real.
  - **service = user is not possible.** Among the 227 eligible jobs, only 5 users own 10 or more jobs (one owns 38). We would need 20 such users.
  - **region = scheduling class is not possible** either. Class 0 has only about 18 of the 200 chosen jobs; the split is 18/74/53/55, and each region needs 50.
  - So **region and service are synthetic**, as for Alibaba.
- **Recommended "user-sorted" grouping.** Order the 200 chosen jobs by (user, job ID), then:
  - service = consecutive blocks of 10;
  - region = consecutive blocks of 50.

  A big user's jobs then fill whole services, so a service roughly stands for an owner or team. This keeps real skew at the service level: service-level Zipf is 0.75 against 0.52 for a random grouping (CPU). The Alibaba-style random grouping also works but separates the (region, service) top-k less well (see §3).

## 3. Measurements

The subset is days 26–28, strict coverage, `random.Random(862).sample(sorted eligible job IDs, 200)`, first 50 tasks by index. That is 10,000 series × 864 steps with no missing cells.

### Value distribution (per-step, deduplicated values)

| metric | min | p50 | p95 | p99 | max | zeros | < 5e-5 (→ 0 at ×1e4) | min > 0 |
|---|---|---|---|---|---|---|---|---|
| CPU rate | 0 | 0.00472 | 0.0906 | 0.178 | 0.640 | 1.5% | 6.0% (pool) | 9.5e-7 (= 2⁻²⁰) |
| canonical mem | 0 | 0.00633 | 0.0797 | 0.168 | 0.227 | 1.4% | 1.5% | 9.5e-7 |

- **Value format.** Values are fractions (normalized to the largest machine). They are floats rounded to about 4 significant digits; the smallest ones are multiples of 2⁻²⁰. None are integers.
- **Variation over time.** Median per-series CV over time is 0.29 for CPU and 0.034 for memory. Memory is almost flat in time.
- **Dead series.** 91 of the 10,000 tasks have zero CPU over all 3 days, and 1 chosen job is entirely zero.
- **Outliers.** CPU above 1 is rare (1.5e-6 of raw rows; raw max 3.96, and 5.23 after per-step averaging on day 13). There are none in days 26–28.

### Zipf MLE of totals (`zipf_mle` copied from `fit_skew.py`), CPU / mem

| level | CPU | mem |
|---|---|---|
| leaf (10,000 tasks) | 0.68 | 0.64 |
| job (200 chosen) | 0.86 | 0.80 |
| job (all 227 eligible) | 1.08 | 1.08 |
| service (20), user-sorted / random | 0.75 / 0.52 | 0.61 / 0.50 |
| region (4), user-sorted / random | 0.51 / 0.53 | 0.43 / 0.49 |
| whole cluster, by job_id / by user (from `skew_summary.csv`) | 1.10 / 1.19 | — |

For comparison, Alibaba CPU had Zipf 0.49 by microservice and 0.43 by pod. Google is clearly more skewed at the job level.

Within a job, tasks are near-ties, just as Alibaba's pods were:
- median CV of task totals within a job: 0.083;
- median gap between the top two tasks: 2.1%;
- median ratio of max to median task: 1.17.

So top-k must rank aggregates, never tasks.

### Top-k separability, CPU

Each cell shows top-k share / relative gap (v_k − v_{k+1}) / v_k.

- "full" is the totals over the whole 864-step loop.
- "1m" means every cyclic 600-sample window, i.e. `sum_over_time(data[1m])`. Each cell gives the median gap, the minimum gap, and the fraction of windows whose top-k set equals the full-loop top-k.
- The 5m and 20m windows are about 3.5 and 14 whole loops, so they look like "full", with minimum gaps ≥ 0.019 everywhere for user-sorted grouping and for job-level top-k.

| grouping / level | k=3 | k=5 | k=8 |
|---|---|---|---|
| **user-sorted (region, service)** full | 0.36 / 0.050 | 0.55 / 0.149 | 0.73 / 0.110 |
| user-sorted (region, service) 1m | gap 0.050, min 0.027, same set 100% | 0.148, min 0.069, 100% | 0.110, min 0.085, 100% |
| random (region, service) full | 0.28 / 0.068 | 0.43 / 0.062 | 0.62 / 0.030 |
| random (region, service) 1m | 0.071, min 0.044, 100% | 0.057, min 0.045, 100% | 0.024, **min 0.000, 92%** |
| (region, service, job) full | 0.16 / 0.269 | 0.22 / 0.022 | 0.31 / 0.022 |
| (region, service, job) 1m | 0.270, min 0.254, 100% | 0.020, **min 0.000, 83%** | 0.015, **min 0.0002, 88%** |

Memory gaps are similar in size, but memory has a tie at the (region, service) k=5 boundary under random grouping (gap 0.000 on day 13 and 0.020 on days 26–28).

Other windows tried (full tables in `scratch/summary.txt`):
- **Days 11–17, ≥99% coverage** (2,016 steps): there is more temporal churn. At 1m, the job-level k=8 set never matches the full-loop set, and the random (region, service) k=3 matches in only 75% of windows.
- **Day 13** (288 steps): a 1m window is 2.1 loops, so rankings are almost static. Its (region, service) gaps are large (0.09–0.29).

## 4. Time mapping

- **Step and loop length.** The step is 300 s, versus 60 s for Alibaba. With one step per sample at 10 samples/s:

  | window | steps | loop length |
  |---|---|---|
  | 1 day | 288 | 28.8 s |
  | **3 days (recommended)** | 864 | 86.4 s |
  | 7 days | 2,016 | 201.6 s |
  | whole trace | 8,352 | 835 s ≈ 13.9 min |

  Alibaba's hour looped every 6 s.
- **How much trace a query window covers.** `[1m]` is 600 steps (50 h of trace), `[5m]` is 10.4 days, and `[20m]` is 41.7 days. Every lookback of 5m or more wraps the loop at any window length the data supports.
- **Fetching more is not worth it for 2011.** The full trace is already local; the limit is task churn, not data. Longer windows lose the "50 tasks at every step" jobs (4 days: 197; 29 days: 5).
- **The 2019 trace (8 cells, 31 days, 5-min `instance_usage`) is the only upgrade.**
  - Gain: **region = cell** becomes real, and collection → instance replaces job → task.
  - Cost: tens to hundreds of GB of `instance_usage` per cell via gsutil or BigQuery.
  - It would not fix the 5-min step or the churn. Worth it only if a real top level matters for the paper.

## 5. Recommendation

- **Metric: CPU rate** (task_usage column 6).
  - It varies over time (CV 0.29) and has a wide value spread (p99/p50 ≈ 38×), which suits the quantile queries.
  - Its job-level skew is 0.86 versus 0.49 for Alibaba.
  - Canonical memory has similar skew but is nearly constant in time.
- **Mapping:**
  - job = job ID, label_0 = task index (the first 50 by index);
  - region and service are synthetic, as blocks over the jobs sorted by (user, job);
  - window = days 26–28 (steps 7490..8353, `start_time` in [2,247,000, 2,506,200) s, parts 00448–00499);
  - strict coverage, 227 eligible jobs → 200 chosen with seed 862.
  - The fallback that needs no extra input is Alibaba-style random grouping, but its (region, service) k=8 gap is thinner.
- **Scaling: ×1,000,000.** Counters in the Count-Min-with-heap sketch are i64 and the update does `value.round() as i64` (asap_sketchlib rev 5158bf2, `countminsketch_topk.rs`).
  - At ×10,000, 6.0% of samples round to 0, and 4.3 points of that are real nonzero values.
  - At ×1e6 the smallest nonzero value (9.5e-7) becomes 1 and no nonzero sample is lost.
  - The largest sum is about 0.64e6 × 12,000 samples × 500 series ≈ 4e12, well inside i64.
  - If you'd rather reuse `synthetic_data_value_scale = 10000` unchanged, expect about 6% zeros.
- **Changes `make_replay.py` needs** (a `datasets/google/make_replay.py`):
  1. **Input.** Read headerless `task_usage/part-00448..00499-of-00500.csv.gz` by column position:
     - 0 = start µs, 1 = end µs, 2 = job ID, 3 = task index, 5 = CPU rate.
     - Also read `job_events` (col 2 = job ID, col 4 = user) for the grouping; take the last non-null value per job.
  2. **Steps.** `step = start_us // 300_000_000`. Keep 7490 ≤ step < 8354.
  3. **Series key.** Use `(job, task index)`. Do not use machine, because tasks migrate between machines.
  4. **Dedup.** For each (job, task, step), take the duration-weighted mean of CPU rate with weight `max(end − start, 1 s)`. About 7% of cells have more than one row.
  5. **Eligibility.** A task is eligible if it reports at all 864 steps. A job is eligible if it has at least 50 such tasks.
     - Sort job IDs as ints, then `random.Random(862).sample(eligible, 200)` gives exactly the subset measured here.
     - Optionally drop tasks whose CPU is zero at every step before taking the first 50.
  6. **Labels.** Order the chosen jobs by (user, job). Use `region = i // 50`, `service = f"{region}.{i // 10 % 5}"`, `job = f"{service}.{i % 10}"`, and `label_0 = f"{job}.{k}"`.
  7. **Mapping CSV.** Write columns `region, service, job, label_0, user, job_id, task_index`.
  8. **Offsets.** `offset_ms = (step − 7490) × 300,000`. The value is CPU × 1e6.
  9. **Table.** `write_table` is unchanged: 864 steps, series-major.
  10. **Implementation.** Don't hold per-pod timestamp sets the way the Alibaba script does. The window has 116M rows and 2.85M tasks, which would take more than 10 GB in Python sets.
      - Aggregate with DuckDB instead. It is in `baselines/PilotDB/.venv`.
      - Or start from `scratch/steps.parquet`, which is already deduplicated per (job, task, step) with CPU and memory.
      - `scripts/analyze_subset.py` already contains the selection logic.
- **Risks:**
  - **Near-ties at the boundary.**
    - Job-level k=5 and k=8 (gaps 2.2%) reach a gap of 0 in some 1m windows, and their top-k sets match the full-loop sets in only 83% and 88% of windows.
    - Under random grouping, (region, service) k=8 does the same (92%).
    - User-sorted grouping removes the (region, service) ties. Job-level ties come from the data, so evaluate top-k with gap-aware or set-overlap metrics.
  - **Task-level flatness.** Leaf-level Zipf is 0.68 and tasks within a job are near-ties. That is fine for the pooled queries, which never rank tasks.
  - **Zeros and sparse series.** 1.5% of samples are 0, 5% of series contain at least one 0, and 91 tasks (1 whole job) are all-zero. Coverage is complete with no gaps in the strict window. The 7-day ≥99% variant forward-fills 0.09% of cells.
  - **Short loop relative to the lookbacks.** 5m and 20m windows sum about 3.5 and 14 identical loops, so they rank like the full loop, and only 1m (0.69 loop) sees temporal churn.
  - **Outliers.** CPU above 1.0 exists elsewhere in the trace (day 13: 5.23). Clip at 1.0 if you use another window.
  - **Selection bias.** Strict coverage favors long-running service jobs. That is realistic for dashboards but excludes batch churn.

## 6. Built replay

`make_google_replay.py` implements §5. It reads the raw CSVs with DuckDB, needs no scratch files, and takes about 40 s with 2.9 GB peak RSS. The output is byte-identical across thread counts.

- **Outputs:**
  - `replay_table_google.json` (70 MB): 10,000 series × 864 steps, no NaNs. `paced_feeder --verify-facts` passes.
  - `replay_mapping.csv`
  - `replay.csv` (332 MB)
- **Values (CPU × 1e6):** p50 4,715; p95 90,580; p99 178,200; max 639,600; 1.46% zeros. 91 series are zero at every step.
- **Fractional values.** 28% of values are fractional: these are the raw values below 0.001, which have 4 significant digits. Count-Min rounds them.
- **Skew.** Zipf is 0.861 for jobs, 0.750 for services and 0.676 for tasks. Top-k figures match §3.
