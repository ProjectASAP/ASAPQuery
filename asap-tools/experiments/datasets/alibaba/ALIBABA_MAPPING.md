# Alibaba trace → random query workloads (handoff)

**Goal:** realistic, strong end-to-end results on Alibaba data for the eval section.

The motivation section (Prometheus, PromSketch, PilotDB; cost vs accuracy) should ideally use the same data, label mapping and similar queries, so the two line up. They don't need to match exactly.

## Data the end-to-end runs feed

**Source:** Alibaba cluster-trace-microservices-v2022, MSMetrics, pod `cpu_utilization` (0–1).
- Columns: `timestamp, msname, msinstanceid, nodeid, cpu_utilization, memory_utilization`.
- 60 s step, about 470k pods per step, 28k distinct msname.
- Only about the first hour is local: `benchmarks/metrics_observability/data/alibaba-cluster-data/cluster-trace-microservices-v2022/0d0_0d1/data/MSMetrics/`.
  - That is `MSMetricsUpdate_0.csv` plus `MSMetrics_1.tar.gz`.
  - Rows are **not** sorted by time.

**Subset:** fit to our synthetic label shape, so the existing workloads, plans and configs apply unchanged.
- 4 region × 5 service × 10 job × 50 label_0 = 10,000 series.
- `job` = one microservice (msname).
  - Seed 862 draws them from the 1,785 msnames with ≥50 pods reporting at all 60 steps.
- `label_0` = one of its pods (msinstanceid; the first 50 by pod number).
- `region` and `service` are synthetic groupings of microservices.
  - The trace has neither level.
  - `nodeid` cuts across msname, so it doesn't nest.
- Label values: region `r`, service `r.s`, job `r.s.j`, label_0 `r.s.j.k`.

**Values:** real pod CPU.

| Statistic | Value |
|---|---|
| min | 0.003 |
| median | 0.12 |
| p95 | 0.40 |
| p99 | 0.58 |
| max | 0.997 |

- No zeros.
- The busiest microservice is about 5× the median one.
- The top 10 of 200 microservices hold 15% of total CPU.
- Region totals are 20–26k.

**Time mapping:** a quick-result compromise.
- Replayed at 10 samples/s per series, the same as the synthetic base set.
- One trace minute becomes one 100 ms sample, so the 60-step hour loops every 6 s.
- Spread and skew across pods and services are real; each series' pattern over time repeats.
- A realistic run needs more trace (`fetchData.sh`) and a smaller speedup.

**Files** (generated, not tracked):
  - `replay.csv`: `offset_ms,region,service,job,label_0,value`; 600k rows, sorted by offset.
  - `replay_mapping.csv`: our labels → msname/msinstanceid.
  - `replay_table.json`: a paced-feeder workload table with `values.replay`.
- Generator: `asap-tools/experiments/datasets/alibaba/make_replay.py`.

## Query workloads

Seed 862, 10 random workloads; manifests are in `asap-tools/experiments/datasets/fake_metric/random_workloads/seed-862/`.

Each end-to-end run uses a workload's 7 `common-promql` queries:
- **Metric and groupings:** metric `data`, grouped by `(region)`, `(region, service)` or `(region, service, job)`.
- **Sums (2):** `sum by (G) (sum_over_time(data[W]))`.

  | Window W | Repeated every |
  |---|---|
  | 1m | 1–2 s |
  | 5m | 5–10 s |
  | 10m | 10–20 s |
  | 20m | 30–60 s |

- **Quantiles (3):** 100 s lookback, repeated every 1 s.
  - PromQL can't express a pooled quantile, so it uses `avg by (G) (quantile_over_time(phi, data[100s]))`.
  - Every system, ClickHouse and LiteOlly included, currently computes this same `avg` of per-series quantiles, so all answers agree.
  - The intended pooled `quantile by (G)` over 100 s is being added (LiteOlly support merged in #887–890).
- **Top-k (2):** `topk(k, …)` over counts, with K capped at 1,000 keys.

On Alibaba data these read naturally: "a quantile of pod CPU per microservice or region over the last 100 s", and "total CPU per service over 5–20 min".

## End-to-end status (2026-10-15)

- **Synthetic base:** 5 systems × 10 workloads, all valid.
  - LiteOlly vs worst baseline: 6.2× less CPU than raw ClickHouse, and 11.6× lower p95 than Prometheus.
- **Alibaba:** pilot w01 × 5 systems is finishing. The full 10 workloads follow if it is valid.
