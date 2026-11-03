# §6.3.1 case studies: when the plan changes

The two case studies of §6.3.1 (performance monitoring, anomaly detection),
each swept one knob at a time to find where the planner's plan stops being
the obvious one. Every table is planned by the ASAP planner (via
`autosketch_vs_asap`) at 95% accuracy under CPU weights (cost in vCPU), with
no SLA and at a grid of batch latency SLAs. Only ASAP's plans are reported;
the result files also hold the runner's baselines.

The base workloads reproduce the paper draft's ASAP plans exactly:
performance monitoring 0.105 vCPU (2 MVs), anomaly detection 0.114 (3).

## Findings

Four knobs change the plan in ways that are not obvious from the workload.
Costs are ASAP's cheapest plan, vCPU (MVs).

### 1. Performance monitoring: a latency SLA builds a hierarchy of windows

The cheapest plan answers every `quantile_over_time` from one 15 s tumbling
MV, so the 24h query merges 5,760 windows per series. Its batch latency is
23.8 s. As the SLA tightens, the planner adds coarser MVs, each serving only
the lookbacks long enough to use it:

| Batch SLA | ASAP | Per-series MVs (window/slide → queries) |
|---|---|---|
| none (23.8 s) | 0.105 (2) | 15s → 1m, 10m, 30m, 6h, 24h |
| 10 s | 0.122 (3) | 15s → 1m; 60s → 10m, 30m, 6h, 24h |
| 1 s | 0.126 (3) | 15s → 1m, 10m, 30m; 30m → 6h, 24h |
| 100 ms | 0.229 (4) | 15s → 1m; 2m/1m → 10m, 30m; 1h/30m → 6h, 24h |
| 10 ms | 1.24 (5) | one sliding MV per lookback pair, ≤ 3 merges each |

The plan at 1 s is not "one MV per dashboard": the 30m query joins the 15 s
MV, and only the 6h and 24h queries get a 30 min tumbling window. A 100×
tighter SLA (1 s → 10 ms) costs 10× (0.126 → 1.24).

### 2. Performance monitoring: how often the long dashboard refreshes

The 6h/24h dashboard refreshes every `long_interval`:

| long_interval | ASAP | Per-series MVs |
|---|---|---|
| 30 min | 0.105 (2) | 15s → all six |
| 5 min | 0.131 (3) | 15s → 1m–30m; 5m → 6h, 24h |
| 1 min | 0.216 (3) | 15s → 1m; 2m/1m → 10m–24h |
| 15 s | 0.627 (3) | 15s → 1m; 2m/15s → 10m–24h |

Merging 5,760 windows is cheap once every 30 min, not every 15 s, so a
refresh rate the operator picks in a dashboard decides whether long lookbacks
get their own coarser MV.

### 3. Performance monitoring: less data, more MVs

Samples per second per series (`per_series`; 200 is the base):

| per_series | ASAP | Per-series MVs |
|---|---|---|
| 20 | 0.0197 (4) | 15s → 1m; 60s → 10m, 30m; 30m → 6h, 24h |
| 50 | 0.0368 (3) | 15s → 1m; 60s → 10m–24h |
| 100 | 0.0651 (3) | 15s → 1m; 60s → 10m–24h |
| 200 | 0.105 (2) | 15s → all six |
| 500, 1000 | 0.218, 0.406 (2) | 15s → all six |

Ingest CPU scales with the sample rate; query-time merging scales with
series × windows and does not. At low rates an extra MV's ingest is cheaper
than the merges it saves, so the planner maintains more MVs for less data.
At 200/s: ingest 0.075 + query 0.029 vCPU with 2 MVs.

### 4. Anomaly detection: one more query makes the plan cheaper

The base workload's six groupings have no common finer grouping among them,
so ASAP needs 3 MVs: (region, service, status) serves four queries by roll-up
and the two endpoint groupings each get their own. Adding a seventh query, a
drill-down p99 by (region, service, endpoint, status) every 5 min (groups
holding ≥ 1% of requests), lets one MV at the finest grouping serve all
seven. Ingest is paid once instead of three times:

| endpoints per service | ASAP, 6 queries | ASAP, + drill-down | Plan with drill-down |
|---|---|---|---|
| 2 | 0.113 (3) | 0.0388 (1) | finest, 15 s → all 7 |
| 10 | 0.113 (3) | 0.0435 (1) | finest, 15 s → all 7 |
| 25 | 0.114 (3) | 0.0522 (1) | finest, 15 s → all 7 |
| 50 | 0.114 (3) | 0.0668 (1) | finest, 15 s → all 7 |
| 100 | 0.114 (3) | 0.0776 (2) | (service, region), 15 s → 3 coarse queries every 15 s / 1 min; finest, 60 s → the other 4 |

The drill-down halves the cost at 25 endpoints (0.114 → 0.052). Rolling up
gets more expensive as the finest grouping grows (2,000 → 40,000 groups),
because the `by (service)` and `by (region)` queries merge every fine group's
20 windows every 15 s. At 100 endpoints that query cost exceeds a second MV's
ingest, and the planner splits the frequent coarse queries off to a
(service, region) MV. A latency SLA has the same effect at 25 endpoints:
2 MVs at 30 ms (0.076), 3 at 10 ms (0.113), 5 at 3 ms (0.188).

### Knobs that do not change the plan

- **Series count** (performance, 1e4–1e5) and **ingest rate** (anomaly,
  2.5e5–2e7 samples/s): every cost scales linearly and the plan is unchanged.
- **Status cardinality** (anomaly, 2–16) and **endpoints without the
  drill-down** (2–100): `by (service)` moves between the (service, endpoint)
  and (region, service, status) MVs, whichever has fewer groups to merge,
  but the cost changes by < 1%.
- **One interval for every anomaly query** (15 s, 1 min, 5 min): the same 3
  MVs with window = interval.
- **Anomaly batch SLA** (0.3–3 ms; the cheapest plan is 3.3 ms): at 2 ms the
  15 s queries roll up from a (service, region) MV instead of
  (region, service, status). Real, but at sub-ms SLAs.

## Suggested §6.3.1 text

> **Performance monitoring.** [workload as in the draft] Without a latency
> target, LiteOlly keeps one DDSketch per series in 15 s tumbling windows and
> answers every lookback, 1 min to 24 h, by merging 4 to 5,760 windows
> (0.10 vCPU). Two changes an
> operator makes without thinking about synopses change that plan. First,
> refreshing the 24 h dashboard every minute instead of every 30 min makes
> the merges dominate: LiteOlly adds a 2 min window, sliding every minute,
> for all lookbacks of 10 min and up (0.22 vCPU). Second, a
> 1 s latency target on the dashboards gives the 6 h and 24 h queries a
> 30 min tumbling MV, while the 30 min query stays on the 15 s one. Less
> obviously, lower sample rates lead to *more* MVs: at 20 samples/s per
> series ingest is cheap enough that LiteOlly maintains three per-series
> window sizes (15 s, 1 min, 30 min) to save merges, while at 200 samples/s
> one suffices.
>
> **Anomaly detection.** [workload as in the draft] LiteOlly keeps one
> DDSketch per (region, service, status) group in 15 s windows and rolls it up
> for four queries; the two endpoint groupings get their own MVs (3 MVs,
> 0.11 vCPU). When the operator adds a seventh query, a drill-down by all
> four labels, the plan gets *cheaper*: one MV at the finest grouping now
> serves all seven queries and ingest is paid once (0.05 vCPU). This sharing has a limit LiteOlly finds on its own: with 100
> endpoints per service the finest grouping has 40,000 groups, and merging
> them every 15 s for the per-service and per-region queries costs more than
> a second MV, so LiteOlly splits those queries off to a (service, region) MV.

## Fargate prices

The result files now hold both weight settings (rerun without `--weights cpu`;
the CPU-only results are unchanged, cost for cost). With memory priced
(AWS Fargate), three plans differ from the CPU-only ones:

- **Performance monitoring, no SLA:** 3 MVs, 0.0075 $/h. The 6h and 24h
  queries share a 30 min tumbling MV, and the 1m–30m queries stay on 15 s;
  CPU-only keeps all six on 15 s (18 GiB). A 1 s SLA therefore no longer
  changes the plan. At 100 ms the planner moves 10m/30m to 2 min windows
  sliding every minute and 6h/24h to 1 h windows sliding every 30 min (4 MVs,
  0.0105 $/h).
- **Performance monitoring, 15 s refresh of the long dashboard:** a 2 min
  window sliding every 15 s serves 10m–24h, as under CPU weights (0.106 $/h;
  LiteOllyPerQuery 0.140, AutoSketch 29.6).
- **Anomaly detection:** the same plans as CPU-only (3 MVs for the six
  queries; with the drill-down, 1 MV at 25 endpoints and 2 at 100).


- CPU weights only: memory is free, so the base performance plan holds
  18 GiB of 15 s windows. Fargate weights would favor coarser windows.
- The drill-down covers groups holding ≥ 1% of requests: at every group, the
  smallest (region, service, endpoint, status) groups fall below the
  saturation curves' first N (1e3 items) and the query is unservable.
- The anomaly metric is named `requests`, not `http`, so no Hydra grid (whose
  accuracy is measured at `http`'s schema only) is a candidate.
- Sweep grids stop where a query falls below the curves: per-series rates
  below 20/s, anomaly ingest below 2.5e5/s, and 16 statuses at most (status
  has Zipf skew 2.0, so its tail groups are tiny), and series from 1e4 (at
  1e3 the per-job instant queries fall below the curves).
- One run (`--runs 1`); costs are deterministic, planning times are not
  reported.

## Files

| File | What it is |
|---|---|
| `inputs/tables/<case>-<knob>-<value>.json` | one table per sweep point (`sketch-bench/scripts/case_study_tables.py`); `base-0` is the draft's workload |
| `inputs/tables/plan.tsv` | each table and its SLA grid (ms) |
| `<case>-<knob>-<value>-tp95.json` | the runner's result, with each plan's `chosen` |
| `summary.md` | every table: ASAP's cost, MVs and latency with no SLA and per SLA, and each distinct plan (`rqe-optimizer/scripts/summarize_case_studies.py`) |

## Reproduce

```sh
cargo build --release -p rqe-optimizer --examples
DIR=rqe-optimizer/results/case-studies
SAT=$(mktemp -d); cp -r rqe-optimizer/results/autosketch-vs-asap-inputs/saturation/out_* $SAT/
cp -r rqe-optimizer/results/autosketch-vs-asap-hydra/inputs/saturation/* $SAT/
python3 $SKETCH_BENCH/scripts/case_study_tables.py --out $DIR/inputs/tables
while IFS=$'\t' read -r table slas; do
  target/release/examples/autosketch_vs_asap synthetic --table $DIR/inputs/tables/$table \
      --target p95 --runs 1 --slas-ms $slas --saturation-dir $SAT \
      --out $DIR/${table%.json}-tp95.json
done < $DIR/inputs/tables/plan.tsv
python3 rqe-optimizer/scripts/summarize_case_studies.py $DIR/*-tp95.json > $DIR/summary.md
```

The whole sweep (38 tables) plans in under a minute.
