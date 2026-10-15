# §6.3 workloads at 10 samples/s, for the end-to-end run

The classic, multi-grouping and all (mixed + multi-grouping) workloads of
`../autosketch-vs-asap-hydra/` (r = 1, m = 1), re-planned at 1/20 of the data
rate: each series is scraped 10 times a second (every 100 ms) instead of 200,
so the classic metric carries 1e5 samples/s, `http` 1e5 records/s and `flows`
2e5 records/s. The templates, windows, accuracy curves, cost table, p95
targets and runner are those of `../autosketch-vs-asap-hydra/`, with two
differences for the end-to-end run:

- **Instant RQEs read one scrape.** `sum by (job)` and `quantile by (job)`
  look back 100 ms, one sample per series (N = 1e3 per job), and are served
  by 100 ms tumbling windows.
- **One RQE per schema query.** A query that several templates ask (14, 15,
  17) is only its strictest template's RQE, so every e2e query is answered by
  one deployment. §6.3 keeps every copy (`--keep-duplicate-queries`).

## Files

- `inputs/tables/`: the tables, `export_autosketch_eval_table.py --synthetic
  --samples-per-sec 10` (classic, all, multigroup at shared = 1). Each one's
  `workloads[0].e2e` holds the queries and metrics to generate. Each query
  but `rate` carries `expr`, its RQE expression tree (leaves name the plans'
  RQE ids), added after planning: the RQEs and plans are unchanged.
- `plans/<table>.plans.json.gz`: each method's chosen plans, in the format of
  `../autosketch-vs-asap-hydra/README.md`, "Plans" (`--runs 1`, kept, then
  `scripts/extract_autosketch_plans.py`).
- `<table>-tp95.json`: the runner's results (`--runs 3 --no-chosen`).

## Served and left-out RQEs

At 10 samples/s, two RQEs of template 16 (p99 latency `GROUP BY CUBE(region,
service, endpoint)`) fall below the accuracy curves' first N (1e3 items):

- `t16_p99_latency_by_region,endpoint/values/5m`
- `t16_p99_latency_by_region,service,endpoint/values/5m`

Their smallest group (all 2,500 groups are covered) holds 403 items in 5m. The
optimizer can't read accuracy below the curves (`SaturationCurves::accuracy`
returns `None`), so no method can serve them. The export leaves them out of
the RQEs and the e2e queries, and logs it. The same groupings at 15m (1,210
items) are kept. With the two RQEs left in, the runner drops exactly these two
as unservable (ASAP and AutoSketch alike) and picks the same plans.

| Workload | RQEs at 200/s (§6.3) | Duplicate queries | Below the curves | RQEs at 10/s, all served |
|---|---|---|---|---|
| classic | 42 | 0 | 0 | 42 |
| multigroup | 62 | 10 | 2 | 50 |
| all | 104 | 10 | 2 | 92 |

Every chosen accuracy is measured (no theory reads), and no Hydra grid is chosen.

## Predicted cost

Cheapest plan (no latency bound) at 10/s:

| Workload | Weights | ASAP | ASAP (no roll-ups) | PerQuery | AutoSketch |
|---|---|---|---|---|---|
| classic | CPU (vCPU) | 0.318 | 0.318 | 0.699 | 135 |
| classic | Fargate ($/h) | 0.0585 | 0.0585 | 0.192 | 6.41 |
| multigroup | CPU | 0.0911 | 0.110 | 0.132 | 0.487 |
| multigroup | Fargate | 0.00846 | 0.0155 | 0.0204 | 0.0348 |
| all | CPU | 0.409 | 0.428 | 0.831 | 136 |
| all | Fargate | 0.0670 | 0.0740 | 0.212 | 6.45 |

ASAP, CPU weights:

| Workload | Memory (GiB) | Deployments | RQEs rolled up |
|---|---|---|---|
| classic | 10.7 | 7 | 0 |
| multigroup | 1.33 | 9 | 33 |
| all | 12.0 | 16 | 33 |

Classic deployments (ASAP, CPU weights):

| Deployment | Window/slide (s) |
|---|---|
| `sum by (job)` (exact-sum) | 0.1/0.1 |
| `quantile by (job)` (DDSketch α = 0.1) | 0.1/0.1 |
| `sum by (job) (sum_over_time)` (exact-sum) | 60/60 |
| `sum_over_time` (exact-sum) | 60/60 |
| `quantile_over_time` (DDSketch α = 0.1) | 900/60 |
| `rate` (exact-increase) | 180/60 |
| top-k (CMS 3×1024, heap 9,216) | 300/60 |

Against the previous 10/s plans (1 s instant lookback, every duplicate query,
the cost table and curves before the denser grid):

- **Memory falls ~30%** (classic 15.3 → 10.7 GiB), and Fargate costs ~25%
  (classic 0.0787 → 0.0585 $/h): with the denser grid, plans pick DDSketch
  α = 0.1, the coarsest that meets the target.
- **CPU rises ~3%** (classic 0.309 → 0.318 vCPU, +1 deployment):
  `sum by (job) (sum_over_time)` no longer shares the instant `sum by (job)`
  deployment, whose window is now 100 ms.
- **Multigroup serves 10 fewer RQEs** and costs ~4% less CPU; 33 RQEs are
  rolled up (was 41).
- The tightest feasible latency bounds are unchanged: classic 21 ms, multigroup
  and all 490 ms.

Every e2e query's RQEs are served by one deployment in every plan. An
independent re-plan in another session gave identical tables and identical
plans in all 120 (method, weights, kind, SLA) combinations.

## Converting the plans in ASAPQuery

`--planner saved` needs `--data-ingestion-interval-ms 100` on these tables:
the scrape interval, which instant RQEs' 0.1 s windows are whole multiples
of. With it, on ASAPQuery `main` (ProjectASAP/ASAPQuery#866), every ASAP plan
converts: classic PromQL 12/12, multigroup SQL 8/8, all SQL 8/8 and PromQL
8/8.

## Reproduce

```sh
cargo build --release -p rqe-optimizer --examples
DIR=rqe-optimizer/results/e2e-rate10; TMP=$(mktemp -d)
# SAT: the saturation dir of ../autosketch-vs-asap-hydra/README.md, "Reproduce"
python3 scripts/export_autosketch_eval_table.py --synthetic --samples-per-sec 10 \
    --revision acc2db1 --out $TMP
for t in classic all multigroup; do
  f=synthetic-templates$t-shared1
  cp $TMP/$f.json $DIR/inputs/tables/
  target/release/examples/autosketch_vs_asap synthetic --table $DIR/inputs/tables/$f.json \
      --target p95 --runs 3 --no-chosen --saturation-dir $SAT --out $DIR/$f-tp95.json
  target/release/examples/autosketch_vs_asap synthetic --table $DIR/inputs/tables/$f.json \
      --target p95 --runs 1 --saturation-dir $SAT --out $TMP/$f-tp95.json
  python3 scripts/extract_autosketch_plans.py $TMP/$f-tp95.json $DIR/plans/$f.plans.json.gz
done
```

Runs: 2026-10-14, sketch-bench `main` (acc2db1), on a 16-core desktop (not an
idle cluster node), so `planning_secs` are indicative only.
