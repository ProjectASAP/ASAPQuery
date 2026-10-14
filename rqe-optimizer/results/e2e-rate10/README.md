# §6.3 workloads at 10 samples/s, for the end-to-end run

The classic, multi-grouping and all (mixed + multi-grouping) workloads of
`../autosketch-vs-asap-hydra/` (r = 1, m = 1), re-planned at 1/20 of the data
rate: each series is scraped 10 times a second (every 100 ms) instead of 200,
so the classic metric carries 1e5 samples/s, `http` 1e5 records/s and `flows`
2e5 records/s. Everything else (templates, windows, accuracy curves, cost
table, p95 targets, runner) is the same as `../autosketch-vs-asap-hydra/`.

## Files

- `inputs/tables/`: the tables, `export_autosketch_eval_table.py --synthetic
  --samples-per-sec 10 --keep-duplicate-queries` (classic, all, multigroup at shared = 1). Each one's
  `workloads[0].e2e` holds the queries and metrics to generate.
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

| Workload | RQEs at 200/s | RQEs at 10/s | Served | Dropped |
|---|---|---|---|---|
| classic | 42 | 42 | 42 | 0 |
| multigroup | 62 | 60 | 60 | 0 |
| all | 104 | 102 | 102 | 0 |

Every chosen accuracy is measured (no theory reads), and no Hydra grid is chosen.

## Predicted cost

Cheapest plan (no latency bound), 200/s (`../autosketch-vs-asap-hydra/`) → 10/s:

| Workload | Weights | ASAP | ASAP (no roll-ups) | PerQuery | AutoSketch |
|---|---|---|---|---|---|
| classic | CPU (vCPU) | 2.38 → 0.309 | 2.38 → 0.309 | 7.77 → 0.725 | 2,739 → 137 |
| classic | Fargate ($/h) | 0.170 → 0.0787 | 0.170 → 0.0787 | 0.623 → 0.324 | 136 → 7.02 |
| multigroup | CPU | 0.221 → 0.0950 | 0.551 → 0.114 | 1.07 → 0.140 | 10.2 → 0.554 |
| multigroup | Fargate | 0.0137 → 0.00872 | 0.0339 → 0.0158 | 0.0597 → 0.0214 | 0.430 → 0.0382 |
| all | CPU | 2.60 → 0.404 | 2.93 → 0.424 | 8.84 → 0.865 | 2,750 → 138 |
| all | Fargate | 0.184 → 0.0874 | 0.204 → 0.0945 | 0.683 → 0.345 | 136 → 7.06 |

ASAP, CPU weights:

| Workload | Memory (GiB) | Deployments | RQEs rolled up |
|---|---|---|---|
| classic | 21.4 → 15.3 | 6 → 6 | 0 → 0 |
| multigroup | 1.06 → 1.34 | 5 → 9 | 52 → 41 |
| all | 22.4 → 16.7 | 11 → 15 | 52 → 41 |

Only ingest CPU scales with the rate. In the cost model, insert, merge and
query CPU and per-instance memory are constants of the cost table's row,
whatever a window holds; only DDSketch's memory is capped by the values it
holds. AutoSketch's cost is almost all ingest and falls 20×. ASAP's ingest
falls ~10× (classic: 1.84 → 0.18 vCPU), because with cheaper ingest the MILP
picks longer windows with fewer merges:

| Classic deployment | Window/slide at 200/s → 10/s (s) |
|---|---|
| `quantile_over_time` (DDSketch α = 0.05) | 300/60 → 900/60 |
| `rate` (exact-increase) | 60/60 → 180/60 |
| top-k (CMS 3×1024) | 60/60 → 300/60 (heap 46,080 → 9,216) |

ASAP's merge and query CPU barely moves (classic 0.54 → 0.13 vCPU,
multigroup 0.106 → 0.078), so it is half or more of the cost at 10/s. Memory
barely moves either, so Fargate costs fall only ~2×. On multigroup, separate
HLL deployments at coarse groupings now cost less than rolling up from fine
ones. The p99 cube moves from DDSketch α = 0.02 to α = 0.05 and still serves
11 RQEs by roll-up. Roll-ups save 17% on multigroup (60% at 200/s) and 5% on
all (11%). With the multi-grouping templates the tightest feasible latency
bound stays 490 ms (SLAs from 1,000 ms); classic's is 21 ms.

## Reproduce

```sh
cargo build --release -p rqe-optimizer --examples
DIR=rqe-optimizer/results/e2e-rate10; TMP=$(mktemp -d)
python3 scripts/export_autosketch_eval_table.py --synthetic --samples-per-sec 10 --keep-duplicate-queries \
    --revision 44ac160dd350f695a09224fd124f0dcfe3dd825a --out $TMP/classic   # classic tables
python3 scripts/export_autosketch_eval_table.py --synthetic --samples-per-sec 10 --keep-duplicate-queries \
    --revision beacf68 --out $TMP/rest                                        # all, multigroup
# copy synthetic-templates{classic,all,multigroup}-shared1.json to $DIR/inputs/tables/
# SAT: the saturation dir of ../autosketch-vs-asap-hydra/README.md, "Reproduce"
for t in classic all multigroup; do
  f=synthetic-templates$t-shared1
  target/release/examples/autosketch_vs_asap synthetic --table $DIR/inputs/tables/$f.json \
      --target p95 --runs 3 --no-chosen --saturation-dir $SAT --out $DIR/$f-tp95.json
  target/release/examples/autosketch_vs_asap synthetic --table $DIR/inputs/tables/$f.json \
      --target p95 --runs 1 --saturation-dir $SAT --out $TMP/$f-tp95.json
  python3 scripts/extract_autosketch_plans.py $TMP/$f-tp95.json $DIR/plans/$f.plans.json.gz
done
```

Without `--samples-per-sec`, the export writes the 200/s tables of
`../autosketch-vs-asap-hydra/inputs/tables/` byte for byte (but the final
newline pre-commit adds). Runs: 2026-10-13,
idle clnode109, runner at sketch-bench `eval/drop-group-rate-templates`
(4dbc20d).
