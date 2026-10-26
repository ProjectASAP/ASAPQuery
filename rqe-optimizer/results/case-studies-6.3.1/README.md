# §6.3.1 case studies: what each method deploys

The two illustrative workloads of the paper's §6.3.1, planned by every method
of the §6.3 evaluation (`../autosketch-vs-asap-hydra/`) with the same inputs:
#213's curves, cost table and Hydra study, 95% accuracy, cost by use.
`inputs/*.json` are the tables, from `scripts/export_case_studies.py`;
`*-tp95.json` are the runner's results, each plan's per-RQE choices kept
(`chosen`).

## Performance monitoring

Three dashboards over `cpu_usage` (the mixed workload's data: 1e4 series in
10 jobs, 200 samples/s each):

```
// repeating every 15s
quantile by (job) (0.5, cpu_usage)            quantile by (job) (0.9, cpu_usage)
quantile_over_time(0.5, cpu_usage[1m])        quantile_over_time(0.9, cpu_usage[1m])
// repeating every 1 min
quantile_over_time(0.9, cpu_usage[10m])       quantile_over_time(0.9, cpu_usage[30m])
// repeating every 30 min
quantile_over_time(0.9, cpu_usage[6h])        quantile_over_time(0.9, cpu_usage[24h])
```

| Method | Cost, CPU (vCPU) | Deployments | What it deploys |
|---|---|---|---|
| ASAP | 0.10 | 2 | DDSketch (α = 0.1) per job over each scrape, for both instant queries; DDSketch per series in 15 s tumbling windows, for all six `quantile_over_time` queries (both quantiles, every lookback), merging 4 to 5,760 windows per query |
| PerQuery | 0.31 | 8 | one DDSketch per query, each with its own tumbling window (15 s, 60 s or 1,800 s) |
| AutoSketch-Adapted | 4.14 | 8 | one sliding DDSketch per query, window = lookback, slide = repeat interval; the 24 h query keeps 48 overlapping windows, each sample inserted into each |

ASAP's cheapest CPU-only plan holds 18 GiB and takes 23.8 s per batch (the
24 h query merges 5,760 windows per series). With memory priced (Fargate
weights) it picks 3 deployments, 0.53 GiB and 493 ms: 0.0075 $/h, against
PerQuery's 0.014 and AutoSketch's 0.17.

## Anomaly detection

p99 request latency over 5 min on `http` (region × service × endpoint ×
status), every group held to the target:

```
// repeating every 15s
quantile by (service) (0.99, request_latency[5m])           quantile by (region) (0.99, request_latency[5m])
// repeating every 1 min
quantile by (service, region) (0.99, request_latency[5m])   quantile by (service, endpoint) (0.99, request_latency[5m])
// repeating every 5 min
quantile by (service, region, status) (0.99, request_latency[5m])   quantile by (region, endpoint) (0.99, request_latency[5m])
```

| Method | Cost, CPU (vCPU) | Deployments | What it deploys |
|---|---|---|---|
| ASAP | 0.11 | 3 | DDSketch per (region, service, status) group in 15 s windows, rolled up to answer by (service), (region), (region, service) and (region, service, status); its own DDSketch for (service, endpoint) and for (region, endpoint), which (region, service, status) cannot produce |
| PerQuery | 0.23 | 6 | one DDSketch per query, at its own grouping and repeat interval |
| AutoSketch-Adapted | 1.96 | 6 | one sliding 5 min DDSketch per query |

A DDSketch merge is the sketch of the pooled data, so a rolled-up answer is
the coarse group's p99, not a quantile of quantiles.

## Reproduce

```sh
cargo build --release -p rqe-optimizer --examples
C=rqe-optimizer/results/case-studies-6.3.1
python3 scripts/export_case_studies.py $C/inputs
SAT=$(mktemp -d); cp -r rqe-optimizer/results/autosketch-vs-asap-inputs/saturation/out_* $SAT/
cp -r rqe-optimizer/results/autosketch-vs-asap-hydra/inputs/saturation/* $SAT/
for t in perf-monitoring anomaly-detection; do
  target/release/examples/autosketch_vs_asap synthetic --table $C/inputs/$t.json \
      --target p95 --runs 3 --saturation-dir $SAT --out $C/$t-tp95.json
done
```

Costs are deterministic; planning times here are from one clnode130 run and
are not reported.
