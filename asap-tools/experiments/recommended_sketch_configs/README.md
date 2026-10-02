# Recommended sketch configs, end to end

Runs the sketch configs that sketch-bench recommends for the dataset-analysis
queries (`asap-tools/dataset-analysis`) through ASAPQuery on replayed cluster
traces, and compares the measured error with sketch-bench's prediction and
with ASAPQuery's default sketch parameters. The planner is unchanged: each
config is passed as the global `sketch_parameters` override.

## Generate the configs

```bash
cd asap-tools/experiments
python recommended_sketch_configs/recommended_sketch_configs.py generate \
    --recommendations <sketch-bench>/docs/figures/saturation/grid_cost/recommendations.csv
```

This writes two configs per query under
`config/experiment_type/recommended_sketch_configs/`: `<query>_recommended`
(the recommended config) and `<query>_default` (config.yaml's
`sketch_parameters`: CMS 3x1024, KLL K=20). Families with no planner sketch
(CountSketch, DDSketch, CMS-heap top-k for non-topk queries) and tables with
no exporter (Alibaba MCRRTUpdate, CallGraph) are skipped and listed.

| dataset-analysis query | PromQL run | planner sketch |
|---|---|---|
| google_2011 `cpu_by_job_id` instant | `sum by (job_id) (google_mean_cpu_usage_rate_0)` | CMS |
| google_2011 `cpu_by_job_id` 5m | `sum by (job_id) (sum_over_time(google_mean_cpu_usage_rate_0[5m]))` | CMS |
| google_2011 `cpu_p99` instant | `quantile(0.99, google_mean_cpu_usage_rate_0)` | KLL |
| alibaba_v2022 `ms_cpu_by_msname` instant | `sum by (ms_name) (alibaba_microservice_cpu_usage)` | CMS |
| alibaba_v2022 `ms_cpu_by_msname` 5m | `sum by (ms_name) (sum_over_time(alibaba_microservice_cpu_usage[5m]))` | CMS |

`cpu_p99` has no range run: its range form `quantile_over_time` is a
per-series quantile in PromQL, not the single value stream sketch-bench
measured.

## Run

Each config runs one `sketchdb` mode with `query_prometheus_too`, so every
ASAP answer has a Prometheus answer for the same timestamp. On a single
machine, with the local provider:

```bash
python experiment_run_e2e.py \
    experiment_type=recommended_sketch_configs/<name> experiment.name=<name> \
    '~providers.cloudlab' '+providers.local.home_dir=<home>' controller.punting=false
```

Trace data goes under `/data/cluster_traces/{google,alibaba_msmetrics}`:
Google `part-00000-of-00500.csv.gz` (task_usage) and Alibaba
`MSMetrics_0.csv.gz` (`MSMetricsUpdate_0.tar.gz` extracted and sorted by
timestamp, as `cluster_data_exporter/bin/alibaba/sort_and_format.sh` does).
The Alibaba configs scrape every 10 s: one scrape holds about 470k series
(117 MB) and takes about 5 s.

Then:

```bash
python recommended_sketch_configs/recommended_sketch_configs.py summarize \
    --recommendations <recommendations.csv> --experiments-dir <home>/experiment_outputs
```

## Results

One run per config on an idle 56-core node (CloudLab clnode109), 20 query
repetitions each, medians over repetitions. Measured error is the ARE over
the 100 largest keys (sketch-bench's CMS metric) for key queries and the rank
error for the p99 query (from the replayed values, see below). Targets are
dataset-analysis's: ARE <= 0.05, rank error <= 0.01.

| query | config | predicted error | measured error | target met | all-keys ARE | QE peak RSS (MB) | ASAP / Prometheus p50 latency (ms) |
|---|---|---|---|---|---|---|---|
| google cpu_by_job_id instant | rec. CMS 3x4096 | 0.042 | 0.00005 | yes | 0.68 | 99 | 124 / 1639 |
| | default CMS 3x1024 | | 0.0031 | yes | 57 | 79 | 116 / 1847 |
| google cpu_by_job_id 5m | rec. CMS 3x4096 | 0.043 | 0.00005 | yes | 0.70 | 90 | 100 / 3804 |
| | default CMS 3x1024 | | 0.0031 | yes | 58 | 74 | 92 / 4262 |
| google cpu_p99 instant | rec. KLL k=200 | 0.0020 | 0.0019 (value err. 10.6%) | yes | | 44 | 3.5 / 1860 |
| | default KLL K=20 | | 0.0153 (value err. 46%) | no | | 43 | 3.7 / 1857 |
| | planner default K=500 | | 0.0004 (value err. 2.1%) | yes | | 46 | 3.3 / 2070 |
| alibaba ms_cpu_by_msname instant | rec. CMS 3x16384 | 0.021 | 0.0013 | yes | 2.2 | 317 | 629 / 3235 |
| | default CMS 3x1024 | | 0.20 | no | 258 | 275 | 620 / 3130 |
| alibaba ms_cpu_by_msname 5m | rec. CMS 3x16384 | 0.021 | 0.0013 | yes | 2.2 | 282 | 678 / 4350 |
| | default CMS 3x1024 | | 0.20 | no | 251 | 258 | 640 / 4494 |

- Every recommended config met its target, and the predictions were upper
  bounds: CMS 16x to 900x below the estimate (the estimate is the worst case
  over the whole trace, rounded toward the harder side; a run replays only
  the first window of one file), KLL k=200 within 5% of it (0.0019 vs
  0.0020).
- The defaults miss the target where the recommendation is larger than them:
  Alibaba `sum by (ms_name)` (28k keys) at 3x1024 has 20% error on its
  largest keys, and KLL K=20 (config.yaml's e2e default) has 1.5% rank error
  for p99. On Google `sum by (job_id)` (3k keys) 3x1024 already meets the
  target, so the recommended 3x4096 is larger than this replay needed.
- The sketch size does not change ASAP query latency here; the larger
  CMS configs add 16 to 42 MB of query-engine RSS across all retained windows.
- The all-keys ARE shows CMS overestimating small keys: it stays far above
  the target for every config, which is why the target is on the 100 largest
  keys.

The p99 rank error is computed offline: the exporter had exported the
93,719 part-0 rows starting by 615 s (aggregation_type 0, the 5-minute window
starting at 600 s), and Prometheus's p99 matched their exact p99 (0.1194);
rank error = |F(estimate) - 0.99| over those values.

`post_experiment/single_experiment/calculate_fidelity.py` was also run on
every experiment. For the key queries its per-key time-series metrics are
NaN or inf: some keys sum to 0 (division by zero) and gauges replayed within
one trace window are constant over the run (correlation undefined). For p99
its MAPE is 13.6% (k=200), 43% (K=20) and 3.7% (K=500).

## Gaps

- One run per config, one trace file per dataset, and only its first
  minutes (Google replays at 1/10 speed, so a run sees the first 5-minute
  window of trace time; Alibaba sees the first 5 to 10 minutes).
- Google metrics are the exporter's `aggregation_type` 0 series, which hold
  most rows; dataset-analysis's `cpu_rate` includes both types.
- CountSketch, DDSketch and the top-k family have no planner sketch, so their
  recommendations are not run. No query in the sets uses HLL.
- Memory is the query engine's whole RSS, not per-sketch bytes; sketch-bench
  reports 49 KB (3x4096) and 197 KB (3x16384) per sketch.

## Reproducing

- Running these configs end to end with the local provider needs the runner fixes in ProjectASAP/ASAPQuery#751.
- `results_summary.csv` is the summary table of the runs above.
- p99 rank error is computed offline against the replayed trace values:
  `python kll_rank_error.py --trace <task_usage part-00000-of-00500.csv.gz> --outputs <experiment_outputs>`
