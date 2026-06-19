# Try asap-planner on a PromQL Workload

Check whether your PromQL workload is amenable to ASAPQuery acceleration by running just the
planner.

## 1. Build

```bash
cargo build --release -p asap_planner
```

This only compiles `asap-planner-rs` and its small internal deps — not the full query engine.

## 2. Write a workload config

```yaml
# promql_workload.yaml
metrics:
  - metric: http_requests_total
    labels: [instance, job, method, status]
query_groups:
  - id: 1
    repetition_delay: 300            # seconds between repeats of this query
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 100.0
    queries:
      - "sum by (job) (rate(http_requests_total[5m]))"
aggregate_cleanup:
  policy: read_based
```

List each recurring query your application actually runs (dashboard panels, alerting rules,
recording rules), with its real repeat interval.

### `metrics` field

`metrics` is a hint: for each metric your queries reference, list every label it carries. The
planner uses the full label set to figure out which dimensions to roll up vs. keep grouped — it
isn't limited to the labels in this particular query's `by (...)` clause. If you have a live
Prometheus instance, you can skip this and pass `--prometheus-url` instead (see below) to
auto-infer label sets per metric.

### `controller_options`

`accuracy_sla` and `latency_sla` are required by the config schema but not currently used by
the planner's decision logic — any numeric values are fine (e.g. the placeholders above).

### Choosing `repetition_delay` and `--prometheus_scrape_interval`

- `--prometheus_scrape_interval` is your actual Prometheus scrape interval — a fact about your
  existing setup, not something to tune.
- `repetition_delay` is how often this specific query actually re-runs — e.g. `300` for a
  dashboard panel refreshing every 5 minutes, or an alert rule's `evaluation_interval`.
- Unlike SQL mode, there's no hard error if `repetition_delay` isn't a multiple of the scrape
  interval, but `rate`/`increase`/`quantile_over_time` queries need at least 60 scraped data
  points per repeat window to be considered worth accelerating when `--enable-punting` is set —
  i.e. `repetition_delay >= 60 * prometheus_scrape_interval`.

## 3. Run the planner

```bash
asap-planner \
  --input_config promql_workload.yaml \
  --output_dir ./out \
  --prometheus_scrape_interval 15 \
  --streaming_engine precompute \
  -v
```

- `--streaming_engine` just needs a valid value (`precompute`, `arroyo`, or `flink`) — none are
  actually started.
- `-v` logs which queries were skipped and why.
- Optional: add `--prometheus-url http://localhost:9090` to auto-infer label sets from a live
  Prometheus instead of hand-listing them under `metrics`.

## 4. Read the result

The planner writes `streaming_config.yaml` and `inference_config.yaml` to `./out`.

- Queries that show up there as aggregations are ones ASAPQuery can accelerate.
- Queries silently skipped (visible with `-v`) are not currently supported — e.g. unsupported
  PromQL patterns (`absent`, complex multi-level aggregations) or queries seen too rarely to
  infer a repeat interval.

If most of your workload appears in `streaming_config.yaml`, ASAPQuery is likely a good fit.

## Alternative: bootstrap from a real query log

Instead of hand-authoring `query_groups`, you can feed the planner a Prometheus query log
(`--query.log-file`) directly with `--query-log` + `--prometheus-url`, and it will infer queries
and repeat intervals from real traffic. See
[Bootstrap Config from Query Log](bootstrap-config-from-query-log.md) for that workflow.
