# Bootstrap Config from Prometheus Query Log

Generate sketch configs from real query traffic instead of hand-authoring a workload YAML.

## Steps

### 1. Enable the Prometheus query log

Add to your Prometheus startup flags:
```
--query.log-file=/var/log/prometheus/query.log
```

Let it run for a representative period (hours to days). Each line is a JSON entry:
```json
{"params":{"query":"rate(http_requests_total[5m])","start":"2025-12-02T18:00:00Z","end":"2025-12-02T18:00:00Z","step":0},"ts":"2025-12-02T18:00:00.001Z"}
```

### 2. Run the planner

`--query-log` mode has no separate metrics-hint file — label sets are fetched directly from
your live Prometheus instance for whichever metrics show up in the log, so `--prometheus-url`
is required:

```bash
asap-planner \
  --query-log /var/log/prometheus/query.log \
  --prometheus-url http://localhost:9090 \
  --output_dir ./configs \
  --prometheus_scrape_interval 15 \
  --streaming_engine arroyo
```

This writes `streaming_config.yaml` and `inference_config.yaml` to `./configs/`.

## Notes

- Queries appearing only once are skipped (need frequency to infer repeat interval)
- Unsupported PromQL patterns (e.g. `absent`, complex multi-level aggregations) are skipped
- Repeat interval is inferred from median inter-arrival time, rounded to the nearest scrape interval
- If you'd rather hand-author the workload (no query log needed) or supply label sets without a
  live Prometheus, use `--input_config` instead — see
  [Try asap-planner on a PromQL Workload](try-asap-planner-promql.md)
