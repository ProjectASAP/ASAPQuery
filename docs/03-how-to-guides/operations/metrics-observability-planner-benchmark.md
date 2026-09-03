# Run the metrics-observability planner benchmark

Run the complete benchmark workflow from the repository root:

```bash
python3 tools/run_metrics_observability_benchmark.py
```

The command extracts PromQL from the six benchmark sources, normalizes Grafana
interval macros, synthesizes metric-label hints from observed selectors, runs
`asap-planner`, filters low-cost queries, and prints summary metrics plus the
top three unplanned structural clusters.

By default it uses:

- scrape interval: `1000ms`;
- repetition interval: `1000ms`;
- streaming engine: `precompute`;
- punting disabled;
- simple selectors and non-aggregation comparisons excluded from the analysis.

Temporal aggregations such as `rate`, `irate`, and `increase` remain eligible.
The eligibility filter is intended to focus coverage on queries ASAPQuery is
meant to accelerate, rather than treating every cheap metric lookup as a
planner miss.

To write a summary-only report suitable for a PR description or review:

```bash
python3 tools/run_metrics_observability_benchmark.py \
  --report /tmp/metrics-observability-summary.md
```

Intermediate manifests, workload files, planner configs, and logs are created
under a temporary directory and removed when the command finishes. The report
contains counts for extracted, eligible, parseable, planned, and unplanned
expressions, followed by the top missing structural clusters.

Override the corpus or timing assumptions when needed:

```bash
python3 tools/run_metrics_observability_benchmark.py \
  --queries-dir /path/to/queries \
  --scrape-interval-ms 1000 \
  --repetition-interval-ms 1000 \
  --top 3
```

Metric labels are inferred only from labels visible in the benchmark queries.
Without a live Prometheus schema, generated planner configurations are useful
for coverage analysis but should not be deployed directly.
