# Try asap-planner on a SQL Workload

Check whether your SQL workload is amenable to ASAPQuery acceleration by running just the
planner.

## 1. Build

```bash
cargo build --release -p asap_planner
```

This only compiles `asap-planner-rs` and its small internal deps — not the full query engine.

## 2. Write a workload config

```yaml
# sql_workload.yaml
tables:
  - name: metrics_table
    time_column: time
    value_columns: [cpu_usage]
    metadata_columns: [hostname, datacenter, region]
query_groups:
  - id: 1
    repetition_delay: 300            # seconds between repeats of this query
    controller_options:
      accuracy_sla: 0.95
      latency_sla: 100.0
    queries:
      - >-
        SELECT avg(cpu_usage) FROM metrics_table
        WHERE time BETWEEN DATEADD(s, -300, NOW()) AND NOW()
        GROUP BY datacenter
aggregate_cleanup:
  policy: read_based
```

List each recurring query your application actually runs, with its real repeat interval.

### Column fields

For each table:
- `time_column` — the column your queries filter/bucket on (e.g. in a `WHERE time BETWEEN ...` clause).
- `value_columns` — the numeric columns being aggregated (e.g. the column passed to `avg()`, `sum()`, etc.).
- `metadata_columns` — every other column your queries `GROUP BY` or filter on (dimensions like `hostname`, `datacenter`, `region`). List all of them, not just the ones used in these queries' `GROUP BY`. The planner uses the full set to figure out which dimensions to roll up vs. keep grouped.

### `controller_options`

`accuracy_sla` and `latency_sla` are required by the config schema but not currently used by
the planner's decision logic — any numeric values are fine (e.g. the placeholders in the
example above).

### Choosing `repetition_delay` and `--data-ingestion-interval`

- `--data-ingestion-interval` is how often new rows actually land in the table (your ingestion
  cadence) — e.g. `15` if a new batch/row arrives every 15 seconds.
- `repetition_delay` is how often this specific query actually re-runs in your application —
  e.g. `300` for a dashboard panel that refreshes every 5 minutes.
- **Constraint:** `repetition_delay` must be an exact multiple of `--data-ingestion-interval`,
  or the planner errors out.

## 3. Run the planner

```bash
asap-planner --query-language sql \
  --input_config sql_workload.yaml \
  --output_dir ./out \
  --data-ingestion-interval 15 \
  --streaming_engine precompute \
  -v
```

- `--data-ingestion-interval` is the expected data ingestion cadence in seconds (required for SQL mode).
- `--streaming_engine` just needs a valid value (`precompute`) — nothing is actually started.
- `-v` logs which queries were skipped and why.

## 4. Read the result

The planner writes `streaming_config.yaml` and `inference_config.yaml` to `./out`.

- Queries that show up there as aggregations are ones ASAPQuery can accelerate.
- Queries silently skipped (visible with `-v`) are not currently supported — e.g. unsupported
  SQL shapes or queries with no inferable repeat pattern.

If most of your workload appears in `streaming_config.yaml`, ASAPQuery is likely a good fit.
