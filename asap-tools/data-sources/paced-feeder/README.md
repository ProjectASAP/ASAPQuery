# paced_feeder

Generates a seeded synthetic workload on the fly and sends it in real time,
one second of event time per wall-clock second, to either:

- **ASAPQuery** over Prometheus remote write (`/api/v1/write`), or
- **ClickHouse** over its HTTP interface (`INSERT … FORMAT RowBinary`).

It drives the data-plane vs ClickHouse evaluation
(`asap-tools/experiments/DATA_PLANE_CLICKHOUSE_EVAL_PLAN.md`). Two feeders
started with the same workload flags send identical data, so each system gets
the same stream without a dataset file on disk.

## Workload

- `--groups C` values of `label_0` (`g0`, `g1`, …) and `--series-per-group s`
  values of `instance` (`i0`, `i1`, …) per group: `C·s` series.
- Each series emits `--samples-per-sec` samples per second, evenly spaced.
- Values are Pareto(`--pareto-shape`, `--pareto-scale`), all ≥ the scale.
- A value depends only on `(--seed, series, sample index)`, never on wall-clock
  time or the sink.

Event time starts at the next whole wall-clock second (or `--start-ms`), so
`NOW()`-relative queries see the data as it arrives.

## Usage

```sh
cargo build --release

# ASAPQuery
target/release/paced_feeder --sink remote-write \
  --url http://localhost:9090/api/v1/write --metric data \
  --groups 1000 --series-per-group 100 --duration-secs 900 \
  --stats-out asap-feed.json

# ClickHouse
target/release/paced_feeder --sink clickhouse \
  --url http://localhost:8123 --table data \
  --groups 1000 --series-per-group 100 --duration-secs 900 \
  --stats-out clickhouse-feed.json
```

The ClickHouse table must have these columns (any engine and ordering):

```sql
CREATE TABLE data (ts DateTime64(3), label_0 String, instance String, value Float64)
ENGINE = MergeTree ORDER BY (label_0, instance, ts);
```

`CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` (or the matching flags) set
credentials.

## Pacing and stats

Each tick's rows are split into `--batch-rows` requests and sent with up to
`--concurrency` in flight. The next tick starts only after every request of
the current one returns, so samples arrive out of order only within a tick.
A tick starting at `t` holds samples spaced evenly over `(t, t + 1 s]`, the
same left-open interval as ASAPQuery's one-second windows, so a tick never
spans two windows. Out-of-order samples within it are then safe for
ASAPQuery as long as its allowed lateness is at least 1 s (the default is
5 s). Prometheus' remote-write receiver needs an out-of-order window of at
least 1 s (`storage.tsdb.out_of_order_time_window`).
`--samples-per-sec` is at most 1000, so timestamps (in ms) never repeat. The
default batch size is 50,000 rows for remote write, which keeps a compressed
request under axum's 2 MB body limit in ASAPQuery's ingest, and 100,000 for
ClickHouse, where every INSERT creates a part.

`--stats-out` writes, per tick, the rows, bytes, start lag and elapsed time,
plus totals:
- `achieved_rows_per_sec`;
- `overrun_ticks` (ticks that took longer than one second);
- `max_start_lag_ms`;
- the first errors.

A run with `overrun_ticks > 0` did not keep up. Rows and bytes count only
successful requests. Any failed request makes the feeder exit non-zero, and a
tick in which every request fails (e.g. a wrong `--url`, table or credentials)
stops the run early; the stats are still written.

Measured on a CloudLab c6320 (feeder pinned to 2 cores, 10 s at 1e6 rows/s):

| Sink | Ticks over 1 s | Feeder CPU |
|---|---|---|
| ClickHouse 26.10 | 0 | 0.15 cores |
| ASAPQuery precompute engine | 0 | 0.34 cores |
