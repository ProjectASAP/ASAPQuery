# paced_feeder

Generates a seeded synthetic workload on the fly and sends it in real time,
one second of event time per wall-clock second, to either:

- **ASAPQuery** over Prometheus remote write (`/api/v1/write`), or
- **ClickHouse** over its HTTP interface (`INSERT … FORMAT RowBinary`).

It drives the data-plane vs ClickHouse evaluation
(`asap-tools/experiments/DATA_PLANE_CLICKHOUSE_EVAL_PLAN.md`). Two feeders
started with the same workload flags (and table) send identical data, so each system gets
the same stream without a dataset file on disk.

## Workload

The feeder generates either the fixed `--groups` workload or the metrics of a
workload table (`--workload-table`). Either way, a value depends only on
`(--seed, metric, series, sample index)`, never on wall-clock time or the sink,
and each series emits `--samples-per-sec` samples per second, evenly spaced.

### `--groups` workload

- `--groups C` values of `label_0` (`g0`, `g1`, …) and `--series-per-group s`
  values of `instance` (`i0`, `i1`, …) per group: `C·s` series of one gauge,
  named by `--metric` (default `data`).
- Values are Pareto(`--pareto-shape`, `--pareto-scale`), all ≥ the scale.
- `--samples-per-sec` defaults to 1.

### `--workload-table` workload

`--workload-table FILE` reads `workloads[0].e2e.metrics` of a sketch-bench
workload table (`scripts/export_autosketch_eval_table.py`) and sends every
metric in it, or only `--metric NAME`. For example:

```json
"data": {"type": "gauge",
         "labels": [{"name": "job", "cardinality": 10},
                    {"name": "label_0", "cardinality": 1000, "child_of": "job"}],
         "samples_per_sec_per_series": 200,
         "values": {"value": {"zipf": 1.1, "keys": 10000}, "dist": {"pareto": 2.0}}},
"data_total": {"type": "counter", "labels": […], "samples_per_sec_per_series": 200,
               "values": {"increment": {"zipf": 1.1, "keys": 10000}}}
```

- **Labels.** A label has `cardinality` values, or, with `child_of`, that many
  under each value of its parent, which must be listed earlier. A root label's
  values are `0`, `1`, …; a child's are its parent's value, a `.`, and the
  child index (`job="3"`, `label_0="3.17"`), so a child value names its parent
  and is unique across parents. Labels without `child_of` form a cross
  product. The metric has one series per combination: the product of all
  cardinalities (1e4 above).
- **Key skew.** `zipf` θ over `keys` keys, where a key is a series (`keys`
  must equal the number of series). The series of Zipf rank `r` (0 the
  heaviest) has scale `N·(r+1)^-θ / Σ_j (j+1)^-θ`, so scales average 1. Ranks
  are a shuffle seeded by `--seed` alone, so the heavy series spread across
  parents (not all under `job="0"`) and a series is equally heavy in every
  metric of the table.
- **Gauge** (`value` and `dist`): sample `n` of series `i` is
  `scale_i · U^(-1/a)` with `U` a hashed uniform in (0, 1]: Pareto(a, 1)
  scaled by the series' weight, which keeps the tail index `a`.
- **Counter** (`increment`): sample `n` of series `i` is `(n+1) · scale_i`,
  i.e. a constant increment `scale_i` per sample, starting at the first
  sample and never resetting, so `rate()` is `scale_i · samples/s`.
- `--samples-per-sec` overrides `samples_per_sec_per_series`, e.g. to scale a
  run down. Without it, the sent metrics must share one rate.
- Other metric types (the multi-grouping `records` metrics) are rejected.

`--verify-facts` checks the generated series against the description without
sending anything: for every non-empty subset of a metric's labels it prints
the expected number of distinct label-value tuples (the product of the
cardinalities of the subset plus every label's `child_of` ancestors) and the
number among the generated series, and exits non-zero if any differ.

```sh
$ paced_feeder --workload-table synthetic-templatesclassic-shared1.json --verify-facts
data: 10000 series
  labels                                       expected    generated
  job                                                10           10
  label_0                                         10000        10000
  job,label_0                                     10000        10000
data_total: 10000 series
  …
```

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

# A workload table's metrics (data and data_total) to ASAPQuery
target/release/paced_feeder --sink remote-write \
  --url http://localhost:9090/api/v1/write \
  --workload-table synthetic-templatesclassic-shared1.json --duration-secs 900
```

Remote write sends each metric's name as `__name__` and its labels sorted by
name, as Prometheus requires; it also works against Prometheus'
remote-write receiver (`--web.enable-remote-write-receiver`).

With `--groups`, the ClickHouse table (`--table`) must have these columns (any
engine and ordering):

```sql
CREATE TABLE data (ts DateTime64(3), label_0 String, instance String, value Float64)
ENGINE = MergeTree ORDER BY (label_0, instance, ts);
```

With `--workload-table`, each metric goes to the table named after it, with
columns `ts`, then the metric's labels in the description's order, then
`value`. For the example above:

```sql
CREATE TABLE data (ts DateTime64(3), job String, label_0 String, value Float64)
ENGINE = MergeTree ORDER BY (job, label_0, ts);
CREATE TABLE data_total (ts DateTime64(3), job String, label_0 String, value Float64)
ENGINE = MergeTree ORDER BY (job, label_0, ts);
```

`CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` (or the matching flags) set
credentials.

## Pacing and stats

Each tick's rows are split into requests of about `--batch-rows` rows and
sent with up to `--concurrency` in flight. A tick's rows are series-major and
requests are cut only between series, so each series' samples of a tick go
in one request, in timestamp order; the next tick starts only after every
request of the current one returns. Each series' samples therefore arrive in
order (counters never seem to reset, and Prometheus needs no out-of-order
window), while samples of different series of a group may arrive out of
order within a tick. A tick starting at `t` holds samples spaced evenly over
`(t, t + 1 s]`, the same left-open interval as ASAPQuery's one-second
windows, so a tick never spans two windows, and that reordering is safe for
ASAPQuery as long as its allowed lateness is at least 1 s (the default is
5 s).
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
