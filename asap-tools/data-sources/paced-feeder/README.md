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
`(--seed, metric, series, sample index)` (a record only on
`(--seed, metric, record index)`), never on wall-clock time or the sink,
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

#### `records` metrics

The multi-grouping templates' metrics (`http`, `flows`, or `data_3_http` with
several metrics) are streams of records, each with a value of every label and
of every value field:

```json
"http": {"type": "records", "records_per_sec": 2000000,
         "labels": [{"name": "region", "cardinality": 4, "skew": 0.5},
                    {"name": "service", "cardinality": 25, "skew": 1.1},
                    {"name": "endpoint", "cardinality": 25, "child_of": "service", "skew": 1.1},
                    {"name": "status", "cardinality": 4, "skew": 2.0}],
         "values": {"latency": {"pareto": 2.0, "scaled_by": "service",
                                "anomaly": {"label": "endpoint", "factor": 10}},
                    "user_id": {"zipf": 0.8, "keys": 1000000}}},
"flows": {"type": "records", "records_per_sec": 4000000,
          "labels": [{"name": "dst_subnet", "cardinality": 1000, "skew": 1.1}, …],
          "values": {"src_ip": {"zipf": 0.0, "keys": 1000000,
                                "burst": {"label": "dst_subnet", "value": 999,
                                          "share": 0.05, "keys": 10000}}}}
```

Every draw of record `n` is a hash of the metric's seed (from `--seed` and its
name), `n` and the draw, so records match sketch-bench's `label_shares()`
(`scripts/export_autosketch_eval_table.py`), which the optimizer assumed:

- **Labels** are drawn independently, a child given its parent: value `i` (a
  child's index `i` under its parent) has weight `(i+1)^-skew`, so `0` is the
  heaviest. Values are named as for gauges (`service="3"`,
  `endpoint="3.17"`).
- **Keys** (`zipf` θ over `keys`): the key of Zipf rank `r` is `r`, so key 0 is
  the most frequent.
- **Burst**: with probability `share` a record is a burst record: its `label`
  is `value` (a child index for a child label) and its key is uniform over
  `keys` burst keys numbered after the background keys (`1000000` to
  `1009999` for `flows`). Other records draw both normally, so `dst_subnet`'s
  shares are Zipf times `1 - share`, plus `share` at `999`.
- **Pareto** (`latency`): `1000 · U^(-1/a)` (`U` in (0, 1]), times its service's
  fixed factor with `scaled_by`, times `factor` on the anomaly label's first
  value (`endpoint="0.0"`, the heaviest endpoint of the heaviest service). A
  service's factor is aqpbm-datagen's `scale_factor` (as in sketch-bench's
  `configs/datagen/hydra_http_latency.yaml`, scale 1000 and `scale_range:
  [1.0, 10.0]`): `10^u` with `u` in [0, 1) a SplitMix64 hash of the value's
  FNV-1a xor `--seed`, so it is the same in every metric of a table. Scaling
  keeps the tail index `a`.
- **Time.** A tick starting at `t` spreads its `records_per_sec` records
  evenly over 100 slots of 10 ms; slot `s` has every record at
  `t + (s + 1) · 10 ms`, so the tick lies in `(t, t + 1 s]`.
- `--records-per-sec` overrides every records metric's rate, e.g. to scale a
  run down.

`--verify-facts` checks the generated data against the description without
sending anything: for every non-empty subset of a metric's labels it prints
the expected number of distinct label-value tuples (the product of the
cardinalities of the subset plus every label's `child_of` ancestors) and the
number among the generated series (for records metrics, among all label-value
combinations a record can have), and exits non-zero if any differ. For records
metrics it also draws records `0..1e6` and compares each label's three
heaviest values' shares (a child's index pooled over parents, i.e. its share
given the parent), the burst value's share and the share of burst records with
`label_shares()`; a share fails if it is off by more than 5 standard errors,
`5·sqrt(p(1-p)/n)`.

```sh
$ paced_feeder --workload-table synthetic-templatesclassic-shared1.json --verify-facts
data: 10000 series
  labels                                       expected    generated
  job                                                10           10
  label_0                                         10000        10000
  job,label_0                                     10000        10000
data_total: 10000 series
  …

$ paced_feeder --workload-table synthetic-templatesmultigroup-shared1.json --verify-facts
flows: 4000000 records/s, 3000000 label combinations
  labels                                       expected    generated
  dst_subnet                                       1000         1000
  …
  shares of 1000000 records (tolerance 5 standard errors)
  label        value                          expected  generated  tolerance
  dst_subnet   0                               0.17047    0.17069    0.00188
  …
  dst_subnet   999                             0.05009    0.05037    0.00109
  …
  burst        records                         0.05000    0.05029    0.00109
  burst keys: 9938 distinct of 10000
http: 2000000 records/s, 10000 label combinations
  …
  endpoint     *.0 (given service)             0.29841    0.29855    0.00229
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

A record of a records metric becomes one sample per value field, named
`<metric>.<field>` (`http.latency`, `http.user_id`), with the record's labels
and timestamp. ASAPQuery's SQL mode routes a sample to the aggregations whose
metric is `table.value_column` (or whose table is the sample's name), so each
field feeds only its own aggregations. Many records of a slot share a series
and timestamp, so Prometheus, which keeps one sample per series and
timestamp, is not a target for records metrics.

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

A records metric goes to the table named after it, with columns `ts`, the
labels in the description's order, then the value fields in name order (keys
are integers below 2^53, exact in `Float64`):

```sql
CREATE TABLE http (ts DateTime64(3), region String, service String,
                   endpoint String, status String, latency Float64, user_id Float64)
ENGINE = MergeTree ORDER BY (region, service, endpoint, status, ts);
CREATE TABLE flows (ts DateTime64(3), dst_subnet String, dst_port String,
                    proto String, src_ip Float64)
ENGINE = MergeTree ORDER BY (dst_subnet, dst_port, proto, ts);
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

For records metrics, each request generates its own records as it encodes
them, so generation runs on all cores. For remote write, `--batch-rows`
counts samples, a record being one per value field (`http`: 25,000 records
per request). Rows in the stats are records.

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

Records metrics at full rate against a local HTTP stub that discards the
bodies (shared, loaded 56-core machine, default flags, mean tick time over
10 s, no overruns):

| Metric | Remote write | ClickHouse |
|---|---|---|
| `http` (2e6 records/s, 4e6 samples/s) | 171 ms, 2.5 cores | 104 ms, 1.1 cores |
| `flows` (4e6 records/s) | 195 ms, 2.8 cores | 117 ms, 1.5 cores |
| both (6e6 records/s) | 340 ms, 4.9 cores | 194 ms, 2.5 cores |
