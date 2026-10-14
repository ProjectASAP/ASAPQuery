# Accelerated random-workload E2E evaluation

## Purpose

Run a practical, end-to-end version of the paper's §6.2 experiment before the
full PR 862 workload.  The experiment should retain the important workload
shape—many input series and few returned series—and show the benefit of
planning a *workload* of related repeating query expressions (RQEs) rather than isolated queries.

The experiment has ten deterministic random workloads.  Each workload contains
exactly ten atomic user queries; binary arithmetic queries are out of scope for
this first version.

The systems are ASAPQuery, Prometheus, VictoriaMetrics, raw ClickHouse, and
ClickHouse with exact materialized views (MVs).  All arms receive identical
data and use the same explicit logical evaluation timestamps.

## Data model

One Prometheus-style gauge, `data`, and a companion cumulative counter,
`data_total`, have 10,000 series.  Their labels form a hierarchy:

```text
region: 4
  service: 5 per region
    job: 10 per service
      label_0: 50 per job
```

Thus `4 * 5 * 10 * 50 = 10,000` leaf series.  Samples arrive every 100 ms
(10 samples/s per series).  Values should have the existing heavy-tailed
distribution used by the synthetic feeder; event/counter increments must be
Zipf-skewed so count-weighted top-k is non-degenerate.

The generator only exposes hierarchy-closed grouping sets:

| Grouping `G` | Number of output groups |
| --- | ---: |
| `region` | 4 |
| `region, service` | 20 |
| `region, service, job` | 200 |

`label_0` is retained for internal, full-cardinality work but is not a
standalone visible grouping: it would produce 10,000 output series.

## Time grid

Each RQE chooses one of these lookback/repetition pairs.  The alternatives
give the seeded generator some variation without making the query schedule
unbounded.

| Lookback `W` | Allowed repeat intervals `T` |
| ---: | ---: |
| 1 s | 1 s |
| 1 min | 1 s, 2 s |
| 5 min | 5 s, 10 s |
| 10 min | 10 s, 20 s |
| 20 min | 30 s, 60 s |

The maximum accelerated backfill is therefore only 20 minutes of event-time
history.  The first 1 s class is for spatial queries; temporal queries use the
1--20 minute classes.  Scraping remains every 100 ms, so each spatial query
still consumes ten samples per leaf series.  Its whole-second repetition and
lookback are compatible with the current exact-MV arm; it is not gated on
[#876](https://github.com/ProjectASAP/ASAPQuery/issues/876).  That issue is
needed only for a future sub-second query/MV schedule.

## Query-template catalogue

Every template instance emits both the optimizer-facing RQE(s) and a concrete
query rendering.  The generator records the query-to-RQE mapping in its
manifest.

### Sum

```promql
sum by (G) (sum_over_time(data[W]))
```

This is a low-output exact control query: it returns 4, 20, or 200 values.

### Top-k by sum

```promql
topk by (G) (k, sum_over_time(data[W]))
```

This is a value-weighted top-k RQE.  Use `k` in `{4, 8}`.  It returns at most
`k` values per group; `G = region, service` with `k = 8`, for example, returns
at most 160 values.

### Top-k by count

Use a skewed event/counter metric and rank the per-series increase:

```promql
topk by (G) (k, increase(data_total[W]))
```

This is a count-weighted top-k RQE.  `count_over_time(data[W])` is not an
acceptable substitute: at a fixed scrape cadence its values are nearly equal
for every healthy series.  The engine's per-series counter-delta path must be
smoke-tested before this template is admitted to an E2E workload.

### Rate

The rate RQE is computed at the full series grouping and must have a final
low-cardinality reducer in the rendered query:

```promql
topk by (G) (k, rate(data_total[W]))
```

A bare `rate(data_total[W])` returns all 10,000 leaf series and is excluded.
Rate-plus-top-k shares the counter-delta prerequisite with count-weighted
top-k, so it is a gated template rather than an unconditional member of the
first pilot workload.

### Quantile

For the 1 s spatial class, PromQL has exact grouped semantics:

```promql
quantile by (G) (phi, data)
```

For a temporal grouped quantile, PromQL cannot directly express a pooled
quantile over every raw sample in each group.  The PromQL-compatible rendering
is instead the average of each leaf series' temporal quantile:

```promql
avg by (G) (quantile_over_time(phi, data[W]))
```

This is deliberately called **average of per-series quantiles**, not a group
quantile.  It has low output cardinality and is the semantics that ClickHouse
must emulate when comparing the same query across all five systems.

SQL can also express the distinct, stronger pooled-quantile semantics:

```sql
SELECT G, QUANTILE(phi, value)
FROM data
WHERE ts > :eval_time - INTERVAL W AND ts <= :eval_time
GROUP BY G
```

That SQL query is a separate logical query and belongs only to the SQL-capable
comparison; it must not be reported as equivalent to the PromQL average of
per-series quantiles.

### Cardinality

True cardinality uses a record/value field, such as `user_id`, rather than
counting Prometheus series:

```sql
SELECT G, COUNT(DISTINCT user_id)
FROM events
WHERE ts > :eval_time - INTERVAL W AND ts <= :eval_time
GROUP BY G
```

It is an HLL/cardinality RQE and has 4, 20, or 200 outputs.  Prometheus and
VictoriaMetrics do not have an equivalent distinct-value query.  Cardinality
queries are therefore executed and reported in the ASAPQuery/ClickHouse/raw-MV
SQL arm, not as a five-system like-for-like comparison.

## Workload generator

The generator takes a stable workload seed and writes one immutable manifest
per workload.  It produces ten workloads with ten user queries each, for 100
queries total.  A query specification contains:

```text
id, capability, metric/value field, grouping G, lookback W, interval T,
quantile phi or top-k k, accuracy SLA, rendered PromQL or SQL, RQE IDs
```

The manifest also contains the label hierarchy, cardinalities of every legal
grouping, value/key distribution facts, feeder seed, and support class
(`common-promql`, `counter-gated-promql`, or `sql-only`).

The `data_facts` contract fixes a 100 ms scrape cadence and 20-minute
backfill, a deterministic Zipf-like ranking of the 10,000 leaf series, a
rank-scaled Pareto gauge and monotonic counter (no resets), plus a 10,000-ID user
universe.  Each leaf rotates through 1,000 deterministically assigned IDs;
the pools overlap across leaves.  The eventual feeder and every baseline
adapter must implement these facts rather than choose their own distributions.

Sampling is constrained rather than purely independent:

- exactly ten atomic queries per workload and no binary queries;
- at least three quantile queries, at least two top-k queries, and at least
  one sum query;
- queries from at least three time classes;
- at least one roll-up chain: same metric, capability, and `(W, T)` at nested
  groupings, for example `(region, service, job)` and `(region, service)`;
- no duplicate `(capability, metric, G, W, T, parameters)` RQEs;
- visible output cardinality is normally at most 160; the 200-group quantile
  and sum cases are allowed as a limited high-output control;
- `phi` is sampled from `{0.50, 0.75, 0.90, 0.95, 0.99}` and `k` from `{4, 8}`.

The generator must validate all rendering constraints before writing a
workload.  In particular, `W` must be compatible with the MV bucket/interval
policy, each RQE needs facts and accuracy coverage, and every requested
grouping must be legal in the hierarchy.

## Planning and rendering model

RQEs are the source of truth for optimization, but query text is not produced
by an arbitrary RQE-to-PromQL converter.  A template emits both in parallel:

```text
template instance
  -> RQEs: capability, grouping, W, T, SLA, distribution facts
  -> user query: PromQL or SQL, annotated with the RQE IDs it consumes
```

The offline optimizer chooses deployments for the RQEs.  Its frozen output is
then converted into ASAPQuery's engine configuration.  The generated manifest
and the converted plan must agree: every query leaf has exactly one planned,
writable deployment.

Fine-to-coarse roll-up is a desired optimization opportunity, not an assumed
property.  Before accepting a random workload, the offline-plan/conversion
preflight must verify that its selected roll-up mappings are supported by the
current planner and engine.  If they are not, retain the workload only with
separate deployments or reject/regenerate it; do not silently claim a roll-up
benefit.

## Staged execution

1. Implement the manifest-driven random workload generator and its validation.
2. Generate the ten seeded workloads; inspect them and freeze the manifests.
3. Choose one representative workload with only `common-promql` templates for
   the five-system pilot.  Do not block this pilot on cardinality or the
   counter-delta-gated templates.
4. Run the optimizer offline at the actual 10 samples/s/series rate, with the
   chosen accuracy SLA and no latency SLA.  Archive inputs, curves/costs,
   selected plan, and converted ASAPQuery configuration.
5. Render baseline inputs from the same manifest: PromQL for Prometheus and
   VictoriaMetrics; semantically equivalent SQL for raw ClickHouse and MV
   ClickHouse.  For a five-system quantile comparison, ClickHouse emulates the
   PromQL average-of-per-series-quantiles semantics.
6. Run the one workload end to end on ASAPQuery, Prometheus, VictoriaMetrics,
   ClickHouse, and ClickHouse+MV.  Accelerated-backfill 20 minutes of logical
   history, drain ingest backlogs, then run a paced measurement tail at the
   frozen explicit evaluation times.
7. Check baseline agreement, ASAPQuery accuracy, query completion/late
   firings, CPU, memory, disk, and per-query latency.  Only after this pilot
   passes should the other nine workloads run.
8. Run SQL-only cardinality and pooled-quantile queries as a separately
   labelled ASAPQuery/ClickHouse/ClickHouse+MV arm; never fold their results
   into a five-system apples-to-apples aggregate.

## Pilot acceptance gates

- The generator reproduces a byte-identical manifest for a fixed seed.
- Every selected RQE has curve and workload-fact coverage.
- The frozen plan converts to a writable ASAPQuery configuration.
- Every common query completes on all five systems at the same logical
  evaluation time.
- Raw ClickHouse and MV ClickHouse agree within floating-point tolerance for
  their shared exact semantics.
- ASAPQuery's observed error satisfies the workload's accuracy SLA.
- Counter-based top-k/rate enters only after its dedicated correctness and
  planner-conversion smoke test passes.
