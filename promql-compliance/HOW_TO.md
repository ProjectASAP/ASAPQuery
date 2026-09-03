# Differential experiment how-to guide

All commands below assume the current directory is
`promql-compliance/runner`.

## Add a dataset

Create a YAML fixture under `promql-compliance/datasets/`:

```yaml
name: cpu-example
series:
  - metric: cpu_usage_seconds_total
    labels:
      host: server-a
      mode: user
    samples:
      - {offset_seconds: 0, value: 0}
      - {offset_seconds: 60, value: 10}
      - {offset_seconds: 120, value: 25}
```

Each `series` entry is one labeled time series. Add another entry with
different labels to represent another series of the same metric. Sample
timestamps are seconds relative to the run’s `baseTime`; values are written to
both targets without changing the payload.

Smoke-test the stack with the checked-in fixture and matching temporal suite:

```bash
go run ./cmd/differential-runner \
  --dataset ../datasets/single-rate.yaml \
  --suite ../suites/temporal.yaml \
  --compose-file ../docker-compose.yml
```

The runner derives the planner’s metric and label hints from the fixture. No
source-code change is needed to add a dataset. To run the `cpu-example` above,
create a suite whose expressions use `cpu_usage_seconds_total`, as shown in
the next section.

## Add or change queries

Create or edit a suite under `promql-compliance/suites/`:

```yaml
name: cpu-queries
comparison_defaults:
  value_tolerance:
    relative: 0
    absolute: 0.000001

queries:
  - name: cpu-rate
    expr: rate(cpu_usage_seconds_total[5m])
    instant_offsets_seconds: [300, 600]
    range:
      start_offset_seconds: 300
      end_offset_seconds: 600
      step_seconds: 60

  - name: user-cpu-at-end
    expr: cpu_usage_seconds_total{mode="user"}
    instant_offsets_seconds: [600]
```

For each query:

- `expr` is the PromQL expression sent to both targets.
- `instant_offsets_seconds` selects instant-query times relative to
  `baseTime`.
- `range` selects a range-query start, end, and evaluation step.
- Instant offsets must lie inside the configured range when both are present.
- A query may contain only instant evaluations, only a range, or both.

Run it with:

```bash
go run ./cmd/differential-runner \
  --dataset ../datasets/cpu-example.yaml \
  --suite ../suites/cpu-queries.yaml \
  --compose-file ../docker-compose.yml
```

## Change query timing

`range.step_seconds` controls the evaluation cadence of a range query. It is
not the same as the planner’s repetition delay.

The runner generates one planner query group per suite query. For each group,
it derives a compatible repetition delay from the largest range selector in
the expression, caps it at five minutes, and adjusts it to work with the
range step. Queries without a range selector use the one-second ingestion
interval. The query’s range step is also passed to the planner as `step_ms`.

There is currently no YAML field for an arbitrary per-query repetition delay.
To change the five-minute cap or one-second ingestion assumption, change the
runner’s generated-config logic in `runner/run.go`. Do not edit the generated
configuration: it is temporary and is deleted after the run.

## Configure tolerance

Set a suite-wide default:

```yaml
comparison_defaults:
  value_tolerance:
    relative: 0.001
    absolute: 0.000001
```

Override either value for one query:

```yaml
queries:
  - name: approximate-query
    expr: some_query
    instant_offsets_seconds: [600]
    comparison:
      value_tolerance:
        relative: 0.01
```

If tolerance is omitted, values are compared exactly. A tolerance should be
small and justified; a broad tolerance can hide a correctness bug.

## Test expected failures

Use this only when the query is deliberately expected to fail on both
targets:

```yaml
- name: intentionally-unsupported
  expr: unsupported_expression
  instant_offsets_seconds: [600]
  expect_error: true
```

For ordinary queries, an error from either target makes the comparison fail.
Two matching errors do not accidentally pass. With `expect_error: true`, both
targets must return errors; a success from either target fails the comparison.

This also makes the suite useful for finding unsupported ASAPQuery queries:
leave `expect_error` unset for a query that Prometheus supports. A Prometheus
success paired with an ASAPQuery error produces `passed: false` and records the
ASAPQuery error in `testError`.

## Run with DEBUG logging

The default Compose stack uses `INFO`. Temporarily change the query engine
service in `promql-compliance/docker-compose.yml`:

```yaml
environment:
  RUST_LOG: DEBUG
```

Then retain the services while running:

```bash
go run ./cmd/differential-runner \
  --dataset ../datasets/single-rate.yaml \
  --suite ../suites/temporal.yaml \
  --compose-file ../docker-compose.yml \
  --keep-services \
  --output /tmp/differential-report-debug.json
```

Inspect the query-engine container:

```bash
docker logs asapquery-differential-queryengine-1 2>&1 \
  | rg "destination=asap|destination=prometheus|none_unsupported|QUERY ENGINE SUCCESS"
```

Interpret the destination markers as follows:

- `destination=asap`: ASAPQuery answered locally.
- `destination=prometheus`: the query was forwarded to the backend.
- `destination=none_unsupported`: the query was unsupported and forwarding was
  disabled.
- `QUERY ENGINE SUCCESS`: the local query engine produced a result.

Clean up the retained stack afterward:

```bash
ASAP_TEST_CONFIG_DIR=/tmp docker compose \
  --project-name asapquery-differential \
  --file ../docker-compose.yml \
  down --volumes --remove-orphans
```

## Use already-running targets

When Prometheus and ASAPQuery are managed outside Compose, omit
`--compose-file` and provide their query and write endpoints:

```bash
go run ./cmd/differential-runner \
  --dataset ../datasets/cpu-example.yaml \
  --suite ../suites/cpu-queries.yaml \
  --reference-url http://localhost:9090 \
  --test-url http://localhost:8088 \
  --reference-write-url http://localhost:9090 \
  --test-write-url http://localhost:9091
```

In this mode, the services’ existing configurations are authoritative. The
runner only writes the fixture data and sends the comparison queries.

## Read the report

The JSON report contains:

- `range`: Prometheus range result versus ASAPQuery range result.
- `instant`: cross-target comparison at each configured instant time.
- `referenceParity`: Prometheus range-at-t versus Prometheus instant-at-t.
- `testParity`: ASAPQuery range-at-t versus ASAPQuery instant-at-t.
- `passed`: the conjunction of every comparison in the report.

On failure, `referenceError`, `testError`, or `diff` identifies the failure
class. The current report records differences and errors, but does not include
the complete raw response bodies.

## Diagnose common failures

- Planner fails during startup: inspect the generated timing error and check
  the query lookback and range step.
- Readiness times out: check that the first suite query is supported and
  should return samples at its first instant time.
- Both results are empty: verify metric names, labels, base time, and sample
  offsets.
- Results disagree immediately after ingestion: allow the ASAPQuery
  precompute pipeline to process the remote-write batches, then rerun.
- `destination=prometheus` appears unexpectedly: inspect the ASAPQuery backend
  configuration and `forward_unsupported_queries` setting.
