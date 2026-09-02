# PromQL differential testing quick start

The primary workflow is now the fixture-driven runner:

```bash
cd promql-compliance/runner
make run
```

That command generates the ASAPQuery planner configuration from the selected
dataset and suite, starts an isolated Prometheus/ASAPQuery Compose project,
sends the same remote-write body to both targets, waits for the data to be
queryable, runs all configured instant and range evaluations, checks
range-at-t against instant-at-t, writes `differential-report.json`, and tears
the project down.

Add another dataset under `datasets/` or query suite under `suites/`; the
runner does not require source-code changes for either.

The copied upstream compliance harness below remains available for running
the large generic PromQL suite, but it is not the recommended path for the
deterministic differential regression workflow.

This directory contains a manual, deterministic differential test setup:

1. Prometheus is the reference implementation.
2. ASAPQuery is the implementation under test.
3. The seeder writes the same samples to both systems.
4. The harness compares configured range and instant-query results.

The PR does not start either service or automatically pass the seeder's
timestamp to the harness.

## Requirements

- Prometheus with remote-write receiving enabled.
- ASAPQuery's precompute engine with HTTP remote-write ingest enabled.
- Go 1.25 or newer.
- The normal ASAPQuery quickstart precompute/inference/streaming setup.

## Start the services

Start Prometheus with the remote-write receiver enabled:

```bash
prometheus --config.file=prometheus.yml \
  --web.enable-remote-write-receiver
```

This guide assumes Prometheus is at `http://localhost:9090`.

Use an ASAPQuery configuration like this, retaining the other settings from
the repository's quickstart configuration:

```yaml
streaming_engine: "precompute"

http_server:
  port: 8088

ingest:
  type: "http_remote_write"
  port: 9091
```

Here, `8088` is the ASAPQuery query API and `9091` is its remote-write ingest
API. The seeder uses the latter; the harness uses the former.

## Seed the data

From `promql-compliance/seeder/`:

```bash
go run ./cmd/seed \
  --reference-url=http://localhost:9090 \
  --test-url=http://localhost:9091
```

The command prints `base-time-ms`. The fixed dataset is written at offsets
`0, 60, ..., 1200` seconds from that base. Set the harness `end_time` to
`base + 1200 seconds`. The `--base-time-ms` flag can pin a recent base time,
but omit it for the usual current-time default.

## Configure and run the harness

Update `harness/config.yaml` with the printed timestamp and the actual seeded
metric names. A minimal configuration looks like:

```yaml
reference_target_config:
  query_url: "http://localhost:9090"
test_target_config:
  query_url: "http://localhost:8088"

query_time_parameters:
  end_time: "2026-08-27T14:30:00Z" # base time plus 1200 seconds
  range_in_seconds: 1200
  resolution_in_seconds: 60

query_tweaks: []

test_cases:
  - query: 'topk(3, http_requests_total)'
  - query: 'rate(http_requests_total[5m])'
  - query: 'sum(rate(http_requests_total[5m]))'
  - query: 'sum(node_memory_used_bytes)'
  - query: 'checkout_up{service="checkout"}'
```

The checked-in config contains placeholder timestamps and query names. Do not
combine it with `promql-test-queries.yml` unless you also seed that upstream
suite's `demo_*` metrics.

Run from `promql-compliance/harness/`:

```bash
go run ./cmd/promql-compliance-tester \
  -config-file=config.yaml \
  -output-format=text \
  -output-passing \
  -query-parallelism=1
```

The harness currently performs one range comparison and one instant comparison
at the configured `end_time` for each test case. It compares Prometheus with
ASAPQuery; it does not directly assert every hand-computed value in the
seeder README.

## Controls

- Edit `seeder/dataset.go` to change metric names, labels, values, gaps, and
  sample offsets.
- Edit `test_cases` to change the PromQL queries.
- Change `end_time`, `range_in_seconds`, and `resolution_in_seconds` to change
  the evaluation window.
- Configure tolerance, dropped labels, case handling, and expected failures
  with `query_tweaks` and test-case fields.
- Configure URLs, headers, and basic authentication in the target blocks.
- Use `-output-format=text|html|json|tsv`, `-output-passing`, and
  `-query-parallelism=N` to control output and concurrency.

After changing the dataset, reseed both systems and update the harness time
window and query names together. For this deterministic dataset, prefer an
empty `query_tweaks` list or a very small tolerance; a broad tolerance can
hide real differences.

## Troubleshooting

- Seeder connection refused: check ASAPQuery's ingest port (`9091`).
- Harness connection refused: check ASAPQuery's query port (`8088`).
- Empty results pass unexpectedly: check metric names and timestamps; equal
  empty responses are still considered equal.
- Prometheus rejects samples: use a recent base time and reseed.
- Results disagree immediately after seeding: wait for ASAPQuery's precompute
  pipeline to process the write.
