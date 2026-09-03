# PromQL differential testing quick start

This is the recommended workflow for comparing Prometheus and ASAPQuery on
the same deterministic data.

## Requirements

- Docker with Compose.
- Go 1.25 or newer.

## Run the checked-in smoke test

```bash
cd promql-compliance/runner
make run
```

The command:

1. Loads `../datasets/single-rate.yaml` and `../suites/temporal.yaml`.
2. Generates planner and query-engine configuration in a temporary directory.
3. Starts Prometheus, the ASAPQuery planner, and the ASAPQuery query engine.
4. Sends identical remote-write data to Prometheus and ASAPQuery.
5. Waits for both targets to expose the data.
6. Runs the configured instant and range queries against both targets.
7. Compares Prometheus with ASAPQuery and checks range-at-t against instant-at-t.
8. Writes `differential-report.json` and tears down the stack.

The command exits with status 1 if any comparison fails.

## Run a different dataset or suite

```bash
go run ./cmd/differential-runner \
  --dataset ../datasets/sparse-checkout.yaml \
  --suite ../suites/temporal.yaml \
  --compose-file ../docker-compose.yml
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the stack and
[HOW_TO.md](HOW_TO.md) for extending datasets, queries, timing, tolerances,
and logging.
