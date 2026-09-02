# Differential runner

The runner executes one dataset/query-suite pair against Prometheus and
ASAPQuery. It sends the same remote-write bytes to both targets, evaluates
each configured instant and range query, and also compares range-at-t against
instant-at-t within each target.

Run the isolated Compose stack and execute the checked-in fixture/suite:

```bash
make run
```

Run against already-running targets:

```bash
go run ./cmd/differential-runner \
  --dataset ../datasets/sparse-checkout.yaml \
  --suite ../suites/temporal.yaml \
  --reference-url http://localhost:9090 \
  --test-url http://localhost:8088 \
  --reference-write-url http://localhost:9090 \
  --test-write-url http://localhost:9091
```

Add repeated `--compose-file` flags to have the runner start and tear down an
isolated Docker Compose project. The runner generates the planner and engine
configuration from the fixture and suite. The report is written as JSON and
the process exits with status 1 when any comparison fails.
