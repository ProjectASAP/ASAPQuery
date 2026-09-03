# Differential experiment architecture

The differential runner answers one question:

> Given the same samples and the same PromQL request, does ASAPQuery produce
> the same result as Prometheus?

The experiment uses Prometheus as the reference implementation. ASAPQuery is
the system under test.

## Stack

```text
                         query requests
                    ┌─────────────────────┐
                    │  differential runner│
                    └─────────┬───────────┘
                              │
                ┌─────────────┴─────────────┐
                │                           │
         Prometheus reference        ASAPQuery query API
            host :19090                 host :18088
                ▲                           ▲
                │                           │
         remote-write data          remote-write ingest
                │                       host :19091
                │                           ▲
                └───────────┬───────────────┘
                            │
                    identical samples

       ASAPQuery planner ──► shared planner-output volume ──► query engine
```

The Compose services are:

| Service | Role | Host ports |
| --- | --- | --- |
| `prometheus` | Reference PromQL implementation and backend | `19090` |
| `planner` | Generates ASAPQuery inference and streaming configuration | none |
| `queryengine` | Runs ASAPQuery in precompute mode | query `18088`, ingest `19091` |

Inside the Compose network, the query engine reaches Prometheus at
`http://prometheus:9090`. The engine configuration sets
`forward_unsupported_queries: false`, so an unsupported query is rejected
instead of being answered by Prometheus.

## Run lifecycle

The runner performs these stages:

1. Load a dataset fixture and a query suite.
2. Choose a base timestamp. Dataset sample offsets and suite evaluation times
   are relative to this timestamp.
3. Generate temporary planner and engine configuration.
4. Start Prometheus and wait for its health check.
5. Run the planner. Its output is placed in a named shared volume.
6. Start the ASAPQuery query engine after the planner completes.
7. Send the same encoded remote-write batches to Prometheus and ASAPQuery.
8. Wait for both query APIs to be ready and for the first probe query to return
   samples.
9. Execute every configured range and instant query against both targets.
10. Write the JSON report and, by default, remove the containers, network, and
    named volumes.

Use `--keep-services` when inspecting logs or making manual requests after the
runner exits.

## Configuration boundaries

The checked-in dataset and suite are inputs to the runner, not service
configuration. The runner generates these temporary files:

- `controller-config.yaml`: metrics, query groups, planner timing, and cleanup
  policy.
- `engine_config.yaml`: HTTP ports, backend, ingestion, logging, and paths to
  planner output.

The planner receives metric and label hints from the dataset, so it does not
need to discover them from a separate live data source in this workflow.

## Comparison model

For each query, the runner can perform four related checks:

- Range comparison: Prometheus range result versus ASAPQuery range result.
- Instant comparison: Prometheus instant result versus ASAPQuery instant result.
- Reference parity: Prometheus range-at-t versus Prometheus instant-at-t.
- Test parity: ASAPQuery range-at-t versus ASAPQuery instant-at-t.

The report passes only if every configured comparison passes. An unexpected
HTTP/query error from either target fails the comparison, even if both targets
fail. The exception is a query explicitly marked `expect_error: true`, where
both targets must return an error.

Equal successful empty results are still equal results; use a dataset and
probe query that should contain samples when testing ingestion readiness.

## Current limitations

The runner’s readiness check currently probes the first suite query and its
first evaluation time. It does not yet expose a general ingestion watermark or
drain signal for proving that every asynchronous batch has finished processing.
For the same reason, suites should currently use a supported, non-empty first
query as their readiness probe. These are tracked as follow-up synchronization
work.
