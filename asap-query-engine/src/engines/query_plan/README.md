# Native query plan

`query_plan` compiles a resolved native query into a request-specific DAG. It is
debug-only for now: the existing executor remains the source of execution.

Nodes are connected by `n<id>` inputs:

- `StoreRead` fetches one aggregation over its requested timestamp bounds.
- `ComposeWindows` turns stored buckets into one aggregate per output timestamp.
- `ResolveKeys` combines a value branch with an optional separate keys branch.
- `Estimate` queries the accumulator statistic with its parameters.
- `LimitTopK` and `Format` are presentation nodes.

`StoreRead` is either a tumbling grid scan or a sliding exact-cover scan. A
range plan has one read branch shared across all output timestamps. Binary
expression DAGs are intentionally deferred to the binary-execution PR.
