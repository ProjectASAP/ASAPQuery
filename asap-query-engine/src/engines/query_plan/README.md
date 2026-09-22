# Native query plan

`query_plan` compiles a resolved native query into a request-specific DAG. It is
debug-only for now: the existing executor remains the source of execution.

Nodes are connected by `n<id>` inputs:

- `StoreRead` — input: none; fetches one aggregation over its timestamp bounds.
- `ComposeWindows` — input: one `StoreRead`; composes buckets for each output timestamp.
- `ResolveKeys` — inputs: a value composition and optional keys composition; resolves output keys.
  Without a keys input, the value accumulator supplies its own keys.
- `Estimate` — input: `ResolveKeys`; computes the requested statistic with its parameters.
- `LimitTopK` — input: `Estimate`; keeps the highest-ranked `k` candidates.
- `Format` — input: `Estimate` or `LimitTopK`; applies protocol-specific output labels.

`StoreRead` is either a tumbling grid scan or a sliding exact-cover scan. A
range plan has one read branch shared across all output timestamps. Binary
expression DAGs are intentionally deferred to the binary-execution PR.
