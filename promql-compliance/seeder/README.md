# promql-compliance seeder

Part of [#594](https://github.com/ProjectASAP/ASAPQuery/issues/594): pushes a
fixed, hand-authored dataset via Prometheus remote-write to two targets — a
real Prometheus (started with `--web.enable-remote-write-receiver`) and
ASAPQuery's own remote-write ingest endpoint (`asap-query-engine/src/drivers/ingest/prometheus_remote_write.rs`,
served at `POST /api/v1/write`, snappy + protobuf, same wire format). Using
the exact same code path against both targets means there's no risk of two
different ingestion mechanisms producing false diffs in the comparison
harness that consumes this.

This is a standalone Go module (`go.mod` in this directory) — there is no
other Go code in the ASAPQuery repo.

## Build / test

```
go build ./...
go test ./...
```

No live Prometheus/ASAPQuery instance is required to build or test: the unit
tests build a `WriteRequest`, snappy-encode it, decode it back, and assert
round-trip equality; the HTTP path is tested against an `httptest.Server`
instead of a real endpoint.

## Usage

```
go run ./cmd/seed --reference-url=http://localhost:9090 --test-url=http://localhost:9091
```

Each URL is the target's base URL; the seeder POSTs to `<url>/api/v1/write`.
Prints the base timestamp (Unix ms) it anchored the dataset to — the
comparison harness needs this to compute absolute query timestamps (see
below).

Optionally pass `--base-time-ms` to pin the anchor instead of using
"now minus 30 minutes" (the default). Real Prometheus rejects samples that
are too old or too far in the future relative to wall-clock time, so the
dataset's timestamps are expressed as **offsets in seconds from a base time
chosen at seed time**, not fixed absolute timestamps. The *values* are fully
deterministic; only the absolute wall-clock placement moves on each run.

## Dataset shape

Defined in `dataset.go`. 20-minute window, sampled every 60s: offsets
0, 60, 120, ..., 1200 (seconds) from the run's base time. Six series across
three metrics:

| Metric | Labels | Kind | Range |
|---|---|---|---|
| `http_requests_total` | `host="a"` | counter, +1/s | offsets 0..1200 (21 samples) |
| `http_requests_total` | `host="b"` | counter, +2/s | offsets 0..1200 (21 samples) |
| `node_memory_used_bytes` | `host="a"` | gauge, triangle 500->1000->500 | offsets 0..1200 (21 samples) |
| `node_memory_used_bytes` | `host="b"` | gauge, flat 2000 | offsets 0..1200 (21 samples) |
| `checkout_up` | `service="checkout",region="us-east"` | gauge, value 1 | offsets 0..300 only (6 samples) |
| `checkout_up` | `service="checkout",region="us-west"` | gauge, value 1 | offsets 900..1200 only (6 samples) |

The `checkout_up` pair is deliberate: the us-east series stops at offset 300
and us-west doesn't start until offset 900, a 600s gap — comfortably past
PromQL's default 5m (300s) staleness/lookback window. This is built to
exercise the same class of instant-vs-range divergence bugs as #589/#583/#584.

## Hand-computed expected values

Let `base` = the printed base-time-ms. All timestamps below are
`base + offset_seconds * 1000`.

- `rate(http_requests_total{host="a"}[5m])` at any `t` in `[base+300s, base+1200s]` = **1.0** exactly.
- `rate(http_requests_total{host="b"}[5m])` at the same `t` = **2.0** exactly.
- `sum(rate(http_requests_total[5m]))` at `t = base+600s` = **3.0**.
- `increase(http_requests_total{host="a"}[5m])` at `t = base+600s` = **300**.
- `http_requests_total{host="a"}` instant value at `t = base+1200s` = **1200**.
- `max_over_time(node_memory_used_bytes{host="a"}[20m])` = **1000**; `min_over_time(...)` = **500**.
- `node_memory_used_bytes{host="a"}` instant value at `t = base+600s` (the peak) = **1000**.
- `avg_over_time(node_memory_used_bytes{host="b"}[20m])` = **2000** exactly (flat series).
- `sum(node_memory_used_bytes)` at `t = base+600s` = **3000** (1000 + 2000).
- Instant query `checkout_up{service="checkout"}` at `t = base+660s` = **empty result vector**
  (us-east's last sample at offset 300 is 360s stale, > 5m lookback; us-west's
  first sample isn't until offset 900). This is the instant/range divergence probe.
- Range query `checkout_up{service="checkout"}[20m]` evaluated at `t = base+1200s`
  returns a matrix with **two series**: us-east (6 samples, offsets 0..300) and
  us-west (6 samples, offsets 900..1200) — i.e. the range query surfaces both
  series even though no single instant in the gap does.

See the doc comment at the top of `dataset.go` for the full derivation.

## Known limitations

- `go mod init`/`go get` pulled the real `github.com/prometheus/prometheus/prompb`
  and `github.com/golang/snappy` packages from the public Go module proxy —
  network access was available in this environment, so no vendoring fallback
  was needed. `prompb` messages use `github.com/gogo/protobuf/proto` for
  marshal/unmarshal (matches upstream `prometheus/prometheus`), which is an
  explicit dependency here (`push.go`).
- Building `prometheus/prometheus` bumped the module's Go toolchain
  requirement to 1.25.8 (from the system's 1.21.13); `go` auto-downloaded and
  used the newer toolchain per `go.mod`'s `go 1.25.8` directive. This only
  affects `promql-compliance/seeder`'s own module — it does not touch the
  Rust workspace or any other part of the repo.
- Not run against a live Prometheus or ASAPQuery instance — none was
  available in this environment. `go build ./...` and `go test ./...` both
  pass; the HTTP POST path (headers, path, body encoding) is covered via
  `httptest.Server` in `push_test.go`.
