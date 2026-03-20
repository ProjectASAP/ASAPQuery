# Simple Store Insert Profile

Date: 2026-03-20

## Scope

Profile the ingestion bottleneck of the new `SimpleMapStore` implementation before making further design changes.

Target paths:

- `new/per_key`
- `new/global`

Workload:

- one-shot insert of 10,000 `Sum` precomputes
- release build
- cleanup policy: `NoCleanup`

## Method

Because `perf` and `valgrind` were not available in the environment, I used temporary feature-gated timing counters on the hot insert path, ran a one-shot insert harness, collected the timings, and then removed the instrumentation.

The profiled code path covered:

- `InternTable::intern()`
- `MutableEpoch::insert()`
- outer-loop metadata work in `per_key` and `global`

## Results

### New Per-Key

Total time for 10,000 inserts: `12.766 ms`

| Component | Time | Share of Total |
| --- | --- | --- |
| `MutableEpoch::insert` | `6.984 ms` | `54.7%` |
| `InternTable::intern` | `0.615 ms` | `4.8%` |
| earliest-timestamp update | `1.069 ms` | `8.4%` |

Breakdown inside `MutableEpoch::insert`:

| Sub-component | Time | Share of Total | Share of `MutableEpoch::insert` |
| --- | --- | --- | --- |
| `window_to_ids` maintenance | `2.974 ms` | `23.3%` | `42.6%` |
| `windows` `HashSet` insert | `1.379 ms` | `10.8%` | `19.7%` |
| `raw.push` | `1.057 ms` | `8.3%` | `15.1%` |
| bounds update | `0.658 ms` | `5.2%` | `9.4%` |

### New Global

Total time for 10,000 inserts: `14.614 ms`

| Component | Time | Share of Total |
| --- | --- | --- |
| `MutableEpoch::insert` | `5.766 ms` | `39.5%` |
| `InternTable::intern` | `0.587 ms` | `4.0%` |
| config lookup | `0.608 ms` | `4.2%` |
| metric-set insert | `0.979 ms` | `6.7%` |
| earliest-timestamp update | `0.630 ms` | `4.3%` |
| per-key map entry | `0.590 ms` | `4.0%` |
| insertion-count update | `1.028 ms` | `7.0%` |

Breakdown inside `MutableEpoch::insert`:

| Sub-component | Time | Share of Total | Share of `MutableEpoch::insert` |
| --- | --- | --- | --- |
| `window_to_ids` maintenance | `2.378 ms` | `16.3%` | `41.2%` |
| `windows` `HashSet` insert | `1.261 ms` | `8.6%` | `21.9%` |
| `raw.push` | `0.577 ms` | `3.9%` | `10.0%` |
| bounds update | `0.617 ms` | `4.2%` | `10.7%` |

## Key Findings

1. The primary ingestion bottleneck is active-epoch index maintenance, not label interning.
2. The hottest single cost is `window_to_ids` maintenance.
3. The second major structural cost is maintaining the distinct-window `HashSet`.
4. `InternTable::intern()` is relatively small in this workload because the batch reuses the same label key almost entirely.
5. The `global` variant also pays meaningful per-item metadata overhead outside `MutableEpoch::insert`.

## Conclusion

The current write-path slowdown is mostly caused by synchronous maintenance of current-epoch query indexes.

Most promising improvements, in order:

1. Make `window_to_ids` cheaper.
   - Avoid storing a second aggregate pointer there.
   - Store offsets or `MetricID`s only, or make this index optional/lazy for the active epoch.
2. Reduce or avoid `windows: HashSet<TimestampRange>` maintenance on the hot path.
   - Add a monotonic-ingest fast path when incoming windows are already time-ordered.
3. Hoist metadata updates out of the per-item loop.
   - Especially in `global`, batch config lookup, metric bookkeeping, earliest-timestamp updates, and count updates.
4. Longer term: split ingest from query indexing.
   - Append to a write-optimized memtable/WAL first, then seal/index asynchronously.

## Notes

- These measurements were taken from targeted instrumentation rather than sampling profiler output.
- The repository was restored to a clean state after profiling.
