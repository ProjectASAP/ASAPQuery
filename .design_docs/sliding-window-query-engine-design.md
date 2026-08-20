# Design: sliding-window query engine execution (#557)

Written up from a `/grilling` session. Supersedes the "not designed yet"
section of [[sliding-window-multi-window-merge-blocker]] — that doc's
findings still hold; this is the design built on top of them. This version
supersedes an earlier draft that proposed `k` separate exact-match store
calls for instant queries (§2 below) before the range-query pipeline
unification was discovered.

## Key insight that shrinks scope

`optimizer/candidate_gen.rs`'s `window_candidates()` already constrains slide
`S` such that `S | W` (window width). That means the `k = range_ms / W`
windows needed to cover a query span, picked at **stride `W`** from the
`S`-spaced storage grid, are non-overlapping by construction — merging them
is exactly as safe as merging tumbling's non-overlapping buckets. No
overlap-aware accounting (subtract, weighted merge, etc.) is needed for
correctness.

Separately: every `subtractable` sketch type in `sketch_properties.rs` is
also `mergeable`, and the live engine has **no subtract execution path at
all today** (`determine_query_method`'s `Subtract` variant is
cost-model-only, used by the offline optimizer, never wired into
`simple_engine`). So Subtract is out of scope for this issue — merge-only
is sufficient for every case `window_compatible` will allow through.

## Decisions

### 1. `capability_matching.rs`

`window_compatible`'s Sliding arm becomes `data_range_ms.is_multiple_of(window_ms)`,
mirroring Tumbling exactly (as the issue specifies). No change to what it
checks about `S` — a config's `S | W` invariant is guaranteed by whatever
constructed it (`candidate_gen.rs`'s own constraint), not re-verified here,
same as Tumbling doesn't re-verify scrape-interval alignment at match time.

### 2. Unified fetch mechanism: reuse the range-query pipeline for instant queries too

There are two independent execution mechanisms in `simple_engine` today:

- **Instant-query path** (`create_store_query_plan` →
  `execute_and_merge_store_queries` → `merge_precomputed_outputs`): one store
  call, merges whatever `TimestampedBucketsMap` comes back, trusting the
  store call's `(start, end)` bounds were already correct.
- **Range-query path** (`execute_range_query_pipeline`, `mod.rs:1423`): one
  big range fetch over the whole query horizon, builds an in-memory
  `bucket_map: HashMap<exact_start_timestamp, bucket>`, then **per output
  step** walks `t = window_start; t < current_time; t += window_size_ms`,
  doing an exact-timestamp lookup and merging whatever's found (missing
  entries silently skipped — "partial data is okay").

For Tumbling both are safe, because every stored bucket already sits on the
one `W`-spaced grid — a plain range fetch naturally returns exactly the
non-overlapping buckets needed either way. For Sliding this isn't true: the
store holds `S`-spaced buckets, denser than `W`. The instant path's "merge
whatever's in the map" would double-count; the range path's stride-`W`
exact-timestamp walk already does the right thing — it only ever looks up
`W`-strided positions, silently ignoring the intervening `S`-spaced entries.

**Decision: extract the range pipeline's per-step "walk by `window_size_ms`,
exact-lookup, merge found ones" logic into a shared helper, and have
**Sliding's instant-query path call it too** (as a single-step case — the
same shape as a range query whose horizon is one step wide). Tumbling's
existing instant-query path (`merge_precomputed_outputs`) is left
**completely untouched** — it isn't broken, and touching working code here
is unnecessary risk for zero benefit.

Net effect:
- Sliding's instant path now does **one range fetch** (`query_precomputed_output`,
  not `_exact`) over `[end - k*W, end)`, then the shared stride-`W` walk
  helper, called once.
- `query_precomputed_output_exact` / `is_exact_query: true` becomes
  **unused once this ships** — nothing sets `is_exact_query: true` anymore.
  Not removing the `Store` trait method or the flag in this issue (that's a
  4-backend-impl change, separate scope) — but flagging it in the
  legacy/unused-code issue (#559) as a fresh entry once #557 lands, so it
  doesn't get forgotten.
- **Code comment requirement:** wherever the shared walk helper is
  extracted, and wherever the old sliding-specific "skip merge, expecting 1
  precompute per key" branch is deleted, leave a comment explaining *why*
  the instant path now reuses range-query machinery (the double-counting
  problem above) — this is the actual non-obvious reasoning a future reader
  would otherwise have to re-derive from scratch.

Rejected (superseded): a loop of `k` separate `query_precomputed_output_exact`
calls for the instant path. Correct, but duplicates stride-selection logic
that the range pipeline already has, instead of reusing it.

### 3. Merge: delete the sliding special case, don't extend it

`execute_and_merge_store_queries`'s current sliding branch
("Skipping merge, expecting 1 precompute per key") assumed exactly one
bucket per key. That assumption is gone. Per §2, Sliding's instant queries
now route through the shared range-pipeline walk helper instead of
`merge_precomputed_outputs` — so this branch is **deleted outright**, not
extended to loop. Net: less code than today.

### 4. Epoch/slide-interval alignment

Worker buckets sit on the `slide_interval_ms` grid, anchored at Unix epoch 0
(`precompute_engine/window_manager.rs`). Two things need alignment, and
they're different in kind:

- **The fetch bounds** (`[end - k*W, end)`) don't need to be grid-aligned —
  `query_precomputed_output` is a plain numeric range scan, not a grid
  lookup, so a generously-bounded span is fine as long as it covers
  everything needed.
- **The walk's lookup points** (`window_start`, and each `t` stepped by `W`)
  *do* need grid alignment — they're exact-timestamp `HashMap` lookups
  against `slide_interval_ms`-grid keys. Misalignment doesn't error, it
  silently returns "no data in window" (instant path) or skips that output
  sample (range path) — a correctness-shaped bug that looks like a data gap,
  not a crash. **This is exactly the kind of thing that needs a code comment
  at the alignment call site**, since the failure mode gives no signal that
  something's wrong.
- **Fix location:** the walk helper itself (shared by both instant and
  range-query callers per §2) floor-aligns `current_time`/`window_start` to
  `slide_interval_ms` from epoch 0, **per invocation** — not just once
  upfront. `step_ms` need not be a multiple of `S`, so alignment can drift
  right back off the grid between range-query steps if only done once at the
  start.
- **Strategy:** silent floor-align + `warn!` log, same precedent as the
  existing `data_ingestion_interval_ms` alignment in
  `align_end_timestamp_promql`.
- **Scope:** only the values used inside the walk change.
  `timestamps.end_timestamp` / each range-query step's nominal `current_time`
  (what's reported back as the answer's timestamp) stays untouched.
- Tracked as a known gap, not fixed here: #558 (silent alignment staleness)
  — both this and the existing ingestion-interval alignment silently serve
  staler-than-requested data with only a log line to show for it.

### 5. `validate_range_query_params`

Confirmed **not load-bearing** for the live range-query algorithm: the real
loop increments `current_time += step_ms` directly (`mod.rs:1567`);
`buckets_per_step`/`lookback_bucket_count` are computed but only ever used in
a debug log line. The check (`step % window_size_ms == 0`) appears to be a
leftover from an earlier, superseded index-based sliding-window algorithm —
the test module's `simulate_sliding_window`/`simulate_sliding_window_with_alignment`
(index-based) vs. `simulate_timestamp_based_lookup` (current, explicitly
commented *"the new implementation that handles gaps in data correctly"*)
are fossil evidence of that evolution.

**Decision:** delete the check entirely (not just relax it for Sliding) —
confirm via the existing test suite that nothing else depends on it before
removing. Rename the parameter from `tumbling_window_ms` to `window_size_ms`
throughout (`validate_range_query_params`, `RangeQueryExecutionContext`,
etc.) since the logic was never actually tumbling-specific, just named as if
it were.

### 6. `cleanup.rs`

Runtime semantics confirmed by reading the actual enforcement code
(`simple_map_store/global.rs:374-399`): `read_count_threshold` is a
delete-after-N-reads counter, incremented once per **store call** that
touches a window's time range — regardless of whether the caller's
downstream logic actually uses that window. It must be a safe **upper
bound** on total future reads a bucket could receive; under-provisioning is
a correctness bug (premature deletion), over-provisioning is just wasted
retention.

This interacts with §2's unification in a way that isn't obvious and
**needs a code comment at the formula site**: because Sliding's fetch now
pulls a whole `[end - k*W, end)` range in one store call (§2), the store-level
read-count accounting (`global.rs:527-531`, which bumps every window
overlapping the scanned range) bumps **every `S`-spaced bucket in that span**
— not just the `k` buckets the in-memory walk actually merges. The number of
buckets touched per query is `data_range_ms / slide_interval_ms`, **not**
`data_range_ms / window_size_ms` — using the latter would under-provision
`read_count_threshold` whenever `S < W` (the normal case), risking deletion
of a bucket before a later query that still needs it.

- New parameter needed: `slide_interval_ms`, threaded from the caller
  (`planner/promql.rs:287`) alongside the existing parameters — for
  Tumbling this is irrelevant (`S == W` there, formula unchanged).
- `ReadBased` (the policy that actually matters — confirmed every config in
  the repo defaults to it, `CircularBuffer` is never selected anywhere):
  `read_count_threshold = (data_range_ms / slide_interval_ms) * num_steps`,
  `num_steps` computed the same way Tumbling already does it.
- `CircularBuffer`: mirror Tumbling's shape too —
  `ceil((data_range_ms + range_duration_ms) / window_size_ms)` — as a
  mechanical translation while already touching this function. Uses
  `window_size_ms` (not `slide_interval_ms`) since it's a retain-N-buckets
  count, not a read-count — the two policies are counting fundamentally
  different things and shouldn't share a divisor by coincidence. **Flagged
  as untested by construction**: nothing in the repo exercises
  `CircularBuffer` at all today, for either window type. Tracked in #559.
- Fix the dangling gap the blocker doc flagged: Sliding's branch currently
  returns before reaching the `NoCleanup` error case, relying on caller
  discipline (every current caller happens to guard `NoCleanup` before
  calling) rather than the function's own match arm.

### 7. Multi-population keys — no change needed (confirmed, not designed)

`create_keys_query_params` already does a plain range query
(`is_exact_query: false`) for both `SetAggregator` (latest window) and
`DeltaSetAggregator` (from-beginning-of-time), regardless of window type.
Both are set-union merges (`mergeable: true, subtractable: false` in
`sketch_properties.rs`) — a key-set union is idempotent, so merging
overlapping `S`-grid buckets on the key side is correct by construction,
unlike value accumulators where overlap causes double-counting. No changes
needed here.

### 8. Aggregation priority — no change needed (confirmed, not designed)

`aggregation_priority` prefers larger `window_size_ms` (fewer buckets to
merge). Same reasoning applies unchanged to Sliding (fewer `k`).

## Test plan

End-to-end correctness test, using the existing
`test_utilities/engine_factories.rs` harness (`create_engine_single_pop*` —
builds a real `SimpleEngine` + `SimpleMapStore`, no new scaffolding needed).

`should_use_sliding_window()` is hardcoded `false` (planner can't emit
Sliding configs until #555 unblocks) — so "plan" in this test means
**hand-constructing** a `Sliding` `AggregationConfig` directly, same as
`capability_matching_tests.rs` already does for its configs, not invoking
the live planner.

Matrix across different `k` (window_size = data_range / {1, 2, 3, 6}) to
cover the stride logic at different divisor counts, e.g.:
- `k=1`: exact-match case, same as today's existing behavior (backward-compat
  check) — now served via the unified range-pipeline-with-one-step path
  instead of `query_precomputed_output_exact`.
- `k=2, 3, 6`: multi-window merge at different strides, both as instant
  queries (single-step walk) and as PromQL range queries (multi-step walk)
  to cover both callers of the shared walk helper.

For each: insert distinct known values (e.g. `Sum` accumulator, values
`1..=N`) at every `slide_interval_ms` across a wide enough span, fire a
query whose `data_range_ms = k * window_size_ms`, and assert the result
equals the sum of exactly the `k` non-overlapping `W`-strided buckets —
**not** the sum of all `S`-spaced buckets in range. That second assertion is
the actual regression catcher: it's what would fail if a future change
reused the range-query path naively instead of the stride-`W` walk.

Also worth a dedicated test for the read-count accounting in §6: insert a
Sliding config, run a query that fetches a `k*W`-wide range, and assert the
store's read-count for **every** `S`-spaced bucket in that range incremented
by 1 — not just the `k` that were merged — to lock in the §6 semantics
against a future "optimize the fetch to only touch what's merged" change
that would silently break the cleanup-threshold math.

## Implementation approach: TDD, staged

This is correctness-critical query-serving logic (wrong-but-plausible
results, not crashes, are the failure mode — double-counted merges,
silently-dropped alignment). Implement test-first, in the same order as the
"Decisions" sections above:

1. Write the §"Test plan" matrix first (`k=1,2,3,6`, instant + range
   variants, plus the read-count accounting test from §6) against a
   hand-built `Sliding` config — confirm each fails against the current code
   for the reason the design predicts (e.g. `k>1` cases fail with "expected
   1 precompute per key", not some unrelated error).
2. Land §1 (`capability_matching.rs`) and §5 (delete the dead
   `validate_range_query_params` check) independently — small, mechanical,
   each verifiable on its own before touching the fetch/merge machinery.
3. Land §2–4 (unification + alignment) together, since they're one
   coherent change to the same code path — re-run the full matrix after,
   not just the cases that were previously failing.
4. Land §6 (`cleanup.rs`) last, with its own dedicated read-count test,
   since it depends on §2's fetch shape being final.

Stop after each stage for review/commit rather than landing this as one
large diff — matches how prior critical-path refactors in this repo have
been staged.

## Follow-up issues (not part of #557)

- #558 — silent floor-alignment staleness (both the existing
  ingestion-interval case and the new slide-interval case).
- #559 — legacy/unused code: `simple_map_store::legacy::{global, per_key}`
  (referenced only by a comparative benchmark, never constructed in
  production), the untested `CircularBuffer` cleanup policy, and (new, from
  this revision) `query_precomputed_output_exact` / `is_exact_query: true`
  becoming unused once §2's unification ships — add this as a fresh bullet
  to #559 during implementation.
