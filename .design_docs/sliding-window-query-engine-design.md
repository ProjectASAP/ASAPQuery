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
(`precompute_engine/window_manager.rs`). The fix differs between instant and
range queries, because §5 gives range queries a way to guarantee alignment
by construction instead of re-deriving it defensively on every step.

- **The fetch bounds** (`[end - k*W, end)`), in both cases, don't need to be
  grid-aligned — `query_precomputed_output` is a plain numeric range scan,
  not a grid lookup, so a generously-bounded span is fine as long as it
  covers everything needed.
- **The walk's lookup points** (`window_start`, and each `t` stepped by `W`)
  *do* need grid alignment — they're exact-timestamp `HashMap` lookups
  against `slide_interval_ms`-grid keys. Misalignment doesn't error, it
  silently returns "no data in window" (instant path) or skips that output
  sample (range path) — a correctness-shaped bug that looks like a data gap,
  not a crash. **This is exactly the kind of thing that needs a code comment
  at the alignment call site**, since the failure mode gives no signal that
  something's wrong.
- **Instant queries:** floor-align `current_time` to `slide_interval_ms`
  once per call, silently + `warn!`, same precedent as the existing
  `data_ingestion_interval_ms` alignment in `align_end_timestamp_promql`.
  There's no `step` to reason about for a single point, so this bounded
  (up to one `S`) staleness is unavoidable here — tracked as a known gap in
  #558, not fixed in this issue.
- **Range queries:** floor-align only the **first** `current_time` (the
  query's `start`), once, before the loop — not per step. §5's new
  `step % slide_interval_ms == 0` validation guarantees every subsequent
  `current_time = start + n*step` stays exactly on the grid by construction
  (adding `S`-divisible increments to a grid-aligned point can't drift off
  the grid), and since `S | W` always holds for a valid config, every
  `W`-strided walk point inside each step stays grid-aligned too. **Zero**
  staleness for range queries that pass validation — strictly better than
  the instant-query floor-align-and-hope approach, and it's why §5 exists
  as a real check rather than dead weight.
- **Scope:** only the values used inside the walk change.
  `timestamps.end_timestamp` / the query's nominal `start` (what's reported
  back as the answer's timestamp(s)) stays untouched.

### 5. `validate_range_query_params`

Initially thought to be dead: the check (`step % window_size_ms == 0`) isn't
load-bearing for the live Tumbling algorithm's actual control flow — the
real loop increments `current_time += step_ms` directly (`mod.rs:1567`), and
`buckets_per_step`/`lookback_bucket_count` (the values that would need this
invariant) are computed but only ever used in a debug log line. Fossil
evidence in the test module (`simulate_sliding_window`, index-based, vs.
`simulate_timestamp_based_lookup`, current, explicitly commented *"the new
implementation that handles gaps in data correctly"*) suggests it's a
leftover from an earlier, superseded index-based algorithm.

**Decision, revised: leave Tumbling's check completely untouched** — "not
load-bearing today" isn't the same as "safe to delete," and this is the one
piece of #557 that could otherwise touch **live** Tumbling query behavior.
Zero risk beats a marginal cleanup; if it's truly dead, that's a separate,
independently-reviewable janitorial change, not bundled into this issue.

**Add a new, real (not vestigial) Sliding branch instead: `step %
slide_interval_ms == 0`, not `window_size_ms`.** This isn't a mechanical
copy of Tumbling's check with `S` swapped in for `W` — it's doing different
work. Tumbling's check (to the extent it ever mattered) was about bucket
alignment; Sliding's is what makes §4's "align the range-query start once,
not every step" simplification correct — it guarantees the grid-alignment
invariant is preserved across every step by construction, turning a
would-be silent per-step misalignment (bounded staleness, easy to miss) into
a loud rejection at the query boundary instead. `step % window_size_ms == 0`
would be **stricter than necessary** for Sliding: since `S | W` always holds,
`step % S == 0` alone is sufficient to keep every `W`-strided walk point on
the grid, and using `W` as the divisor would wrongly reject queries like
`step == S` (refresh every slide interval) — exactly the natural cadence
you'd want to allow.

Rename the parameter from `tumbling_window_ms` to `window_size_ms` at the
same time, since Tumbling's arm was never actually tumbling-specific logic,
just named as if it were — but that's the only change on the Tumbling side.

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

## Implementation approach: TDD, stacked PRs

This is correctness-critical query-serving logic (wrong-but-plausible
results, not crashes, are the failure mode — double-counted merges,
silently-dropped alignment). Test-first, and staged as separately-reviewable
stacked PRs rather than one large diff.

The key fact enabling this: `should_use_sliding_window()` is hardcoded
`false` (planner can't emit a Sliding config until #555 unblocks), so
**every piece of this stack is inert in production** — nothing a real query
can reach changes until the final PR lands. And `engine_factories.rs`'s test
harness registers configs via explicit `query_configs` entries, which
`promql.rs:1034` checks *before* falling back to capability matching — so
the execution-side tests can hand-register a `k>1` Sliding config and
exercise it directly, completely bypassing `window_compatible`. That's what
makes the ordering below safe: execution gets built and proven correct
*before* capability matching is relaxed to let real queries reach it.

1. **PR A — §5, `validate_range_query_params`.** Add the Sliding-only
   `step % slide_interval_ms == 0` branch; Tumbling's existing check is
   untouched. Independent of the rest of the stack.
2. **PR B — §2–4, execution unification (base of the stack).** Shared
   stride-`W` walk helper; Sliding's instant path reuses it instead of
   `merge_precomputed_outputs`; delete the old "skip merge, expecting 1
   precompute per key" branch; alignment fix (instant: floor-align once per
   call; range: floor-align only the start, relying on PR A's validation).
   Tested via hand-built `k=1,2,3,6` configs registered through
   `query_configs`, covering both instant and range-query callers of the
   shared walk helper, plus the "sum of `k` strided buckets, not all
   `S`-spaced buckets" regression assertion from the Test plan.
3. **PR C — §6, `cleanup.rs` (stacked on B).** Needs B's fetch shape final
   for the `slide_interval_ms`-based `read_count_threshold` formula. Own
   dedicated read-count accounting test (Test plan, last paragraph).
4. **PR D — §1, `capability_matching.rs` relaxation (stacked last, the
   "activation" PR).** Only lands once B/C are proven — this is what
   actually lets `find_compatible_aggregation` select a `k>1` Sliding
   config, so capability matching never promises more than execution can
   already deliver.

Write each PR's test subset first, confirm it fails against the current code
for the reason the design predicts (e.g. PR B's `k>1` cases should fail with
"expected 1 precompute per key," not some unrelated error), then implement.
Stop after each PR for review/commit rather than landing the stack in one
shot.

## Follow-up issues (not part of #557)

- #558 — silent floor-alignment staleness (both the existing
  ingestion-interval case and the new slide-interval case).
- #559 — legacy/unused code: `simple_map_store::legacy::{global, per_key}`
  (referenced only by a comparative benchmark, never constructed in
  production), the untested `CircularBuffer` cleanup policy, and (new, from
  this revision) `query_precomputed_output_exact` / `is_exact_query: true`
  becoming unused once §2's unification ships — add this as a fresh bullet
  to #559 during implementation.
