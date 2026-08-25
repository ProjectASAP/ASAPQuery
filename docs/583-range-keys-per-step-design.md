# #583 range-query per-step keys — grilling session recap

Design discussion for the fix to [#583](https://github.com/ProjectASAP/ASAPQuery/issues/583)
("Range query key expansion uses one snapshot instead of per-step keys").
No fix code has been written yet — this session was scoped to (1) confirming
understanding of the bug, (2) working out the fix design question-by-question,
and (3) building out a RED test suite that pins every failure mode discussed.
Recorded here so the eventual implementation turn doesn't have to re-derive
any of this.

## Bug recap

`execute_range_query_pipeline` (`asap-query-engine/src/engines/simple_engine/mod.rs`)
fetches and merges `keys_query` **once**, anchored at the range's `end`, and
reuses that single snapshot for every output timestamp in the range. Two
distinct problems fall out of this:

1. **Stale snapshot reused across steps** — a key that starts or stops
   existing partway through the range gets phantom samples before it existed,
   or is silently dropped even at timestamps where it legitimately existed.
   Confirmed to also affect the binary-expr arm path
   (`handle_binary_expr_range_promql` → `build_arm_range_context` →
   `finish_range_context`), which the issue only flagged as "worth checking."
2. **`keys_query`'s window is instant-anchored, not range-aware** —
   `create_keys_query_params` computes the keys window purely from
   `end_timestamp`. `finish_range_context` widens `values_query` to
   `[start-lookback, end]` but clones `keys_query` unchanged. For
   `SetAggregator` ("latest window only") this drops labels that existed
   earlier in the range but fell out of the final instant's window.
   `DeltaSetAggregator`'s `[0, end]` window happens to already be correct
   (it's a full replay-from-start aggregator), so this half of the bug is
   `SetAggregator`-specific.

Both collapse to one fix: fetch/merge keys **per output step**, scoped to
that step's own window, not once at `end`.

## Q&A

### Q1 — architecture: per-step store queries, or one fetch + per-step in-memory merge?

**Answer: one fetch + per-step in-memory merge.** The values side of
`execute_range_query_pipeline` already solves this exact problem: one
`execute_store_query` fetches all raw buckets across the whole range once,
then the `current_time` loop does an in-memory windowed merge per step
(`bucket_map` lookup + `create_window_merger`). The keys side should mirror
this instead of issuing N separate store queries (one per output timestamp).

### Q2 — how does `finish_range_context` compute each side's widened window?

**Answer: generalize the existing `values_query` widening formula to
`keys_query`, with zero `AggregationType` branching in the fix.**

`finish_range_context` already does, for values:
```rust
let lookback_ms = base_context.store_plan.values_query.end_timestamp
    - base_context.store_plan.values_query.start_timestamp;
extended_store_plan.values_query.start_timestamp = start_ms.saturating_sub(lookback_ms);
extended_store_plan.values_query.end_timestamp = end_ms;
```
It doesn't know or care *why* that window is that width. The same trick
works for `keys_query` and happens to handle both aggregation types
correctly for free:

- `create_keys_query_params` already computed the instant keys window:
  `[end-window_size, end]` for `SetAggregator`, `[0, end]` for
  `DeltaSetAggregator`.
- `keys_lookback_ms = keys_query.end - keys_query.start` → `window_size` for
  `SetAggregator`, `end_ms` (the range's end) for `DeltaSetAggregator`.
- Widen the same way: `start_ms.saturating_sub(keys_lookback_ms)`. For
  `SetAggregator` that's a normal sliding window. For `DeltaSetAggregator`,
  since `keys_lookback_ms == end_ms` and the per-step loop invariant is
  `current_time <= end_ms`, `current_time.saturating_sub(keys_lookback_ms)`
  saturates to `0` at **every** step — "replay from the beginning," for free,
  provably (not by luck): the `while current_time <= end_ms` loop condition
  guarantees `current_time <= end_ms` before every iteration body runs.

This also means "always merge from t=0 for DeltaSetAggregator" (a specific
follow-up question raised) is already guaranteed by this derivation — no
separate `if key_agg_type == DeltaSetAggregator { start = 0 }` branch is
needed anywhere.

### Q3 — recompute-from-scratch per step, or incremental carry-forward?

**Answer: recompute-from-scratch first; optimize later if measured.**

For `DeltaSetAggregator`, each step's window is `[0, t]`, growing every
step. Recomputing fresh each step (mirroring the existing values loop
exactly) costs `O(N²)` bucket-merges total across `N` output steps (step 1
processes 1 bucket, step 2 processes 2, ..., step N processes N). An
incremental version — carry a running merged accumulator forward across the
loop, only folding in new buckets since the last step — gives the same
result (verified, see below) in `O(N)` total, but requires restructuring the
loop to persist per-key-group merger state across iterations.

Decision: ship recompute-from-scratch (least new code, easiest to verify,
matches the existing values-loop pattern exactly). `SetAggregator`'s window
is bounded by `window_size` regardless of range length, so it never has this
blowup — only `DeltaSetAggregator` does. Leave a comment flagging the
`O(N²)` replay cost as a known ceiling; revisit only if it's an actual
measured problem.

**Correctness check performed before trusting this:** `NaiveMerger.merge_all()`
folds buckets **pairwise, left-to-right** (`buckets[0].merge_with(buckets[1])`,
then that result `.merge_with(buckets[2])`, ...) rather than passing the
whole slice to `DeltaSetAggregatorAccumulator::merge_accumulators` in one
flat N-way call. This distinction matters: `merge_accumulators`'s "key in
both added and removed → cancel both" conflict logic is symmetric and
order-blind when given the whole batch at once, but `NaiveMerger`'s
sequential fold means each binary step only ever resolves conflicts between
"the running state so far" and "the next bucket" — which correctly threads
chronological order through, including for a key that toggles 3+ times
within one merge window. Hand-traced against a 5-step add/remove/add/remove/add
sequence and confirmed correct at every intermediate step, not just the
final one. Pinned as an explicit, readable regression test (see below) since
this is easy to miss if `WindowMerger`'s implementation ever changes.

### Q4 — does the keys bucket_map walk need its own step increment?

**Answer: yes.** The per-step window-building loop scans `bucket_map` in
increments of the tumbling bucket width (`t += tumbling_window_ms` in the
existing values loop). That `tumbling_window_ms` is the **value**
aggregation's bucket width. The **key** aggregation (`aggregation_id_for_key`)
can have a different `window_size_ms`. The fix needs a separate
`keys_tumbling_window_ms`, fetched from the key aggregation's own config
(the same way `create_keys_query_params` already does internally), used to
walk the keys `bucket_map` — reusing the value side's width would silently
skip every keys bucket whose start isn't a multiple of that width.

### Q5 — does `do_merge` still matter for keys once per-step merging is in place?

**Answer: no, drop it for the range path.** Today `fetch_and_merge_keys`
takes a `do_merge` flag derived from the **value** aggregation's window
(`create_store_query_plan`: `do_merge = range_ms > value_aggregation.window_size_ms`)
and reuses it wholesale for the keys merge. Once keys mirror the values loop
(Q1), `do_merge` becomes moot the same way it already is for values (the
values `current_time` loop never consults it — it always does the
bucket-map-window-merge dance regardless of window count). The range
pipeline's keys fetch should become a raw `execute_store_query(keys_params)`
call (no merge), replacing `fetch_and_merge_keys`, for the range path only —
the instant-query path is unaffected.

Related: [#581](https://github.com/ProjectASAP/ASAPQuery/issues/581)
"Unify PromQL instant and range query fetch/merge paths" independently names
this exact gap (range path's "fresh `WindowMerger` re-merged from scratch
per step" vs instant's `do_merge` short-circuit) as one of three drift bugs
(#570, #582, #587) between the two paths. This fix is a partial step toward
#581, not a full closer.

### Q6 — a group with keys but no value data anywhere: fatal, or skip?

**Answer: non-fatal skip + loud warning, per (step, group).** Today,
`execute_range_query_pipeline` hard-fails the **entire** range query if any
group resolved from `merged_keys` has no matching entry in `all_data`:
`all_data.get(group_key).ok_or_else(|| "No value for key: ...")?`. This is a
one-time check against a single global `groups` list today. Once key
expansion is per-step, a group having zero value data isn't an anomaly — a
key can legitimately exist per the keys aggregation before/after the value
aggregation ever has data for it (e.g. ingestion boundaries). This should
downgrade from hard-error (poisoning every other group's results in the same
query) to skip-this-group + warn loudly.

### Q7 — multiple independent groups (real `grouping_labels`): does per-step scoping leak across groups?

Explored via a concrete example (region=us gains host-b mid-range,
region=eu's host-x never changes — does host-b leak into region=eu's
output?). **Finding: not a real distinct risk in today's code.** Store
query results already come back partitioned by `group_key`
(`MergedOutputsMap = HashMap<Option<KeyByLabelValues>, ...>`), and
`merge_precomputed_outputs` merges *within* each group_key's own buckets,
not across groups. The RED tests written for this (see below) confirmed it:
failures were always "this group's own key is phantom-early" (Bug 1), never
actual cross-group bleed. Kept as regression guards for once Bug 1 is fixed,
not because they proved a second bug.

A related candidate — "a group that only appears partway through the
range" — was traced through by hand and found to **not** actually be RED
against today's code as originally described (a `DeltaSetAggregator`'s
`keys_query` already spans `[0, end]` today, so a brand-new group is picked
up correctly by coincidence when its only key never changes). It was
replaced with the Q6 scenario instead (a group with keys but zero value
data, which **does** genuinely hard-fail today).

### Q8 — a toggle within a single output step's window, at the full-pipeline level

Raised as "no existing test has two key-delta buckets landing inside the
same step's window" — **this claim was checked and found wrong**: since
`DeltaSetAggregator`'s window always starts at `0` (Q2), the oscillating
5-window test's own `t=2000` checkpoint already merges 2 toggle-buckets
(`[0,1000)` add, `[1000,2000)` remove) within one window. No new test needed
for that framing.

Re-checking turned up a real, different gap: every `SetAggregator` test uses
`window_size_ms == bucket_width_ms`, so each window only ever contains
**exactly one** bucket. `DeltaSetAggregator` gets multi-bucket-per-window
coverage "for free" via its always-widened `[0,t]` window; `SetAggregator`'s
bounded sliding window has never been exercised merging more than one
bucket. Added `range_query_set_aggregator_merges_multiple_buckets_within_one_window`:
two `SetAggregator` buckets colliding at the same `(start=0,end=1000)` (same
construction as `range_query_sliding_window_merges_both_buckets`, applied to
keys instead of values) must union into `{host-a, host-b}`, guarding against
a per-step `bucket_map` implementation that does `.insert()` instead of
`.entry().or_default().push()` for the keys side and silently drops one of
the colliding buckets.

## RED test inventory

All in `asap-query-engine/src/tests/native_range_query_tests.rs` unless noted.
16 tests total: 5 green (regression guards / already-correct behavior), 11 RED
(pin the fix's target behavior).

| Test | Pins |
|---|---|
| `range_query_dual_population_key_appearing_midrange_has_no_phantom_earlier_sample` | Bug 1, `DeltaSetAggregator`, plain range path |
| `range_query_set_aggregator_earlier_key_not_silently_dropped` | Bug 2, `SetAggregator`, plain range path |
| `range_query_binary_expr_arm_key_appearing_midrange_has_no_phantom_earlier_sample` | Bug 1 through `build_arm_range_context` (binary-expr scalar arm) |
| `range_query_binary_expr_arm_set_aggregator_earlier_key_not_silently_dropped` | Bug 2 through `build_arm_range_context` |
| `range_query_delta_set_aggregator_oscillating_add_remove_across_five_windows` | Multi-toggle replay correctness (add/remove ×5), asserted at every step |
| `range_query_delta_set_aggregator_key_change_on_middle_step_not_just_boundary` | 3-step range, change lands on the interior step, not a boundary |
| `range_query_delta_set_aggregator_key_bucket_width_differs_from_value_bucket_width` | Q4 — keys bucket_map must step by the key aggregation's own width |
| `range_query_dual_population_per_step_key_change_does_not_leak_across_groups` | Q7 — one group changes, one doesn't |
| `range_query_dual_population_simultaneous_cross_group_adds_stay_isolated` | Q7 — both groups change at the same timestamp (sharper signature) |
| `range_query_dual_population_group_with_no_value_data_is_skipped_not_fatal` | Q6 — orphaned group must not poison the whole query |
| `range_query_set_aggregator_merges_multiple_buckets_within_one_window` | Q8 — `SetAggregator` must union, not drop, colliding same-timestamp buckets within one window |
| `naive_merger_sequential_fold_replays_delta_set_toggles_at_every_window` (`window_merger.rs`, **GREEN**) | Explicit, readable pin of the NaiveMerger-sequential-fold requirement Q3 depends on; contrasts against a flat `merge_accumulators` call to make the distinction undeniable |

`assert_all_at` (test-module helper) collects every mismatched
`(labels, timestamp, expected)` case into one panic message instead of
stopping at the first failing `assert!`, per explicit request — applied to
all multi-assertion tests in the file.

`create_range_engine_dual_input_with_windows` (thin superset of
`create_range_engine_dual_input`) lets tests set different bucket widths for
the value vs. key aggregation configs, needed for the Q4 test; existing call
sites untouched.

## Status

The fix landed (PR [#595](https://github.com/ProjectASAP/ASAPQuery/pull/595),
draft), staged as planned:

- Stage 1 — widened `keys_query`'s window in `finish_range_context`
  (`keys_lookback_ms`/`keys_tumbling_window_ms` on
  `RangeQueryExecutionContext`), behavior-inert on its own.
- Stage 2 — the actual per-step keys merge in
  `execute_range_query_pipeline` (raw fetch, `KeysSource` enum, per-step
  windowed merge, Q6's non-fatal group skip).
- Stage 3 — confirmed the binary-expr arm path needed no separate code
  change, as predicted.
- Stage 4 — full regression pass, 0 failures.

All 16 `native_range_query_tests` pass; full workspace suite (`cargo test
--workspace`) passes with 0 failures.

Follow-up refactor (`widen_query_window`, dedupping the values/keys window
formula) and a broader duplication survey against the instant-query path
were also done post-fix. Two items came out of that survey as genuine
design decisions rather than mechanical refactors, and were filed as
separate issues rather than folded into #583:

- [#596](https://github.com/ProjectASAP/ASAPQuery/issues/596) — range
  queries never got the CMS/KLL batch-merge fast path the instant path has
  (`NaiveMerger` only does the sequential fallback); the two fallback
  implementations also differ in error-handling policy, so unifying them
  isn't free.
- [#597](https://github.com/ProjectASAP/ASAPQuery/issues/597) — #583's Q6
  (skip a group with keys-but-no-value-data instead of hard-failing) was
  only applied to the range path; the instant path
  (`collect_results_separate_keys`) still hard-fails on the identical case.

## Explicitly not done here

- Incremental carry-forward merging for `DeltaSetAggregator` (Q3) — deferred
  until proven necessary.
- Broader unification with #581 — out of scope for #583, but this fix moves
  in that direction.
