# Native query DAG: remaining work

This is the completion checklist for the native range-query DAG cutover.

## Required before removing the temporary comparison path

- [x] Re-run the Docker differential matrix from an isolated Compose lifecycle and record the result for every case.
  Each case now has an isolated Compose project, base timestamp, and host-port trio. The 2026-10-01 reports are in `/tmp/asapquery-differential-reports`.
- [x] Re-run the Docker differential matrix against `main` and classify the result per suite.
  `quantiles` passes on both revisions. The existing `request-rate` temporal case passes on both; the PR's newly added final-window `sum_over_time` cases fail and characterize a pre-existing native-query parity gap. The new off-grid rate case fails because native execution does not fall back at non-grid timestamps. `olly-bench` retains its documented non-CI planner-coverage failures.
  The initial `aggregations` comparison had 14 new failures relative to `main`:
  grouped `topk` over a bare selector, `sum_over_time`, and `count_over_time`
  returned too few series. The pre-existing rate-based `topk` and `sum` failures
  remained. The DAG-focused aggregation suite passed, so the current coverage did
  not reproduce the regression; add a legacy-versus-DAG E2E case for grouped topk
  before fixing it.
- [x] Record the final isolated Docker matrix after the grouped-topk fix.
  The 2026-10-03 run is in `/tmp/asapquery-differential-reports-2026-10-03-final`:
  `native-dag-aggregations` passes 4/4 and `quantiles` passes 35/35.
  `aggregations` has 5/47 failures, all pre-existing rate-based `topk`/`sum`
  cases; the 14 grouped-topk regressions are resolved. `temporal` retains 2/3
  final-window `sum_over_time` mismatches, `off-grid-rate` retains its 1/1
  non-grid `rate` mismatch, and `olly-bench` retains its documented 22/25
  planner-coverage failures. `make run-all` therefore exits nonzero by design.
- [x] Add public E2E characterization for each classified DAG regression, then fix only DAG-caused regressions.
  Grouped `topk by (...)` now has legacy-versus-DAG coverage for a bare selector,
  `sum_over_time`, and `count_over_time`. The limiter ranks candidates within each
  timestamp/group partition, fixing the 14 grouped-topk Docker regressions.

## Public error API

- [x] Complete the agreed API migration: `handle_query_promql` and `handle_range_query_promql` return `Result<Option<...>, QueryExecutionError>` (merged separately in #749).
- [x] Keep the error taxonomy explicit: `Ok(None)` means unsupported and fallback is allowed; `Err(QueryExecutionError)` means an accepted native execution failed and fallback is forbidden.

## Retain temporary cutover machinery while staging DAG execution

- [x] Keep DAG as the production default.
  `NativeRangeExecutionMode::Dag` is the default, and all legacy modes are compiled
  only by the `native_query_legacy_test_support` feature.
- [ ] Retain `native_query_legacy_test_support`, `NativeRangeExecutionMode`, the legacy executor, and legacy-versus-DAG E2E tests until a future cutover decision.
- [x] Retain DAG-only E2E behavior tests, malformed-plan graph validation, local HTTP error/no-fallback coverage, and Docker compliance suites.

## Follow-up scope

- [ ] Implement complete native-query DAG execution in #743: represent arithmetic, constants, label matching, timestamp alignment, and fallback decisions in the plan rather than only executing each native query arm through a DAG.
