# Native query DAG: remaining work

This is the completion checklist for the native range-query DAG cutover.

## Required before removing the temporary comparison path

- [x] Re-run the Docker differential matrix from an isolated Compose lifecycle and record the result for every case.
  Each case now has an isolated Compose project, base timestamp, and host-port trio. The 2026-10-01 reports are in `/tmp/asapquery-differential-reports`.
- [x] Re-run the Docker differential matrix against `main` and classify the result per suite.
  `quantiles` passes on both revisions. The existing `request-rate` temporal case passes on both; the PR's newly added final-window `sum_over_time` cases fail and characterize a pre-existing native-query parity gap. The new off-grid rate case fails because native execution does not fall back at non-grid timestamps. `olly-bench` retains its documented non-CI planner-coverage failures.
  `aggregations` has 14 new failures relative to `main`: grouped `topk` over a bare selector, `sum_over_time`, and `count_over_time` return too few series. The pre-existing rate-based `topk` and `sum` failures remain. The DAG-focused aggregation suite passes, so the current coverage does not reproduce the regression; add a legacy-versus-DAG E2E case for grouped topk before fixing it.
- [ ] Add any missing public E2E characterization needed by a classified regression, then fix only DAG-caused regressions.

## Public error API

- [x] Complete the agreed API migration: `handle_query_promql` and `handle_range_query_promql` return `Result<Option<...>, QueryExecutionError>` (merged separately in #749).
- [x] Keep the error taxonomy explicit: `Ok(None)` means unsupported and fallback is allowed; `Err(QueryExecutionError)` means an accepted native execution failed and fallback is forbidden.

## Remove temporary cutover machinery

- [ ] After the differential matrix is accepted, delete `native_query_legacy_test_support`, `NativeRangeExecutionMode`, the legacy executor, and legacy-versus-DAG-only tests.
- [ ] Retain DAG-only E2E behavior tests, malformed-plan graph validation, local HTTP error/no-fallback coverage, and Docker compliance suites.

## Follow-up scope

- [ ] Implement complete native-query DAG execution in #743: represent arithmetic, constants, label matching, timestamp alignment, and fallback decisions in the plan rather than only executing each native query arm through a DAG.
