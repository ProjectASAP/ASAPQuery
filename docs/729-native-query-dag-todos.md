# Native query DAG: remaining work

This is the completion checklist for the native range-query DAG cutover.

## Required before removing the temporary comparison path

- [x] Re-run the Docker differential matrix from an isolated Compose lifecycle and record the result for every case.
  Each case now has an isolated Compose project, base timestamp, and host-port trio. The 2026-10-01 reports are in `/tmp/asapquery-differential-reports`.
- [x] Classify every remaining Prometheus differential mismatch using the legacy-versus-DAG E2E matrix.
  No DAG regression was observed: `aggregations-native-dag` and `quantiles` pass, and the legacy-versus-DAG E2E matrix passes for range leaves, sparse input, keyed count, and self-keyed topk.
  `temporal` retains the final-window `sum_over_time` mismatch (and its `+ 1` wrapper); `off-grid-rate` runs natively at the non-grid timestamps rather than falling back and returns partial-window rates; `aggregations` retains grouped-topk cardinality differences and unsupported rate-based topk/sum queries; and non-CI `olly-bench` retains its documented planner-coverage failures. These are native-query capability/parity gaps, not evidence that the DAG cutover changed the covered execution behavior.
- [ ] Add any missing public E2E characterization needed by a classified regression, then fix only DAG-caused regressions.

## Public error API

- [x] Complete the agreed API migration: `handle_query_promql` and `handle_range_query_promql` return `Result<Option<...>, QueryExecutionError>` (merged separately in #749).
- [x] Keep the error taxonomy explicit: `Ok(None)` means unsupported and fallback is allowed; `Err(QueryExecutionError)` means an accepted native execution failed and fallback is forbidden.

## Remove temporary cutover machinery

- [ ] After the differential matrix is accepted, delete `native_query_legacy_test_support`, `NativeRangeExecutionMode`, the legacy executor, and legacy-versus-DAG-only tests.
- [ ] Retain DAG-only E2E behavior tests, malformed-plan graph validation, local HTTP error/no-fallback coverage, and Docker compliance suites.

## Follow-up scope

- [ ] Implement complete native-query DAG execution in #743: represent arithmetic, constants, label matching, timestamp alignment, and fallback decisions in the plan rather than only executing each native query arm through a DAG.
