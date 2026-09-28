# Native query DAG: remaining work

This is the completion checklist for the native range-query DAG cutover.

## Required before removing the temporary comparison path

- [ ] Re-run the Docker differential matrix from an isolated Compose lifecycle and record the result for every case.
  The current runner uses the fixed Compose project name `asapquery-differential`; interrupted or overlapping runs can race `up`/`down` and leave a Prometheus instance that rejects a repeat fixture as out-of-order. Make each invocation isolated (or serialize it), use a unique base timestamp, and retain every JSON report.
- [ ] Classify every remaining Prometheus differential mismatch as either a pre-existing legacy parity gap or a DAG regression. The legacy-versus-DAG E2E matrix is the evidence for that classification.
- [ ] Add any missing public E2E characterization needed by a classified regression, then fix only DAG-caused regressions.

## Public error API

- [ ] Complete the agreed API migration: make `handle_query_promql` and `handle_range_query_promql` return `Result<Option<...>, QueryExecutionError>` rather than flattening native failures to `None`.
  `try_handle_*` already supplies this contract and the HTTP server uses it. Migrate direct callers and their capability-miss assertions in one focused commit, then remove the compatibility wrappers.
- [ ] Keep the error taxonomy explicit: `Ok(None)` means unsupported and fallback is allowed; `Err(QueryExecutionError)` means an accepted native execution failed and fallback is forbidden.

## Remove temporary cutover machinery

- [ ] After the differential matrix is accepted, delete `native_query_legacy_test_support`, `NativeRangeExecutionMode`, the legacy executor, and legacy-versus-DAG-only tests.
- [ ] Retain DAG-only E2E behavior tests, malformed-plan graph validation, local HTTP error/no-fallback coverage, and Docker compliance suites.

## Follow-up scope

- [ ] Implement complete native-query DAG execution in #743: represent arithmetic, constants, label matching, timestamp alignment, and fallback decisions in the plan rather than only executing each native query arm through a DAG.
