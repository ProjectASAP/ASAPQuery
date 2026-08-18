---
name: test-coverage-review
description: >
  ASAPQuery-specific test coverage checklist: regression/negative-path
  coverage and preserving why-comments on regression tests. Use whenever adding or
  changing behavior in ASAPQuery. Check this before considering a feature or fix done, not only
  when asked to review tests.
---

# ASAPQuery test coverage checklist

## 1. Cover the negative/failure paths, not just the happy path

New behavior — especially error branches, merge/fallback logic, and
edge-case query syntax — needs a dedicated test for what happens when it
fails, not just a test that the normal case works.

**Why:** Failure modes are most likely to surface in production, and tests that only check happy paths won't catch these.
A missing correctness/regression test also means there's no
guard against a future refactor quietly changing behavior (e.g. a new store
implementation silently diverging from the one it replaces).

Checklist when adding a feature or fix:
- New merge/combine/accumulator logic → test the error path, not just the
  success path.
- A new query-syntax feature (e.g. new PromQL/SQL clause) → test that
  existing related behavior (e.g. `topk`) still works alongside it.
- Replacing or refactoring a store/backend → add a correctness test proving
  it produces the same output as what it replaces, not just a benchmark.

## 2. Keep the explanatory comment on regression tests

If a test exists because of a specific past bug, the comment explaining
*that bug* is part of the test, not decoration. Don't strip it during
cleanup or refactor.

**Why:** a regression test with no comment just looks like an arbitrary edge
case to the next reader — the comment is what makes it legible why this
input is tested at all, and prevents someone "simplifying" the test away
later.
