# Process conventions

- **Keep PRs small and single-purpose.** Split a PR that mixes multiple
  objectives (e.g. a rename + unrelated behavior change) into separate PRs so
  the repo stays runnable and reviewable at every merge point.
- **PR titles follow Conventional Commits**: `<type>(<scope>): <summary>`,
  e.g. `fix(precompute): ...`, `feat(query-engine): ...`,
  `refactor(asap-tools): ...`, `perf(precompute): ...`. Match the existing
  scope names used in this repo's PR history (`precompute`, `query-engine`,
  `planner`, `asap-tools`, `sql-parser`, `deps`, etc.) rather than inventing
  new ones.

# Code design and test coverage

See `.agents/skills/code-design-review.md` and
`.agents/skills/test-coverage-review.md` — apply them while writing code,
not just when asked to review.
