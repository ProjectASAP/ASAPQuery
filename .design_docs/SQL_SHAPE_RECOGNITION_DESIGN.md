# SQL Query-Shape Recognition and Maintenance — Design

For the proposed migration of this BGP workload to ASAPPlanner, including ownership,
IR gaps, implementation slices, and end-to-end acceptance criteria, see the
[ASAPPlanner integration plan](ASAPPLANNER_INTEGRATION_PLAN.md).

This document describes the `(ρ, τ, γ, eval, π)` decomposition the SQL path uses to decide
which analyst queries can be answered from a continuously maintained summary instead of a
full scan, what gets maintained for each recognized shape, and where that logic lives in the
codebase. It complements `CAPABILITY_MATCHING_DESIGN.md` (how a query at serve time is matched
against whatever got registered) by covering the layer above it: how a query gets *recognized*
and *registered* in the first place.

## The decomposition

BGPSketch never assumes an incoming query begins as a conventional SQL aggregate it can just
execute. It first works out what shape the query is, what a continuously maintained summary for
that shape would need to look like, and only then decides how to serve it:

```
ρ(Q) -> (Q', π)
D --τ--> D' --γ--> D''
A = π(eval(Q', D''))
```

- **ρ (recognize)** — classifies an incoming query `Q` against a fixed vocabulary of shapes,
  splitting it into `Q'` (the portion whose required statistics can be derived and maintained
  ahead of time) and `π` (the residual, result-level computation left for query time). ρ runs
  identically at planner time (deciding what to maintain) and query time (deciding how to serve
  a live query), from one shared AST-level pattern library in `sql_utilities` — so a query is
  never classified differently depending on which one is asking.
- **τ (derive)** — converts raw BGP records into the observations `Q'` actually needs. Three
  forms recur: a **stateless projection** (extracting the origin ASN from an AS path, bucketing
  a timestamp), a **fanout** (one record emitting several observations — every ASN in a path, or
  every adjacent AS-path edge), and a **stateful comparison** (checking the current observation
  against previously observed route state to detect a path change or measure an inter-arrival
  gap).
- **γ (maintain)** — folds τ's observations into maintained state `D''`. Summary-polymorphic:
  which sketch or exact structure it picks is determined by the statistic the query needs and
  whether it asked for an exact or approximate answer, not hardcoded per shape.
- **eval, π (answer)** — reads whatever `D''` holds and applies the residual computation `π`
  deferred from ρ's split. π is computation deliberately *not* run over every arriving record,
  because its input is already the small maintained result: ranking, ratios, percentages of a
  total, and set-membership joins all live here.

### A layer beneath ρ: spatial-filter enforceability

ρ's shape recognition ("is this a classic single aggregate, a multi-aggregate row, a correlated
subquery, ...") is a different question from a second one asked of every recognized query's WHERE
clause: **can each of its predicates actually be folded into the per-row ingest-time filter?**
That second question has its own dedicated family of recognizers —
`parse_spatial_predicate` and its per-shape helpers (`parse_eq`, `parse_range`, `parse_in`,
`parse_match_call`, `parse_has_split_member`, ...) in
`asap-common/dependencies/rs/sql_utilities/src/ast_matching/spatial_filter.rs` — answering a
narrower, lower-level question than ρ's shape classification. A query can be unambiguously
"classic single aggregate" at the ρ layer and still fail to register at all if one of its WHERE
predicates isn't one `parse_spatial_predicate` recognizes (`is_ingest_filter_fully_enforceable`),
in which case it falls back to exact/ClickHouse execution instead. This isn't a peer to ρ/τ/γ/π —
it has no τ, γ, or π of its own — it's a precondition inside ρ's own "can this be maintained at
all" decision, and it's the reason two queries with the identical aggregate shape can land in
different places depending on what's in their WHERE clause.
