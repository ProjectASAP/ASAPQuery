## AST Matching: Python vs Rust — detailed comparison

Date: 2025-08-26

Purpose: dump a full, function-level and class-level comparison of the `ast_matching` modules in
Python (dependencies/py/promql_utilities/promql_utilities/ast_matching) and Rust
(dependencies/rs/promql_utilities/src/ast_matching). Each discrepancy or change is tagged as
"MUST HAVE" (correctness-related) or "GOOD TO HAVE" (portability/ergonomics/perf).

---

Files compared
- Python
  - PromQLPattern.py
  - PromQLPatternBuilder.py
- Rust
  - promql_pattern.rs
  - promql_pattern_builder.rs
  - promql_pattern_factory.rs

Note: This file assumes the versions present in the repo as of the timestamp above. The Rust
`promql_pattern.rs` file already includes `SubqueryExpr` handling (line ranges present in the
attachment).

### High-level summary
- Both sides implement: pattern builder -> pattern object -> matcher that walks a parsed PromQL AST
  and optionally collects tokens.
- Major conceptual parity but concrete representation, naming, and normalization differences exist.

---

## Class/struct level mapping

- Python: `PromQLPattern` (class)
  - Holds pattern dict, exposes `matches(node)` -> `MatchResult(matches: bool, tokens: Dict)`.
  - Key internals: `_node_to_dict`, `_matches_recursive`.

- Python: `PromQLPatternBuilder` (static-method-only dataclass)
  - Produces Python-native pattern dicts (or `None` for `any()` wildcard).

- Rust: `PromQLPattern` (struct)
  - Holds `ast_pattern: HashMap<String, Value>`, typed token model, `expected_pattern_type`.
  - Exposes `matches(&Expr)` -> `PromQLMatchResult` (typed tokens).
  - Internals: `matches_recursive`, typed `match_*` helpers.

- Rust: `PromQLPatternBuilder` (impl)
  - Produces `HashMap<String, serde_json::Value>` patterns.

- Rust: typed token structs (`TokenData`, `MetricToken`, `FunctionToken`, ...).

Discrepancy tag: class/struct correspondence — GOOD TO HAVE. It's fine for Rust to use typed tokens, but if cross-language token portability is desired, aligning JSON shapes is recommended.

---

## Function-by-function comparison (Python -> Rust)

Legend: MUST HAVE = correctness/security-related; GOOD TO HAVE = portability/ergonomics/perf.

1) Builder: any()
  - Python: `PromQLPatternBuilder.any()` returns `None`. Python matcher treats `pattern is None` as wildcard -> matches anything.
  - Rust: `PromQLPatternBuilder::any()` returns an empty `HashMap<String, Value>` (i.e., `{}`). `matches_recursive` requires a `type` string and returns false if missing; an empty map does NOT act as wildcard.
  - Discrepancy: semantics differ and lead to non-matching behavior in Rust when user expects wildcard.
  - Tag: MUST HAVE (pattern wildcard semantics affect correctness of many patterns).
  - Suggested fixes (MUST HAVE): make Rust `matches_recursive` treat empty pattern as wildcard (e.g., `if pattern.is_empty() { return true; }`) or change `any()` to return a sentinel `Value::Null` and handle it.

2) Builder: binary_op / BinaryExpr naming
  - Python builder returns `type: "BinaryOpExpr"` (PromQLPatternBuilder.binary_op).
  - Python `_node_to_dict` for actual AST Binary returns `type: "BinaryExpr"`.
  - Therefore patterns built by Python builder will not match binary AST nodes; token collection for binary ops (which checks "BinaryOpExpr") will also never trigger.
  - Rust builder and matcher consistently use `"BinaryExpr"`.
  - Discrepancy: naming typo/inconsistency in Python.
  - Tag: MUST HAVE (causes incorrect matching of binary expressions).
  - Suggested fix (MUST HAVE): change Python builder to produce `"BinaryExpr"` (or change `_node_to_dict` to produce `"BinaryOpExpr"`, but updating builder is minimal).

3) Builder: function (`function` / `Call` / `func` field shape)
  - Python builder sets `func` to `{"type":"Function","name": [ ... ]}` (dict with `name` list).
  - Rust builder sets `func` to `Value::Array([ func_object ])` (an array containing the func object). Rust matcher expects this array-wrapped shape.
  - Both matchers work with their own builders but cross-language serialized patterns will differ.
  - Discrepancy: pattern JSON shape mismatch; porting patterns across languages will fail unless normalized.
  - Tag: GOOD TO HAVE (affects portability, not correctness inside a single language runtime).
  - Suggested fix (GOOD TO HAVE): normalize representations to a single shape (prefer object rather than array) or make matchers accept both shapes.

4) Function args matching and collection
  - Python `_matches_recursive` checks `args` as list; requires same length and recurses per-item; `_collect_args_as` stores `tokens[collect_args_as] = node_dict['args']` (raw arg ASTs) and `_collect_as` stores args raw.
  - Rust `match_function_call` checks arg count and recurses. For `_collect_as` Rust stores args as `format!("{:?}", arg)` (stringified) and for `_collect_args_as` does the same. Earlier Rust code used placeholders for args in some versions; current code stringifies args (improvement).
  - Discrepancy: token shape differs (Python raw AST vs Rust stringified args).
  - Tag: GOOD TO HAVE (token shape matters for portability and downstream consumers).

5) Aggregate / AggregateExpr
  - Python builder stores `op` possibly as list or string (builder converts to list), `modifier` field as `by`/`without` stored under `modifier` key.
  - Rust builder stores `op` as array, stores `by` and `without` separately in the pattern JSON. Rust `match_aggregation` checks membership and recurses into `expr`. Rust sets `param` to `agg.param.as_ref().map(|p| format!("{:?}", p))` while Python earlier stored `param` more directly.
  - Discrepancy: minor shape/field naming differences for modifiers (`modifier` vs `by`/`without`) and param normalization.
  - Tag: GOOD TO HAVE (affects portability; correctness preserved if each side consumes its own builder).
  - Suggested fix (GOOD TO HAVE): agree on `by`/`without` keys or accept both forms in matchers.

6) MatrixSelector / range vector
  - Python `_node_to_dict` exposes `range` verbatim from parser; builder stores `range` string.
  - Rust `match_matrix_selector` converts `ms.range` (std::time::Duration) to `chrono::Duration` in tokens and stores `offset` from `ms.vs.offset`. Rust token normalizes duration; Python currently leaves raw parser value.
  - Discrepancy: duration representation difference and `offset` location naming.
  - Tag: GOOD TO HAVE (normalization difference — important for portability but not strictly correctness inside runtime).

7) NumberLiteral numeric comparison
  - Python compares pattern value vs node value using equality (exact) in general code; there is no explicit epsilon handling unless the pattern_value is TokenType then handled specially. (Note: Python code uses TokenType branch for token comparisons; numeric equality uses Python's `==` semantics on floats.)
  - Rust compares floats using `if (num.val - expected_f64).abs() > f64::EPSILON { return false; }` i.e., epsilon-based equivalence.
  - Discrepancy: Python exact vs Rust EPSILON tolerance.
  - Tag: MUST HAVE (numeric equality semantics can cause correctness surprises across languages).
  - Suggested fix (MUST HAVE): pick one policy (recommended: epsilon compare) and apply to Python; or clearly document language-specific rule.

8) SubqueryExpr
  - Python: builder + `_node_to_dict` include `SubqueryExpr` support (range, step, offset) and `_matches_recursive` handles nested dicts for subquery patterns.
  - Rust: the current `promql_pattern.rs` includes `match_subquery` and `SubqueryToken` — so Rust supports subquery matching now.
  - Discrepancy: earlier there was a gap; currently parity exists in repo (good).
  - Tag: GOOD TO HAVE (presence is correctness-related only if you rely on subquery patterns; treat as MUST HAVE if you need subquery correctness). For correctness: mark MUST HAVE if you plan to support subquery-based pattern matching; otherwise GOOD TO HAVE.

9) AtModifier (`@` modifier) handling
  - Python: stores `at` raw in `node_dict` and in tokens (no conversion) — flexible.
  - Rust: converts `AtModifier::At(t)` to seconds since UNIX_EPOCH and panics on `AtModifier::Start` or `AtModifier::End` (explicit panics). That means Rust can panic on certain AST values.
  - Discrepancy: Rust panics on `Start/End`, Python will simply put the value in token.
  - Tag: MUST HAVE (panic on parser output is correctness/robustness issue).
  - Suggested fix (MUST HAVE): make Rust handle `Start`/`End` gracefully (either encode them as sentinel strings or treat as non-matching rather than panic). Convert time to a normalized representation but don't panic.

10) Pattern strictness & missing-type handling
  - Python: `if pattern is None: return True` (wildcard) and when a key exists with value `None` the matcher treats that as wildcard for that field. Python is permissive.
  - Rust: `matches_recursive` requires `pattern.get("type")` to be a `Value::String` and returns false otherwise. Nested checks require `Value::Object` for nested patterns. Rust is strict about pattern shape.
  - Discrepancy: permissiveness vs strictness causes different failure modes and different ways of expressing wildcards in nested positions.
  - Tag: MUST HAVE (expressing patterns consistently across languages is essential for correctness of pattern design).
  - Suggested fix (MUST HAVE): either document the strict JSON contract for Rust builders or make Rust accept `Value::Null` or empty maps as wildcards; conversely, validate Python patterns to guarantee shape if you prefer Rust's strictness.

11) Token shapes and type normalization
  - Python tokens: lightweight dicts; include `ast` fields that carry parser nodes. Values are not normalized (e.g., `at` raw).
  - Rust tokens: typed structs, normalized fields (`at_modifier: Option<u64>`, `RangeToken.range: chrono::Duration`) and some stringification via `format!("{:?}", ...)` for parameters/args when necessary.
  - Discrepancy: serialization and field names differ; cross-language consumers will need mapping.
  - Tag: GOOD TO HAVE (portability/contract-related). If consumers rely on specific token fields for correctness, escalate to MUST HAVE.

12) Utility / Factory functions
  - Rust includes `PromQLPatternFactory` with prebuilt patterns for OnlyTemporal / OnlySpatial patterns.
  - Python lacks the same factory file (you can emulate using `PromQLPatternBuilder`).
  - Discrepancy: convenience API mismatch.
  - Tag: GOOD TO HAVE.

---

## Per-function diffs (concise) — where to look

- PromQLPattern.__init__ (py)  vs PromQLPattern::new (rs)
  - Both store the pattern. Python stores pattern as an arbitrary dict possibly `None`; Rust requires `HashMap<String, Value>` and an explicit `expected_pattern_type`.
  - Tag: GOOD TO HAVE.

- PromQLPattern.matches(node) (py) vs PromQLPattern::matches(&Expr) (rs)
  - Both call recursive matching and return a pair of (matches, tokens). Python returns `MatchResult(matches, tokens)` where tokens are a plain dict; Rust returns typed `PromQLMatchResult`.
  - Tag: GOOD TO HAVE.

- _node_to_dict (py) vs explicit typed match arms (rs)
  - Python converts parser nodes to dict forms used by recursive matcher.
  - Rust uses pattern_type & node enum and calls typed `match_*` helpers directly. Rust does not use a transient dict representation.
  - Tag: GOOD TO HAVE (architectural difference; both valid).

- _matches_recursive (py) vs matches_recursive (rs)
  - Python: flexible dict-driven matching with list/dict/TokenType handlers and `_collect_as` logic.
  - Rust: strict: pattern must include `type` string; then match arms call typed helpers.
  - Key correctness mismatch: Python supports `pattern is None` wildcard; Rust requires `type` key.
  - Tag: MUST HAVE for wildcard semantics.

- match_metric_selector (rs) vs VectorSelector handling in Python
  - Both check `name` membership; both can collect labels. Rust extracts equality-match labels only (`MatchOp::Equal`) and builds typed `MetricToken` with `at_modifier` normalized to seconds or panics on Start/End.
  - Python exposes `labels` as `matchers` and leaves `at` raw.
  - Tag: MUST HAVE for panic behavior on `@` variants; GOOD TO HAVE for normalization parity.

- match_function_call (rs) vs Call handling in Python
  - Similar high-level behavior (name membership, arg count, recursive matching). Differences in tokenization and `func` pattern shape.
  - Tag: GOOD TO HAVE.

- match_aggregation (rs) vs AggregateExpr handling in Python
  - Both check `op` membership and recurse into `expr`. Rust builds typed `AggregationToken` and stringifies `param`; Python stores param in token dict.
  - Tag: GOOD TO HAVE.

- match_matrix_selector (rs) vs MatrixSelector handling in Python
  - Both support vector_selector nested matching and token collection. Rust normalizes durations into chrono::Duration and extracts `offset`; Python leaves raw range and step/offset fields in node dict.
  - Tag: GOOD TO HAVE.

- match_binary_operation (rs) vs BinaryExpr handling in Python
  - Rust expects pattern type `BinaryExpr` and checks `op`, left/right recursion, collects token.
  - Python builder mismatch (BinaryOpExpr vs BinaryExpr) is a MUST HAVE fix.

- match_number_literal (rs) vs NumberLiteral handling in Python
  - Rust uses epsilon comparison; Python uses direct equality (unless pattern is None). Make numeric equality policy consistent (MUST HAVE).

- match_subquery (rs) vs Subquery handling in Python
  - Current repo: Rust includes `match_subquery` and `SubqueryToken` (parity achieved). If you rely on subquery correctness, tests must validate behavior.
  - Tag: GOOD TO HAVE / MUST HAVE depending on usage.

---

## Concrete list of discrepancies & tags (compact)

1. any() wildcard semantics — MUST HAVE
2. Python binary builder `type` naming (`BinaryOpExpr` vs `BinaryExpr`) — MUST HAVE
3. Numeric equality epsilon (Py exact vs Rust eps) — MUST HAVE
4. Rust panics on `AtModifier::Start` / `End` — MUST HAVE
5. `func` shape (object vs array-wrapped object) — GOOD TO HAVE
6. Token shapes and normalization (raw AST vs typed/normalized representation) — GOOD TO HAVE
7. Aggregation modifier naming (`modifier` vs `by`/`without`) — GOOD TO HAVE
8. Matrix range and offset normalization differences — GOOD TO HAVE
9. Subquery support parity (now present in Rust) — GOOD TO HAVE (escalate to MUST HAVE if subqueries are required)
10. Presence of `PromQLPatternFactory` in Rust but not Python — GOOD TO HAVE

---

## Minimal recommended fixes (priority order)
1. Fix Python builder `binary_op` to set `type: "BinaryExpr"` (MUST HAVE)
2. Make Rust `matches_recursive` treat empty `pattern` (or `Value::Null`) as wildcard, or change `PromQLPatternBuilder::any()` to return `Value::Null` and recognize it (MUST HAVE)
3. Unify numeric equality policy (use epsilon both sides) (MUST HAVE)
4. Prevent Rust panics on `AtModifier::Start`/`End`: encode them as sentinel strings (e.g., "start"/"end") or treat as non-match (MUST HAVE)
5. Add optional tolerant parsing for `func` pattern shapes (accept both array-wrapped and object forms) (GOOD TO HAVE)
6. Add small JSON-token serializer in Python matching Rust token schema, or vice versa, for portability (GOOD TO HAVE)

---

## Must-have tests to add (short list)
- `test_any_wildcard_matches_any_node` (Py + Rust)
- `test_binary_expr_matching` (detect Python builder bug)
- `test_numeric_equality_policy` (float epsilon consistency)
- `test_at_modifier_no_panic` (Rust must not panic for `Start`/`End`)
- `test_token_contracts` (verify presence and basic types of token fields)

## Good-to-have tests
- cross-language serialized pattern roundtrip tests
- token schema parity tests (JSON serialize Rust tokens, compare to Python tokens)
- factory pattern equivalence (Rust `PromQLPatternFactory` vs composed Python builder)

---

If you'd like, I can now:
- apply the MUST HAVE code fixes (small, targeted edits) and run the unit tests; or
- add the MUST HAVE tests first to surface current failures.

Tell me which action to run next and I'll edit files + run tests.
