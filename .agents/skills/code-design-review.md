# ASAPQuery code design checklist

Use whenever writing or editing Rust (asap-query-engine, asap-planner-rs,
asap_sketchlib) or Python (asap-tools) code in this repo, not just when
asked to "review" — apply it while implementing, refactoring, or fixing
bugs, and consult it again before finishing any PR-sized change here.

Apply these when writing code, not just at review time. Catching these before the PR exists is cheaper than fixing them after.

## 1. Fail loud

Don't let a failure path quietly resolve to a default, an empty value, or a
swallowed error. If something goes wrong, panic, return `Result`/`Error`, or
raise — never fall through to a value that looks like success.

**Why:** a hardcoded fallback or `unwrap_or_default()` on a failure path
turns a real bug into silent wrong output. It's much harder to find a bug
that never surfaces than one that panics immediately.

❌ `kll_to_msgpack` returning an empty `Vec<u8>` on serialization failure —
looks like "no data" to every caller, not "this broke."

✅ Same failure path in the CMS serializer: panics, so the break is visible
at the call site instead of corrupting output downstream.

Applies to: error branches in Rust (`unwrap_or`, `unwrap_or_default`,
`.ok()` discarding an `Err`), Python `except: pass` / bare fallback returns,
and config resolution that silently picks a default when a required value is
missing.

## 2. No magic numbers or strings, one source of truth for each

Pull literal numbers (timeouts, poll intervals, thresholds) and strings into named constants, and don't let the same default get set in more than one place.

**Why:** an inline `30` or `300` in one file, and the same 30 hardcoded
again three files over, are the same bug waiting to happen twice — someone
changes one and not the other, and now the values silently disagree. This is
the same failure mode whether it's a bare literal or a duplicated default
value, so treat them as one problem: give the value exactly one home.

❌ Timeout/poll literals (`30`, `60`, `300`, `10`, `5`, `2`, `0.5s`)
scattered inline across several files in one module, while the rest of the
module uses named interval constants — breaks the existing pattern.

❌ A default sketch backend set as a fallback argument in multiple
functions instead of one global/config value.

✅ One named constant (or one config-file entry with no inline fallback
default) that every caller references.

## 3. Names carry the truth about the value

A name should precisely describe what it holds or does — including units for
anything time-like (`_MS`, `_SECS`) — and must be renamed the moment its
behavior changes underneath it.

**Why:** "seconds or milliseconds?" is a question that shouldn't need to be
asked; the field name should already answer it. A stale name (a "clone"
helper that now moves, a "pieces" builder that now also does
post-processing) actively misleads the next reader into wrong assumptions.

❌ `impl_clone_accumulator_methods` macro that now also generates
`into_accumulator`, an explicit move — the name promises cloning it doesn't
do.

❌ `KEY_SLIDE_INTERVAL` / `KEY_WINDOW_SIZE` holding millisecond values with
no `_MS` suffix, inconsistent with the rest of the same PR's naming.

✅ Rename on every semantic change, even mid-refactor, even if it touches
more call sites — don't defer it to "later."

## 4. No half-built abstractions, no duplicated logic

Either finish an abstraction's boundary (all callers go through it) or
delete it — don't leave a shim that some callers bypass. Before writing new
logic, check whether an existing helper/module already does it.

**Why:** a half-finished shim (e.g. isolating an internal type, but callers
still reach through it directly) gives the illusion of a stable boundary
while every upstream change still breaks N call sites — worse than no
abstraction, because it hides the real blast radius. Duplicated logic (a
poll-with-retry loop, a query-string decomposer) drifts the moment one copy
gets fixed and the other doesn't.

❌ `output/mod.rs` declaring three submodules that don't exist on disk — an
abandoned mid-refactor stub left in the tree.

❌ A service-readiness poll loop reimplemented locally when
`DockerServiceBase._wait_for_service_ready` already provides it.

✅ Either route every caller through the shim/abstraction, or delete it and
let callers use the underlying thing directly. Either way, grep for existing
helpers before writing a new poll/dedup/parse routine.

## Also check: internal consistency

Within one file or module, pick one convention (error type, dedup structure,
timeout threading) and use it everywhere in that file — don't mix
`Vec::contains` dedup in one function and `IndexSet` in the next function of
the same file, or thread a `timeout` param into one call and hardcode a
different value in the next.
