//! Shared spatial-filter predicate AST, parser, and evaluator - the single
//! implementation of "what WHERE-clause shapes can the ingest-time router
//! actually enforce, and how do we check one real row against a given
//! clause." Used by both:
//!   - the query engine's ingest-time sample router
//!     (`sample_matches_spatial_filter` in
//!     `precompute_engine/ingest_source.rs`), which needs to decide whether
//!     a real streamed row matches a registered precompute's filter, and
//!   - the planner's pre-flight safety check
//!     (`is_ingest_filter_fully_enforceable` in `pattern_rewrites.rs`),
//!     which needs to know *before* building a precompute whether the
//!     filter it's about to register is something the router above can
//!     actually enforce - registering a precompute behind a filter clause
//!     the router can't evaluate would silently serve every row in the
//!     window, mislabeled as the filtered subset.
//!
//! One parser, one evaluator: a clause shape recognized by one side and not
//! the other is exactly the kind of drift this module exists to prevent
//! (see the module doc on `pattern_rewrites`).
//!
//! Deliberately conservative: `parse_spatial_predicate` returns `None` for
//! any clause shape it doesn't recognize (an OR, a column-to-column
//! comparison, an unsupported function), and callers on both sides treat
//! `None` as "can't enforce this" - the planner refuses to register a
//! precompute behind it (falls through to punting), and the ingest router
//! only silently tolerates it for a small warned set of pre-existing
//! configs during a rollout, never for anything newly planned.

use std::collections::HashMap;

use chrono::{Datelike, TimeZone, Timelike, Utc};
use regex::Regex;

use super::pattern_rewrites::{
    find_matching_close_paren, is_bare_identifier, is_quoted_literal, split_top_level_and,
    split_top_level_commas, strip_wrapping_parens,
};

#[derive(Debug, Clone)]
pub enum SpatialPredicate {
    Eq(String, String),
    Ne(String, String),
    In(String, Vec<String>),
    Lt(String, String),
    Le(String, String),
    Gt(String, String),
    Ge(String, String),
    /// `match(col, 'regex')` - ClickHouse's `match()` is RE2-based; the
    /// `regex` crate's engine is RE2-family too, so plain character-class /
    /// anchor / quantifier patterns (the only kind these queries use) agree.
    Match(String, Box<Regex>),
    NotMatch(String, Box<Regex>),
    /// `startsWith(col, 'prefix')`.
    StartsWith(String, String),
    /// `positionCaseInsensitive(col, 'substr') > 0`.
    ContainsCi(String, String),
    /// `positionCaseInsensitive(col, 'substr') = 0`.
    NotContainsCi(String, String),
    /// `has(splitByChar('sep', col), 'literal')` - `sep` is always a single
    /// character in every query this module has seen; ClickHouse's own
    /// `splitByChar` also only accepts a single-character separator.
    HasSplitMember(String, char, String),
    NotHasSplitMember(String, char, String),
    /// `length(splitByChar('sep', col)) = n`.
    SplitLenEq(String, char, usize),
    /// `toInt64OrNull(col) IS NULL` (a `toString(col)` wrapper around `col`
    /// is stripped before this predicate is built, since label values are
    /// already strings and `toString` is a no-op on them).
    NotParsesAsInt(String),
    ParsesAsInt(String),
    /// `toHour(timestamp) <op> n` - evaluated against the *sample's own*
    /// timestamp, not a label; `n` is 0-23.
    HourLt(u8),
    HourLe(u8),
    HourGt(u8),
    HourGe(u8),
    /// `toDayOfWeek(timestamp) IN (...)` - ISO weekday numbers, Monday = 1
    /// .. Sunday = 7 (ClickHouse's default `toDayOfWeek` mode), evaluated
    /// against the sample's own timestamp, interpreted as UTC (this
    /// workload's `timestamp` column is `DateTime('UTC')`, so ClickHouse's
    /// own `toDayOfWeek`/`toHour` on it are UTC regardless of server
    /// session timezone).
    DayOfWeekIn(Vec<u8>),
    /// `toDayOfWeek(timestamp) NOT IN (...)`.
    DayOfWeekNotIn(Vec<u8>),
    /// `length(splitByChar('sep', col)) <op> n`.
    SplitLenLt(String, char, usize),
    SplitLenLe(String, char, usize),
    SplitLenGt(String, char, usize),
    SplitLenGe(String, char, usize),
    /// `<numeric-cast>(splitByChar('sep', col)[n]) <op> threshold` - e.g.
    /// `toUInt8OrZero(splitByChar('/', prefix)[2]) >= 24`, a CIDR mask
    /// filter. `n` is ClickHouse's 1-indexed array position. A token that
    /// doesn't parse as a number (missing separator, non-numeric field)
    /// never satisfies the comparison, matching `toUInt8OrZero`'s own
    /// "parse failure -> 0" semantics only when the threshold is
    /// itself <= 0 - conservative otherwise, which is the safer direction
    /// for a filter (excluding a row ingest can't confidently classify,
    /// rather than risk wrongly including a genuinely different row).
    SplitTokenLt(String, char, usize, f64),
    SplitTokenLe(String, char, usize, f64),
    SplitTokenGt(String, char, usize, f64),
    SplitTokenGe(String, char, usize, f64),
    /// `timestamp <op> 'YYYY-MM-DD HH:MM:SS'` - a range check on the bare
    /// time column itself (e.g. a sub-window filter inside a countIf, as
    /// opposed to the query's own outer time bound, which the classic
    /// parser already extracts into `TimeInfo` rather than leaving in
    /// `spatial_filter`). Evaluated against the sample's own timestamp
    /// (epoch ms, UTC), not a label - "timestamp" is never a real key in a
    /// sample's label map, so treating it as a bare-label range comparison
    /// like `Lt`/`Le`/`Gt`/`Ge` would silently and permanently evaluate to
    /// "no such label" on every row.
    TimestampLt(i64),
    TimestampLe(i64),
    TimestampGt(i64),
    TimestampGe(i64),
    /// `(<col> = '' OR <col> IS NULL)`, either order (q093's "missing
    /// next_hop" shape) - the one specific OR-shape this module recognizes
    /// (every other OR is deliberately left unenforceable, see the module
    /// doc comment). This dataset's CSV ingest has no true NULL, only an
    /// empty string, so the two branches are already equivalent in
    /// practice; evaluated as "the label is empty or entirely absent."
    IsEmptyOrNull(String),
    /// `splitByChar('sep', col1)[-1] = col2` (q069's "does the last
    /// AS-path hop match the origin field" shape) - the one column-to-
    /// column comparison this module recognizes; both columns are already
    /// present in a sample's own label map, so this is still a per-row
    /// check, just against another label instead of a literal.
    LastSplitTokenEqColumn(String, char, String),
    /// `splitByChar('sep', col1)[-1] != col2`.
    LastSplitTokenNeColumn(String, char, String),
}

/// Splits `filter` into atomic (non-AND) clauses, recursing into any clause
/// that - after stripping wrapping parens - reveals a further top-level AND.
/// Needed because `build_multi_aggregate_surrogates` folds a countIf's own
/// (possibly compound) condition into the surrogate's WHERE clause wrapped
/// in one parenthesized group, e.g. `collector = 'x' AND (a != '' AND b IS
/// NULL)` - a single call to `split_top_level_and` treats `(a != '' AND b IS
/// NULL)` as one clause (parens keep it from splitting there), and without
/// this recursion it would never be tried as the two enforceable predicates
/// it actually is.
fn flatten_and_clauses(filter: &str) -> Vec<String> {
    let clauses = split_top_level_and(filter.trim());
    if clauses.len() > 1 {
        return clauses.iter().flat_map(|c| flatten_and_clauses(c)).collect();
    }
    let stripped = strip_wrapping_parens(filter.trim());
    if stripped != filter.trim() && split_top_level_and(stripped).len() > 1 {
        return flatten_and_clauses(stripped);
    }
    vec![filter.trim().to_string()]
}

/// True when every top-level AND-clause of `filter` parses to a known,
/// evaluable `SpatialPredicate`. An empty filter (no extra predicate beyond
/// the metric/time window, which the classic parser already extracts into
/// `TimeInfo` rather than `spatial_filter`) is trivially enforceable.
pub fn is_filter_fully_enforceable(filter: &str) -> bool {
    let trimmed = filter.trim();
    if trimmed.is_empty() {
        return true;
    }
    flatten_and_clauses(trimmed)
        .iter()
        .all(|clause| parse_spatial_predicate(clause).is_some())
}

/// Evaluates `filter` against one row's labels and timestamp. Returns
/// `(matches, unsupported_clauses)`: any clause that doesn't parse is
/// treated as "always true" (the long-standing permissive fallback for
/// clause shapes this module doesn't yet recognize) but is also named in
/// the second element so the caller can warn once per distinct clause
/// rather than silently accept it as enforced.
pub fn evaluate_filter<'a>(
    filter: &str,
    labels: &HashMap<&str, &str>,
    timestamp_ms: i64,
) -> (bool, Vec<String>) {
    let filter = filter.trim();
    if filter.is_empty() {
        return (true, Vec::new());
    }
    let mut unsupported = Vec::new();
    for clause in flatten_and_clauses(filter) {
        match parse_spatial_predicate(&clause) {
            Some(pred) => {
                if !evaluate_predicate(&pred, labels, timestamp_ms) {
                    return (false, unsupported);
                }
            }
            None => unsupported.push(clause),
        }
    }
    (true, unsupported)
}

pub fn evaluate_predicate(
    pred: &SpatialPredicate,
    labels: &HashMap<&str, &str>,
    timestamp_ms: i64,
) -> bool {
    let get = |col: &str| labels.get(col).copied();
    match pred {
        SpatialPredicate::Eq(col, expected) => get(col) == Some(expected.as_str()),
        SpatialPredicate::Ne(col, excluded) => get(col) != Some(excluded.as_str()),
        SpatialPredicate::In(col, allowed) => {
            get(col).is_some_and(|v| allowed.iter().any(|a| a == v))
        }
        SpatialPredicate::Lt(col, rhs) => compare_values(get(col), rhs, std::cmp::Ordering::Less),
        SpatialPredicate::Le(col, rhs) => {
            compare_values(get(col), rhs, std::cmp::Ordering::Less)
                || get(col) == Some(rhs.as_str())
        }
        SpatialPredicate::Gt(col, rhs) => {
            compare_values(get(col), rhs, std::cmp::Ordering::Greater)
        }
        SpatialPredicate::Ge(col, rhs) => {
            compare_values(get(col), rhs, std::cmp::Ordering::Greater)
                || get(col) == Some(rhs.as_str())
        }
        SpatialPredicate::Match(col, re) => get(col).is_some_and(|v| re.is_match(v)),
        SpatialPredicate::NotMatch(col, re) => get(col).is_some_and(|v| !re.is_match(v)),
        SpatialPredicate::StartsWith(col, prefix) => {
            get(col).is_some_and(|v| v.starts_with(prefix.as_str()))
        }
        SpatialPredicate::ContainsCi(col, substr) => get(col)
            .is_some_and(|v| v.to_lowercase().contains(&substr.to_lowercase())),
        SpatialPredicate::NotContainsCi(col, substr) => !get(col)
            .is_some_and(|v| v.to_lowercase().contains(&substr.to_lowercase())),
        SpatialPredicate::HasSplitMember(col, sep, member) => {
            get(col).is_some_and(|v| v.split(*sep).any(|part| part == member))
        }
        SpatialPredicate::NotHasSplitMember(col, sep, member) => {
            !get(col).is_some_and(|v| v.split(*sep).any(|part| part == member))
        }
        SpatialPredicate::SplitLenEq(col, sep, n) => {
            get(col).is_some_and(|v| v.split(*sep).count() == *n)
        }
        SpatialPredicate::NotParsesAsInt(col) => {
            !get(col).is_some_and(|v| v.trim().parse::<i64>().is_ok())
        }
        SpatialPredicate::ParsesAsInt(col) => {
            get(col).is_some_and(|v| v.trim().parse::<i64>().is_ok())
        }
        SpatialPredicate::HourLt(n) => hour_of(timestamp_ms) < *n,
        SpatialPredicate::HourLe(n) => hour_of(timestamp_ms) <= *n,
        SpatialPredicate::HourGt(n) => hour_of(timestamp_ms) > *n,
        SpatialPredicate::HourGe(n) => hour_of(timestamp_ms) >= *n,
        SpatialPredicate::DayOfWeekIn(days) => days.contains(&day_of_week_of(timestamp_ms)),
        SpatialPredicate::DayOfWeekNotIn(days) => !days.contains(&day_of_week_of(timestamp_ms)),
        SpatialPredicate::SplitLenLt(col, sep, n) => {
            get(col).is_some_and(|v| v.split(*sep).count() < *n)
        }
        SpatialPredicate::SplitLenLe(col, sep, n) => {
            get(col).is_some_and(|v| v.split(*sep).count() <= *n)
        }
        SpatialPredicate::SplitLenGt(col, sep, n) => {
            get(col).is_some_and(|v| v.split(*sep).count() > *n)
        }
        SpatialPredicate::SplitLenGe(col, sep, n) => {
            get(col).is_some_and(|v| v.split(*sep).count() >= *n)
        }
        SpatialPredicate::SplitTokenLt(col, sep, n, threshold) => {
            split_token_as_f64(get(col), *sep, *n).is_some_and(|v| v < *threshold)
        }
        SpatialPredicate::SplitTokenLe(col, sep, n, threshold) => {
            split_token_as_f64(get(col), *sep, *n).is_some_and(|v| v <= *threshold)
        }
        SpatialPredicate::SplitTokenGt(col, sep, n, threshold) => {
            split_token_as_f64(get(col), *sep, *n).is_some_and(|v| v > *threshold)
        }
        SpatialPredicate::SplitTokenGe(col, sep, n, threshold) => {
            split_token_as_f64(get(col), *sep, *n).is_some_and(|v| v >= *threshold)
        }
        SpatialPredicate::TimestampLt(bound) => timestamp_ms < *bound,
        SpatialPredicate::TimestampLe(bound) => timestamp_ms <= *bound,
        SpatialPredicate::TimestampGt(bound) => timestamp_ms > *bound,
        SpatialPredicate::TimestampGe(bound) => timestamp_ms >= *bound,
        SpatialPredicate::IsEmptyOrNull(col) => get(col).is_none_or(|v| v.is_empty()),
        SpatialPredicate::LastSplitTokenEqColumn(col1, sep, col2) => {
            let last = get(col1).and_then(|v| v.split(*sep).last());
            last.is_some() && last == get(col2)
        }
        SpatialPredicate::LastSplitTokenNeColumn(col1, sep, col2) => {
            let last = get(col1).and_then(|v| v.split(*sep).last());
            last.is_some() && last != get(col2)
        }
    }
}

/// UTC hour-of-day (0-23) for an epoch-millisecond timestamp.
fn hour_of(timestamp_ms: i64) -> u8 {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .map(|dt| dt.hour() as u8)
        .unwrap_or(0)
}

/// ISO weekday (Monday = 1 .. Sunday = 7), matching ClickHouse's default
/// `toDayOfWeek` mode, for an epoch-millisecond timestamp interpreted as UTC.
fn day_of_week_of(timestamp_ms: i64) -> u8 {
    Utc.timestamp_millis_opt(timestamp_ms)
        .single()
        .map(|dt| dt.weekday().number_from_monday() as u8)
        .unwrap_or(0)
}

/// `col`'s value split on `sep`, `n`th token (1-indexed, ClickHouse array
/// convention), parsed as f64 - `None` if the column is missing, the token
/// doesn't exist, or it doesn't parse as a number.
fn split_token_as_f64(col_value: Option<&str>, sep: char, n: usize) -> Option<f64> {
    if n == 0 {
        return None;
    }
    col_value?
        .split(sep)
        .nth(n - 1)?
        .trim()
        .parse::<f64>()
        .ok()
}

/// Numeric-if-both-parse-as-numbers, else lexicographic string comparison -
/// reasonable general semantics for a range comparison against a column
/// represented as a string label.
fn compare_values(actual: Option<&str>, rhs: &str, want: std::cmp::Ordering) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    let ord = match (actual.parse::<f64>(), rhs.parse::<f64>()) {
        (Ok(a), Ok(b)) => a.partial_cmp(&b),
        _ => Some(actual.cmp(rhs)),
    };
    ord == Some(want)
}

pub fn parse_spatial_predicate(clause: &str) -> Option<SpatialPredicate> {
    let clause = strip_wrapping_parens(clause).trim();

    if let Some(inner) = clause
        .strip_prefix("NOT ")
        .or_else(|| clause.strip_prefix("not "))
    {
        return parse_negated(inner.trim());
    }

    // toString(col) <op> ... - label values are already strings, so
    // toString is a no-op; strip it and re-parse the rest against the bare
    // column.
    if let Some(rewritten) = strip_to_string_wrapper(clause) {
        return parse_spatial_predicate(&rewritten);
    }

    // The one specific OR shape this module recognizes - checked before
    // every AND-oriented parser below, which would otherwise only ever see
    // one half of the OR (or the whole unsplit clause) and fail to match.
    if let Some(p) = parse_is_empty_or_null(clause) {
        return Some(p);
    }

    if let Some(p) = parse_match_call(clause) {
        return Some(p);
    }
    if let Some(p) = parse_starts_with(clause) {
        return Some(p);
    }
    if let Some(p) = parse_position_case_insensitive(clause) {
        return Some(p);
    }
    if let Some(p) = parse_has_split_member(clause) {
        return Some(p);
    }
    if let Some(p) = parse_last_split_token_vs_column(clause) {
        return Some(p);
    }
    if let Some(p) = parse_split_len_eq(clause) {
        return Some(p);
    }
    if let Some(p) = parse_split_token_range(clause) {
        return Some(p);
    }
    if let Some(p) = parse_parses_as_int(clause) {
        return Some(p);
    }
    if let Some(p) = parse_hour_compare(clause) {
        return Some(p);
    }
    if let Some(p) = parse_day_of_week_not_in(clause) {
        return Some(p);
    }
    if let Some(p) = parse_day_of_week_in(clause) {
        return Some(p);
    }
    if let Some(p) = parse_in(clause) {
        return Some(p);
    }
    if let Some(p) = parse_timestamp_range(clause) {
        return Some(p);
    }
    if let Some(p) = parse_range(clause) {
        return Some(p);
    }
    if let Some(p) = parse_ne(clause) {
        return Some(p);
    }
    parse_eq(clause)
}

fn parse_negated(inner: &str) -> Option<SpatialPredicate> {
    match parse_match_call(inner) {
        Some(SpatialPredicate::Match(col, re)) => return Some(SpatialPredicate::NotMatch(col, re)),
        _ => {}
    }
    match parse_has_split_member(inner) {
        Some(SpatialPredicate::HasSplitMember(col, sep, member)) => {
            return Some(SpatialPredicate::NotHasSplitMember(col, sep, member))
        }
        _ => {}
    }
    match parse_parses_as_int(inner) {
        Some(SpatialPredicate::ParsesAsInt(col)) => {
            return Some(SpatialPredicate::NotParsesAsInt(col))
        }
        Some(SpatialPredicate::NotParsesAsInt(col)) => {
            return Some(SpatialPredicate::ParsesAsInt(col))
        }
        _ => {}
    }
    None
}

/// `toDayOfWeek(timestamp) NOT IN (n1, n2, ...)` - `NOT IN` isn't reachable
/// through `parse_negated` (which only handles a leading bare `NOT `; `NOT
/// IN` is its own infix operator, not a prefix on the whole clause), so this
/// is tried directly, mirroring `parse_day_of_week_in`.
fn parse_day_of_week_not_in(clause: &str) -> Option<SpatialPredicate> {
    let lower = clause.to_lowercase();
    if !lower.starts_with("todayofweek(") {
        return None;
    }
    let close_idx = find_matching_close_paren(clause, "todayofweek".len())?;
    let inner = clause["todayofweek(".len()..close_idx].trim();
    if !inner.eq_ignore_ascii_case("timestamp") {
        return None;
    }
    let rest = clause[close_idx + 1..].trim();
    let rest_upper = rest.to_uppercase();
    let in_rest = rest_upper.strip_prefix("NOT IN")?;
    let paren_rest = rest[rest.len() - in_rest.len()..].trim_start();
    let inner_list = paren_rest.strip_prefix('(')?.strip_suffix(')')?;
    let days: Option<Vec<u8>> = inner_list
        .split(',')
        .map(|d| d.trim().parse::<u8>().ok().filter(|d| (1..=7).contains(d)))
        .collect();
    let days = days?;
    if days.is_empty() {
        None
    } else {
        Some(SpatialPredicate::DayOfWeekNotIn(days))
    }
}

/// `toString(<col>)` immediately followed by more clause text - returns the
/// clause with that prefix replaced by the bare column name.
fn strip_to_string_wrapper(clause: &str) -> Option<String> {
    let lower = clause.to_lowercase();
    if !lower.starts_with("tostring(") {
        return None;
    }
    let open_idx = "tostring".len();
    let close_idx = find_matching_close_paren(clause, open_idx)?;
    let inner = clause[open_idx + 1..close_idx].trim();
    if !is_bare_identifier(inner) {
        return None;
    }
    let rest = &clause[close_idx + 1..];
    Some(format!("{inner}{rest}"))
}

/// `match(col, 'regex')` - the sole two-argument form these queries use.
fn parse_match_call(clause: &str) -> Option<SpatialPredicate> {
    let lower = clause.to_lowercase();
    if !lower.starts_with("match(") {
        return None;
    }
    let close_idx = find_matching_close_paren(clause, "match".len())?;
    if clause[close_idx + 1..].trim() != "" {
        return None; // trailing text after match(...) isn't this shape
    }
    let args = split_top_level_commas(&clause["match(".len()..close_idx]);
    let [col, pattern] = args.as_slice() else {
        return None;
    };
    let col = col.trim();
    if !is_bare_identifier(col) {
        return None;
    }
    let pattern = pattern.trim();
    if !is_quoted_literal(pattern) {
        return None;
    }
    let regex_src = &pattern[1..pattern.len() - 1];
    let re = Regex::new(regex_src).ok()?;
    Some(SpatialPredicate::Match(col.to_string(), Box::new(re)))
}

fn parse_starts_with(clause: &str) -> Option<SpatialPredicate> {
    let lower = clause.to_lowercase();
    if !lower.starts_with("startswith(") {
        return None;
    }
    let close_idx = find_matching_close_paren(clause, "startswith".len())?;
    if !clause[close_idx + 1..].trim().is_empty() {
        return None;
    }
    let args = split_top_level_commas(&clause["startswith(".len()..close_idx]);
    let [col, prefix] = args.as_slice() else {
        return None;
    };
    let col = col.trim();
    if !is_bare_identifier(col) {
        return None;
    }
    let prefix = prefix.trim();
    if !is_quoted_literal(prefix) {
        return None;
    }
    Some(SpatialPredicate::StartsWith(
        col.to_string(),
        prefix[1..prefix.len() - 1].to_string(),
    ))
}

/// `positionCaseInsensitive(col, 'substr') > 0` / `= 0`.
fn parse_position_case_insensitive(clause: &str) -> Option<SpatialPredicate> {
    let lower = clause.to_lowercase();
    if !lower.starts_with("positioncaseinsensitive(") {
        return None;
    }
    let close_idx = find_matching_close_paren(clause, "positioncaseinsensitive".len())?;
    let rest = clause[close_idx + 1..].trim();

    let args = split_top_level_commas(&clause["positioncaseinsensitive(".len()..close_idx]);
    let [col, substr] = args.as_slice() else {
        return None;
    };
    let col = col.trim();
    if !is_bare_identifier(col) {
        return None;
    }
    let substr = substr.trim();
    if !is_quoted_literal(substr) {
        return None;
    }
    let substr = substr[1..substr.len() - 1].to_string();

    if rest == "> 0" {
        Some(SpatialPredicate::ContainsCi(col.to_string(), substr))
    } else if rest == "= 0" {
        Some(SpatialPredicate::NotContainsCi(col.to_string(), substr))
    } else {
        None
    }
}

/// `has(splitByChar('sep', col), 'literal')`.
fn parse_has_split_member(clause: &str) -> Option<SpatialPredicate> {
    let lower = clause.to_lowercase();
    if !lower.starts_with("has(") {
        return None;
    }
    let close_idx = find_matching_close_paren(clause, "has".len())?;
    if !clause[close_idx + 1..].trim().is_empty() {
        return None;
    }
    let args = split_top_level_commas(&clause["has(".len()..close_idx]);
    let [split_expr, member] = args.as_slice() else {
        return None;
    };
    let (col, sep) = parse_split_by_char(split_expr.trim())?;
    let member = member.trim();
    if !is_quoted_literal(member) {
        return None;
    }
    Some(SpatialPredicate::HasSplitMember(
        col,
        sep,
        member[1..member.len() - 1].to_string(),
    ))
}

/// `length(splitByChar('sep', col)) <op> n`, `<op>` one of `=`, `<`, `<=`,
/// `>`, `>=`. Longer operators (`<=`, `>=`) are tried before their
/// single-character prefixes so `<=`/`>=` don't get misread as `<`/`>` with
/// a stray leading `=` in the remaining number text.
fn parse_split_len_eq(clause: &str) -> Option<SpatialPredicate> {
    let lower = clause.to_lowercase();
    if !lower.starts_with("length(") {
        return None;
    }
    let close_idx = find_matching_close_paren(clause, "length".len())?;
    let inner = clause["length(".len()..close_idx].trim();
    let (col, sep) = parse_split_by_char(inner)?;
    let rest = clause[close_idx + 1..].trim();

    if let Some(n_str) = rest.strip_prefix("<=") {
        let n: usize = n_str.trim().parse().ok()?;
        return Some(SpatialPredicate::SplitLenLe(col, sep, n));
    }
    if let Some(n_str) = rest.strip_prefix(">=") {
        let n: usize = n_str.trim().parse().ok()?;
        return Some(SpatialPredicate::SplitLenGe(col, sep, n));
    }
    if let Some(n_str) = rest.strip_prefix('<') {
        let n: usize = n_str.trim().parse().ok()?;
        return Some(SpatialPredicate::SplitLenLt(col, sep, n));
    }
    if let Some(n_str) = rest.strip_prefix('>') {
        let n: usize = n_str.trim().parse().ok()?;
        return Some(SpatialPredicate::SplitLenGt(col, sep, n));
    }
    if let Some(n_str) = rest.strip_prefix('=') {
        let n: usize = n_str.trim().parse().ok()?;
        return Some(SpatialPredicate::SplitLenEq(col, sep, n));
    }
    None
}

const NUMERIC_CAST_PREFIXES: &[&str] = &[
    "touint8orzero(",
    "touint16orzero(",
    "touint32orzero(",
    "touint64orzero(",
    "toint8orzero(",
    "toint16orzero(",
    "toint32orzero(",
    "toint64orzero(",
    "tofloat32orzero(",
    "tofloat64orzero(",
];

/// `<numeric-cast>(splitByChar('sep', col)[n]) <op> threshold` - e.g.
/// `toUInt8OrZero(splitByChar('/', prefix)[2]) >= 24`. The cast wrapper is
/// required (unlike `parse_split_len_eq`'s bare `length(...)`, which is
/// already numeric): a raw string token compared with `<`/`>` would be
/// lexicographic, not the numeric comparison the query actually means.
fn parse_split_token_range(clause: &str) -> Option<SpatialPredicate> {
    let lower = clause.to_lowercase();
    let cast_prefix = NUMERIC_CAST_PREFIXES.iter().find(|p| lower.starts_with(**p))?;
    let cast_close = find_matching_close_paren(clause, cast_prefix.len() - 1)?;
    let split_expr = clause[cast_prefix.len()..cast_close].trim();

    let split_lower = split_expr.to_lowercase();
    if !split_lower.starts_with("splitbychar(") {
        return None;
    }
    let split_close = find_matching_close_paren(split_expr, "splitbychar".len())?;
    let split_args = split_top_level_commas(&split_expr["splitbychar(".len()..split_close]);
    let [sep_lit, col] = split_args.as_slice() else {
        return None;
    };
    let sep_lit = sep_lit.trim();
    if !is_quoted_literal(sep_lit) {
        return None;
    }
    let mut chars = sep_lit[1..sep_lit.len() - 1].chars();
    let sep = chars.next()?;
    if chars.next().is_some() {
        return None;
    }
    let col = col.trim();
    if !is_bare_identifier(col) {
        return None;
    }

    let idx_rest = split_expr[split_close + 1..].trim();
    let idx_str = idx_rest.strip_prefix('[')?.strip_suffix(']')?;
    let n: usize = idx_str.trim().parse().ok()?;
    if n == 0 {
        return None;
    }

    let rest = clause[cast_close + 1..].trim();
    if let Some(t_str) = rest.strip_prefix("<=") {
        let t: f64 = t_str.trim().parse().ok()?;
        return Some(SpatialPredicate::SplitTokenLe(col.to_string(), sep, n, t));
    }
    if let Some(t_str) = rest.strip_prefix(">=") {
        let t: f64 = t_str.trim().parse().ok()?;
        return Some(SpatialPredicate::SplitTokenGe(col.to_string(), sep, n, t));
    }
    if let Some(t_str) = rest.strip_prefix('<') {
        let t: f64 = t_str.trim().parse().ok()?;
        return Some(SpatialPredicate::SplitTokenLt(col.to_string(), sep, n, t));
    }
    if let Some(t_str) = rest.strip_prefix('>') {
        let t: f64 = t_str.trim().parse().ok()?;
        return Some(SpatialPredicate::SplitTokenGt(col.to_string(), sep, n, t));
    }
    None
}

/// Parses `splitByChar('sep', col)`, returning `(col, sep_char)`.
fn parse_split_by_char(s: &str) -> Option<(String, char)> {
    let lower = s.to_lowercase();
    if !lower.starts_with("splitbychar(") {
        return None;
    }
    let close_idx = find_matching_close_paren(s, "splitbychar".len())?;
    if !s[close_idx + 1..].trim().is_empty() {
        return None;
    }
    let args = split_top_level_commas(&s["splitbychar(".len()..close_idx]);
    let [sep_lit, col] = args.as_slice() else {
        return None;
    };
    let sep_lit = sep_lit.trim();
    if !is_quoted_literal(sep_lit) {
        return None;
    }
    let sep_inner = &sep_lit[1..sep_lit.len() - 1];
    let mut chars = sep_inner.chars();
    let sep = chars.next()?;
    if chars.next().is_some() {
        return None; // multi-character separator - not what ClickHouse's splitByChar accepts either
    }
    let col = col.trim();
    if !is_bare_identifier(col) {
        return None;
    }
    Some((col.to_string(), sep))
}

/// `toInt64OrNull(<col>) IS NULL` / `IS NOT NULL` (a `toString(...)` wrapper
/// around `<col>` has already been stripped by `parse_spatial_predicate`
/// before this runs, since it recurses through `strip_to_string_wrapper`
/// first).
fn parse_parses_as_int(clause: &str) -> Option<SpatialPredicate> {
    let lower = clause.to_lowercase();
    if !lower.starts_with("toint64ornull(") {
        return None;
    }
    let close_idx = find_matching_close_paren(clause, "toint64ornull".len())?;
    let inner = clause["toint64ornull(".len()..close_idx].trim();
    // The inner argument may itself be `toString(col)` - strip that too.
    let col = strip_to_string_wrapper(&format!("{inner} ")).map_or_else(
        || inner.to_string(),
        |rewritten| rewritten.trim().to_string(),
    );
    if !is_bare_identifier(&col) {
        return None;
    }
    let rest = clause[close_idx + 1..].trim().to_uppercase();
    if rest == "IS NULL" {
        Some(SpatialPredicate::NotParsesAsInt(col))
    } else if rest == "IS NOT NULL" {
        Some(SpatialPredicate::ParsesAsInt(col))
    } else {
        None
    }
}

/// `toHour(timestamp) <op> n`, `n` a plain integer 0-23.
fn parse_hour_compare(clause: &str) -> Option<SpatialPredicate> {
    let lower = clause.to_lowercase();
    if !lower.starts_with("tohour(") {
        return None;
    }
    let close_idx = find_matching_close_paren(clause, "tohour".len())?;
    let inner = clause["tohour(".len()..close_idx].trim();
    if !inner.eq_ignore_ascii_case("timestamp") {
        return None;
    }
    let rest = clause[close_idx + 1..].trim();
    for (op, ctor) in [
        ("<=", SpatialPredicate::HourLe as fn(u8) -> SpatialPredicate),
        (">=", SpatialPredicate::HourGe as fn(u8) -> SpatialPredicate),
        ("<", SpatialPredicate::HourLt as fn(u8) -> SpatialPredicate),
        (">", SpatialPredicate::HourGt as fn(u8) -> SpatialPredicate),
    ] {
        if let Some(n_str) = rest.strip_prefix(op) {
            let n: u8 = n_str.trim().parse().ok()?;
            if n <= 23 {
                return Some(ctor(n));
            }
        }
    }
    None
}

/// `toDayOfWeek(timestamp) IN (n1, n2, ...)`, each `n` a plain integer 1-7.
fn parse_day_of_week_in(clause: &str) -> Option<SpatialPredicate> {
    let lower = clause.to_lowercase();
    if !lower.starts_with("todayofweek(") {
        return None;
    }
    let close_idx = find_matching_close_paren(clause, "todayofweek".len())?;
    let inner = clause["todayofweek(".len()..close_idx].trim();
    if !inner.eq_ignore_ascii_case("timestamp") {
        return None;
    }
    let rest = clause[close_idx + 1..].trim();
    let rest_upper = rest.to_uppercase();
    let in_rest = rest_upper.strip_prefix("IN")?;
    let paren_rest = rest[rest.len() - in_rest.len()..].trim_start();
    let inner_list = paren_rest.strip_prefix('(')?.strip_suffix(')')?;
    let days: Option<Vec<u8>> = inner_list
        .split(',')
        .map(|d| d.trim().parse::<u8>().ok().filter(|d| (1..=7).contains(d)))
        .collect();
    let days = days?;
    if days.is_empty() {
        None
    } else {
        Some(SpatialPredicate::DayOfWeekIn(days))
    }
}

fn parse_in(clause: &str) -> Option<SpatialPredicate> {
    let upper = clause.to_uppercase();
    let in_idx = upper.find(" IN ")?;
    let label = clause[..in_idx].trim();
    if !is_bare_identifier(label) {
        return None;
    }
    let rest = clause[in_idx + 4..].trim();
    let inner = rest.strip_prefix('(')?.trim_end().strip_suffix(')')?;
    let values: Vec<String> = split_top_level_commas(inner)
        .into_iter()
        .map(|v| {
            let v = v.trim();
            if is_quoted_literal(v) {
                v[1..v.len() - 1].to_string()
            } else {
                v.to_string()
            }
        })
        .filter(|v| !v.is_empty())
        .collect();
    if values.is_empty() {
        None
    } else {
        Some(SpatialPredicate::In(label.to_string(), values))
    }
}

/// `timestamp <op> 'YYYY-MM-DD HH:MM:SS'` - see `SpatialPredicate::TimestampLt`
/// et al. Tried before `parse_range` so a bare `timestamp` comparison never
/// falls through to a generic label lookup ("timestamp" is never a real key
/// in a sample's label map).
fn parse_timestamp_range(clause: &str) -> Option<SpatialPredicate> {
    for (op, ctor) in [
        (
            "<=",
            SpatialPredicate::TimestampLe as fn(i64) -> SpatialPredicate,
        ),
        (
            ">=",
            SpatialPredicate::TimestampGe as fn(i64) -> SpatialPredicate,
        ),
        (
            "<",
            SpatialPredicate::TimestampLt as fn(i64) -> SpatialPredicate,
        ),
        (
            ">",
            SpatialPredicate::TimestampGt as fn(i64) -> SpatialPredicate,
        ),
    ] {
        if let Some(idx) = clause.find(op) {
            let lhs = clause[..idx].trim();
            let rhs = clause[idx + op.len()..].trim();
            if !lhs.eq_ignore_ascii_case("timestamp") || !is_quoted_literal(rhs) {
                continue;
            }
            let literal = &rhs[1..rhs.len() - 1];
            let epoch_ms = parse_utc_datetime_ms(literal)?;
            return Some(ctor(epoch_ms));
        }
    }
    None
}

/// Parses a `'YYYY-MM-DD HH:MM:SS'` literal as UTC - this workload's
/// `timestamp` column is `DateTime('UTC')`, so a literal compared against it
/// in ClickHouse is interpreted as UTC regardless of session timezone.
fn parse_utc_datetime_ms(s: &str) -> Option<i64> {
    let naive = chrono::NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%d %H:%M:%S").ok()?;
    Some(naive.and_utc().timestamp_millis())
}

fn parse_range(clause: &str) -> Option<SpatialPredicate> {
    for (op, ctor) in [
        (
            "<=",
            SpatialPredicate::Le as fn(String, String) -> SpatialPredicate,
        ),
        (">=", SpatialPredicate::Ge as fn(String, String) -> SpatialPredicate),
        ("<", SpatialPredicate::Lt as fn(String, String) -> SpatialPredicate),
        (">", SpatialPredicate::Gt as fn(String, String) -> SpatialPredicate),
    ] {
        if let Some(idx) = clause.find(op) {
            let lhs = clause[..idx].trim();
            let rhs = clause[idx + op.len()..].trim();
            if !is_bare_identifier(lhs) || !is_quoted_literal(rhs) {
                continue;
            }
            return Some(ctor(lhs.to_string(), rhs[1..rhs.len() - 1].to_string()));
        }
    }
    None
}

fn parse_ne(clause: &str) -> Option<SpatialPredicate> {
    // sqlparser's own Display impl renders a parsed `!=` back out as `<>`.
    let (lhs, rhs) = clause.split_once("!=").or_else(|| clause.split_once("<>"))?;
    let lhs = lhs.trim();
    let rhs = rhs.trim();
    if !is_bare_identifier(lhs) || !is_quoted_literal(rhs) {
        return None;
    }
    Some(SpatialPredicate::Ne(
        lhs.to_string(),
        rhs[1..rhs.len() - 1].to_string(),
    ))
}

/// Splits `s` on a single top-level " OR " (case-insensitive, not nested in
/// parens/quotes), returning `None` if there isn't exactly one - same
/// conservative shape as every other single-purpose splitter in this
/// module (`split_top_level_and`, `find_comparison_op` in
/// pattern_rewrites.rs), deliberately not a general OR parser (see the
/// module doc comment on why OR stays otherwise unsupported).
fn split_top_level_or_pair(s: &str) -> Option<(String, String)> {
    let mut depth = 0i32;
    let mut in_quotes = false;
    let mut split_at = None;
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c == '\'' {
            in_quotes = !in_quotes;
            i += 1;
            continue;
        }
        if !in_quotes {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                _ if depth == 0 => {
                    let rest = &s[i..];
                    let preceded_by_space = i == 0 || bytes[i - 1] == b' ';
                    if preceded_by_space
                        && (rest.starts_with("OR ") || rest.starts_with("or "))
                    {
                        if split_at.is_some() {
                            return None; // more than one top-level OR - not this shape
                        }
                        split_at = Some(i);
                        i += 3;
                        continue;
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    let i = split_at?;
    Some((s[..i].trim().to_string(), s[i + 3..].trim().to_string()))
}

/// `(<col> = '' OR <col> IS NULL)`, either order (q093's "missing next_hop"
/// shape) - see `SpatialPredicate::IsEmptyOrNull`'s doc comment for why
/// this one specific OR is recognized when every other OR is not.
fn parse_is_empty_or_null(clause: &str) -> Option<SpatialPredicate> {
    let (a, b) = split_top_level_or_pair(clause)?;

    fn as_empty_eq(s: &str) -> Option<&str> {
        let (lhs, rhs) = s.split_once('=')?;
        let lhs = lhs.trim();
        let rhs = rhs.trim();
        if is_bare_identifier(lhs) && rhs == "''" {
            Some(lhs)
        } else {
            None
        }
    }
    fn as_is_null(s: &str) -> Option<&str> {
        let upper = s.to_uppercase();
        let rest = upper.strip_suffix("IS NULL")?;
        let col = s[..rest.len()].trim();
        is_bare_identifier(col).then_some(col)
    }

    let col = as_empty_eq(&a)
        .zip(as_is_null(&b))
        .or_else(|| as_is_null(&a).zip(as_empty_eq(&b)))
        .and_then(|(c1, c2)| (c1 == c2).then_some(c1))?;

    Some(SpatialPredicate::IsEmptyOrNull(col.to_string()))
}

/// `splitByChar('sep', col1)[-1] = col2` / `!= col2` - see
/// `SpatialPredicate::LastSplitTokenEqColumn`'s doc comment.
fn parse_last_split_token_vs_column(clause: &str) -> Option<SpatialPredicate> {
    // sqlparser's own Display impl renders a parsed `!=` back out as `<>`
    // (see parse_ne's identical fallback above).
    let (is_eq, lhs, rhs) = if let Some((l, r)) = clause.split_once("!=") {
        (false, l, r)
    } else if let Some((l, r)) = clause.split_once("<>") {
        (false, l, r)
    } else if let Some((l, r)) = clause.split_once('=') {
        (true, l, r)
    } else {
        return None;
    };
    let lhs = lhs.trim();
    let rhs = rhs.trim();
    let lower = lhs.to_lowercase();
    if !lower.starts_with("splitbychar(") {
        return None;
    }
    let close_idx = find_matching_close_paren(lhs, "splitbychar".len())?;
    let inner = &lhs["splitbychar(".len()..close_idx];
    let parts = split_top_level_commas(inner);
    let [sep_lit, col1] = parts.as_slice() else {
        return None;
    };
    let sep_lit = sep_lit.trim();
    if !is_quoted_literal(sep_lit) {
        return None;
    }
    let mut chars = sep_lit[1..sep_lit.len() - 1].chars();
    let sep_char = chars.next()?;
    if chars.next().is_some() {
        return None;
    }
    let col1 = col1.trim();
    if !is_bare_identifier(col1) {
        return None;
    }
    if lhs[close_idx + 1..].trim() != "[-1]" {
        return None;
    }
    if !is_bare_identifier(rhs) {
        return None;
    }
    Some(if is_eq {
        SpatialPredicate::LastSplitTokenEqColumn(col1.to_string(), sep_char, rhs.to_string())
    } else {
        SpatialPredicate::LastSplitTokenNeColumn(col1.to_string(), sep_char, rhs.to_string())
    })
}

fn parse_eq(clause: &str) -> Option<SpatialPredicate> {
    let (lhs, rhs) = clause.split_once('=')?;
    let lhs = lhs.trim();
    let rhs = rhs.trim();
    if !is_bare_identifier(lhs) || !is_quoted_literal(rhs) {
        return None;
    }
    Some(SpatialPredicate::Eq(
        lhs.to_string(),
        rhs[1..rhs.len() - 1].to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(pairs: &[(&'static str, &'static str)]) -> HashMap<&'static str, &'static str> {
        pairs.iter().copied().collect()
    }

    #[test]
    fn eq_ne_in_still_work() {
        assert!(is_filter_fully_enforceable("collector = 'rrc00'"));
        assert!(is_filter_fully_enforceable(
            "collector = 'rrc00' AND operation != 'W'"
        ));
        assert!(is_filter_fully_enforceable("peer_asn IN ('174', '3356')"));

        let l = labels(&[("collector", "rrc00"), ("operation", "A")]);
        assert!(evaluate_filter("collector = 'rrc00'", &l, 0).0);
        assert!(!evaluate_filter("collector = 'other'", &l, 0).0);
        assert!(evaluate_filter("operation != 'W'", &l, 0).0);
        assert!(evaluate_filter("collector IN ('rrc00', 'rrc01')", &l, 0).0);
    }

    #[test]
    fn starts_with() {
        assert!(is_filter_fully_enforceable("startsWith(prefix, '172.217.')"));
        let l = labels(&[("prefix", "172.217.1.0/24")]);
        assert!(evaluate_filter("startsWith(prefix, '172.217.')", &l, 0).0);
        let l2 = labels(&[("prefix", "10.0.0.0/24")]);
        assert!(!evaluate_filter("startsWith(prefix, '172.217.')", &l2, 0).0);
    }

    #[test]
    fn match_regex() {
        assert!(is_filter_fully_enforceable("match(communities, '[0-9]+:666')"));
        assert!(is_filter_fully_enforceable("NOT match(prefix, ':')"));
        let l = labels(&[("communities", "65535:666")]);
        assert!(evaluate_filter("match(communities, '[0-9]+:666')", &l, 0).0);
        let l2 = labels(&[("communities", "65535:667")]);
        assert!(!evaluate_filter("match(communities, '[0-9]+:666')", &l2, 0).0);

        let l3 = labels(&[("prefix", "10.0.0.0/24")]);
        assert!(evaluate_filter("NOT match(prefix, ':')", &l3, 0).0);
        let l4 = labels(&[("prefix", "2001:db8::/32")]);
        assert!(!evaluate_filter("NOT match(prefix, ':')", &l4, 0).0);
    }

    #[test]
    fn position_case_insensitive() {
        assert!(is_filter_fully_enforceable(
            "positionCaseInsensitive(communities, '65535:65281') > 0"
        ));
        assert!(is_filter_fully_enforceable(
            "positionCaseInsensitive(communities, '65535:65281') = 0"
        ));
        let l = labels(&[("communities", "1:2 65535:65281 3:4")]);
        assert!(
            evaluate_filter(
                "positionCaseInsensitive(communities, '65535:65281') > 0",
                &l,
                0
            )
            .0
        );
        assert!(
            !evaluate_filter(
                "positionCaseInsensitive(communities, '65535:65281') = 0",
                &l,
                0
            )
            .0
        );
    }

    #[test]
    fn has_split_member() {
        assert!(is_filter_fully_enforceable(
            "has(splitByChar(' ', as_path), '174')"
        ));
        let l = labels(&[("as_path", "3356 174 15169")]);
        assert!(evaluate_filter("has(splitByChar(' ', as_path), '174')", &l, 0).0);
        let l2 = labels(&[("as_path", "3356 1740 15169")]);
        assert!(!evaluate_filter("has(splitByChar(' ', as_path), '174')", &l2, 0).0);

        assert!(is_filter_fully_enforceable(
            "NOT has(splitByChar(' ', as_path), '174')"
        ));
        assert!(evaluate_filter("NOT has(splitByChar(' ', as_path), '174')", &l2, 0).0);
    }

    #[test]
    fn split_len_eq() {
        assert!(is_filter_fully_enforceable(
            "length(splitByChar(' ', as_path)) = 1"
        ));
        let l = labels(&[("as_path", "174")]);
        assert!(evaluate_filter("length(splitByChar(' ', as_path)) = 1", &l, 0).0);
        let l2 = labels(&[("as_path", "3356 174")]);
        assert!(!evaluate_filter("length(splitByChar(' ', as_path)) = 1", &l2, 0).0);
    }

    #[test]
    fn split_token_range() {
        // q045's actual condition.
        let cond = "toUInt8OrZero(splitByChar('/', prefix)[2]) >= 24";
        assert!(is_filter_fully_enforceable(cond));
        let specific = labels(&[("prefix", "10.0.0.0/24")]);
        let broad = labels(&[("prefix", "10.0.0.0/16")]);
        let malformed = labels(&[("prefix", "10.0.0.0")]);
        assert!(evaluate_filter(cond, &specific, 0).0);
        assert!(!evaluate_filter(cond, &broad, 0).0);
        assert!(!evaluate_filter(cond, &malformed, 0).0);

        assert!(is_filter_fully_enforceable(
            "toUInt8OrZero(splitByChar('/', prefix)[2]) < 24"
        ));
        assert!(!evaluate_filter(
            "toUInt8OrZero(splitByChar('/', prefix)[2]) < 24",
            &specific,
            0
        )
        .0);
        assert!(evaluate_filter(
            "toUInt8OrZero(splitByChar('/', prefix)[2]) < 24",
            &broad,
            0
        )
        .0);
    }

    #[test]
    fn split_len_range_ops() {
        // q119's actual condition.
        assert!(is_filter_fully_enforceable(
            "length(splitByChar(' ', communities)) >= 2"
        ));
        let one = labels(&[("communities", "65535:666")]);
        let two = labels(&[("communities", "65535:666 65535:667")]);
        assert!(!evaluate_filter("length(splitByChar(' ', communities)) >= 2", &one, 0).0);
        assert!(evaluate_filter("length(splitByChar(' ', communities)) >= 2", &two, 0).0);

        assert!(is_filter_fully_enforceable("length(splitByChar(' ', communities)) <= 1"));
        assert!(evaluate_filter("length(splitByChar(' ', communities)) <= 1", &one, 0).0);
        assert!(!evaluate_filter("length(splitByChar(' ', communities)) <= 1", &two, 0).0);

        assert!(is_filter_fully_enforceable("length(splitByChar(' ', communities)) > 1"));
        assert!(!evaluate_filter("length(splitByChar(' ', communities)) > 1", &one, 0).0);
        assert!(evaluate_filter("length(splitByChar(' ', communities)) > 1", &two, 0).0);

        assert!(is_filter_fully_enforceable("length(splitByChar(' ', communities)) < 2"));
        assert!(evaluate_filter("length(splitByChar(' ', communities)) < 2", &one, 0).0);
        assert!(!evaluate_filter("length(splitByChar(' ', communities)) < 2", &two, 0).0);
    }

    #[test]
    fn day_of_week_not_in() {
        // q100's actual condition.
        assert!(is_filter_fully_enforceable("toDayOfWeek(timestamp) NOT IN (6, 7)"));
        let l = labels(&[]);
        // 2024-01-20 was a Saturday (ISO day 6).
        let sat_ms: i64 = 1_705_708_654_000 + 24 * 3600 * 1000;
        assert!(!evaluate_filter("toDayOfWeek(timestamp) NOT IN (6, 7)", &l, sat_ms).0);
        // 2024-01-19 was a Friday (ISO day 5).
        let fri_ms: i64 = 1_705_708_654_000;
        assert!(evaluate_filter("toDayOfWeek(timestamp) NOT IN (6, 7)", &l, fri_ms).0);
    }

    #[test]
    fn nested_parenthesized_and_group_flattens() {
        // q200's actual shape: a countIf's own compound condition folded
        // into the surrogate WHERE clause as one parenthesized AND-group
        // alongside the base spatial filter.
        let cond = "collector = 'rrc00' AND (toString(local_pref) != '' AND \
                     toInt64OrNull(toString(local_pref)) IS NULL)";
        assert!(is_filter_fully_enforceable(cond));
        let l = labels(&[("collector", "rrc00"), ("local_pref", "abc")]);
        assert!(evaluate_filter(cond, &l, 0).0);
        let l2 = labels(&[("collector", "rrc00"), ("local_pref", "100")]);
        assert!(!evaluate_filter(cond, &l2, 0).0);
    }

    #[test]
    fn to_string_wrapper_and_int_parse() {
        assert!(is_filter_fully_enforceable("toString(local_pref) != ''"));
        assert!(is_filter_fully_enforceable(
            "toInt64OrNull(toString(local_pref)) IS NULL"
        ));
        let numeric = labels(&[("local_pref", "100")]);
        let non_numeric = labels(&[("local_pref", "abc")]);
        let empty = labels(&[("local_pref", "")]);

        assert!(evaluate_filter("toString(local_pref) != ''", &numeric, 0).0);
        assert!(!evaluate_filter("toString(local_pref) != ''", &empty, 0).0);

        assert!(!evaluate_filter(
            "toInt64OrNull(toString(local_pref)) IS NULL",
            &numeric,
            0
        )
        .0);
        assert!(evaluate_filter("toInt64OrNull(toString(local_pref)) IS NULL", &non_numeric, 0).0);

        // q200's actual compound condition: both clauses AND-ed.
        let cond = "toString(local_pref) != '' AND toInt64OrNull(toString(local_pref)) IS NULL";
        assert!(is_filter_fully_enforceable(cond));
        assert!(evaluate_filter(cond, &non_numeric, 0).0);
        assert!(!evaluate_filter(cond, &numeric, 0).0);
        assert!(!evaluate_filter(cond, &empty, 0).0);
    }

    #[test]
    fn hour_and_day_of_week_use_sample_timestamp() {
        assert!(is_filter_fully_enforceable("toHour(timestamp) < 12"));
        assert!(is_filter_fully_enforceable("toDayOfWeek(timestamp) IN (6, 7)"));

        // 2024-01-19 23:57:34 UTC - a Friday (ISO day 5), hour 23.
        let ts_ms: i64 = 1_705_708_654_000;
        let l = labels(&[]);
        assert!(!evaluate_filter("toHour(timestamp) < 12", &l, ts_ms).0);
        assert!(evaluate_filter("toHour(timestamp) >= 12", &l, ts_ms).0);
        assert!(!evaluate_filter("toDayOfWeek(timestamp) IN (6, 7)", &l, ts_ms).0);

        // 2024-01-20 was a Saturday (ISO day 6).
        let sat_ms: i64 = 1_705_708_654_000 + 24 * 3600 * 1000;
        assert!(evaluate_filter("toDayOfWeek(timestamp) IN (6, 7)", &l, sat_ms).0);
    }

    #[test]
    fn range_on_bare_label_numeric_and_string() {
        assert!(is_filter_fully_enforceable("local_pref >= '100'"));
        let l = labels(&[("local_pref", "150")]);
        assert!(evaluate_filter("local_pref >= '100'", &l, 0).0);
        let l2 = labels(&[("local_pref", "50")]);
        assert!(!evaluate_filter("local_pref >= '100'", &l2, 0).0);
    }

    #[test]
    fn timestamp_range_uses_sample_timestamp_not_a_label() {
        // "timestamp" is never a real label key - if this fell through to
        // the generic bare-label range comparison, get("timestamp") would
        // always be None and the clause would always evaluate false,
        // silently zeroing every countIf sub-window count (q159/q161's
        // actual shape: two adjacent 15-minute sub-windows compared via
        // countIf(timestamp >= ... AND timestamp < ...)).
        let cond = "timestamp >= '2024-01-10 12:00:00' AND timestamp < '2024-01-10 12:15:00'";
        assert!(is_filter_fully_enforceable(cond));

        let empty_labels = labels(&[]);
        // 2024-01-10 12:07:30 UTC - inside the window.
        let inside_ms: i64 = 1_704_888_450_000;
        assert!(evaluate_filter(cond, &empty_labels, inside_ms).0);
        // 2024-01-10 12:20:00 UTC - after the window.
        let after_ms: i64 = inside_ms + 13 * 60 * 1000;
        assert!(!evaluate_filter(cond, &empty_labels, after_ms).0);
        // 2024-01-10 11:59:00 UTC - before the window.
        let before_ms: i64 = inside_ms - 9 * 60 * 1000;
        assert!(!evaluate_filter(cond, &empty_labels, before_ms).0);
    }

    #[test]
    fn unsupported_shapes_stay_unsupported() {
        // OR composition - never split into safe AND-clauses.
        assert!(!is_filter_fully_enforceable(
            "startsWith(prefix, '10.') OR startsWith(prefix, '192.')"
        ));
        // column-to-column comparison, not a literal.
        assert!(!is_filter_fully_enforceable("has(splitByChar(' ', as_path), origin)"));
        // arrayExists lambda - not attempted.
        assert!(!is_filter_fully_enforceable(
            "arrayExists(x -> toUInt32OrZero(x) BETWEEN 1 AND 2, splitByChar(' ', as_path))"
        ));
        // unknown function entirely.
        assert!(!is_filter_fully_enforceable("someWeirdFunction(prefix, 'x')"));

        let (matches, unsupported) =
            evaluate_filter("someWeirdFunction(prefix, 'x')", &labels(&[]), 0);
        assert!(matches); // permissive fallback for the real ingest evaluator
        assert_eq!(unsupported, vec!["someWeirdFunction(prefix, 'x')".to_string()]);
    }

    #[test]
    fn parenthesized_clause_from_countif_folding() {
        // build_multi_aggregate_surrogates always wraps the folded
        // condition in parens.
        assert!(is_filter_fully_enforceable(
            "collector = 'rrc00' AND (startsWith(prefix, '10.'))"
        ));
        let l = labels(&[("collector", "rrc00"), ("prefix", "10.0.0.0/24")]);
        assert!(
            evaluate_filter("collector = 'rrc00' AND (startsWith(prefix, '10.'))", &l, 0).0
        );
    }

    #[test]
    fn is_empty_or_null_shape() {
        // q093's shape, either operand order.
        assert!(is_filter_fully_enforceable("(next_hop = '' OR next_hop IS NULL)"));
        assert!(is_filter_fully_enforceable("(next_hop IS NULL OR next_hop = '')"));
        assert!(is_filter_fully_enforceable(
            "collector = 'rrc00' AND (next_hop = '' OR next_hop IS NULL)"
        ));

        assert!(evaluate_filter("(next_hop = '' OR next_hop IS NULL)", &labels(&[("next_hop", "")]), 0).0);
        assert!(evaluate_filter("(next_hop = '' OR next_hop IS NULL)", &labels(&[]), 0).0);
        assert!(!evaluate_filter(
            "(next_hop = '' OR next_hop IS NULL)",
            &labels(&[("next_hop", "10.0.0.1")]),
            0
        ).0);
    }

    #[test]
    fn is_empty_or_null_rejects_mismatched_columns() {
        assert!(!is_filter_fully_enforceable("(next_hop = '' OR med IS NULL)"));
    }

    #[test]
    fn last_split_token_vs_column_shape() {
        // q069's shape, either operator.
        assert!(is_filter_fully_enforceable("splitByChar(' ', as_path)[-1] = origin"));
        assert!(is_filter_fully_enforceable("splitByChar(' ', as_path)[-1] != origin"));

        assert!(evaluate_filter(
            "splitByChar(' ', as_path)[-1] = origin",
            &labels(&[("as_path", "65001 65002 65003"), ("origin", "65003")]),
            0
        ).0);
        assert!(!evaluate_filter(
            "splitByChar(' ', as_path)[-1] = origin",
            &labels(&[("as_path", "65001 65002 65003"), ("origin", "65001")]),
            0
        ).0);
        assert!(evaluate_filter(
            "splitByChar(' ', as_path)[-1] != origin",
            &labels(&[("as_path", "65001 65002 65003"), ("origin", "65001")]),
            0
        ).0);
        assert!(!evaluate_filter(
            "splitByChar(' ', as_path)[-1] != origin",
            &labels(&[("as_path", "65001 65002 65003"), ("origin", "65003")]),
            0
        ).0);
    }

    #[test]
    fn last_split_token_vs_column_accepts_angle_bracket_ne() {
        // sqlparser's Display re-renders a parsed `!=` as `<>` - the exact
        // shape the classic parser hands back for q069's second countIf.
        assert!(is_filter_fully_enforceable("splitByChar(' ', as_path)[-1] <> origin"));
    }

    #[test]
    fn last_split_token_vs_column_rejects_literal_rhs() {
        // A quoted-literal RHS isn't a bare column, so this new parser
        // correctly declines it - and ordinary parse_eq/parse_ne also
        // decline since the LHS itself (`splitByChar(...)[-1]`) isn't a
        // bare identifier either, so the whole clause stays unenforceable
        // rather than being silently mismatched by either parser.
        assert!(parse_spatial_predicate("splitByChar(' ', as_path)[-1] = 'AS65003'").is_none());
    }

    #[test]
    fn general_or_still_unenforceable() {
        // Confirms the narrow fix didn't loosen the general OR gate - two
        // startsWith() calls joined by OR is neither this shape nor any
        // other recognized predicate (already covered above; this is an
        // explicit regression anchor for the new code path specifically).
        assert!(!is_filter_fully_enforceable(
            "(startsWith(prefix, '10.') OR startsWith(prefix, '192.'))"
        ));
    }
}
