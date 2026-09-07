//! Shared "complex SQL shape -> simplified surrogate query" detectors.
//!
//! ASAP's planner and query engine both need to recognize the same
//! ClickHouse-specific SQL shapes (CTE + window function, nested subqueries
//! with array functions) and rewrite them into a plain query the classic
//! SQLPatternParser/SQLPatternMatcher machinery already understands - the
//! planner does this once, offline, to decide what to build; the query
//! engine does it on every incoming request, to know what to serve. Before
//! this module existed, each pattern's detection logic was either
//! duplicated across both crates (risking silent drift - this is exactly
//! how a `<=`-vs-`<` truncation bug ended up fixed in one copy and not the
//! other) or only implemented on one side, leaving the other unable to
//! recognize the pattern at all. This is the single implementation both
//! crates call.
//!
//! Three patterns, three building blocks:
//!   - lag-transition: `lagInFrame(col) OVER (PARTITION BY ... ORDER BY ...)`
//!     wrapped in a CTE with an outer countIf -> a derived event stream a
//!     stateful-transition operator maintains at ingest time.
//!   - token-select: a nested subquery tokenizing a column with
//!     `arrayFilter(x -> match(x, regex), splitByWhitespace(col))`, then
//!     indexing the last token (`[-1]`) -> a computed label (`token_select`).
//!   - token-explode: the same tokenizer, but every token becomes its own
//!     row via `arrayJoin(...)` instead of indexing one -> a computed label
//!     (`token_explode`).

use super::sqlhelper::{OrderByItem, SQLSchema};
use super::sqlpattern_parser::SQLPatternParser;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser as SqlParser;

/// Finds the index of the `)` matching the `(` at `open_idx`, accounting for
/// nesting.
pub fn find_matching_close_paren(s: &str, open_idx: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    if bytes.get(open_idx) != Some(&b'(') {
        return None;
    }
    let mut depth = 0i32;
    for (i, &b) in bytes.iter().enumerate().skip(open_idx) {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

pub(crate) fn extract_paren_arg_after<'a>(sql: &'a str, marker_lower: &str) -> Option<&'a str> {
    let lower = sql.to_lowercase();
    let marker_idx = lower.find(marker_lower)?;
    let open_idx = marker_idx + marker_lower.len() - 1; // marker ends in "("
    let close_idx = find_matching_close_paren(sql, open_idx)?;
    Some(sql[open_idx + 1..close_idx].trim())
}

/// Exact-operator-match timestamp bound extraction: a naive
/// `starts_with("<")` also matches "<=", silently truncating an inclusive
/// bound to an exclusive one.
pub fn extract_ts_bound(sql: &str, op: &str) -> Option<String> {
    let lower = sql.to_lowercase();
    let mut search_start = 0usize;
    loop {
        let rel_idx = lower[search_start..].find("timestamp")?;
        let idx = search_start + rel_idx;
        let after = lower[idx + "timestamp".len()..].trim_start();
        let exact = after.starts_with(op) && !after[op.len()..].starts_with('=');
        if exact {
            let after_original = &sql[idx..];
            let q1 = after_original.find('\'')?;
            let rest = &after_original[q1 + 1..];
            let q2 = rest.find('\'')?;
            return Some(rest[..q2].to_string());
        }
        search_start = idx + "timestamp".len();
    }
}

// ---------------------------------------------------------------------------
// Lag-transition pattern
// ---------------------------------------------------------------------------

pub fn looks_like_lag_transition_sql(query: &str) -> bool {
    let q = query.to_lowercase();
    q.contains("laginframe(")
        && q.contains("partition by")
        && q.contains("countif(")
        && q.contains("group by")
}

fn extract_partition_by(sql: &str) -> Option<Vec<String>> {
    let lower = sql.to_lowercase();
    let start = lower.find("partition by")? + "partition by".len();
    let end_rel = lower[start..].find("order by")?;
    let cols = &sql[start..start + end_rel];
    let out: Vec<String> = cols
        .split(',')
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty())
        .collect();
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn extract_laginframe_state_column(sql: &str) -> Option<String> {
    let arg = extract_paren_arg_after(sql, "laginframe(")?;
    // lagInFrame(col) or lagInFrame(col, offset, default) - only the
    // single-column, default-offset form is supported (matches the
    // detection guard: offset isn't checked, so only bare `col` is safe).
    let col = arg.split(',').next()?.trim();
    if col.is_empty() {
        None
    } else {
        Some(col.to_string())
    }
}

/// The alias immediately after the `OVER (...)` clause closes, e.g.
/// `lagInFrame(as_path) OVER (...) AS previous_path` -> "previous_path".
fn extract_over_alias(sql: &str) -> Option<String> {
    let lower = sql.to_lowercase();
    let over_idx = lower.find("over")?;
    let paren_rel = lower[over_idx..].find('(')?;
    let open_idx = over_idx + paren_rel;
    let close_idx = find_matching_close_paren(sql, open_idx)?;
    let after = &sql[close_idx + 1..];
    let after_lower = after.to_lowercase();
    let as_idx = after_lower.find("as ")?;
    if !after[..as_idx].trim().is_empty() {
        return None;
    }
    let rest = after[as_idx + 3..].trim_start();
    let alias: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if alias.is_empty() {
        None
    } else {
        Some(alias)
    }
}

/// The raw countIf(...) predicate in the OUTER query, e.g.
/// `countIf(previous_path != '' AND previous_path != as_path) AS path_changes`
/// -> "previous_path != '' AND previous_path != as_path".
fn extract_outer_countif_predicate(sql: &str) -> Option<String> {
    let lower = sql.to_lowercase();
    let idx = lower.rfind("countif(")?;
    let open_idx = idx + "countif(".len() - 1;
    let close_idx = find_matching_close_paren(sql, open_idx)?;
    Some(sql[open_idx + 1..close_idx].trim().to_string())
}

#[derive(Debug, Clone)]
pub struct LagTransitionMatch {
    pub partition_by: Vec<String>,
    pub state_column: String,
    pub previous_alias: String,
    pub predicate: String,
    pub group_label: String,
    pub alias: String,
    pub start: String,
    pub end: String,
    pub limit: String,
}

impl LagTransitionMatch {
    pub fn derived_metric(&self) -> String {
        format!("derived_lag_transition_{}", self.alias)
    }
}

pub fn parse_lag_transition_query(query: &str) -> Option<LagTransitionMatch> {
    let partition_by = extract_partition_by(query)?;
    let state_column = extract_laginframe_state_column(query)?;
    let previous_alias = extract_over_alias(query)?;
    let predicate = extract_outer_countif_predicate(query)?;

    let lower = query.to_lowercase();
    let group_idx = lower.rfind("group by")? + "group by".len();
    let group_label = query[group_idx..]
        .split_whitespace()
        .next()?
        .trim()
        .trim_end_matches(',')
        .to_string();

    let countif_idx = lower.rfind("countif(")?;
    let open_idx = countif_idx + "countif(".len() - 1;
    let close_idx = find_matching_close_paren(query, open_idx)?;
    let after_countif = &query[close_idx + 1..];
    let after_countif_lower = after_countif.to_lowercase();
    let as_idx = after_countif_lower.find(" as ")?;
    let alias_part = &after_countif[as_idx + 4..];
    let alias = alias_part
        .split(|c: char| c.is_whitespace() || c == ',' || c == '\n')
        .find(|s| !s.trim().is_empty())?
        .trim()
        .to_string();

    let start = extract_ts_bound(query, ">=")?;
    let end = extract_ts_bound(query, "<")?;

    let limit = lower
        .rfind("limit")
        .map(|i| &query[i + "limit".len()..])
        .and_then(|s| s.split_whitespace().next())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "100".to_string());

    Some(LagTransitionMatch {
        partition_by,
        state_column,
        previous_alias,
        predicate,
        group_label,
        alias,
        start,
        end,
        limit,
    })
}

/// The plain-aggregation surrogate this pattern lowers to. Must stay
/// byte-for-byte identical regardless of caller (planner or engine): the
/// query-time matcher parses whatever inference_config.yaml registered as
/// the template and compares its *structured* form against the structured
/// form of whatever the engine rewrites an incoming request to - an
/// ORDER BY tie-break present on one side and not the other is enough to
/// make that match fail even though both queries are equivalent.
pub fn build_lag_transition_surrogate(m: &LagTransitionMatch) -> String {
    format!(
        "SELECT\n    {group_label},\n    count() AS {alias}\nFROM {metric}\nWHERE timestamp >= '{start}'\n  AND timestamp <  '{end}'\nGROUP BY {group_label}\nORDER BY {alias} DESC, {group_label} ASC\nLIMIT {limit}",
        group_label = m.group_label,
        alias = m.alias,
        metric = m.derived_metric(),
        start = m.start,
        end = m.end,
        limit = m.limit,
    )
}

/// Detects and rewrites in one call - what the query engine needs at serve
/// time. The planner needs the structured `LagTransitionMatch` too (to build
/// a StatefulTransitionConfig), so it calls `parse_lag_transition_query` +
/// `build_lag_transition_surrogate` directly instead of this.
pub fn rewrite_lag_transition_query(query: &str) -> Option<String> {
    if !looks_like_lag_transition_sql(query) {
        return None;
    }
    let m = parse_lag_transition_query(query)?;
    Some(build_lag_transition_surrogate(&m))
}

// ---------------------------------------------------------------------------
// Lag-gap pattern: `dateDiff('second', lagInFrame(<col>) OVER (PARTITION BY
// <p> ORDER BY <col>), <col>) AS <gap_alias>` - the "gap between
// consecutive rows" shape, e.g.
//   SELECT peer_ip, quantile(0.5)(gap) AS median_gap_seconds
//   FROM (
//     SELECT peer_ip, timestamp,
//            dateDiff('second', lagInFrame(timestamp) OVER (PARTITION BY peer_ip ORDER BY timestamp), timestamp) AS gap
//     FROM ... WHERE ...
//   )
//   WHERE gap IS NOT NULL AND gap > 0
//   GROUP BY peer_ip ORDER BY median_gap_seconds LIMIT 30
// Unlike the boolean-predicate lag-transition above (which counts EVENTS
// where a condition holds), this always emits a NUMERIC value once a
// previous row exists - lowered via StatefulTransitionConfig's gap_unit
// mode (see asap_types::stateful_transition) into a derived metric stream
// the classic single-aggregate path can quantile()/avg()/max() over like
// any other value column.
// ---------------------------------------------------------------------------

pub fn looks_like_lag_gap_sql(query: &str) -> bool {
    let q = query.to_lowercase();
    q.contains("laginframe(") && q.contains("datediff(") && q.contains("partition by")
}

#[derive(Debug, Clone)]
pub struct LagGapMatch {
    pub partition_col: String,
    pub state_column: String,
    pub gap_alias: String,
    pub gap_unit: String,
    pub min_gap: Option<f64>,
    pub outer_agg_expr: String,
    pub outer_agg_alias: String,
    pub start: String,
    pub end: String,
    pub order_by_and_limit: String,
}

impl LagGapMatch {
    pub fn derived_metric(&self) -> String {
        format!("derived_lag_gap_{}", self.gap_alias)
    }
}

pub fn parse_lag_gap_query(query: &str) -> Option<LagGapMatch> {
    let lower = query.to_lowercase();

    // --- Locate and parse the dateDiff(...) call ---
    let dd_idx = lower.find("datediff(")?;
    let dd_open = dd_idx + "datediff(".len() - 1;
    let dd_close = find_matching_close_paren(query, dd_open)?;
    let dd_args = split_top_level_commas(&query[dd_open + 1..dd_close]);
    let [unit_lit, lag_expr, curr_expr] = dd_args.as_slice() else {
        return None;
    };
    let unit_lit = unit_lit.trim();
    if !is_quoted_literal(unit_lit) {
        return None;
    }
    let gap_unit = unit_lit[1..unit_lit.len() - 1].to_string();
    let curr_expr = curr_expr.trim();
    if !is_bare_identifier(curr_expr) {
        return None;
    }
    let state_column = curr_expr.to_string();

    let lag_expr = lag_expr.trim();
    let lag_expr_lower = lag_expr.to_lowercase();
    if !lag_expr_lower.starts_with("laginframe(") {
        return None;
    }
    let lag_close = find_matching_close_paren(lag_expr, "laginframe(".len() - 1)?;
    let lag_col = lag_expr["laginframe(".len()..lag_close].trim();
    if lag_col != state_column {
        return None; // lagInFrame must remember the same column dateDiff diffs against
    }
    let after_lag = lag_expr[lag_close + 1..].trim_start();
    let after_lag_lower = after_lag.to_lowercase();
    if !after_lag_lower.starts_with("over") {
        return None;
    }
    let over_paren = after_lag.find('(')?;
    if !after_lag[..over_paren].trim().eq_ignore_ascii_case("over") {
        return None;
    }
    let over_close = find_matching_close_paren(after_lag, over_paren)?;
    if !after_lag[over_close + 1..].trim().is_empty() {
        return None; // nothing may follow OVER(...) inside the lagInFrame arg
    }
    let over_inner = after_lag[over_paren + 1..over_close].trim();
    let over_inner_lower = over_inner.to_lowercase();
    let pb_idx = over_inner_lower.find("partition by")?;
    if !over_inner[..pb_idx].trim().is_empty() {
        return None;
    }
    let ob_idx = over_inner_lower.find("order by")?;
    let partition_col = over_inner[pb_idx + "partition by".len()..ob_idx].trim();
    if !is_bare_identifier(partition_col) {
        return None;
    }
    let order_col = over_inner[ob_idx + "order by".len()..].trim();
    if order_col != state_column {
        return None; // "gap between consecutive rows" needs ordering by the diffed column itself
    }

    // --- Alias for the dateDiff(...) call ---
    let after_dd = query[dd_close + 1..].trim_start();
    let after_dd_lower = after_dd.to_lowercase();
    let as_idx = after_dd_lower.find("as ")?;
    if !after_dd[..as_idx].trim().is_empty() {
        return None;
    }
    let gap_alias: String = after_dd[as_idx + 3..]
        .trim_start()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if gap_alias.is_empty() {
        return None;
    }

    // --- Inner FROM/WHERE time bounds ---
    let inner_lower = lower[..dd_idx].to_string() + &lower[dd_idx..];
    let inner_from_idx = inner_lower.find("from")?;
    let start = extract_ts_bound(&query[inner_from_idx..], ">=")?;
    let end = extract_ts_bound(&query[inner_from_idx..], "<")?;

    // --- Outer query: SELECT <partition_col>, <agg_expr> AS <agg_alias> ...
    // FROM (<inner>) [WHERE <gap_alias> ... > <n> ...] GROUP BY <partition_col>
    // [ORDER BY ...] [LIMIT ...] ---
    let select_end = lower.find("select")? + "select".len();
    let outer_from_idx = lower.find("from")?;
    if select_end > outer_from_idx {
        return None;
    }
    let outer_items = split_top_level_commas(&query[select_end..outer_from_idx]);
    let [group_item, agg_item] = outer_items.as_slice() else {
        return None; // exactly the partition column and one aggregate over the gap
    };
    let group_item = group_item.trim();
    if group_item != partition_col {
        return None;
    }
    let (outer_agg_expr, outer_agg_alias) = split_expr_alias(agg_item.trim())?;
    if !outer_agg_expr.contains(gap_alias.as_str()) {
        return None; // the outer aggregate must actually reference the gap
    }

    let after_outer_from = &query[outer_from_idx + "from".len()..];
    let after_outer_from_trim = after_outer_from.trim_start();
    if !after_outer_from_trim.starts_with('(') {
        return None;
    }
    let inner_close_idx = find_matching_close_paren(after_outer_from_trim, 0)?;
    let after_inner_close = after_outer_from_trim[inner_close_idx + 1..].trim_start();
    let after_inner_close_lower = after_inner_close.to_lowercase();

    let min_gap = if after_inner_close_lower.starts_with("where") {
        let where_text_end = after_inner_close_lower
            .find("group by")
            .unwrap_or(after_inner_close.len());
        let where_text = &after_inner_close[..where_text_end];
        // Permissive: look for "<gap_alias> > <number>" anywhere in the
        // WHERE clause (alongside e.g. a no-op "IS NOT NULL" guard our
        // always-emit-once-previous-exists design already satisfies).
        let needle = format!("{gap_alias} >");
        where_text.find(&needle).and_then(|i| {
            where_text[i + needle.len()..]
                .trim_start()
                .split_whitespace()
                .next()?
                .parse::<f64>()
                .ok()
        })
    } else {
        None
    };

    let group_idx_lower = after_inner_close_lower.find("group by")?;
    let after_group = &after_inner_close[group_idx_lower + "group by".len()..];
    let after_group_lower = after_group.to_lowercase();
    let group_end = after_group_lower
        .find("order by")
        .or_else(|| after_group_lower.find("limit"))
        .unwrap_or(after_group.len());
    let group_by_col = after_group[..group_end].trim();
    if group_by_col != partition_col {
        return None;
    }
    let order_by_and_limit = after_group[group_end..].trim().trim_end_matches(';').to_string();

    let (order_by_items, _) = parse_order_by_and_limit(&order_by_and_limit);
    let known = [partition_col, outer_agg_alias.as_str()];
    if order_by_items
        .iter()
        .any(|item| !known.contains(&item.column.as_str()))
    {
        return None;
    }

    Some(LagGapMatch {
        partition_col: partition_col.to_string(),
        state_column,
        gap_alias,
        gap_unit,
        min_gap,
        outer_agg_expr,
        outer_agg_alias,
        start,
        end,
        order_by_and_limit,
    })
}

/// The plain-aggregation surrogate this pattern lowers to, over the
/// derived gap metric stream - same "FROM <derived_metric> WHERE time
/// bounds GROUP BY <partition>" shape build_lag_transition_surrogate uses,
/// just with the caller's own outer aggregate instead of a hardcoded
/// count(). Must stay byte-for-byte identical regardless of caller (see
/// build_lag_transition_surrogate's own note - the same reasoning applies).
pub fn build_lag_gap_surrogate(m: &LagGapMatch) -> String {
    format!(
        "SELECT\n    {partition_col},\n    {agg_expr} AS {agg_alias}\nFROM {metric}\nWHERE timestamp >= '{start}'\n  AND timestamp <  '{end}'\nGROUP BY {partition_col}\n{tail}",
        partition_col = m.partition_col,
        agg_expr = m.outer_agg_expr,
        agg_alias = m.outer_agg_alias,
        metric = m.derived_metric(),
        start = m.start,
        end = m.end,
        tail = m.order_by_and_limit,
    )
}

pub fn rewrite_lag_gap_query(query: &str) -> Option<String> {
    if !looks_like_lag_gap_sql(query) {
        return None;
    }
    let m = parse_lag_gap_query(query)?;
    Some(build_lag_gap_surrogate(&m))
}

// ---------------------------------------------------------------------------
// Token-select / token-explode patterns: a nested subquery that tokenizes a
// space-separated column with `arrayFilter(x -> match(x, '<regex>'),
// splitByWhitespace(col))`, then either indexes the last token (`[-1]`,
// token-select) or explodes every token into its own row (`arrayJoin(...)`,
// token-explode).
// ---------------------------------------------------------------------------

struct TokenExtraction {
    source_col: String,
    filter_regex: String,
    inner_alias: String,
}

/// Extracts just the tokenizer inputs from `arrayFilter(x -> match(x,
/// '<regex>'), splitByWhitespace(col))`, without requiring an alias
/// immediately after - arrayFilter is aliased directly in the token-select
/// shape (`... AS as_path_array`), but nested unaliased inside arrayJoin(...)
/// in the token-explode shape, so the alias step has to be optional here and
/// handled separately by each caller.
fn extract_regex_and_source_col(query: &str) -> Option<(String, String, usize)> {
    let lower = query.to_lowercase();
    let af_idx = lower.find("arrayfilter(")?;
    let open_idx = af_idx + "arrayfilter(".len() - 1;
    let close_idx = find_matching_close_paren(query, open_idx)?;
    let inner = &query[open_idx + 1..close_idx];
    let inner_lower = inner.to_lowercase();

    let match_idx = inner_lower.find("match(")?;
    let match_open = match_idx + "match(".len() - 1;
    let match_close = find_matching_close_paren(inner, match_open)?;
    let match_args = &inner[match_open + 1..match_close];
    let q1 = match_args.find('\'')?;
    let rest = &match_args[q1 + 1..];
    let q2 = rest.find('\'')?;
    let filter_regex = rest[..q2].to_string();

    let source_col = extract_paren_arg_after(inner, "splitbywhitespace(")?.to_string();

    Some((source_col, filter_regex, close_idx))
}

fn extract_token_filter(query: &str) -> Option<TokenExtraction> {
    let (source_col, filter_regex, close_idx) = extract_regex_and_source_col(query)?;

    let after = &query[close_idx + 1..];
    let after_lower = after.to_lowercase();
    let as_idx = after_lower.find("as ")?;
    if !after[..as_idx].trim().is_empty() {
        return None;
    }
    let rest = after[as_idx + 3..].trim_start();
    let inner_alias: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if inner_alias.is_empty() {
        return None;
    }

    Some(TokenExtraction {
        source_col,
        filter_regex,
        inner_alias,
    })
}

/// The raw WHERE clause of the *inner* subquery (the real time/spatial
/// filters), stopping before the outer query's synthetic
/// `WHERE length(...) > 0` guard - that guard is exactly what
/// `on_missing: skip_sample` already means at ingest time.
fn extract_inner_where(query: &str, subquery_close_idx: usize) -> Option<String> {
    let inner_text = &query[..subquery_close_idx];
    let lower = inner_text.to_lowercase();
    let where_idx = lower.rfind("where")?;
    let after_where = inner_text[where_idx + "where".len()..].trim();
    Some(after_where.trim_end().to_string())
}

/// The inner subquery's own FROM target (e.g. "bgp.bgp_updates"), so the
/// surrogate references the same base table rather than assuming a fixed
/// name. Scoped strictly to the subquery body - taking the first "from"
/// from the start of the whole query would find the *outer* query's
/// "FROM (" instead.
fn extract_inner_from(query: &str, subquery_open_idx: usize, subquery_close_idx: usize) -> Option<String> {
    let inner_text = &query[subquery_open_idx + 1..subquery_close_idx];
    let lower = inner_text.to_lowercase();
    let from_idx = lower.find("from")?;
    let after_from = &inner_text[from_idx + "from".len()..];
    let where_idx = after_from.to_lowercase().find("where")?;
    Some(after_from[..where_idx].trim().to_string())
}

pub fn looks_like_token_select_sql(query: &str) -> bool {
    let q = query.to_lowercase();
    q.contains("arrayfilter(")
        && q.contains("splitbywhitespace(")
        && q.contains("match(")
        && q.contains("[-1]")
        && q.contains("group by")
}

#[derive(Debug, Clone)]
pub struct TokenSelectMatch {
    pub label: String,
    pub source_col: String,
    pub filter_regex: String,
    /// The outer SELECT's non-label item, e.g. "count() AS x".
    pub select_expr: String,
    pub from_target: String,
    pub where_clause: String,
    pub group_by: String,
    pub order_by_and_limit: String,
}

pub fn parse_token_select_query(query: &str) -> Option<TokenSelectMatch> {
    let tok = extract_token_filter(query)?;

    let lower = query.to_lowercase();
    let from_idx = lower.find("from")?;
    let open_paren_rel = lower[from_idx..].find('(')?;
    let subquery_open = from_idx + open_paren_rel;
    let subquery_close = find_matching_close_paren(query, subquery_open)?;

    let outer_select = &query[..from_idx];
    let outer_tail = &query[subquery_close + 1..];
    let outer_tail_lower = outer_tail.to_lowercase();

    let index_marker = format!("{}[-1]", tok.inner_alias);
    let index_pos = outer_select.find(&index_marker)?;
    let after_index = &outer_select[index_pos + index_marker.len()..];
    let after_index_lower = after_index.to_lowercase();
    let as_idx = after_index_lower.find("as ")?;
    let after_as = after_index[as_idx + 3..].trim_start();
    let label: String = after_as
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if label.is_empty() {
        return None;
    }
    let comma_idx = after_as.find(',')?;
    let select_expr = after_as[comma_idx + 1..].trim().to_string();

    let where_clause = extract_inner_where(query, subquery_close)?;
    let from_target = extract_inner_from(query, subquery_open, subquery_close)?;

    let group_idx = outer_tail_lower.find("group by")? + "group by".len();
    let after_group = &outer_tail[group_idx..];
    let after_group_lower = after_group.to_lowercase();
    let group_end = after_group_lower
        .find("order by")
        .or_else(|| after_group_lower.find("limit"))
        .unwrap_or(after_group.len());
    let group_by = after_group[..group_end].trim().trim_end_matches(',').to_string();

    let order_start = after_group_lower.find("order by").unwrap_or(group_end);
    let order_by_and_limit = after_group[order_start..].trim().to_string();

    Some(TokenSelectMatch {
        label,
        source_col: tok.source_col,
        filter_regex: tok.filter_regex,
        select_expr,
        from_target,
        where_clause,
        group_by,
        order_by_and_limit,
    })
}

pub fn build_token_select_surrogate(m: &TokenSelectMatch) -> String {
    format!(
        "SELECT {label}, {select_expr} FROM {from_target} WHERE {where_clause} GROUP BY {group_by} {order_by_and_limit}",
        label = m.label,
        select_expr = m.select_expr,
        from_target = m.from_target,
        where_clause = m.where_clause,
        group_by = m.group_by,
        order_by_and_limit = m.order_by_and_limit,
    )
}

pub fn rewrite_token_select_query(query: &str) -> Option<String> {
    if !looks_like_token_select_sql(query) {
        return None;
    }
    let m = parse_token_select_query(query)?;
    Some(build_token_select_surrogate(&m))
}

pub fn looks_like_token_explode_sql(query: &str) -> bool {
    let q = query.to_lowercase();
    q.contains("arrayjoin(")
        && q.contains("arrayfilter(")
        && q.contains("splitbywhitespace(")
        && q.contains("match(")
        && q.contains("group by")
}

#[derive(Debug, Clone)]
pub struct TokenExplodeMatch {
    pub label: String,
    pub source_col: String,
    pub filter_regex: String,
    pub select_expr: String,
    pub from_target: String,
    pub where_clause: String,
    pub group_by: String,
    pub order_by_and_limit: String,
}

pub fn parse_token_explode_query(query: &str) -> Option<TokenExplodeMatch> {
    let lower = query.to_lowercase();
    let aj_idx = lower.find("arrayjoin(")?;
    let aj_open = aj_idx + "arrayjoin(".len() - 1;
    let aj_close = find_matching_close_paren(query, aj_open)?;

    let aj_inner = &query[aj_open + 1..aj_close];
    let (source_col, filter_regex, _) = extract_regex_and_source_col(aj_inner)?;

    let after = &query[aj_close + 1..];
    let after_lower = after.to_lowercase();
    let as_idx = after_lower.find("as ")?;
    if !after[..as_idx].trim().is_empty() {
        return None;
    }
    let rest = after[as_idx + 3..].trim_start();
    let label: String = rest
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if label.is_empty() {
        return None;
    }

    let from_idx = lower.find("from")?;
    let open_paren_rel = lower[from_idx..].find('(')?;
    let subquery_open = from_idx + open_paren_rel;
    let subquery_close = find_matching_close_paren(query, subquery_open)?;
    if aj_close > subquery_close {
        return None;
    }

    let outer_select = &query[..from_idx];
    let outer_tail = &query[subquery_close + 1..];
    let outer_tail_lower = outer_tail.to_lowercase();

    let label_pos = outer_select.find(label.as_str())?;
    let after_label = &outer_select[label_pos + label.len()..];
    let comma_idx = after_label.find(',')?;
    let select_expr = after_label[comma_idx + 1..].trim().to_string();

    let where_clause = extract_inner_where(query, subquery_close)?;
    let from_target = extract_inner_from(query, subquery_open, subquery_close)?;

    let group_idx = outer_tail_lower.find("group by")? + "group by".len();
    let after_group = &outer_tail[group_idx..];
    let after_group_lower = after_group.to_lowercase();
    let group_end = after_group_lower
        .find("order by")
        .or_else(|| after_group_lower.find("limit"))
        .unwrap_or(after_group.len());
    let group_by = after_group[..group_end].trim().trim_end_matches(',').to_string();

    let order_start = after_group_lower.find("order by").unwrap_or(group_end);
    let order_by_and_limit = after_group[order_start..].trim().to_string();

    Some(TokenExplodeMatch {
        label,
        source_col,
        filter_regex,
        select_expr,
        from_target,
        where_clause,
        group_by,
        order_by_and_limit,
    })
}

pub fn build_token_explode_surrogate(m: &TokenExplodeMatch) -> String {
    format!(
        "SELECT {label}, {select_expr} FROM {from_target} WHERE {where_clause} GROUP BY {group_by} {order_by_and_limit}",
        label = m.label,
        select_expr = m.select_expr,
        from_target = m.from_target,
        where_clause = m.where_clause,
        group_by = m.group_by,
        order_by_and_limit = m.order_by_and_limit,
    )
}

pub fn rewrite_token_explode_query(query: &str) -> Option<String> {
    if !looks_like_token_explode_sql(query) {
        return None;
    }
    let m = parse_token_explode_query(query)?;
    Some(build_token_explode_surrogate(&m))
}

// ---------------------------------------------------------------------------
// Flat token-explode: `arrayJoin(splitByChar(<sep>, <col>)) AS <label>`
// directly in a top-level SELECT list (no subquery, no arrayFilter/regex
// condition), e.g.
//   SELECT arrayJoin(splitByChar(' ', as_path)) AS asn, count(*) AS cnt
//   FROM ... WHERE ... GROUP BY asn ORDER BY cnt DESC LIMIT 40
// A strict subset of the nested-subquery + regex-filtered shape above -
// same "one row explodes into N" semantics (and the same underlying
// ingest-time token_explode ComputedLabelConfig type, whose filter_regex
// is simply omitted here), just without the subquery wrapper or a
// filtering condition. Produces the same TokenExplodeMatch the subquery
// shape does, so it reuses every downstream builder/planner/serve-time
// function unchanged - see get_token_explode_streaming_aggregation_configs's
// `parse_token_explode_query(...).or_else(|| parse_flat_token_explode_query(...))`
// fallback.
// ---------------------------------------------------------------------------

pub fn looks_like_flat_token_explode_sql(query: &str) -> bool {
    let q = query.to_lowercase();
    q.contains("arrayjoin(")
        && q.contains("splitbychar(")
        && q.contains("group by")
        && !q.contains("arrayfilter(")
}

pub fn parse_flat_token_explode_query(query: &str) -> Option<TokenExplodeMatch> {
    let lower = query.to_lowercase();
    let select_kw_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_kw_end > from_idx {
        return None;
    }
    let after_from = query[from_idx + "from".len()..].trim_start();
    if after_from.starts_with('(') {
        return None; // the nested-subquery shape belongs to the other detector
    }
    if lower[from_idx..].matches("select").count() > 0 {
        return None;
    }

    let items = split_top_level_commas(&query[select_kw_end..from_idx]);
    let (first, rest) = items.split_first()?;
    if rest.is_empty() {
        return None; // needs at least one real aggregate alongside the exploded label
    }
    let (explode_expr, label) = split_expr_alias(first.trim())?;

    let explode_expr = explode_expr.trim();
    let explode_lower = explode_expr.to_lowercase();
    if !explode_lower.starts_with("arrayjoin(") {
        return None;
    }
    let aj_close = find_matching_close_paren(explode_expr, "arrayjoin(".len() - 1)?;
    if aj_close != explode_expr.len() - 1 {
        return None; // nothing may follow arrayJoin(...) itself
    }
    let aj_inner = explode_expr["arrayjoin(".len()..aj_close].trim();
    let aj_inner_lower = aj_inner.to_lowercase();
    if !aj_inner_lower.starts_with("splitbychar(") {
        return None;
    }
    let sbc_close = find_matching_close_paren(aj_inner, "splitbychar(".len() - 1)?;
    if sbc_close != aj_inner.len() - 1 {
        return None;
    }
    let sbc_args = split_top_level_commas(&aj_inner["splitbychar(".len()..sbc_close]);
    let [sep_lit, source_col] = sbc_args.as_slice() else {
        return None;
    };
    let sep_lit = sep_lit.trim();
    if !is_quoted_literal(sep_lit) {
        return None;
    }
    let source_col = source_col.trim();
    if !is_bare_identifier(source_col) {
        return None;
    }

    let select_expr = rest
        .iter()
        .map(|s| s.trim())
        .collect::<Vec<_>>()
        .join(", ");

    let group_idx = lower.find("group by")?;
    let where_clause = query[from_idx + "from".len()..group_idx]
        .trim()
        .to_string();
    // where_clause currently holds "<table> WHERE <clause>" (or just
    // "<table>") - split the table name off, matching TokenExplodeMatch's
    // existing from_target/where_clause split (both get interpolated
    // separately into build_token_explode_surrogate's "FROM {from_target}
    // WHERE {where_clause}" template).
    let where_lower = where_clause.to_lowercase();
    let (from_target, where_clause) = match where_lower.find("where") {
        Some(w_idx) => (
            where_clause[..w_idx].trim().to_string(),
            where_clause[w_idx + "where".len()..].trim().to_string(),
        ),
        None => (where_clause, String::new()),
    };
    if from_target.is_empty() {
        return None;
    }

    let after_group = &query[group_idx + "group by".len()..];
    let after_group_lower = after_group.to_lowercase();
    let group_end = after_group_lower
        .find("order by")
        .or_else(|| after_group_lower.find("limit"))
        .unwrap_or(after_group.len());
    let group_by = after_group[..group_end].trim().trim_end_matches(';').to_string();
    if group_by != label {
        return None; // must group by exactly the exploded label, nothing else
    }
    let order_by_and_limit = after_group[group_end..]
        .trim()
        .trim_end_matches(';')
        .to_string();

    Some(TokenExplodeMatch {
        label,
        source_col: source_col.to_string(),
        filter_regex: String::new(),
        select_expr,
        from_target,
        where_clause,
        group_by,
        order_by_and_limit,
    })
}

pub fn rewrite_flat_token_explode_query(query: &str) -> Option<String> {
    if !looks_like_flat_token_explode_sql(query) {
        return None;
    }
    let m = parse_flat_token_explode_query(query)?;
    Some(build_token_explode_surrogate(&m))
}

// ---------------------------------------------------------------------------
// arrayZip edge-explode: `arrayJoin(arrayZip(<slice1>, <slice2>)) AS <label>`
// where `<slice1>`/`<slice2>` are two adjacent, overlapping
// `arraySlice(splitByChar(<sep>, <col>), ...)` windows over the same
// column's split tokens - e.g.
//   SELECT arrayJoin(arrayZip(
//            arraySlice(splitByChar(' ', as_path), 1, length(splitByChar(' ', as_path)) - 1),
//            arraySlice(splitByChar(' ', as_path), 2, length(splitByChar(' ', as_path)) - 1)
//          )) AS as_edge, count(*) AS cnt
//   FROM ... WHERE ... GROUP BY as_edge ORDER BY cnt DESC LIMIT 50
// (q067's AS-path adjacency/"edge" extraction: tokens [A,B,C] explode into
// rows for the pairs (A,B) and (B,C)). Same "one row explodes into N" shape
// as flat token-explode above - reuses TokenExplodeMatch and
// build_token_explode_surrogate unchanged, since the registered surrogate
// (`SELECT <label>, <rest> FROM ... GROUP BY <label> ...`) is identical
// once the label is a real ingested column; only the ComputedLabelConfig
// *type* registered from it differs (token_pair_explode, not
// token_explode) - see get_arrayzip_edge_explode_streaming_aggregation_configs
// in asap-planner-rs.
// ---------------------------------------------------------------------------

pub fn looks_like_arrayzip_edge_explode_sql(query: &str) -> bool {
    let q = query.to_lowercase();
    q.contains("arrayjoin(") && q.contains("arrayzip(") && q.contains("group by")
}

/// Parses one `arraySlice(splitByChar('<sep>', <col>), <start>, <len_expr>)`
/// arrayZip argument, requiring `start == expected_start` and `len_expr` to
/// be exactly `length(splitByChar('<sep>', <col>)) - 1` (textually, using
/// the SAME sep/col just parsed, whitespace-tolerant) - the "every token
/// but the first/last" window that, zipped together, produces adjacent
/// pairs. Narrow by design: any other length expression isn't this shape.
fn parse_edge_array_slice_arg(expr: &str, expected_start: usize) -> Option<(char, String)> {
    let lower = expr.to_lowercase();
    if !lower.starts_with("arrayslice(") {
        return None;
    }
    let close_idx = find_matching_close_paren(expr, "arrayslice(".len() - 1)?;
    if close_idx != expr.len() - 1 {
        return None;
    }
    let inner = &expr["arrayslice(".len()..close_idx];
    let args = split_top_level_commas(inner);
    let [split_expr, start_lit, len_expr] = args.as_slice() else {
        return None;
    };
    let split_expr = split_expr.trim();
    let split_lower = split_expr.to_lowercase();
    if !split_lower.starts_with("splitbychar(") {
        return None;
    }
    let sc_close = find_matching_close_paren(split_expr, "splitbychar(".len() - 1)?;
    if sc_close != split_expr.len() - 1 {
        return None;
    }
    let sbc_args = split_top_level_commas(&split_expr["splitbychar(".len()..sc_close]);
    let [sep_lit, col] = sbc_args.as_slice() else {
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
    let col = col.trim();
    if !is_bare_identifier(col) {
        return None;
    }

    let start: usize = start_lit.trim().parse().ok()?;
    if start != expected_start {
        return None;
    }

    let expected_len_expr = format!("length(splitByChar('{sep_char}', {col})) - 1");
    let normalize = |s: &str| s.chars().filter(|c| !c.is_whitespace()).collect::<String>();
    if normalize(len_expr.trim()) != normalize(&expected_len_expr) {
        return None;
    }

    Some((sep_char, col.to_string()))
}

pub fn parse_arrayzip_edge_explode_query(query: &str) -> Option<TokenExplodeMatch> {
    let lower = query.to_lowercase();
    let select_kw_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_kw_end > from_idx {
        return None;
    }
    let after_from = query[from_idx + "from".len()..].trim_start();
    if after_from.starts_with('(') {
        return None;
    }
    if lower[from_idx..].matches("select").count() > 0 {
        return None;
    }

    let items = split_top_level_commas(&query[select_kw_end..from_idx]);
    let (first, rest) = items.split_first()?;
    if rest.is_empty() {
        return None;
    }
    let (explode_expr, label) = split_expr_alias(first.trim())?;

    let explode_expr = explode_expr.trim();
    let explode_lower = explode_expr.to_lowercase();
    if !explode_lower.starts_with("arrayjoin(") {
        return None;
    }
    let aj_close = find_matching_close_paren(explode_expr, "arrayjoin(".len() - 1)?;
    if aj_close != explode_expr.len() - 1 {
        return None;
    }
    let aj_inner = explode_expr["arrayjoin(".len()..aj_close].trim();
    let aj_inner_lower = aj_inner.to_lowercase();
    if !aj_inner_lower.starts_with("arrayzip(") {
        return None;
    }
    let az_close = find_matching_close_paren(aj_inner, "arrayzip(".len() - 1)?;
    if az_close != aj_inner.len() - 1 {
        return None;
    }
    let az_args = split_top_level_commas(&aj_inner["arrayzip(".len()..az_close]);
    let [arg0, arg1] = az_args.as_slice() else {
        return None;
    };
    let (sep0, col0) = parse_edge_array_slice_arg(arg0.trim(), 1)?;
    let (sep1, col1) = parse_edge_array_slice_arg(arg1.trim(), 2)?;
    // Only whitespace-separated AS-path tokenization is registered for -
    // get_arrayzip_edge_explode_streaming_aggregation_configs hardcodes
    // the "whitespace" tokenizer (same convention flat token-explode's own
    // planner registration already uses), so any other separator would
    // silently mis-tokenize at ingest time.
    if sep0 != sep1 || col0 != col1 || sep0 != ' ' {
        return None;
    }
    let source_col = col0;

    let select_expr = rest.iter().map(|s| s.trim()).collect::<Vec<_>>().join(", ");

    let group_idx = lower.find("group by")?;
    let where_clause = query[from_idx + "from".len()..group_idx].trim().to_string();
    let where_lower = where_clause.to_lowercase();
    let (from_target, where_clause) = match where_lower.find("where") {
        Some(w_idx) => (
            where_clause[..w_idx].trim().to_string(),
            where_clause[w_idx + "where".len()..].trim().to_string(),
        ),
        None => (where_clause, String::new()),
    };
    if from_target.is_empty() {
        return None;
    }

    let after_group = &query[group_idx + "group by".len()..];
    let after_group_lower = after_group.to_lowercase();
    let group_end = after_group_lower
        .find("order by")
        .or_else(|| after_group_lower.find("limit"))
        .unwrap_or(after_group.len());
    let group_by = after_group[..group_end].trim().trim_end_matches(';').to_string();
    if group_by != label {
        return None; // must group by exactly the exploded label, nothing else
    }
    let order_by_and_limit = after_group[group_end..]
        .trim()
        .trim_end_matches(';')
        .to_string();

    Some(TokenExplodeMatch {
        label,
        source_col,
        filter_regex: String::new(),
        select_expr,
        from_target,
        where_clause,
        group_by,
        order_by_and_limit,
    })
}

pub fn rewrite_arrayzip_edge_explode_query(query: &str) -> Option<String> {
    if !looks_like_arrayzip_edge_explode_sql(query) {
        return None;
    }
    let m = parse_arrayzip_edge_explode_query(query)?;
    Some(build_token_explode_surrogate(&m))
}

// ---------------------------------------------------------------------------
// MOAS (multiple-origin-AS) pattern: group by an existing column (e.g.
// `prefix`), aggregate the *set* of distinct values of some other column
// (e.g. origin ASN), keeping only groups whose set has more than one
// member. Two raw-SQL shapes reach the same semantics:
//   - literal: the origin value is already a plain column
//     (`COUNT(DISTINCT origin_asn)` + `DISTINCT_SET(origin_asn)`).
//   - tokenized: the origin value is derived by the same tokenizer building
//     block token-select/token-explode use (`uniqExact(as_path_array[-1])`
//     + `groupUniqArray(as_path_array[-1])`, fed by a nested subquery
//     tokenizing `as_path`). This shape additionally needs a computed label
//     emitted at ingest time, same as token-select.
// Both normalize to one canonical surrogate:
//   SELECT {group_by}, COUNT(DISTINCT {label}) AS origin_count
//   FROM {from_target} WHERE {where_clause} GROUP BY {group_by}
//   ORDER BY origin_count DESC LIMIT 1000000
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MoasMatch {
    pub group_by: String,
    pub label: String,
    pub from_target: String,
    pub where_clause: String,
    /// Set only when `label` is a computed value (the tokenized last
    /// AS-path element) rather than an existing column - the caller must
    /// emit a ComputedLabelConfig for `label` using these
    /// (source_col, filter_regex).
    pub computed_label: Option<(String, String)>,
}

pub fn looks_like_moas_literal_sql(query: &str) -> bool {
    let q = query.to_lowercase();
    q.contains("prefix")
        && q.contains("origin_asn")
        && q.contains("count(distinct")
        && q.contains("distinct_set")
        && q.contains("group by")
}

pub fn looks_like_moas_tokenized_sql(query: &str) -> bool {
    let q = query.to_lowercase();
    q.contains("arrayfilter(")
        && q.contains("splitbywhitespace(")
        && q.contains("match(")
        && q.contains("groupuniqarray(")
        && q.contains("group by")
}

/// True for either MOAS raw-SQL shape - the one gate both the planner and
/// the query engine call to decide "try MOAS handling" before falling
/// through to other patterns.
pub fn looks_like_moas_sql(query: &str) -> bool {
    looks_like_moas_literal_sql(query) || looks_like_moas_tokenized_sql(query)
}

/// True for the canonical MOAS *surrogate* shape (what gets registered in
/// inference_config.yaml and what query-time lookup scans registered
/// queries for) - distinct from `looks_like_moas_sql`, which recognizes raw
/// user SQL. Deliberately loose: any registered query built by
/// `build_moas_surrogate` matches this, regardless of which raw shape it
/// came from.
pub fn looks_like_moas_registered_sql(query: &str) -> bool {
    let q = query.to_lowercase();
    q.contains("count(distinct") && q.contains("origin_count") && q.contains("group by")
}

fn parse_moas_literal_query(query: &str) -> Option<MoasMatch> {
    let lower = query.to_lowercase();
    let from_idx = lower.find("from ")?;
    let where_idx = lower.find("where ")?;
    let group_idx = lower.find("group by ")?;
    if !(from_idx < where_idx && where_idx < group_idx) {
        return None;
    }
    let from_target = query[from_idx + "from ".len()..where_idx].trim().to_string();
    let where_clause = query[where_idx + "where ".len()..group_idx].trim().to_string();
    Some(MoasMatch {
        group_by: "prefix".to_string(),
        label: "origin_asn".to_string(),
        from_target,
        where_clause,
        computed_label: None,
    })
}

fn parse_moas_tokenized_query(query: &str) -> Option<MoasMatch> {
    let tok = extract_token_filter(query)?;

    let lower = query.to_lowercase();
    let from_idx = lower.find("from")?;
    let open_paren_rel = lower[from_idx..].find('(')?;
    let subquery_open = from_idx + open_paren_rel;
    let subquery_close = find_matching_close_paren(query, subquery_open)?;

    let outer_select = &query[..from_idx];
    let outer_select_lower = outer_select.to_lowercase();
    let outer_tail = &query[subquery_close + 1..];
    let outer_tail_lower = outer_tail.to_lowercase();

    // Confirm the tokenized array is actually aggregated as a set: both the
    // cardinality expression and groupUniqArray must reference
    // `{inner_alias}[-1]` (the same "last token" indexing token-select
    // uses).
    let index_marker = format!("{}[-1]", tok.inner_alias.to_lowercase());
    if !outer_select_lower.contains(&index_marker) || !outer_select_lower.contains("groupuniqarray(")
    {
        return None;
    }

    let where_clause = extract_inner_where(query, subquery_close)?;
    let from_target = extract_inner_from(query, subquery_open, subquery_close)?;

    let group_idx = outer_tail_lower.find("group by")? + "group by".len();
    let after_group = &outer_tail[group_idx..];
    let after_group_lower = after_group.to_lowercase();
    let group_end = after_group_lower
        .find("having")
        .or_else(|| after_group_lower.find("order by"))
        .or_else(|| after_group_lower.find("limit"))
        .unwrap_or(after_group.len());
    let group_by = after_group[..group_end]
        .trim()
        .trim_end_matches(',')
        .to_string();
    if group_by.is_empty() {
        return None;
    }

    // A HAVING clause, if present, must be asking for exactly "more than
    // one" - the engine's MOAS handler hardcodes a >1 filter (that's the
    // whole definition of MOAS), so silently matching a HAVING with a
    // different threshold would misrepresent the query rather than serve
    // it correctly.
    if let Some(having_idx) = after_group_lower.find("having") {
        let having_text = &after_group[having_idx + "having".len()..];
        let having_lower = having_text.to_lowercase();
        let having_end = having_lower
            .find("order by")
            .or_else(|| having_lower.find("limit"))
            .unwrap_or(having_text.len());
        let normalized: String = having_text[..having_end]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        let normalized_lower = normalized.to_lowercase();
        if !(normalized_lower.contains(">1") || normalized_lower.contains(">=2")) {
            return None;
        }
    }

    let label = format!("{}_last_token", tok.source_col);

    Some(MoasMatch {
        group_by,
        label,
        from_target,
        where_clause,
        computed_label: Some((tok.source_col, tok.filter_regex)),
    })
}

pub fn parse_moas_query(query: &str) -> Option<MoasMatch> {
    if looks_like_moas_tokenized_sql(query) {
        return parse_moas_tokenized_query(query);
    }
    if looks_like_moas_literal_sql(query) {
        return parse_moas_literal_query(query);
    }
    None
}

pub fn build_moas_surrogate(m: &MoasMatch) -> String {
    format!(
        "SELECT {group_by}, COUNT(DISTINCT {label}) AS origin_count FROM {from_target} WHERE {where_clause} GROUP BY {group_by} ORDER BY origin_count DESC LIMIT 1000000",
        group_by = m.group_by,
        label = m.label,
        from_target = m.from_target,
        where_clause = m.where_clause,
    )
}

pub fn rewrite_moas_query(query: &str) -> Option<String> {
    let m = parse_moas_query(query)?;
    Some(build_moas_surrogate(&m))
}

const AGGREGATE_FUNCTIONS: &[&str] = &[
    "COUNT(",
    "COUNTIF(",
    "SUM(",
    "AVG(",
    "MIN(",
    "MAX(",
    "UNIQ(",
    "UNIQEXACT(",
    "UNIQCOMBINED(",
    "UNIQCOMBINED64(",
    "GROUPUNIQARRAY(",
    "GROUPARRAY(",
    "TOPK(",
    "QUANTILE(",
    "ANY(",
    "ANYLAST(",
    "ARGMIN(",
    "ARGMAX(",
];

/// Aggregate functions the classic single-aggregate parser actually
/// recognizes as a statistic (see `get_sql_statistics` in the planner) -
/// narrower than `AGGREGATE_FUNCTIONS` above, which also includes shapes
/// (countIf, groupUniqArray, topk, ...) that require their own dedicated
/// pattern and would otherwise fail loudly if let through here.
const SIMPLE_SCALAR_AGGREGATE_FUNCTIONS: &[&str] = &[
    "COUNT(",
    "SUM(",
    "AVG(",
    "MIN(",
    "MAX(",
    "UNIQ(",
    "UNIQEXACT(",
    "UNIQCOMBINED(",
    "UNIQCOMBINED64(",
    "QUANTILE(",
];

/// True for a scalar aggregate with no `GROUP BY` at all - one global value,
/// no partition key (`SELECT count(*) FROM ... WHERE ...`). Unlike a raw row
/// scan, this *is* servable: the classic single-aggregate parser now accepts
/// an empty GROUP BY (see `get_groupbys`'s `allow_empty`), producing a
/// precompute with an empty grouping key - the same mechanism already used
/// for whole-window HLL cardinality. Deliberately narrow: exactly one
/// occurrence of exactly one recognized simple aggregate function, nothing
/// else that would need its own pattern (`DISTINCT`, `HAVING`, a second
/// SELECT, multiple aggregates).
pub fn looks_like_scalar_aggregate_sql(query: &str) -> bool {
    let upper = query.to_uppercase();
    if upper.contains("GROUP BY") || upper.contains("HAVING") {
        return false;
    }
    let Some(select_end) = upper.find("SELECT").map(|i| i + "SELECT".len()) else {
        return false;
    };
    let Some(from_idx) = upper.find("FROM") else {
        return false;
    };
    if select_end > from_idx {
        return false;
    }
    // A real `SELECT DISTINCT ...` clause only ever appears as the first
    // keyword right after SELECT. Checking for the substring "DISTINCT"
    // anywhere in the query text (the previous version of this check) is
    // wrong: it also matches inside an alias like
    // `uniqExact(peer_ip) AS distinct_peers`, wrongly treating every
    // `distinct_*`-aliased scalar aggregate as a `SELECT DISTINCT` query
    // and punting it for the wrong reason.
    if upper[select_end..from_idx].trim_start().starts_with("DISTINCT") {
        return false;
    }
    // A nested-subquery FROM belongs to other patterns.
    let after_from = query[from_idx + "FROM".len()..].trim_start();
    if after_from.starts_with('(') {
        return false;
    }
    let select_list = &upper[..from_idx];
    let total_known: usize = AGGREGATE_FUNCTIONS
        .iter()
        .map(|f| select_list.matches(f).count())
        .sum();
    let simple_known: usize = SIMPLE_SCALAR_AGGREGATE_FUNCTIONS
        .iter()
        .map(|f| select_list.matches(f).count())
        .sum();
    // Exactly one aggregate-shaped call in the SELECT list, and it has to be
    // one of the simple ones - if it's e.g. countIf, total_known would still
    // be 1 but simple_known would be 0, correctly rejecting it.
    total_known == 1 && simple_known == 1
}

/// For a query matching `looks_like_scalar_aggregate_sql`, extracts the
/// column name inside the sole aggregate function's parentheses (e.g.
/// `min(timestamp)` -> `Some("timestamp")`). Returns `None` for `count(*)`
/// (no column to check) or when the argument isn't a bare identifier (an
/// expression has nothing a value-column check needs to see). Callers use
/// this to reject a shape the classic parser can't actually build - e.g. an
/// aggregate over the table's own time column, which isn't a value column at
/// all and would otherwise fail loudly with `InvalidValueCol` deep inside
/// `get_streaming_aggregation_configs` instead of punting cleanly up front.
pub fn scalar_aggregate_value_column(query: &str) -> Option<String> {
    let upper = query.to_uppercase();
    let from_idx = upper.find("FROM")?;
    let select_list = &query[..from_idx];
    let select_list_upper = &upper[..from_idx];
    for func in SIMPLE_SCALAR_AGGREGATE_FUNCTIONS {
        if select_list_upper.contains(func) {
            let arg = extract_paren_arg_after(select_list, &func.to_lowercase())?;
            if arg.is_empty() || arg == "*" {
                return None;
            }
            return if arg.chars().all(|c| c.is_alphanumeric() || c == '_') {
                Some(arg.to_string())
            } else {
                None
            };
        }
    }
    None
}

/// True when `query` matches `looks_like_scalar_aggregate_sql` but the sole
/// aggregate's argument is neither `*` nor a bare identifier - e.g. a
/// multi-argument call like `uniqExact(prefix, operation, as_path)`
/// (a composite/tuple distinct count), or a computed expression like
/// `uniqExact(splitByChar(' ', as_path)[1])`. `scalar_aggregate_value_column`
/// can't be reused to detect this: it already returns `None` for the
/// legitimate `count(*)` case (no real value-column argument, handled by a
/// synthetic column downstream), so a caller can't tell that `None` apart
/// from "the argument is some third, unusable shape" without re-deriving
/// it. Neither shape has a single bare-column value the classic parser's
/// value-column validation can use, and reaching
/// `get_streaming_aggregation_configs` with one fails loudly (aborting the
/// whole planning run per `?`, not just this query) instead of punting
/// cleanly - so `is_exact_only()` checks this proactively, the same
/// treatment as the time-column case just above it.
pub fn scalar_aggregate_has_unusable_value_arg(query: &str) -> bool {
    if !looks_like_scalar_aggregate_sql(query) {
        return false;
    }
    let upper = query.to_uppercase();
    let Some(from_idx) = upper.find("FROM") else {
        return false;
    };
    let select_list = &query[..from_idx];
    let select_list_upper = &upper[..from_idx];
    for func in SIMPLE_SCALAR_AGGREGATE_FUNCTIONS {
        if select_list_upper.contains(func) {
            let Some(arg) = extract_paren_arg_after(select_list, &func.to_lowercase()) else {
                return true;
            };
            let arg = arg.trim();
            return !(arg == "*" || is_bare_identifier(arg));
        }
    }
    false
}

/// True when the query's WHERE clause contains a nested `SELECT`
/// (`prefix NOT IN (SELECT prefix FROM ...)`, `origin IN (SELECT origin
/// FROM ... GROUP BY ...)`). The ingest-time spatial filter only evaluates
/// literal equality/inequality/IN-list clauses against a single streamed
/// row - it has no way to run a correlated subquery, so a filter like this
/// would either match nothing or (worse) silently fail open. ClickHouse
/// itself has no such limitation, so the right answer is to route the whole
/// query there rather than build a summary against a filter we can't
/// actually evaluate.
///
/// Deliberately narrow: only looks for "select" appearing after the WHERE
/// keyword, not a general subquery detector - a FROM-clause subquery
/// (MOAS/token-select/token-explode's own nested tokenizing subquery) is a
/// completely different, already-handled shape, and its own WHERE always
/// precedes any outer WHERE, so this check doesn't see it.
pub fn looks_like_unevaluable_where_subquery_sql(query: &str) -> bool {
    let lower = query.to_lowercase();
    // Not " where " with required surrounding spaces - real analyst SQL is
    // pretty-printed across lines (e.g. "...bgp_updates\nWHERE collector..."),
    // so the whitespace before WHERE is often a newline, not a space.
    let Some(where_idx) = lower.find("where") else {
        return false;
    };
    lower[where_idx + "where".len()..].contains("select")
}

/// True for a query with no `GROUP BY` clause at all: a raw row scan
/// (`SELECT DISTINCT ...`, a bare column listing), or a scalar/whole-window
/// aggregate (`SELECT count(*) FROM ... WHERE ...`, no GROUP BY). Neither
/// shape is servable by this architecture's precomputed summaries, which
/// are always keyed by a GROUP BY partition - a raw scan for the obvious
/// reason (exact per-row output can't come from a lossy aggregate), and a
/// scalar aggregate because nothing here builds a "single global value, no
/// grouping key" precompute. Callers should treat a `true` result as "route
/// this query straight to the source of truth," not as "try harder to plan
/// it." Intended as a last-resort check, run only after every other pattern
/// in this module (including multi-aggregate) has had a chance to claim the
/// query.
pub fn looks_like_exact_only_sql(query: &str) -> bool {
    let upper = query.to_uppercase();
    if upper.contains("GROUP BY") {
        return false;
    }
    // A scalar aggregate and a plain SELECT DISTINCT are the two no-GROUP-BY
    // shapes that ARE servable - see looks_like_scalar_aggregate_sql and
    // parse_select_distinct_query. Everything else without a GROUP BY (raw
    // scans, window functions) still punts.
    !looks_like_scalar_aggregate_sql(query) && parse_select_distinct_query(query).is_none()
}

// ---------------------------------------------------------------------------
// SELECT DISTINCT <col> pattern: a plain distinct-value listing with no
// GROUP BY (e.g. `SELECT DISTINCT prefix FROM ... WHERE ... LIMIT 100`).
// Lowered to a `uniqExact(col)` surrogate purely to reuse the classic
// single-aggregate parser's schema validation, spatial-filter extraction,
// and time-window computation (get_groupbys' allow_empty already accepts a
// scalar CARDINALITY aggregate) - the resulting config is then swapped from
// an approximate cardinality estimate to an exact SetAggregator tracking the
// column's actual distinct values, the same swap `parse_moas_query`'s path
// makes for `COUNT(DISTINCT label)` under a GROUP BY, just with an empty
// grouping key instead of an outer group-by column.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct SelectDistinctMatch {
    /// The distinct column - a bare identifier only, no function call, no
    /// table-alias prefix (an aliased query like `SELECT DISTINCT w.prefix`
    /// isn't a bare identifier and correctly fails to match, punting).
    pub column: String,
    /// "FROM ... WHERE ..." (or just "FROM ..."), verbatim.
    pub from_where: String,
    /// Raw `ORDER BY ... LIMIT ...` tail (either or both, or empty), verbatim.
    pub order_by_and_limit: String,
}

pub fn parse_select_distinct_query(query: &str) -> Option<SelectDistinctMatch> {
    let lower = query.to_lowercase();
    let select_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_end > from_idx {
        return None;
    }

    let select_list = query[select_end..from_idx].trim();
    let select_list_lower = select_list.to_lowercase();
    if !select_list_lower.starts_with("distinct") {
        return None;
    }
    let column = select_list["distinct".len()..].trim().to_string();
    if !is_bare_identifier(&column) {
        return None;
    }

    // A nested-subquery FROM belongs to other patterns.
    let after_from = query[from_idx + "from".len()..].trim_start();
    if after_from.starts_with('(') {
        return None;
    }

    // A nested SELECT inside the WHERE clause isn't evaluable by the
    // ingest-time spatial filter - same reasoning as
    // looks_like_unevaluable_where_subquery_sql, checked here directly
    // (rather than relying on dispatch order) so this parser is safe to
    // call standalone.
    if lower[from_idx..].matches("select").count() > 0 {
        return None;
    }

    let after_from_lower = &lower[from_idx..];
    let tail_start_rel = after_from_lower
        .find("order by")
        .or_else(|| after_from_lower.find("limit"))
        .unwrap_or(query.len() - from_idx);
    let tail_start = from_idx + tail_start_rel;

    let from_where = query[from_idx..tail_start]
        .trim_end()
        .trim_end_matches(';')
        .to_string();
    let order_by_and_limit = query[tail_start..].trim().trim_end_matches(';').to_string();

    Some(SelectDistinctMatch {
        column,
        from_where,
        order_by_and_limit,
    })
}

/// The `uniqExact(col)` surrogate used purely to reuse the classic
/// single-aggregate parser's schema/spatial-filter/time-window handling -
/// see the module doc above `SelectDistinctMatch`.
pub fn build_select_distinct_surrogate(m: &SelectDistinctMatch) -> String {
    format!(
        "SELECT uniqExact({column}) AS __distinct_count__ {from_where}",
        column = m.column,
        from_where = m.from_where,
    )
}

// ---------------------------------------------------------------------------
// groupArray(DISTINCT <col>) pattern: ClickHouse's exact distinct-value-list
// aggregate (as opposed to uniqExact, which only returns their *count*) -
// e.g. `SELECT prefix, groupArray(DISTINCT origin) AS origins FROM ...
// GROUP BY prefix` (q116), or with no GROUP BY at all, one global list
// (q165's `SELECT groupArray(DISTINCT origin) AS origins_list FROM ...`).
// Reuses the exact same CARDINALITY -> SetAggregator swap SELECT DISTINCT
// and MOAS both make, generalized to an *optional* single GROUP BY column -
// SELECT DISTINCT's empty grouping key when absent, MOAS's one grouping
// column when present - lowered to a `uniqExact(col)` surrogate purely to
// reuse the classic single-aggregate parser's schema validation and window
// computation, then swapped to an exact set at registration time. Every
// query this targets so far pairs it with its own `uniqExact(col)` sibling
// for the count, so it always appears as one branch of a multi-aggregate
// SELECT list, split out by parse_multi_aggregate_query before this ever
// runs - see handle_multi_aggregate_all_labels_sql in the query engine for
// the serve-time half.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct GroupArrayDistinctMatch {
    /// `None` for the no-GROUP-BY scalar form (one global list, q165);
    /// `Some(col)` for the grouped form (one list per group, q116) - the
    /// GROUP BY clause must be exactly this one column, same restriction
    /// `ArgAggMatch` places on its own group_by_col.
    pub group_by: Option<String>,
    /// The column whose distinct values are collected.
    pub column: String,
    /// The original SELECT-list alias (e.g. "origins") - carried through
    /// unchanged to the combined multi-aggregate result's output label.
    pub alias: String,
    /// "FROM ... WHERE ..." (or just "FROM ..."), verbatim.
    pub from_where: String,
    /// Raw `LIMIT n` tail, if any, verbatim - no ORDER BY (this mechanism's
    /// result is a label, a joined list, not a sortable value - same
    /// reasoning as `ArgAggMatch` rejecting ORDER BY entirely).
    pub limit: Option<String>,
}

/// Parses `groupArray(DISTINCT <col>) AS <alias>` into its column and
/// alias, or `None` if `expr` isn't exactly that shape - same style as
/// `parse_countif_expr` just above.
fn parse_group_array_call(expr: &str) -> Option<(String, String)> {
    let lower = expr.to_lowercase();
    if !lower.starts_with("grouparray(") {
        return None;
    }
    let open_idx = "grouparray".len();
    let close_idx = find_matching_close_paren(expr, open_idx)?;
    let inner = expr[open_idx + 1..close_idx].trim();
    let inner_lower = inner.to_lowercase();
    if !inner_lower.starts_with("distinct") {
        return None;
    }
    let column = inner["distinct".len()..].trim().to_string();
    if !is_bare_identifier(&column) {
        return None;
    }

    let after = &expr[close_idx + 1..];
    let after_lower = after.to_lowercase();
    let as_idx = after_lower.find("as ")?;
    if !after[..as_idx].trim().is_empty() {
        return None;
    }
    let alias = after[as_idx + 3..].trim().to_string();
    if alias.is_empty() {
        return None;
    }
    Some((column, alias))
}

pub fn looks_like_group_array_distinct_sql(query: &str) -> bool {
    parse_group_array_distinct_query(query).is_some()
}

pub fn parse_group_array_distinct_query(query: &str) -> Option<GroupArrayDistinctMatch> {
    let lower = query.to_lowercase();
    let select_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_end > from_idx {
        return None;
    }

    let select_list = &query[select_end..from_idx];
    let items = split_top_level_commas(select_list);

    // Exactly one groupArray(DISTINCT ...) item, plus at most one plain
    // bare-identifier GROUP BY column alongside it - anything else (a
    // second aggregate sharing this SELECT list, a computed group column)
    // isn't a shape this parser handles. The multi-aggregate split already
    // isolates this branch into its own SELECT list before this ever runs,
    // so in practice `items.len()` is 1 or 2.
    let mut group_col: Option<String> = None;
    let mut found: Option<(String, String)> = None;
    for item in &items {
        let item_trim = item.trim();
        if let Some((column, alias)) = parse_group_array_call(item_trim) {
            if found.is_some() {
                return None;
            }
            found = Some((column, alias));
        } else if is_bare_identifier(item_trim) {
            if group_col.is_some() {
                return None;
            }
            group_col = Some(item_trim.to_string());
        } else {
            return None;
        }
    }
    let (column, alias) = found?;

    let after_from = query[from_idx + "from".len()..].trim_start();
    if after_from.starts_with('(') {
        return None;
    }
    if lower[from_idx..].matches("select").count() > 0 {
        return None;
    }

    let after_from_lower = &lower[from_idx..];
    if after_from_lower.contains("order by") {
        return None;
    }

    let (from_where, group_by, limit) = match after_from_lower.find("group by") {
        Some(g_rel) => {
            let from_where = query[from_idx..from_idx + g_rel].trim_end().to_string();
            let after_group = &query[from_idx + g_rel + "group by".len()..];
            let after_group_lower = after_group.to_lowercase();
            let group_end = after_group_lower.find("limit").unwrap_or(after_group.len());
            let clause = after_group[..group_end].trim().to_string();
            if !is_bare_identifier(&clause) {
                return None;
            }
            let limit = after_group_lower.find("limit").map(|l_rel| {
                after_group[l_rel + "limit".len()..]
                    .trim()
                    .trim_end_matches(';')
                    .to_string()
            });
            (from_where, Some(clause), limit)
        }
        None => {
            let limit_rel = after_from_lower.find("limit");
            let from_where_end = limit_rel.unwrap_or(query.len() - from_idx);
            let from_where = query[from_idx..from_idx + from_where_end].trim_end().to_string();
            let limit = limit_rel.map(|l_rel| {
                query[from_idx + l_rel + "limit".len()..]
                    .trim()
                    .trim_end_matches(';')
                    .to_string()
            });
            (from_where, None, limit)
        }
    };

    // The GROUP BY column (if any) must be the same one column carried
    // alongside the groupArray item in the SELECT list - a mismatch (or a
    // GROUP BY with no matching SELECT-list column, or vice versa) isn't a
    // shape this mechanism serves.
    match (&group_by, &group_col) {
        (Some(g), Some(s)) if g == s => {}
        (None, None) => {}
        _ => return None,
    }

    Some(GroupArrayDistinctMatch {
        group_by,
        column,
        alias,
        from_where,
        limit,
    })
}

/// Fixed alias for the internal CARDINALITY surrogate this pattern lowers
/// to - shared verbatim between the planner (registration) and the query
/// engine (serve-time lookup), the same way `build_select_distinct_surrogate`
/// hardcodes `__distinct_count__`.
pub const GROUP_ARRAY_DISTINCT_SURROGATE_ALIAS: &str = "__group_array_distinct_count__";

pub fn build_group_array_distinct_surrogate(m: &GroupArrayDistinctMatch) -> String {
    match &m.group_by {
        Some(group_by) => format!(
            "SELECT {group_by}, uniqExact({column}) AS {alias} {from_where} GROUP BY {group_by}",
            group_by = group_by,
            column = m.column,
            alias = GROUP_ARRAY_DISTINCT_SURROGATE_ALIAS,
            from_where = m.from_where,
        ),
        None => format!(
            "SELECT uniqExact({column}) AS {alias} {from_where}",
            column = m.column,
            alias = GROUP_ARRAY_DISTINCT_SURROGATE_ALIAS,
            from_where = m.from_where,
        ),
    }
}

// ---------------------------------------------------------------------------
// Computed-expression GROUP BY: `GROUP BY <computed_expr> AS <alias>` where
// the group key is derived directly in the outer SELECT from a real column
// (no nested subquery, unlike token_select/token_explode's tokenized MOAS
// shape) - e.g. `splitByChar('/', prefix)[2] AS prefix_len ... GROUP BY
// prefix_len`. Recognizes exactly three shapes:
//   splitByChar('<sep>', <col>)[<n>]   -> the nth token (1-indexed)
//   length(splitByChar('<sep>', <col>)) -> token count
//   length(<col>)                       -> byte length of the raw column
// Lowered to `ComputedLabelConfig` (computed_labels.rs in the query engine
// already knows how to derive all three at ingest time) and a surrogate
// that references the alias as if it were an ordinary metadata column -
// the same mechanism token_select/token_explode already use, just for a
// directly-written expression instead of a nested-subquery tokenizer.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ComputedGroupByMatch {
    pub alias: String,
    pub label_type: &'static str,
    pub source_col: String,
    pub tokenizer: Option<String>,
    pub select: Option<String>,
    /// The rest of the SELECT list (everything after the computed
    /// expression's own comma), verbatim - e.g. `count(*) AS cnt`.
    pub aggregate_and_rest: String,
    /// "FROM ... WHERE ..." (or just "FROM ..."), verbatim.
    pub from_where: String,
    /// Raw `ORDER BY ... LIMIT ...` tail (either or both, or empty), verbatim.
    pub order_by_and_limit: String,
    /// The full GROUP BY clause text, verbatim - the computed alias plus
    /// any additional plain-identifier columns grouped alongside it (e.g.
    /// `prefix_len, operation`). Reused as-is in the surrogate: every
    /// column in it already resolves correctly there, since the alias
    /// becomes a real metadata column by ingest time and the other
    /// columns were already real.
    pub group_by_clause: String,
}

/// Splits `<expr> AS <alias>` (case-insensitive `AS`), returning
/// `(expr.trim(), alias)`. `alias` must be a bare identifier.
fn split_expr_alias(item: &str) -> Option<(String, String)> {
    let lower = item.to_lowercase();
    let as_idx = lower.rfind(" as ")?;
    let expr = item[..as_idx].trim().to_string();
    let alias = item[as_idx + 4..].trim().to_string();
    if expr.is_empty() || !is_bare_identifier(&alias) {
        return None;
    }
    Some((expr, alias))
}

/// Matches one of the three computed-label shapes this pattern supports,
/// returning `(label_type, source_col, tokenizer, select)` ready to drop
/// into a `ComputedLabelConfig`.
/// Result of `parse_computed_value_agg_query`: an aggregate whose single
/// argument is a computed expression (`avg(length(splitByChar(' ',
/// as_path)))`, `uniqExact(toDate(timestamp))`, ...) that
/// Replaces the table reference right after a query's first `FROM` with
/// `derived_metric`, leaving everything else - WHERE, GROUP BY, ORDER BY,
/// LIMIT, the whole SELECT list - untouched. Deliberately narrow: only
/// meant for the flat, no-subquery classic-aggregate shape the raw/computed
/// -value-aggregate mechanism already validated the query matches (a
/// nested `FROM (SELECT ...)` would need its *inner* FROM replaced, not
/// this outer one - out of scope for that mechanism, so this helper
/// doesn't need to handle it). Shared by the planner (building the
/// surrogate to register) and the query engine (rewriting a live query to
/// match it - see `rewrite_raw_value_agg_query`); the two must stay
/// byte-for-byte identical on the same input; a real value column and its
/// derived-value counterpart pointing at different table strings for the
/// same query is exactly the "plans but never serves" bug a shared
/// implementation prevents.
pub fn replace_from_table(query: &str, derived_metric: &str) -> String {
    let lower = query.to_lowercase();
    let Some(from_idx) = lower.find("from") else {
        return query.to_string();
    };
    let after_from = from_idx + "from".len();
    let rest = &query[after_from..];
    let trimmed = rest.trim_start();
    let leading_ws = rest.len() - trimmed.len();
    let table_end = trimmed
        .find(|c: char| c.is_whitespace())
        .unwrap_or(trimmed.len());
    let table_start_abs = after_from + leading_ws;
    let table_end_abs = table_start_abs + table_end;
    format!(
        "{}{}{}",
        &query[..table_start_abs],
        derived_metric,
        &query[table_end_abs..]
    )
}

/// The table token right after a query's first `FROM`, e.g. `"bgp"` for
/// `... FROM bgp WHERE ...`. Same extraction `replace_from_table` does,
/// without the replacement - used where the name itself is needed (schema
/// lookups) rather than a substitution target.
fn from_table_name(query: &str) -> Option<&str> {
    let lower = query.to_lowercase();
    let from_idx = lower.find("from")?;
    let after_from = &query[from_idx + "from".len()..];
    let trimmed = after_from.trim_start();
    let table_end = trimmed
        .find(|c: char| c.is_whitespace())
        .unwrap_or(trimmed.len());
    let name = &trimmed[..table_end];
    // "FROM bgp.bgp_updates" - the schema registers the table as bare "bgp"
    // (the part before the dot, matching how SQLPatternParser itself derives
    // `SQLQueryData.metric`), not the full "bgp.bgp_updates" this would
    // otherwise return. Without stripping it, every schema.get_time_column/
    // get_metadata_columns lookup below misses (no table named
    // "bgp.bgp_updates" exists), so this function silently rejected every
    // query at all, and rewrite_arg_agg_query never rewrote anything -
    // ARGMAX/ARGMIN over a raw column stayed pointed at the wrong metric
    // and could never find the aggregation the planner registered for it.
    let name = name.split('.').next().unwrap_or(name);
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

/// Serve-time counterpart to asap-planner-rs's
/// `get_arg_agg_streaming_aggregation_configs`: rewrites a live
/// `argMax(x, <time_column>)` / `argMin(x, <time_column>)` query into the
/// exact surrogate the planner registered (pointing at the metric's
/// `derived_value_arg_<col>_<table>` virtual table). Without this, the
/// registered surrogate's `metric` field is the derived table but the live
/// query's is the real one, and `matches_sql_pattern` requires exact
/// equality - the same "plans but never serves" bug
/// `rewrite_raw_value_agg_query` exists to prevent for the numeric case.
pub fn rewrite_arg_agg_query(query: &str, schema: &SQLSchema) -> Option<String> {
    let m = parse_arg_agg_query(query)?;
    let table_name = from_table_name(&m.from_where)?;

    if schema.get_time_column(table_name) != Some(&m.cmp_col) {
        return None;
    }
    if !schema
        .get_metadata_columns(table_name)
        .is_some_and(|cols| cols.contains(&m.arg_col))
    {
        return None;
    }

    let derived_metric = format!("derived_value_arg_{}_{}", m.arg_col, table_name);
    Some(replace_from_table(query, &derived_metric))
}

/// Strips a `<numeric-cast>(toString(<col>))` or bare `toString(<col>)`
/// wrapper down to `<col>` - not a real computation (label values are
/// already strings, so `toString` is a no-op, and the numeric cast's only
/// effect the aggregate cares about is discarding a type annotation the
/// engine never tracked anyway), just analysts writing
/// `avg(toFloat64OrZero(toString(med)))` for a column ASAP already treats
/// as numeric everywhere else. `None` for anything else, including an
/// already-bare identifier (that's `get_raw_value_agg_streaming_aggregation_configs`'s
/// job, not this one).
pub fn strip_trivial_string_cast_wrapper(expr: &str) -> Option<String> {
    let trimmed = expr.trim();
    if is_bare_identifier(trimmed) {
        return None;
    }
    let lower = trimmed.to_lowercase();
    let inner = if let Some(f) = NUMERIC_CAST_FUNCTIONS.iter().find(|f| lower.starts_with(**f)) {
        let close_idx = find_matching_close_paren(trimmed, f.len() - 1)?;
        if close_idx != trimmed.len() - 1 {
            return None;
        }
        trimmed[f.len()..close_idx].trim()
    } else {
        trimmed
    };
    let inner_lower = inner.to_lowercase();
    if !inner_lower.starts_with("tostring(") {
        return None;
    }
    let close_idx = find_matching_close_paren(inner, "tostring".len())?;
    if close_idx != inner.len() - 1 {
        return None;
    }
    let col = inner["tostring(".len()..close_idx].trim();
    if is_bare_identifier(col) {
        Some(col.to_string())
    } else {
        None
    }
}

/// Finds the first supported aggregate call in `query` whose argument is a
/// trivial `toString`/numeric-cast wrapper around a bare column (see
/// `strip_trivial_string_cast_wrapper`), returning `query` with that
/// wrapper stripped down to the bare column - e.g.
/// `avg(toFloat64OrZero(toString(med))) AS avg_med` becomes
/// `avg(med) AS avg_med`. `None` if no aggregate call matches this shape.
pub fn strip_query_string_cast_wrapper(query: &str) -> Option<String> {
    for name in COMPUTED_VALUE_AGG_NAMES {
        let Some((start, end)) = find_agg_call(query, name) else {
            continue;
        };
        let open_idx = start + name.len();
        let inner = &query[open_idx + 1..end - 1];
        if let Some(col) = strip_trivial_string_cast_wrapper(inner) {
            return Some(format!("{}{}({}){}", &query[..start], name, col, &query[end..]));
        }
    }
    None
}

/// `parse_computed_label_shape` already knows how to derive at ingest time,
/// rather than a bare column.
pub struct ComputedValueAggMatch {
    pub label_type: &'static str,
    pub source_col: String,
    pub tokenizer: Option<String>,
    pub select: Option<String>,
    /// Synthetic column name standing in for the computed expression -
    /// unique per (label_type, source_col) pair so two different computed
    /// aggregates over the same source column in different queries share
    /// one registration.
    pub synthetic_label: String,
    /// `query` with the aggregate's computed-expression argument replaced
    /// by `synthetic_label`, everything else verbatim.
    pub surrogate: String,
}

const COMPUTED_VALUE_AGG_NAMES: &[&str] =
    &["MIN", "MAX", "SUM", "AVG", "UNIQEXACT", "UNIQ", "UNIQCOMBINED"];

/// Finds the first occurrence of `name(` in `query` that starts a real
/// function call (not a substring of a longer identifier, e.g. `SUM(` inside
/// `CHECKSUM(`), returning the byte range of `name(...)` including both
/// parens.
fn find_agg_call(query: &str, name: &str) -> Option<(usize, usize)> {
    let lower = query.to_lowercase();
    let needle = format!("{}(", name.to_lowercase());
    let mut search_from = 0;
    while let Some(rel_idx) = lower[search_from..].find(&needle) {
        let start = search_from + rel_idx;
        let preceded_by_ident = start > 0
            && lower.as_bytes()[start - 1].is_ascii_alphanumeric()
            || start > 0 && lower.as_bytes()[start - 1] == b'_';
        if !preceded_by_ident {
            let open_idx = start + name.len();
            if let Some(close_idx) = find_matching_close_paren(query, open_idx) {
                return Some((start, close_idx + 1));
            }
        }
        search_from = start + needle.len();
    }
    None
}

/// Finds `quantile(<level>)(<expr>)` (ClickHouse's parametric quantile call
/// syntax - two paren groups, unlike every name `find_agg_call` handles) in
/// `query`, returning `(level_literal, call_start, expr_open_idx,
/// expr_close_idx, call_end)`. `None` if there's no `quantile(` not
/// immediately followed (after its own closing paren) by a second `(`.
fn find_quantile_call(query: &str) -> Option<(String, usize, usize, usize, usize)> {
    let lower = query.to_lowercase();
    let mut search_from = 0;
    while let Some(rel_idx) = lower[search_from..].find("quantile(") {
        let start = search_from + rel_idx;
        let preceded_by_ident = start > 0
            && (lower.as_bytes()[start - 1].is_ascii_alphanumeric()
                || lower.as_bytes()[start - 1] == b'_');
        if !preceded_by_ident {
            let level_open = start + "quantile".len();
            if let Some(level_close) = find_matching_close_paren(query, level_open) {
                let after_level = &query[level_close + 1..];
                let after_level_trimmed = after_level.trim_start();
                if after_level_trimmed.starts_with('(') {
                    let expr_open = level_close + 1 + (after_level.len() - after_level_trimmed.len());
                    if let Some(expr_close) = find_matching_close_paren(query, expr_open) {
                        let level = query[level_open + 1..level_close].trim().to_string();
                        return Some((level, start, expr_open, expr_close, expr_close + 1));
                    }
                }
            }
        }
        search_from = start + "quantile(".len();
    }
    None
}

/// Detects `<AGG>(<computed-expr>) AS <alias>` where `<computed-expr>` is a
/// shape `parse_computed_label_shape` recognizes (token_select, token
/// count/string length, hour-of-day, day-of-week) rather than a bare
/// column - e.g. `avg(length(splitByChar(' ', as_path))) AS avg_len` or
/// `uniqExact(toDate(timestamp))`. Deliberately narrow, mirroring
/// `parse_computed_group_by_query`'s own scope: the first matching
/// aggregate call is taken as-is and its argument text is swapped for a
/// synthetic column name, leaving the rest of the query (GROUP BY, HAVING,
/// ORDER BY, LIMIT, other aggregates in a multi-aggregate split) untouched -
/// safe because by the time this runs on a single-surrogate branch (see
/// `get_computed_value_agg_streaming_aggregation_configs`), that branch
/// already carries exactly one aggregate.
pub fn parse_computed_value_agg_query(query: &str) -> Option<ComputedValueAggMatch> {
    // QUANTILE first: ClickHouse's parametric call syntax is TWO paren
    // groups (`quantile(<level>)(<expr>)`), unlike every name in
    // COMPUTED_VALUE_AGG_NAMES below (`name(<expr>)`, one group) - the
    // ordinary loop's find_agg_call would treat the level literal itself
    // as the aggregated expression, so this needs its own parser.
    if let Some((level, start, expr_open, expr_close, end)) = find_quantile_call(query) {
        let inner = query[expr_open + 1..expr_close].trim();
        if !inner.is_empty() && !is_bare_identifier(inner) {
            if let Some((label_type, source_col, tokenizer, select)) =
                parse_computed_label_shape(inner)
            {
                let synthetic_label =
                    format!("computed_{}_{}", label_type, source_col.to_lowercase());
                let surrogate = format!(
                    "{prefix}quantile({level})({synthetic_label}){suffix}",
                    prefix = &query[..start],
                    suffix = &query[end..],
                );
                return Some(ComputedValueAggMatch {
                    label_type,
                    source_col,
                    tokenizer,
                    select,
                    synthetic_label,
                    surrogate,
                });
            }
        }
    }

    for name in COMPUTED_VALUE_AGG_NAMES {
        let Some((start, end)) = find_agg_call(query, name) else {
            continue;
        };
        let open_idx = start + name.len();
        let inner = query[open_idx + 1..end - 1].trim();
        if inner.is_empty() || is_bare_identifier(inner) {
            continue;
        }
        let Some((label_type, source_col, tokenizer, select)) = parse_computed_label_shape(inner)
        else {
            continue;
        };
        let synthetic_label = format!(
            "computed_{}_{}",
            label_type,
            source_col.to_lowercase()
        );
        let surrogate = format!(
            "{}{}({}){}",
            &query[..start],
            name,
            synthetic_label,
            &query[end..]
        );
        return Some(ComputedValueAggMatch {
            label_type,
            source_col,
            tokenizer,
            select,
            synthetic_label,
            surrogate,
        });
    }
    None
}

/// Serve-time counterpart to asap-planner-rs's
/// `get_raw_value_agg_streaming_aggregation_configs` /
/// `get_computed_value_agg_streaming_aggregation_configs`: rewrites a live
/// MIN/MAX/SUM/AVG/uniqExact query over a raw (non-value) column - or a
/// computed expression / trivial toString format wrapper around one - into
/// the exact surrogate the planner registered (pointing at the metric's
/// `derived_value_<col>_<table>` virtual table), so `find_query_config_sql`'s
/// exact-metric-equality check can match it. Without this, every query the
/// planner-side mechanism plans would still never be served: the live
/// query's `metric` stays the original table, the registered template's
/// `metric` is the derived one, and `matches_sql_pattern` requires equality.
/// `schema` must be the query engine's live schema - by the time this runs,
/// a synthetic computed-value label is already a real metadata column on
/// every table (the planner adds every computed label name to every
/// table's `metadata_columns` in the registered config), so parsing the
/// computed-value case's rewritten surrogate against it resolves exactly
/// like the planner's own two-stage lowering did.
pub fn rewrite_raw_value_agg_query(query: &str, schema: &SQLSchema) -> Option<String> {
    // Trivial toString/numeric-cast wrapper first, e.g.
    // avg(toFloat64OrZero(toString(med))) -> avg(med).
    let unwrapped = strip_query_string_cast_wrapper(query);
    let query = unwrapped.as_deref().unwrap_or(query);

    // Computed-expression case: swap the computed expression for its
    // synthetic column, then fall through to the bare-column logic below
    // exactly as if the query had always referenced that column.
    let owned;
    let query = if let Some(m) = parse_computed_value_agg_query(query) {
        owned = m.surrogate;
        owned.as_str()
    } else {
        query
    };

    let stmts = SqlParser::parse_sql(&ClickHouseDialect {}, query).ok()?;
    let parser = SQLPatternParser::new(schema, 0.0);
    let qdata = parser.parse_query(&stmts)?;

    let agg_info = &qdata.aggregation_info;
    if !matches!(
        agg_info.get_name(),
        "MIN" | "MAX" | "SUM" | "AVG" | "CARDINALITY" | "QUANTILE"
    ) {
        return None;
    }
    let raw_col = agg_info.get_value_column_name();
    let table_name = &qdata.metric;

    if schema.is_valid_value_column(table_name, raw_col) {
        // Already a real value column - nothing for this mechanism to do.
        return None;
    }
    let is_time_col = schema
        .get_time_column(table_name)
        .is_some_and(|t| t == raw_col);
    let is_metadata_col = schema
        .get_metadata_columns(table_name)
        .is_some_and(|cols| cols.contains(raw_col));
    if !is_time_col && !is_metadata_col {
        return None;
    }

    let derived_metric = format!("derived_value_{}_{}", raw_col, table_name);
    Some(replace_from_table(query, &derived_metric))
}

fn parse_computed_label_shape(
    expr: &str,
) -> Option<(&'static str, String, Option<String>, Option<String>)> {
    let trimmed = expr.trim();
    let lower = trimmed.to_lowercase();

    // splitByChar('sep', col)[1] || '<lit1>' || splitByChar('sep', col)[2]
    // || '<lit2>' - concatenation of the first two split tokens from the
    // SAME column and separator, with literal text joining/following them
    // (q146's `octet1.octet2.0.0/16` supernet-from-prefix shape). Checked
    // first, before every other branch below: the whole expression starts
    // with `splitByChar(`, same as the bare single-token shape those
    // branches parse, but `strip_suffix(']')` on their own `?`-chained
    // parse would abort this *entire function* (not just that branch) the
    // moment it sees the trailing `|| '...'` tail instead of a lone `]` -
    // checking `||` up front avoids ever reaching that branch on this
    // shape. Exactly four top-level `||`-joined parts, alternating
    // token/literal; narrow by design - always the first two tokens
    // (ClickHouse indices 1 and 2), not arbitrary positions, since that's
    // the only shape any target query needs. The two literals are packed
    // into `select` separated by an unprintable byte (\u{1}) that can
    // never appear in a SQL string literal, so `compute_label_values`'s
    // "concat_two_tokens" arm can recover both without a delimiter
    // collision.
    if lower.contains("||") {
        let parts = split_top_level_concat(trimmed);
        if parts.len() == 4 {
            let tok0 = parse_split_by_char_index(&parts[0]);
            let lit1 = parse_string_literal_value(&parts[1]);
            let tok1 = parse_split_by_char_index(&parts[2]);
            let lit2 = parse_string_literal_value(&parts[3]);
            if let (Some((sep0, col0, idx0)), Some(lit1), Some((sep1, col1, idx1)), Some(lit2)) =
                (tok0, lit1, tok1, lit2)
            {
                if sep0 == sep1 && col0 == col1 && idx0 == 0 && idx1 == 1 {
                    return Some((
                        "concat_two_tokens",
                        col0,
                        Some(format!("char:{sep0}")),
                        Some(format!("{lit1}\u{1}{lit2}")),
                    ));
                }
            }
        }
        return None;
    }

    // A numeric-cast wrapper around one of the shapes below (e.g.
    // `toUInt16OrZero(splitByChar('.', prefix)[1])`, q169's "first octet as
    // a number" pattern) doesn't change what gets stored - every one of
    // these shapes already only ever produces digit strings for the
    // queries that use them (an IPv4 octet, a token count, a byte length),
    // so the cast's actual value-coercion is a no-op; only its data type
    // annotation is being discarded, which the engine already renders
    // correctly (ORDER BY on the computed alias sorts numerically whether
    // or not ClickHouse would have called it a UInt16). Unwrap and recurse
    // rather than duplicate the three shapes below under a cast prefix.
    if let Some(f) = NUMERIC_CAST_FUNCTIONS.iter().find(|f| lower.starts_with(**f)) {
        if let Some(close_idx) = find_matching_close_paren(trimmed, f.len() - 1) {
            if close_idx == trimmed.len() - 1 {
                let inner = trimmed[f.len()..close_idx].trim();
                return parse_computed_label_shape(inner);
            }
        }
    }

    fn parse_split_by_char_parts(inner: &str) -> Option<(char, String)> {
        let parts = split_top_level_commas(inner);
        let [sep_lit, col] = parts.as_slice() else {
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
        let col = col.trim();
        if !is_bare_identifier(col) {
            return None;
        }
        Some((sep_char, col.to_string()))
    }

    // splitByChar('sep', col)[n] - the nth token, 1-indexed (ClickHouse
    // array indexing), converted to token_select's 0-indexed `nth:N`.
    if lower.starts_with("splitbychar(") {
        let close_idx = find_matching_close_paren(trimmed, "splitbychar".len())?;
        let (sep_char, col) = parse_split_by_char_parts(&trimmed["splitbychar(".len()..close_idx])?;
        let rest = trimmed[close_idx + 1..].trim();
        let idx_str = rest.strip_prefix('[')?.strip_suffix(']')?;
        let n: usize = idx_str.trim().parse().ok()?;
        if n == 0 {
            return None; // 0 isn't a valid ClickHouse array index
        }
        return Some((
            "token_select",
            col,
            Some(format!("char:{sep_char}")),
            Some(format!("nth:{}", n - 1)),
        ));
    }

    // arraySlice(splitByChar('sep', col), start, len) - a slice of the
    // column's split tokens, ClickHouse 1-indexed offset + length (q174's
    // "first N hops of an AS path" shape). Rendered at ingest time as a
    // ClickHouse-style `['a','b']` array-literal string - opaque as a
    // GROUP BY key (referential equality is all that's needed to group
    // correctly), but still recognizable to a human reading the served
    // label.
    if lower.starts_with("arrayslice(") {
        let close_idx = find_matching_close_paren(trimmed, "arrayslice".len())?;
        if !trimmed[close_idx + 1..].trim().is_empty() {
            return None; // nothing may follow arraySlice(...) itself
        }
        let inner = &trimmed["arrayslice(".len()..close_idx];
        let args = split_top_level_commas(inner);
        let [split_expr, start_lit, len_lit] = args.as_slice() else {
            return None;
        };
        let split_expr = split_expr.trim();
        let split_lower = split_expr.to_lowercase();
        if !split_lower.starts_with("splitbychar(") {
            return None;
        }
        let sc_close = find_matching_close_paren(split_expr, "splitbychar".len())?;
        if !split_expr[sc_close + 1..].trim().is_empty() {
            return None;
        }
        let (sep_char, col) =
            parse_split_by_char_parts(&split_expr["splitbychar(".len()..sc_close])?;
        let start: usize = start_lit.trim().parse().ok()?;
        let len: usize = len_lit.trim().parse().ok()?;
        if start == 0 || len == 0 {
            return None; // 0 isn't a valid ClickHouse array offset/length
        }
        return Some((
            "array_slice",
            col,
            Some(format!("char:{sep_char}")),
            Some(format!("{}:{}", start - 1, len)),
        ));
    }

    // length(splitByChar('sep', col)) - token count.
    // length(col) - plain byte length of the raw column.
    if lower.starts_with("length(") {
        let close_idx = find_matching_close_paren(trimmed, "length".len())?;
        if !trimmed[close_idx + 1..].trim().is_empty() {
            return None; // nothing may follow length(...) itself
        }
        let inner = trimmed["length(".len()..close_idx].trim();
        let inner_lower = inner.to_lowercase();
        if inner_lower.starts_with("splitbychar(") {
            let sc_close = find_matching_close_paren(inner, "splitbychar".len())?;
            if !inner[sc_close + 1..].trim().is_empty() {
                return None;
            }
            let (sep_char, col) =
                parse_split_by_char_parts(&inner["splitbychar(".len()..sc_close])?;
            return Some(("split_length", col, Some(format!("char:{sep_char}")), None));
        }
        if is_bare_identifier(inner) {
            return Some(("string_length", inner.to_string(), None, None));
        }
        return None;
    }

    // toHour(col) - hour of day (0-23), collapsing the source column's
    // values across whatever date range the query covers into one of 24
    // cyclical buckets (e.g. "seasonality across the full month").
    if lower.starts_with("tohour(") {
        let close_idx = find_matching_close_paren(trimmed, "tohour".len())?;
        if !trimmed[close_idx + 1..].trim().is_empty() {
            return None; // nothing may follow toHour(...) itself
        }
        let inner = trimmed["tohour(".len()..close_idx].trim();
        if !is_bare_identifier(inner) {
            return None;
        }
        return Some(("hour_of_day", inner.to_string(), None, None));
    }

    // toDayOfWeek(col) - ISO weekday (1=Monday..7=Sunday, matching
    // ClickHouse's own toDayOfWeek convention), same cyclical-collapse
    // shape as toHour above.
    if lower.starts_with("todayofweek(") {
        let close_idx = find_matching_close_paren(trimmed, "todayofweek".len())?;
        if !trimmed[close_idx + 1..].trim().is_empty() {
            return None;
        }
        let inner = trimmed["todayofweek(".len()..close_idx].trim();
        if !is_bare_identifier(inner) {
            return None;
        }
        return Some(("day_of_week", inner.to_string(), None, None));
    }

    // toDate(col) - the calendar day the column's timestamp falls on. Same
    // shape as toHour/toDayOfWeek above, but a real (non-cyclical) bucket -
    // see the "date_bucket" branch of compute_label_values for what value
    // this label actually carries.
    if lower.starts_with("todate(") {
        let close_idx = find_matching_close_paren(trimmed, "todate".len())?;
        if !trimmed[close_idx + 1..].trim().is_empty() {
            return None;
        }
        let inner = trimmed["todate(".len()..close_idx].trim();
        if !is_bare_identifier(inner) {
            return None;
        }
        return Some(("date_bucket", inner.to_string(), None, None));
    }

    // toStartOfFiveMinutes(col) - the column's timestamp floored to the
    // nearest 5-minute mark (q187's "5-minute bucket trend" shape). Same
    // discretizing-computed-label treatment as toDate above (each bucket
    // is just another categorical GROUP BY value, not a real streaming
    // time-series window) - see the "five_minute_bucket" branch of
    // compute_label_values for what value this label actually carries.
    if lower.starts_with("tostartoffiveminutes(") {
        let close_idx = find_matching_close_paren(trimmed, "tostartoffiveminutes".len())?;
        if !trimmed[close_idx + 1..].trim().is_empty() {
            return None;
        }
        let inner = trimmed["tostartoffiveminutes(".len()..close_idx].trim();
        if !is_bare_identifier(inner) {
            return None;
        }
        return Some(("five_minute_bucket", inner.to_string(), None, None));
    }

    // toStartOfWeek(col) - the calendar week (Sunday-aligned, ClickHouse's
    // default mode 0) the column's timestamp falls in, as a "YYYY-MM-DD"
    // string (q133's weekly-bucketed MOAS shape). Same discretizing-
    // computed-label treatment as toDate/toStartOfFiveMinutes above - see
    // the "week_start_bucket" branch of compute_label_values for the exact
    // date arithmetic.
    if lower.starts_with("tostartofweek(") {
        let close_idx = find_matching_close_paren(trimmed, "tostartofweek".len())?;
        if !trimmed[close_idx + 1..].trim().is_empty() {
            return None;
        }
        let inner = trimmed["tostartofweek(".len()..close_idx].trim();
        if !is_bare_identifier(inner) {
            return None;
        }
        return Some(("week_start_bucket", inner.to_string(), None, None));
    }

    None
}

/// `splitByChar('sep', col)[n]` -> `(sep, col, n-1)` (0-indexed) - the same
/// shape the `splitByChar(...)[n]` branch above parses for token_select,
/// factored out standalone (rather than reused directly) so this doesn't
/// risk touching that branch's own already-verified logic.
fn parse_split_by_char_index(expr: &str) -> Option<(char, String, usize)> {
    let trimmed = expr.trim();
    let lower = trimmed.to_lowercase();
    if !lower.starts_with("splitbychar(") {
        return None;
    }
    let close_idx = find_matching_close_paren(trimmed, "splitbychar".len())?;
    let inner = &trimmed["splitbychar(".len()..close_idx];
    let parts = split_top_level_commas(inner);
    let [sep_lit, col] = parts.as_slice() else {
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
    let col = col.trim();
    if !is_bare_identifier(col) {
        return None;
    }
    let rest = trimmed[close_idx + 1..].trim();
    let idx_str = rest.strip_prefix('[')?.strip_suffix(']')?;
    let n: usize = idx_str.trim().parse().ok()?;
    if n == 0 {
        return None; // 0 isn't a valid ClickHouse array index
    }
    Some((sep_char, col.to_string(), n - 1))
}

/// A single-quoted SQL string literal's value, with the surrounding quotes
/// stripped - `None` if `expr` isn't quoted-literal shaped.
fn parse_string_literal_value(expr: &str) -> Option<String> {
    let trimmed = expr.trim();
    if !is_quoted_literal(trimmed) {
        return None;
    }
    Some(trimmed[1..trimmed.len() - 1].to_string())
}

/// Paren- and quote-aware top-level split on the `||` (string
/// concatenation) operator - same shape as `split_top_level_commas`/
/// `split_top_level_and` but for `||`.
fn split_top_level_concat(s: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut in_quotes = false;
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' {
            in_quotes = !in_quotes;
            current.push(c);
            i += 1;
            continue;
        }
        if !in_quotes {
            if c == '(' {
                depth += 1;
                current.push(c);
                i += 1;
                continue;
            }
            if c == ')' {
                depth -= 1;
                current.push(c);
                i += 1;
                continue;
            }
            if depth == 0 && c == '|' && i + 1 < chars.len() && chars[i + 1] == '|' {
                items.push(current.trim().to_string());
                current.clear();
                i += 2;
                continue;
            }
        }
        current.push(c);
        i += 1;
    }
    if !current.trim().is_empty() {
        items.push(current.trim().to_string());
    }
    items
}

const NUMERIC_CAST_FUNCTIONS: &[&str] = &[
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

/// `<numeric-cast>(<known_alias>)` -> `Some(<known_alias>)`, e.g.
/// `toUInt8OrZero(prefix_len)` with `known_alias = "prefix_len"` unwraps to
/// `"prefix_len"`. `None` for anything else (a different cast function, a
/// different inner expression, or already a bare identifier).
fn unwrap_numeric_cast(expr: &str, known_alias: &str) -> Option<String> {
    let trimmed = expr.trim();
    let lower = trimmed.to_lowercase();
    let f = NUMERIC_CAST_FUNCTIONS.iter().find(|f| lower.starts_with(**f))?;
    if !trimmed.ends_with(')') {
        return None;
    }
    let inner = trimmed[f.len()..trimmed.len() - 1].trim();
    if inner == known_alias {
        Some(known_alias.to_string())
    } else {
        None
    }
}

/// `argMax(x, y)` / `argMin(x, y)`, scoped deliberately narrow: `y` must be
/// the table's own time column (validated by the caller, which has schema
/// access this text-only parser doesn't) - see `DerivedValueKind::ArgMax`'s
/// doc comment for why. The shape matched is exactly what both real target
/// queries (and `build_multi_aggregate_surrogates`' own split-out
/// surrogate, for a query that also has another aggregate alongside the
/// argMax/argMin) produce: one real GROUP BY column, one argMax/argMin
/// call, no ORDER BY (this mechanism's result is a label, not a value -
/// sorting it isn't built).
pub struct ArgAggMatch {
    pub is_max: bool,
    pub arg_col: String,
    pub cmp_col: String,
    pub alias: String,
    pub group_by_col: String,
    pub from_where: String,
    pub limit: Option<String>,
}

pub fn parse_arg_agg_query(query: &str) -> Option<ArgAggMatch> {
    let lower = query.to_lowercase();
    let select_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_end > from_idx {
        return None;
    }

    let items = split_top_level_commas(&query[select_end..from_idx]);
    if items.len() != 2 {
        return None;
    }

    let group_by_col = items[0].trim();
    if !is_bare_identifier(group_by_col) {
        return None;
    }

    let (agg_expr, alias) = split_expr_alias(items[1].trim())?;
    let agg_expr = agg_expr.trim();
    let agg_lower = agg_expr.to_lowercase();
    let (is_max, prefix_len) = if agg_lower.starts_with("argmax(") {
        (true, "argmax(".len())
    } else if agg_lower.starts_with("argmin(") {
        (false, "argmin(".len())
    } else {
        return None;
    };
    let close_idx = find_matching_close_paren(agg_expr, prefix_len - 1)?;
    if close_idx != agg_expr.len() - 1 {
        return None; // nothing may trail the call itself
    }
    let args = split_top_level_commas(&agg_expr[prefix_len..close_idx]);
    let [arg_col, cmp_col] = args.as_slice() else {
        return None;
    };
    let arg_col = arg_col.trim();
    let cmp_col = cmp_col.trim();
    if !is_bare_identifier(arg_col) || !is_bare_identifier(cmp_col) {
        return None;
    }

    let after_from = query[from_idx + "from".len()..].trim_start();
    if after_from.starts_with('(') {
        return None;
    }
    if lower[from_idx..].matches("select").count() > 0 {
        return None;
    }

    let group_idx = lower.find("group by")?;
    let from_where = query[from_idx..group_idx].trim_end().to_string();

    let after_group = &query[group_idx + "group by".len()..];
    let after_group_lower = after_group.to_lowercase();
    // No ORDER BY - this mechanism's result is a label (a string), not a
    // numeric value; sorting by it isn't built. A query with ORDER BY on
    // this alias isn't matched at all here, and falls through to punting
    // like any other unsupported shape.
    if after_group_lower.contains("order by") {
        return None;
    }
    let group_end = after_group_lower
        .find("limit")
        .unwrap_or(after_group.len());
    let group_by_clause = after_group[..group_end].trim().trim_end_matches(';').trim();
    if group_by_clause != group_by_col {
        return None;
    }

    let limit_text = after_group[group_end..].trim().trim_end_matches(';').trim();
    let limit = if limit_text.is_empty() {
        None
    } else {
        let n = limit_text.strip_prefix("LIMIT").or_else(|| limit_text.strip_prefix("limit"))?;
        Some(n.trim().to_string())
    };

    Some(ArgAggMatch {
        is_max,
        arg_col: arg_col.to_string(),
        cmp_col: cmp_col.to_string(),
        alias,
        group_by_col: group_by_col.to_string(),
        from_where,
        limit,
    })
}

pub fn parse_computed_group_by_query(query: &str) -> Option<ComputedGroupByMatch> {
    let lower = query.to_lowercase();
    let select_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_end > from_idx {
        return None;
    }

    let items = split_top_level_commas(&query[select_end..from_idx]);
    if items.len() < 2 {
        return None;
    }
    let (computed_expr, alias) = split_expr_alias(items[0].trim())?;
    let (label_type, source_col, tokenizer, select) = parse_computed_label_shape(&computed_expr)?;

    // A nested-subquery FROM belongs to other patterns.
    let after_from = query[from_idx + "from".len()..].trim_start();
    if after_from.starts_with('(') {
        return None;
    }
    // A nested SELECT inside the WHERE clause isn't evaluable by the
    // ingest-time spatial filter - same reasoning as
    // looks_like_unevaluable_where_subquery_sql.
    if lower[from_idx..].matches("select").count() > 0 {
        return None;
    }

    let group_idx = lower.find("group by")?;
    let from_where = query[from_idx..group_idx].trim_end().to_string();

    let after_group = &query[group_idx + "group by".len()..];
    let after_group_lower = after_group.to_lowercase();
    let group_end = after_group_lower
        .find("order by")
        .or_else(|| after_group_lower.find("limit"))
        .unwrap_or(after_group.len());
    let group_by_clause = after_group[..group_end].trim().trim_end_matches(';').trim();
    // The computed alias must be one of the GROUP BY columns. Any other
    // columns alongside it must be plain identifiers (real columns, not a
    // second computed expression - that combination isn't built) - the
    // surrogate reuses this text verbatim, so anything else here would
    // need its own rewriting the surrogate builder doesn't do.
    let group_by_parts = split_top_level_commas(group_by_clause);
    if !group_by_parts.iter().any(|p| p.trim() == alias) {
        return None;
    }
    if group_by_parts
        .iter()
        .any(|p| p.trim() != alias && !is_bare_identifier(p.trim()))
    {
        return None;
    }
    let group_by_clause = group_by_clause.to_string();

    let mut order_by_and_limit = after_group[group_end..]
        .trim()
        .trim_end_matches(';')
        .to_string();

    // `ORDER BY <numeric-cast>(<alias>)` (e.g. `toUInt8OrZero(prefix_len)`)
    // is how analysts sort a computed string column numerically rather
    // than lexicographically ("2" before "10"). Unwrap it to the bare
    // alias before the classic single-aggregate parser this surrogate goes
    // through ever sees it - it only accepts a bare column/alias for ORDER
    // BY and would fail loudly rather than punt cleanly on the cast form.
    // This is safe, not just convenient: sort_and_truncate_instant_vector's
    // label-comparison branch already parses both sides as f64 and falls
    // back to lexicographic only when that fails, so a bare `prefix_len`
    // sorts numerically on its own - the cast wrapper was never adding
    // ordering behavior the engine didn't already have.
    let (initial_order_by_items, _) = parse_order_by_and_limit(&order_by_and_limit);
    for item in &initial_order_by_items {
        if let Some(unwrapped) = unwrap_numeric_cast(&item.column, &alias) {
            order_by_and_limit = order_by_and_limit.replacen(&item.column, &unwrapped, 1);
        }
    }

    // Conservatively reject any remaining ORDER BY item that isn't a bare
    // identifier - anything other than the numeric-cast shape just handled
    // is left for a future pass.
    let (order_by_items, _) = parse_order_by_and_limit(&order_by_and_limit);
    if order_by_items.iter().any(|item| !is_bare_identifier(&item.column)) {
        return None;
    }

    let aggregate_and_rest = items[1..]
        .iter()
        .map(|s| s.trim())
        .collect::<Vec<_>>()
        .join(", ");

    Some(ComputedGroupByMatch {
        alias,
        label_type,
        source_col,
        tokenizer,
        select,
        aggregate_and_rest,
        from_where,
        order_by_and_limit,
        group_by_clause,
    })
}

/// Reference the computed alias as if it were an ordinary metadata column -
/// the label itself replaces the computed expression entirely, since by
/// ingest time it genuinely is a real column (see `ComputedLabelConfig`).
pub fn build_computed_group_by_surrogate(m: &ComputedGroupByMatch) -> String {
    format!(
        "SELECT {alias}, {rest} {from_where} GROUP BY {group_by_clause} {tail}",
        alias = m.alias,
        rest = m.aggregate_and_rest,
        from_where = m.from_where,
        group_by_clause = m.group_by_clause,
        tail = m.order_by_and_limit,
    )
}

/// Rewrites an incoming raw query matching `ComputedGroupByMatch` into its
/// surrogate - the same shape `get_computed_group_by_streaming_aggregation_configs`
/// registered at planning time, so the query engine's ordinary structural
/// matcher (`find_query_config_sql`) finds it. Called from
/// `rewrite_recognized_pattern` in the query engine, the same entry point
/// `rewrite_token_select_query`/`rewrite_token_explode_query` use.
pub fn rewrite_computed_group_by_query(query: &str) -> Option<String> {
    let m = parse_computed_group_by_query(query)?;
    Some(build_computed_group_by_surrogate(&m))
}

// ---------------------------------------------------------------------------
// Bare-key HAVING: `SELECT <cols> FROM ... GROUP BY <same cols> HAVING
// count(*) <op> <n>` - no aggregate anywhere in the SELECT list at all, e.g.
// `SELECT prefix FROM ... GROUP BY prefix HAVING count(*) = 1` ("prefixes
// announced exactly once"). A hidden count drives the filter but never
// appears in the output. Lowered to `SELECT <cols>, count(*) AS
// __having_count__ ... GROUP BY <cols> HAVING __having_count__ <op> <n>` -
// exactly the classic single-aggregate + HAVING shape already supported -
// and served by dropping the count column from the rendered output (see
// handle_group_by_having_count_sql in the query engine, using the same
// `has_value: false` mechanism SELECT DISTINCT uses).
// ---------------------------------------------------------------------------

/// Sentinel alias for the hidden `count(*)` this shape's surrogate
/// introduces. The planner (`asap-planner-rs/src/planner/sql.rs`) checks for
/// this exact alias to force exact (not approximate) treatment for this
/// query shape specifically - an exact-equality HAVING filter is maximally
/// sensitive to sketch noise, unlike a range-tolerant classic HAVING filter,
/// so this shape alone opts out of the default approximate COUNT treatment.
pub const HAVING_COUNT_ALIAS: &str = "__having_count__";

#[derive(Debug, Clone)]
pub struct GroupByHavingCountMatch {
    pub columns: Vec<String>,
    /// "FROM ... WHERE ..." (or just "FROM ..."), verbatim.
    pub from_where: String,
    /// One of "=", "!=", "<>", "<", "<=", ">", ">=".
    pub having_op_text: String,
    pub having_value: u64,
    /// Raw `ORDER BY ... LIMIT ...` tail (either or both, or empty), verbatim.
    pub order_by_and_limit: String,
}

pub fn parse_group_by_having_count_query(query: &str) -> Option<GroupByHavingCountMatch> {
    let lower = query.to_lowercase();
    let select_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_end > from_idx {
        return None;
    }

    let columns: Vec<String> = split_top_level_commas(&query[select_end..from_idx])
        .into_iter()
        .map(|s| s.trim().to_string())
        .collect();
    if columns.is_empty() || !columns.iter().all(|c| is_bare_identifier(c)) {
        return None;
    }

    let after_from = query[from_idx + "from".len()..].trim_start();
    if after_from.starts_with('(') {
        return None;
    }
    if lower[from_idx..].matches("select").count() > 0 {
        return None;
    }

    let group_idx = lower.find("group by")?;
    let from_where = query[from_idx..group_idx].trim_end().to_string();

    let after_group = &query[group_idx + "group by".len()..];
    let after_group_lower = after_group.to_lowercase();
    let having_idx = after_group_lower.find("having")?;
    let group_by_cols: Vec<String> = split_top_level_commas(&after_group[..having_idx])
        .into_iter()
        .map(|s| s.trim().to_string())
        .collect();
    // Group-by columns must be exactly the SELECT-list columns (same set,
    // same count) - a query grouping by something the output doesn't show
    // isn't this shape.
    if group_by_cols.len() != columns.len() || !columns.iter().all(|c| group_by_cols.contains(c)) {
        return None;
    }

    let after_having = after_group[having_idx + "having".len()..].trim_start();
    let ah_lower = after_having.to_lowercase();
    let count_prefix_len = if ah_lower.starts_with("count(*)") {
        "count(*)".len()
    } else if ah_lower.starts_with("count()") {
        "count()".len()
    } else {
        return None;
    };
    let rest = after_having[count_prefix_len..].trim_start();

    let (op_text, op_len) = ["<=", ">=", "!=", "<>", "=", "<", ">"]
        .iter()
        .find_map(|op| rest.starts_with(op).then_some((*op, op.len())))?;
    let after_op = rest[op_len..].trim_start();
    let digits_end = after_op
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after_op.len());
    if digits_end == 0 {
        return None;
    }
    let having_value: u64 = after_op[..digits_end].parse().ok()?;
    let order_by_and_limit = after_op[digits_end..].trim().trim_end_matches(';').to_string();

    // Conservatively reject any ORDER BY item that isn't a bare identifier -
    // same reasoning as parse_computed_group_by_query.
    let (order_by_items, _) = parse_order_by_and_limit(&order_by_and_limit);
    if order_by_items.iter().any(|item| !is_bare_identifier(&item.column)) {
        return None;
    }

    Some(GroupByHavingCountMatch {
        columns,
        from_where,
        having_op_text: op_text.to_string(),
        having_value,
        order_by_and_limit,
    })
}

pub fn build_group_by_having_count_surrogate(m: &GroupByHavingCountMatch) -> String {
    let cols = m.columns.join(", ");
    format!(
        "SELECT {cols}, count(*) AS {alias} {from_where} GROUP BY {cols} HAVING {alias} {op} {val} {tail}",
        cols = cols,
        alias = HAVING_COUNT_ALIAS,
        from_where = m.from_where,
        op = m.having_op_text,
        val = m.having_value,
        tail = m.order_by_and_limit,
    )
}

pub fn rewrite_group_by_having_count_query(query: &str) -> Option<String> {
    let m = parse_group_by_having_count_query(query)?;
    Some(build_group_by_having_count_surrogate(&m))
}

// ---------------------------------------------------------------------------
// Hidden-countIf HAVING: `SELECT <col>, <agg>(<arg>) AS <alias> FROM ...
// GROUP BY <col> HAVING countIf(<cond1>) <op1> <n1> [AND countIf(<cond2>)
// <op2> <n2> ...]` - one real, exposed aggregate in the SELECT list, plus
// one or more countIf(...) conditions used ONLY as a HAVING filter, never
// selected (q113's `count(*) AS withdrawal_cnt ... HAVING countIf(operation
// = 'A') = 0 AND countIf(operation = 'W') > 0` - "peers with only
// withdrawals, no announcements"). Different from
// `parse_multi_aggregate_having` (which only resolves HAVING against
// aliases *already exposed* in the SELECT list) and from
// `GroupByHavingCountMatch` (which has no SELECT-list aggregate at all,
// just a single bare `count(*)` HAVING). Each hidden countIf becomes its
// own independent surrogate (same countIf -> count()+WHERE-fold trick
// `build_multi_aggregate_surrogates` uses), executed and merged with the
// exposed aggregate at serve time purely to filter rows - see
// handle_hidden_countif_having_sql in the query engine, which renders only
// the exposed column, exactly like an ordinary classic single-aggregate
// result.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct HiddenCountifHavingMatch {
    pub group_col: String,
    /// The exposed aggregate's own expression, e.g. "count(*)".
    pub agg_expr: String,
    pub alias: String,
    /// "FROM ... WHERE ..." (or just "FROM ..."), verbatim.
    pub from_where: String,
    /// `(condition, op, threshold)` per hidden `countIf(<condition>) <op>
    /// <threshold>` HAVING clause, in original order. Always 1 or more.
    pub hidden: Vec<(String, String, f64)>,
    /// Raw `ORDER BY ... LIMIT ...` tail (either or both, or empty), verbatim.
    pub order_by_and_limit: String,
}

pub fn parse_hidden_countif_having_query(query: &str) -> Option<HiddenCountifHavingMatch> {
    let lower = query.to_lowercase();
    let select_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_end > from_idx {
        return None;
    }

    let items = split_top_level_commas(&query[select_end..from_idx]);
    if items.len() != 2 {
        return None;
    }
    let group_col = items[0].trim();
    if !is_bare_identifier(group_col) {
        return None;
    }
    let (agg_expr, alias) = split_expr_alias(items[1].trim())?;
    let agg_upper = agg_expr.to_uppercase();
    if !AGGREGATE_FUNCTIONS.iter().any(|f| agg_upper.contains(f)) {
        return None;
    }

    let after_from = query[from_idx + "from".len()..].trim_start();
    if after_from.starts_with('(') {
        return None;
    }
    if lower[from_idx..].matches("select").count() > 0 {
        return None;
    }

    let group_idx = lower.find("group by")?;
    let from_where = query[from_idx..group_idx].trim_end().to_string();

    let after_group = &query[group_idx + "group by".len()..];
    let after_group_lower = after_group.to_lowercase();
    let having_idx = after_group_lower.find("having")?;
    let group_by_clause = after_group[..having_idx].trim().to_string();
    if group_by_clause != group_col {
        return None; // exactly the one exposed group column
    }

    let after_having = &after_group[having_idx + "having".len()..];
    let after_having_lower = after_having.to_lowercase();
    let having_end = after_having_lower
        .find("order by")
        .or_else(|| after_having_lower.find("limit"))
        .unwrap_or(after_having.len());
    let having_text = &after_having[..having_end];
    let order_by_and_limit = after_having[having_end..]
        .trim()
        .trim_end_matches(';')
        .to_string();

    let clauses = split_top_level_and(having_text);
    if clauses.is_empty() {
        return None;
    }
    let mut hidden = Vec::new();
    for clause in clauses {
        let clause = clause.trim();
        let clause_lower = clause.to_lowercase();
        if !clause_lower.starts_with("countif(") {
            return None;
        }
        let close_idx = find_matching_close_paren(clause, "countif".len())?;
        let cond = clause["countif(".len()..close_idx].trim().to_string();
        if !is_ingest_filter_safe_condition(&cond) {
            return None;
        }
        let rest = clause[close_idx + 1..].trim();
        let (op_text, op_len) = ["<=", ">=", "!=", "<>", "=", "<", ">"]
            .iter()
            .find_map(|op| rest.starts_with(op).then_some((*op, op.len())))?;
        let threshold: f64 = rest[op_len..].trim().parse().ok()?;
        hidden.push((cond, op_text.to_string(), threshold));
    }

    let (order_by_items, _) = parse_order_by_and_limit(&order_by_and_limit);
    if order_by_items
        .iter()
        .any(|item| item.column != group_col && item.column != alias)
    {
        return None;
    }

    Some(HiddenCountifHavingMatch {
        group_col: group_col.to_string(),
        agg_expr,
        alias,
        from_where,
        hidden,
        order_by_and_limit,
    })
}

/// First surrogate is the exposed aggregate (unchanged column order/alias,
/// so this drops straight into the classic single-aggregate pipeline like
/// any other simple GROUP BY query); one further surrogate per hidden
/// countIf, each folding its own condition into the WHERE clause and
/// counting under a synthetic per-index alias.
pub fn build_hidden_countif_having_surrogates(m: &HiddenCountifHavingMatch) -> Vec<String> {
    let mut surrogates = vec![format!(
        "SELECT {col}, {agg} AS {alias} {from_where} GROUP BY {col}",
        col = m.group_col,
        agg = m.agg_expr,
        alias = m.alias,
        from_where = m.from_where,
    )];
    let where_has_clause = m.from_where.to_uppercase().contains("WHERE");
    for (i, (cond, _, _)) in m.hidden.iter().enumerate() {
        let joiner = if where_has_clause { "AND" } else { "WHERE" };
        surrogates.push(format!(
            "SELECT {col}, count(*) AS {alias} {from_where} {joiner} ({cond}) GROUP BY {col}",
            col = m.group_col,
            alias = hidden_having_alias(i),
            from_where = m.from_where,
            joiner = joiner,
            cond = cond,
        ));
    }
    surrogates
}

/// Synthetic alias for the `i`th hidden countIf branch's surrogate -
/// shared between the surrogate builder and the query engine's serve-time
/// handler, which never actually reads the alias text itself (each
/// surrogate is executed and read back independently, keyed by its
/// position in `m.hidden`), but keeps both sides' surrogate text
/// byte-identical.
pub fn hidden_having_alias(i: usize) -> String {
    format!("__hidden_having_{i}__")
}

// ---------------------------------------------------------------------------
// Two-stage histogram: `SELECT <outer_col>, count(*) AS <outer_alias> FROM
// (SELECT <inner_col>, count(*) AS <inner_alias> FROM ... GROUP BY
// <inner_col>) GROUP BY <outer_col>` where `<outer_col>` is exactly the
// inner query's own result column (q078's "how many prefixes had N
// updates" histogram: group prefixes by update count, then count how many
// prefixes land in each bucket). The inner query is a completely ordinary,
// already-supported classic single-aggregate GROUP BY - only registered as
// itself, nothing new. The outer stage isn't a real second precompute at
// all: it's a re-aggregation of the inner query's OWN result set, computed
// entirely at serve time by executing the inner query through the normal
// classic pipeline and tallying its output rows locally - see
// handle_two_stage_histogram_sql in the query engine. Narrow by design:
// both stages must be exactly `count(*)`/`count()`, the only aggregate any
// target query needs.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TwoStageHistogramMatch {
    pub outer_alias: String,
    pub outer_group_col: String,
    pub inner_group_col: String,
    pub inner_alias: String,
    /// The INNER query's own "FROM ... WHERE ..." (or just "FROM ..."), verbatim.
    pub from_where: String,
    /// The OUTER query's raw `ORDER BY ... LIMIT ...` tail, verbatim.
    pub order_by_and_limit: String,
}

fn is_bare_count_star(expr: &str) -> bool {
    let lower = expr.trim().to_lowercase();
    lower == "count(*)" || lower == "count()"
}

pub fn parse_two_stage_histogram_query(query: &str) -> Option<TwoStageHistogramMatch> {
    let lower = query.to_lowercase();
    let select_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_end > from_idx {
        return None;
    }

    let outer_items = split_top_level_commas(&query[select_end..from_idx]);
    if outer_items.len() != 2 {
        return None;
    }
    let outer_group_col = outer_items[0].trim();
    if !is_bare_identifier(outer_group_col) {
        return None;
    }
    let (outer_agg_expr, outer_alias) = split_expr_alias(outer_items[1].trim())?;
    if !is_bare_count_star(&outer_agg_expr) {
        return None;
    }

    let after_from = &query[from_idx + "from".len()..];
    let after_from_trimmed = after_from.trim_start();
    if !after_from_trimmed.starts_with('(') {
        return None;
    }
    let subquery_open =
        from_idx + "from".len() + (after_from.len() - after_from_trimmed.len());
    let subquery_close = find_matching_close_paren(query, subquery_open)?;

    let inner_query = query[subquery_open + 1..subquery_close].trim();
    let inner_lower = inner_query.to_lowercase();
    let inner_select_end = inner_lower.find("select")? + "select".len();
    let inner_from_idx = inner_lower.find("from")?;
    if inner_select_end > inner_from_idx {
        return None;
    }
    // No further nested SELECT/subquery inside the inner query - a
    // three-level shape isn't handled.
    if inner_lower[inner_from_idx..].matches("select").count() > 0 {
        return None;
    }
    let inner_items = split_top_level_commas(&inner_query[inner_select_end..inner_from_idx]);
    if inner_items.len() != 2 {
        return None;
    }
    let inner_group_col = inner_items[0].trim();
    if !is_bare_identifier(inner_group_col) {
        return None;
    }
    let (inner_agg_expr, inner_alias) = split_expr_alias(inner_items[1].trim())?;
    if !is_bare_count_star(&inner_agg_expr) {
        return None;
    }

    // The outer stage must group by exactly the inner's own result column
    // - anything else isn't a re-aggregation of the inner's output.
    if outer_group_col != inner_alias {
        return None;
    }

    let inner_group_idx = inner_lower.find("group by")?;
    let inner_from_where = inner_query[inner_from_idx..inner_group_idx]
        .trim_end()
        .to_string();
    let inner_after_group = inner_query[inner_group_idx + "group by".len()..]
        .trim()
        .trim_end_matches(';')
        .to_string();
    if inner_after_group != inner_group_col {
        // No HAVING/ORDER BY/LIMIT inside the inner query, and GROUP BY
        // must be exactly its own one column - conservative, matching
        // every other narrow shape in this module.
        return None;
    }

    let outer_tail = &query[subquery_close + 1..];
    let outer_tail_lower = outer_tail.to_lowercase();
    let outer_group_idx = outer_tail_lower.find("group by")?;
    let after_outer_group = &outer_tail[outer_group_idx + "group by".len()..];
    let after_outer_group_lower = after_outer_group.to_lowercase();
    let group_end = after_outer_group_lower
        .find("order by")
        .or_else(|| after_outer_group_lower.find("limit"))
        .unwrap_or(after_outer_group.len());
    let outer_group_by_clause = after_outer_group[..group_end].trim().to_string();
    if outer_group_by_clause != outer_group_col {
        return None;
    }
    let order_by_and_limit = after_outer_group[group_end..]
        .trim()
        .trim_end_matches(';')
        .to_string();

    let (order_by_items, _) = parse_order_by_and_limit(&order_by_and_limit);
    if order_by_items
        .iter()
        .any(|item| item.column != outer_group_col && item.column != outer_alias)
    {
        return None;
    }

    Some(TwoStageHistogramMatch {
        outer_alias,
        outer_group_col: outer_group_col.to_string(),
        inner_group_col: inner_group_col.to_string(),
        inner_alias,
        from_where: inner_from_where,
        order_by_and_limit,
    })
}

/// The INNER query, standing alone - an entirely ordinary classic
/// single-aggregate GROUP BY, registered/served exactly like any other
/// such query. Shared between the planner (registration) and the query
/// engine (serve-time execution of this same surrogate as one leg of the
/// two-stage read).
pub fn build_two_stage_histogram_inner_surrogate(m: &TwoStageHistogramMatch) -> String {
    format!(
        "SELECT {col}, count(*) AS {alias} {from_where} GROUP BY {col}",
        col = m.inner_group_col,
        alias = m.inner_alias,
        from_where = m.from_where,
    )
}

// Multi-aggregate pattern: a flat (no subquery) query with one GROUP BY and
// 2+ independent aggregate expressions in the SELECT list, e.g.
//   SELECT collector, peer_ip, count() AS updates, uniqExact(prefix) AS distinct_prefixes
//   FROM ... GROUP BY collector, peer_ip
// The classic SQLQueryData model tracks exactly one aggregate per query, so
// this can't be planned/matched as-is. It splits cleanly into N independent
// single-aggregate queries sharing the same FROM/WHERE/GROUP BY - each one
// individually is exactly the shape the classic path already handles, so
// this building block only has to handle the split, not reimplement
// aggregation planning or matching.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MultiAggregateMatch {
    /// The non-aggregate SELECT-list items (the GROUP BY columns), in
    /// original order.
    pub group_by_cols: Vec<String>,
    /// The aggregate SELECT-list items (e.g. "count() AS updates"), in
    /// original order. Always 2 or more.
    pub aggregate_exprs: Vec<String>,
    /// "FROM ... WHERE ..." (or just "FROM ..." with no WHERE), verbatim.
    pub from_where: String,
    /// Raw text of the GROUP BY clause (column list), verbatim.
    pub group_by_clause: String,
    /// `HAVING <alias1> <op1> <n1> [AND <alias2> <op2> <n2> ...]` - zero or
    /// more independent AND-joined clauses, each comparing one exposed
    /// aggregate alias to a constant (e.g. q026's `HAVING withdrawals > 5
    /// AND announcements > 5`). Every value already fetched per-surrogate
    /// at serve time, so this is evaluated there with no new registration -
    /// see handle_multi_aggregate_sql. Empty means no HAVING clause.
    pub having: Vec<(String, String, f64)>,
    /// `ORDER BY (<alias1> + <alias2>) [ASC|DESC]` - a derived sort key
    /// summing exactly two exposed aliases (q063's `ORDER BY (ann_cnt +
    /// with_cnt) DESC`), computed at serve time from their already-fetched
    /// values. `None` when ORDER BY (if any) is the ordinary bare-alias
    /// form handled generically by sort_and_truncate_instant_vector.
    pub order_by_sum: Option<(String, String, bool)>,
    /// Raw `ORDER BY ... LIMIT ...` tail (either or both, or empty), verbatim.
    pub order_by_and_limit: String,
    /// Set when the GROUP BY key is a computed expression rather than a
    /// bare column (q091's `length(splitByChar(' ', as_path)) AS
    /// path_len`) - `(alias, label_type, source_col, tokenizer, select)`,
    /// the same 4-tuple shape `parse_computed_label_shape` returns, plus
    /// the alias. The caller (get_streaming_aggregation_configs's
    /// multi-aggregate split loop) must register this as a
    /// `ComputedLabelConfig` and add `alias` to every surrogate's schema
    /// before planning them - see
    /// get_computed_group_by_with_computed_value_streaming_aggregation_configs's
    /// doc comment for the sibling single-aggregate case this generalizes.
    pub computed_group_by: Option<(String, &'static str, String, Option<String>, Option<String>)>,
}

/// Paren- and quote-aware top-level comma split - a SELECT list item like
/// `count(distinct foo)` must not be split on the comma inside its own
/// argument list.
pub(crate) fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut in_quotes = false;
    for c in s.chars() {
        if c == '\'' {
            in_quotes = !in_quotes;
            current.push(c);
            continue;
        }
        if !in_quotes {
            if c == '(' {
                depth += 1;
                current.push(c);
                continue;
            }
            if c == ')' {
                depth -= 1;
                current.push(c);
                continue;
            }
            if c == ',' && depth == 0 {
                items.push(current.trim().to_string());
                current.clear();
                continue;
            }
        }
        current.push(c);
    }
    if !current.trim().is_empty() {
        items.push(current.trim().to_string());
    }
    items
}

/// Earliest-starting comparison operator in `s`; among ties at the same
/// start index (e.g. "<=" and "<" both start where the "<" is), the longer
/// operator wins, so "<=" isn't mis-split as "<" followed by "= n". Shared
/// by the multi-aggregate HAVING-clause parser and derived-ratio's
/// HAVING-sum parser (kept separate there since it's tangled up with sum-
/// specific parsing, not worth a risky refactor to merge post hoc).
fn find_comparison_op(s: &str) -> Option<(&'static str, usize)> {
    let mut best: Option<(&str, usize)> = None;
    for op in ["<=", ">=", "!=", "<>", "=", "<", ">"] {
        if let Some(i) = s.find(op) {
            best = match best {
                None => Some((op, i)),
                Some((best_op, best_i)) if i < best_i || (i == best_i && op.len() > best_op.len()) => {
                    Some((op, i))
                }
                other => other,
            };
        }
    }
    best
}

/// Parses `HAVING <alias1> <op1> <n1> [AND <alias2> <op2> <n2> ...]` -
/// every clause must independently compare one of `known_aliases` to a
/// constant. `None` for any other HAVING shape (a sum, an OR, a clause
/// referencing an unknown column, ...) - conservative, same reasoning as
/// every other detector in this module.
fn parse_multi_aggregate_having(
    having_text: &str,
    known_aliases: &[String],
) -> Option<Vec<(String, String, f64)>> {
    let clauses = split_top_level_and(having_text);
    if clauses.is_empty() {
        return None;
    }
    clauses
        .into_iter()
        .map(|clause| {
            let (op_text, op_idx) = find_comparison_op(&clause)?;
            let alias = clause[..op_idx].trim();
            if !known_aliases.iter().any(|a| a == alias) {
                return None;
            }
            let threshold: f64 = clause[op_idx + op_text.len()..].trim().parse().ok()?;
            Some((alias.to_string(), op_text.to_string(), threshold))
        })
        .collect()
}

/// Parses `(<alias1> + <alias2>)` as an ORDER BY target, where both
/// aliases are known - the derived-sort-key shape (q063's `ORDER BY
/// (ann_cnt + with_cnt) DESC`). `None` for anything else, including a
/// bare alias (handled generically elsewhere).
fn parse_order_by_sum(order_text: &str, known_aliases: &[String]) -> Option<(String, String, bool)> {
    let trimmed = order_text.trim();
    let lower = trimmed.to_lowercase();
    let (target, descending) = if let Some(stripped) = lower.strip_suffix("desc") {
        (trimmed[..stripped.len()].trim(), true)
    } else if let Some(stripped) = lower.strip_suffix("asc") {
        (trimmed[..stripped.len()].trim(), false)
    } else {
        (trimmed, true)
    };
    let inner = target.strip_prefix('(')?.strip_suffix(')')?;
    let (a, b) = split_top_level_char(inner, '+')?;
    let a = a.trim().to_string();
    let b = b.trim().to_string();
    if known_aliases.contains(&a) && known_aliases.contains(&b) {
        Some((a, b, descending))
    } else {
        None
    }
}

pub fn parse_multi_aggregate_query(query: &str) -> Option<MultiAggregateMatch> {
    let lower = query.to_lowercase();

    let select_kw_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_kw_end > from_idx {
        return None;
    }

    // A nested-subquery FROM belongs to other patterns (MOAS/token-select/
    // token-explode), not this one.
    let after_from = query[from_idx + "from".len()..].trim_start();
    if after_from.starts_with('(') {
        return None;
    }

    // A nested SELECT inside the WHERE clause (`prefix NOT IN (SELECT ...)`,
    // `origin IN (SELECT ...)`) isn't evaluable by the ingest-time spatial
    // filter, which only understands literal equality/inequality/IN-list
    // clauses - it has no way to run a correlated subquery against the
    // streaming data. Bail out entirely rather than silently building a
    // filter that can never match correctly; the caller falls through to
    // is_exact_only / punting instead.
    if lower[from_idx..].matches("select").count() > 0 {
        return None;
    }

    let select_list = &query[select_kw_end..from_idx];
    let items = split_top_level_commas(select_list);

    let mut group_by_cols = Vec::new();
    let mut aggregate_exprs = Vec::new();
    for item in items {
        let item_upper = item.to_uppercase();
        if AGGREGATE_FUNCTIONS.iter().any(|f| item_upper.contains(f)) {
            aggregate_exprs.push(item);
        } else {
            group_by_cols.push(item);
        }
    }
    // A time-bucket GROUP BY column (`toStartOfInterval(...) AS bucket`)
    // paired with multiple countIf() outputs is the bucketed-countif shape,
    // not flat multi-aggregate - it produces one row per time bucket (a
    // range vector), not one row per GROUP BY key. Defer to the
    // purpose-built bucketed-countif handler rather than racing it: this
    // function runs earlier in the dispatch chain, so without this check it
    // would silently steal every bucketed-countif query.
    const TIME_BUCKET_FUNCTIONS: &[&str] = &[
        "TOSTARTOF",
        "TODATE(",
        "TOHOUR(",
        "TODAYOFWEEK(",
    ];
    if group_by_cols
        .iter()
        .any(|c| TIME_BUCKET_FUNCTIONS.iter().any(|f| c.to_uppercase().contains(f)))
    {
        return None;
    }

    if aggregate_exprs.len() < 2 {
        return None;
    }

    let known_aliases: Vec<String> = aggregate_exprs
        .iter()
        .filter_map(|e| split_expr_alias(e).map(|(_, alias)| alias))
        .collect();

    let (from_where, group_by_clause, having, order_by_and_limit) = match lower.find("group by") {
        Some(group_idx) => {
            let from_where = query[from_idx..group_idx].trim_end().to_string();
            let after_group = &query[group_idx + "group by".len()..];
            let after_group_lower = after_group.to_lowercase();
            let group_end = after_group_lower
                .find("having")
                .or_else(|| after_group_lower.find("order by"))
                .or_else(|| after_group_lower.find("limit"))
                .unwrap_or(after_group.len());
            let group_by_clause = after_group[..group_end].trim().to_string();
            if group_by_clause.is_empty() {
                return None;
            }

            let (having, tail_start) = if let Some(h_idx) = after_group_lower.find("having") {
                let after_having = after_group[h_idx + "having".len()..].trim_start();
                let after_having_lower = after_having.to_lowercase();
                let having_end = after_having_lower
                    .find("order by")
                    .or_else(|| after_having_lower.find("limit"))
                    .unwrap_or(after_having.len());
                let having_text = &after_having[..having_end];
                let having = parse_multi_aggregate_having(having_text, &known_aliases)?;
                (having, h_idx + "having".len() + having_end)
            } else {
                (Vec::new(), group_end)
            };

            let order_by_and_limit =
                after_group[tail_start..].trim().trim_end_matches(';').to_string();
            (from_where, group_by_clause, having, order_by_and_limit)
        }
        None => {
            // No GROUP BY at all: a scalar multi-aggregate (e.g. two
            // countIf()s side by side over the whole filtered set, no
            // partition key). Only valid when every SELECT-list item is an
            // aggregate expression - a bare column with no GROUP BY isn't a
            // shape this pattern (or valid SQL) supports. No HAVING either
            // - HAVING without GROUP BY filters the single global row,
            // which no target query needs yet - conservative, not handled.
            if !group_by_cols.is_empty() || lower.contains("having") {
                return None;
            }
            let after_from = &query[from_idx..];
            let after_from_lower = after_from.to_lowercase();
            let tail_start = after_from_lower
                .find("order by")
                .or_else(|| after_from_lower.find("limit"))
                .unwrap_or(after_from.len());
            let from_where = after_from[..tail_start].trim_end().to_string();
            let order_by_and_limit =
                after_from[tail_start..].trim().trim_end_matches(';').to_string();
            (from_where, String::new(), Vec::new(), order_by_and_limit)
        }
    };

    // ORDER BY (<alias1> + <alias2>) [ASC|DESC] - a derived sort key, the
    // one shape sort_and_truncate_instant_vector's generic bare-alias/
    // group-col lookup can't resolve on its own.
    let order_by_sum = {
        let ob_lower = order_by_and_limit.to_lowercase();
        ob_lower.find("order by").and_then(|ob_idx| {
            let after_order = &order_by_and_limit[ob_idx + "order by".len()..];
            let after_order_lower = after_order.to_lowercase();
            let end = after_order_lower.find("limit").unwrap_or(after_order.len());
            let first_item = split_top_level_commas(&after_order[..end]).into_iter().next()?;
            parse_order_by_sum(&first_item, &known_aliases)
        })
    };

    // Detect a computed-expression GROUP BY key (q091's
    // `length(splitByChar(' ', as_path)) AS path_len`) - only when
    // group_by_clause is a single bare column that doesn't already match
    // one of group_by_cols's bare-identifier entries, so this can never
    // change behavior for any query that already worked (bare single- or
    // multi-column GROUP BY, whatever its relationship to group_by_cols
    // was before). See MultiAggregateMatch::computed_group_by's doc
    // comment for how the caller uses this.
    let computed_group_by = if !group_by_clause.contains(',')
        && !group_by_cols.iter().any(|c| c.trim() == group_by_clause)
    {
        group_by_cols.iter().find_map(|c| {
            let (expr, alias) = split_expr_alias(c.trim())?;
            if alias != group_by_clause {
                return None;
            }
            let (label_type, source_col, tokenizer, select) = parse_computed_label_shape(&expr)?;
            Some((alias, label_type, source_col, tokenizer, select))
        })
    } else {
        None
    };

    Some(MultiAggregateMatch {
        group_by_cols,
        aggregate_exprs,
        from_where,
        group_by_clause,
        having,
        order_by_sum,
        order_by_and_limit,
        computed_group_by,
    })
}

/// Parses a raw `ORDER BY col1 [ASC|DESC], col2 [ASC|DESC] LIMIT n` tail
/// (either clause optional, either order, trailing `;` tolerated) into
/// structured `OrderByItem`s + limit. Deliberately simple text parsing, not
/// a full SQL parse: multi-aggregate detection already works this way, and
/// this only ever runs on a tail already isolated by the caller.
pub fn parse_order_by_and_limit(text: &str) -> (Vec<OrderByItem>, Option<u64>) {
    let trimmed = text.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_lowercase();

    let limit = lower
        .rfind("limit")
        .and_then(|i| trimmed[i + "limit".len()..].split_whitespace().next())
        .and_then(|s| s.parse::<u64>().ok());

    let order_by = if let Some(ob_idx) = lower.find("order by") {
        let after_order = &trimmed[ob_idx + "order by".len()..];
        let after_order_lower = after_order.to_lowercase();
        let end = after_order_lower.find("limit").unwrap_or(after_order.len());
        split_top_level_commas(&after_order[..end])
            .into_iter()
            .filter_map(|item| {
                let item_lower = item.to_lowercase();
                let (column, ascending) = if let Some(stripped) = item_lower.strip_suffix("desc") {
                    (item[..stripped.len()].trim().to_string(), false)
                } else if let Some(stripped) = item_lower.strip_suffix("asc") {
                    (item[..stripped.len()].trim().to_string(), true)
                } else {
                    (item.trim().to_string(), true)
                };
                if column.is_empty() {
                    None
                } else {
                    Some(OrderByItem { column, ascending })
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    (order_by, limit)
}

/// Splits `countIf(<condition>) AS <alias>` on top-level " AND " (case-
/// insensitive), respecting quotes and parens - same shape as
/// `split_top_level_commas` but for AND instead of comma.
pub(crate) fn split_top_level_and(s: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut in_quotes = false;
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\'' {
            in_quotes = !in_quotes;
            current.push(c);
            i += 1;
            continue;
        }
        if !in_quotes {
            if c == '(' {
                depth += 1;
                current.push(c);
                i += 1;
                continue;
            }
            if c == ')' {
                depth -= 1;
                current.push(c);
                i += 1;
                continue;
            }
            if depth == 0 {
                let rest: String = chars[i..].iter().collect();
                let preceded_by_space = i == 0 || chars[i - 1] == ' ';
                if preceded_by_space && rest.to_uppercase().starts_with("AND ") {
                    items.push(current.trim().to_string());
                    current.clear();
                    i += 4;
                    continue;
                }
            }
        }
        current.push(c);
        i += 1;
    }
    if !current.trim().is_empty() {
        items.push(current.trim().to_string());
    }
    items
}

/// Strips one layer of parentheses fully wrapping `s`, if present (e.g.
/// `(communities = '')` -> `communities = ''`). Building a surrogate by
/// wrapping a countIf condition in parens before appending it to a WHERE
/// clause (see `build_multi_aggregate_surrogates`) leaves that condition as
/// its own top-level AND-clause with the parens still attached. Both this
/// module's own safety check and the query engine's runtime spatial-filter
/// matcher (`parse_spatial_clause` in `ingest_source.rs`, which calls this
/// same function) need the parens gone before doing `<label> <op> <value>`
/// matching - otherwise the label comes out mangled as e.g. `(communities`,
/// which can never match a real row.
pub fn strip_wrapping_parens(s: &str) -> &str {
    let trimmed = s.trim();
    if let Some(inner) = trimmed.strip_prefix('(').and_then(|r| r.strip_suffix(')')) {
        // Only strip if the parens actually wrap the whole clause end to
        // end - depth must never dip below zero before the close, or
        // something like `(a) = (b)` would be mangled by stripping just
        // the outer characters.
        let mut depth = 0i32;
        for c in inner.chars() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth < 0 {
                        return trimmed;
                    }
                }
                _ => {}
            }
        }
        if depth == 0 {
            return inner.trim();
        }
    }
    trimmed
}

pub(crate) fn is_bare_identifier(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_alphanumeric() || c == '_')
}

pub(crate) fn is_quoted_literal(s: &str) -> bool {
    s.len() >= 2 && s.starts_with('\'') && s.ends_with('\'')
}

/// True when `condition` is safe to fold into the ingest-time spatial
/// filter (see `sample_matches_spatial_filter` in the query engine) - every
/// top-level AND-clause parses to a `SpatialPredicate` the ingest router can
/// actually evaluate (see `spatial_filter::parse_spatial_predicate`). The
/// ingest-time filter only enforces that recognized subset and (by its own
/// documented design) silently treats any clause shape it doesn't recognize
/// as "always true" rather than rejecting the sample. Folding a countIf
/// condition the filter can't actually enforce would silently serve a plain
/// row count mislabeled as the matched-condition count - wrong data with no
/// visible error. Deliberately conservative: anything that doesn't parse is
/// rejected, which leaves the countIf unfolded (and therefore unplannable
/// via the classic single-aggregate path), safely punting the whole query
/// instead.
fn is_ingest_filter_safe_condition(condition: &str) -> bool {
    let trimmed = condition.trim();
    if trimmed.is_empty() {
        return false;
    }
    super::spatial_filter::is_filter_fully_enforceable(trimmed)
}

/// True when `spatial_filter` (the exact string an `IntermediateAggConfig`'s
/// `spatial_filter` field carries into the query engine) is entirely
/// composed of clauses the ingest-time filter evaluator can enforce - see
/// `spatial_filter::is_filter_fully_enforceable`. An empty filter (no extra
/// predicate beyond the metric/time window, which the classic parser
/// already extracts into `TimeInfo` rather than `spatial_filter`) is
/// trivially safe. Exposed so callers that already built a real config can
/// reject one whose filter the ingest-time router would silently ignore - a
/// WHERE clause the parser doesn't recognize ends up verbatim in
/// `spatial_filter`, and without this check would silently serve every row
/// in the window instead of the intended subset.
pub fn is_ingest_filter_fully_enforceable(spatial_filter: &str) -> bool {
    super::spatial_filter::is_filter_fully_enforceable(spatial_filter)
}

/// Splits `countIf(<condition>) AS <alias>` into its condition and alias, or
/// `None` if `expr` isn't a clean single countIf call, or the condition
/// isn't safe to fold into the ingest-time spatial filter (see
/// `is_ingest_filter_safe_condition`). `countIf` has no classic
/// single-aggregate equivalent (its argument is a boolean condition, not a
/// value column), but `countIf(cond)` and `count()` filtered to `cond` are
/// exactly the same precompute: the condition folds into the surrogate's
/// WHERE clause instead.
fn parse_countif_expr(expr: &str) -> Option<(String, String)> {
    let trimmed = expr.trim_start();
    let lower = trimmed.to_lowercase();
    if !lower.starts_with("countif(") {
        return None;
    }
    let open_idx = "countif".len();
    let close_idx = find_matching_close_paren(trimmed, open_idx)?;
    let condition = trimmed[open_idx + 1..close_idx].trim().to_string();
    if !is_ingest_filter_safe_condition(&condition) {
        return None;
    }
    let after = &trimmed[close_idx + 1..];
    let after_lower = after.to_lowercase();
    let as_idx = after_lower.find("as ")?;
    if !after[..as_idx].trim().is_empty() {
        return None;
    }
    let alias = after[as_idx + 3..].trim().to_string();
    if condition.is_empty() || alias.is_empty() {
        return None;
    }
    Some((condition, alias))
}

pub fn looks_like_multi_aggregate_sql(query: &str) -> bool {
    parse_multi_aggregate_query(query).is_some()
}

/// One single-aggregate surrogate per aggregate expression, in the same
/// order as `m.aggregate_exprs` - each independently plannable/servable by
/// the existing classic single-aggregate machinery.
pub fn build_multi_aggregate_surrogates(m: &MultiAggregateMatch) -> Vec<String> {
    let where_has_clause = m.from_where.to_uppercase().contains("WHERE");
    // Empty group_by_clause means the original query had no GROUP BY at all
    // (a scalar multi-aggregate) - each surrogate must omit both the
    // GROUP-BY column in the SELECT list and the GROUP BY clause itself,
    // rather than emitting a leading comma / trailing empty GROUP BY.
    let has_group_by = !m.group_by_clause.is_empty();
    let select_prefix = if has_group_by {
        format!("{}, ", m.group_by_clause)
    } else {
        String::new()
    };
    let group_by_suffix = if has_group_by {
        format!(" GROUP BY {}", m.group_by_clause)
    } else {
        String::new()
    };
    m.aggregate_exprs
        .iter()
        .map(|expr| {
            if let Some((condition, alias)) = parse_countif_expr(expr) {
                let joiner = if where_has_clause { "AND" } else { "WHERE" };
                format!(
                    "SELECT {select_prefix}count() AS {alias} {from_where} {joiner} ({condition}){group_by_suffix}",
                    select_prefix = select_prefix,
                    alias = alias,
                    from_where = m.from_where,
                    joiner = joiner,
                    condition = condition,
                    group_by_suffix = group_by_suffix,
                )
            } else {
                format!(
                    "SELECT {select_prefix}{expr} {from_where}{group_by_suffix}",
                    select_prefix = select_prefix,
                    expr = expr,
                    from_where = m.from_where,
                    group_by_suffix = group_by_suffix,
                )
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Derived ratio: a SELECT list whose last item divides one aggregate by
// either another aggregate (guarded by `greatest(<agg>, <floor>)` against
// divide-by-zero) or a plain numeric constant, e.g.
//   SELECT peer_ip, countIf(operation = 'W') AS withdrawals,
//          countIf(operation = 'A') AS announcements,
//          countIf(operation = 'W') / greatest(countIf(operation = 'A'), 1) AS ratio
//   FROM ... GROUP BY peer_ip HAVING announcements + withdrawals > 20
//   ORDER BY ratio DESC LIMIT 20
// or the simpler constant-denominator form:
//   SELECT peer_asn, count(*) / 6.0 AS avg_updates_per_hour
//   FROM ... GROUP BY peer_asn ORDER BY avg_updates_per_hour DESC LIMIT 25
// Registers the underlying aggregate(s) as independent single-aggregate
// surrogates (the same split multi-aggregate uses) and computes the ratio -
// and, if present, the `<alias> + <alias> <op> <n>` HAVING - at serve time
// by joining their per-key results. Conservative: any other numerator,
// denominator, or HAVING shape falls through unrecognized.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct RatioComponent {
    pub expr: String,
    /// Alias this component is also exposed under as its own output
    /// column, when the SELECT list names it separately (e.g. the
    /// `withdrawals`/`announcements` columns above). `None` when the
    /// aggregate only ever appears inside the ratio expression itself
    /// (the `count(*) / 6.0` shape, which exposes no raw count column).
    pub exposed_alias: Option<String>,
}

#[derive(Debug, Clone)]
pub enum RatioDenominator {
    Aggregate {
        component: RatioComponent,
        floor: Option<f64>,
    },
    Constant(f64),
}

#[derive(Debug, Clone)]
pub struct DerivedRatioMatch {
    pub group_by_cols: Vec<String>,
    pub numerator: RatioComponent,
    pub denominator: RatioDenominator,
    /// Scale-up applied to the numerator before dividing (e.g. `100.0` for
    /// a `<agg> * 100.0 / <agg>` percentage). `1.0` when the numerator has
    /// no multiplier.
    pub multiplier: f64,
    /// Decimal places to round the final ratio to, from an outer
    /// `round(<ratio>, <n>)` wrapper. `None` when there's no rounding.
    pub decimals: Option<u32>,
    pub ratio_alias: String,
    /// "FROM ... WHERE ..." (or just "FROM ..."), verbatim.
    pub from_where: String,
    /// Raw text of the GROUP BY clause (column list), verbatim.
    pub group_by_clause: String,
    /// `<alias> + <alias> <op> <n>` HAVING over the two exposed aggregate
    /// aliases (either order) - only recognized when both the numerator
    /// and denominator are exposed aggregates. (op_text, threshold)
    pub having_sum: Option<(String, f64)>,
    /// Raw `ORDER BY ... LIMIT ...` tail (either or both, or empty), verbatim.
    pub order_by_and_limit: String,
}

/// Finds the single top-level occurrence of `target` in `s` (not nested
/// inside parens/quotes) and splits on it. `None` if there isn't exactly
/// one.
fn split_top_level_char(s: &str, target: char) -> Option<(String, String)> {
    let mut depth = 0i32;
    let mut in_quotes = false;
    let mut split_at = None;
    for (i, c) in s.char_indices() {
        if c == '\'' {
            in_quotes = !in_quotes;
            continue;
        }
        if in_quotes {
            continue;
        }
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            c if c == target && depth == 0 => {
                if split_at.is_some() {
                    return None; // more than one top-level occurrence - not this shape
                }
                split_at = Some(i);
            }
            _ => {}
        }
    }
    let i = split_at?;
    Some((s[..i].trim().to_string(), s[i + 1..].trim().to_string()))
}

/// Finds the single top-level `/` in `s` (not nested inside parens/quotes)
/// and splits on it. `None` if there isn't exactly one.
fn split_top_level_slash(s: &str) -> Option<(String, String)> {
    split_top_level_char(s, '/')
}

/// Matches `greatest(<inner>, <floor>)` (case-insensitive), splitting the
/// two arguments on their top-level comma. `None` if `s` isn't a `greatest`
/// call with exactly two arguments.
fn parse_greatest_floor(s: &str) -> Option<(String, f64)> {
    let trimmed = s.trim();
    let lower = trimmed.to_lowercase();
    if !lower.starts_with("greatest(") || !trimmed.ends_with(')') {
        return None;
    }
    let inner = &trimmed["greatest(".len()..trimmed.len() - 1];
    let parts = split_top_level_commas(inner);
    if parts.len() != 2 {
        return None;
    }
    let floor: f64 = parts[1].trim().parse().ok()?;
    Some((parts[0].trim().to_string(), floor))
}

/// `<alias_a> + <alias_b> <op> <n>` (either alias order), where
/// `{alias_a, alias_b}` must exactly match `known_aliases` - the two
/// exposed aggregate aliases. `None` for any other HAVING shape.
fn parse_having_sum(having_text: &str, known_aliases: &[String; 2]) -> Option<(String, f64)> {
    let trimmed = having_text.trim();

    // Earliest-starting operator wins; among ties at the same start index
    // (e.g. "<=" and "<" both start where the "<" is), the longer operator
    // wins, so "<=" isn't mis-split as "<" followed by "= n".
    let mut best: Option<(&str, usize)> = None;
    for op in ["<=", ">=", "!=", "<>", "=", "<", ">"] {
        if let Some(i) = trimmed.find(op) {
            best = match best {
                None => Some((op, i)),
                Some((best_op, best_i)) if i < best_i || (i == best_i && op.len() > best_op.len()) => {
                    Some((op, i))
                }
                other => other,
            };
        }
    }
    let (op_text, op_idx) = best?;

    let lhs = trimmed[..op_idx].trim();
    let rhs = trimmed[op_idx + op_text.len()..].trim();
    let threshold: f64 = rhs.parse().ok()?;

    let sum_parts: Vec<&str> = lhs.split('+').map(|p| p.trim()).collect();
    if sum_parts.len() != 2 || !sum_parts.iter().all(|p| is_bare_identifier(p)) {
        return None;
    }
    let mut got = sum_parts;
    got.sort_unstable();
    let mut want: Vec<&str> = known_aliases.iter().map(String::as_str).collect();
    want.sort_unstable();
    if got != want {
        return None;
    }

    Some((op_text.to_string(), threshold))
}

pub fn parse_derived_ratio_query(query: &str) -> Option<DerivedRatioMatch> {
    let lower = query.to_lowercase();
    let select_kw_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_kw_end > from_idx {
        return None;
    }

    let after_from = query[from_idx + "from".len()..].trim_start();
    if after_from.starts_with('(') {
        return None;
    }
    if lower[from_idx..].matches("select").count() > 0 {
        return None;
    }

    let select_list = &query[select_kw_end..from_idx];
    let items = split_top_level_commas(select_list);
    if items.is_empty() {
        return None;
    }

    // The ratio expression is always the last SELECT-list item, aliased.
    let (ratio_expr_raw, ratio_alias) = split_expr_alias(items.last()?)?;

    // An outer `round(<ratio>, <decimals>)` wrapper (q185's
    // `round(countIf(...) * 100.0 / count(*), 2)`) is common alongside a
    // percentage multiplier - unwrap it and remember the decimal count,
    // applied at serve time after the division (same rounding
    // grand-total-pct already does).
    let (ratio_expr_raw, decimals) = match strip_call_wrapper(&ratio_expr_raw, "round(") {
        Some(inner) => {
            let parts = split_top_level_commas(inner);
            let [expr, decimals_str] = parts.as_slice() else {
                return None;
            };
            let decimals: u32 = decimals_str.trim().parse().ok()?;
            (expr.trim().to_string(), Some(decimals))
        }
        None => (ratio_expr_raw, None),
    };

    let (num_text, denom_text) = split_top_level_slash(&ratio_expr_raw)?;
    // A window-function OVER(...) anywhere in the numerator or denominator
    // means this is grand-total-pct's or running-total's shape (a self-
    // total or a running sum), not a plain ratio of two independent
    // aggregates - reject outright rather than let find_component below
    // mistake "sum(count(*)) OVER ()" for a literal aggregate expression
    // just because it contains "SUM(" as a substring. Regression found the
    // hard way: the round()-unwrap above used to be enough of a barrier on
    // its own (an un-unwrapped `round(... OVER (), 2)` has no top-level '/'
    // for split_top_level_slash to find), but unwrapping round() to
    // support q185's multiplier removed that accidental protection.
    let looks_like_window_fn = |s: &str| {
        let lower = s.to_lowercase();
        let no_ws: String = lower.chars().filter(|c| !c.is_whitespace()).collect();
        no_ws.contains("over(")
    };
    if looks_like_window_fn(&num_text) || looks_like_window_fn(&denom_text) {
        return None;
    }
    // `<agg> * <multiplier> / ...` (e.g. `countIf(...) * 100.0 / count(*)`)
    // - a plain percentage scale-up on the numerator, same shape and same
    // reasoning as grand-total-pct's own multiplier.
    let (num_text, multiplier) = match split_top_level_char(&num_text, '*') {
        Some((base, mult_str)) => (base, mult_str.trim().parse::<f64>().ok()?),
        None => (num_text, 1.0),
    };

    let mut group_by_cols = Vec::new();
    let mut labeled_aggregates: Vec<(String, String)> = Vec::new(); // (alias, expr)
    for item in &items[..items.len() - 1] {
        if let Some((expr, alias)) = split_expr_alias(item) {
            let expr_upper = expr.to_uppercase();
            if AGGREGATE_FUNCTIONS.iter().any(|f| expr_upper.contains(f)) {
                labeled_aggregates.push((alias, expr));
                continue;
            }
        }
        let bare = item.trim();
        if is_bare_identifier(bare) {
            group_by_cols.push(bare.to_string());
        } else {
            return None; // unrecognized select-list item shape
        }
    }
    if group_by_cols.is_empty() {
        return None; // this mechanism always groups by something
    }

    let find_component = |text: &str| -> Option<RatioComponent> {
        let text = text.trim();
        if is_bare_identifier(text) {
            // Bare reference to an already-labeled aggregate alias.
            let (alias, expr) = labeled_aggregates.iter().find(|(a, _)| a == text)?;
            return Some(RatioComponent {
                expr: expr.clone(),
                exposed_alias: Some(alias.clone()),
            });
        }
        // A repeated aggregate expression - must actually be one. If it
        // textually matches an already-labeled aggregate's expr, reuse
        // that alias so the same aggregate isn't registered/rendered
        // twice under two different guises.
        let text_upper = text.to_uppercase();
        if !AGGREGATE_FUNCTIONS.iter().any(|f| text_upper.contains(f)) {
            return None;
        }
        let exposed_alias = labeled_aggregates
            .iter()
            .find(|(_, e)| e.trim() == text)
            .map(|(a, _)| a.clone());
        Some(RatioComponent {
            expr: text.to_string(),
            exposed_alias,
        })
    };

    let numerator = find_component(&num_text)?;

    let denominator = if let Some((inner, floor)) = parse_greatest_floor(&denom_text) {
        let component = find_component(&inner)?;
        RatioDenominator::Aggregate {
            component,
            floor: Some(floor),
        }
    } else if let Ok(constant) = denom_text.trim().parse::<f64>() {
        RatioDenominator::Constant(constant)
    } else if let Some(component) = find_component(&denom_text) {
        RatioDenominator::Aggregate {
            component,
            floor: None,
        }
    } else {
        return None;
    };

    let group_idx = lower.find("group by")?;
    let from_where = query[from_idx..group_idx].trim_end().to_string();
    let after_group = &query[group_idx + "group by".len()..];
    let after_group_lower = after_group.to_lowercase();

    let having_start = after_group_lower.find("having");
    let group_end = having_start
        .or_else(|| after_group_lower.find("order by"))
        .or_else(|| after_group_lower.find("limit"))
        .unwrap_or(after_group.len());
    let group_by_clause = after_group[..group_end].trim().to_string();
    if group_by_clause.is_empty() {
        return None;
    }
    let group_by_clause_cols: Vec<String> = split_top_level_commas(&group_by_clause)
        .into_iter()
        .map(|s| s.trim().to_string())
        .collect();
    if group_by_clause_cols.len() != group_by_cols.len()
        || !group_by_cols.iter().all(|c| group_by_clause_cols.contains(c))
    {
        return None;
    }

    let (having_sum, tail_start) = if let Some(h_idx) = having_start {
        let after_having = after_group[h_idx + "having".len()..].trim_start();
        let after_having_lower = after_having.to_lowercase();
        let having_end = after_having_lower
            .find("order by")
            .or_else(|| after_having_lower.find("limit"))
            .unwrap_or(after_having.len());
        let having_text = &after_having[..having_end];

        // Only recognized when exactly two aggregates are exposed under
        // their own alias - a HAVING over anything other than a plain sum
        // of both exposed aliases isn't this shape.
        if labeled_aggregates.len() != 2 {
            return None;
        }
        let known: [String; 2] = [
            labeled_aggregates[0].0.clone(),
            labeled_aggregates[1].0.clone(),
        ];
        let parsed = parse_having_sum(having_text, &known)?;
        (Some(parsed), h_idx + "having".len() + having_end)
    } else {
        (None, group_end)
    };

    let order_by_and_limit = after_group[tail_start..].trim().trim_end_matches(';').to_string();

    // Conservative: any ORDER BY item must be a bare identifier equal to
    // the ratio alias, an exposed aggregate alias, or a group-by column -
    // same reasoning as the other detectors in this module.
    let (order_by_items, _) = parse_order_by_and_limit(&order_by_and_limit);
    let known_cols: Vec<&str> = group_by_cols
        .iter()
        .map(String::as_str)
        .chain(labeled_aggregates.iter().map(|(a, _)| a.as_str()))
        .chain(std::iter::once(ratio_alias.as_str()))
        .collect();
    if order_by_items
        .iter()
        .any(|item| !known_cols.contains(&item.column.as_str()))
    {
        return None;
    }

    Some(DerivedRatioMatch {
        group_by_cols,
        numerator,
        denominator,
        multiplier,
        decimals,
        ratio_alias,
        from_where,
        group_by_clause,
        having_sum,
        order_by_and_limit,
    })
}

/// Builds the 1 or 2 independent single-aggregate surrogates needed to
/// answer a `DerivedRatioMatch`: the numerator's aggregate, and (only when
/// the denominator is itself an aggregate, not a constant) the
/// denominator's. Reuses `build_multi_aggregate_surrogates` - a ratio's
/// components are exactly the same "N independent single-aggregate
/// surrogates sharing one FROM/WHERE/GROUP BY" shape multi-aggregate
/// already builds, just consumed differently at serve time.
/// Sentinel aliases a ratio surrogate registers its aggregate under when
/// the original query doesn't expose that aggregate as its own column
/// (`RatioComponent::exposed_alias` is `None`, e.g. `count(*) / 6.0`'s
/// numerator). `build_multi_aggregate_surrogates` needs SOME alias to fold
/// a `countIf(...)` expression into `count() AS <alias> ... AND (...)`
/// (see `parse_countif_expr`), and the serve-time handler needs a stable
/// name to read each component's value back out by, regardless of whether
/// the original query happened to name it.
pub const RATIO_NUMERATOR_ALIAS: &str = "__ratio_numerator__";
pub const RATIO_DENOMINATOR_ALIAS: &str = "__ratio_denominator__";

pub fn build_derived_ratio_surrogates(m: &DerivedRatioMatch) -> Vec<String> {
    let numerator_alias = m
        .numerator
        .exposed_alias
        .clone()
        .unwrap_or_else(|| RATIO_NUMERATOR_ALIAS.to_string());
    let mut aggregate_exprs = vec![format!("{} AS {}", m.numerator.expr, numerator_alias)];
    if let RatioDenominator::Aggregate { component, .. } = &m.denominator {
        let denom_alias = component
            .exposed_alias
            .clone()
            .unwrap_or_else(|| RATIO_DENOMINATOR_ALIAS.to_string());
        aggregate_exprs.push(format!("{} AS {}", component.expr, denom_alias));
    }
    let helper = MultiAggregateMatch {
        group_by_cols: m.group_by_cols.clone(),
        aggregate_exprs,
        from_where: m.from_where.clone(),
        group_by_clause: m.group_by_clause.clone(),
        having: Vec::new(),
        order_by_sum: None,
        order_by_and_limit: String::new(),
        computed_group_by: None,
    };
    build_multi_aggregate_surrogates(&helper)
}

// ---------------------------------------------------------------------------
// Grand-total percentage: `round(<agg_expr> * <multiplier> / sum(<same agg_expr>)
// OVER (), <decimals>) AS <alias>` - an aggregate-of-aggregate window
// function, but a specific one: an EMPTY `OVER ()` (no PARTITION BY, no
// ORDER BY) means the "window" is the whole result set, and the numerator
// is always the same aggregate as the denominator (self/total-of-self).
// Unlike lagInFrame (which needs per-row ingest-time state to know the
// PREVIOUS row), this needs nothing at ingest time at all - the "total"
// is just the sum of the SAME per-key values the classic single-aggregate
// path already fetches, computed once at serve time after the fact, e.g.
//   SELECT operation, count(*) AS cnt,
//          round(count(*) * 100.0 / sum(count(*)) OVER (), 2) AS pct
//   FROM ... GROUP BY operation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct GrandTotalPctMatch {
    pub group_by_cols: Vec<String>,
    pub agg_expr: String,
    pub agg_alias: String,
    pub multiplier: f64,
    pub decimals: u32,
    pub pct_alias: String,
    pub from_where: String,
    pub group_by_clause: String,
    pub order_by_and_limit: String,
}

/// Strips a `<prefix>(...)` wrapper, requiring the parens to close exactly
/// at the end of `s` (nothing trailing). Case-insensitive on `prefix`.
fn strip_call_wrapper<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let trimmed = s.trim();
    let lower = trimmed.to_lowercase();
    if !lower.starts_with(prefix) || !trimmed.ends_with(')') {
        return None;
    }
    let close_idx = find_matching_close_paren(trimmed, prefix.len() - 1)?;
    if close_idx != trimmed.len() - 1 {
        return None; // something follows the closing paren - not a clean wrapper
    }
    Some(trimmed[prefix.len()..close_idx].trim())
}

pub fn parse_grand_total_pct_query(query: &str) -> Option<GrandTotalPctMatch> {
    let lower = query.to_lowercase();
    let select_kw_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_kw_end > from_idx {
        return None;
    }

    let after_from = query[from_idx + "from".len()..].trim_start();
    if after_from.starts_with('(') {
        return None;
    }
    if lower[from_idx..].matches("select").count() > 0 {
        return None;
    }

    let select_list = &query[select_kw_end..from_idx];
    let items = split_top_level_commas(select_list);
    if items.len() < 3 {
        return None; // group col(s) + the base aggregate + the pct expression
    }

    let (pct_expr_raw, pct_alias) = split_expr_alias(items.last()?)?;
    let inner = strip_call_wrapper(&pct_expr_raw, "round(")?;
    let round_args = split_top_level_commas(inner);
    let [ratio_expr, decimals_str] = round_args.as_slice() else {
        return None;
    };
    let decimals: u32 = decimals_str.trim().parse().ok()?;

    // The ratio expression must end in an empty `OVER ()` - a non-empty
    // PARTITION BY/ORDER BY means this isn't a whole-result-set total.
    let ratio_lower = ratio_expr.to_lowercase();
    let over_idx = ratio_lower.rfind("over")?;
    let after_over = ratio_expr[over_idx + "over".len()..].trim_start();
    if !after_over.starts_with('(') {
        return None;
    }
    let close_idx = find_matching_close_paren(after_over, 0)?;
    if !after_over[1..close_idx].trim().is_empty() || after_over[close_idx + 1..].trim().len() > 0
    {
        return None; // non-empty OVER(...) or trailing text after it
    }
    let sum_expr = ratio_expr[..over_idx].trim();
    let (numerator, denominator_sum) = split_top_level_char(sum_expr, '/')?;
    let denominator_expr = strip_call_wrapper(&denominator_sum, "sum(")?;

    let (num_base, multiplier) = match split_top_level_char(&numerator, '*') {
        Some((base, mult_str)) => (base, mult_str.trim().parse::<f64>().ok()?),
        None => (numerator.trim().to_string(), 1.0),
    };

    // The numerator and the denominator's inner aggregate must be the SAME
    // expression - this mechanism is "self over its own total", not an
    // arbitrary second aggregate (that's the derived-ratio mechanism).
    if num_base.trim() != denominator_expr.trim() {
        return None;
    }

    let mut group_by_cols = Vec::new();
    let mut agg_expr = None;
    let mut agg_alias = None;
    for item in &items[..items.len() - 1] {
        if let Some((expr, alias)) = split_expr_alias(item) {
            let expr_upper = expr.to_uppercase();
            if AGGREGATE_FUNCTIONS.iter().any(|f| expr_upper.contains(f)) {
                if agg_expr.is_some() {
                    return None; // only one base aggregate supported
                }
                agg_expr = Some(expr);
                agg_alias = Some(alias);
                continue;
            }
        }
        let bare = item.trim();
        if is_bare_identifier(bare) {
            group_by_cols.push(bare.to_string());
        } else {
            return None;
        }
    }
    let agg_expr = agg_expr?;
    let agg_alias = agg_alias?;
    if group_by_cols.is_empty() {
        return None;
    }
    // The numerator must be the SAME aggregate as the exposed one - not a
    // different aggregate that happens to also be summed over ().
    if num_base.trim() != agg_expr.trim() {
        return None;
    }

    let group_idx = lower.find("group by")?;
    let from_where = query[from_idx..group_idx].trim_end().to_string();
    let after_group = &query[group_idx + "group by".len()..];
    let after_group_lower = after_group.to_lowercase();
    let group_end = after_group_lower
        .find("order by")
        .or_else(|| after_group_lower.find("limit"))
        .unwrap_or(after_group.len());
    let group_by_clause = after_group[..group_end].trim().to_string();
    if group_by_clause.is_empty() {
        return None;
    }
    let group_by_clause_cols: Vec<String> = split_top_level_commas(&group_by_clause)
        .into_iter()
        .map(|s| s.trim().to_string())
        .collect();
    if group_by_clause_cols.len() != group_by_cols.len()
        || !group_by_cols.iter().all(|c| group_by_clause_cols.contains(c))
    {
        return None;
    }

    let order_by_and_limit = after_group[group_end..].trim().trim_end_matches(';').to_string();
    let (order_by_items, _) = parse_order_by_and_limit(&order_by_and_limit);
    let known_cols: Vec<&str> = group_by_cols
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(agg_alias.as_str()))
        .chain(std::iter::once(pct_alias.as_str()))
        .collect();
    if order_by_items
        .iter()
        .any(|item| !known_cols.contains(&item.column.as_str()))
    {
        return None;
    }

    Some(GrandTotalPctMatch {
        group_by_cols,
        agg_expr,
        agg_alias,
        multiplier,
        decimals,
        pct_alias,
        from_where,
        group_by_clause,
        order_by_and_limit,
    })
}

/// The single classic-shape surrogate needed to answer a
/// `GrandTotalPctMatch`: the base aggregate, grouped the same way. The
/// "total" is computed at serve time by summing this surrogate's own
/// results - no second surrogate needed (see `handle_grand_total_pct_sql`).
pub fn build_grand_total_pct_surrogate(m: &GrandTotalPctMatch) -> String {
    format!(
        "SELECT {cols}, {expr} AS {alias} {from_where} GROUP BY {cols}",
        cols = m.group_by_cols.join(", "),
        expr = m.agg_expr,
        alias = m.agg_alias,
        from_where = m.from_where,
    )
}

// ---------------------------------------------------------------------------
// Running total: `sum(<agg_alias>) OVER (ORDER BY <col> [ASC|DESC]) AS
// <alias>`, over a subquery that's itself the plain classic single-
// aggregate shape, e.g.
//   SELECT peer_asn, cnt, sum(cnt) OVER (ORDER BY cnt DESC) AS running_total
//   FROM (SELECT peer_asn, count(*) AS cnt FROM ... GROUP BY peer_asn)
//   ORDER BY cnt DESC LIMIT 40
// Like grand-total-pct, this needs no ingest-time work: the running total
// is computed at serve time by sorting the inner surrogate's own already-
// fetched per-key results into the window's ORDER BY and walking a
// cumulative sum (see handle_running_total_sql).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct RunningTotalMatch {
    pub group_by_cols: Vec<String>,
    pub agg_expr: String,
    pub agg_alias: String,
    pub inner_from_where: String,
    pub inner_group_by_clause: String,
    pub window_order_col: String,
    pub window_order_ascending: bool,
    pub running_alias: String,
    pub outer_order_by_and_limit: String,
}

/// Parses the inner subquery of a running-total query: the plain classic
/// single-aggregate shape (`SELECT <cols>, <agg> AS <alias> FROM ... WHERE
/// ... GROUP BY <cols>`, no ORDER BY/LIMIT/HAVING of its own - those belong
/// to the outer query). Returns (group_by_cols, agg_expr, agg_alias,
/// from_where, group_by_clause).
fn parse_inner_single_aggregate(
    sql: &str,
) -> Option<(Vec<String>, String, String, String, String)> {
    let lower = sql.to_lowercase();
    let select_kw_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_kw_end > from_idx {
        return None;
    }
    let after_from = sql[from_idx + "from".len()..].trim_start();
    if after_from.starts_with('(') {
        return None;
    }
    if lower[from_idx..].matches("select").count() > 0 {
        return None;
    }

    let items = split_top_level_commas(&sql[select_kw_end..from_idx]);
    let mut group_by_cols = Vec::new();
    let mut agg_expr = None;
    let mut agg_alias = None;
    for item in &items {
        if let Some((expr, alias)) = split_expr_alias(item) {
            let expr_upper = expr.to_uppercase();
            if AGGREGATE_FUNCTIONS.iter().any(|f| expr_upper.contains(f)) {
                if agg_expr.is_some() {
                    return None; // only one aggregate supported
                }
                agg_expr = Some(expr);
                agg_alias = Some(alias);
                continue;
            }
        }
        let bare = item.trim();
        if is_bare_identifier(bare) {
            group_by_cols.push(bare.to_string());
        } else {
            return None;
        }
    }
    let agg_expr = agg_expr?;
    let agg_alias = agg_alias?;
    if group_by_cols.is_empty() {
        return None;
    }

    let group_idx = lower.find("group by")?;
    let from_where = sql[from_idx..group_idx].trim_end().to_string();
    let after_group = sql[group_idx + "group by".len()..].trim().trim_end_matches(';');
    // No ORDER BY/LIMIT/HAVING of its own - this is purely the inner
    // aggregate, everything else belongs to the outer query.
    let after_group_lower = after_group.to_lowercase();
    if after_group_lower.contains("order by")
        || after_group_lower.contains("limit")
        || after_group_lower.contains("having")
    {
        return None;
    }
    let group_by_clause = after_group.to_string();
    let group_by_clause_cols: Vec<String> = split_top_level_commas(&group_by_clause)
        .into_iter()
        .map(|s| s.trim().to_string())
        .collect();
    if group_by_clause_cols.len() != group_by_cols.len()
        || !group_by_cols.iter().all(|c| group_by_clause_cols.contains(c))
    {
        return None;
    }

    Some((group_by_cols, agg_expr, agg_alias, from_where, group_by_clause))
}

pub fn parse_running_total_query(query: &str) -> Option<RunningTotalMatch> {
    let lower = query.to_lowercase();
    let select_kw_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_kw_end > from_idx {
        return None;
    }

    let items = split_top_level_commas(&query[select_kw_end..from_idx]);
    let [group_col_item, agg_ref_item, running_item] = items.as_slice() else {
        return None; // exactly group col, re-exposed aggregate, running-total expr
    };

    // `sum(<alias>) OVER (...)` - unlike grand-total-pct's `sum(...) OVER
    // ()`, the OVER(...) here is never empty (it always carries an ORDER
    // BY), so strip_call_wrapper (which requires nothing to follow the
    // wrapped call) doesn't apply - find sum(...)'s own close paren
    // directly and treat everything after it as the sibling OVER(...).
    let (running_expr_raw, running_alias) = split_expr_alias(running_item.trim())?;
    let trimmed_running = running_expr_raw.trim();
    if !trimmed_running.to_lowercase().starts_with("sum(") {
        return None;
    }
    let sum_close_idx = find_matching_close_paren(trimmed_running, "sum(".len() - 1)?;
    let summed_alias = trimmed_running["sum(".len()..sum_close_idx].trim();
    if !is_bare_identifier(summed_alias) {
        return None;
    }
    let after_sum_close = trimmed_running[sum_close_idx + 1..].trim_start();

    let after_over_lower = after_sum_close.to_lowercase();
    if !after_over_lower.starts_with("over") {
        return None;
    }
    let paren_start = after_sum_close.find('(')?;
    if !after_sum_close[..paren_start].trim().eq_ignore_ascii_case("over") {
        return None;
    }
    let over_close = find_matching_close_paren(after_sum_close, paren_start)?;
    if !after_sum_close[over_close + 1..].trim().is_empty() {
        return None; // nothing may follow OVER(...)
    }
    let over_inner = after_sum_close[paren_start + 1..over_close].trim();
    let over_inner_lower = over_inner.to_lowercase();
    let ob_idx = over_inner_lower.find("order by")?;
    if !over_inner[..ob_idx].trim().is_empty() {
        return None; // only ORDER BY supported inside OVER(...), no PARTITION BY
    }
    let order_text = over_inner[ob_idx + "order by".len()..].trim();
    let order_lower = order_text.to_lowercase();
    let (window_order_col, window_order_ascending) =
        if let Some(stripped) = order_lower.strip_suffix("desc") {
            (order_text[..stripped.len()].trim().to_string(), false)
        } else if let Some(stripped) = order_lower.strip_suffix("asc") {
            (order_text[..stripped.len()].trim().to_string(), true)
        } else {
            (order_text.trim().to_string(), true)
        };
    if !is_bare_identifier(&window_order_col) {
        return None;
    }

    let group_col = group_col_item.trim();
    let agg_ref = agg_ref_item.trim();
    if !is_bare_identifier(group_col) || !is_bare_identifier(agg_ref) {
        return None;
    }
    if summed_alias != agg_ref {
        return None; // sum(...) must reference the re-exposed aggregate column
    }

    let after_from = query[from_idx + "from".len()..].trim_start();
    let inner_open = after_from.strip_prefix('(')?;
    let close_idx = find_matching_close_paren(after_from, 0)?;
    let inner_sql = inner_open[..close_idx - 1].trim();
    let (group_by_cols, agg_expr, agg_alias, inner_from_where, inner_group_by_clause) =
        parse_inner_single_aggregate(inner_sql)?;

    if group_by_cols != [group_col.to_string()] || agg_alias != agg_ref {
        return None;
    }
    if window_order_col != agg_alias && !group_by_cols.contains(&window_order_col) {
        return None;
    }

    let outer_tail = after_from[close_idx + 1..].trim().trim_end_matches(';');
    let outer_order_by_and_limit = outer_tail.to_string();
    let (order_by_items, _) = parse_order_by_and_limit(&outer_order_by_and_limit);
    let known_cols = [group_col, agg_alias.as_str(), running_alias.as_str()];
    if order_by_items
        .iter()
        .any(|item| !known_cols.contains(&item.column.as_str()))
    {
        return None;
    }

    Some(RunningTotalMatch {
        group_by_cols,
        agg_expr,
        agg_alias,
        inner_from_where,
        inner_group_by_clause,
        window_order_col,
        window_order_ascending,
        running_alias,
        outer_order_by_and_limit,
    })
}

/// The single classic-shape surrogate needed to answer a
/// `RunningTotalMatch` - just the inner subquery, verbatim (it's already
/// the classic single-aggregate shape).
pub fn build_running_total_surrogate(m: &RunningTotalMatch) -> String {
    format!(
        "SELECT {cols}, {expr} AS {alias} {from_where} GROUP BY {cols}",
        cols = m.group_by_cols.join(", "),
        expr = m.agg_expr,
        alias = m.agg_alias,
        from_where = m.inner_from_where,
    )
}

#[cfg(test)]
mod derived_ratio_tests {
    use super::*;

    #[test]
    fn two_aggregate_ratio_with_having() {
        let sql = "SELECT peer_ip, countIf(operation = 'W') AS withdrawals, countIf(operation = 'A') AS announcements, countIf(operation = 'W') / greatest(countIf(operation = 'A'), 1) AS ratio FROM bgp.bgp_updates WHERE collector = 'rrc00' AND timestamp >= '2024-01-15 00:00:00' AND timestamp < '2024-01-16 00:00:00' GROUP BY peer_ip HAVING announcements + withdrawals > 20 ORDER BY ratio DESC LIMIT 20";
        let m = parse_derived_ratio_query(sql).expect("should match");
        assert_eq!(m.group_by_cols, vec!["peer_ip".to_string()]);
        assert_eq!(m.numerator.exposed_alias.as_deref(), Some("withdrawals"));
        match &m.denominator {
            RatioDenominator::Aggregate { component, floor } => {
                assert_eq!(component.exposed_alias.as_deref(), Some("announcements"));
                assert_eq!(*floor, Some(1.0));
            }
            _ => panic!("expected Aggregate denominator"),
        }
        assert_eq!(m.ratio_alias, "ratio");
        let (op, threshold) = m.having_sum.clone().expect("having sum");
        assert_eq!(op, ">");
        assert_eq!(threshold, 20.0);
        let surrogates = build_derived_ratio_surrogates(&m);
        assert_eq!(surrogates.len(), 2);
        println!("{:#?}", surrogates);
    }

    #[test]
    fn aggregate_over_constant_ratio() {
        let sql = "SELECT peer_asn, count(*) / 6.0 AS avg_updates_per_hour FROM bgp.bgp_updates WHERE collector = 'rrc00' AND timestamp >= '2024-01-08 00:00:00' AND timestamp < '2024-01-08 06:00:00' GROUP BY peer_asn ORDER BY avg_updates_per_hour DESC LIMIT 25";
        let m = parse_derived_ratio_query(sql).expect("should match");
        assert_eq!(m.group_by_cols, vec!["peer_asn".to_string()]);
        assert_eq!(m.numerator.exposed_alias, None);
        match &m.denominator {
            RatioDenominator::Constant(c) => assert_eq!(*c, 6.0),
            _ => panic!("expected Constant denominator"),
        }
        assert_eq!(m.ratio_alias, "avg_updates_per_hour");
        assert!(m.having_sum.is_none());
        let surrogates = build_derived_ratio_surrogates(&m);
        assert_eq!(surrogates.len(), 1);
        println!("{:#?}", surrogates);
    }
}

#[cfg(test)]
mod grand_total_pct_tests {
    use super::*;

    #[test]
    fn simple_grand_total_pct() {
        let sql = "SELECT operation, count(*) AS cnt, round(count(*) * 100.0 / sum(count(*)) OVER (), 2) AS pct FROM bgp.bgp_updates WHERE collector = 'rrc00' AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-02-01 00:00:00' GROUP BY operation";
        let m = parse_grand_total_pct_query(sql).expect("should match");
        assert_eq!(m.group_by_cols, vec!["operation".to_string()]);
        assert_eq!(m.agg_expr, "count(*)");
        assert_eq!(m.agg_alias, "cnt");
        assert_eq!(m.multiplier, 100.0);
        assert_eq!(m.decimals, 2);
        assert_eq!(m.pct_alias, "pct");
        let surrogate = build_grand_total_pct_surrogate(&m);
        println!("{surrogate}");
        assert!(surrogate.contains("GROUP BY operation"));
    }

    #[test]
    fn with_order_and_limit() {
        let sql = "SELECT peer_asn, count(*) AS cnt, round(count(*) * 100.0 / sum(count(*)) OVER (), 2) AS pct_of_day FROM bgp.bgp_updates WHERE collector = 'rrc00' AND timestamp >= '2024-01-06 00:00:00' AND timestamp < '2024-01-07 00:00:00' GROUP BY peer_asn ORDER BY cnt DESC LIMIT 5";
        let m = parse_grand_total_pct_query(sql).expect("should match");
        assert_eq!(m.order_by_and_limit, "ORDER BY cnt DESC LIMIT 5");
    }
}

#[cfg(test)]
mod running_total_tests {
    use super::*;

    #[test]
    fn simple_running_total() {
        let sql = "SELECT peer_asn, cnt, sum(cnt) OVER (ORDER BY cnt DESC) AS running_total FROM (SELECT peer_asn, count(*) AS cnt FROM bgp.bgp_updates WHERE collector = 'rrc00' AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-02-01 00:00:00' GROUP BY peer_asn) ORDER BY cnt DESC LIMIT 40";
        let m = parse_running_total_query(sql).expect("should match");
        assert_eq!(m.group_by_cols, vec!["peer_asn".to_string()]);
        assert_eq!(m.agg_expr, "count(*)");
        assert_eq!(m.agg_alias, "cnt");
        assert_eq!(m.window_order_col, "cnt");
        assert!(!m.window_order_ascending);
        assert_eq!(m.running_alias, "running_total");
        assert_eq!(m.outer_order_by_and_limit, "ORDER BY cnt DESC LIMIT 40");
        let surrogate = build_running_total_surrogate(&m);
        println!("{surrogate}");
        assert!(surrogate.contains("GROUP BY peer_asn"));
        assert!(!surrogate.to_lowercase().contains("order by"));
    }
}

// ---------------------------------------------------------------------------
// Bucketed top-N: `row_number() OVER (PARTITION BY <bucket> ORDER BY <cnt>
// DESC) AS rnk ... WHERE rnk <= N` wrapped around a GROUP BY on a real time
// bucket PLUS a real key column together, e.g.
//   SELECT hour, prefix, cnt, rnk FROM (
//     SELECT toStartOfHour(timestamp) AS hour, prefix, count(*) AS cnt,
//            row_number() OVER (PARTITION BY hour ORDER BY cnt DESC) AS rnk
//     FROM ... WHERE ... GROUP BY hour, prefix
//   ) WHERE rnk <= 5 ORDER BY hour, rnk
// Unlike the cyclical computed-GROUP-BY mechanism (toHour/toDayOfWeek,
// which COLLAPSE a range into 24/7 buckets), `toStartOfHour` produces one
// REAL bucket per actual hour in the range - the same "bucket" concept the
// bucketed-countif mechanism uses, just with a real GROUP BY key (prefix)
// nested inside each bucket instead of a single scalar count. Registered
// with the key column as the grouping label and the window size FORCED to
// the bucket size (same trick bucketed-countif uses); served by querying
// each bucket's exact window individually (no merge needed - the window
// size already equals one bucket) and ranking that bucket's results
// in-memory - see handle_bucketed_topn_sql.
// ---------------------------------------------------------------------------

/// Bucket functions this mechanism understands: (name, bucket_ms).
/// Deliberately narrower than sqlpattern_parser's FIXED_TIME_BUCKET_FUNCTIONS
/// (only what's actually been exercised) - extend as new target queries need it.
const TOPN_BUCKET_FUNCTIONS: &[(&str, u64)] = &[
    ("tostartofhour(", 3_600_000),
    ("tostartofminute(", 60_000),
    ("todate(", 86_400_000),
];

#[derive(Debug, Clone)]
pub struct BucketedTopNMatch {
    pub bucket_fn: String,
    pub bucket_ms: u64,
    pub time_col: String,
    pub bucket_alias: String,
    pub key_col: String,
    pub cnt_alias: String,
    pub rnk_alias: String,
    pub descending: bool,
    pub n: u64,
    pub from_where: String,
    pub start: String,
    pub end: String,
    pub outer_order_by_and_limit: String,
}

pub fn parse_bucketed_topn_query(query: &str) -> Option<BucketedTopNMatch> {
    let lower = query.to_lowercase();
    let select_kw_end = lower.find("select")? + "select".len();
    let outer_from_idx = lower.find("from")?;
    if select_kw_end > outer_from_idx {
        return None;
    }

    let after_outer_from = query[outer_from_idx + "from".len()..].trim_start();
    let inner_open = after_outer_from.strip_prefix('(')?;
    let inner_close_idx = find_matching_close_paren(after_outer_from, 0)?;
    let inner_sql = inner_open[..inner_close_idx - 1].trim();

    // --- Parse the inner query ---
    let inner_lower = inner_sql.to_lowercase();
    let inner_select_end = inner_lower.find("select")? + "select".len();
    let inner_from_idx = inner_lower.find("from")?;
    if inner_select_end > inner_from_idx {
        return None;
    }
    let after_inner_from = inner_sql[inner_from_idx + "from".len()..].trim_start();
    if after_inner_from.starts_with('(') {
        return None;
    }
    if inner_lower[inner_from_idx..].matches("select").count() > 0 {
        return None;
    }

    let inner_items = split_top_level_commas(&inner_sql[inner_select_end..inner_from_idx]);
    let [bucket_item, key_item, cnt_item, rnk_item] = inner_items.as_slice() else {
        return None; // exactly bucket, key, count, row_number
    };

    let (bucket_expr, bucket_alias) = split_expr_alias(bucket_item.trim())?;
    let bucket_expr_lower = bucket_expr.trim().to_lowercase();
    let (bucket_fn, bucket_ms) = TOPN_BUCKET_FUNCTIONS
        .iter()
        .find(|(f, _)| bucket_expr_lower.starts_with(f))
        .copied()?;
    let close_idx = find_matching_close_paren(bucket_expr.trim(), bucket_fn.len() - 1)?;
    if close_idx != bucket_expr.trim().len() - 1 {
        return None; // nothing may follow the bucket function call
    }
    let time_col = bucket_expr.trim()[bucket_fn.len()..close_idx].trim().to_string();
    if !is_bare_identifier(&time_col) {
        return None;
    }

    let key_col = key_item.trim();
    if !is_bare_identifier(key_col) {
        return None;
    }

    let (cnt_expr, cnt_alias) = split_expr_alias(cnt_item.trim())?;
    if !cnt_expr.trim().eq_ignore_ascii_case("count(*)") && !cnt_expr.trim().eq_ignore_ascii_case("count()") {
        return None; // only plain count(*) supported for now
    }

    let (rnk_expr, rnk_alias) = split_expr_alias(rnk_item.trim())?;
    let rnk_expr_lower = rnk_expr.to_lowercase();
    if !rnk_expr_lower.trim_start().starts_with("row_number()") {
        return None;
    }
    let over_idx = rnk_expr_lower.find("over")?;
    let after_over = rnk_expr[over_idx + "over".len()..].trim_start();
    let paren_start = after_over.find('(')?;
    if !after_over[..paren_start].trim().is_empty() {
        return None;
    }
    let over_close = find_matching_close_paren(after_over, paren_start)?;
    if !after_over[over_close + 1..].trim().is_empty() {
        return None; // nothing may follow OVER(...)
    }
    let over_inner = after_over[paren_start + 1..over_close].trim();
    let over_inner_lower = over_inner.to_lowercase();
    let pb_idx = over_inner_lower.find("partition by")?;
    if !over_inner[..pb_idx].trim().is_empty() {
        return None;
    }
    let ob_idx = over_inner_lower.find("order by")?;
    // PARTITION BY may reference either the bucket's own alias or repeat
    // its raw expression (ClickHouse allows both - `PARTITION BY hour` or
    // `PARTITION BY toStartOfHour(timestamp)`).
    let partition_col = over_inner[pb_idx + "partition by".len()..ob_idx].trim();
    if partition_col != bucket_alias && partition_col != bucket_expr.trim() {
        return None; // must partition by the bucket, not the key
    }
    let order_text = over_inner[ob_idx + "order by".len()..].trim();
    let order_lower = order_text.to_lowercase();
    let (order_col, descending) = if let Some(stripped) = order_lower.strip_suffix("desc") {
        (order_text[..stripped.len()].trim().to_string(), true)
    } else if let Some(stripped) = order_lower.strip_suffix("asc") {
        (order_text[..stripped.len()].trim().to_string(), false)
    } else {
        (order_text.trim().to_string(), true)
    };
    // Same alias-or-raw-expression flexibility for the ORDER BY target.
    if order_col != cnt_alias && order_col.to_lowercase() != cnt_expr.trim().to_lowercase() {
        return None; // ranking must be by the count
    }

    let group_idx = inner_lower.find("group by")?;
    let inner_from_where = inner_sql[inner_from_idx..group_idx].trim_end().to_string();
    let after_group = inner_sql[group_idx + "group by".len()..].trim().trim_end_matches(';');
    let group_cols: Vec<String> = split_top_level_commas(after_group)
        .into_iter()
        .map(|s| s.trim().to_string())
        .collect();
    if group_cols.len() != 2 || !group_cols.contains(&bucket_alias) || !group_cols.contains(&key_col.to_string()) {
        return None;
    }

    let start = extract_ts_bound(&inner_from_where, ">=")?;
    let end = extract_ts_bound(&inner_from_where, "<")?;

    // --- Parse the outer query ---
    let outer_items = split_top_level_commas(&query[select_kw_end..outer_from_idx]);
    let expected: [&str; 4] = [&bucket_alias, key_col, &cnt_alias, &rnk_alias];
    if outer_items.len() != 4
        || !outer_items
            .iter()
            .zip(expected.iter())
            .all(|(item, exp)| item.trim() == *exp)
    {
        return None; // outer SELECT must re-expose all 4 inner columns, in order
    }

    let after_inner_close = after_outer_from[inner_close_idx + 1..].trim();
    let after_inner_close_lower = after_inner_close.to_lowercase();
    let where_idx = after_inner_close_lower.find("where")?;
    if !after_inner_close[..where_idx].trim().is_empty() {
        return None;
    }
    let after_where = after_inner_close[where_idx + "where".len()..].trim_start();
    let after_where_lower = after_where.to_lowercase();
    if !after_where_lower.starts_with(&rnk_alias.to_lowercase()) {
        return None; // outer filter must be exactly `rnk <= n`
    }
    let after_rnk = after_where[rnk_alias.len()..].trim_start();
    if !after_rnk.starts_with("<=") {
        return None;
    }
    let after_op = after_rnk[2..].trim_start();
    let tail_start = after_op
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after_op.len());
    if tail_start == 0 {
        return None;
    }
    let n: u64 = after_op[..tail_start].parse().ok()?;
    let outer_order_by_and_limit = after_op[tail_start..].trim().trim_end_matches(';').to_string();

    let (order_by_items, _) = parse_order_by_and_limit(&outer_order_by_and_limit);
    let known = [bucket_alias.as_str(), key_col, cnt_alias.as_str(), rnk_alias.as_str()];
    if order_by_items
        .iter()
        .any(|item| !known.contains(&item.column.as_str()))
    {
        return None;
    }

    Some(BucketedTopNMatch {
        bucket_fn: bucket_fn.to_string(),
        bucket_ms,
        time_col,
        bucket_alias,
        key_col: key_col.to_string(),
        cnt_alias,
        rnk_alias,
        descending,
        n,
        from_where: inner_from_where,
        start,
        end,
        outer_order_by_and_limit,
    })
}

/// The single classic-shape surrogate needed to answer a
/// `BucketedTopNMatch` for one bucket window: exact same shape the
/// classic single-aggregate path already handles, just registered (by the
/// planner) with a window size forced to `bucket_ms` instead of the
/// query's own overall duration.
/// The registration-time surrogate. Critically, its own WHERE-clause
/// duration must be EXACTLY one bucket (not the query's overall range) -
/// `SQLQueryData::matches_sql_pattern` requires durations to match near-
/// exactly (only the absolute start time is ignored), so registering with
/// the full multi-bucket range would never match any of the single-bucket
/// surrogates `handle_bucketed_topn_sql` issues per hour at serve time.
/// Any canonical one-bucket window works - this one uses [start,
/// start+bucket_ms) - since only the DURATION needs to match, not the
/// specific timestamps.
pub fn build_bucketed_topn_surrogate(m: &BucketedTopNMatch) -> String {
    let canonical_end = single_bucket_end(&m.start, m.bucket_ms).unwrap_or_else(|| m.end.clone());
    let bucketed_from_where = m.from_where.replacen(&m.end, &canonical_end, 1);
    format!(
        "SELECT {key}, count(*) AS {alias} {from_where} GROUP BY {key}",
        key = m.key_col,
        alias = m.cnt_alias,
        from_where = bucketed_from_where,
    )
}

fn single_bucket_end(start: &str, bucket_ms: u64) -> Option<String> {
    let start_dt = chrono::NaiveDateTime::parse_from_str(start.trim(), "%Y-%m-%d %H:%M:%S").ok()?;
    let end_ms = start_dt.and_utc().timestamp_millis() + bucket_ms as i64;
    Some(
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(end_ms)?
            .format("%Y-%m-%d %H:%M:%S")
            .to_string(),
    )
}

#[cfg(test)]
mod bucketed_topn_tests {
    use super::*;

    #[test]
    fn hourly_top5_prefixes() {
        let sql = "SELECT hour, prefix, cnt, rnk\nFROM (\n  SELECT toStartOfHour(timestamp) AS hour, prefix, count(*) AS cnt,\n         row_number() OVER (PARTITION BY hour ORDER BY cnt DESC) AS rnk\n  FROM bgp.bgp_updates\n  WHERE collector = 'rrc00'\n    AND timestamp >= '2024-01-11 00:00:00' AND timestamp < '2024-01-12 00:00:00'\n  GROUP BY hour, prefix\n)\nWHERE rnk <= 5\nORDER BY hour, rnk";
        let m = parse_bucketed_topn_query(sql).expect("should match");
        assert_eq!(m.bucket_ms, 3_600_000);
        assert_eq!(m.time_col, "timestamp");
        assert_eq!(m.bucket_alias, "hour");
        assert_eq!(m.key_col, "prefix");
        assert_eq!(m.cnt_alias, "cnt");
        assert_eq!(m.rnk_alias, "rnk");
        assert!(m.descending);
        assert_eq!(m.n, 5);
        assert_eq!(m.start, "2024-01-11 00:00:00");
        assert_eq!(m.end, "2024-01-12 00:00:00");
        assert_eq!(m.outer_order_by_and_limit, "ORDER BY hour, rnk");
        let surrogate = build_bucketed_topn_surrogate(&m);
        println!("{surrogate}");
        assert!(surrogate.contains("GROUP BY prefix"));
    }
}

#[cfg(test)]
mod flat_token_explode_tests {
    use super::*;

    #[test]
    fn as_path_explosion() {
        let sql = "SELECT arrayJoin(splitByChar(' ', as_path)) AS asn, count(*) AS cnt FROM bgp.bgp_updates WHERE collector = 'rrc00' AND operation = 'A' AND timestamp >= '2024-01-05 09:00:00' AND timestamp < '2024-01-05 10:00:00' GROUP BY asn ORDER BY cnt DESC LIMIT 40";
        assert!(looks_like_flat_token_explode_sql(sql));
        let m = parse_flat_token_explode_query(sql).expect("should match");
        assert_eq!(m.label, "asn");
        assert_eq!(m.source_col, "as_path");
        assert_eq!(m.filter_regex, "");
        assert_eq!(m.select_expr, "count(*) AS cnt");
        assert_eq!(m.from_target, "bgp.bgp_updates");
        assert_eq!(m.group_by, "asn");
        assert_eq!(m.order_by_and_limit, "ORDER BY cnt DESC LIMIT 40");
        let surrogate = build_token_explode_surrogate(&m);
        println!("{surrogate}");
        assert!(surrogate.contains("GROUP BY asn"));
    }
}

#[cfg(test)]
mod raw_value_agg_serve_time_rewrite_tests {
    use super::*;
    use std::collections::HashSet;

    fn bgp_schema() -> SQLSchema {
        let metadata_columns: HashSet<String> = [
            "collector", "operation", "prefix", "peer_ip", "peer_asn", "as_path", "origin",
            "next_hop", "local_pref", "med", "communities",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let value_columns: HashSet<String> = ["__event_count__".to_string()].into_iter().collect();
        SQLSchema::new(vec![super::super::sqlhelper::Table::new(
            "bgp".to_string(),
            "timestamp".to_string(),
            value_columns,
            metadata_columns,
        )])
    }

    #[test]
    fn bare_time_column_min_rewrites_to_derived_table() {
        let sql = "SELECT min(timestamp) AS first_seen FROM bgp WHERE collector = 'rrc00' \
                    AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-01-02 00:00:00'";
        let rewritten =
            rewrite_raw_value_agg_query(sql, &bgp_schema()).expect("should rewrite");
        assert!(
            rewritten.contains("FROM derived_value_timestamp_bgp"),
            "got: {rewritten}"
        );
        assert!(rewritten.contains("min(timestamp)"));
    }

    #[test]
    fn trivial_string_cast_wrapper_rewrites_like_the_bare_column() {
        let sql = "SELECT avg(toFloat64OrZero(toString(med))) AS avg_med FROM bgp \
                    WHERE collector = 'rrc00' \
                    AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-01-02 00:00:00'";
        let rewritten =
            rewrite_raw_value_agg_query(sql, &bgp_schema()).expect("should rewrite");
        assert!(
            rewritten.contains("FROM derived_value_med_bgp"),
            "got: {rewritten}"
        );
        assert!(
            rewritten.to_lowercase().contains("avg(med)"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn computed_expression_rewrites_to_synthetic_label_and_derived_table() {
        let sql = "SELECT uniqExact(toDate(timestamp)) AS active_days FROM bgp \
                    WHERE collector = 'rrc00' \
                    AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-02-01 00:00:00'";
        // The planner adds every computed label to every table's metadata
        // columns in the registered config - simulate that here, since a
        // live schema built from a real inference_config.yaml would already
        // carry it (see rewrite_raw_value_agg_query's own doc comment).
        let mut metadata_columns: HashSet<String> = [
            "collector", "operation", "prefix", "peer_ip", "peer_asn", "as_path", "origin",
            "next_hop", "local_pref", "med", "communities",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        metadata_columns.insert("computed_date_bucket_timestamp".to_string());
        let value_columns: HashSet<String> = ["__event_count__".to_string()].into_iter().collect();
        let schema = SQLSchema::new(vec![super::super::sqlhelper::Table::new(
            "bgp".to_string(),
            "timestamp".to_string(),
            value_columns,
            metadata_columns,
        )]);
        let rewritten = rewrite_raw_value_agg_query(sql, &schema).expect("should rewrite");
        assert!(
            rewritten.contains("FROM derived_value_computed_date_bucket_timestamp_bgp"),
            "got: {rewritten}"
        );
        assert!(
            rewritten.contains("uniqexact(computed_date_bucket_timestamp)")
                || rewritten.contains("UNIQEXACT(computed_date_bucket_timestamp)")
                || rewritten.to_lowercase().contains("uniqexact(computed_date_bucket_timestamp)"),
            "got: {rewritten}"
        );
    }

    #[test]
    fn real_value_column_is_left_alone() {
        let sql = "SELECT count(*) AS cnt FROM bgp WHERE collector = 'rrc00' \
                    AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-01-02 00:00:00'";
        assert!(rewrite_raw_value_agg_query(sql, &bgp_schema()).is_none());
    }
}

#[cfg(test)]
mod arg_agg_tests {
    use super::*;
    use std::collections::HashSet;

    fn bgp_schema() -> SQLSchema {
        let metadata_columns: HashSet<String> = [
            "collector", "operation", "prefix", "peer_ip", "peer_asn", "as_path", "origin",
            "next_hop", "local_pref", "med", "communities",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let value_columns: HashSet<String> = ["__event_count__".to_string()].into_iter().collect();
        SQLSchema::new(vec![super::super::sqlhelper::Table::new(
            "bgp".to_string(),
            "timestamp".to_string(),
            value_columns,
            metadata_columns,
        )])
    }

    #[test]
    fn parses_single_argmax_query() {
        // q140's actual shape.
        let sql = "SELECT prefix, argMax(as_path, timestamp) AS latest_path FROM bgp \
                    WHERE collector = 'rrc00' AND operation = 'A' \
                    AND timestamp >= '2024-01-08 00:00:00' AND timestamp < '2024-01-08 06:00:00' \
                    GROUP BY prefix LIMIT 200";
        let m = parse_arg_agg_query(sql).expect("should match");
        assert!(m.is_max);
        assert_eq!(m.arg_col, "as_path");
        assert_eq!(m.cmp_col, "timestamp");
        assert_eq!(m.alias, "latest_path");
        assert_eq!(m.group_by_col, "prefix");
        assert_eq!(m.limit.as_deref(), Some("200"));
    }

    #[test]
    fn parses_multi_aggregate_split_argmax_surrogate() {
        // What build_multi_aggregate_surrogates produces for q139's argMax
        // branch: no ORDER BY/LIMIT (those apply to the combined result).
        let sql = "SELECT prefix, argMax(operation, timestamp) AS last_operation FROM bgp.bgp_updates \
                    WHERE collector = 'rrc00' AND timestamp >= '2024-01-05 00:00:00' \
                    AND timestamp < '2024-01-06 00:00:00' GROUP BY prefix";
        let m = parse_arg_agg_query(sql).expect("should match");
        assert!(m.is_max);
        assert_eq!(m.arg_col, "operation");
        assert_eq!(m.cmp_col, "timestamp");
        assert_eq!(m.group_by_col, "prefix");
        assert_eq!(m.limit, None);
    }

    #[test]
    fn parses_argmin() {
        let sql = "SELECT prefix, argMin(operation, timestamp) AS first_operation FROM bgp \
                    WHERE collector = 'rrc00' AND timestamp >= '2024-01-05 00:00:00' \
                    AND timestamp < '2024-01-06 00:00:00' GROUP BY prefix";
        let m = parse_arg_agg_query(sql).expect("should match");
        assert!(!m.is_max);
    }

    #[test]
    fn rejects_order_by() {
        // This mechanism's result is a label, not a value - sorting by it
        // isn't built; a query with ORDER BY on the alias must not match.
        let sql = "SELECT prefix, argMax(as_path, timestamp) AS latest_path FROM bgp \
                    WHERE collector = 'rrc00' AND timestamp >= '2024-01-08 00:00:00' \
                    AND timestamp < '2024-01-08 06:00:00' GROUP BY prefix ORDER BY latest_path";
        assert!(parse_arg_agg_query(sql).is_none());
    }

    #[test]
    fn rejects_wrong_group_by_column() {
        // GROUP BY column must be the same one selected - a second,
        // unrelated GROUP BY column isn't a shape this mechanism handles.
        let sql = "SELECT prefix, argMax(as_path, timestamp) AS latest_path FROM bgp \
                    WHERE collector = 'rrc00' AND timestamp >= '2024-01-08 00:00:00' \
                    AND timestamp < '2024-01-08 06:00:00' GROUP BY peer_ip";
        assert!(parse_arg_agg_query(sql).is_none());
    }

    #[test]
    fn rewrite_redirects_from_clause_to_derived_table() {
        let sql = "SELECT prefix, argMax(as_path, timestamp) AS latest_path FROM bgp \
                    WHERE collector = 'rrc00' AND operation = 'A' \
                    AND timestamp >= '2024-01-08 00:00:00' AND timestamp < '2024-01-08 06:00:00' \
                    GROUP BY prefix LIMIT 200";
        let rewritten = rewrite_arg_agg_query(sql, &bgp_schema()).expect("should rewrite");
        assert!(
            rewritten.contains("FROM derived_value_arg_as_path_bgp"),
            "got: {rewritten}"
        );
        assert!(rewritten.contains("argMax(as_path, timestamp)"), "got: {rewritten}");
    }

    #[test]
    fn rewrite_rejects_wrong_comparison_column() {
        // cmp_col must be the table's own time column - a query comparing
        // against something else isn't a shape this mechanism serves.
        let sql = "SELECT prefix, argMax(as_path, local_pref) AS latest_path FROM bgp \
                    WHERE collector = 'rrc00' AND timestamp >= '2024-01-08 00:00:00' \
                    AND timestamp < '2024-01-08 06:00:00' GROUP BY prefix";
        assert!(rewrite_arg_agg_query(sql, &bgp_schema()).is_none());
    }

    #[test]
    fn rewrite_rejects_unknown_arg_column() {
        let sql = "SELECT prefix, argMax(nonexistent_col, timestamp) AS x FROM bgp \
                    WHERE collector = 'rrc00' AND timestamp >= '2024-01-08 00:00:00' \
                    AND timestamp < '2024-01-08 06:00:00' GROUP BY prefix";
        assert!(rewrite_arg_agg_query(sql, &bgp_schema()).is_none());
    }

    #[test]
    fn multi_aggregate_argmax_branch_surrogate_rewrites_to_planner_registered_table() {
        // q139's exact shape: argMax is NOT the last aggregate expression -
        // the case that used to fail to serve because
        // handle_multi_aggregate_sql's numeric-only path had no branch for
        // a non-numeric (string) aggregate result. This test locks in the
        // wiring the "all-labels" combiner (asap-query-engine's
        // handle_multi_aggregate_all_labels_sql) depends on: the surrogate
        // build_multi_aggregate_surrogates produces for the ARGMAX branch
        // must, once passed through rewrite_arg_agg_query, redirect to
        // *exactly* the derived table name the planner itself registers
        // (get_arg_agg_streaming_aggregation_configs in asap-planner-rs) -
        // any mismatch here would mean the combiner's serve-time lookup can
        // never find the planner's registered QueryConfig.
        let sql = "SELECT prefix, argMax(operation, timestamp) AS last_operation, \
                    max(timestamp) AS last_seen FROM bgp \
                    WHERE collector = 'rrc00' AND timestamp >= '2024-01-05 00:00:00' \
                    AND timestamp < '2024-01-06 00:00:00' GROUP BY prefix LIMIT 200";
        let m = parse_multi_aggregate_query(sql).expect("q139's shape must be recognized");
        assert_eq!(m.aggregate_exprs.len(), 2);
        assert!(
            m.aggregate_exprs[0].to_uppercase().contains("ARGMAX("),
            "argMax must be the FIRST aggregate, not the last - that's the whole point of this shape"
        );

        let surrogates = build_multi_aggregate_surrogates(&m);
        assert_eq!(surrogates.len(), 2);

        let rewritten = rewrite_arg_agg_query(&surrogates[0], &bgp_schema())
            .expect("the argMax branch surrogate must independently match the arg-agg rewrite");
        assert!(
            rewritten.contains("FROM derived_value_arg_operation_bgp"),
            "got: {rewritten}"
        );
        // GROUP BY / WHERE from the original multi-aggregate query must
        // survive into the split-out surrogate unchanged.
        assert!(rewritten.contains("GROUP BY prefix"), "got: {rewritten}");
        assert!(rewritten.contains("collector = 'rrc00'"), "got: {rewritten}");
    }
}

#[cfg(test)]
mod group_array_distinct_tests {
    use super::*;

    #[test]
    fn parses_grouped_form() {
        // q116's split-out groupArray branch (as build_multi_aggregate_surrogates
        // produces it - no HAVING/ORDER BY/LIMIT, those apply to the combined result).
        let sql = "SELECT prefix, groupArray(DISTINCT origin) AS origins FROM bgp.bgp_updates \
                    WHERE collector = 'rrc00' AND operation = 'A' \
                    AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-02-01 00:00:00' \
                    GROUP BY prefix";
        let m = parse_group_array_distinct_query(sql).expect("should match");
        assert_eq!(m.group_by.as_deref(), Some("prefix"));
        assert_eq!(m.column, "origin");
        assert_eq!(m.alias, "origins");
        assert_eq!(m.limit, None);
    }

    #[test]
    fn parses_scalar_form_no_group_by() {
        // q165's shape: no GROUP BY at all, one global distinct-value list.
        let sql = "SELECT groupArray(DISTINCT origin) AS origins_list FROM bgp.bgp_updates \
                    WHERE collector = 'rrc00' AND prefix = '8.8.8.0/24' AND operation = 'A' \
                    AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-02-01 00:00:00'";
        let m = parse_group_array_distinct_query(sql).expect("should match");
        assert_eq!(m.group_by, None);
        assert_eq!(m.column, "origin");
        assert_eq!(m.alias, "origins_list");
    }

    #[test]
    fn parses_standalone_query_with_limit() {
        let sql = "SELECT prefix, groupArray(DISTINCT origin) AS origins FROM bgp \
                    WHERE collector = 'rrc00' GROUP BY prefix LIMIT 50";
        let m = parse_group_array_distinct_query(sql).expect("should match");
        assert_eq!(m.limit.as_deref(), Some("50"));
    }

    #[test]
    fn rejects_order_by() {
        let sql = "SELECT prefix, groupArray(DISTINCT origin) AS origins FROM bgp \
                    WHERE collector = 'rrc00' GROUP BY prefix ORDER BY origins";
        assert!(parse_group_array_distinct_query(sql).is_none());
    }

    #[test]
    fn rejects_group_by_column_not_in_select_list() {
        let sql = "SELECT prefix, groupArray(DISTINCT origin) AS origins FROM bgp \
                    WHERE collector = 'rrc00' GROUP BY peer_ip";
        assert!(parse_group_array_distinct_query(sql).is_none());
    }

    #[test]
    fn rejects_group_by_with_no_matching_select_column() {
        // GROUP BY present but the SELECT list only has the groupArray item -
        // mismatched shape, not a "no GROUP BY" scalar form.
        let sql = "SELECT groupArray(DISTINCT origin) AS origins FROM bgp \
                    WHERE collector = 'rrc00' GROUP BY prefix";
        assert!(parse_group_array_distinct_query(sql).is_none());
    }

    #[test]
    fn rejects_uniqexact_alone() {
        // Not this pattern at all - just the sibling cardinality aggregate.
        let sql = "SELECT prefix, uniqExact(origin) AS distinct_origins FROM bgp \
                    WHERE collector = 'rrc00' GROUP BY prefix";
        assert!(parse_group_array_distinct_query(sql).is_none());
    }

    #[test]
    fn build_surrogate_grouped() {
        let sql = "SELECT prefix, groupArray(DISTINCT origin) AS origins FROM bgp.bgp_updates \
                    WHERE collector = 'rrc00' GROUP BY prefix";
        let m = parse_group_array_distinct_query(sql).expect("should match");
        let surrogate = build_group_array_distinct_surrogate(&m);
        assert_eq!(
            surrogate,
            format!(
                "SELECT prefix, uniqExact(origin) AS {} FROM bgp.bgp_updates WHERE collector = 'rrc00' GROUP BY prefix",
                GROUP_ARRAY_DISTINCT_SURROGATE_ALIAS
            )
        );
    }

    #[test]
    fn build_surrogate_scalar() {
        let sql = "SELECT groupArray(DISTINCT origin) AS origins_list FROM bgp.bgp_updates \
                    WHERE prefix = '8.8.8.0/24'";
        let m = parse_group_array_distinct_query(sql).expect("should match");
        let surrogate = build_group_array_distinct_surrogate(&m);
        assert_eq!(
            surrogate,
            format!(
                "SELECT uniqExact(origin) AS {} FROM bgp.bgp_updates WHERE prefix = '8.8.8.0/24'",
                GROUP_ARRAY_DISTINCT_SURROGATE_ALIAS
            )
        );
    }

    #[test]
    fn q116_multi_aggregate_split_produces_matching_group_array_branch() {
        // q116's exact shape: uniqExact(origin) (the count, numeric) paired
        // with groupArray(DISTINCT origin) (the list, non-numeric) under
        // one GROUP BY prefix. Locks in the same wiring contract as
        // multi_aggregate_argmax_branch_surrogate_rewrites_to_planner_registered_table:
        // the surrogate build_multi_aggregate_surrogates produces for the
        // groupArray branch must, once passed through
        // parse_group_array_distinct_query + build_group_array_distinct_surrogate,
        // match exactly what the planner's own
        // get_group_array_distinct_streaming_aggregation_configs registers -
        // any drift here would mean the multi-aggregate combiner's
        // serve-time find_query_config_sql lookup can never find it.
        let sql = "SELECT prefix, uniqExact(origin) AS distinct_origins, \
                    groupArray(DISTINCT origin) AS origins FROM bgp.bgp_updates \
                    WHERE collector = 'rrc00' AND operation = 'A' \
                    AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-02-01 00:00:00' \
                    GROUP BY prefix HAVING distinct_origins >= 3 \
                    ORDER BY distinct_origins DESC LIMIT 30";
        let m = parse_multi_aggregate_query(sql).expect("q116's shape must be recognized");
        assert_eq!(m.aggregate_exprs.len(), 2);
        assert!(m.aggregate_exprs[1].to_uppercase().contains("GROUPARRAY("));

        let surrogates = build_multi_aggregate_surrogates(&m);
        assert_eq!(surrogates.len(), 2);

        let gad = parse_group_array_distinct_query(&surrogates[1])
            .expect("the groupArray branch surrogate must independently match");
        assert_eq!(gad.group_by.as_deref(), Some("prefix"));
        assert_eq!(gad.column, "origin");

        let registered_shape = build_group_array_distinct_surrogate(&gad);
        assert!(
            registered_shape.contains("FROM bgp.bgp_updates"),
            "got: {registered_shape}"
        );
        assert!(registered_shape.contains("GROUP BY prefix"), "got: {registered_shape}");
        assert!(registered_shape.contains("collector = 'rrc00'"), "got: {registered_shape}");
    }

    #[test]
    fn q165_scalar_multi_aggregate_split_produces_matching_group_array_branch() {
        // q165's shape: no GROUP BY at all - both aggregates scalar over
        // the whole filtered set.
        let sql = "SELECT uniqExact(origin) AS distinct_origins, \
                    groupArray(DISTINCT origin) AS origins_list FROM bgp.bgp_updates \
                    WHERE collector = 'rrc00' AND prefix = '8.8.8.0/24' AND operation = 'A' \
                    AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-02-01 00:00:00'";
        let m = parse_multi_aggregate_query(sql).expect("q165's shape must be recognized");
        assert_eq!(m.aggregate_exprs.len(), 2);
        assert!(m.group_by_clause.is_empty());

        let surrogates = build_multi_aggregate_surrogates(&m);
        let gad = parse_group_array_distinct_query(&surrogates[1])
            .expect("the groupArray branch surrogate must independently match");
        assert_eq!(gad.group_by, None);
        assert_eq!(gad.column, "origin");
    }
}

#[cfg(test)]
mod concat_two_tokens_tests {
    use super::*;

    #[test]
    fn parses_q146_supernet_shape() {
        let expr = "splitByChar('.', prefix)[1] || '.' || splitByChar('.', prefix)[2] || '.0.0/16'";
        let (label_type, source_col, tokenizer, select) =
            parse_computed_label_shape(expr).expect("should match");
        assert_eq!(label_type, "concat_two_tokens");
        assert_eq!(source_col, "prefix");
        assert_eq!(tokenizer.as_deref(), Some("char:."));
        assert_eq!(select.as_deref(), Some(".\u{1}.0.0/16"));
    }

    #[test]
    fn full_q146_query_registers_as_computed_group_by() {
        let sql = "SELECT splitByChar('.', prefix)[1] || '.' || splitByChar('.', prefix)[2] \
                    || '.0.0/16' AS supernet, count(*) AS cnt FROM bgp.bgp_updates \
                    WHERE collector = 'rrc00' AND NOT match(prefix, ':') \
                    AND timestamp >= '2024-01-25 00:00:00' AND timestamp < '2024-01-26 00:00:00' \
                    GROUP BY supernet ORDER BY cnt DESC LIMIT 25";
        let m = parse_computed_group_by_query(sql).expect("should match");
        assert_eq!(m.alias, "supernet");
        assert_eq!(m.label_type, "concat_two_tokens");
        assert_eq!(m.source_col, "prefix");

        let surrogate = build_computed_group_by_surrogate(&m);
        assert!(surrogate.contains("SELECT supernet, count(*) AS cnt"), "got: {surrogate}");
        assert!(surrogate.contains("GROUP BY supernet"), "got: {surrogate}");
    }

    #[test]
    fn rejects_mismatched_columns() {
        // Two different source columns - not a shape this pattern serves.
        let expr = "splitByChar('.', prefix)[1] || '.' || splitByChar('.', peer_ip)[2] || '.0.0/16'";
        assert!(parse_computed_label_shape(expr).is_none());
    }

    #[test]
    fn rejects_mismatched_separators() {
        let expr = "splitByChar('.', prefix)[1] || '.' || splitByChar('/', prefix)[2] || '.0.0/16'";
        assert!(parse_computed_label_shape(expr).is_none());
    }

    #[test]
    fn rejects_non_first_two_indices() {
        // Indices 2 and 3, not 1 and 2 - out of scope for this narrow shape.
        let expr = "splitByChar('.', prefix)[2] || '.' || splitByChar('.', prefix)[3] || '.0.0/16'";
        assert!(parse_computed_label_shape(expr).is_none());
    }

    #[test]
    fn rejects_wrong_part_count() {
        let expr = "splitByChar('.', prefix)[1] || '.' || splitByChar('.', prefix)[2]";
        assert!(parse_computed_label_shape(expr).is_none());
    }
}

#[cfg(test)]
mod array_slice_tests {
    use super::*;

    #[test]
    fn parses_q174_first_two_hops_shape() {
        let expr = "arraySlice(splitByChar(' ', as_path), 1, 2)";
        let (label_type, source_col, tokenizer, select) =
            parse_computed_label_shape(expr).expect("should match");
        assert_eq!(label_type, "array_slice");
        assert_eq!(source_col, "as_path");
        assert_eq!(tokenizer.as_deref(), Some("char: "));
        assert_eq!(select.as_deref(), Some("0:2"));
    }

    #[test]
    fn full_q174_query_registers_as_computed_group_by() {
        let sql = "SELECT arraySlice(splitByChar(' ', as_path), 1, 2) AS first_two_hops, \
                    count(*) AS cnt FROM bgp.bgp_updates \
                    WHERE collector = 'rrc00' AND operation = 'A' \
                    AND timestamp >= '2024-01-12 00:00:00' AND timestamp < '2024-01-13 00:00:00' \
                    AND length(splitByChar(' ', as_path)) >= 2 \
                    GROUP BY first_two_hops ORDER BY cnt DESC LIMIT 25";
        let m = parse_computed_group_by_query(sql).expect("should match");
        assert_eq!(m.alias, "first_two_hops");
        assert_eq!(m.label_type, "array_slice");
        assert_eq!(m.source_col, "as_path");

        let surrogate = build_computed_group_by_surrogate(&m);
        assert!(surrogate.contains("SELECT first_two_hops, count(*) AS cnt"), "got: {surrogate}");
        assert!(surrogate.contains("GROUP BY first_two_hops"), "got: {surrogate}");
    }

    #[test]
    fn rejects_zero_start() {
        let expr = "arraySlice(splitByChar(' ', as_path), 0, 2)";
        assert!(parse_computed_label_shape(expr).is_none());
    }

    #[test]
    fn rejects_non_split_source() {
        let expr = "arraySlice(as_path, 1, 2)";
        assert!(parse_computed_label_shape(expr).is_none());
    }

    #[test]
    fn different_start_and_length() {
        let expr = "arraySlice(splitByChar(',', prefix), 2, 3)";
        let (_, _, _, select) = parse_computed_label_shape(expr).expect("should match");
        assert_eq!(select.as_deref(), Some("1:3"));
    }
}

#[cfg(test)]
mod arrayzip_edge_explode_tests {
    use super::*;

    fn q067_sql() -> &'static str {
        "SELECT arrayJoin(arrayZip(\
             arraySlice(splitByChar(' ', as_path), 1, length(splitByChar(' ', as_path)) - 1), \
             arraySlice(splitByChar(' ', as_path), 2, length(splitByChar(' ', as_path)) - 1)\
           )) AS as_edge, count(*) AS cnt \
         FROM bgp.bgp_updates \
         WHERE collector = 'rrc00' AND operation = 'A' \
           AND timestamp >= '2024-01-05 09:00:00' AND timestamp < '2024-01-05 10:00:00' \
           AND length(splitByChar(' ', as_path)) > 1 \
         GROUP BY as_edge ORDER BY cnt DESC LIMIT 50"
    }

    #[test]
    fn parses_q067_shape() {
        let m = parse_arrayzip_edge_explode_query(q067_sql()).expect("should match");
        assert_eq!(m.label, "as_edge");
        assert_eq!(m.source_col, "as_path");
        assert_eq!(m.select_expr, "count(*) AS cnt");
        assert_eq!(m.group_by, "as_edge");
        assert!(m.order_by_and_limit.contains("ORDER BY cnt DESC"), "got: {}", m.order_by_and_limit);
        assert!(m.order_by_and_limit.contains("LIMIT 50"), "got: {}", m.order_by_and_limit);
    }

    #[test]
    fn builds_surrogate_referencing_bare_label() {
        let m = parse_arrayzip_edge_explode_query(q067_sql()).expect("should match");
        let surrogate = build_token_explode_surrogate(&m);
        assert!(surrogate.starts_with("SELECT as_edge, count(*) AS cnt"), "got: {surrogate}");
        assert!(surrogate.contains("GROUP BY as_edge"), "got: {surrogate}");
        assert!(!surrogate.to_lowercase().contains("arrayjoin"), "got: {surrogate}");
    }

    #[test]
    fn rewrite_produces_same_surrogate() {
        let rewritten = rewrite_arrayzip_edge_explode_query(q067_sql()).expect("should rewrite");
        let m = parse_arrayzip_edge_explode_query(q067_sql()).unwrap();
        assert_eq!(rewritten, build_token_explode_surrogate(&m));
    }

    #[test]
    fn rejects_mismatched_start_offsets() {
        // Both slices starting at 1 - not adjacent pairs.
        let sql = "SELECT arrayJoin(arrayZip(\
                       arraySlice(splitByChar(' ', as_path), 1, length(splitByChar(' ', as_path)) - 1), \
                       arraySlice(splitByChar(' ', as_path), 1, length(splitByChar(' ', as_path)) - 1)\
                     )) AS as_edge, count(*) AS cnt FROM bgp \
                   WHERE collector = 'rrc00' GROUP BY as_edge";
        assert!(parse_arrayzip_edge_explode_query(sql).is_none());
    }

    #[test]
    fn rejects_mismatched_columns() {
        let sql = "SELECT arrayJoin(arrayZip(\
                       arraySlice(splitByChar(' ', as_path), 1, length(splitByChar(' ', as_path)) - 1), \
                       arraySlice(splitByChar(' ', next_hop), 2, length(splitByChar(' ', next_hop)) - 1)\
                     )) AS as_edge, count(*) AS cnt FROM bgp \
                   WHERE collector = 'rrc00' GROUP BY as_edge";
        assert!(parse_arrayzip_edge_explode_query(sql).is_none());
    }

    #[test]
    fn rejects_non_whitespace_separator() {
        let sql = "SELECT arrayJoin(arrayZip(\
                       arraySlice(splitByChar(',', tags), 1, length(splitByChar(',', tags)) - 1), \
                       arraySlice(splitByChar(',', tags), 2, length(splitByChar(',', tags)) - 1)\
                     )) AS edge, count(*) AS cnt FROM bgp \
                   WHERE collector = 'rrc00' GROUP BY edge";
        assert!(parse_arrayzip_edge_explode_query(sql).is_none());
    }

    #[test]
    fn rejects_wrong_group_by() {
        let sql = "SELECT arrayJoin(arrayZip(\
                       arraySlice(splitByChar(' ', as_path), 1, length(splitByChar(' ', as_path)) - 1), \
                       arraySlice(splitByChar(' ', as_path), 2, length(splitByChar(' ', as_path)) - 1)\
                     )) AS as_edge, count(*) AS cnt FROM bgp \
                   WHERE collector = 'rrc00' GROUP BY prefix";
        assert!(parse_arrayzip_edge_explode_query(sql).is_none());
    }
}

#[cfg(test)]
mod quantile_computed_value_agg_tests {
    use super::*;

    #[test]
    fn parses_quantile_over_split_length() {
        // q072's median_len branch.
        let sql = "SELECT quantile(0.5)(length(splitByChar(' ', as_path))) AS median_len \
                    FROM bgp.bgp_updates WHERE collector = 'rrc00' AND operation = 'A'";
        let m = parse_computed_value_agg_query(sql).expect("should match");
        assert_eq!(m.label_type, "split_length");
        assert_eq!(m.source_col, "as_path");
        assert_eq!(
            m.surrogate,
            format!(
                "SELECT quantile(0.5)({}) AS median_len \
                 FROM bgp.bgp_updates WHERE collector = 'rrc00' AND operation = 'A'",
                m.synthetic_label
            )
        );
    }

    #[test]
    fn parses_quantile_with_different_level() {
        let sql = "SELECT quantile(0.95)(length(splitByChar(' ', as_path))) AS p95_len \
                    FROM bgp.bgp_updates";
        let m = parse_computed_value_agg_query(sql).expect("should match");
        assert!(m.surrogate.contains("quantile(0.95)("), "got: {}", m.surrogate);
    }

    #[test]
    fn bare_column_quantile_is_left_alone() {
        // Not this mechanism's job - a bare-column quantile is the
        // classic path's job, and re-detecting it here would be wrong
        // (is_bare_identifier(inner) short-circuits before ever calling
        // parse_computed_label_shape).
        let sql = "SELECT quantile(0.5)(med) AS median_med FROM bgp.bgp_updates";
        assert!(parse_computed_value_agg_query(sql).is_none());
    }

    #[test]
    fn multi_aggregate_split_branch_parses_independently() {
        // What build_multi_aggregate_surrogates produces for q072's
        // second branch, in isolation.
        let sql = "SELECT quantile(0.95)(length(splitByChar(' ', as_path))) AS p95_len \
                    FROM bgp.bgp_updates WHERE collector = 'rrc00' AND operation = 'A' \
                    AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-01-08 00:00:00'";
        let m = parse_multi_aggregate_query(
            "SELECT quantile(0.5)(length(splitByChar(' ', as_path))) AS median_len, \
             quantile(0.95)(length(splitByChar(' ', as_path))) AS p95_len, \
             max(length(splitByChar(' ', as_path))) AS max_len FROM bgp.bgp_updates \
             WHERE collector = 'rrc00' AND operation = 'A' \
             AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-01-08 00:00:00'",
        )
        .expect("q072's shape must be recognized as multi-aggregate");
        assert_eq!(m.aggregate_exprs.len(), 3);
        let surrogates = build_multi_aggregate_surrogates(&m);
        assert_eq!(surrogates.len(), 3);
        assert!(surrogates[1].contains("quantile(0.95)"), "got: {}", surrogates[1]);
        assert_eq!(
            parse_computed_value_agg_query(&surrogates[1])
                .expect("branch surrogate should independently match")
                .surrogate
                .replace(char::is_whitespace, " "),
            parse_computed_value_agg_query(sql)
                .expect("hand-written equivalent should match")
                .surrogate
                .replace(char::is_whitespace, " "),
        );
    }
}


#[cfg(test)]
mod computed_group_by_composed_value_agg_tests {
    use super::*;

    #[test]
    fn q013_composes_date_bucket_group_by_with_split_length_value() {
        let sql = "SELECT toDate(timestamp) AS day, \
                    avg(length(splitByChar(' ', as_path))) AS avg_path_len \
                    FROM bgp.bgp_updates WHERE collector = 'rrc00' AND operation = 'A' \
                    AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-01-08 00:00:00' \
                    GROUP BY day ORDER BY day";
        let m = parse_computed_group_by_query(sql).expect("group-by half should match");
        assert_eq!(m.label_type, "date_bucket");
        let surrogate = build_computed_group_by_surrogate(&m);
        let m2 = parse_computed_value_agg_query(&surrogate).expect("value half should match");
        assert_eq!(m2.label_type, "split_length");
        assert!(m2.surrogate.contains("day, AVG("), "got: {}", m2.surrogate);
        assert!(m2.surrogate.contains("GROUP BY day"), "got: {}", m2.surrogate);
    }

    #[test]
    fn q187_composes_five_minute_bucket_group_by_with_split_length_value() {
        let sql = "SELECT toStartOfFiveMinutes(timestamp) AS bucket, \
                    avg(length(splitByChar(' ', as_path))) AS avg_path_len \
                    FROM bgp.bgp_updates WHERE collector = 'rrc00' AND operation = 'A' \
                    AND timestamp >= '2024-01-05 00:00:00' AND timestamp < '2024-01-06 00:00:00' \
                    GROUP BY bucket ORDER BY bucket LIMIT 300";
        let m = parse_computed_group_by_query(sql).expect("group-by half should match");
        assert_eq!(m.label_type, "five_minute_bucket");
        let surrogate = build_computed_group_by_surrogate(&m);
        let m2 = parse_computed_value_agg_query(&surrogate).expect("value half should match");
        assert_eq!(m2.label_type, "split_length");
    }

    #[test]
    fn ordinary_bare_aggregate_does_not_trigger_value_composition() {
        // q146/q174's shape - count(*) has no computed argument, so the
        // second (value-agg) half must not match at all.
        let sql = "SELECT toDate(timestamp) AS day, count(*) AS cnt \
                    FROM bgp.bgp_updates WHERE collector = 'rrc00' GROUP BY day";
        let m = parse_computed_group_by_query(sql).expect("group-by half should match");
        let surrogate = build_computed_group_by_surrogate(&m);
        assert!(parse_computed_value_agg_query(&surrogate).is_none());
    }
}

#[cfg(test)]
mod multi_aggregate_computed_group_by_tests {
    use super::*;

    #[test]
    fn q091_detects_computed_group_by_key() {
        let sql = "SELECT length(splitByChar(' ', as_path)) AS path_len, \
                    avg(toFloat64OrZero(toString(med))) AS avg_med, count(*) AS cnt \
                    FROM bgp.bgp_updates WHERE collector = 'rrc00' AND operation = 'A' \
                    AND timestamp >= '2024-01-14 00:00:00' AND timestamp < '2024-01-15 00:00:00' \
                    GROUP BY path_len ORDER BY path_len";
        let m = parse_multi_aggregate_query(sql).expect("should match multi-aggregate");
        let (alias, label_type, source_col, _tok, _sel) = m
            .computed_group_by
            .expect("should detect computed GROUP BY key");
        assert_eq!(alias, "path_len");
        assert_eq!(label_type, "split_length");
        assert_eq!(source_col, "as_path");
    }

    #[test]
    fn ordinary_bare_group_by_key_is_not_flagged_computed() {
        // Every previously-passing multi-aggregate shape (bare GROUP BY
        // column) must see computed_group_by stay None - no regression.
        let sql = "SELECT peer_ip, countIf(operation = 'W') AS withdrawals, \
                    countIf(operation = 'A') AS announcements \
                    FROM bgp.bgp_updates WHERE collector = 'rrc00' GROUP BY peer_ip";
        let m = parse_multi_aggregate_query(sql).expect("should match multi-aggregate");
        assert!(m.computed_group_by.is_none());
    }

    #[test]
    fn surrogates_reference_bare_alias_not_full_expression() {
        let sql = "SELECT length(splitByChar(' ', as_path)) AS path_len, \
                    avg(toFloat64OrZero(toString(med))) AS avg_med, count(*) AS cnt \
                    FROM bgp.bgp_updates WHERE collector = 'rrc00' GROUP BY path_len";
        let m = parse_multi_aggregate_query(sql).expect("should match multi-aggregate");
        let surrogates = build_multi_aggregate_surrogates(&m);
        assert_eq!(surrogates.len(), 2);
        for s in &surrogates {
            assert!(s.contains("SELECT path_len,"), "got: {s}");
            assert!(!s.to_lowercase().contains("splitbychar"), "got: {s}");
        }
    }
}

#[cfg(test)]
mod hidden_countif_having_tests {
    use super::*;

    fn q113_sql() -> &'static str {
        "SELECT peer_ip, count(*) AS withdrawal_cnt FROM bgp.bgp_updates \
         WHERE collector = 'rrc00' \
         AND timestamp >= '2024-01-29 00:00:00' AND timestamp < '2024-01-30 00:00:00' \
         GROUP BY peer_ip \
         HAVING countIf(operation = 'A') = 0 AND countIf(operation = 'W') > 0 \
         ORDER BY withdrawal_cnt DESC LIMIT 20"
    }

    #[test]
    fn parses_q113_shape() {
        let m = parse_hidden_countif_having_query(q113_sql()).expect("should match");
        assert_eq!(m.group_col, "peer_ip");
        assert_eq!(m.alias, "withdrawal_cnt");
        assert_eq!(m.hidden.len(), 2);
        assert_eq!(m.hidden[0], ("operation = 'A'".to_string(), "=".to_string(), 0.0));
        assert_eq!(m.hidden[1], ("operation = 'W'".to_string(), ">".to_string(), 0.0));
    }

    #[test]
    fn builds_three_surrogates() {
        let m = parse_hidden_countif_having_query(q113_sql()).expect("should match");
        let surrogates = build_hidden_countif_having_surrogates(&m);
        assert_eq!(surrogates.len(), 3);
        assert!(surrogates[0].contains("SELECT peer_ip, count(*) AS withdrawal_cnt"), "got: {}", surrogates[0]);
        assert!(!surrogates[0].to_lowercase().contains("having"), "got: {}", surrogates[0]);
        assert!(surrogates[1].contains("(operation = 'A')"), "got: {}", surrogates[1]);
        assert!(surrogates[2].contains("(operation = 'W')"), "got: {}", surrogates[2]);
    }

    #[test]
    fn rejects_exposed_countif_in_having() {
        // The HAVING condition IS exposed in SELECT - that's
        // parse_multi_aggregate_having's job (already-known alias), not
        // this shape. This mechanism requires a single SELECT aggregate,
        // so this case just fails to match at all (only one item, "cnt",
        // in the select list here means this isn't even a candidate).
        let sql = "SELECT peer_ip, countIf(operation = 'W') AS cnt FROM bgp \
                    WHERE collector = 'rrc00' GROUP BY peer_ip HAVING cnt > 0";
        assert!(parse_hidden_countif_having_query(sql).is_none());
    }

    #[test]
    fn rejects_non_countif_having_clause() {
        let sql = "SELECT peer_ip, count(*) AS cnt FROM bgp \
                    WHERE collector = 'rrc00' GROUP BY peer_ip HAVING cnt > 5";
        assert!(parse_hidden_countif_having_query(sql).is_none());
    }

    #[test]
    fn rejects_unenforceable_countif_condition() {
        let sql = "SELECT peer_ip, count(*) AS cnt FROM bgp \
                    WHERE collector = 'rrc00' GROUP BY peer_ip \
                    HAVING countIf(operation = origin) = 0";
        assert!(parse_hidden_countif_having_query(sql).is_none());
    }
}

#[cfg(test)]
mod two_stage_histogram_tests {
    use super::*;

    fn q078_sql() -> &'static str {
        "SELECT update_count, count(*) AS num_prefixes FROM ( \
           SELECT prefix, count(*) AS update_count FROM bgp.bgp_updates \
           WHERE collector = 'rrc00' \
           AND timestamp >= '2024-01-02 00:00:00' AND timestamp < '2024-01-03 00:00:00' \
           GROUP BY prefix \
         ) GROUP BY update_count ORDER BY update_count"
    }

    #[test]
    fn parses_q078_shape() {
        let m = parse_two_stage_histogram_query(q078_sql()).expect("should match");
        assert_eq!(m.outer_group_col, "update_count");
        assert_eq!(m.outer_alias, "num_prefixes");
        assert_eq!(m.inner_group_col, "prefix");
        assert_eq!(m.inner_alias, "update_count");
    }

    #[test]
    fn builds_inner_surrogate() {
        let m = parse_two_stage_histogram_query(q078_sql()).expect("should match");
        let surrogate = build_two_stage_histogram_inner_surrogate(&m);
        assert_eq!(
            surrogate,
            "SELECT prefix, count(*) AS update_count FROM bgp.bgp_updates WHERE collector = 'rrc00' \
             AND timestamp >= '2024-01-02 00:00:00' AND timestamp < '2024-01-03 00:00:00' GROUP BY prefix"
        );
    }

    #[test]
    fn rejects_outer_group_col_not_matching_inner_alias() {
        let sql = "SELECT prefix, count(*) AS n FROM ( \
                       SELECT prefix, count(*) AS update_count FROM bgp \
                       WHERE collector = 'rrc00' GROUP BY prefix \
                     ) GROUP BY prefix";
        assert!(parse_two_stage_histogram_query(sql).is_none());
    }

    #[test]
    fn rejects_non_count_inner_aggregate() {
        let sql = "SELECT total, count(*) AS n FROM ( \
                       SELECT prefix, sum(local_pref) AS total FROM bgp \
                       WHERE collector = 'rrc00' GROUP BY prefix \
                     ) GROUP BY total";
        assert!(parse_two_stage_histogram_query(sql).is_none());
    }
}

// ---------------------------------------------------------------------------
// Correlated IN/NOT IN subquery: `<col> [NOT] IN (SELECT ...)` as a
// top-level WHERE or HAVING clause, where the subquery's own SELECT list
// is exactly `<col>` (q089's "withdrawn but never announced" prefixes,
// q129's "top-5 origin ASNs" filter, q141's "peers active in the first
// half but not the second", q160's "prefixes seen on day 1" snapshot
// join). Both halves - the outer query with the IN/NOT IN clause
// stripped, and the inner subquery standing alone - are each an entirely
// ordinary, already-supported query shape (a classic aggregate, argMax,
// SELECT DISTINCT, or top-k), so neither side needs any new planning
// mechanism; only the DETECTION and TEXT SPLIT are new. Serve time reads
// both sides independently (through the query engine's own ordinary
// per-shape handlers) and joins them by checking set membership - see
// handle_correlated_in_subquery_sql in the query engine.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CorrelatedInSubqueryMatch {
    pub join_col: String,
    pub negated: bool,
    /// The outer query with the `[AND] <col> [NOT] IN (...)` clause (and
    /// its now-empty WHERE/HAVING keyword, if it was the only predicate)
    /// removed, otherwise verbatim.
    pub outer_query: String,
    /// The subquery's own SQL, standalone (its own SELECT/FROM/WHERE/
    /// GROUP BY/ORDER BY/LIMIT, unchanged) - the query engine reads this
    /// as if it had been asked for directly.
    pub inner_query: String,
}

/// Finds the first `<col> [NOT] IN (<subquery>)` in `query` whose
/// parenthesized content starts with `SELECT` (as opposed to a literal
/// `IN ('a', 'b')` list, which `SpatialPredicate::In` already handles).
/// Returns `(byte range of "[AND ]<col> [NOT] IN (...)" including any
/// immediately-preceding " AND ", col, negated, inner_query)`.
fn find_correlated_in_clause(query: &str) -> Option<(std::ops::Range<usize>, String, bool, String)> {
    let lower = query.to_lowercase();
    let mut search_from = 0;
    while let Some(rel) = lower[search_from..].find(" in (") {
        let in_idx = search_from + rel;
        let (negated, col_end) = if lower[..in_idx].ends_with(" not") {
            (true, in_idx - " not".len())
        } else {
            (false, in_idx)
        };
        let before = &query[..col_end];
        let col_start = before
            .rfind(|c: char| !(c.is_alphanumeric() || c == '_'))
            .map(|i| i + 1)
            .unwrap_or(0);
        let col = query[col_start..col_end].trim();
        let paren_open = in_idx + " in (".len() - 1;
        if is_bare_identifier(col) {
            let after_paren = query[paren_open + 1..].trim_start();
            if after_paren.to_lowercase().starts_with("select") {
                if let Some(paren_close) = find_matching_close_paren(query, paren_open) {
                    let inner_query = query[paren_open + 1..paren_close].trim().to_string();
                    // Extend the removed range backward over a preceding
                    // " AND " (or "AND " right after WHERE/HAVING), so the
                    // caller doesn't have to special-case which side the
                    // joiner is on.
                    let clause_start = col_start;
                    let leading = &query[..clause_start];
                    let leading_trimmed = leading.trim_end();
                    let remove_start = if leading_trimmed.to_lowercase().ends_with("and") {
                        leading_trimmed.len() - "and".len()
                    } else {
                        clause_start
                    };
                    return Some((
                        remove_start..paren_close + 1,
                        col.to_string(),
                        negated,
                        inner_query,
                    ));
                }
            }
        }
        search_from = in_idx + " in (".len();
    }
    None
}

pub fn looks_like_correlated_in_subquery_sql(query: &str) -> bool {
    find_correlated_in_clause(query).is_some()
}

/// Normalizes a `SELECT <col> FROM ... GROUP BY <col> [ORDER BY ...]
/// [LIMIT ...]` query - a bare group-by column with no aggregate anywhere
/// in the SELECT list - into a plannable shape. Two cases, both arising
/// as one half of a correlated-IN-subquery split (q129's inner top-k
/// subquery, q141's outer "distinct peers seen" listing) where the
/// original raw SQL simply never exposed the aggregate it needed:
///   - No ORDER BY, or ORDER BY on `col` itself: this is exactly a
///     `SELECT DISTINCT <col>` listing (GROUP BY with no aggregate is a
///     distinct-value listing by definition) - rewritten to that already-
///     supported shape, dropping the now-redundant GROUP BY.
///   - ORDER BY on an aggregate expression not in the SELECT list (q129's
///     `ORDER BY count(*) DESC LIMIT 5`, valid ClickHouse but invisible to
///     the classic parser, which only ever looks at the SELECT list): the
///     aggregate is exposed as a new SELECT-list item under a synthetic
///     alias, and ORDER BY rewritten to reference that alias - turning it
///     into an ordinary, already-detectable top-k shape.
/// Returns `query` unchanged (not `None`) for anything that isn't this
/// exact bare shape, so callers can call this unconditionally on any
/// query text without it being a NOP hazard.
fn normalize_bare_group_by_query(query: &str) -> String {
    let lower = query.to_lowercase();
    let Some(select_end) = lower.find("select").map(|i| i + "select".len()) else {
        return query.to_string();
    };
    let Some(from_idx) = lower.find("from") else {
        return query.to_string();
    };
    if select_end > from_idx {
        return query.to_string();
    }
    let select_col = query[select_end..from_idx].trim();
    if !is_bare_identifier(select_col) {
        return query.to_string();
    }
    let Some(group_idx) = lower.find("group by") else {
        return query.to_string();
    };
    let from_where = query[from_idx..group_idx].trim_end().to_string();

    let after_group = &query[group_idx + "group by".len()..];
    let after_group_lower = after_group.to_lowercase();
    let group_end = after_group_lower
        .find("having")
        .or_else(|| after_group_lower.find("order by"))
        .or_else(|| after_group_lower.find("limit"))
        .unwrap_or(after_group.len());
    let group_by_clause = after_group[..group_end].trim();
    if group_by_clause != select_col || after_group_lower[..group_end].contains("having") {
        return query.to_string();
    }
    let tail = after_group[group_end..].trim().trim_end_matches(';');

    let tail_lower = tail.to_lowercase();
    if let Some(ob_idx) = tail_lower.find("order by") {
        let after_order = &tail[ob_idx + "order by".len()..];
        let after_order_lower = after_order.to_lowercase();
        let end = after_order_lower.find("limit").unwrap_or(after_order.len());
        let order_items = split_top_level_commas(&after_order[..end]);
        if let [item] = order_items.as_slice() {
            let item_lower = item.to_lowercase();
            let (order_expr, direction) = if let Some(s) = item_lower.strip_suffix("desc") {
                (item[..s.len()].trim(), " DESC")
            } else if let Some(s) = item_lower.strip_suffix("asc") {
                (item[..s.len()].trim(), " ASC")
            } else {
                (item.trim(), "")
            };
            let order_upper = order_expr.to_uppercase();
            let is_aggregate = AGGREGATE_FUNCTIONS.iter().any(|f| order_upper.contains(f));
            if is_aggregate {
                const TOPK_ALIAS: &str = "__topk_count__";
                let limit_tail = tail[ob_idx..][end + "order by".len()..].trim();
                return format!(
                    "SELECT {col}, {order_expr} AS {alias} {from_where} GROUP BY {col} \
                     ORDER BY {alias}{direction} {limit_tail}",
                    col = select_col,
                    order_expr = order_expr,
                    alias = TOPK_ALIAS,
                    from_where = from_where,
                    direction = direction,
                    limit_tail = limit_tail,
                )
                .trim()
                .to_string();
            }
        }
    }

    // No ORDER BY (or ORDER BY on the column itself, left as-is - a
    // distinct listing sorted by its own value needs no rewriting): a
    // plain distinct-value listing.
    format!(
        "SELECT DISTINCT {col} {from_where} {tail}",
        col = select_col,
        from_where = from_where,
        tail = tail,
    )
    .trim()
    .to_string()
}

/// Strips a `FROM <table> AS <alias>` table alias and every `<alias>.`
/// column-qualifier from `query`, returning the alias-free text otherwise
/// unchanged. Every parser in this module expects bare column names
/// (`is_bare_identifier` rejects a `.`), so a query written with a table
/// alias - e.g. q028's `SELECT DISTINCT w.prefix FROM bgp.bgp_updates AS w
/// WHERE w.collector = ... AND w.prefix NOT IN (...)` - would otherwise
/// still fail to match after the IN-subquery clause is stripped out, since
/// every remaining `w.`-qualified reference is left behind. Only handles
/// the `FROM <table> AS <alias>` spelling (not a bareword alias with no
/// `AS`) - the one form seen in practice so far. Returns `query` unchanged
/// if there's no such alias.
fn strip_table_alias(query: &str) -> String {
    let lower = query.to_lowercase();
    let Some(from_idx) = lower.find("from") else {
        return query.to_string();
    };
    let after_from = &query[from_idx + "from".len()..];
    // "FROM" is immediately followed by whitespace, so after_from's own
    // first character IS whitespace - skip it to find where the table name
    // itself actually starts before looking for where it ends.
    let table_start = after_from
        .find(|c: char| !c.is_whitespace())
        .unwrap_or(after_from.len());
    let after_table_start = &after_from[table_start..];
    let after_table_start_lower = after_table_start.to_lowercase();
    let table_end = after_table_start_lower
        .find(|c: char| c.is_whitespace())
        .unwrap_or(after_table_start.len());
    let rest = after_table_start[table_end..].trim_start();
    let rest_lower = rest.to_lowercase();
    let Some(alias_rest_lower) = rest_lower.strip_prefix("as ") else {
        return query.to_string();
    };
    let alias_end = alias_rest_lower
        .find(|c: char| c.is_whitespace())
        .unwrap_or(alias_rest_lower.len());
    let as_alias_len = "as ".len() + alias_end;
    let alias = rest["as ".len()..as_alias_len].trim();
    if alias.is_empty() || !is_bare_identifier(alias) {
        return query.to_string();
    }
    let as_alias_text = &rest[..as_alias_len];
    let result = query.replacen(as_alias_text, "", 1);
    let alias_prefix = format!("{alias}.");
    result.replace(&alias_prefix, "")
}

pub fn parse_correlated_in_subquery_query(query: &str) -> Option<CorrelatedInSubqueryMatch> {
    let dealiased = strip_table_alias(query);
    let query = dealiased.as_str();
    let (removed_range, join_col, negated, inner_query) = find_correlated_in_clause(query)?;

    // The inner subquery's own SELECT list must be exactly the join
    // column (optionally DISTINCT) - conservative, matching every other
    // detector in this module: a subquery selecting anything else isn't a
    // plain membership set.
    let inner_lower = inner_query.to_lowercase();
    let inner_select_end = inner_lower.find("select")? + "select".len();
    let inner_from_idx = inner_lower.find("from")?;
    if inner_select_end > inner_from_idx {
        return None;
    }
    let inner_select_list = inner_query[inner_select_end..inner_from_idx].trim();
    let inner_select_list_lower = inner_select_list.to_lowercase();
    let bare_selected_col = inner_select_list_lower
        .strip_prefix("distinct")
        .map(|s| s.trim())
        .unwrap_or(inner_select_list);
    if bare_selected_col != join_col {
        return None;
    }
    // No further nested SELECT inside the subquery itself.
    if inner_lower[inner_from_idx..].matches("select").count() > 0 {
        return None;
    }

    // `SELECT <col> FROM ... WHERE ...` with no DISTINCT and no GROUP BY
    // (q089's shape) is, for IN/NOT IN membership-set purposes, exactly
    // equivalent to `SELECT DISTINCT <col> FROM ...` (duplicates don't
    // change set membership) - and unlike the bare form, SELECT DISTINCT
    // is an already-supported, plannable shape. Normalize so the inner
    // query registers/serves through that existing mechanism instead of
    // being rejected as an un-aggregated raw scan. Left alone when it
    // already says DISTINCT or has its own GROUP BY (q129's top-k shape).
    let inner_query = if !inner_select_list_lower.starts_with("distinct")
        && !inner_lower[inner_from_idx..].contains("group by")
    {
        format!(
            "SELECT DISTINCT{}",
            &inner_query[inner_select_end..]
        )
    } else {
        inner_query
    };

    let before = query[..removed_range.start].trim_end();
    let after = query[removed_range.end..].trim_start();

    // If the removed clause was the *entire* WHERE or HAVING predicate,
    // the keyword itself must go too, rather than leaving a dangling
    // "WHERE GROUP BY ..." / "HAVING LIMIT ...".
    let before_lower = before.to_lowercase();
    let (before, after) = if before_lower.ends_with("where") {
        (before[..before.len() - "where".len()].trim_end(), after)
    } else if before_lower.ends_with("having") {
        (before[..before.len() - "having".len()].trim_end(), after)
    } else {
        (before, after)
    };

    let outer_query = if after.is_empty() {
        before.to_string()
    } else {
        format!("{before} {after}")
    };
    let outer_query = normalize_bare_group_by_query(&outer_query);
    let inner_query = normalize_bare_group_by_query(&inner_query);

    Some(CorrelatedInSubqueryMatch {
        join_col,
        negated,
        outer_query,
        inner_query,
    })
}

#[cfg(test)]
mod correlated_in_subquery_tests {
    use super::*;

    #[test]
    fn q089_not_in_where_only_predicate_in_subquery() {
        let sql = "SELECT prefix, count(*) AS withdrawal_events \
                    FROM bgp.bgp_updates \
                    WHERE collector = 'rrc00' AND operation = 'W' \
                    AND timestamp >= '2024-01-31 00:00:00' AND timestamp < '2024-02-01 00:00:00' \
                    AND prefix NOT IN ( \
                      SELECT prefix FROM bgp.bgp_updates \
                      WHERE collector = 'rrc00' AND operation = 'A' \
                      AND timestamp >= '2024-01-31 00:00:00' AND timestamp < '2024-02-01 00:00:00' \
                    ) \
                    GROUP BY prefix ORDER BY withdrawal_events DESC LIMIT 30";
        let m = parse_correlated_in_subquery_query(sql).expect("should match");
        assert_eq!(m.join_col, "prefix");
        assert!(m.negated);
        assert!(!m.outer_query.to_lowercase().contains(" in ("), "got: {}", m.outer_query);
        assert!(m.outer_query.contains("GROUP BY prefix"), "got: {}", m.outer_query);
        assert!(m.outer_query.contains("operation = 'W'"), "got: {}", m.outer_query);
        assert!(m.inner_query.contains("operation = 'A'"), "got: {}", m.inner_query);
        assert!(!m.inner_query.to_lowercase().contains("group by"), "got: {}", m.inner_query);
        // Normalized to DISTINCT - a bare `SELECT prefix FROM ...` isn't a
        // plannable shape on its own, but is set-equivalent to DISTINCT.
        assert!(m.inner_query.to_lowercase().starts_with("select distinct"), "got: {}", m.inner_query);
    }

    #[test]
    fn q129_in_where_topk_subquery() {
        let sql = "SELECT origin, communities, count(*) AS cnt \
                    FROM bgp.bgp_updates \
                    WHERE collector = 'rrc00' AND operation = 'A' AND communities != '' \
                    AND timestamp >= '2024-01-10 00:00:00' AND timestamp < '2024-01-11 00:00:00' \
                    AND origin IN ( \
                      SELECT origin FROM bgp.bgp_updates \
                      WHERE collector = 'rrc00' AND operation = 'A' \
                      AND timestamp >= '2024-01-10 00:00:00' AND timestamp < '2024-01-11 00:00:00' \
                      GROUP BY origin ORDER BY count(*) DESC LIMIT 5 \
                    ) \
                    GROUP BY origin, communities ORDER BY origin, cnt DESC";
        let m = parse_correlated_in_subquery_query(sql).expect("should match");
        assert_eq!(m.join_col, "origin");
        assert!(!m.negated);
        assert!(m.outer_query.contains("communities != ''"), "got: {}", m.outer_query);
        assert!(m.inner_query.contains("LIMIT 5"), "got: {}", m.inner_query);
        // Normalized: the ORDER BY aggregate must be exposed in the
        // SELECT list under an alias, or the classic parser can never see
        // it (it only ever looks at SELECT-list items).
        assert!(m.inner_query.contains("origin, count(*) AS"), "got: {}", m.inner_query);
        assert!(m.inner_query.contains("ORDER BY __topk_count__ DESC"), "got: {}", m.inner_query);
    }

    #[test]
    fn q141_not_in_having_only_predicate() {
        let sql = "SELECT peer_ip FROM bgp.bgp_updates \
                    WHERE collector = 'rrc00' \
                    AND timestamp >= '2024-01-22 00:00:00' AND timestamp < '2024-01-22 12:00:00' \
                    GROUP BY peer_ip \
                    HAVING peer_ip NOT IN ( \
                      SELECT DISTINCT peer_ip FROM bgp.bgp_updates \
                      WHERE collector = 'rrc00' \
                      AND timestamp >= '2024-01-22 12:00:00' AND timestamp < '2024-01-23 00:00:00' \
                    ) LIMIT 30";
        let m = parse_correlated_in_subquery_query(sql).expect("should match");
        assert_eq!(m.join_col, "peer_ip");
        assert!(m.negated);
        assert!(!m.outer_query.to_lowercase().contains("having"), "got: {}", m.outer_query);
        assert!(m.outer_query.contains("LIMIT 30"), "got: {}", m.outer_query);
        assert!(m.inner_query.to_lowercase().contains("distinct"), "got: {}", m.inner_query);
        // Normalized: a bare `GROUP BY peer_ip` with no aggregate is a
        // distinct-value listing, an already-plannable shape.
        assert!(m.outer_query.to_lowercase().starts_with("select distinct"), "got: {}", m.outer_query);
        assert!(!m.outer_query.to_lowercase().contains("group by"), "got: {}", m.outer_query);
    }

    #[test]
    fn q160_in_where_select_distinct_subquery() {
        let sql = "SELECT prefix, argMax(as_path, timestamp) AS latest_path_by_jan15 \
                    FROM bgp.bgp_updates \
                    WHERE collector = 'rrc00' AND operation = 'A' \
                    AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-01-15 00:00:00' \
                    AND prefix IN ( \
                      SELECT DISTINCT prefix FROM bgp.bgp_updates \
                      WHERE collector = 'rrc00' AND operation = 'A' \
                      AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-01-02 00:00:00' \
                    ) \
                    GROUP BY prefix LIMIT 100";
        let m = parse_correlated_in_subquery_query(sql).expect("should match");
        assert_eq!(m.join_col, "prefix");
        assert!(!m.negated);
        assert!(m.outer_query.contains("argMax(as_path, timestamp)"), "got: {}", m.outer_query);
        assert!(m.outer_query.contains("GROUP BY prefix LIMIT 100"), "got: {}", m.outer_query);
    }

    #[test]
    fn q028_table_aliased_not_in_subquery() {
        let sql = "SELECT DISTINCT w.prefix \
                    FROM bgp.bgp_updates AS w \
                    WHERE w.collector = 'rrc00' \
                    AND w.operation = 'W' \
                    AND w.timestamp >= '2024-01-05 09:00:00' AND w.timestamp < '2024-01-05 10:00:00' \
                    AND w.prefix NOT IN ( \
                      SELECT prefix FROM bgp.bgp_updates \
                      WHERE collector = 'rrc00' \
                      AND operation = 'A' \
                      AND timestamp >= '2024-01-05 09:00:00' AND timestamp < '2024-01-05 10:00:00' \
                    ) \
                    LIMIT 100";
        let m = parse_correlated_in_subquery_query(sql).expect("should match");
        assert_eq!(m.join_col, "prefix");
        assert!(m.negated);
        // The table alias must be gone everywhere, not just from the
        // removed IN-clause - every downstream parser rejects a dotted
        // identifier as non-bare.
        assert!(!m.outer_query.contains('.') || m.outer_query.contains("bgp.bgp_updates"),
            "got: {}", m.outer_query);
        assert!(!m.outer_query.contains("w."), "got: {}", m.outer_query);
        assert!(!m.outer_query.contains(" AS w"), "got: {}", m.outer_query);
        assert!(m.outer_query.to_lowercase().starts_with("select distinct prefix"), "got: {}", m.outer_query);
        assert!(m.outer_query.contains("operation = 'W'"), "got: {}", m.outer_query);
        assert!(m.inner_query.contains("operation = 'A'"), "got: {}", m.inner_query);
    }

    #[test]
    fn rejects_literal_in_list() {
        // Already handled by SpatialPredicate::In - must not collide.
        let sql = "SELECT count(*) AS cnt FROM bgp WHERE collector IN ('rrc00', 'rrc01')";
        assert!(parse_correlated_in_subquery_query(sql).is_none());
    }

    #[test]
    fn rejects_subquery_selecting_something_else() {
        let sql = "SELECT count(*) AS cnt FROM bgp WHERE prefix IN (SELECT origin FROM bgp WHERE collector = 'rrc00')";
        assert!(parse_correlated_in_subquery_query(sql).is_none());
    }
}

// ---------------------------------------------------------------------------
// Weekly MOAS histogram: `SELECT <week_col>, count(*) AS <outer_alias> FROM
// (SELECT toStartOfWeek(<time_col>) AS <week_col>, <prefix_col>,
// uniqExact(<origin_col>) AS <cnt_alias> FROM ... WHERE ... GROUP BY
// <week_col>, <prefix_col> HAVING <cnt_alias> > <n>) GROUP BY <week_col>`
// (q133's "origin AS churn per week" shape: count, per calendar week, how
// many prefixes were MOAS'd - saw more than one origin ASN - that week).
// The inner query is MOAS generalized with an extra weekly-bucket grouping
// dimension (see get_weekly_moas_histogram_streaming_aggregation_configs
// in asap-planner-rs, a dedicated function rather than a change to
// get_moas_streaming_aggregation_configs, to keep zero risk to MOAS's own
// already-passing single-column-group shape). The outer stage, like
// TwoStageHistogramMatch, isn't a real second precompute: it's a count of
// how many (week, prefix) keys in the inner SetAggregator passed the
// origins-count HAVING filter, computed entirely at serve time - see
// handle_weekly_moas_histogram_sql in the query engine.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct WeeklyMoasHistogramMatch {
    pub week_col: String,
    pub time_col: String,
    pub prefix_col: String,
    pub origin_col: String,
    pub cnt_alias: String,
    pub having_op: String,
    pub having_threshold: f64,
    /// The INNER query's own "FROM ... WHERE ...", verbatim.
    pub from_where: String,
    pub outer_alias: String,
    /// The OUTER query's raw `ORDER BY ... LIMIT ...` tail, verbatim.
    pub order_by_and_limit: String,
}

pub fn parse_weekly_moas_histogram_query(query: &str) -> Option<WeeklyMoasHistogramMatch> {
    let lower = query.to_lowercase();
    let select_end = lower.find("select")? + "select".len();
    let from_idx = lower.find("from")?;
    if select_end > from_idx {
        return None;
    }

    let outer_items = split_top_level_commas(&query[select_end..from_idx]);
    if outer_items.len() != 2 {
        return None;
    }
    let outer_week_item = outer_items[0].trim().to_string();
    let (outer_agg_expr, outer_alias) = split_expr_alias(outer_items[1].trim())?;
    if !is_bare_count_star(&outer_agg_expr) {
        return None;
    }

    let after_from = &query[from_idx + "from".len()..];
    let after_from_trimmed = after_from.trim_start();
    if !after_from_trimmed.starts_with('(') {
        return None;
    }
    let subquery_open =
        from_idx + "from".len() + (after_from.len() - after_from_trimmed.len());
    let subquery_close = find_matching_close_paren(query, subquery_open)?;

    let inner_query = query[subquery_open + 1..subquery_close].trim();
    let inner_lower = inner_query.to_lowercase();
    let inner_select_end = inner_lower.find("select")? + "select".len();
    let inner_from_idx = inner_lower.find("from")?;
    if inner_select_end > inner_from_idx {
        return None;
    }
    if inner_lower[inner_from_idx..].matches("select").count() > 0 {
        return None;
    }

    let inner_items = split_top_level_commas(&inner_query[inner_select_end..inner_from_idx]);
    let [week_expr, prefix_item, cnt_item] = inner_items.as_slice() else {
        return None;
    };
    let (week_expr, week_col) = split_expr_alias(week_expr.trim())?;
    let (_label_type, time_col, _tok, _sel) = parse_computed_label_shape(&week_expr)
        .filter(|(label_type, ..)| *label_type == "week_start_bucket")?;

    let prefix_col = prefix_item.trim();
    if !is_bare_identifier(prefix_col) {
        return None;
    }

    let (cnt_expr, cnt_alias) = split_expr_alias(cnt_item.trim())?;
    let cnt_lower = cnt_expr.to_lowercase();
    if !cnt_lower.starts_with("uniqexact(") {
        return None;
    }
    let close_idx = find_matching_close_paren(&cnt_expr, "uniqexact".len())?;
    if close_idx != cnt_expr.len() - 1 {
        return None;
    }
    let origin_col = cnt_expr["uniqexact(".len()..close_idx].trim();
    if !is_bare_identifier(origin_col) {
        return None;
    }
    let origin_col = origin_col.to_string();

    // The outer's own week column, resolved two ways: either a bare
    // passthrough of the inner's own week alias, or (q133's actual shape)
    // a redundant `toStartOfWeek(<inner_week_alias>) AS <outer_alias>` -
    // ClickHouse's own subquery output re-wrapped in the same bucketing
    // function again, a no-op on an already-week-start value, but not
    // textually a bare reference to it.
    let outer_week_col = if outer_week_item == week_col {
        outer_week_item
    } else {
        let (redundant_expr, redundant_alias) = split_expr_alias(&outer_week_item)?;
        let (label_type, source_col, ..) = parse_computed_label_shape(&redundant_expr)?;
        if label_type != "week_start_bucket" || source_col != week_col {
            return None;
        }
        redundant_alias
    };

    let inner_group_idx = inner_lower.find("group by")?;
    let inner_from_where = inner_query[inner_from_idx..inner_group_idx]
        .trim_end()
        .to_string();

    let after_inner_group = &inner_query[inner_group_idx + "group by".len()..];
    let after_inner_group_lower = after_inner_group.to_lowercase();
    let having_idx = after_inner_group_lower.find("having")?;
    let inner_group_cols: Vec<String> = split_top_level_commas(&after_inner_group[..having_idx])
        .into_iter()
        .map(|s| s.trim().to_string())
        .collect();
    if inner_group_cols.len() != 2
        || inner_group_cols[0] != week_col
        || inner_group_cols[1] != prefix_col
    {
        return None;
    }

    let having_text = after_inner_group[having_idx + "having".len()..]
        .trim()
        .trim_end_matches(';');
    // No ORDER BY/LIMIT inside the inner query itself - conservative,
    // matching TwoStageHistogramMatch's own inner-query restriction.
    if having_text.to_lowercase().contains("order by") || having_text.to_lowercase().contains("limit") {
        return None;
    }
    if !having_text.to_lowercase().starts_with(&cnt_alias.to_lowercase()) {
        return None;
    }
    let rest = having_text[cnt_alias.len()..].trim();
    let (op_text, op_len) = ["<=", ">=", "!=", "<>", "=", "<", ">"]
        .iter()
        .find_map(|op| rest.starts_with(op).then_some((*op, op.len())))?;
    let having_threshold: f64 = rest[op_len..].trim().parse().ok()?;

    let outer_tail = &query[subquery_close + 1..];
    let outer_tail_lower = outer_tail.to_lowercase();
    let outer_group_idx = outer_tail_lower.find("group by")?;
    let after_outer_group = &outer_tail[outer_group_idx + "group by".len()..];
    let after_outer_group_lower = after_outer_group.to_lowercase();
    let group_end = after_outer_group_lower
        .find("order by")
        .or_else(|| after_outer_group_lower.find("limit"))
        .unwrap_or(after_outer_group.len());
    let outer_group_by_clause = after_outer_group[..group_end].trim().to_string();
    if outer_group_by_clause != outer_week_col {
        return None;
    }
    let order_by_and_limit = after_outer_group[group_end..]
        .trim()
        .trim_end_matches(';')
        .to_string();

    let (order_by_items, _) = parse_order_by_and_limit(&order_by_and_limit);
    if order_by_items
        .iter()
        .any(|item| item.column != outer_week_col && item.column != outer_alias)
    {
        return None;
    }

    Some(WeeklyMoasHistogramMatch {
        week_col: outer_week_col.to_string(),
        time_col,
        prefix_col: prefix_col.to_string(),
        origin_col,
        cnt_alias,
        having_op: op_text.to_string(),
        having_threshold,
        from_where: inner_from_where,
        outer_alias,
        order_by_and_limit,
    })
}

/// The INNER query, standing alone, as a MOAS-style query with an extra
/// weekly-bucket grouping column - `SELECT toStartOfWeek(<time_col>) AS
/// <week_col>, <prefix_col>, uniqExact(<origin_col>) AS <cnt_alias> FROM
/// ... GROUP BY <week_col>, <prefix_col>`. Deliberately drops the HAVING
/// clause: the surrogate itself must plan as an ordinary (unfiltered)
/// grouped cardinality query - filtering by origins count happens
/// entirely at serve time, same as MOAS's own `> 1` threshold.
pub fn build_weekly_moas_histogram_inner_surrogate(m: &WeeklyMoasHistogramMatch) -> String {
    // References week_col bare, not `toStartOfWeek(time_col)` again - by
    // the time this surrogate is parsed, week_col is already a real
    // computed metadata column (see the ComputedLabelConfig this
    // mechanism registers), the same "reference the alias as if it were
    // an ordinary column" convention every other computed-label surrogate
    // in this module uses (e.g. build_computed_group_by_surrogate).
    format!(
        "SELECT {week_col}, {prefix_col}, uniqExact({origin_col}) AS {cnt_alias} {from_where} GROUP BY {week_col}, {prefix_col}",
        week_col = m.week_col,
        prefix_col = m.prefix_col,
        origin_col = m.origin_col,
        cnt_alias = m.cnt_alias,
        from_where = m.from_where,
    )
}

#[cfg(test)]
mod weekly_moas_histogram_tests {
    use super::*;

    fn q133_sql() -> &'static str {
        "SELECT toStartOfWeek(timestamp) AS week_start, count(*) AS moas_prefix_count \
         FROM ( \
           SELECT toStartOfWeek(timestamp) AS timestamp, prefix, uniqExact(origin) AS origins \
           FROM bgp.bgp_updates \
           WHERE collector = 'rrc00' AND operation = 'A' \
           AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-02-01 00:00:00' \
           GROUP BY timestamp, prefix \
           HAVING origins > 1 \
         ) \
         GROUP BY week_start ORDER BY week_start"
    }

    #[test]
    fn parses_q133_shape() {
        let m = parse_weekly_moas_histogram_query(q133_sql()).expect("should match");
        assert_eq!(m.week_col, "week_start");
        assert_eq!(m.time_col, "timestamp");
        assert_eq!(m.prefix_col, "prefix");
        assert_eq!(m.origin_col, "origin");
        assert_eq!(m.cnt_alias, "origins");
        assert_eq!(m.having_op, ">");
        assert_eq!(m.having_threshold, 1.0);
        assert_eq!(m.outer_alias, "moas_prefix_count");
    }

    #[test]
    fn builds_inner_surrogate_without_having() {
        let m = parse_weekly_moas_histogram_query(q133_sql()).expect("should match");
        let surrogate = build_weekly_moas_histogram_inner_surrogate(&m);
        // Bare week_start reference, not a recomputed toStartOfWeek(...) -
        // week_start is registered as its own real computed metadata
        // column, so the surrogate must reference it directly (matching
        // every other computed-label surrogate's convention) or the
        // classic parser can't handle the computed expression at all.
        assert_eq!(
            surrogate,
            "SELECT week_start, prefix, uniqExact(origin) AS origins FROM bgp.bgp_updates \
             WHERE collector = 'rrc00' AND operation = 'A' \
             AND timestamp >= '2024-01-01 00:00:00' AND timestamp < '2024-02-01 00:00:00' \
             GROUP BY week_start, prefix"
        );
        assert!(surrogate.contains("GROUP BY week_start, prefix"), "got: {surrogate}");
        assert!(!surrogate.to_lowercase().contains("having"), "got: {surrogate}");
        assert!(surrogate.contains("uniqExact(origin) AS origins"), "got: {surrogate}");
    }

    #[test]
    fn rejects_mismatched_outer_group_col() {
        let sql = "SELECT other_col, count(*) AS n FROM ( \
                       SELECT toStartOfWeek(timestamp) AS week_start, prefix, uniqExact(origin) AS origins \
                       FROM bgp WHERE collector = 'rrc00' GROUP BY week_start, prefix HAVING origins > 1 \
                     ) GROUP BY other_col";
        assert!(parse_weekly_moas_histogram_query(sql).is_none());
    }
}

