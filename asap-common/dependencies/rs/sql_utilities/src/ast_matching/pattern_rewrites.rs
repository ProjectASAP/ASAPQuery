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

fn extract_paren_arg_after<'a>(sql: &'a str, marker_lower: &str) -> Option<&'a str> {
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

/// True for a query with no aggregate function anywhere in its text and no
/// GROUP BY clause: a raw row scan (`SELECT DISTINCT ...`, or a bare column
/// listing). No precomputed summary can ever answer this - the whole point
/// of DISTINCT / raw-listing semantics is exact per-row output, which by
/// definition a lossy aggregate summary cannot provide. Callers should treat
/// a `true` result as "route this query straight to the source of truth,"
/// not as "try harder to plan it." Intended as a last-resort check, run only
/// after every other pattern in this module has had a chance to claim the
/// query.
const AGGREGATE_FUNCTIONS: &[&str] = &[
    "COUNT(",
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

pub fn looks_like_exact_only_sql(query: &str) -> bool {
    let upper = query.to_uppercase();
    if upper.contains("GROUP BY") {
        return false;
    }
    !AGGREGATE_FUNCTIONS.iter().any(|f| upper.contains(f))
}

// ---------------------------------------------------------------------------
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
}

/// Paren- and quote-aware top-level comma split - a SELECT list item like
/// `count(distinct foo)` must not be split on the comma inside its own
/// argument list.
fn split_top_level_commas(s: &str) -> Vec<String> {
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

pub fn parse_multi_aggregate_query(query: &str) -> Option<MultiAggregateMatch> {
    let lower = query.to_lowercase();
    if lower.contains("having") {
        // Not handled by this pattern yet - HAVING needs its own semantics
        // per aggregate, not just a split.
        return None;
    }

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
    if aggregate_exprs.len() < 2 {
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

    Some(MultiAggregateMatch {
        group_by_cols,
        aggregate_exprs,
        from_where,
        group_by_clause,
    })
}

pub fn looks_like_multi_aggregate_sql(query: &str) -> bool {
    parse_multi_aggregate_query(query).is_some()
}

/// One single-aggregate surrogate per aggregate expression, in the same
/// order as `m.aggregate_exprs` - each independently plannable/servable by
/// the existing classic single-aggregate machinery.
pub fn build_multi_aggregate_surrogates(m: &MultiAggregateMatch) -> Vec<String> {
    m.aggregate_exprs
        .iter()
        .map(|expr| {
            format!(
                "SELECT {group_by}, {expr} {from_where} GROUP BY {group_by}",
                group_by = m.group_by_clause,
                expr = expr,
                from_where = m.from_where,
            )
        })
        .collect()
}
