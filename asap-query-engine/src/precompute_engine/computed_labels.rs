use chrono::{Datelike, NaiveDateTime, Timelike};
use regex::Regex;

// ComputedLabelConfig now lives in asap_types so asap-planner-rs can
// construct it (from detecting a nested-subquery SQL shape) and the engine
// can consume it (to actually compute the label at ingest time) without
// either crate depending on the other - see asap_types::computed_label for
// the shared definition and design note.
pub use asap_types::computed_label::ComputedLabelConfig;

pub fn should_skip_on_missing(rule: &ComputedLabelConfig) -> bool {
    rule.on_missing.as_deref() == Some("skip_sample")
}

fn tokenize(rule: &ComputedLabelConfig, raw_value: &str) -> Result<Vec<String>, String> {
    let tokenizer = rule.tokenizer.as_deref().unwrap_or("whitespace");

    let mut tokens: Vec<String> = if tokenizer == "whitespace" {
        raw_value
            .split_whitespace()
            .filter(|x| !x.is_empty())
            .map(|x| x.to_string())
            .collect()
    } else if let Some(sep) = tokenizer.strip_prefix("char:") {
        // Matches ClickHouse's splitByChar(sep, col): split on exactly one
        // literal separator character, keeping empty segments (unlike the
        // whitespace tokenizer above, which drops them) - "a//b".split('/')
        // is ["a", "", "b"] in ClickHouse, not ["a", "b"].
        let mut chars = sep.chars();
        let sep_char = chars
            .next()
            .ok_or_else(|| "char tokenizer needs exactly one separator character".to_string())?;
        if chars.next().is_some() {
            return Err(format!(
                "char tokenizer separator must be exactly one character, got {:?}",
                sep
            ));
        }
        raw_value.split(sep_char).map(|x| x.to_string()).collect()
    } else {
        return Err(format!(
            "unsupported computed-label tokenizer {:?}; use \"whitespace\" or \"char:<c>\"",
            tokenizer
        ));
    };

    if let Some(pat) = rule.filter_regex.as_deref() {
        let re = Regex::new(pat)
            .map_err(|e| format!("invalid computed-label filter_regex {:?}: {}", pat, e))?;
        tokens.retain(|x| re.is_match(x));
    }

    Ok(tokens)
}

/// Parses the ingest CSV's timestamp string format ("%Y-%m-%d %H:%M:%S",
/// ClickHouse's default DateTime rendering) for the hour_of_day/day_of_week
/// computed-label types.
fn parse_ingest_timestamp(raw_value: &str) -> Result<NaiveDateTime, String> {
    NaiveDateTime::parse_from_str(raw_value, "%Y-%m-%d %H:%M:%S")
        .map_err(|e| format!("invalid timestamp {:?} for cyclical computed label: {}", raw_value, e))
}

pub fn compute_label_values(
    rule: &ComputedLabelConfig,
    raw_value: &str,
) -> Result<Vec<String>, String> {
    match rule.r#type.as_str() {
        "field_alias" => {
            if raw_value.is_empty() && should_skip_on_missing(rule) {
                Ok(vec![])
            } else {
                Ok(vec![raw_value.to_string()])
            }
        }

        "token_select" => {
            let tokens = tokenize(rule, raw_value)?;
            if tokens.is_empty() {
                return Ok(vec![]);
            }

            let selected = match rule.select.as_deref().unwrap_or("last") {
                "first" => tokens.first().cloned(),
                "last" => tokens.last().cloned(),
                sel if sel.starts_with("nth:") => {
                    let idx: usize = sel["nth:".len()..].parse().map_err(|e| {
                        format!("invalid token_select index in select={:?}: {}", sel, e)
                    })?;
                    tokens.get(idx).cloned()
                }
                other => {
                    return Err(format!(
                        "unsupported token_select selector {:?}; use first, last, or nth:N",
                        other
                    ));
                }
            };

            Ok(selected.into_iter().collect())
        }

        "token_explode" => tokenize(rule, raw_value),

        // arrayJoin(arrayZip(arraySlice(...), arraySlice(...))) - every
        // adjacent pair of the column's split tokens, exploded into one
        // row per pair (q067's AS-path "edge" extraction: tokens [A,B,C]
        // become the pairs (A,B) and (B,C)). Rendered as a ClickHouse-style
        // `('a','b')` tuple-literal string, matching how ClickHouse itself
        // renders a Tuple(String, String).
        "token_pair_explode" => {
            let tokens = tokenize(rule, raw_value)?;
            if tokens.len() < 2 {
                return Ok(vec![]);
            }
            Ok(tokens
                .windows(2)
                .map(|w| format!("('{}','{}')", w[0].replace('\'', "\\'"), w[1].replace('\'', "\\'")))
                .collect())
        }

        // length(splitByChar(sep, col)) - the token *count*, not a
        // specific token. Reuses the same tokenize() as token_select/
        // token_explode, just reports len() instead of indexing.
        "split_length" => {
            let tokens = tokenize(rule, raw_value)?;
            Ok(vec![tokens.len().to_string()])
        }

        // length(col) - plain byte length of the raw column value.
        // ClickHouse's length() on a String returns byte count, matching
        // Rust's str::len(); this dataset's columns are ASCII, so byte
        // count and character count coincide.
        "string_length" => Ok(vec![raw_value.len().to_string()]),

        // toHour(col) - hour of day (0-23) from a "%Y-%m-%d %H:%M:%S"
        // timestamp string, matching ClickHouse's toHour().
        "hour_of_day" => {
            let dt = parse_ingest_timestamp(raw_value)?;
            Ok(vec![dt.hour().to_string()])
        }

        // toDayOfWeek(col) - ISO weekday (1=Monday..7=Sunday), matching
        // ClickHouse's toDayOfWeek(). chrono's Weekday::number_from_monday
        // already uses this exact 1-7 convention.
        "day_of_week" => {
            let dt = parse_ingest_timestamp(raw_value)?;
            Ok(vec![dt.weekday().number_from_monday().to_string()])
        }

        // toDate(col) - the calendar day, as a "YYYY-MM-DD" string, matching
        // ClickHouse's own toDate() rendering. This is also usable as a
        // GROUP BY key display value (see computed_group_by), unlike
        // hour_of_day/day_of_week's plain integers - a derived-value stream
        // built over this label (see DerivedValueSource::ComputedLabel in
        // csv_ingest.rs) additionally falls back to parsing this format
        // when a plain f64 parse fails, so distinct-count-of-days still
        // works from the exact same label.
        "date_bucket" => {
            let dt = parse_ingest_timestamp(raw_value)?;
            Ok(vec![dt.format("%Y-%m-%d").to_string()])
        }

        // toStartOfFiveMinutes(col) - the timestamp floored to the nearest
        // 5-minute mark, as a "YYYY-MM-DD HH:MM:00" string matching
        // ClickHouse's own toStartOfFiveMinutes() rendering (q187's
        // "5-minute bucket trend" shape).
        "five_minute_bucket" => {
            let dt = parse_ingest_timestamp(raw_value)?;
            let floored_minute = (dt.minute() / 5) * 5;
            let floored = dt
                .with_minute(floored_minute)
                .and_then(|d| d.with_second(0))
                .ok_or_else(|| format!("failed to floor timestamp {:?} to 5-minute mark", raw_value))?;
            Ok(vec![floored.format("%Y-%m-%d %H:%M:%S").to_string()])
        }

        // toStartOfWeek(col) - the Sunday-aligned calendar week the
        // timestamp falls in (ClickHouse's default mode 0), as a
        // "YYYY-MM-DD" date string (q133's weekly-bucketed MOAS shape).
        // chrono's Weekday::num_days_from_sunday() is 0 for Sunday itself,
        // matching the days to step back to reach that week's Sunday.
        "week_start_bucket" => {
            let dt = parse_ingest_timestamp(raw_value)?;
            let days_since_sunday = dt.weekday().num_days_from_sunday() as i64;
            let week_start = dt.date() - chrono::Duration::days(days_since_sunday);
            Ok(vec![week_start.format("%Y-%m-%d").to_string()])
        }

        // splitByChar(sep, col)[1] || '<lit1>' || splitByChar(sep, col)[2]
        // || '<lit2>' - the first two split tokens joined with literal
        // text (q146's `octet1.octet2.0.0/16` supernet-from-prefix shape).
        // `select` packs the two literals as `lit1\u{1}lit2` (see
        // parse_computed_label_shape's doc comment on this branch).
        "concat_two_tokens" => {
            let tokens = tokenize(rule, raw_value)?;
            let (lit1, lit2) = rule
                .select
                .as_deref()
                .and_then(|s| s.split_once('\u{1}'))
                .ok_or_else(|| {
                    "concat_two_tokens requires a select field of \"lit1\\u{1}lit2\"".to_string()
                })?;
            let (Some(t0), Some(t1)) = (tokens.first(), tokens.get(1)) else {
                return Ok(vec![]);
            };
            Ok(vec![format!("{t0}{lit1}{t1}{lit2}")])
        }

        // arraySlice(splitByChar(sep, col), start, len) - a slice of the
        // column's split tokens (q174's "first N hops" shape), rendered as
        // a ClickHouse-style `['a','b']` array-literal string. `select`
        // packs the 0-indexed start offset and length as `start:len` (see
        // parse_computed_label_shape's doc comment on this branch).
        "array_slice" => {
            let tokens = tokenize(rule, raw_value)?;
            let (start_str, len_str) = rule
                .select
                .as_deref()
                .and_then(|s| s.split_once(':'))
                .ok_or_else(|| {
                    "array_slice requires a select field of \"start:len\"".to_string()
                })?;
            let start: usize = start_str
                .parse()
                .map_err(|e| format!("invalid array_slice start {:?}: {}", start_str, e))?;
            let len: usize = len_str
                .parse()
                .map_err(|e| format!("invalid array_slice len {:?}: {}", len_str, e))?;
            let end = start.saturating_add(len);
            if tokens.len() < end {
                return Ok(vec![]);
            }
            let quoted: Vec<String> = tokens[start..end]
                .iter()
                .map(|v| format!("'{}'", v.replace('\'', "\\'")))
                .collect();
            Ok(vec![format!("[{}]", quoted.join(","))])
        }

        other => Err(format!("unsupported computed-label type {:?}", other)),
    }
}

#[cfg(test)]
mod concat_two_tokens_tests {
    use super::*;

    fn rule(select: &str) -> ComputedLabelConfig {
        ComputedLabelConfig {
            r#type: "concat_two_tokens".to_string(),
            source_col: "prefix".to_string(),
            tokenizer: Some("char:.".to_string()),
            filter_regex: None,
            select: Some(select.to_string()),
            on_missing: None,
        }
    }

    #[test]
    fn builds_supernet_from_ipv4_prefix() {
        let r = rule(".\u{1}.0.0/16");
        let values = compute_label_values(&r, "8.8.8.0/24").expect("should compute");
        assert_eq!(values, vec!["8.8.0.0/16".to_string()]);
    }

    #[test]
    fn different_octets() {
        let r = rule(".\u{1}.0.0/16");
        let values = compute_label_values(&r, "192.168.1.0/24").expect("should compute");
        assert_eq!(values, vec!["192.168.0.0/16".to_string()]);
    }

    #[test]
    fn missing_select_field_errors() {
        let mut r = rule(".\u{1}.0.0/16");
        r.select = None;
        assert!(compute_label_values(&r, "8.8.8.0/24").is_err());
    }

    #[test]
    fn too_few_tokens_produces_no_value() {
        // A raw value with fewer than 2 '.'-separated segments can't yield
        // both octets - skipped, not an error, matching every other
        // computed-label shape's "no value" convention.
        let r = rule(".\u{1}.0.0/16");
        let values = compute_label_values(&r, "8").expect("should not error");
        assert_eq!(values, Vec::<String>::new());
    }
}

#[cfg(test)]
mod array_slice_tests {
    use super::*;

    fn rule(select: &str) -> ComputedLabelConfig {
        ComputedLabelConfig {
            r#type: "array_slice".to_string(),
            source_col: "as_path".to_string(),
            tokenizer: Some("char: ".to_string()),
            filter_regex: None,
            select: Some(select.to_string()),
            on_missing: None,
        }
    }

    #[test]
    fn builds_first_two_hops_array_string() {
        let r = rule("0:2");
        let values = compute_label_values(&r, "65001 65002 65003").expect("should compute");
        assert_eq!(values, vec!["['65001','65002']".to_string()]);
    }

    #[test]
    fn too_few_tokens_produces_no_value() {
        let r = rule("0:2");
        let values = compute_label_values(&r, "65001").expect("should not error");
        assert_eq!(values, Vec::<String>::new());
    }

    #[test]
    fn nonzero_start_offset() {
        let r = rule("1:2");
        let values = compute_label_values(&r, "65001 65002 65003 65004").expect("should compute");
        assert_eq!(values, vec!["['65002','65003']".to_string()]);
    }

    #[test]
    fn missing_select_field_errors() {
        let mut r = rule("0:2");
        r.select = None;
        assert!(compute_label_values(&r, "65001 65002").is_err());
    }
}

#[cfg(test)]
mod token_pair_explode_tests {
    use super::*;

    fn rule() -> ComputedLabelConfig {
        ComputedLabelConfig {
            r#type: "token_pair_explode".to_string(),
            source_col: "as_path".to_string(),
            tokenizer: Some("whitespace".to_string()),
            filter_regex: None,
            select: None,
            on_missing: Some("skip_sample".to_string()),
        }
    }

    #[test]
    fn explodes_adjacent_pairs() {
        let values = compute_label_values(&rule(), "65001 65002 65003").expect("should compute");
        assert_eq!(
            values,
            vec!["('65001','65002')".to_string(), "('65002','65003')".to_string()]
        );
    }

    #[test]
    fn single_token_produces_no_pairs() {
        let values = compute_label_values(&rule(), "65001").expect("should not error");
        assert_eq!(values, Vec::<String>::new());
    }

    #[test]
    fn two_tokens_produce_one_pair() {
        let values = compute_label_values(&rule(), "65001 65002").expect("should compute");
        assert_eq!(values, vec!["('65001','65002')".to_string()]);
    }

    #[test]
    fn long_path_produces_n_minus_one_pairs() {
        let values =
            compute_label_values(&rule(), "1 2 3 4 5").expect("should compute");
        assert_eq!(values.len(), 4);
        assert_eq!(values[0], "('1','2')");
        assert_eq!(values[3], "('4','5')");
    }
}


#[cfg(test)]
mod week_start_bucket_tests {
    use super::*;

    fn rule() -> ComputedLabelConfig {
        ComputedLabelConfig {
            r#type: "week_start_bucket".to_string(),
            source_col: "timestamp".to_string(),
            tokenizer: None,
            filter_regex: None,
            select: None,
            on_missing: None,
        }
    }

    #[test]
    fn monday_maps_to_preceding_sunday() {
        let values = compute_label_values(&rule(), "2024-01-01 12:00:00").expect("should compute");
        assert_eq!(values, vec!["2023-12-31".to_string()]);
    }

    #[test]
    fn wednesday_maps_to_preceding_sunday() {
        let values = compute_label_values(&rule(), "2024-01-10 08:30:00").expect("should compute");
        assert_eq!(values, vec!["2024-01-07".to_string()]);
    }

    #[test]
    fn sunday_maps_to_itself() {
        let values = compute_label_values(&rule(), "2024-01-07 23:59:59").expect("should compute");
        assert_eq!(values, vec!["2024-01-07".to_string()]);
    }
}
