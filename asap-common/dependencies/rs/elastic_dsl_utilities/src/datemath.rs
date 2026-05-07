use crate::ast_parsing::query_info::{FieldName, Predicate, TermValue};
use serde::{Deserialize, Serialize};

/// Time range bounds resolved into epoch milliseconds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedTimeRange {
    pub field: FieldName,
    pub gte_ms: Option<i64>,
    pub lte_ms: Option<i64>,
}

/// An optional time range applied to a timestamp field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeRange {
    pub field: FieldName,
    pub gte: Option<String>,
    pub lte: Option<String>,
}

impl TimeRange {
    /// Parse a date-math expression into epoch milliseconds using the provided
    /// `now_ms` as reference for `now`-relative expressions.
    ///
    /// Supported forms:
    /// - `now`
    /// - `now-30s`, `now+5m`, `now-1h`, `now-2d`, `now-1w`, `now-500ms`
    /// - RFC3339 timestamps (e.g. `2026-03-22T12:34:56Z`)
    /// - Plain integer timestamps (returned as-is)
    pub fn parse_date_math(expr: &str, now_ms: i64) -> Option<i64> {
        if expr == "now" {
            return Some(now_ms);
        }

        if let Some(delta) = Self::parse_now_delta_ms(expr) {
            return now_ms.checked_add(delta);
        }

        if let Ok(v) = expr.parse::<i64>() {
            return Some(v);
        }

        chrono::DateTime::parse_from_rfc3339(expr)
            .ok()
            .map(|dt| dt.timestamp_millis())
    }

    /// Resolve `gte`/`lte` date-math strings into numeric epoch-millisecond
    /// values relative to `now_ms`.
    pub fn resolve_epoch_millis(&self, now_ms: i64) -> Option<ResolvedTimeRange> {
        let gte_ms = match &self.gte {
            Some(v) => Some(Self::parse_date_math(v, now_ms)?),
            None => None,
        };
        let lte_ms = match &self.lte {
            Some(v) => Some(Self::parse_date_math(v, now_ms)?),
            None => None,
        };

        Some(ResolvedTimeRange {
            field: self.field.clone(),
            gte_ms,
            lte_ms,
        })
    }

    fn parse_now_delta_ms(expr: &str) -> Option<i64> {
        let rest = expr.strip_prefix("now")?;
        if rest.is_empty() {
            return Some(0);
        }

        let sign_char = rest.chars().next()?;
        let sign = match sign_char {
            '+' => 1_i64,
            '-' => -1_i64,
            _ => return None,
        };

        let offset = &rest[1..];
        if offset.is_empty() {
            return None;
        }

        let digit_count = offset.chars().take_while(|c| c.is_ascii_digit()).count();
        if digit_count == 0 || digit_count == offset.len() {
            return None;
        }

        let qty = offset[..digit_count].parse::<i64>().ok()?;
        let unit = &offset[digit_count..];
        let unit_ms = match unit {
            "ms" => 1_i64,
            "s" => 1_000_i64,
            "m" => 60_000_i64,
            "h" => 3_600_000_i64,
            "d" => 86_400_000_i64,
            "w" => 604_800_000_i64,
            _ => return None,
        };

        qty.checked_mul(unit_ms)?.checked_mul(sign)
    }
}

pub fn range_query_to_time_range(predicate: &Predicate, now_ms: i64) -> Option<ResolvedTimeRange> {
    match predicate {
        Predicate::Range { field, gte, lte } => {
            let tr = TimeRange {
                field: field.clone(),
                gte: gte.as_ref().and_then(|v| match v {
                    TermValue::String(s) => Some(s.clone()),
                    _ => None,
                }),
                lte: lte.as_ref().and_then(|v| match v {
                    TermValue::String(s) => Some(s.clone()),
                    _ => None,
                }),
            };
            let resolved = tr.resolve_epoch_millis(now_ms)?;
            Some(resolved)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_date_math_supports_now_relative_and_numeric() {
        let now_ms = 1_700_000_000_000_i64;
        assert_eq!(TimeRange::parse_date_math("now", now_ms), Some(now_ms));
        assert_eq!(
            TimeRange::parse_date_math("now-30s", now_ms),
            Some(now_ms - 30_000)
        );
        assert_eq!(
            TimeRange::parse_date_math("now+5m", now_ms),
            Some(now_ms + 300_000)
        );
        assert_eq!(
            TimeRange::parse_date_math("1700000000123", now_ms),
            Some(1_700_000_000_123)
        );
    }

    #[test]
    fn parse_date_math_supports_rfc3339() {
        let now_ms = 0;
        let value = TimeRange::parse_date_math("2026-03-22T12:34:56Z", now_ms)
            .expect("RFC3339 timestamp should parse");
        assert_eq!(value, 1_774_182_896_000);
    }

    #[test]
    fn parse_date_math_rejects_invalid_expressions() {
        let now_ms = 1_700_000_000_000_i64;
        assert_eq!(TimeRange::parse_date_math("now+", now_ms), None);
        assert_eq!(TimeRange::parse_date_math("now-10", now_ms), None);
        assert_eq!(TimeRange::parse_date_math("yesterday", now_ms), None);
    }

    #[test]
    fn resolve_epoch_millis_resolves_both_bounds() {
        let range = TimeRange {
            field: "@timestamp".to_string(),
            gte: Some("now-1m".to_string()),
            lte: Some("now".to_string()),
        };
        let now_ms = 2_000_000_i64;

        let resolved = range
            .resolve_epoch_millis(now_ms)
            .expect("range should resolve");
        assert_eq!(resolved.field, "@timestamp");
        assert_eq!(resolved.gte_ms, Some(1_940_000));
        assert_eq!(resolved.lte_ms, Some(2_000_000));
    }

    #[test]
    fn range_query_to_time_range_converts_range_predicate() {
        let predicate = Predicate::Range {
            field: "@timestamp".to_string(),
            gte: Some(TermValue::String("now-30s".to_string())),
            lte: Some(TermValue::String("now".to_string())),
        };
        let now_ms = 1_000_000_i64;

        let resolved =
            range_query_to_time_range(&predicate, now_ms).expect("range predicate should convert");
        assert_eq!(resolved.field, "@timestamp");
        assert_eq!(resolved.gte_ms, Some(970_000));
        assert_eq!(resolved.lte_ms, Some(1_000_000));
    }

    #[test]
    fn range_query_to_time_range_returns_none_for_non_range_predicates() {
        let predicate = Predicate::Term {
            field: "service".to_string(),
            value: TermValue::String("frontend".to_string()),
        };
        assert!(range_query_to_time_range(&predicate, 100).is_none());
    }
}
