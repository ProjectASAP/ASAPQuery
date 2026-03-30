use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub query: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    /// Step in seconds; 0 means instant query.
    pub step: u64,
    pub ts: DateTime<Utc>,
}

// ── raw serde shapes ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct RawEntry {
    params: RawParams,
    ts: DateTime<Utc>,
}

#[derive(Deserialize)]
struct RawParams {
    query: String,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    step: u64,
}

// ── public API ────────────────────────────────────────────────────────────────

/// Parse a Prometheus query log file (newline-delimited JSON).
/// Malformed or incomplete lines are skipped with a warning; they never cause a panic.
pub fn parse_log_file(path: &std::path::Path) -> Result<Vec<LogEntry>, std::io::Error> {
    let contents = std::fs::read_to_string(path)?;
    Ok(parse_log_str(&contents))
}

/// Parse a Prometheus query log from an in-memory string.
pub fn parse_log_str(input: &str) -> Vec<LogEntry> {
    input
        .lines()
        .enumerate()
        .filter_map(|(line_no, line)| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            match serde_json::from_str::<RawEntry>(line) {
                Ok(raw) => Some(LogEntry {
                    query: raw.params.query,
                    start: raw.params.start,
                    end: raw.params.end,
                    step: raw.params.step,
                    ts: raw.ts,
                }),
                Err(e) => {
                    tracing::warn!(line = line_no + 1, error = %e, "skipping malformed query log line");
                    None
                }
            }
        })
        .collect()
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const INSTANT_LINE: &str = r#"{"params":{"end":"2025-12-02T18:00:00.000Z","query":"rate(http_requests_total[5m])","start":"2025-12-02T18:00:00.000Z","step":0},"ts":"2025-12-02T18:00:00.001Z"}"#;
    const RANGE_LINE: &str = r#"{"params":{"end":"2025-12-02T19:00:00.000Z","query":"rate(http_requests_total[5m])","start":"2025-12-02T18:00:00.000Z","step":30},"ts":"2025-12-02T18:00:00.001Z"}"#;

    #[test]
    fn parse_valid_instant_entry() {
        let entries = parse_log_str(INSTANT_LINE);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.query, "rate(http_requests_total[5m])");
        assert_eq!(e.step, 0);
        assert_eq!(e.start, e.end);
    }

    #[test]
    fn parse_valid_range_entry() {
        let entries = parse_log_str(RANGE_LINE);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.step, 30);
        assert_ne!(e.start, e.end);
        let duration = (e.end - e.start).num_seconds();
        assert_eq!(duration, 3600);
    }

    #[test]
    fn malformed_json_returns_empty() {
        let entries = parse_log_str("not valid json at all");
        assert!(entries.is_empty());
    }

    #[test]
    fn missing_params_field_skipped() {
        let entries = parse_log_str(r#"{"ts":"2025-12-02T18:00:00.000Z"}"#);
        assert!(entries.is_empty());
    }

    #[test]
    fn mixed_lines_skips_bad() {
        let input = format!("{}\nnot json\n{}", INSTANT_LINE, INSTANT_LINE);
        let entries = parse_log_str(&input);
        assert_eq!(entries.len(), 2);
    }
}
