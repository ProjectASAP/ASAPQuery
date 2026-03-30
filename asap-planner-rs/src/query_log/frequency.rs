use chrono::{DateTime, Utc};
use std::collections::HashMap;

use super::parser::LogEntry;

/// A single (query_string, step) variant paired with all its log entries.
type QueryVariant<'a> = ((String, u64), Vec<&'a LogEntry>);

#[derive(Debug, Clone, PartialEq)]
pub struct InstantQueryInfo {
    pub query: String,
    /// Median inter-arrival time rounded to nearest scrape interval (seconds).
    pub repetition_delay: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RangeQueryInfo {
    pub query: String,
    /// Median inter-arrival time rounded to nearest scrape interval (seconds).
    pub repetition_delay: u64,
    /// Step from params.step (seconds).
    pub step: u64,
    /// Median of (end − start) across occurrences (seconds).
    pub range_duration: u64,
}

/// Infer query repetition delays from a slice of parsed log entries.
///
/// Returns `(instant_queries, range_queries)`.
///
/// Rules:
/// - Entries with step == 0 are instant; others are range.
/// - Group by (query_string, step).
/// - If the same query string appears under multiple step values, keep the
///   variant with the most occurrences and warn about discarded variants.
/// - Groups with only 1 occurrence are skipped with a warning.
/// - `repetition_delay` = median inter-arrival time, rounded to nearest
///   `scrape_interval` second.
/// - If `|raw_median − rounded| / scrape_interval ≥ 0.1` a warning is emitted.
pub fn infer_queries(
    entries: &[LogEntry],
    scrape_interval: u64,
) -> (Vec<InstantQueryInfo>, Vec<RangeQueryInfo>) {
    // Group by (query_string, step)
    let mut groups: HashMap<(String, u64), Vec<&LogEntry>> = HashMap::new();
    for entry in entries {
        groups
            .entry((entry.query.clone(), entry.step))
            .or_default()
            .push(entry);
    }

    // Per query string, if it appears under multiple steps, keep the most frequent.
    let mut by_query: HashMap<String, Vec<QueryVariant>> = HashMap::new();
    for ((query, step), entries) in groups {
        by_query
            .entry(query.clone())
            .or_default()
            .push(((query, step), entries));
    }

    let mut instant_results: Vec<InstantQueryInfo> = Vec::new();
    let mut range_results: Vec<RangeQueryInfo> = Vec::new();

    for (_query_str, mut variants) in by_query {
        // Sort descending by count so [0] is the most frequent variant.
        variants.sort_by(|a, b| b.1.len().cmp(&a.1.len()));

        if variants.len() > 1 {
            tracing::warn!(
                query = %variants[0].0.0,
                kept_step = variants[0].0.1,
                kept_count = variants[0].1.len(),
                "query appears with multiple step values; keeping most-frequent variant"
            );
        }

        let ((query, step), variant_entries) = variants.remove(0);

        if variant_entries.len() < 2 {
            tracing::warn!(%query, "query appears only once in log; skipping");
            continue;
        }

        let repetition_delay = infer_repetition_delay(&variant_entries, scrape_interval, &query);

        if step == 0 {
            instant_results.push(InstantQueryInfo {
                query,
                repetition_delay,
            });
        } else {
            let range_duration = median_range_duration(&variant_entries);
            range_results.push(RangeQueryInfo {
                query,
                repetition_delay,
                step,
                range_duration,
            });
        }
    }

    (instant_results, range_results)
}

/// Compute median inter-arrival time from timestamps and round to nearest scrape interval.
fn infer_repetition_delay(entries: &[&LogEntry], scrape_interval: u64, query: &str) -> u64 {
    let mut timestamps: Vec<DateTime<Utc>> = entries.iter().map(|e| e.ts).collect();
    timestamps.sort();

    let deltas: Vec<f64> = timestamps
        .windows(2)
        .map(|w| (w[1] - w[0]).num_seconds() as f64)
        .collect();

    let raw_median = median_f64(&deltas);
    let rounded = round_to_nearest(raw_median, scrape_interval);

    let misalignment = (raw_median - rounded as f64).abs() / scrape_interval as f64;
    if misalignment >= 0.1 {
        tracing::warn!(
            %query,
            raw_median_secs = raw_median,
            rounded_secs = rounded,
            misalignment_pct = misalignment * 100.0,
            "inferred repetition_delay is poorly aligned with scrape_interval; result may be inaccurate"
        );
    }

    rounded
}

/// Median of (end − start) durations in seconds.
fn median_range_duration(entries: &[&LogEntry]) -> u64 {
    let mut durations: Vec<f64> = entries
        .iter()
        .map(|e| (e.end - e.start).num_seconds() as f64)
        .collect();
    durations.sort_by(|a, b| a.partial_cmp(b).unwrap());
    median_f64(&durations) as u64
}

/// Median of a sorted-or-unsorted slice of f64 values.
fn median_f64(values: &[f64]) -> f64 {
    assert!(!values.is_empty());
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    }
}

/// Round `value` to the nearest multiple of `interval`, minimum 1 interval.
fn round_to_nearest(value: f64, interval: u64) -> u64 {
    let interval_f = interval as f64;
    let rounded = (value / interval_f).round() as u64;
    rounded.max(1) * interval
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_instant_entries(
        base_ts: DateTime<Utc>,
        interval_secs: i64,
        count: u32,
    ) -> Vec<LogEntry> {
        (0..count)
            .map(|i| LogEntry {
                query: "rate(http_requests_total[5m])".to_string(),
                start: base_ts + chrono::Duration::seconds(interval_secs * i as i64),
                end: base_ts + chrono::Duration::seconds(interval_secs * i as i64),
                step: 0,
                ts: base_ts + chrono::Duration::seconds(interval_secs * i as i64),
            })
            .collect()
    }

    fn make_range_entries(
        base_ts: DateTime<Utc>,
        interval_secs: i64,
        range_secs: i64,
        step: u64,
        count: u32,
    ) -> Vec<LogEntry> {
        (0..count)
            .map(|i| {
                let start = base_ts + chrono::Duration::seconds(interval_secs * i as i64);
                LogEntry {
                    query: "rate(http_requests_total[5m])".to_string(),
                    start,
                    end: start + chrono::Duration::seconds(range_secs),
                    step,
                    ts: base_ts + chrono::Duration::seconds(interval_secs * i as i64),
                }
            })
            .collect()
    }

    fn base_ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2025, 12, 2, 18, 0, 0).unwrap()
    }

    #[test]
    fn single_occurrence_skipped() {
        let entries = make_instant_entries(base_ts(), 60, 1);
        let (instants, ranges) = infer_queries(&entries, 15);
        assert!(instants.is_empty());
        assert!(ranges.is_empty());
    }

    #[test]
    fn median_inter_arrival_odd_count() {
        // 5 entries at exactly 60s apart → 4 deltas all 60s → median=60 → rounded=60
        let entries = make_instant_entries(base_ts(), 60, 5);
        let (instants, _) = infer_queries(&entries, 15);
        assert_eq!(instants.len(), 1);
        assert_eq!(instants[0].repetition_delay, 60);
    }

    #[test]
    fn median_inter_arrival_even_count() {
        // 4 entries at 60s apart → 3 deltas all 60s → median=60 → rounded=60
        let entries = make_instant_entries(base_ts(), 60, 4);
        let (instants, _) = infer_queries(&entries, 15);
        assert_eq!(instants.len(), 1);
        assert_eq!(instants[0].repetition_delay, 60);
    }

    #[test]
    fn round_down_to_nearest_scrape() {
        // raw=16s, scrape=15 → nearest=15 (|16-15|/15=6.7% < 10%, no warn)
        assert_eq!(round_to_nearest(16.0, 15), 15);
    }

    #[test]
    fn round_up_to_nearest_scrape() {
        // raw=23s, scrape=15 → nearest=30 (23 is closer to 30 than 15)
        assert_eq!(round_to_nearest(23.0, 15), 30);
    }

    #[test]
    fn misaligned_still_returns_result() {
        // raw=22s, scrape=15 → rounds to 15, but |22-15|/15=46.7% ≥ 10% → warn emitted
        // We only verify the value is returned (warning is logged, not returned)
        assert_eq!(round_to_nearest(22.0, 15), 15);
    }

    #[test]
    fn range_duration_from_start_end() {
        let entries = make_range_entries(base_ts(), 60, 3600, 30, 5);
        let (_, ranges) = infer_queries(&entries, 15);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].range_duration, 3600);
        assert_eq!(ranges[0].step, 30);
    }

    #[test]
    fn same_query_multiple_steps_keeps_most_frequent() {
        let mut entries = make_instant_entries(base_ts(), 60, 3); // step=0, 3 occurrences
        let range_entries = make_range_entries(base_ts(), 60, 3600, 30, 2); // step=30, 2 occurrences
        entries.extend(range_entries);

        let (instants, ranges) = infer_queries(&entries, 15);
        // step=0 variant has more occurrences → kept as instant
        assert_eq!(instants.len(), 1);
        // step=30 variant discarded
        assert!(ranges.is_empty());
    }
}
