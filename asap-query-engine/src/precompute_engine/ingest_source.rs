use crate::drivers::ingest::prometheus_remote_write::DecodedSample;
use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
use crate::precompute_engine::worker::{extract_metric_name, parse_labels_from_series_key};
use arc_swap::ArcSwap;
use asap_types::aggregation_config::AggregationConfig;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use tracing::{debug, warn};

/// Distinct unrecognized spatial-filter clauses already warned about. This
/// check runs per (sample, config) in the ingest hot path, so warning
/// unconditionally would mean one log line per matching CSV row - up to
/// millions of times for one unrecognized clause. Warn once per distinct
/// clause per process instead.
fn warned_unsupported_clauses() -> &'static Mutex<HashSet<String>> {
    static CACHE: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Everything a source needs to push decoded samples into the worker pool.
#[derive(Clone)]
pub struct IngestContext {
    pub(crate) router: SeriesRouter,
    /// Aggregation configs for group-key extraction.
    /// Wrapped in Arc so the same ArcSwap is shared with PrecomputeEngineHandle.
    /// The handle calls ArcSwap::store() to push a new Vec; this context sees it
    /// immediately via the shared Arc pointer (lock-free on the read path).
    pub(crate) agg_configs: Arc<ArcSwap<Vec<Arc<AggregationConfig>>>>,
    /// When true, skip group-key extraction and pass raw samples through.
    pub(crate) pass_raw_samples: bool,
}

/// An ingest source for the precompute engine.
///
/// Implementors decode incoming data (HTTP, Kafka, file, etc.) and push it
/// into the engine via [`route_decoded_samples`].
#[async_trait::async_trait]
pub trait IngestSource: Send + Sync {
    async fn run(
        self: Box<Self>,
        ctx: IngestContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

pub(crate) fn extract_group_key(series_key: &str, config: &AggregationConfig) -> String {
    let labels = parse_labels_from_series_key(series_key);
    let mut values = Vec::new();
    for label_name in &config.grouping_labels.labels {
        if let Some(val) = labels.get(label_name.as_str()) {
            values.push(*val);
        } else {
            values.push("");
        }
    }
    values.join(";")
}

/// Evaluate the simple label-filter subset emitted by the SQL planner for
/// precompute routing, e.g.:
///
/// collector = 'rrc00' AND operation = 'A'
///
/// This intentionally supports conjunctions of equality predicates. If a
/// clause is not understood, preserve the previous permissive behavior for
/// that clause instead of rejecting the sample.
fn sample_matches_spatial_filter(series_key: &str, config: &AggregationConfig) -> bool {
    let filter = config.spatial_filter.trim();

    if filter.is_empty() {
        return true;
    }

    let metric_name = extract_metric_name(series_key);

    // Preserve compatibility with older configs that used spatial_filter as a
    // metric-like matcher rather than a label predicate.
    if filter == metric_name || config.spatial_filter_normalized == metric_name {
        return true;
    }

    let labels = parse_labels_from_series_key(series_key);

    for clause in split_spatial_filter_clauses(filter) {
        let Some(parsed) = parse_spatial_clause(&clause) else {
            // Preserve the previous permissive behavior for clause shapes we
            // genuinely don't recognize, but make it loud: a silent `debug!`
            // here is indistinguishable from "this filter is enforced" in
            // normal operation, and has already let an unsupported `IN (...)`
            // clause through unfiltered in practice.
            let is_new = warned_unsupported_clauses()
                .lock()
                .unwrap()
                .insert(clause.clone());
            if is_new {
                warn!(
                    "Ignoring unsupported spatial filter clause during ingest routing \
                     (samples that should be filtered by it may pass through unfiltered); \
                     further occurrences of this exact clause are suppressed: {}",
                    clause
                );
            }
            continue;
        };

        let matches = match &parsed {
            SpatialClause::Eq(label, expected) => {
                matches!(labels.get(label.as_str()), Some(actual) if *actual == expected.as_str())
            }
            SpatialClause::Ne(label, excluded) => {
                !matches!(labels.get(label.as_str()), Some(actual) if *actual == excluded.as_str())
            }
            SpatialClause::In(label, allowed) => match labels.get(label.as_str()) {
                Some(actual) => allowed.iter().any(|v| v == actual),
                None => false,
            },
        };

        if !matches {
            return false;
        }
    }

    true
}

enum SpatialClause {
    Eq(String, String),
    Ne(String, String),
    In(String, Vec<String>),
}

/// Splits a spatial-filter string on top-level `AND`/`,` separators, without
/// splitting on separators that appear inside a single-quoted literal (e.g.
/// `operation = 'read,write'` must stay one clause, not two) or inside
/// parentheses (e.g. `peer_asn IN ('174', '3356')` has commas between the
/// quoted values that are themselves outside any quotes, but they belong to
/// the IN-list, not to the top-level clause separator).
fn split_spatial_filter_clauses(filter: &str) -> Vec<String> {
    let cleaned = filter
        .trim()
        .trim_start_matches('{')
        .trim_end_matches('}')
        .trim();

    let mut clauses = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut paren_depth: i32 = 0;
    let chars: Vec<char> = cleaned.chars().collect();
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
                paren_depth += 1;
                current.push(c);
                i += 1;
                continue;
            }
            if c == ')' {
                paren_depth = (paren_depth - 1).max(0);
                current.push(c);
                i += 1;
                continue;
            }

            if paren_depth == 0 {
                if c == ',' {
                    clauses.push(current.trim().to_string());
                    current.clear();
                    i += 1;
                    continue;
                }

                // Match " AND " / " and " as a whole-word separator.
                let rest: String = chars[i..].iter().collect();
                let rest_upper = rest.to_uppercase();
                if rest_upper.starts_with(" AND ") {
                    clauses.push(current.trim().to_string());
                    current.clear();
                    i += 5;
                    continue;
                }
            }
        }

        current.push(c);
        i += 1;
    }

    clauses.push(current.trim().to_string());
    clauses.into_iter().filter(|c| !c.is_empty()).collect()
}

fn strip_quotes(s: &str) -> String {
    s.trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
        .to_string()
}

fn parse_spatial_clause(clause: &str) -> Option<SpatialClause> {
    if clause.contains("=~") || clause.contains("!~") {
        return None;
    }

    let upper = clause.to_uppercase();
    if let Some(in_idx) = upper.find(" IN ") {
        let label = strip_quotes(&clause[..in_idx]);
        if label.is_empty() {
            return None;
        }
        let rest = clause[in_idx + 4..].trim();
        let inner = rest.strip_prefix('(')?.trim_end().strip_suffix(')')?;
        let values: Vec<String> = inner
            .split(',')
            .map(|v| strip_quotes(v))
            .filter(|v| !v.is_empty())
            .collect();
        if values.is_empty() {
            return None;
        }
        return Some(SpatialClause::In(label, values));
    }

    // SQLPatternParser's extracted spatial_filter strings come from
    // sqlparser's own Display impl, which renders a parsed `!=` back out as
    // `<>` - so a query the analyst wrote with `!=` shows up here as `<>`.
    // Recognize both spellings of the same operator.
    if let Some((lhs, rhs)) = clause.split_once("!=").or_else(|| clause.split_once("<>")) {
        let label = strip_quotes(lhs);
        if label.is_empty() {
            return None;
        }
        return Some(SpatialClause::Ne(label, strip_quotes(rhs)));
    }

    let (lhs, rhs) = clause.split_once('=')?;
    let label = strip_quotes(lhs);
    if label.is_empty() {
        return None;
    }
    Some(SpatialClause::Eq(label, strip_quotes(rhs)))
}

/// Group decoded samples by (agg_id, group_key) and route them to workers.
///
/// Returns an error if the router fails to deliver any message.
pub(crate) async fn route_decoded_samples(
    ctx: &IngestContext,
    samples: Vec<DecodedSample>,
    ingest_received_at: Instant,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if samples.is_empty() {
        return Ok(());
    }

    if ctx.pass_raw_samples {
        let mut by_series: HashMap<&str, Vec<(i64, f64)>> = HashMap::new();
        for s in &samples {
            by_series
                .entry(&s.labels)
                .or_default()
                .push((s.timestamp_ms, s.value));
        }
        let messages: Vec<WorkerMessage> = by_series
            .into_iter()
            .map(|(k, v)| WorkerMessage::RawSamples {
                series_key: k.to_string(),
                samples: v,
                ingest_received_at,
            })
            .collect();
        ctx.router
            .route_group_batch(messages, ingest_received_at)
            .await?;
        return Ok(());
    }

    // Group-by mode: for each sample, find matching agg configs and group by
    // (agg_id, group_key). This is the equivalent of Arroyo's GROUP BY.
    //
    // Key: (agg_id, group_key) → Vec<(series_key, timestamp_ms, value)>
    type GroupKey = (u64, String);
    type SampleTuple = (String, i64, f64);
    let mut by_group: HashMap<GroupKey, Vec<SampleTuple>> = HashMap::new();

    // Load agg_configs once per request (lock-free ArcSwap read).
    let agg_configs = ctx.agg_configs.load();

    // On first batch: log config metrics vs sample metric to diagnose mismatches.
    static FIRST_BATCH_LOGGED: AtomicBool = AtomicBool::new(false);
    if !FIRST_BATCH_LOGGED.swap(true, Ordering::Relaxed) {
        if let Some(first) = samples.first() {
            let sample_metric = extract_metric_name(&first.labels);
            warn!(
                sample_metric,
                sample_labels = %first.labels,
                num_agg_configs = agg_configs.len(),
                "routing: first batch diagnostic"
            );
            for cfg in agg_configs.iter() {
                warn!(
                    agg_id = cfg.aggregation_id,
                    config_metric = %cfg.metric,
                    config_spatial_filter = %cfg.spatial_filter,
                    table_name = ?cfg.table_name,
                    "routing: agg config metric"
                );
            }
        }
    }

    let mut matched_samples: usize = 0;
    for s in &samples {
        let metric_name = extract_metric_name(&s.labels);
        for config in agg_configs.iter() {
            if config.metric != metric_name
                && config.spatial_filter_normalized != metric_name
                && config.spatial_filter != metric_name
                && config.table_name.as_deref() != Some(metric_name)
            {
                continue;
            }
            if !sample_matches_spatial_filter(&s.labels, config) {
                continue;
            }

            matched_samples += 1;
            let group_key = extract_group_key(&s.labels, config);
            by_group
                .entry((config.aggregation_id, group_key))
                .or_default()
                .push((s.labels.clone(), s.timestamp_ms, s.value));
        }
    }

    debug!(
        total_samples = samples.len(),
        matched_samples,
        groups_formed = by_group.len(),
        "routing: batch match summary"
    );

    let messages: Vec<WorkerMessage> = by_group
        .into_iter()
        .map(
            |((agg_id, group_key), samples)| WorkerMessage::GroupSamples {
                agg_id,
                group_key,
                samples,
                ingest_received_at,
            },
        )
        .collect();

    ctx.router
        .route_group_batch(messages, ingest_received_at)
        .await?;
    Ok(())
}
