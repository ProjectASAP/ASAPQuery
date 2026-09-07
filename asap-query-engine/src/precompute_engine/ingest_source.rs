use crate::drivers::ingest::prometheus_remote_write::DecodedSample;
use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
use crate::precompute_engine::worker::{extract_metric_name, parse_labels_from_series_key};
use arc_swap::ArcSwap;
use asap_types::aggregation_config::AggregationConfig;
use sql_utilities::ast_matching::spatial_filter;
use sql_utilities::ast_matching::spatial_filter::SpatialPredicate;
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

/// Parsed spatial-filter clauses, keyed by the raw filter string. A config's
/// `spatial_filter` text never changes, but `sample_matches_spatial_filter`
/// used to split and parse it from scratch on every (sample, config) check -
/// with many configs sharing one metric (e.g. every plain `bgp_updates`
/// aggregation, differing only by this filter text), that is real repeated
/// work across millions of samples. Cache the parse instead: keyed by the
/// filter string itself (not by aggregation_id, which risks staleness if an
/// id is ever reused for a different filter), so distinct configs that
/// happen to share identical filter text also share one cache entry.
/// Clauses that don't parse are simply absent from the cached list - the
/// permissive "not enforced, sample passes through" behavior for an
/// unrecognized clause is a property of the clause text, not of any one
/// evaluation, so it is safe to bake into the cached result.
fn parsed_spatial_filter_cache() -> &'static Mutex<HashMap<String, Arc<Vec<SpatialPredicate>>>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<Vec<SpatialPredicate>>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn parsed_spatial_filter_clauses(filter: &str) -> Arc<Vec<SpatialPredicate>> {
    if let Some(cached) = parsed_spatial_filter_cache().lock().unwrap().get(filter) {
        return cached.clone();
    }

    let mut parsed = Vec::new();
    for clause in split_spatial_filter_clauses(filter) {
        match spatial_filter::parse_spatial_predicate(&clause) {
            Some(predicate) => parsed.push(predicate),
            None => {
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
            }
        }
    }

    let parsed = Arc::new(parsed);
    parsed_spatial_filter_cache()
        .lock()
        .unwrap()
        .insert(filter.to_string(), parsed.clone());
    parsed
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

/// Indexes `configs` by every one of the four fields a sample's metric name
/// may be matched against (see `route_decoded_samples`), so routing a
/// sample is a hash lookup instead of a linear scan over every registered
/// config. A config whose fields collide on the same key string (common:
/// `metric` and `table_name` are often equal) is inserted once per distinct
/// key, not once per field, so it still appears at most once in any single
/// candidate list - matching the original scan's per-sample semantics
/// exactly.
fn index_agg_configs_by_metric(
    configs: &[Arc<AggregationConfig>],
) -> HashMap<&str, Vec<Arc<AggregationConfig>>> {
    let mut index: HashMap<&str, Vec<Arc<AggregationConfig>>> = HashMap::new();
    for cfg in configs {
        let mut keys: HashSet<&str> = HashSet::new();
        keys.insert(cfg.metric.as_str());
        keys.insert(cfg.spatial_filter_normalized.as_str());
        keys.insert(cfg.spatial_filter.as_str());
        if let Some(table_name) = cfg.table_name.as_deref() {
            keys.insert(table_name);
        }
        for key in keys {
            index.entry(key).or_default().push(cfg.clone());
        }
    }
    index
}

pub(crate) fn extract_group_key(labels: &HashMap<&str, &str>, config: &AggregationConfig) -> String {
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

/// Evaluate the label/timestamp-filter subset emitted by the SQL planner for
/// precompute routing, e.g.:
///
/// collector = 'rrc00' AND operation = 'A' AND startsWith(prefix, '10.')
///
/// Per-clause parsing and evaluation is `sql_utilities::spatial_filter`'s -
/// the single implementation shared with the planner's pre-flight
/// `is_ingest_filter_fully_enforceable` check, so a clause shape one side
/// recognizes and the other doesn't can't happen. If a clause still isn't
/// understood (some shape neither side has been taught yet), preserve the
/// previous permissive behavior for that clause instead of rejecting the
/// sample.
fn sample_matches_spatial_filter(
    metric_name: &str,
    labels: &HashMap<&str, &str>,
    timestamp_ms: i64,
    config: &AggregationConfig,
) -> bool {
    let filter = config.spatial_filter.trim();

    if filter.is_empty() {
        return true;
    }

    // Preserve compatibility with older configs that used spatial_filter as a
    // metric-like matcher rather than a label predicate.
    if filter == metric_name || config.spatial_filter_normalized == metric_name {
        return true;
    }

    for parsed in parsed_spatial_filter_clauses(filter).iter() {
        if !spatial_filter::evaluate_predicate(parsed, labels, timestamp_ms) {
            return false;
        }
    }

    true
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
    // Key: (agg_id, group_key) → Vec<(series_key, timestamp_ms, value, arg_value)>
    type GroupKey = (u64, String);
    type SampleTuple = (String, i64, f64, Option<String>);
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

    // A sample matches a config if its metric name equals any one of the
    // config's four possible metric-identifying fields (see the loop below,
    // pre-index rewrite). Scanning all registered configs per sample was
    // O(samples * configs): fine at a handful of configs, but a real
    // workload can register 1,000+ (one or more per recognized query), and
    // this loop runs per sample - the actual ingest throughput bottleneck
    // at that scale, independent of file-parsing parallelism. Indexing by
    // metric name once per batch turns the per-sample cost into a hash
    // lookup against the small candidate set that could actually match,
    // matching the O(1)-per-field-per-config semantics of the original
    // scan exactly (each config appears at most once per candidate list,
    // even if more than one of its four fields collides on the same key).
    let metric_index = index_agg_configs_by_metric(&agg_configs);

    let mut matched_samples: usize = 0;
    for s in &samples {
        let metric_name = extract_metric_name(&s.labels);
        let Some(candidates) = metric_index.get(metric_name) else {
            continue;
        };
        // Parsed once per sample and shared across every candidate config,
        // rather than once per (sample, candidate) - candidates sharing a
        // metric (e.g. every plain `bgp_updates` aggregation, differing
        // only by spatial_filter) previously each re-parsed the identical
        // label string from scratch.
        let labels = parse_labels_from_series_key(&s.labels);
        for config in candidates {
            if !sample_matches_spatial_filter(metric_name, &labels, s.timestamp_ms, config) {
                continue;
            }

            matched_samples += 1;
            let group_key = extract_group_key(&labels, config);
            by_group
                .entry((config.aggregation_id, group_key))
                .or_default()
                .push((s.labels.clone(), s.timestamp_ms, s.value, s.arg_value.clone()));
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
