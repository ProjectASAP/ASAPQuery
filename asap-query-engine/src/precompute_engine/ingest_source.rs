use crate::drivers::ingest::prometheus_remote_write::DecodedSample;
use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
use crate::precompute_engine::worker::{extract_metric_name, parse_labels_from_series_key};
use arc_swap::ArcSwap;
use asap_types::aggregation_config::AggregationConfig;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, warn};

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
/// Implementors decode incoming data (HTTP, file, etc.) and push it
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
