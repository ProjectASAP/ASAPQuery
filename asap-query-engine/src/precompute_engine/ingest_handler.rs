use crate::drivers::ingest::prometheus_remote_write::decode_prometheus_remote_write;
use crate::drivers::ingest::victoriametrics_remote_write::decode_victoriametrics_remote_write;
use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
use crate::precompute_engine::worker::{extract_metric_name, parse_labels_from_series_key};
use asap_types::aggregation_config::AggregationConfig;
use axum::{body::Bytes, extract::State, http::StatusCode};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::warn;

/// Shared state for the ingest HTTP handler.
pub(crate) struct IngestState {
    pub(crate) router: SeriesRouter,
    pub(crate) samples_ingested: std::sync::atomic::AtomicU64,
    /// Aggregation configs for group-key extraction.
    pub(crate) agg_configs: Vec<Arc<AggregationConfig>>,
    /// When true, skip group-key extraction and pass raw samples through.
    pub(crate) pass_raw_samples: bool,
}

/// Extract the group key (grouping label values joined by semicolons)
/// for a given series key and aggregation config.
fn extract_group_key(series_key: &str, config: &AggregationConfig) -> String {
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

/// Shared logic: group decoded samples by (agg_id, group_key) and route to workers.
async fn route_decoded_samples(
    state: &IngestState,
    samples: Vec<crate::drivers::ingest::prometheus_remote_write::DecodedSample>,
    ingest_received_at: Instant,
) -> StatusCode {
    if samples.is_empty() {
        return StatusCode::NO_CONTENT;
    }

    let count = samples.len() as u64;
    state
        .samples_ingested
        .fetch_add(count, std::sync::atomic::Ordering::Relaxed);

    if state.pass_raw_samples {
        // Raw mode: group by series key and send as RawSamples
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

        if let Err(e) = state
            .router
            .route_group_batch(messages, ingest_received_at)
            .await
        {
            warn!("Batch routing error: {}", e);
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
        return StatusCode::NO_CONTENT;
    }

    // Group-by mode: for each sample, find matching agg configs and group by
    // (agg_id, group_key). This is the equivalent of Arroyo's GROUP BY.
    //
    // Key: (agg_id, group_key) → Vec<(series_key, timestamp_ms, value)>
    type GroupKey = (u64, String);
    type SampleTuple = (String, i64, f64);
    let mut by_group: HashMap<GroupKey, Vec<SampleTuple>> = HashMap::new();

    for s in &samples {
        let metric_name = extract_metric_name(&s.labels);
        for config in &state.agg_configs {
            if config.metric != metric_name
                && config.spatial_filter_normalized != metric_name
                && config.spatial_filter != metric_name
            {
                continue;
            }
            let group_key = extract_group_key(&s.labels, config);
            by_group
                .entry((config.aggregation_id, group_key))
                .or_default()
                .push((s.labels.clone(), s.timestamp_ms, s.value));
        }
    }

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

    if let Err(e) = state
        .router
        .route_group_batch(messages, ingest_received_at)
        .await
    {
        warn!("Batch routing error: {}", e);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    StatusCode::NO_CONTENT
}

/// Axum handler for Prometheus remote write (Snappy + Protobuf).
pub(crate) async fn handle_prometheus_ingest(
    State(state): State<Arc<IngestState>>,
    body: Bytes,
) -> StatusCode {
    let ingest_received_at = Instant::now();
    let samples = match decode_prometheus_remote_write(&body) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to decode Prometheus remote write: {}", e);
            return StatusCode::BAD_REQUEST;
        }
    };
    route_decoded_samples(&state, samples, ingest_received_at).await
}

/// Axum handler for VictoriaMetrics remote write (Zstd + Protobuf).
pub(crate) async fn handle_victoriametrics_ingest(
    State(state): State<Arc<IngestState>>,
    body: Bytes,
) -> StatusCode {
    let ingest_received_at = Instant::now();
    let samples = match decode_victoriametrics_remote_write(&body) {
        Ok(s) => s,
        Err(e) => {
            warn!("Failed to decode VictoriaMetrics remote write: {}", e);
            return StatusCode::BAD_REQUEST;
        }
    };
    route_decoded_samples(&state, samples, ingest_received_at).await
}
