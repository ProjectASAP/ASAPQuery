use crate::data_model::{AggregateCore, KeyByLabelValues, PrecomputedOutput};
use crate::precompute_engine::accumulator_factory::{
    create_accumulator_updater, AccumulatorUpdater,
};
use crate::precompute_engine::config::LateDataPolicy;
use crate::precompute_engine::output_sink::OutputSink;
use crate::precompute_engine::series_buffer::SeriesBuffer;
use crate::precompute_engine::series_router::WorkerMessage;
use crate::precompute_engine::window_manager::WindowManager;
use crate::precompute_operators::sum_accumulator::SumAccumulator;
use sketch_db_common::aggregation_config::AggregationConfig;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, debug_span, info, warn};

/// Per-aggregation state within a series: the window manager and active
/// window accumulators.
struct AggregationState {
    config: AggregationConfig,
    window_manager: WindowManager,
    /// Active windows keyed by window_start_ms.
    active_windows: HashMap<i64, Box<dyn AccumulatorUpdater>>,
}

/// Per-series state owned by the worker.
struct SeriesState {
    buffer: SeriesBuffer,
    previous_watermark_ms: i64,
    /// One AggregationState per matching aggregation config.
    aggregations: Vec<AggregationState>,
}

/// Worker that processes samples for a shard of the series space.
pub struct Worker {
    id: usize,
    receiver: mpsc::Receiver<WorkerMessage>,
    output_sink: Arc<dyn OutputSink>,
    /// Map from series key to per-series state.
    series_map: HashMap<String, SeriesState>,
    /// Aggregation configs, keyed by aggregation_id.
    agg_configs: HashMap<u64, AggregationConfig>,
    /// Max buffer size per series.
    max_buffer_per_series: usize,
    /// Allowed lateness in ms.
    allowed_lateness_ms: i64,
    /// When true, skip aggregation and pass raw samples through.
    pass_raw_samples: bool,
    /// Aggregation ID stamped on each raw-mode output.
    raw_mode_aggregation_id: u64,
    /// Policy for handling late samples that arrive after their window has closed.
    late_data_policy: LateDataPolicy,
}

impl Worker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: usize,
        receiver: mpsc::Receiver<WorkerMessage>,
        output_sink: Arc<dyn OutputSink>,
        agg_configs: HashMap<u64, AggregationConfig>,
        max_buffer_per_series: usize,
        allowed_lateness_ms: i64,
        pass_raw_samples: bool,
        raw_mode_aggregation_id: u64,
        late_data_policy: LateDataPolicy,
    ) -> Self {
        Self {
            id,
            receiver,
            output_sink,
            series_map: HashMap::new(),
            agg_configs,
            max_buffer_per_series,
            allowed_lateness_ms,
            pass_raw_samples,
            raw_mode_aggregation_id,
            late_data_policy,
        }
    }

    /// Run the worker loop. Blocks until shutdown.
    pub async fn run(mut self) {
        info!("Worker {} started", self.id);

        while let Some(msg) = self.receiver.recv().await {
            match msg {
                WorkerMessage::Samples {
                    series_key,
                    samples,
                    ingest_received_at,
                } => {
                    let sample_count = samples.len();
                    let _span = debug_span!(
                        "worker_process",
                        worker_id = self.id,
                        series = %series_key,
                        sample_count,
                    )
                    .entered();
                    if let Err(e) = self.process_samples(&series_key, samples) {
                        warn!("Worker {} error processing {}: {}", self.id, series_key, e);
                    }
                    debug!(
                        e2e_latency_us = ingest_received_at.elapsed().as_micros() as u64,
                        "e2e: ingest->worker complete"
                    );
                }
                WorkerMessage::Flush => {
                    if let Err(e) = self.flush_all() {
                        warn!("Worker {} flush error: {}", self.id, e);
                    }
                }
                WorkerMessage::Shutdown => {
                    info!("Worker {} shutting down", self.id);
                    // Final flush before shutdown
                    if let Err(e) = self.flush_all() {
                        warn!("Worker {} final flush error: {}", self.id, e);
                    }
                    break;
                }
            }
        }

        info!(
            "Worker {} stopped, {} active series",
            self.id,
            self.series_map.len()
        );
    }

    /// Find all aggregation configs whose metric/spatial_filter matches this series.
    fn matching_agg_configs(&self, series_key: &str) -> Vec<(u64, &AggregationConfig)> {
        let metric_name = extract_metric_name(series_key);

        self.agg_configs
            .iter()
            .filter(|(_, config)| {
                // Match on metric name
                config.metric == metric_name
                    || config.spatial_filter_normalized == metric_name
                    || config.spatial_filter == metric_name
            })
            .map(|(&id, config)| (id, config))
            .collect()
    }

    /// Get or create the SeriesState for a series key.
    fn get_or_create_series_state(&mut self, series_key: &str) -> &mut SeriesState {
        if !self.series_map.contains_key(series_key) {
            let matching = self.matching_agg_configs(series_key);
            let aggregations = matching
                .into_iter()
                .map(|(_, config)| AggregationState {
                    window_manager: WindowManager::new(config.window_size, config.slide_interval),
                    config: config.clone(),
                    active_windows: HashMap::new(),
                })
                .collect();

            self.series_map.insert(
                series_key.to_string(),
                SeriesState {
                    buffer: SeriesBuffer::new(self.max_buffer_per_series),
                    previous_watermark_ms: i64::MIN,
                    aggregations,
                },
            );
        }

        self.series_map.get_mut(series_key).unwrap()
    }

    fn process_samples(
        &mut self,
        series_key: &str,
        samples: Vec<(i64, f64)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.pass_raw_samples {
            return self.process_samples_raw(series_key, samples);
        }

        // Copy scalars out of self before taking &mut self.series_map
        let worker_id = self.id;
        let allowed_lateness_ms = self.allowed_lateness_ms;
        let late_data_policy = self.late_data_policy;

        // Ensure state exists
        self.get_or_create_series_state(series_key);

        let state = self.series_map.get_mut(series_key).unwrap();

        if state.aggregations.is_empty() {
            return Ok(());
        }

        // Insert samples into buffer, dropping late arrivals
        for &(ts, val) in &samples {
            if state.buffer.watermark_ms() != i64::MIN
                && ts < state.buffer.watermark_ms() - allowed_lateness_ms
            {
                debug!(
                    "Worker {} dropping late sample for {}: ts={} watermark={}",
                    worker_id,
                    series_key,
                    ts,
                    state.buffer.watermark_ms()
                );
                continue;
            }
            state.buffer.insert(ts, val);
        }

        let current_wm = state.buffer.watermark_ms();
        let previous_wm = state.previous_watermark_ms;

        let mut emit_batch: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> = Vec::new();

        for agg_state in &mut state.aggregations {
            let closed = agg_state
                .window_manager
                .closed_windows(previous_wm, current_wm);

            // Feed each incoming sample to the correct active window accumulator
            for &(ts, val) in &samples {
                if current_wm != i64::MIN && ts < current_wm - allowed_lateness_ms {
                    continue; // already dropped
                }

                let window_starts = agg_state.window_manager.window_starts_containing(ts);

                for window_start in window_starts {
                    let window_end = window_start + agg_state.window_manager.window_size_ms();

                    // Check if this window was already closed in a previous batch
                    if !agg_state.active_windows.contains_key(&window_start)
                        && current_wm >= window_end
                    {
                        // Window already closed — handle according to policy
                        match late_data_policy {
                            LateDataPolicy::Drop => {
                                debug!(
                                    "Dropping late sample for closed window [{}, {})",
                                    window_start, window_end
                                );
                                continue;
                            }
                            LateDataPolicy::ForwardToStore => {
                                let mut updater = create_accumulator_updater(&agg_state.config);
                                if updater.is_keyed() {
                                    let key =
                                        extract_key_from_series(series_key, &agg_state.config);
                                    updater.update_keyed(&key, val, ts);
                                } else {
                                    updater.update_single(val, ts);
                                }
                                let key = if updater.is_keyed() {
                                    Some(extract_key_from_series(series_key, &agg_state.config))
                                } else {
                                    None
                                };
                                let output = PrecomputedOutput::new(
                                    window_start as u64,
                                    window_end as u64,
                                    key,
                                    agg_state.config.aggregation_id,
                                );
                                emit_batch.push((output, updater.take_accumulator()));
                                debug!(
                                    "Forwarding late sample to store for closed window [{}, {})",
                                    window_start, window_end
                                );
                                continue;
                            }
                        }
                    }

                    // Normal path: window is still open (or newly opened)
                    let updater = agg_state
                        .active_windows
                        .entry(window_start)
                        .or_insert_with(|| create_accumulator_updater(&agg_state.config));

                    if updater.is_keyed() {
                        let key = extract_key_from_series(series_key, &agg_state.config);
                        updater.update_keyed(&key, val, ts);
                    } else {
                        updater.update_single(val, ts);
                    }
                }
            }

            // Emit closed windows
            for window_start in &closed {
                if let Some(mut updater) = agg_state.active_windows.remove(window_start) {
                    let (_, window_end) = agg_state.window_manager.window_bounds(*window_start);

                    let key = if updater.is_keyed() {
                        Some(extract_key_from_series(series_key, &agg_state.config))
                    } else {
                        None
                    };

                    let output = PrecomputedOutput::new(
                        *window_start as u64,
                        window_end as u64,
                        key,
                        agg_state.config.aggregation_id,
                    );

                    let accumulator = updater.take_accumulator();
                    emit_batch.push((output, accumulator));
                }
            }
        }

        state.previous_watermark_ms = current_wm;

        // Emit to output sink
        if !emit_batch.is_empty() {
            debug!(
                "Worker {} emitting {} outputs for {}",
                worker_id,
                emit_batch.len(),
                series_key
            );
            self.output_sink.emit_batch(emit_batch)?;
        }

        Ok(())
    }

    /// Raw fast-path: emit each sample as a standalone `SumAccumulator`.
    fn process_samples_raw(
        &self,
        series_key: &str,
        samples: Vec<(i64, f64)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut emit_batch: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> =
            Vec::with_capacity(samples.len());

        for (ts, val) in samples {
            let output =
                PrecomputedOutput::new(ts as u64, ts as u64, None, self.raw_mode_aggregation_id);
            let accumulator = SumAccumulator::with_sum(val);
            emit_batch.push((output, Box::new(accumulator)));
        }

        if !emit_batch.is_empty() {
            debug!(
                "Worker {} raw-emitting {} samples for {}",
                self.id,
                emit_batch.len(),
                series_key
            );
            self.output_sink.emit_batch(emit_batch)?;
        }

        Ok(())
    }

    /// Flush all series — force-close windows that are past due.
    fn flush_all(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.pass_raw_samples {
            return Ok(());
        }

        let mut emit_batch: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> = Vec::new();

        for (series_key, state) in &mut self.series_map {
            let current_wm = state.buffer.watermark_ms();
            let previous_wm = state.previous_watermark_ms;

            for agg_state in &mut state.aggregations {
                let closed = agg_state
                    .window_manager
                    .closed_windows(previous_wm, current_wm);

                for window_start in &closed {
                    if let Some(mut updater) = agg_state.active_windows.remove(window_start) {
                        let (_, window_end) = agg_state.window_manager.window_bounds(*window_start);

                        let key = if updater.is_keyed() {
                            Some(extract_key_from_series(series_key, &agg_state.config))
                        } else {
                            None
                        };

                        let output = PrecomputedOutput::new(
                            *window_start as u64,
                            window_end as u64,
                            key,
                            agg_state.config.aggregation_id,
                        );

                        let accumulator = updater.take_accumulator();
                        emit_batch.push((output, accumulator));
                    }
                }
            }

            state.previous_watermark_ms = current_wm;
        }

        if !emit_batch.is_empty() {
            debug!(
                "Worker {} flush emitting {} outputs",
                self.id,
                emit_batch.len()
            );
            self.output_sink.emit_batch(emit_batch)?;
        }

        Ok(())
    }
}

/// Extract the metric name from a series key like `"metric_name{key1=\"val1\"}"`.
pub fn extract_metric_name(series_key: &str) -> &str {
    match series_key.find('{') {
        Some(pos) => &series_key[..pos],
        None => series_key,
    }
}

/// Extract grouping label values from a series key string based on the
/// aggregation config's `grouping_labels`.
///
/// The series key format is: `metric_name{label1="val1",label2="val2",...}`
pub fn extract_key_from_series(series_key: &str, config: &AggregationConfig) -> KeyByLabelValues {
    let labels = parse_labels_from_series_key(series_key);
    let mut values = Vec::new();

    for label_name in &config.grouping_labels.labels {
        if let Some(val) = labels.get(label_name.as_str()) {
            values.push(val.to_string());
        } else {
            values.push(String::new());
        }
    }

    KeyByLabelValues::new_with_labels(values)
}

/// Parse label key-value pairs from a series key string.
/// `"metric{a=\"b\",c=\"d\"}"` → `{("a", "b"), ("c", "d")}`
fn parse_labels_from_series_key(series_key: &str) -> HashMap<&str, &str> {
    let mut labels = HashMap::new();

    let start = match series_key.find('{') {
        Some(pos) => pos + 1,
        None => return labels,
    };
    let end = match series_key.rfind('}') {
        Some(pos) => pos,
        None => return labels,
    };

    if start >= end {
        return labels;
    }

    let label_str = &series_key[start..end];

    // Parse comma-separated key="value" pairs
    // Simple parser that handles the expected format
    let mut remaining = label_str;
    while !remaining.is_empty() {
        // Find the '=' separator
        let eq_pos = match remaining.find('=') {
            Some(pos) => pos,
            None => break,
        };
        let key = remaining[..eq_pos].trim();

        // Expect "value" after =
        let after_eq = &remaining[eq_pos + 1..];
        if !after_eq.starts_with('"') {
            break;
        }

        // Find closing quote
        let value_start = 1; // skip opening quote
        let value_end = match after_eq[value_start..].find('"') {
            Some(pos) => value_start + pos,
            None => break,
        };

        let value = &after_eq[value_start..value_end];
        labels.insert(key, value);

        // Move past the closing quote and optional comma
        let consumed = value_end + 1; // past closing quote
        remaining = &after_eq[consumed..];
        if remaining.starts_with(',') {
            remaining = &remaining[1..];
        }
    }

    labels
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_metric_name() {
        assert_eq!(
            extract_metric_name("http_requests_total{method=\"GET\"}"),
            "http_requests_total"
        );
        assert_eq!(extract_metric_name("up"), "up");
        assert_eq!(
            extract_metric_name("cpu_usage{host=\"a\",zone=\"us\"}"),
            "cpu_usage"
        );
    }

    #[test]
    fn test_parse_labels() {
        let labels = parse_labels_from_series_key("metric{method=\"GET\",status=\"200\"}");
        assert_eq!(labels.get("method"), Some(&"GET"));
        assert_eq!(labels.get("status"), Some(&"200"));
    }

    #[test]
    fn test_parse_labels_no_labels() {
        let labels = parse_labels_from_series_key("metric");
        assert!(labels.is_empty());
    }

    #[test]
    fn test_parse_labels_empty_braces() {
        let labels = parse_labels_from_series_key("metric{}");
        assert!(labels.is_empty());
    }

    #[test]
    fn test_extract_key_from_series() {
        let config = AggregationConfig::new(
            1,
            "SingleSubpopulation".to_string(),
            "Sum".to_string(),
            HashMap::new(),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![
                "method".to_string(),
                "status".to_string(),
            ]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            60,
            30,
            "tumbling".to_string(),
            "http_requests_total".to_string(),
            "http_requests_total".to_string(),
            Some(60),
            Some(0),
            None,
            None,
        );

        let key = extract_key_from_series(
            "http_requests_total{method=\"GET\",status=\"200\"}",
            &config,
        );
        assert_eq!(key.labels, vec!["GET".to_string(), "200".to_string()]);
    }
}
