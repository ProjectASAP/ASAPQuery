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
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, debug_span, info, warn};

/// Per-aggregation state within a series: the window manager and active
/// pane accumulators.
///
/// Uses pane-based sliding window computation: each sample is routed to
/// exactly 1 pane (sub-window of size `slide_interval`). When a window
/// closes, its constituent panes are merged. This reduces per-sample
/// accumulator updates from W to 1 (where W = window_size / slide_interval).
struct AggregationState {
    config: AggregationConfig,
    window_manager: WindowManager,
    /// Active panes keyed by pane_start_ms.
    /// BTreeMap for ordered iteration (needed for pane eviction).
    active_panes: BTreeMap<i64, Box<dyn AccumulatorUpdater>>,
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
                    active_panes: BTreeMap::new(),
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

            // Pane-based sample routing: each sample goes to exactly 1 pane
            for &(ts, val) in &samples {
                if current_wm != i64::MIN && ts < current_wm - allowed_lateness_ms {
                    continue; // already dropped
                }

                let pane_start = agg_state.window_manager.pane_start_for(ts);
                let pane_end = pane_start + agg_state.window_manager.slide_interval_ms();

                // Check if pane was already evicted (late data for a closed window).
                // A pane is evicted when its oldest window closes, i.e. the window
                // starting at pane_start. If that window is closed, the pane is gone.
                if !agg_state.active_panes.contains_key(&pane_start)
                    && current_wm >= pane_start + agg_state.window_manager.window_size_ms()
                {
                    // The window starting at this pane_start is already closed,
                    // so this pane was evicted — handle as late data.
                    let window_start = pane_start;
                    let window_end = pane_start + agg_state.window_manager.window_size_ms();
                    match late_data_policy {
                        LateDataPolicy::Drop => {
                            debug!(
                                "Dropping late sample for evicted pane [{}, {})",
                                pane_start, pane_end
                            );
                            continue;
                        }
                        LateDataPolicy::ForwardToStore => {
                            let mut updater = create_accumulator_updater(&agg_state.config);
                            if updater.is_keyed() {
                                let key = extract_key_from_series(series_key, &agg_state.config);
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
                                "Forwarding late sample to store for evicted pane [{}, {})",
                                pane_start, pane_end
                            );
                            continue;
                        }
                    }
                }

                // Normal path: route sample to its single pane
                let updater = agg_state
                    .active_panes
                    .entry(pane_start)
                    .or_insert_with(|| create_accumulator_updater(&agg_state.config));

                if updater.is_keyed() {
                    let key = extract_key_from_series(series_key, &agg_state.config);
                    updater.update_keyed(&key, val, ts);
                } else {
                    updater.update_single(val, ts);
                }
            }

            // Emit closed windows by merging their constituent panes
            for window_start in &closed {
                let (_, window_end) = agg_state.window_manager.window_bounds(*window_start);
                let pane_starts = agg_state.window_manager.panes_for_window(*window_start);

                // Merge pane accumulators for this window.
                // - Oldest pane (index 0): take_accumulator + remove (no future window needs it)
                // - Remaining panes: snapshot_accumulator (shared with newer windows)
                let mut merged: Option<Box<dyn AggregateCore>> = None;

                for (i, &ps) in pane_starts.iter().enumerate() {
                    let pane_acc = if i == 0 {
                        // Oldest pane: destructive take + evict from active_panes
                        agg_state
                            .active_panes
                            .remove(&ps)
                            .map(|mut updater| updater.take_accumulator())
                    } else {
                        // Shared pane: non-destructive snapshot
                        agg_state
                            .active_panes
                            .get(&ps)
                            .map(|updater| updater.snapshot_accumulator())
                    };

                    if let Some(acc) = pane_acc {
                        merged = Some(match merged {
                            None => acc,
                            Some(existing) => existing.merge_with(acc.as_ref()).unwrap_or(existing),
                        });
                    }
                }

                if let Some(accumulator) = merged {
                    let key = {
                        // Check keyed-ness from accumulator type name or config
                        let test_updater = create_accumulator_updater(&agg_state.config);
                        if test_updater.is_keyed() {
                            Some(extract_key_from_series(series_key, &agg_state.config))
                        } else {
                            None
                        }
                    };

                    let output = PrecomputedOutput::new(
                        *window_start as u64,
                        window_end as u64,
                        key,
                        agg_state.config.aggregation_id,
                    );

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
                    let (_, window_end) = agg_state.window_manager.window_bounds(*window_start);
                    let pane_starts = agg_state.window_manager.panes_for_window(*window_start);

                    let mut merged: Option<Box<dyn AggregateCore>> = None;

                    for (i, &ps) in pane_starts.iter().enumerate() {
                        let pane_acc = if i == 0 {
                            agg_state
                                .active_panes
                                .remove(&ps)
                                .map(|mut updater| updater.take_accumulator())
                        } else {
                            agg_state
                                .active_panes
                                .get(&ps)
                                .map(|updater| updater.snapshot_accumulator())
                        };

                        if let Some(acc) = pane_acc {
                            merged = Some(match merged {
                                None => acc,
                                Some(existing) => {
                                    existing.merge_with(acc.as_ref()).unwrap_or(existing)
                                }
                            });
                        }
                    }

                    if let Some(accumulator) = merged {
                        let key = {
                            let test_updater = create_accumulator_updater(&agg_state.config);
                            if test_updater.is_keyed() {
                                Some(extract_key_from_series(series_key, &agg_state.config))
                            } else {
                                None
                            }
                        };

                        let output = PrecomputedOutput::new(
                            *window_start as u64,
                            window_end as u64,
                            key,
                            agg_state.config.aggregation_id,
                        );

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

    use flate2::{write::GzEncoder, Compression};
    use serde_json::json;
    use std::io::Write;


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

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    use crate::data_model::StreamingConfig;
    use crate::precompute_engine::config::LateDataPolicy;
    use crate::precompute_engine::output_sink::CapturingOutputSink;
    use crate::precompute_operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator;
    use crate::precompute_operators::multiple_sum_accumulator::MultipleSumAccumulator;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use sketch_core::kll::KllSketch;

    fn make_agg_config(
        id: u64,
        metric: &str,
        agg_type: &str,
        agg_sub_type: &str,
        window_secs: u64,
        slide_secs: u64,
        grouping: Vec<&str>,
    ) -> AggregationConfig {
        let window_type = if slide_secs == 0 || slide_secs == window_secs {
            "tumbling"
        } else {
            "sliding"
        };
        AggregationConfig::new(
            id,
            agg_type.to_string(),
            agg_sub_type.to_string(),
            HashMap::new(),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(
                grouping.iter().map(|s| s.to_string()).collect(),
            ),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            window_secs,
            slide_secs,
            window_type.to_string(),
            metric.to_string(),
            metric.to_string(),
            None,
            None,
            None,
            None,
        )
    }

    fn make_worker(
        agg_configs: HashMap<u64, AggregationConfig>,
        sink: Arc<CapturingOutputSink>,
        pass_raw: bool,
        raw_agg_id: u64,
        late_policy: LateDataPolicy,
    ) -> Worker {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Worker::new(
            0,
            rx,
            sink,
            agg_configs,
            10_000,  // max_buffer_per_series
            0,       // allowed_lateness_ms
            pass_raw,
            raw_agg_id,
            late_policy,
        )
    }

    // -----------------------------------------------------------------------
    // Test: raw mode — each sample forwarded as SumAccumulator with sum==value
    // -----------------------------------------------------------------------

    #[test]
    fn test_raw_mode_forwarding() {
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(HashMap::new(), sink.clone(), true, 99, LateDataPolicy::Drop);

        let samples = vec![(1000_i64, 1.5_f64), (2000, 2.5), (3000, 7.0)];
        worker
            .process_samples("cpu{host=\"a\"}", samples.clone())
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 3, "should emit one output per raw sample");

        for ((ts, val), (output, acc)) in samples.iter().zip(captured.iter()) {
            assert_eq!(output.start_timestamp as i64, *ts, "start should equal ts");
            assert_eq!(output.end_timestamp as i64, *ts, "end should equal ts");
            assert_eq!(output.aggregation_id, 99);
            let sum_acc = acc
                .as_any()
                .downcast_ref::<SumAccumulator>()
                .expect("should be SumAccumulator");
            assert!(
                (sum_acc.sum - val).abs() < 1e-10,
                "sum should equal sample value"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test: tumbling window — correct window boundaries and sum
    // -----------------------------------------------------------------------

    #[test]
    fn test_tumbling_window_correctness() {
        // 10s tumbling window
        let config = make_agg_config(1, "cpu", "SingleSubpopulation", "Sum", 10, 0, vec![]);
        let mut agg_configs = HashMap::new();
        agg_configs.insert(1, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // Samples in window [0, 10000ms): sum should be 1+2+3=6.
        // Send one at a time so the watermark advances incrementally —
        // a batch's max-ts becomes the new watermark, and with
        // allowed_lateness_ms=0 any ts < watermark in the same call is dropped.
        worker
            .process_samples("cpu", vec![(1000_i64, 1.0)])
            .unwrap();
        worker
            .process_samples("cpu", vec![(5000_i64, 2.0)])
            .unwrap();
        worker
            .process_samples("cpu", vec![(9000_i64, 3.0)])
            .unwrap();
        // No windows closed yet (watermark still below 10000)
        assert_eq!(sink.len(), 0);

        // Sample at t=10000ms advances watermark to 10000, closing [0, 10000)
        worker
            .process_samples("cpu", vec![(10000_i64, 100.0)])
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "exactly one window should close");

        let (output, acc) = &captured[0];
        assert_eq!(output.aggregation_id, 1);
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 10_000);
        assert!(
            output.key.is_none(),
            "SingleSubpopulation should have no key"
        );

        let sum_acc = acc
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("should be SumAccumulator");
        assert!(
            (sum_acc.sum - 6.0).abs() < 1e-10,
            "sum should be 1+2+3=6, got {}",
            sum_acc.sum
        );
    }

    // -----------------------------------------------------------------------
    // Test: sliding window pane sharing — one sample, two window emits, same sum
    // -----------------------------------------------------------------------

    #[test]
    fn test_sliding_window_pane_sharing() {
        // 30s window, 10s slide → W=3 panes per window
        let config = make_agg_config(2, "cpu", "SingleSubpopulation", "Sum", 30, 10, vec![]);
        let mut agg_configs = HashMap::new();
        agg_configs.insert(2, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // Sample at t=15000ms → goes to pane 10000ms
        // previous_wm == i64::MIN → no windows close
        worker
            .process_samples("cpu", vec![(15_000_i64, 42.0)])
            .unwrap();
        assert_eq!(sink.len(), 0);

        // Sample at t=45000ms → advances watermark to 45000ms
        // Closes windows [0, 30000) and [10000, 40000)
        worker
            .process_samples("cpu", vec![(45_000_i64, 0.0)])
            .unwrap();

        let captured = sink.drain();
        // Both windows should emit — one from pane merge snapshot, one from take
        // Window [0, 30000): panes [0, 10000, 20000]; pane 10000 snapshot → sum=42
        // Window [10000, 40000): panes [10000, 20000, 30000]; pane 10000 take → sum=42
        assert_eq!(
            captured.len(),
            2,
            "two windows containing the pane should emit"
        );

        let window_starts: Vec<u64> = captured.iter().map(|(o, _)| o.start_timestamp).collect();
        assert!(window_starts.contains(&0), "window [0, 30000) should emit");
        assert!(
            window_starts.contains(&10_000),
            "window [10000, 40000) should emit"
        );

        for (output, acc) in &captured {
            let sum_acc = acc
                .as_any()
                .downcast_ref::<SumAccumulator>()
                .expect("should be SumAccumulator");
            assert!(
                (sum_acc.sum - 42.0).abs() < 1e-10,
                "window {:?} should have sum=42 via pane sharing, got {}",
                output.start_timestamp,
                sum_acc.sum
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test: GROUP BY — two series on same worker produce separate accumulators
    // -----------------------------------------------------------------------

    #[test]
    fn test_groupby_separate_emits_per_series() {
        // MultipleSubpopulation Sum with grouping on "host"
        // Two series on same worker → same window accumulator per-agg holds both keys
        let config = make_agg_config(
            3,
            "cpu",
            "MultipleSubpopulation",
            "Sum",
            10,
            0,
            vec!["host"],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(3, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // Feed two series in the same window [0, 10000ms)
        worker
            .process_samples("cpu{host=\"A\"}", vec![(1000_i64, 10.0)])
            .unwrap();
        worker
            .process_samples("cpu{host=\"B\"}", vec![(2000_i64, 20.0)])
            .unwrap();
        assert_eq!(sink.len(), 0, "no windows closed yet");

        // Advance watermark to close [0, 10000) for series "A"
        worker
            .process_samples("cpu{host=\"A\"}", vec![(10_000_i64, 0.0)])
            .unwrap();
        // Also advance "B"'s watermark
        worker
            .process_samples("cpu{host=\"B\"}", vec![(10_000_i64, 0.0)])
            .unwrap();

        let captured = sink.drain();
        // Each series has its own SeriesState and independent pane accumulators.
        // The MultipleSubpopulation accumulator for each series records its own key.
        // So we get 2 emits (one per series), each a MultipleSumAccumulator with a single key.
        assert_eq!(
            captured.len(),
            2,
            "each series emits independently — no ingest-time merge"
        );

        // Verify the grouping keys are distinct
        let mut found_a = false;
        let mut found_b = false;
        for (output, acc) in &captured {
            assert_eq!(output.start_timestamp, 0);
            assert_eq!(output.end_timestamp, 10_000);
            let ms_acc = acc
                .as_any()
                .downcast_ref::<MultipleSumAccumulator>()
                .expect("should be MultipleSumAccumulator");
            for (key, &sum) in &ms_acc.sums {
                if key.labels == vec!["A".to_string()] {
                    assert!((sum - 10.0).abs() < 1e-10);
                    found_a = true;
                }
                if key.labels == vec!["B".to_string()] {
                    assert!((sum - 20.0).abs() < 1e-10);
                    found_b = true;
                }
            }
        }
        assert!(found_a, "expected emit for host=A");
        assert!(found_b, "expected emit for host=B");
    }

    #[test]
    fn test_arroyosketch_multiple_sum_matches_handcrafted_precompute_output() {
        let config = make_agg_config(11, "cpu", "MultipleSum", "sum", 10, 0, vec!["host"]);
        let mut agg_configs = HashMap::new();
        agg_configs.insert(11, config.clone());

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            agg_configs.clone(),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
        );

        worker
            .process_samples("cpu{host=\"A\"}", vec![(1_000_i64, 1.0)])
            .unwrap();
        worker
            .process_samples("cpu{host=\"A\"}", vec![(5_000_i64, 2.0)])
            .unwrap();
        worker
            .process_samples("cpu{host=\"A\"}", vec![(9_000_i64, 3.0)])
            .unwrap();
        worker
            .process_samples("cpu{host=\"A\"}", vec![(10_000_i64, 0.0)])
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "expected one closed window output");

        let (handcrafted_output, handcrafted_acc) = &captured[0];
        let handcrafted_acc = handcrafted_acc
            .as_any()
            .downcast_ref::<MultipleSumAccumulator>()
            .expect("hand-crafted engine should emit MultipleSumAccumulator");

        let mut arroyo_sums = HashMap::new();
        arroyo_sums.insert("A".to_string(), 6.0);
        let arroyo_precompute_bytes =
            rmp_serde::to_vec(&arroyo_sums).expect("Arroyo MessagePack encoding should succeed");

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(&arroyo_precompute_bytes)
            .expect("gzip encoding should succeed");
        let arroyo_json = json!({
            "aggregation_id": 11,
            "window": {
                "start": "1970-01-01T00:00:00",
                "end": "1970-01-01T00:00:10"
            },
            "key": "A",
            "precompute": hex::encode(encoder.finish().expect("gzip finalize should succeed"))
        });

        let streaming_config = StreamingConfig::new(agg_configs);
        let (arroyo_output, arroyo_acc) =
            PrecomputedOutput::deserialize_from_json_arroyo(&arroyo_json, &streaming_config)
                .expect("Arroyo precompute should deserialize");
        let arroyo_acc = arroyo_acc
            .as_any()
            .downcast_ref::<MultipleSumAccumulator>()
            .expect("Arroyo payload should deserialize to MultipleSumAccumulator");

        assert_eq!(
            handcrafted_output.aggregation_id,
            arroyo_output.aggregation_id
        );
        assert_eq!(
            handcrafted_output.start_timestamp,
            arroyo_output.start_timestamp
        );
        assert_eq!(
            handcrafted_output.end_timestamp,
            arroyo_output.end_timestamp
        );
        assert_eq!(handcrafted_output.key, arroyo_output.key);
        assert_eq!(handcrafted_acc.sums, arroyo_acc.sums);
    }

    #[test]
    fn test_arroyosketch_kll_matches_handcrafted_precompute_output() {
        let mut config = make_agg_config(12, "latency", "DatasketchesKLL", "", 10, 0, vec![]);
        config
            .parameters
            .insert("K".to_string(), serde_json::Value::from(20_u64));

        let mut agg_configs = HashMap::new();
        agg_configs.insert(12, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            agg_configs.clone(),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
        );

        let samples = vec![(1_000_i64, 10.0), (5_000_i64, 20.0), (9_000_i64, 30.0)];
        for &(ts, value) in &samples {
            worker
                .process_samples("latency", vec![(ts, value)])
                .unwrap();
        }
        worker
            .process_samples("latency", vec![(10_000_i64, 0.0)])
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "expected one closed window output");

        let (handcrafted_output, handcrafted_acc) = &captured[0];
        let handcrafted_acc = handcrafted_acc
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .expect("hand-crafted engine should emit DatasketchesKLLAccumulator");

        let arroyo_precompute_bytes = KllSketch::aggregate_kll(20, &[10.0, 20.0, 30.0])
            .expect("Arroyo KLL aggregation should produce bytes");

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(&arroyo_precompute_bytes)
            .expect("gzip encoding should succeed");
        let arroyo_json = json!({
            "aggregation_id": 12,
            "window": {
                "start": "1970-01-01T00:00:00",
                "end": "1970-01-01T00:00:10"
            },
            "key": "",
            "precompute": hex::encode(encoder.finish().expect("gzip finalize should succeed"))
        });

        let streaming_config = StreamingConfig::new(agg_configs);
        let (arroyo_output, arroyo_acc) =
            PrecomputedOutput::deserialize_from_json_arroyo(&arroyo_json, &streaming_config)
                .expect("Arroyo KLL precompute should deserialize");
        let arroyo_acc = arroyo_acc
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .expect("Arroyo payload should deserialize to DatasketchesKLLAccumulator");

        assert_eq!(
            handcrafted_output.aggregation_id,
            arroyo_output.aggregation_id
        );
        assert_eq!(
            handcrafted_output.start_timestamp,
            arroyo_output.start_timestamp
        );
        assert_eq!(
            handcrafted_output.end_timestamp,
            arroyo_output.end_timestamp
        );
        assert_eq!(handcrafted_output.key, None);
        assert_eq!(
            arroyo_output.key,
            Some(KeyByLabelValues::new_with_labels(vec![String::new()]))
        );
        assert_eq!(handcrafted_acc.inner.k, arroyo_acc.inner.k);
        assert_eq!(
            handcrafted_acc.inner.sketch.get_n(),
            arroyo_acc.inner.sketch.get_n()
        );

        for quantile in [0.0, 0.5, 1.0] {
            assert_eq!(
                handcrafted_acc.get_quantile(quantile),
                arroyo_acc.get_quantile(quantile)
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test: late data drop — sample behind watermark - allowed_lateness not emitted
    // -----------------------------------------------------------------------

    #[test]
    fn test_late_data_drop() {
        let config = make_agg_config(4, "cpu", "SingleSubpopulation", "Sum", 10, 0, vec![]);
        let mut agg_configs = HashMap::new();
        agg_configs.insert(4, config);

        let sink = Arc::new(CapturingOutputSink::new());
        // allowed_lateness_ms = 0
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut worker = Worker::new(
            0,
            rx,
            sink.clone(),
            agg_configs,
            10_000,  // max_buffer_per_series
            0,       // allowed_lateness_ms
            false,   // pass_raw_samples
            0,       // raw_mode_aggregation_id
            LateDataPolicy::Drop,
        );

        // Establish watermark at t=20000ms (closes [0, 10000) and [10000, 20000))
        worker
            .process_samples("cpu", vec![(20_000_i64, 1.0)])
            .unwrap();
        let _ = sink.drain(); // discard any earlier emissions

        // Send a late sample (ts=5000 is behind watermark=20000 with lateness=0)
        worker
            .process_samples("cpu", vec![(5_000_i64, 99.0)])
            .unwrap();

        // No new emission should occur (late sample is dropped)
        assert_eq!(sink.len(), 0, "late sample should be dropped, not emitted");
    }

    // -----------------------------------------------------------------------
    // Test: late data ForwardToStore — late sample emitted as mini-accumulator
    // -----------------------------------------------------------------------

    #[test]
    fn test_late_data_forward_to_store() {
        let config = make_agg_config(5, "cpu", "SingleSubpopulation", "Sum", 10, 0, vec![]);
        let mut agg_configs = HashMap::new();
        agg_configs.insert(5, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        // allowed_lateness_ms = 15000 — large enough that ts=8000 passes the
        // lateness filter (8000 >= 20000 - 15000 = 5000) while pane 0 is already
        // evicted (window [0,10000) closed when watermark reached 20000).
        let mut worker = Worker::new(
            0,
            rx,
            sink.clone(),
            agg_configs,
            10_000,  // max_buffer_per_series
            15_000,  // allowed_lateness_ms
            false,   // pass_raw_samples
            0,       // raw_mode_aggregation_id
            LateDataPolicy::ForwardToStore,
        );

        // Seed pane 0, then advance watermark to 20000 (evicts pane 0)
        worker.process_samples("cpu", vec![(500_i64, 1.0)]).unwrap();
        worker
            .process_samples("cpu", vec![(20_000_i64, 0.0)])
            .unwrap();
        let _ = sink.drain(); // discard the [0,10000) window emit

        // Send a late sample for the evicted pane 0 (ts=8000 passes the
        // lateness filter but pane 0 is gone → ForwardToStore path)
        worker
            .process_samples("cpu", vec![(8_000_i64, 55.0)])
            .unwrap();

        let captured = sink.drain();
        assert_eq!(
            captured.len(),
            1,
            "ForwardToStore policy should emit the late sample"
        );

        let (output, acc) = &captured[0];
        assert_eq!(output.aggregation_id, 5);
        // The late sample is emitted with the window it belongs to: pane_start=0, window=[0,10000)
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 10_000);

        let sum_acc = acc
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("should be SumAccumulator");
        assert!(
            (sum_acc.sum - 55.0).abs() < 1e-10,
            "late sample sum should be 55.0, got {}",
            sum_acc.sum
        );
    }

    // -----------------------------------------------------------------------
    // Test: worker built from a parsed streaming_config YAML
    // -----------------------------------------------------------------------

    #[test]
    fn test_worker_from_streaming_config_yaml() {
        // A minimal streaming_config.yaml payload — the same format the Python
        // controller writes to disk and the engine reads at startup.
        let yaml = r#"
aggregations:
- aggregationId: 10
  aggregationType: SingleSubpopulation
  aggregationSubType: Sum
  labels:
    grouping: []
    rollup: []
    aggregated: []
  metric: requests_total
  parameters: {}
  tumblingWindowSize: 10
  windowSize: 10
  windowType: tumbling
  slideInterval: 0
  spatialFilter: ''
"#;

        let data: serde_yaml::Value = serde_yaml::from_str(yaml).expect("valid YAML");
        let streaming_config =
            StreamingConfig::from_yaml_data(&data, None).expect("valid streaming config");

        assert!(
            streaming_config.contains(10),
            "aggregation 10 should be present"
        );

        let agg_configs = streaming_config.get_all_aggregation_configs().clone();
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // Three samples inside window [0, 10_000ms)
        worker
            .process_samples("requests_total", vec![(1_000_i64, 3.0)])
            .unwrap();
        worker
            .process_samples("requests_total", vec![(5_000_i64, 4.0)])
            .unwrap();
        worker
            .process_samples("requests_total", vec![(9_000_i64, 5.0)])
            .unwrap();
        assert_eq!(sink.len(), 0, "window not yet closed");

        // Advance watermark past window boundary to close [0, 10_000ms)
        worker
            .process_samples("requests_total", vec![(10_000_i64, 0.0)])
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "exactly one window should close");

        let (output, acc) = &captured[0];
        assert_eq!(output.aggregation_id, 10);
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 10_000);

        let sum_acc = acc
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("should be SumAccumulator");
        assert!(
            (sum_acc.sum - 12.0).abs() < 1e-10,
            "sum should be 3+4+5=12, got {}",
            sum_acc.sum
        );
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
            0,
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
