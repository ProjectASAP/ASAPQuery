use crate::data_model::{AggregateCore, KeyByLabelValues, PrecomputedOutput};
use crate::precompute_engine::accumulator_factory::{
    create_accumulator_updater, AccumulatorUpdater,
};
use crate::precompute_engine::config::LateDataPolicy;
use crate::precompute_engine::output_sink::OutputSink;
use crate::precompute_engine::series_router::WorkerMessage;
use crate::precompute_engine::window_manager::WindowManager;
use crate::precompute_operators::sum_accumulator::SumAccumulator;
use asap_types::aggregation_config::AggregationConfig;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, debug_span, info, warn};

/// Per-group aggregation state: window manager + active pane accumulators.
/// This is the equivalent of one (agg_id, group_key) in Arroyo's GROUP BY.
///
/// All raw series sharing the same grouping label values feed into the same
/// accumulator, producing one output per (group_key, window) — exactly like
/// Arroyo's `GROUP BY window, key`.
struct GroupState {
    config: Arc<AggregationConfig>,
    window_manager: WindowManager,
    /// Active panes keyed by pane_start_ms.
    active_panes: BTreeMap<i64, Box<dyn AccumulatorUpdater>>,
    /// Per-group watermark: tracks the maximum timestamp seen across all
    /// series in this group on this worker.
    previous_watermark_ms: i64,
}

/// Runtime configuration for a Worker, grouping non-structural parameters.
pub struct WorkerRuntimeConfig {
    pub max_buffer_per_series: usize,
    pub allowed_lateness_ms: i64,
    pub pass_raw_samples: bool,
    pub raw_mode_aggregation_id: u64,
    pub late_data_policy: LateDataPolicy,
}

/// Worker that processes samples for a shard of the group space.
///
/// Unlike the old per-series design, this worker maintains accumulators
/// keyed by `(agg_id, group_key)`. Multiple raw series with the same
/// grouping label values share a single accumulator, producing one merged
/// output per window — matching Arroyo's `GROUP BY` semantics.
pub struct Worker {
    id: usize,
    receiver: mpsc::Receiver<WorkerMessage>,
    output_sink: Arc<dyn OutputSink>,
    /// Map from (agg_id, group_key) to per-group state.
    group_states: HashMap<(u64, String), GroupState>,
    /// Aggregation configs, keyed by aggregation_id.
    agg_configs: HashMap<u64, Arc<AggregationConfig>>,
    /// Allowed lateness in ms.
    allowed_lateness_ms: i64,
    /// When true, skip aggregation and pass raw samples through.
    pass_raw_samples: bool,
    /// Aggregation ID stamped on each raw-mode output.
    raw_mode_aggregation_id: u64,
    /// Policy for handling late samples that arrive after their window has closed.
    late_data_policy: LateDataPolicy,
    /// This worker's watermark atomic, shared with engine for cross-worker reads.
    /// Updated during flush with max(all group watermarks).
    worker_watermark: Arc<AtomicI64>,
    /// All worker watermark atomics (including self), for computing global watermark.
    all_worker_watermarks: Vec<Arc<AtomicI64>>,
    /// Externally-readable group count for diagnostics.
    group_count: Arc<AtomicUsize>,
}

impl Worker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: usize,
        receiver: mpsc::Receiver<WorkerMessage>,
        output_sink: Arc<dyn OutputSink>,
        agg_configs: HashMap<u64, Arc<AggregationConfig>>,
        runtime_config: WorkerRuntimeConfig,
        group_count: Arc<AtomicUsize>,
        worker_watermark: Arc<AtomicI64>,
        all_worker_watermarks: Vec<Arc<AtomicI64>>,
    ) -> Self {
        let WorkerRuntimeConfig {
            max_buffer_per_series: _,
            allowed_lateness_ms,
            pass_raw_samples,
            raw_mode_aggregation_id,
            late_data_policy,
        } = runtime_config;
        Self {
            id,
            receiver,
            output_sink,
            group_states: HashMap::new(),
            agg_configs,
            allowed_lateness_ms,
            pass_raw_samples,
            raw_mode_aggregation_id,
            late_data_policy,
            worker_watermark,
            all_worker_watermarks,
            group_count,
        }
    }

    /// Run the worker loop. Blocks until shutdown.
    pub async fn run(mut self) {
        info!("Worker {} started", self.id);

        while let Some(msg) = self.receiver.recv().await {
            match msg {
                WorkerMessage::GroupSamples {
                    agg_id,
                    group_key,
                    samples,
                    ingest_received_at,
                } => {
                    let sample_count = samples.len();
                    let _span = debug_span!(
                        "worker_process_group",
                        worker_id = self.id,
                        agg_id,
                        group = %group_key,
                        sample_count,
                    )
                    .entered();
                    if let Err(e) = self.process_group_samples(agg_id, &group_key, samples) {
                        warn!(
                            "Worker {} error processing group ({}, {}): {}",
                            self.id, agg_id, group_key, e
                        );
                    }
                    debug!(
                        e2e_latency_us = ingest_received_at.elapsed().as_micros() as u64,
                        "e2e: ingest->worker complete"
                    );
                }
                WorkerMessage::RawSamples {
                    series_key,
                    samples,
                    ingest_received_at,
                } => {
                    let _span = debug_span!(
                        "worker_process_raw",
                        worker_id = self.id,
                        series = %series_key,
                        sample_count = samples.len(),
                    )
                    .entered();
                    if let Err(e) = self.process_samples_raw(&series_key, samples) {
                        warn!("Worker {} raw error for {}: {}", self.id, series_key, e);
                    }
                    debug!(
                        e2e_latency_us = ingest_received_at.elapsed().as_micros() as u64,
                        "e2e: ingest->worker complete (raw)"
                    );
                }
                WorkerMessage::Flush => {
                    if let Err(e) = self.flush_all() {
                        warn!("Worker {} flush error: {}", self.id, e);
                    }
                }
                WorkerMessage::Shutdown => {
                    info!("Worker {} shutting down", self.id);
                    if let Err(e) = self.flush_all() {
                        warn!("Worker {} final flush error: {}", self.id, e);
                    }
                    break;
                }
            }
        }

        info!(
            "Worker {} stopped, {} active groups",
            self.id,
            self.group_states.len()
        );
    }

    /// Get or create the GroupState for a (agg_id, group_key) pair.
    /// Returns None if agg_id has no matching config.
    fn get_or_create_group_state(
        &mut self,
        agg_id: u64,
        group_key: &str,
    ) -> Option<&mut GroupState> {
        let key = (agg_id, group_key.to_string());
        if !self.group_states.contains_key(&key) {
            let config = self.agg_configs.get(&agg_id)?;
            let gs = GroupState {
                window_manager: WindowManager::new(config.window_size, config.slide_interval),
                config: Arc::clone(config),
                active_panes: BTreeMap::new(),
                previous_watermark_ms: i64::MIN,
            };
            self.group_states.insert(key.clone(), gs);
            self.group_count
                .store(self.group_states.len(), Ordering::Relaxed);
        }
        self.group_states.get_mut(&key)
    }

    /// Process a batch of samples for a specific (agg_id, group_key).
    /// All samples in the batch feed into the same shared accumulator.
    ///
    /// This is the core of the Arroyo-equivalent GROUP BY logic.
    pub fn process_group_samples(
        &mut self,
        agg_id: u64,
        group_key: &str,
        samples: Vec<(String, i64, f64)>, // (series_key, timestamp_ms, value)
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let worker_id = self.id;
        let allowed_lateness_ms = self.allowed_lateness_ms;
        let late_data_policy = self.late_data_policy;

        if self.get_or_create_group_state(agg_id, group_key).is_none() {
            warn!(
                "Worker {} skipping samples for unknown agg_id={}, group_key={}",
                self.id, agg_id, group_key
            );
            return Ok(());
        }
        let state = self
            .group_states
            .get_mut(&(agg_id, group_key.to_string()))
            .unwrap();

        // Find the max timestamp in this batch to advance the watermark
        let batch_max_ts = samples
            .iter()
            .map(|(_, ts, _)| *ts)
            .max()
            .unwrap_or(i64::MIN);
        let previous_wm = state.previous_watermark_ms;
        let current_wm = if batch_max_ts > previous_wm {
            batch_max_ts
        } else {
            previous_wm
        };

        let mut emit_batch: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> = Vec::new();

        // Route each sample to its pane
        for (series_key, ts, val) in &samples {
            // Drop late samples
            if previous_wm != i64::MIN && *ts < previous_wm - allowed_lateness_ms {
                debug!(
                    "Worker {} dropping late sample for group ({}, {}): ts={} watermark={}",
                    worker_id, agg_id, group_key, ts, previous_wm
                );
                continue;
            }

            let pane_start = state.window_manager.pane_start_for(*ts);
            let pane_end = pane_start + state.window_manager.slide_interval_ms();

            // Check if pane was already evicted (late data for a closed window)
            if !state.active_panes.contains_key(&pane_start)
                && current_wm >= pane_start + state.window_manager.window_size_ms()
            {
                let window_start = pane_start;
                let window_end = pane_start + state.window_manager.window_size_ms();
                match late_data_policy {
                    LateDataPolicy::Drop => {
                        debug!(
                            "Dropping late sample for evicted pane [{}, {})",
                            pane_start, pane_end
                        );
                        continue;
                    }
                    LateDataPolicy::ForwardToStore => {
                        let mut updater = create_accumulator_updater(&state.config);
                        apply_sample(&mut *updater, series_key, *val, *ts, &state.config);
                        let key = build_group_key_label_values(group_key);
                        let output = PrecomputedOutput::new(
                            window_start as u64,
                            window_end as u64,
                            Some(key),
                            agg_id,
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

            // Normal path: route sample to its single pane accumulator
            let updater = state
                .active_panes
                .entry(pane_start)
                .or_insert_with(|| create_accumulator_updater(&state.config));

            apply_sample(&mut **updater, series_key, *val, *ts, &state.config);
        }

        // Check for closed windows
        let closed = state.window_manager.closed_windows(previous_wm, current_wm);

        for window_start in &closed {
            let (_, window_end) = state.window_manager.window_bounds(*window_start);
            let pane_starts = state.window_manager.panes_for_window(*window_start);

            if let Some(accumulator) = merge_panes_for_window(&mut state.active_panes, &pane_starts)
            {
                let key = build_group_key_label_values(group_key);
                let output = PrecomputedOutput::new(
                    *window_start as u64,
                    window_end as u64,
                    Some(key),
                    agg_id,
                );
                emit_batch.push((output, accumulator));
            }
        }

        state.previous_watermark_ms = current_wm;

        // Emit to output sink
        if !emit_batch.is_empty() {
            debug!(
                "Worker {} emitting {} outputs for group ({}, {})",
                worker_id,
                emit_batch.len(),
                agg_id,
                group_key
            );
            self.output_sink.emit_batch(emit_batch)?;
        }

        Ok(())
    }

    /// Raw fast-path: emit each sample as a standalone `SumAccumulator`.
    pub fn process_samples_raw(
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

    /// Flush all groups with cross-group watermark propagation.
    ///
    /// 1. Compute worker watermark = max(all group watermarks)
    /// 2. Publish it for cross-worker reads
    /// 3. Compute global watermark = min(all worker watermarks)
    /// 4. Advance idle groups to the global watermark, closing due windows
    fn flush_all(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.pass_raw_samples {
            return Ok(());
        }

        // Step 1: Compute worker watermark = max of all group watermarks.
        let worker_wm = self
            .group_states
            .values()
            .map(|s| s.previous_watermark_ms)
            .filter(|&wm| wm != i64::MIN)
            .max()
            .unwrap_or(i64::MIN);

        // Step 2: Publish our worker watermark for cross-worker reads.
        self.worker_watermark.store(worker_wm, Ordering::Release);

        // Step 3: Compute global watermark = min(all worker watermarks).
        let global_wm = self.compute_global_watermark();

        // Step 4: For each group, advance watermark and close due windows.
        let mut emit_batch: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> = Vec::new();

        for ((agg_id, group_key), state) in &mut self.group_states {
            if state.previous_watermark_ms == i64::MIN {
                continue; // No samples received yet — no panes to close.
            }

            // Effective watermark: max(group's own, global) + 1ms for boundary.
            let propagated_wm = if global_wm != i64::MIN {
                state.previous_watermark_ms.max(global_wm)
            } else {
                state.previous_watermark_ms
            };
            let effective_wm = propagated_wm.saturating_add(1);

            let closed = state
                .window_manager
                .closed_windows(state.previous_watermark_ms, effective_wm);

            for window_start in &closed {
                let (_, window_end) = state.window_manager.window_bounds(*window_start);
                let pane_starts = state.window_manager.panes_for_window(*window_start);

                if let Some(accumulator) =
                    merge_panes_for_window(&mut state.active_panes, &pane_starts)
                {
                    let key = build_group_key_label_values(group_key);
                    let output = PrecomputedOutput::new(
                        *window_start as u64,
                        window_end as u64,
                        Some(key),
                        *agg_id,
                    );
                    emit_batch.push((output, accumulator));
                }
            }

            // Update group watermark to reflect the advancement.
            if effective_wm > state.previous_watermark_ms {
                state.previous_watermark_ms = effective_wm;
            }
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

    /// Compute the global watermark as min(all worker watermarks), ignoring
    /// workers that haven't started yet (still at i64::MIN).
    fn compute_global_watermark(&self) -> i64 {
        let mut global_wm = i64::MAX;
        let mut any_started = false;
        for wm_atomic in &self.all_worker_watermarks {
            let wm = wm_atomic.load(Ordering::Acquire);
            if wm != i64::MIN {
                global_wm = global_wm.min(wm);
                any_started = true;
            }
        }
        if any_started {
            global_wm
        } else {
            i64::MIN
        }
    }
}

/// Build a `KeyByLabelValues` from a semicolon-delimited group key string.
/// e.g. "constant" → KeyByLabelValues { labels: ["constant"] }
/// e.g. "us-east;svc-a" → KeyByLabelValues { labels: ["us-east", "svc-a"] }
/// e.g. "" → KeyByLabelValues { labels: [""] }
fn build_group_key_label_values(group_key: &str) -> KeyByLabelValues {
    let labels: Vec<String> = group_key.split(';').map(|s| s.to_string()).collect();
    KeyByLabelValues::new_with_labels(labels)
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
pub fn parse_labels_from_series_key(series_key: &str) -> HashMap<&str, &str> {
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
    let mut remaining = label_str;
    while !remaining.is_empty() {
        let eq_pos = match remaining.find('=') {
            Some(pos) => pos,
            None => break,
        };
        let key = remaining[..eq_pos].trim();

        let after_eq = &remaining[eq_pos + 1..];
        if !after_eq.starts_with('"') {
            break;
        }

        let value_start = 1; // skip opening quote
        let value_end = match after_eq[value_start..].find('"') {
            Some(pos) => value_start + pos,
            None => break,
        };

        let value = &after_eq[value_start..value_end];
        labels.insert(key, value);

        let consumed = value_end + 1;
        remaining = &after_eq[consumed..];
        if remaining.starts_with(',') {
            remaining = &remaining[1..];
        }
    }

    labels
}

/// Route a single sample to `updater`, dispatching keyed vs. non-keyed based on config.
///
/// For keyed accumulators (MultipleSum, CMS, HydraKLL), the key is extracted
/// from the series' **aggregated_labels** — these are the labels that become
/// the key dimension *inside* the sketch (e.g., which bucket in a CMS, which
/// entry in a MultipleSumAccumulator's HashMap). This matches the Arroyo SQL
/// pattern: `udf(concat_ws(';', aggregated_labels), value)`.
fn apply_sample(
    updater: &mut dyn AccumulatorUpdater,
    series_key: &str,
    val: f64,
    ts: i64,
    config: &AggregationConfig,
) {
    if updater.is_keyed() {
        let key = extract_aggregated_key_from_series(series_key, config);
        updater.update_keyed(&key, val, ts);
    } else {
        updater.update_single(val, ts);
    }
}

/// Extract aggregated label values from a series key string.
/// These are the labels that form the key dimension *inside* keyed accumulators
/// (MultipleSum, CMS, HydraKLL), matching Arroyo's `agg_columns`.
fn extract_aggregated_key_from_series(
    series_key: &str,
    config: &AggregationConfig,
) -> KeyByLabelValues {
    let labels = parse_labels_from_series_key(series_key);
    let mut values = Vec::new();

    for label_name in &config.aggregated_labels.labels {
        if let Some(val) = labels.get(label_name.as_str()) {
            values.push(val.to_string());
        } else {
            values.push(String::new());
        }
    }

    KeyByLabelValues::new_with_labels(values)
}

/// Merge the pane accumulators that constitute a closed window.
///
/// The oldest pane (index 0) is taken destructively from `active_panes`
/// (no future window needs it). All later panes are snapshot-read
/// (non-destructive; they are shared by newer overlapping windows).
///
/// Returns `None` if all panes for the window are absent.
fn merge_panes_for_window(
    active_panes: &mut BTreeMap<i64, Box<dyn AccumulatorUpdater>>,
    pane_starts: &[i64],
) -> Option<Box<dyn AggregateCore>> {
    let mut merged: Option<Box<dyn AggregateCore>> = None;

    for (i, &ps) in pane_starts.iter().enumerate() {
        let pane_acc = if i == 0 {
            // Oldest pane: destructive take + evict
            active_panes
                .remove(&ps)
                .map(|mut updater| updater.take_accumulator())
        } else {
            // Shared pane: non-destructive snapshot
            active_panes
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

    merged
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
    use asap_types::enums::{AggregationType, WindowType};
    use sketch_core::kll::KllSketch;

    fn make_agg_config(
        id: u64,
        metric: &str,
        agg_type: AggregationType,
        agg_sub_type: &str,
        window_secs: u64,
        slide_secs: u64,
        grouping: Vec<&str>,
    ) -> AggregationConfig {
        make_agg_config_full(
            id,
            metric,
            agg_type,
            agg_sub_type,
            window_secs,
            slide_secs,
            grouping,
            vec![],
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn make_agg_config_full(
        id: u64,
        metric: &str,
        agg_type: AggregationType,
        agg_sub_type: &str,
        window_secs: u64,
        slide_secs: u64,
        grouping: Vec<&str>,
        aggregated: Vec<&str>,
    ) -> AggregationConfig {
        let window_type = if slide_secs == 0 || slide_secs == window_secs {
            WindowType::Tumbling
        } else {
            WindowType::Sliding
        };
        AggregationConfig::new(
            id,
            agg_type,
            agg_sub_type.to_string(),
            HashMap::new(),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(
                grouping.iter().map(|s| s.to_string()).collect(),
            ),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(
                aggregated.iter().map(|s| s.to_string()).collect(),
            ),
            promql_utilities::data_model::key_by_label_names::KeyByLabelNames::new(vec![]),
            String::new(),
            window_secs,
            slide_secs,
            window_type,
            metric.to_string(),
            metric.to_string(),
            None,
            None,
            None,
            None,
        )
    }

    fn make_worker(
        agg_configs: HashMap<u64, Arc<AggregationConfig>>,
        sink: Arc<CapturingOutputSink>,
        pass_raw: bool,
        raw_agg_id: u64,
        late_policy: LateDataPolicy,
    ) -> Worker {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let wm = Arc::new(AtomicI64::new(i64::MIN));
        Worker::new(
            0,
            rx,
            sink,
            agg_configs,
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms: 0,
                pass_raw_samples: pass_raw,
                raw_mode_aggregation_id: raw_agg_id,
                late_data_policy: late_policy,
            },
            Arc::new(AtomicUsize::new(0)),
            wm.clone(),
            vec![wm],
        )
    }

    fn arc_configs(
        configs: HashMap<u64, AggregationConfig>,
    ) -> HashMap<u64, Arc<AggregationConfig>> {
        configs.into_iter().map(|(k, v)| (k, Arc::new(v))).collect()
    }

    /// Helper to make GroupSamples from simple (ts, val) pairs for a single series.
    fn group_samples(series_key: &str, samples: Vec<(i64, f64)>) -> Vec<(String, i64, f64)> {
        samples
            .into_iter()
            .map(|(ts, val)| (series_key.to_string(), ts, val))
            .collect()
    }

    // -----------------------------------------------------------------------
    // Test: raw mode — each sample forwarded as SumAccumulator with sum==value
    // -----------------------------------------------------------------------

    #[test]
    fn test_raw_mode_forwarding() {
        let sink = Arc::new(CapturingOutputSink::new());
        let worker = make_worker(HashMap::new(), sink.clone(), true, 99, LateDataPolicy::Drop);

        let samples = vec![(1000_i64, 1.5_f64), (2000, 2.5), (3000, 7.0)];
        worker
            .process_samples_raw("cpu{host=\"a\"}", samples.clone())
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 3, "should emit one output per raw sample");

        for ((ts, val), (output, acc)) in samples.iter().zip(captured.iter()) {
            assert_eq!(output.start_timestamp as i64, *ts);
            assert_eq!(output.end_timestamp as i64, *ts);
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
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec![],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(1, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            arc_configs(agg_configs),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
        );

        // Samples in window [0, 10000ms): sum should be 1+2+3=6.
        // All go to the same group (agg_id=1, group_key="")
        worker
            .process_group_samples(1, "", group_samples("cpu", vec![(1000, 1.0)]))
            .unwrap();
        worker
            .process_group_samples(1, "", group_samples("cpu", vec![(5000, 2.0)]))
            .unwrap();
        worker
            .process_group_samples(1, "", group_samples("cpu", vec![(9000, 3.0)]))
            .unwrap();
        assert_eq!(sink.len(), 0);

        // Sample at t=10000ms closes [0, 10000)
        worker
            .process_group_samples(1, "", group_samples("cpu", vec![(10000, 100.0)]))
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "exactly one window should close");

        let (output, acc) = &captured[0];
        assert_eq!(output.aggregation_id, 1);
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 10_000);

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
    // Test: GROUP BY — multiple series merged into same group accumulator
    // -----------------------------------------------------------------------

    #[test]
    fn test_group_by_merges_series() {
        // SingleSubpopulation Sum with no grouping labels
        // Two different series in the same group → both feed same accumulator
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec![],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(1, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            arc_configs(agg_configs),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
        );

        // Two different series, same group (agg_id=1, group_key="")
        // Both feed into the same accumulator
        worker
            .process_group_samples(
                1,
                "",
                vec![
                    ("cpu{host=\"A\"}".to_string(), 1000, 10.0),
                    ("cpu{host=\"B\"}".to_string(), 2000, 20.0),
                ],
            )
            .unwrap();
        assert_eq!(sink.len(), 0);

        // Close the window
        worker
            .process_group_samples(1, "", group_samples("cpu{host=\"A\"}", vec![(10000, 0.0)]))
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "one output per group per window");

        let (output, acc) = &captured[0];
        assert_eq!(output.aggregation_id, 1);
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 10_000);

        let sum_acc = acc
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("should be SumAccumulator");
        assert!(
            (sum_acc.sum - 30.0).abs() < 1e-10,
            "sum should be 10+20=30, got {} (both series merged)",
            sum_acc.sum
        );
    }

    // -----------------------------------------------------------------------
    // Test: GROUP BY with grouping labels — different groups produce separate outputs
    // -----------------------------------------------------------------------

    #[test]
    fn test_different_groups_separate_outputs() {
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec!["pattern"],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(1, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            arc_configs(agg_configs),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
        );

        // Group "constant" gets samples
        worker
            .process_group_samples(
                1,
                "constant",
                group_samples("cpu{pattern=\"constant\"}", vec![(1000, 5.0)]),
            )
            .unwrap();
        // Group "sine" gets samples
        worker
            .process_group_samples(
                1,
                "sine",
                group_samples("cpu{pattern=\"sine\"}", vec![(2000, 7.0)]),
            )
            .unwrap();

        // Close both groups' windows
        worker
            .process_group_samples(
                1,
                "constant",
                group_samples("cpu{pattern=\"constant\"}", vec![(10000, 0.0)]),
            )
            .unwrap();
        worker
            .process_group_samples(
                1,
                "sine",
                group_samples("cpu{pattern=\"sine\"}", vec![(10000, 0.0)]),
            )
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 2, "two groups → two outputs");

        let mut sums_by_key: HashMap<String, f64> = HashMap::new();
        for (output, acc) in &captured {
            let sum_acc = acc.as_any().downcast_ref::<SumAccumulator>().unwrap();
            let key = output.key.as_ref().unwrap().labels.join(";");
            sums_by_key.insert(key, sum_acc.sum);
        }
        assert!((sums_by_key["constant"] - 5.0).abs() < 1e-10);
        assert!((sums_by_key["sine"] - 7.0).abs() < 1e-10);
    }

    // -----------------------------------------------------------------------
    // Test: KLL GROUP BY — multiple series merged into one KLL sketch per group
    // -----------------------------------------------------------------------

    #[test]
    fn test_kll_group_by_merges_series() {
        let mut config = make_agg_config(
            1,
            "latency",
            AggregationType::DatasketchesKLL,
            "",
            10,
            0,
            vec!["pattern"],
        );
        config
            .parameters
            .insert("K".to_string(), serde_json::Value::from(20_u64));
        let mut agg_configs = HashMap::new();
        agg_configs.insert(1, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            arc_configs(agg_configs),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
        );

        // Three different series all in group "constant" — all feed one KLL
        worker
            .process_group_samples(
                1,
                "constant",
                vec![
                    (
                        "latency{pattern=\"constant\",host=\"a\"}".to_string(),
                        1000,
                        10.0,
                    ),
                    (
                        "latency{pattern=\"constant\",host=\"b\"}".to_string(),
                        2000,
                        20.0,
                    ),
                    (
                        "latency{pattern=\"constant\",host=\"c\"}".to_string(),
                        3000,
                        30.0,
                    ),
                ],
            )
            .unwrap();

        // Close the window
        worker
            .process_group_samples(
                1,
                "constant",
                group_samples(
                    "latency{pattern=\"constant\",host=\"a\"}",
                    vec![(10000, 0.0)],
                ),
            )
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "one KLL output for the whole group");

        let (output, acc) = &captured[0];
        assert_eq!(output.aggregation_id, 1);
        let kll = acc
            .as_any()
            .downcast_ref::<DatasketchesKLLAccumulator>()
            .expect("should be KLL");
        assert_eq!(
            kll.inner.count(),
            3,
            "KLL should contain all 3 series' samples"
        );
    }

    // -----------------------------------------------------------------------
    // Test: sliding window pane sharing
    // -----------------------------------------------------------------------

    #[test]
    fn test_sliding_window_pane_sharing() {
        // 30s window, 10s slide → W=3 panes per window
        let config = make_agg_config(
            2,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            30,
            10,
            vec![],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(2, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            arc_configs(agg_configs),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
        );

        // Sample at t=15000ms → goes to pane 10000ms
        worker
            .process_group_samples(2, "", group_samples("cpu", vec![(15_000, 42.0)]))
            .unwrap();
        assert_eq!(sink.len(), 0);

        // Sample at t=45000ms → advances watermark to 45000ms
        // Closes windows [0, 30000) and [10000, 40000)
        worker
            .process_group_samples(2, "", group_samples("cpu", vec![(45_000, 0.0)]))
            .unwrap();

        let captured = sink.drain();
        assert_eq!(
            captured.len(),
            2,
            "two windows containing the pane should emit"
        );

        let window_starts: Vec<u64> = captured.iter().map(|(o, _)| o.start_timestamp).collect();
        assert!(window_starts.contains(&0));
        assert!(window_starts.contains(&10_000));

        for (_output, acc) in &captured {
            let sum_acc = acc
                .as_any()
                .downcast_ref::<SumAccumulator>()
                .expect("should be SumAccumulator");
            assert!(
                (sum_acc.sum - 42.0).abs() < 1e-10,
                "window should have sum=42 via pane sharing, got {}",
                sum_acc.sum
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test: MultipleSubpopulation — keyed accumulator with aggregated labels
    // Matches planner output: grouping=[], aggregated=[host]
    // All series go to one group, host is the key dimension INSIDE the sketch
    // -----------------------------------------------------------------------

    #[test]
    fn test_keyed_accumulator_aggregated_labels() {
        // Like planner output for `sum by (host) (cpu)`:
        // grouping=[] (empty), aggregated=[host] (key inside MultipleSumAccumulator)
        let config = make_agg_config_full(
            3,
            "cpu",
            AggregationType::MultipleSubpopulation,
            "Sum",
            10,
            0,
            vec![],       // grouping: empty — one output group
            vec!["host"], // aggregated: host is the key INSIDE the sketch
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(3, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            arc_configs(agg_configs),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
        );

        // Both series go to the SAME group (group_key="" since grouping is empty).
        // The host label is extracted as the aggregated key inside the accumulator.
        worker
            .process_group_samples(
                3,
                "",
                vec![
                    ("cpu{host=\"A\"}".to_string(), 1000, 10.0),
                    ("cpu{host=\"B\"}".to_string(), 2000, 20.0),
                ],
            )
            .unwrap();

        // Close the single group's window
        worker
            .process_group_samples(3, "", group_samples("cpu{host=\"A\"}", vec![(10000, 0.0)]))
            .unwrap();

        let captured = sink.drain();
        assert_eq!(
            captured.len(),
            1,
            "one group → one output (both hosts inside)"
        );

        let (_output, acc) = &captured[0];
        let ms_acc = acc
            .as_any()
            .downcast_ref::<MultipleSumAccumulator>()
            .expect("should be MultipleSumAccumulator");

        // The MultipleSumAccumulator should have two internal keys: "A" and "B"
        assert_eq!(ms_acc.sums.len(), 2, "two host keys inside one accumulator");

        let mut found_a = false;
        let mut found_b = false;
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
        assert!(found_a, "expected key A inside accumulator");
        assert!(found_b, "expected key B inside accumulator");
    }

    // -----------------------------------------------------------------------
    // Test: Arroyo KLL equivalence — same output as Arroyo pipeline
    // -----------------------------------------------------------------------
    #[test]
    fn test_arroyosketch_multiple_sum_matches_handcrafted_precompute_output() {
        let config = make_agg_config(
            11,
            "cpu",
            AggregationType::MultipleSum,
            "sum",
            10,
            0,
            vec!["host"],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(11, config.clone());

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            arc_configs(agg_configs.clone()),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
        );

        worker
            .process_group_samples(
                11,
                "A",
                group_samples("cpu{host=\"A\"}", vec![(1_000_i64, 1.0)]),
            )
            .unwrap();
        worker
            .process_group_samples(
                11,
                "A",
                group_samples("cpu{host=\"A\"}", vec![(5_000_i64, 2.0)]),
            )
            .unwrap();
        worker
            .process_group_samples(
                11,
                "A",
                group_samples("cpu{host=\"A\"}", vec![(9_000_i64, 3.0)]),
            )
            .unwrap();
        worker
            .process_group_samples(
                11,
                "A",
                group_samples("cpu{host=\"A\"}", vec![(10_000_i64, 0.0)]),
            )
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "expected one closed window output");

        let (handcrafted_output, handcrafted_acc) = &captured[0];
        let handcrafted_acc = handcrafted_acc
            .as_any()
            .downcast_ref::<MultipleSumAccumulator>()
            .expect("hand-crafted engine should emit MultipleSumAccumulator");

        // grouping=["host"] means the host value goes in the outer key ("A"),
        // and aggregated=[] means the accumulator sub-key has no labels.
        assert_eq!(handcrafted_output.aggregation_id, 11);
        assert_eq!(handcrafted_output.start_timestamp, 0);
        assert_eq!(handcrafted_output.end_timestamp, 10_000);
        assert_eq!(
            handcrafted_output.key,
            Some(KeyByLabelValues::new_with_labels(vec!["A".to_string()]))
        );

        let mut expected_sums = HashMap::new();
        expected_sums.insert(KeyByLabelValues::new_with_labels(vec![]), 6.0);
        assert_eq!(handcrafted_acc.sums, expected_sums);
    }

    #[test]
    fn test_arroyosketch_kll_matches_handcrafted_precompute_output() {
        let mut config = make_agg_config(
            12,
            "latency",
            AggregationType::DatasketchesKLL,
            "",
            10,
            0,
            vec![],
        );
        config
            .parameters
            .insert("K".to_string(), serde_json::Value::from(20_u64));

        let mut agg_configs = HashMap::new();
        agg_configs.insert(12, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            arc_configs(agg_configs.clone()),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
        );

        let samples = vec![(1_000_i64, 10.0), (5_000_i64, 20.0), (9_000_i64, 30.0)];
        for &(ts, value) in &samples {
            worker
                .process_group_samples(12, "", group_samples("latency", vec![(ts, value)]))
                .unwrap();
        }
        worker
            .process_group_samples(12, "", group_samples("latency", vec![(10_000, 0.0)]))
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
        assert_eq!(handcrafted_acc.inner.k, arroyo_acc.inner.k);
        assert_eq!(handcrafted_acc.inner.count(), arroyo_acc.inner.count());

        for quantile in [0.0, 0.5, 1.0] {
            assert_eq!(
                handcrafted_acc.get_quantile(quantile),
                arroyo_acc.get_quantile(quantile)
            );
        }
    }

    // -----------------------------------------------------------------------
    // Test: Arroyo MultipleSum equivalence
    // -----------------------------------------------------------------------

    #[test]
    fn test_arroyosketch_multiple_sum_empty_grouping_matches_handcrafted_precompute_output() {
        // Like planner output: grouping=[], aggregated=[host]
        let config = make_agg_config_full(
            11,
            "cpu",
            AggregationType::MultipleSum,
            "sum",
            10,
            0,
            vec![],
            vec!["host"],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(11, config.clone());

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            arc_configs(agg_configs.clone()),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
        );

        // All samples go to group "" (empty group key since grouping=[]).
        // The host label is the aggregated key inside the accumulator.
        worker
            .process_group_samples(11, "", group_samples("cpu{host=\"A\"}", vec![(1_000, 1.0)]))
            .unwrap();
        worker
            .process_group_samples(11, "", group_samples("cpu{host=\"A\"}", vec![(5_000, 2.0)]))
            .unwrap();
        worker
            .process_group_samples(11, "", group_samples("cpu{host=\"A\"}", vec![(9_000, 3.0)]))
            .unwrap();
        worker
            .process_group_samples(
                11,
                "",
                group_samples("cpu{host=\"A\"}", vec![(10_000, 0.0)]),
            )
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "expected one closed window output");

        let (handcrafted_output, handcrafted_acc) = &captured[0];
        let handcrafted_acc = handcrafted_acc
            .as_any()
            .downcast_ref::<MultipleSumAccumulator>()
            .expect("hand-crafted engine should emit MultipleSumAccumulator");

        // Arroyo: GROUP BY '' (empty key), UDF gets host="A" as aggregated key
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
            "key": "",
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

    // -----------------------------------------------------------------------
    // Test: late data drop
    // -----------------------------------------------------------------------

    #[test]
    fn test_late_data_drop() {
        let config = make_agg_config(
            4,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec![],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(4, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let wm = Arc::new(AtomicI64::new(i64::MIN));
        let mut worker = Worker::new(
            0,
            rx,
            sink.clone(),
            arc_configs(agg_configs),
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms: 0,
                pass_raw_samples: false,
                raw_mode_aggregation_id: 0,
                late_data_policy: LateDataPolicy::Drop,
            },
            Arc::new(AtomicUsize::new(0)),
            wm.clone(),
            vec![wm],
        );

        // Establish watermark at t=20000ms
        worker
            .process_group_samples(4, "", group_samples("cpu", vec![(20_000, 1.0)]))
            .unwrap();
        let _ = sink.drain();

        // Send a late sample
        worker
            .process_group_samples(4, "", group_samples("cpu", vec![(5_000, 99.0)]))
            .unwrap();

        assert_eq!(sink.len(), 0, "late sample should be dropped");
    }

    // -----------------------------------------------------------------------
    // Test: late data ForwardToStore
    // -----------------------------------------------------------------------

    #[test]
    fn test_late_data_forward_to_store() {
        let config = make_agg_config(
            5,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec![],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(5, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let wm = Arc::new(AtomicI64::new(i64::MIN));
        let mut worker = Worker::new(
            0,
            rx,
            sink.clone(),
            arc_configs(agg_configs),
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms: 15_000,
                pass_raw_samples: false,
                raw_mode_aggregation_id: 0,
                late_data_policy: LateDataPolicy::ForwardToStore,
            },
            Arc::new(AtomicUsize::new(0)),
            wm.clone(),
            vec![wm],
        );

        // Seed then advance watermark to 20000
        worker
            .process_group_samples(5, "", group_samples("cpu", vec![(500, 1.0)]))
            .unwrap();
        worker
            .process_group_samples(5, "", group_samples("cpu", vec![(20_000, 0.0)]))
            .unwrap();
        let _ = sink.drain();

        // Send late sample for evicted pane
        worker
            .process_group_samples(5, "", group_samples("cpu", vec![(8_000, 55.0)]))
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "ForwardToStore should emit");

        let (output, acc) = &captured[0];
        assert_eq!(output.aggregation_id, 5);
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
    // Test: worker from streaming_config YAML
    // -----------------------------------------------------------------------

    #[test]
    fn test_worker_from_streaming_config_yaml() {
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

        assert!(streaming_config.contains(10));

        let agg_configs = arc_configs(streaming_config.get_all_aggregation_configs().clone());
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        worker
            .process_group_samples(10, "", group_samples("requests_total", vec![(1_000, 3.0)]))
            .unwrap();
        worker
            .process_group_samples(10, "", group_samples("requests_total", vec![(5_000, 4.0)]))
            .unwrap();
        worker
            .process_group_samples(10, "", group_samples("requests_total", vec![(9_000, 5.0)]))
            .unwrap();
        assert_eq!(sink.len(), 0);

        worker
            .process_group_samples(10, "", group_samples("requests_total", vec![(10_000, 0.0)]))
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1);

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
            AggregationType::SingleSubpopulation,
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
            WindowType::Tumbling,
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

    #[test]
    fn test_build_group_key_label_values() {
        let key = build_group_key_label_values("constant");
        assert_eq!(key.labels, vec!["constant".to_string()]);

        let key = build_group_key_label_values("us-east;svc-a");
        assert_eq!(key.labels, vec!["us-east".to_string(), "svc-a".to_string()]);

        let key = build_group_key_label_values("");
        assert_eq!(key.labels, vec!["".to_string()]);
    }

    // -----------------------------------------------------------------------
    // Tests: cross-group watermark propagation
    // -----------------------------------------------------------------------

    #[test]
    fn test_intra_worker_watermark_propagation() {
        // Two groups on the same worker. Group A advances to t=100s.
        // Group B has data at t=10s and then goes idle.
        // After flush, group B's idle windows should close via propagation.
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec![],
        );
        let agg_configs = arc_configs(HashMap::from([(1, config)]));
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(agg_configs, sink.clone(), false, 0, LateDataPolicy::Drop);

        // Group A: send sample at t=5s (within window [0, 10s))
        worker
            .process_group_samples(1, "groupA", group_samples("cpu", vec![(5_000, 1.0)]))
            .unwrap();
        // Group B: send sample at t=5s (within window [0, 10s))
        worker
            .process_group_samples(1, "groupB", group_samples("cpu", vec![(5_000, 2.0)]))
            .unwrap();
        let _ = sink.drain();

        // Advance group A's watermark to t=100s (closes many windows).
        worker
            .process_group_samples(1, "groupA", group_samples("cpu", vec![(100_000, 3.0)]))
            .unwrap();
        let _ = sink.drain();

        // Group B has NOT received new data — its watermark is still at 5s.
        // Flush should propagate group A's watermark to group B.
        worker.flush_all().unwrap();
        let flushed = sink.drain();

        // Group B's window [0, 10s) should now be closed via propagation.
        let group_b_outputs: Vec<_> = flushed
            .iter()
            .filter(|(out, _)| {
                out.key
                    .as_ref()
                    .map(|k| k.labels == vec!["groupB".to_string()])
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            !group_b_outputs.is_empty(),
            "idle group B should have windows closed via watermark propagation"
        );
    }

    #[test]
    fn test_compute_global_watermark_min_of_started() {
        let wm0 = Arc::new(AtomicI64::new(100_000));
        let wm1 = Arc::new(AtomicI64::new(80_000));
        let wm2 = Arc::new(AtomicI64::new(90_000));
        let all = vec![wm0.clone(), wm1.clone(), wm2.clone()];

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let worker = Worker::new(
            0,
            rx,
            Arc::new(CapturingOutputSink::new()),
            HashMap::new(),
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms: 0,
                pass_raw_samples: false,
                raw_mode_aggregation_id: 0,
                late_data_policy: LateDataPolicy::Drop,
            },
            Arc::new(AtomicUsize::new(0)),
            wm0,
            all,
        );

        assert_eq!(worker.compute_global_watermark(), 80_000);
    }

    #[test]
    fn test_compute_global_watermark_ignores_unstarted() {
        let wm0 = Arc::new(AtomicI64::new(100_000));
        let wm1 = Arc::new(AtomicI64::new(i64::MIN)); // not started
        let all = vec![wm0.clone(), wm1.clone()];

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let worker = Worker::new(
            0,
            rx,
            Arc::new(CapturingOutputSink::new()),
            HashMap::new(),
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms: 0,
                pass_raw_samples: false,
                raw_mode_aggregation_id: 0,
                late_data_policy: LateDataPolicy::Drop,
            },
            Arc::new(AtomicUsize::new(0)),
            wm0,
            all,
        );

        assert_eq!(
            worker.compute_global_watermark(),
            100_000,
            "unstarted workers (i64::MIN) should be ignored"
        );
    }

    #[test]
    fn test_compute_global_watermark_all_unstarted() {
        let wm0 = Arc::new(AtomicI64::new(i64::MIN));
        let wm1 = Arc::new(AtomicI64::new(i64::MIN));
        let all = vec![wm0.clone(), wm1.clone()];

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let worker = Worker::new(
            0,
            rx,
            Arc::new(CapturingOutputSink::new()),
            HashMap::new(),
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms: 0,
                pass_raw_samples: false,
                raw_mode_aggregation_id: 0,
                late_data_policy: LateDataPolicy::Drop,
            },
            Arc::new(AtomicUsize::new(0)),
            wm0,
            all,
        );

        assert_eq!(
            worker.compute_global_watermark(),
            i64::MIN,
            "all unstarted should return i64::MIN"
        );
    }

    #[test]
    fn test_flush_publishes_worker_watermark() {
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10,
            0,
            vec![],
        );
        let agg_configs = arc_configs(HashMap::from([(1, config)]));
        let sink = Arc::new(CapturingOutputSink::new());
        let wm = Arc::new(AtomicI64::new(i64::MIN));
        let all = vec![wm.clone()];
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut worker = Worker::new(
            0,
            rx,
            sink,
            agg_configs,
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms: 0,
                pass_raw_samples: false,
                raw_mode_aggregation_id: 0,
                late_data_policy: LateDataPolicy::Drop,
            },
            Arc::new(AtomicUsize::new(0)),
            wm.clone(),
            all,
        );

        assert_eq!(wm.load(Ordering::Acquire), i64::MIN);

        // Send data at t=50s
        worker
            .process_group_samples(1, "", group_samples("cpu", vec![(50_000, 1.0)]))
            .unwrap();

        // Flush should publish worker watermark
        worker.flush_all().unwrap();
        assert_eq!(
            wm.load(Ordering::Acquire),
            50_000,
            "worker watermark should be published after flush"
        );
    }
}
