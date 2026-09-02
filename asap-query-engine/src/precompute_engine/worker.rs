use crate::data_model::{AggregateCore, AggregationType, KeyByLabelValues, PrecomputedOutput};
use crate::precompute_engine::accumulator_factory::{
    create_accumulator_updater, AccumulatorUpdater,
};
use crate::precompute_engine::config::LateDataPolicy;
use crate::precompute_engine::output_sink::OutputSink;
use crate::precompute_engine::series_router::WorkerMessage;
use crate::precompute_engine::window_manager::WindowManager;
use crate::precompute_operators::delta_set_aggregator_accumulator::DeltaSetAggregatorAccumulator;
use crate::precompute_operators::sum_accumulator::SumAccumulator;
use asap_types::aggregation_config::AggregationConfig;
use std::collections::{BTreeMap, HashMap, HashSet};
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
    /// Key population emitted by the previous non-empty DeltaSetAggregator
    /// window for this (aggregation_id, group_key). DeltaSetAggregator outputs
    /// are differences between consecutive window populations, so this state
    /// must live at group scope rather than inside a single pane updater.
    delta_set_previous_keys: HashSet<KeyByLabelValues>,
    /// Per-group watermark: tracks the maximum timestamp seen across all
    /// series in this group on this worker.
    previous_watermark_ms: i64,
    /// Wall-clock-time (ms since epoch) at which each currently-open pane
    /// was last touched by a sample. Refreshed on every touch, not just the
    /// first. Used by `flush_all`'s wall-clock fallback to close panes that
    /// have been *idle* too long when event-time has stagnated — a pane
    /// still receiving samples is never "too old," no matter how long it's
    /// been open, only a pane nothing has touched in a while is. Keyed by
    /// `pane_start_ms`, mirroring `active_panes`. Entries are GC'd by
    /// `prune_pane_wall_clock_last_touch` after each window-close cycle so
    /// the bookkeeping doesn't leak as panes turn over.
    ///
    /// No absolute ceiling on pane lifetime is enforced here by design: a
    /// pane touched forever without ever going idle (e.g. a misbehaving
    /// source stuck emitting a stagnant timestamp at a low but nonzero rate)
    /// stays open, growing memory, until process shutdown force-closes it
    /// via `force_close_all`. Deferred rather than bolted on speculatively —
    /// distinct failure mode from the bulk-load case this field fixes, with
    /// no evidence it happens in practice. Add `now - first_touch >= max_ms`
    /// if it does.
    pane_wall_clock_last_touch_ms: BTreeMap<i64, i64>,
}

impl GroupState {
    /// Drop wall-clock-last-touch entries whose pane no longer exists in
    /// `active_panes`. Called after window-close cycles in
    /// `process_group_samples` and `flush_all` so the bookkeeping doesn't
    /// leak as panes turn over.
    fn prune_pane_wall_clock_last_touch(&mut self) {
        let active = &self.active_panes;
        self.pane_wall_clock_last_touch_ms
            .retain(|ps, _| active.contains_key(ps));
    }
}

/// Runtime configuration for a Worker, grouping non-structural parameters.
pub struct WorkerRuntimeConfig {
    pub max_buffer_per_series: usize,
    pub allowed_lateness_ms: i64,
    pub pass_raw_samples: bool,
    pub raw_mode_aggregation_id: u64,
    pub late_data_policy: LateDataPolicy,
    /// See `PrecomputeEngineConfig::wall_clock_grace_period_ms`. Set to a
    /// non-positive value to disable the wall-clock fallback entirely
    /// (event-time-only behaviour, matching pre-fix semantics).
    pub wall_clock_grace_period_ms: i64,
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
    /// Per-group state, keyed by `agg_id` then by an interned `Arc<str>`
    /// group key. Nesting lets the per-sample hot path look up by `&str`
    /// (no allocation); the group-key string is allocated once, on first
    /// sight of the group, and shared via the `Arc`.
    group_states: HashMap<u64, HashMap<Arc<str>, GroupState>>,
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
    /// Grace period (ms) for the wall-clock fallback in `flush_all`.
    /// `<= 0` disables the fallback (event-time-only).
    wall_clock_grace_period_ms: i64,
    /// Injectable clock returning current wall-clock time in milliseconds
    /// since the unix epoch. Production uses `SystemTime::now`; tests
    /// override with a deterministic fake via `set_now_ms_fn` (directly, or
    /// via `PrecomputeEngine::with_now_ms_fn` for integration tests).
    now_ms_fn: Box<dyn Fn() -> i64 + Send + Sync>,
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
            wall_clock_grace_period_ms,
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
            wall_clock_grace_period_ms,
            now_ms_fn: Box::new(default_now_ms),
        }
    }

    /// Test-support setter for the wall-clock source. Replaces the default
    /// `SystemTime::now`-backed clock with a deterministic fake so tests can
    /// drive the wall-clock fallback in `flush_all` without real sleeping.
    /// Crate-visible (not `#[cfg(test)]`-gated) so `PrecomputeEngine::
    /// with_now_ms_fn` can reach it from integration tests in `tests/`,
    /// which link the library normally and don't see `#[cfg(test)]` items.
    /// Production code never calls this.
    pub(crate) fn set_now_ms_fn(&mut self, f: Box<dyn Fn() -> i64 + Send + Sync>) {
        self.now_ms_fn = f;
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
                    // Force-close any windows still open after the final flush.
                    // `flush_all` only advances the watermark by +1ms (plus the
                    // wall-clock fallback, whose grace may not have elapsed for a
                    // one-shot batch), so the trailing window can remain open and
                    // its data would never reach the store. No more samples will
                    // arrive after shutdown, so close every remaining pane.
                    if let Err(e) = self.force_close_all() {
                        warn!("Worker {} shutdown force-close error: {}", self.id, e);
                    }
                    break;
                }
                WorkerMessage::UpdateAggConfigs(new_configs) => {
                    // Flush and evict group states for agg IDs that are being removed.
                    // Must happen before swapping agg_configs so GroupState.config is
                    // still valid during the final window close.
                    let removed_ids: Vec<u64> = self
                        .agg_configs
                        .keys()
                        .filter(|id| !new_configs.contains_key(id))
                        .copied()
                        .collect();

                    if !removed_ids.is_empty() {
                        let mut emit_batch: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> =
                            Vec::new();

                        for agg_id in &removed_ids {
                            // Drain all group states for this agg_id in one move.
                            let Some(inner) = self.group_states.remove(agg_id) else {
                                continue;
                            };

                            for (group_key_str, mut state) in inner {
                                if state.previous_watermark_ms == i64::MIN {
                                    continue; // No samples received — nothing to emit.
                                }
                                // Force-close all open windows; no new samples will arrive
                                // for this removed agg_id. Advance to a *finite* bound
                                // (`max_pane + window_size_ms`), NOT `i64::MAX`:
                                // `closed_windows` enumerates window starts one slide at a
                                // time, so `i64::MAX` would loop ~`i64::MAX / slide` times
                                // and overflow (see `force_close_all`).
                                let mut active_panes = state.active_panes;
                                let closed = match active_panes.keys().next_back() {
                                    Some(&max_pane) => {
                                        let force_wm = max_pane
                                            .saturating_add(state.window_manager.window_size_ms());
                                        state
                                            .window_manager
                                            .closed_windows(state.previous_watermark_ms, force_wm)
                                    }
                                    None => Vec::new(), // no open panes
                                };

                                for window_start in &closed {
                                    let (_, window_end) =
                                        state.window_manager.window_bounds(*window_start);
                                    let pane_starts =
                                        state.window_manager.panes_for_window(*window_start);
                                    if let Some(accumulator) =
                                        merge_panes_for_window(&mut active_panes, &pane_starts)
                                    {
                                        let accumulator = match finalize_closed_accumulator(
                                            accumulator,
                                            &state.config,
                                            &mut state.delta_set_previous_keys,
                                        ) {
                                            Ok(accumulator) => accumulator,
                                            Err(e) => {
                                                warn!(
                                                    "Worker {}: failed to finalize DeltaSetAggregator \
                                                     for removed agg_id={}: {}",
                                                    self.id, agg_id, e
                                                );
                                                continue;
                                            }
                                        };
                                        let group_key_lv =
                                            build_group_key_label_values(&group_key_str);
                                        let output = PrecomputedOutput::new(
                                            *window_start as u64,
                                            window_end as u64,
                                            Some(group_key_lv),
                                            *agg_id,
                                        );
                                        emit_batch.push((output, accumulator));
                                    }
                                }
                            }
                        }

                        if !emit_batch.is_empty() {
                            if let Err(e) = self.output_sink.emit_batch(emit_batch) {
                                warn!(
                                    "Worker {}: error flushing removed agg_ids {:?}: {}",
                                    self.id, removed_ids, e
                                );
                            }
                        }

                        self.group_count
                            .store(self.total_groups(), Ordering::Relaxed);
                        info!(
                            "Worker {}: evicted {} removed agg_id(s) {:?}",
                            self.id,
                            removed_ids.len(),
                            removed_ids,
                        );
                    }

                    let added = new_configs.len().saturating_sub(self.agg_configs.len());
                    self.agg_configs = new_configs;
                    info!(
                        "Worker {}: agg_configs updated ({} total, ~{} added)",
                        self.id,
                        self.agg_configs.len(),
                        added,
                    );
                }
            }
        }

        info!(
            "Worker {} stopped, {} active groups",
            self.id,
            self.total_groups()
        );
    }

    /// Total number of live groups across all agg_ids.
    fn total_groups(&self) -> usize {
        self.group_states.values().map(|m| m.len()).sum()
    }

    /// Get or create the GroupState for a (agg_id, group_key) pair.
    /// Returns None if agg_id has no matching config.
    fn get_or_create_group_state(
        &mut self,
        agg_id: u64,
        group_key: &str,
    ) -> Option<&mut GroupState> {
        // Fast path: group already exists — borrow-based lookup, no allocation.
        let exists = self
            .group_states
            .get(&agg_id)
            .is_some_and(|m| m.contains_key(group_key));
        if !exists {
            // Creation path: requires a config, and allocates the interned key once.
            let config = Arc::clone(self.agg_configs.get(&agg_id)?);
            let gs = GroupState {
                window_manager: WindowManager::new(config.window_size_ms, config.slide_interval_ms),
                config,
                active_panes: BTreeMap::new(),
                delta_set_previous_keys: HashSet::new(),
                previous_watermark_ms: i64::MIN,
                pane_wall_clock_last_touch_ms: BTreeMap::new(),
            };
            self.group_states
                .entry(agg_id)
                .or_default()
                .insert(Arc::from(group_key), gs);
            self.group_count
                .store(self.total_groups(), Ordering::Relaxed);
        }
        self.group_states.get_mut(&agg_id)?.get_mut(group_key)
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
        // Sample the wall clock before borrowing `state` (the closure lives
        // on `self`); used to stamp each pane's birth time below.
        let now_ms = (self.now_ms_fn)();

        let state = match self.get_or_create_group_state(agg_id, group_key) {
            Some(state) => state,
            None => {
                warn!(
                    "Worker {} skipping samples for unknown agg_id={}, group_key={}",
                    self.id, agg_id, group_key
                );
                return Ok(());
            }
        };

        // Find the max timestamp in this batch to advance the watermark
        let batch_max_ts = samples
            .iter()
            .map(|(_, ts, _)| *ts)
            .max()
            .unwrap_or(i64::MIN);
        let batch_min_ts = samples
            .iter()
            .map(|(_, ts, _)| *ts)
            .min()
            .unwrap_or(batch_max_ts);
        let previous_wm = state.previous_watermark_ms;
        let current_wm = if batch_max_ts > previous_wm {
            batch_max_ts
        } else {
            previous_wm
        };
        // On the first batch there is no prior watermark. Use the earliest
        // sample as the closure baseline so a batch spanning multiple windows
        // can close windows older than that sample after all samples are routed.
        let closure_previous_wm = if previous_wm == i64::MIN {
            batch_min_ts
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
                && previous_wm >= pane_start + state.window_manager.window_size_ms()
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
                    LateDataPolicy::ForwardToStore
                        if state.config.aggregation_type == AggregationType::DeltaSetAggregator =>
                    {
                        warn!(
                            "Dropping late DeltaSetAggregator sample for evicted pane [{}, {}): ForwardToStore is unsupported for stateful key deltas",
                            pane_start,
                            pane_end
                        );
                        continue;
                    }
                    LateDataPolicy::ForwardToStore => {
                        let mut updater = create_accumulator_updater(&state.config)?;
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

            // Normal path: route sample to its single pane accumulator.
            // Refresh the pane's last-touch wall-clock time on every touch
            // (not just the first) so the wall-clock fallback in `flush_all`
            // only ages out panes that have gone idle, never a pane still
            // actively receiving samples — e.g. a bulk load whose rows all
            // share one event-time and take longer than the grace period to
            // ingest must not have its window force-closed mid-ingest.
            state
                .pane_wall_clock_last_touch_ms
                .insert(pane_start, now_ms);
            let updater = match state.active_panes.entry(pane_start) {
                std::collections::btree_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::btree_map::Entry::Vacant(e) => {
                    e.insert(create_accumulator_updater(&state.config)?)
                }
            };

            apply_sample(&mut **updater, series_key, *val, *ts, &state.config);
        }

        // Check for closed windows
        let closed = state
            .window_manager
            .closed_windows(closure_previous_wm, current_wm);

        for window_start in &closed {
            let (_, window_end) = state.window_manager.window_bounds(*window_start);
            let pane_starts = state.window_manager.panes_for_window(*window_start);

            if let Some(accumulator) = merge_panes_for_window(&mut state.active_panes, &pane_starts)
            {
                let accumulator = finalize_closed_accumulator(
                    accumulator,
                    &state.config,
                    &mut state.delta_set_previous_keys,
                )?;
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
        state.prune_pane_wall_clock_last_touch();

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
            .flat_map(|m| m.values())
            .map(|s| s.previous_watermark_ms)
            .filter(|&wm| wm != i64::MIN)
            .max()
            .unwrap_or(i64::MIN);

        // Step 2: Publish our worker watermark for cross-worker reads.
        self.worker_watermark.store(worker_wm, Ordering::Release);

        // Step 3: Compute global watermark = min(all worker watermarks).
        let global_wm = self.compute_global_watermark();

        // Sample the wall clock and grace period once for this flush cycle,
        // before borrowing `group_states` mutably below.
        let now_ms = (self.now_ms_fn)();
        let grace_ms = self.wall_clock_grace_period_ms;

        // Step 4: For each group, advance watermark and close due windows.
        let mut emit_batch: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> = Vec::new();

        for (agg_id, inner) in &mut self.group_states {
            for (group_key, state) in inner.iter_mut() {
                if state.previous_watermark_ms == i64::MIN {
                    continue; // No samples received yet — no panes to close.
                }

                // Effective watermark: max(group's own, global) + 1ms for boundary.
                let propagated_wm = if global_wm != i64::MIN {
                    state.previous_watermark_ms.max(global_wm)
                } else {
                    state.previous_watermark_ms
                };
                let mut effective_wm = propagated_wm.saturating_add(1);

                // Wall-clock fallback for stuck event-time. If every sample
                // carries the same timestamp (e.g. a one-shot batch where
                // all records fall in the same second), `previous_watermark_ms`
                // freezes and `closed_windows(prev, prev+1)` returns empty
                // forever — the window never closes and the store stays empty
                // even though data has been ingested. Force `effective_wm`
                // past `pane_start + window_size_ms` for any pane that has
                // gone *idle* — untouched by a sample — for `window_size +
                // grace` of WALL-CLOCK time. This deliberately does NOT
                // trigger for a pane still actively receiving samples (e.g. a
                // bulk load taking longer than the grace period to ingest):
                // `pane_wall_clock_last_touch_ms` is refreshed on every touch,
                // so only silence, not age, ages a pane out. Set
                // `wall_clock_grace_period_ms <= 0` to opt out and keep strict
                // event-time semantics.
                if grace_ms > 0 {
                    let window_size_ms = state.window_manager.window_size_ms();
                    for (&pane_start, &pane_last_touch_ms) in &state.pane_wall_clock_last_touch_ms {
                        if now_ms.saturating_sub(pane_last_touch_ms) >= window_size_ms + grace_ms {
                            let force_to = pane_start.saturating_add(window_size_ms);
                            if force_to > effective_wm {
                                effective_wm = force_to;
                            }
                        }
                    }
                }

                let closed = state
                    .window_manager
                    .closed_windows(state.previous_watermark_ms, effective_wm);

                for window_start in &closed {
                    let (_, window_end) = state.window_manager.window_bounds(*window_start);
                    let pane_starts = state.window_manager.panes_for_window(*window_start);

                    if let Some(accumulator) =
                        merge_panes_for_window(&mut state.active_panes, &pane_starts)
                    {
                        let accumulator = finalize_closed_accumulator(
                            accumulator,
                            &state.config,
                            &mut state.delta_set_previous_keys,
                        )?;
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
                // Monotonic advance — never retreat. Both the event-time
                // boundary `propagated_wm + 1` and the wall-clock fallback
                // only push `effective_wm` forward, so this is safe.
                if effective_wm > state.previous_watermark_ms {
                    state.previous_watermark_ms = effective_wm;
                }

                state.prune_pane_wall_clock_last_touch();
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

    /// Force-close every window still open on shutdown.
    ///
    /// Unlike `flush_all` — which only advances the watermark by `+1ms` (plus
    /// the wall-clock fallback, gated on grace having elapsed) — this emits the
    /// window for every remaining pane unconditionally, because no further
    /// samples will arrive once the engine is shutting down. Without it, a
    /// one-shot batch whose records all fall in a single window (so event-time
    /// never advances past the window end) would leave that window open forever
    /// and never write it to the store.
    ///
    /// To advance past the open windows we use a *finite* bound derived from
    /// the largest open pane (`max_pane + window_size_ms`) rather than
    /// `i64::MAX`: `WindowManager::closed_windows` enumerates window starts up
    /// to `current_wm` one slide at a time, so passing `i64::MAX` would loop
    /// ~`i64::MAX / slide` times and overflow. `max_pane + window_size_ms` is
    /// the smallest watermark that closes the latest open window.
    ///
    /// Idempotent: closed panes are drained from `active_panes` and their
    /// wall-clock bookkeeping is pruned, so a second call emits nothing.
    fn force_close_all(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.pass_raw_samples {
            return Ok(());
        }

        let mut emit_batch: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)> = Vec::new();

        for (agg_id, inner) in &mut self.group_states {
            for (group_key, state) in inner.iter_mut() {
                if state.previous_watermark_ms == i64::MIN {
                    continue; // never received data — nothing to close
                }
                // The latest window start equals the largest open pane start;
                // closing window `[start, start + size)` needs `wm >= start + size`.
                let Some(&max_pane) = state.active_panes.keys().next_back() else {
                    continue; // no open panes
                };
                let force_wm = max_pane.saturating_add(state.window_manager.window_size_ms());

                let closed = state
                    .window_manager
                    .closed_windows(state.previous_watermark_ms, force_wm);

                for window_start in &closed {
                    let (_, window_end) = state.window_manager.window_bounds(*window_start);
                    let pane_starts = state.window_manager.panes_for_window(*window_start);

                    if let Some(accumulator) =
                        merge_panes_for_window(&mut state.active_panes, &pane_starts)
                    {
                        let accumulator = finalize_closed_accumulator(
                            accumulator,
                            &state.config,
                            &mut state.delta_set_previous_keys,
                        )?;
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

                if force_wm > state.previous_watermark_ms {
                    state.previous_watermark_ms = force_wm;
                }
                state.prune_pane_wall_clock_last_touch();
            }
        }

        if !emit_batch.is_empty() {
            debug!(
                "Worker {} shutdown force-close emitting {} outputs",
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

/// Default wall-clock-now source: milliseconds since the unix epoch.
/// Used by `Worker::new`. Tests override via `set_now_ms_fn`.
fn default_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        // Pre-1970 wall clock (only happens if the host clock is grossly
        // misconfigured) — fall back to 0 so the fallback simply doesn't
        // trigger rather than panicking.
        .unwrap_or(0)
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
    // Parse the series labels at most once and share them across value resolution
    // and key extraction — both walk the same label string. Skip parsing entirely
    // when neither path needs labels (non-keyed accumulator with no value_column),
    // which is the common SUM/quantile case on the per-sample ingest hot path.
    let keyed = updater.is_keyed();
    let labels = if keyed || config.value_column.is_some() {
        parse_labels_from_series_key(series_key)
    } else {
        HashMap::new()
    };

    let value = resolve_sample_value(&labels, val, config);
    if keyed {
        let key = extract_aggregated_key_from_series(&labels, config);
        updater.update_keyed(&key, value, ts);
    } else {
        updater.update_single(value, ts);
    }
}

/// Resolve which scalar to aggregate for a sample.
///
/// The wire format collapses each sample to a single scalar (`wire_val`) plus a
/// set of labels carried in `series_key`. `wire_val` is whatever the source
/// considered the default value column (e.g. `pkt_len` for the netflow dataset).
/// But an aggregation may target a *different* column via `config.value_column`
/// — most importantly `COUNT(DISTINCT dstip)`, where the HLL sketch must hash
/// `dstip`, not the wire `pkt_len`.
///
/// Rule (general, self-correcting): if `config.value_column` names a column that
/// is present among the series labels, aggregate that label's value; otherwise
/// fall back to `wire_val`. Numeric value columns such as `pkt_len` are never
/// sent as labels, so SUM / quantile / top-k transparently keep using the wire
/// scalar, while label-present distinct targets such as `dstip` get substituted.
///
/// `labels` is the already-parsed label map for the sample's series key (parsed
/// once by `apply_sample` and shared with key extraction), so this is allocation-
/// free on the hot path.
///
/// The label value is parsed as `f64`. This is lossless for the netflow IPv4
/// `u32` columns (≤ 2^32 < 2^53) and consistent with the replay's int→f64
/// convention; cardinality only requires distinct inputs to map to distinct
/// hashes, so the exact hash need not match the baseline engine.
///
/// # Panics
/// Panics if `value_column` names a label whose value is not numeric. Non-numeric
/// distinct targets (e.g. `proto`) are not yet supported; this is a temporary
/// guard until the path is refactored to return a `Result` (see follow-up for a
/// byte/string hashing path).
fn resolve_sample_value(
    labels: &HashMap<&str, &str>,
    wire_val: f64,
    config: &AggregationConfig,
) -> f64 {
    let Some(col) = config.value_column.as_deref() else {
        return wire_val;
    };

    let Some(raw) = labels.get(col) else {
        return wire_val;
    };

    match raw.parse::<f64>() {
        Ok(v) => v,
        // Non-numeric distinct targets (e.g. COUNT(DISTINCT proto)) are not yet
        // supported: silently falling back to the wire value would produce an
        // INCORRECT aggregate, so fail loudly instead. This panic is a temporary
        // measure — the longer-term fix is to make this path return a `Result`
        // and propagate the error up to the caller.
        Err(_) => panic!(
            "value_column '{col}' label value {raw:?} is not numeric; non-numeric distinct \
             targets (e.g. COUNT(DISTINCT proto)) are not yet supported"
        ),
    }
}

/// Extract aggregated label values from a series key string.
/// These are the labels that form the key dimension *inside* keyed accumulators
/// (MultipleSum, CMS, HydraKLL), matching Arroyo's `agg_columns`.
fn extract_aggregated_key_from_series(
    labels: &HashMap<&str, &str>,
    config: &AggregationConfig,
) -> KeyByLabelValues {
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
            // Oldest pane: evict and MOVE the accumulator out (no clone).
            active_panes
                .remove(&ps)
                .map(|updater| updater.into_accumulator())
        } else {
            // Shared pane: non-destructive snapshot
            active_panes
                .get(&ps)
                .map(|updater| updater.snapshot_accumulator())
        };

        if let Some(acc) = pane_acc {
            merged = Some(match merged {
                None => acc,
                Some(existing) => match existing.merge_with(acc.as_ref()) {
                    Ok(merged) => merged,
                    Err(e) => {
                        warn!(
                            "Failed to merge pane at start={ps}: {e} -- keeping prior state, \
                             discarding this pane's contribution"
                        );
                        existing
                    }
                },
            });
        }
    }

    merged
}

/// Convert a closed DeltaSetAggregator population into the stateful delta
/// format used by the Arroyo implementation: keys newly present in this
/// window go in `added`, and keys absent from this window go in `removed`.
///
/// The updater can only collect the current window's observed keys. The
/// previous population therefore belongs to `GroupState`, which survives the
/// per-pane updater lifecycle and is isolated for each `(agg_id, group_key)`.
fn finalize_closed_accumulator(
    accumulator: Box<dyn AggregateCore>,
    config: &AggregationConfig,
    previous_delta_set_keys: &mut HashSet<KeyByLabelValues>,
) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
    if config.aggregation_type != AggregationType::DeltaSetAggregator {
        return Ok(accumulator);
    }

    let delta = accumulator
        .as_any()
        .downcast_ref::<DeltaSetAggregatorAccumulator>()
        .ok_or_else(|| {
            format!(
                "DeltaSetAggregator config produced {} instead of DeltaSetAggregatorAccumulator",
                accumulator.type_name()
            )
        })?;
    let current_keys: HashSet<KeyByLabelValues> = delta
        .get_keys()
        .ok_or("DeltaSetAggregator accumulator could not resolve its current keys")?
        .into_iter()
        .collect();

    let added = current_keys
        .difference(previous_delta_set_keys)
        .cloned()
        .collect();
    let removed = previous_delta_set_keys
        .difference(&current_keys)
        .cloned()
        .collect();
    *previous_delta_set_keys = current_keys;

    Ok(Box::new(DeltaSetAggregatorAccumulator::new_with_sets(
        added, removed,
    )))
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
    // resolve_sample_value: choose value_column label over the wire scalar
    // -----------------------------------------------------------------------

    #[test]
    fn resolve_sample_value_uses_label_when_value_column_present() {
        // COUNT(DISTINCT dstip): the wire scalar is pkt_len, but the HLL must
        // hash dstip, which is carried as a series label.
        let mut config = make_agg_config(
            4,
            "netflow_table",
            AggregationType::HLL,
            "",
            1,
            1,
            vec!["srcip"],
        );
        config.value_column = Some("dstip".to_string());
        let series = "netflow_table{srcip=\"10\",dstip=\"4242\",proto=\"TCP\"}";
        let labels = parse_labels_from_series_key(series);
        assert_eq!(resolve_sample_value(&labels, 1400.0, &config), 4242.0);
    }

    #[test]
    fn resolve_sample_value_falls_back_when_column_not_a_label() {
        // Numeric value columns like pkt_len are sent as the wire scalar, never
        // as a label, so resolution must transparently keep the wire value.
        let mut config = make_agg_config(
            1,
            "netflow_table",
            AggregationType::SingleSubpopulation,
            "Sum",
            1,
            1,
            vec!["srcip"],
        );
        config.value_column = Some("pkt_len".to_string());
        let series = "netflow_table{srcip=\"10\",dstip=\"4242\"}";
        let labels = parse_labels_from_series_key(series);
        assert_eq!(resolve_sample_value(&labels, 1400.0, &config), 1400.0);
    }

    #[test]
    fn resolve_sample_value_none_value_column_uses_wire_value() {
        let config = make_agg_config(
            1,
            "netflow_table",
            AggregationType::SingleSubpopulation,
            "Sum",
            1,
            1,
            vec!["srcip"],
        );
        let series = "netflow_table{srcip=\"10\",dstip=\"4242\"}";
        let labels = parse_labels_from_series_key(series);
        assert_eq!(resolve_sample_value(&labels, 1400.0, &config), 1400.0);
    }

    #[test]
    #[should_panic(expected = "is not numeric")]
    fn resolve_sample_value_non_numeric_label_panics() {
        // Non-numeric distinct targets are a follow-up; for now we fail loudly
        // rather than silently producing an incorrect aggregate.
        let mut config = make_agg_config(
            4,
            "netflow_table",
            AggregationType::HLL,
            "",
            1,
            1,
            vec!["srcip"],
        );
        config.value_column = Some("proto".to_string());
        let series = "netflow_table{srcip=\"10\",proto=\"TCP\"}";
        let labels = parse_labels_from_series_key(series);
        let _ = resolve_sample_value(&labels, 1400.0, &config);
    }

    #[test]
    fn hll_counts_distinct_value_column_not_wire_value() {
        use crate::precompute_operators::hll_accumulator::HllAccumulator;

        // COUNT(DISTINCT dstip) GROUP BY srcip, 1s tumbling window.
        let mut config = make_agg_config(
            4,
            "netflow_table",
            AggregationType::HLL,
            "",
            1000,
            1000,
            vec!["srcip"],
        );
        config.value_column = Some("dstip".to_string());
        let mut agg_configs = HashMap::new();
        agg_configs.insert(4, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            arc_configs(agg_configs),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
        );

        // Two samples, same srcip, DIFFERENT dstip, IDENTICAL wire value
        // (pkt_len). Window [0, 1000).
        worker
            .process_group_samples(
                4,
                "10",
                vec![
                    (
                        "netflow_table{srcip=\"10\",dstip=\"100\",proto=\"TCP\"}".to_string(),
                        100,
                        1400.0,
                    ),
                    (
                        "netflow_table{srcip=\"10\",dstip=\"200\",proto=\"TCP\"}".to_string(),
                        200,
                        1400.0,
                    ),
                ],
            )
            .unwrap();

        // Advance the watermark past the window end to close [0, 1000).
        worker
            .process_group_samples(
                4,
                "10",
                vec![(
                    "netflow_table{srcip=\"10\",dstip=\"300\",proto=\"TCP\"}".to_string(),
                    5000,
                    1400.0,
                )],
            )
            .unwrap();

        let captured = sink.drain();
        let (_output, acc) = captured
            .iter()
            .find(|(o, _)| o.start_timestamp == 0)
            .expect("window [0, 1000) should emit a closed HLL pane");
        let hll = acc
            .as_any()
            .downcast_ref::<HllAccumulator>()
            .expect("should be HllAccumulator");
        let est = hll.estimate();
        assert!(
            est > 1.5 && est < 3.0,
            "HLL should count 2 distinct dstip (got {est}); \
             with the bug it would count 1 distinct pkt_len"
        );
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    use crate::data_model::StreamingConfig;
    use crate::precompute_engine::config::LateDataPolicy;
    use crate::precompute_engine::output_sink::CapturingOutputSink;
    use crate::precompute_operators::datasketches_kll_accumulator::DatasketchesKLLAccumulator;
    use crate::precompute_operators::delta_set_aggregator_accumulator::DeltaSetAggregatorAccumulator;
    use crate::precompute_operators::multiple_sum_accumulator::MultipleSumAccumulator;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use asap_sketchlib::KllSketch;
    use asap_types::enums::{AggregationType, WindowType};

    fn make_agg_config(
        id: u64,
        metric: &str,
        agg_type: AggregationType,
        agg_sub_type: &str,
        window_size_ms: u64,
        slide_interval_ms: u64,
        grouping: Vec<&str>,
    ) -> AggregationConfig {
        make_agg_config_full(
            id,
            metric,
            agg_type,
            agg_sub_type,
            window_size_ms,
            slide_interval_ms,
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
        window_size_ms: u64,
        slide_interval_ms: u64,
        grouping: Vec<&str>,
        aggregated: Vec<&str>,
    ) -> AggregationConfig {
        let window_type = if slide_interval_ms == 0 || slide_interval_ms == window_size_ms {
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
            window_size_ms,
            slide_interval_ms,
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
                wall_clock_grace_period_ms: 0,
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
            10_000,
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

    #[test]
    fn test_delta_set_aggregator_emits_changes_relative_to_previous_window() {
        // Regression for the stateful DeltaSetAggregator contract: each
        // non-empty window emits keys added to or removed from the previous
        // window, matching asap-summary-ingest's Arroyo UDAF.
        let config = make_agg_config_full(
            2,
            "cpu",
            AggregationType::DeltaSetAggregator,
            "",
            1_000,
            1_000,
            vec![],
            vec!["host"],
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

        worker
            .process_group_samples(2, "", vec![("cpu{host=\"a\"}".to_string(), 100, 1.0)])
            .unwrap();
        worker
            .process_group_samples(2, "", vec![("cpu{host=\"b\"}".to_string(), 1_000, 1.0)])
            .unwrap();

        let first_window = sink
            .drain()
            .into_iter()
            .find(|(output, _)| output.start_timestamp == 0)
            .expect("first DeltaSetAggregator window should be emitted");
        let first_delta = first_window
            .1
            .as_any()
            .downcast_ref::<DeltaSetAggregatorAccumulator>()
            .expect("first output should be DeltaSetAggregatorAccumulator");
        let key_a = KeyByLabelValues::new_with_labels(vec!["a".to_string()]);
        assert!(first_delta.added.contains(&key_a));
        assert!(first_delta.removed.is_empty());

        worker
            .process_group_samples(2, "", vec![("cpu{host=\"b\"}".to_string(), 2_000, 1.0)])
            .unwrap();

        let second_window = sink
            .drain()
            .into_iter()
            .find(|(output, _)| output.start_timestamp == 1_000)
            .expect("second DeltaSetAggregator window should be emitted");
        let second_delta = second_window
            .1
            .as_any()
            .downcast_ref::<DeltaSetAggregatorAccumulator>()
            .expect("second output should be DeltaSetAggregatorAccumulator");
        let key_b = KeyByLabelValues::new_with_labels(vec!["b".to_string()]);
        assert!(second_delta.added.contains(&key_b));
        assert!(second_delta.removed.contains(&key_a));
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
            10_000,
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
            10_000,
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
            10_000,
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
            30_000,
            10_000,
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
            10_000,
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
    // Test: keyed accumulator with a numeric (non-label) value_column.
    // Regression guard for the value_column resolution change — existing keyed
    // aggregations (MultipleSum/top-k) set value_column to the wire scalar
    // (e.g. pkt_len), which is NOT carried as a label, so the wire value must
    // still flow through unchanged while the key comes from aggregated labels.
    // -----------------------------------------------------------------------

    #[test]
    fn test_keyed_accumulator_numeric_value_column_uses_wire_value() {
        // Like planner output for `SUM(pkt_len) GROUP BY dstip`:
        // grouping=[] (one output group), aggregated=[dstip] (key inside sketch),
        // value_column=pkt_len (the wire scalar, never sent as a label).
        let mut config = make_agg_config_full(
            5,
            "netflow_table",
            AggregationType::MultipleSubpopulation,
            "Sum",
            10_000,
            0,
            vec![],        // grouping: empty — one output group
            vec!["dstip"], // aggregated: dstip is the key INSIDE the sketch
        );
        config.value_column = Some("pkt_len".to_string());
        let mut agg_configs = HashMap::new();
        agg_configs.insert(5, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            arc_configs(agg_configs),
            sink.clone(),
            false,
            0,
            LateDataPolicy::Drop,
        );

        // Two samples with the same dstip and one with a different dstip. The
        // summed values are the WIRE scalars (100+200 for A, 50 for B); if
        // resolution wrongly substituted dstip, the sums would be garbage.
        worker
            .process_group_samples(
                5,
                "",
                vec![
                    ("netflow_table{dstip=\"A\"}".to_string(), 1000, 100.0),
                    ("netflow_table{dstip=\"A\"}".to_string(), 2000, 200.0),
                    ("netflow_table{dstip=\"B\"}".to_string(), 3000, 50.0),
                ],
            )
            .unwrap();

        // Close the window [0, 10000).
        worker
            .process_group_samples(
                5,
                "",
                group_samples("netflow_table{dstip=\"A\"}", vec![(10000, 0.0)]),
            )
            .unwrap();

        let captured = sink.drain();
        assert_eq!(captured.len(), 1, "one group → one output");

        let (_output, acc) = &captured[0];
        let ms_acc = acc
            .as_any()
            .downcast_ref::<MultipleSumAccumulator>()
            .expect("should be MultipleSumAccumulator");

        assert_eq!(
            ms_acc.sums.len(),
            2,
            "two dstip keys inside one accumulator"
        );

        let mut found_a = false;
        let mut found_b = false;
        for (key, &sum) in &ms_acc.sums {
            if key.labels == vec!["A".to_string()] {
                assert!(
                    (sum - 300.0).abs() < 1e-10,
                    "dstip=A must sum the wire pkt_len values (100+200), got {sum}"
                );
                found_a = true;
            }
            if key.labels == vec!["B".to_string()] {
                assert!(
                    (sum - 50.0).abs() < 1e-10,
                    "dstip=B must sum the wire pkt_len value (50), got {sum}"
                );
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
            10_000,
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
            10_000,
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
            10_000,
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
            10_000,
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
                wall_clock_grace_period_ms: 0,
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
            10_000,
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
                wall_clock_grace_period_ms: 0,
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

    #[test]
    fn test_late_data_forward_to_store_drops_delta_set_aggregator() {
        let config = make_agg_config_full(
            6,
            "cpu",
            AggregationType::DeltaSetAggregator,
            "",
            10_000,
            0,
            vec![],
            vec!["host"],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(6, config);

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
                wall_clock_grace_period_ms: 0,
            },
            Arc::new(AtomicUsize::new(0)),
            wm.clone(),
            vec![wm],
        );

        worker
            .process_group_samples(6, "", group_samples("cpu{host=\"a\"}", vec![(500, 1.0)]))
            .unwrap();
        worker
            .process_group_samples(6, "", group_samples("cpu{host=\"a\"}", vec![(20_000, 0.0)]))
            .unwrap();
        let _ = sink.drain();

        worker
            .process_group_samples(6, "", group_samples("cpu{host=\"b\"}", vec![(8_000, 55.0)]))
            .unwrap();

        assert_eq!(
            sink.len(),
            0,
            "ForwardToStore must drop late DeltaSetAggregator samples"
        );
    }

    #[test]
    fn test_delta_set_aggregator_keeps_on_time_samples_in_first_multi_window_batch() {
        let config = make_agg_config_full(
            7,
            "cpu",
            AggregationType::DeltaSetAggregator,
            "",
            10_000,
            0,
            vec![],
            vec!["host"],
        );
        let mut agg_configs = HashMap::new();
        agg_configs.insert(7, config);

        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker(
            arc_configs(agg_configs),
            sink.clone(),
            false,
            0,
            LateDataPolicy::ForwardToStore,
        );

        worker
            .process_group_samples(
                7,
                "",
                vec![
                    ("cpu{host=\"a\"}".to_string(), 500, 1.0),
                    ("cpu{host=\"b\"}".to_string(), 20_000, 1.0),
                ],
            )
            .unwrap();
        worker
            .process_group_samples(7, "", vec![("cpu{host=\"b\"}".to_string(), 30_000, 1.0)])
            .unwrap();

        let first_window = sink
            .drain()
            .into_iter()
            .find(|(output, _)| output.start_timestamp == 0)
            .expect("first DeltaSetAggregator window should be emitted");
        let first_delta = first_window
            .1
            .as_any()
            .downcast_ref::<DeltaSetAggregatorAccumulator>()
            .expect("first output should be DeltaSetAggregatorAccumulator");
        let key_a = KeyByLabelValues::new_with_labels(vec!["a".to_string()]);
        assert!(
            first_delta.added.contains(&key_a),
            "on-time key in the first batch must not be treated as late"
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
  windowSizeMs: 10000
  windowType: tumbling
  slideIntervalMs: 0
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
            10_000,
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
                wall_clock_grace_period_ms: 0,
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
                wall_clock_grace_period_ms: 0,
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
                wall_clock_grace_period_ms: 0,
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
            10_000,
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
                wall_clock_grace_period_ms: 0,
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

    // -----------------------------------------------------------------------
    // Test: UpdateAggConfigs enables processing of a new aggregation at runtime
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_update_agg_configs_enables_new_aggregation_at_runtime() {
        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10_000, // 10s tumbling window
            0,
            vec![],
        );

        let sink = Arc::new(CapturingOutputSink::new());
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let wm = Arc::new(AtomicI64::new(i64::MIN));
        // Start worker with NO agg configs — it doesn't know about agg_id=1 yet.
        let worker = Worker::new(
            0,
            rx,
            sink.clone(),
            HashMap::new(),
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms: 0,
                pass_raw_samples: false,
                raw_mode_aggregation_id: 0,
                late_data_policy: LateDataPolicy::Drop,
                wall_clock_grace_period_ms: 0,
            },
            Arc::new(AtomicUsize::new(0)),
            wm.clone(),
            vec![wm],
        );
        let handle = tokio::spawn(async move { worker.run().await });

        // Sample arrives before config update — silently ignored (agg_id unknown).
        tx.send(WorkerMessage::GroupSamples {
            agg_id: 1,
            group_key: String::new(),
            samples: vec![("cpu".to_string(), 1_000, 1.0)],
            ingest_received_at: std::time::Instant::now(),
        })
        .await
        .unwrap();

        // Push the new agg config at runtime.
        let mut new_configs = HashMap::new();
        new_configs.insert(1, Arc::new(config));
        tx.send(WorkerMessage::UpdateAggConfigs(new_configs))
            .await
            .unwrap();

        // Post-update samples — should be processed now.
        tx.send(WorkerMessage::GroupSamples {
            agg_id: 1,
            group_key: String::new(),
            samples: vec![("cpu".to_string(), 5_000, 2.0)],
            ingest_received_at: std::time::Instant::now(),
        })
        .await
        .unwrap();
        // t=10_000 closes window [0, 10_000).
        tx.send(WorkerMessage::GroupSamples {
            agg_id: 1,
            group_key: String::new(),
            samples: vec![("cpu".to_string(), 10_000, 0.0)],
            ingest_received_at: std::time::Instant::now(),
        })
        .await
        .unwrap();

        tx.send(WorkerMessage::Shutdown).await.unwrap();
        handle.await.unwrap();

        let mut captured = sink.drain();
        // Two windows close:
        //  1. [0, 10_000) — closed inline when the t=10_000 sample advanced
        //     the watermark; contains only the post-update t=5_000 sample.
        //  2. [10_000, 20_000) — left open by the watermark but force-closed on
        //     shutdown; contains the t=10_000 sample. Before the shutdown
        //     force-close this trailing window was silently lost.
        captured.sort_by_key(|(o, _)| o.start_timestamp);
        assert_eq!(
            captured.len(),
            2,
            "window [0,10_000) closes inline; [10_000,20_000) force-closes on shutdown"
        );

        let (output, acc) = &captured[0];
        assert_eq!(output.aggregation_id, 1);
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 10_000);

        let sum_acc = acc
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("should be SumAccumulator");
        // Pre-update sample (t=1000, val=1.0) was dropped — agg_id was unknown.
        // Post-update sample (t=5000, val=2.0) is the only one in this window.
        assert!(
            (sum_acc.sum - 2.0).abs() < 1e-10,
            "only post-update sample should be aggregated, got {}",
            sum_acc.sum
        );

        // The trailing window force-closed on shutdown holds the t=10_000
        // sample (val=0.0).
        let (trailing_output, trailing_acc) = &captured[1];
        assert_eq!(trailing_output.start_timestamp, 10_000);
        assert_eq!(trailing_output.end_timestamp, 20_000);
        let trailing_sum = trailing_acc
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("should be SumAccumulator");
        assert!(
            trailing_sum.sum.abs() < 1e-10,
            "trailing window should hold the t=10_000 sample (val=0.0), got {}",
            trailing_sum.sum
        );
    }

    // -----------------------------------------------------------------------
    // Test: removing an agg_id force-closes its open windows with a finite
    // bound — at realistic (epoch-ms) timestamps.
    //
    // The removed-agg cleanup used to advance the watermark to `i64::MAX`.
    // `closed_windows` enumerates window starts one slide at a time, so with a
    // real epoch-ms watermark that loop runs ~`i64::MAX / slide` iterations and
    // overflows `start + window_size_ms` (panics in debug) — the bug the
    // reviewer flagged. With the finite `max_pane + window_size_ms` bound this
    // completes instantly and emits exactly the open window.
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_update_agg_configs_removed_id_force_closes_at_epoch_ms_timestamps() {
        // Realistic event time: ~2023-11-14T22:13:20Z in ms. With the old
        // i64::MAX code this is what made closed_windows blow up.
        let base_ms: i64 = 1_700_000_000_000;
        let window_size_ms = 10_000u64;

        let config = make_agg_config(
            1,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            window_size_ms,
            0,
            vec![],
        );

        let sink = Arc::new(CapturingOutputSink::new());
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let wm = Arc::new(AtomicI64::new(i64::MIN));
        let mut agg_configs = HashMap::new();
        agg_configs.insert(1u64, Arc::new(config));
        let worker = Worker::new(
            0,
            rx,
            sink.clone(),
            agg_configs,
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms: 0,
                pass_raw_samples: false,
                raw_mode_aggregation_id: 0,
                late_data_policy: LateDataPolicy::Drop,
                wall_clock_grace_period_ms: 0,
            },
            Arc::new(AtomicUsize::new(0)),
            wm.clone(),
            vec![wm],
        );
        let handle = tokio::spawn(async move { worker.run().await });

        // Open a window at a large epoch-ms timestamp; nothing advances the
        // watermark past it, so it's still open when the agg_id is removed.
        tx.send(WorkerMessage::GroupSamples {
            agg_id: 1,
            group_key: String::new(),
            samples: vec![("cpu".to_string(), base_ms, 7.0)],
            ingest_received_at: std::time::Instant::now(),
        })
        .await
        .unwrap();

        // Remove agg_id=1 from the config → triggers the removed-agg force-close.
        tx.send(WorkerMessage::UpdateAggConfigs(HashMap::new()))
            .await
            .unwrap();

        tx.send(WorkerMessage::Shutdown).await.unwrap();
        // If the finite bound regressed to i64::MAX this join would hang or
        // panic on overflow instead of completing.
        handle.await.unwrap();

        let captured = sink.drain();
        assert_eq!(
            captured.len(),
            1,
            "removing the agg_id must force-close the single open window"
        );
        let (output, acc) = &captured[0];
        let window_ms = window_size_ms as i64;
        let expected_start = base_ms - (base_ms % window_ms);
        assert_eq!(output.start_timestamp as i64, expected_start);
        assert_eq!(output.end_timestamp as i64, expected_start + window_ms);
        let sum_acc = acc
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("should be SumAccumulator");
        assert!(
            (sum_acc.sum - 7.0).abs() < 1e-10,
            "force-closed window should hold the ingested sample (val=7.0), got {}",
            sum_acc.sum
        );
    }

    // -----------------------------------------------------------------------
    // Test: wall-clock fallback closes an idle window when event-time freezes
    //
    // Reproduces the one-shot-batch failure mode: every record falls in the
    // same window and no later timestamp ever arrives to advance the
    // watermark, so `flush_all`'s `+1ms` event-time advance is a no-op and the
    // window never closes — the store stays empty. The wall-clock fallback
    // force-closes the pane once it has been alive for `window_size + grace`
    // of wall-clock time. Uses an injected fake clock so it runs in
    // microseconds instead of sleeping for real seconds.
    // -----------------------------------------------------------------------

    /// Build a Worker identical to the inline test setups but with an explicit
    /// `wall_clock_grace_period_ms`. Test-local helper.
    fn make_worker_with_grace(
        agg_configs: HashMap<u64, AggregationConfig>,
        sink: Arc<CapturingOutputSink>,
        wall_clock_grace_period_ms: i64,
    ) -> Worker {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let wm = Arc::new(AtomicI64::new(i64::MIN));
        Worker::new(
            0,
            rx,
            sink,
            arc_configs(agg_configs),
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                // 1, not 0: every flush_all() call unconditionally nudges the
                // event-time watermark forward by 1ms (the "+1ms boundary
                // advance" that lets an idle stream make progress),
                // independent of the wall-clock fallback these tests target.
                // A test that calls flush_all() and then processes another
                // same-timestamp touch would otherwise have that 1ms of
                // drift alone mark the touch "late" and drop it via a wholly
                // different code path. 1ms is the exact amount one
                // intervening flush contributes, not an arbitrary buffer.
                allowed_lateness_ms: 1,
                pass_raw_samples: false,
                raw_mode_aggregation_id: 0,
                late_data_policy: LateDataPolicy::Drop,
                wall_clock_grace_period_ms,
            },
            Arc::new(AtomicUsize::new(0)),
            wm.clone(),
            vec![wm],
        )
    }

    #[test]
    fn wall_clock_fallback_closes_idle_window() {
        // 10s tumbling window.
        let cfg = make_agg_config(
            7,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10_000,
            0,
            vec![],
        );
        let agg_configs = HashMap::from([(7, cfg)]);
        let sink = Arc::new(CapturingOutputSink::new());
        // 5s grace period — production default.
        let mut worker = make_worker_with_grace(agg_configs, sink.clone(), 5_000);

        // Pin wall-clock at t_wall=1_000_000ms during ingest. Every sample
        // carries the SAME frozen event-time (t_event=0), so the watermark
        // never advances past the window.
        let wall_clock = Arc::new(AtomicI64::new(1_000_000));
        let wc_clone = wall_clock.clone();
        worker.set_now_ms_fn(Box::new(move || wc_clone.load(Ordering::Relaxed)));

        for i in 0..10 {
            worker
                .process_group_samples(7, "", group_samples("cpu", vec![(0, 1.0 + i as f64)]))
                .expect("ingest must accept frozen-event-time samples");
        }
        assert_eq!(
            sink.len(),
            0,
            "no output yet: event-time hasn't advanced past the window"
        );

        // Flush at the same wall-clock time (pane just born). Fallback must
        // NOT trigger — pane is younger than window_size + grace = 15s.
        worker.flush_all().unwrap();
        assert_eq!(
            sink.len(),
            0,
            "flush at t_wall=pane_birth must not close the window"
        );

        // Advance wall-clock by exactly window_size + grace = 15s. Now the
        // pane is old enough that the fallback must close and emit its window,
        // even though event-time is still pinned at 0.
        wall_clock.store(1_000_000 + 10_000 + 5_000, Ordering::Relaxed);
        worker.flush_all().unwrap();

        let captured = sink.drain();
        assert_eq!(
            captured.len(),
            1,
            "wall-clock fallback must close the idle window once wall-clock age \
             exceeds window_size + grace"
        );
        let (output, _acc) = &captured[0];
        assert_eq!(output.aggregation_id, 7);
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 10_000);

        // Idempotent: a window closed by the fallback drains its pane and its
        // wall-clock bookkeeping, so a subsequent flush must not re-emit.
        worker.flush_all().unwrap();
        assert_eq!(sink.len(), 0, "already-closed window must not re-emit");
    }

    // Regression test for issue #474: a bulk one-shot load whose rows all
    // share one event-time can take longer than `window_size_ms +
    // wall_clock_grace_period_ms` to ingest. Before the fix, the wall-clock
    // fallback measured age since a pane was *first* touched, so it force-
    // closed the window mid-ingest and every sample arriving afterward hit
    // the (hardcoded-Drop) late-data path — a silent, uniform undercount.
    // The fix refreshes the pane's wall-clock bookkeeping on every touch, so
    // the fallback only fires once a pane has gone genuinely idle.
    #[test]
    fn wall_clock_fallback_does_not_close_a_pane_still_receiving_samples() {
        // Production defaults: 1s tumbling window, 5s grace ⇒ old birth-time
        // deadline was window_size + grace = 6s.
        let cfg = make_agg_config(
            7,
            "netflow_bytes",
            AggregationType::SingleSubpopulation,
            "Sum",
            1_000,
            0,
            vec![],
        );
        let agg_configs = HashMap::from([(7, cfg)]);
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker_with_grace(agg_configs, sink.clone(), 5_000);

        let wall_clock = Arc::new(AtomicI64::new(1_000_000));
        let wc_clone = wall_clock.clone();
        worker.set_now_ms_fn(Box::new(move || wc_clone.load(Ordering::Relaxed)));

        // Every sample shares event-time 0 (the degenerate bulk-load case),
        // but the pane is touched once per simulated wall-clock second for
        // 7 seconds straight — actively receiving data the whole time.
        let mut expected_sum = 0.0;
        for i in 0..7 {
            wall_clock.store(1_000_000 + i * 1_000, Ordering::Relaxed);
            let val = 1.0 + i as f64;
            expected_sum += val;
            worker
                .process_group_samples(7, "", group_samples("netflow_bytes", vec![(0, val)]))
                .expect("ingest must accept frozen-event-time samples");
        }

        // Flush at wall_clock = birth(1_000_000) + 6_500ms — past the OLD
        // birth-time deadline of birth + window_size + grace = 1_006_000,
        // but the pane was last touched at 1_006_000 (i=6), only 500ms ago.
        // A pane still this fresh must not be force-closed.
        wall_clock.store(1_000_000 + 6_500, Ordering::Relaxed);
        worker.flush_all().unwrap();
        assert_eq!(
            sink.len(),
            0,
            "a pane still actively receiving samples must not be force-closed \
             just because it has been open longer than window_size + grace"
        );

        // Ingest continues past the mid-ingest check: an 8th touch lands at
        // wall_clock = 1_007_000, refreshing last-touch again.
        wall_clock.store(1_000_000 + 7_000, Ordering::Relaxed);
        let val = 1.0 + 7_f64;
        expected_sum += val;
        worker
            .process_group_samples(7, "", group_samples("netflow_bytes", vec![(0, val)]))
            .expect("ingest must accept frozen-event-time samples");

        // Ingest stops after the 8th touch (wall_clock = 1_007_000). Advance
        // past last_touch + window_size + grace and flush: NOW the fallback
        // must close the window, with every sample's contribution intact.
        wall_clock.store(1_000_000 + 7_000 + 1_000 + 5_000 + 1, Ordering::Relaxed);
        worker.flush_all().unwrap();

        let captured = sink.drain();
        assert_eq!(
            captured.len(),
            1,
            "pane must close once it actually goes idle, with no data lost \
             from any of the 8 touches"
        );
        let (output, acc) = &captured[0];
        assert_eq!(output.aggregation_id, 7);
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 1_000);
        let sum_acc = acc
            .as_any()
            .downcast_ref::<SumAccumulator>()
            .expect("Sum aggregation should emit a SumAccumulator");
        assert!(
            (sum_acc.sum - expected_sum).abs() < 1e-9,
            "expected all 8 touches merged: expected {}, got {}",
            expected_sum,
            sum_acc.sum
        );
    }

    #[test]
    fn wall_clock_fallback_disabled_preserves_event_time_only_semantics() {
        let cfg = make_agg_config(
            7,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10_000,
            0,
            vec![],
        );
        let agg_configs = HashMap::from([(7, cfg)]);
        let sink = Arc::new(CapturingOutputSink::new());
        // grace=0 disables the fallback entirely.
        let mut worker = make_worker_with_grace(agg_configs, sink.clone(), 0);

        let wall_clock = Arc::new(AtomicI64::new(1_000_000));
        let wc_clone = wall_clock.clone();
        worker.set_now_ms_fn(Box::new(move || wc_clone.load(Ordering::Relaxed)));

        worker
            .process_group_samples(7, "", group_samples("cpu", vec![(0, 42.0)]))
            .unwrap();

        // Even after a wall-clock eternity, grace=0 keeps strict event-time
        // semantics — the window never closes because event-time is frozen.
        wall_clock.store(1_000_000 + 86_400_000, Ordering::Relaxed); // +24h
        worker.flush_all().unwrap();
        assert_eq!(
            sink.len(),
            0,
            "grace=0 must disable the fallback — event-time-only semantics"
        );
    }

    // -----------------------------------------------------------------------
    // Test: shutdown force-close emits the trailing window
    //
    // The immediate-shutdown batch case: every record falls in one window and
    // no later timestamp ever advances the watermark, so flush_all (with the
    // wall-clock fallback disabled, grace=0) leaves the window open. On
    // shutdown, force_close_all must close and emit it so the data reaches the
    // store instead of being lost.
    // -----------------------------------------------------------------------

    #[test]
    fn shutdown_force_close_emits_trailing_window() {
        // 10s tumbling window; grace=0 isolates the force-close from the
        // wall-clock fallback.
        let cfg = make_agg_config(
            7,
            "cpu",
            AggregationType::SingleSubpopulation,
            "Sum",
            10_000,
            0,
            vec![],
        );
        let agg_configs = HashMap::from([(7, cfg)]);
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = make_worker_with_grace(agg_configs, sink.clone(), 0);

        // All samples land in window [0, 10_000); the watermark freezes below
        // the window end because no later timestamp ever arrives.
        for i in 0..5 {
            worker
                .process_group_samples(
                    7,
                    "",
                    group_samples("cpu", vec![(1_000 + i as i64 * 100, 1.0)]),
                )
                .unwrap();
        }

        // A final flush must NOT close the window (event-time frozen, fallback
        // disabled) — this is the bug the force-close fixes.
        worker.flush_all().unwrap();
        assert_eq!(
            sink.len(),
            0,
            "trailing window must remain open after the final flush"
        );

        // Shutdown force-close closes and emits the trailing window.
        worker.force_close_all().unwrap();
        let captured = sink.drain();
        assert_eq!(
            captured.len(),
            1,
            "shutdown force-close must emit the trailing window"
        );
        let (output, _acc) = &captured[0];
        assert_eq!(output.aggregation_id, 7);
        assert_eq!(output.start_timestamp, 0);
        assert_eq!(output.end_timestamp, 10_000);

        // Idempotent: panes are drained, so a second force-close emits nothing.
        worker.force_close_all().unwrap();
        assert_eq!(
            sink.len(),
            0,
            "force-close must be idempotent once panes are drained"
        );
    }
}
