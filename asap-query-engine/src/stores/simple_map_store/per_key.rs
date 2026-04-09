use crate::data_model::{AggregateCore, CleanupPolicy, PrecomputedOutput, StreamingConfig};
use crate::stores::simple_map_store::common::{
    EpochID, InternTable, MetricBucketMap, MetricID, MutableEpoch, SealedEpoch, TimestampRange,
};
use crate::stores::{Store, StoreResult, TimestampedBucketsMap};
use dashmap::DashMap;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use tracing::{debug, error, info};

type StoreKey = u64; // aggregation_id

/// Per-aggregation_id data protected by RwLock
struct StoreKeyData {
    /// Label interning table (Optimization 1)
    intern: InternTable,

    /// Active epoch — always present, accepts inserts.
    current_epoch: MutableEpoch,

    /// Sealed (immutable) epochs stored as flat sorted Vecs (Optimization 2).
    sealed_epochs: BTreeMap<EpochID, SealedEpoch>,

    /// Monotonically increasing ID of the current epoch.
    current_epoch_id: EpochID,

    /// Max distinct time-windows per epoch before sealing.
    /// None = unlimited (set on first insert from num_aggregates_to_retain).
    epoch_capacity: Option<usize>,

    /// Max total epochs (1 current + sealed) to retain before dropping the oldest.
    max_epochs: usize,

    /// Track how many times each timestamp range has been read.
    /// Behind Mutex so range queries can use a read lock on the outer RwLock.
    read_counts: Mutex<HashMap<TimestampRange, u64>>,
}

impl StoreKeyData {
    fn new() -> Self {
        Self {
            intern: InternTable::new(),
            current_epoch: MutableEpoch::new(),
            sealed_epochs: BTreeMap::new(),
            current_epoch_id: 0,
            epoch_capacity: None,
            max_epochs: 4,
            read_counts: Mutex::new(HashMap::new()),
        }
    }

    /// Set epoch_capacity on first insert (no-op after first call).
    fn configure_epochs(&mut self, num_aggregates_to_retain: Option<u64>) {
        if self.epoch_capacity.is_none() {
            if let Some(cap) = num_aggregates_to_retain {
                self.epoch_capacity = Some(cap as usize);
            }
        }
    }

    /// Seal the current epoch when full, then evict the minimum number of oldest windows
    /// to keep total distinct windows ≤ `epoch_capacity * max_epochs`.
    ///
    /// Matches legacy per-window eviction semantics: only the exact number of windows
    /// needed to reach the retention limit are removed, which may be fewer than a full epoch.
    fn maybe_rotate_epoch(&mut self) {
        let capacity = match self.epoch_capacity {
            Some(c) if c > 0 => c,
            _ => return, // unlimited
        };
        let retention_limit = capacity * self.max_epochs;

        // Step 1: seal current epoch if it has hit the window capacity threshold.
        if self.current_epoch.window_count() >= capacity {
            let hint = self.current_epoch.len();
            let old = std::mem::replace(&mut self.current_epoch, MutableEpoch::with_capacity(hint));
            self.sealed_epochs.insert(self.current_epoch_id, old.seal());
            self.current_epoch_id += 1;
        }

        // Step 2: evict oldest windows until total distinct windows ≤ retention_limit.
        // Uses O(E) distinct_window_count() calls (E ≤ max_epochs, a small constant).
        let total: usize = self.current_epoch.window_count()
            + self
                .sealed_epochs
                .values()
                .map(|e| e.distinct_window_count())
                .sum::<usize>();

        if total <= retention_limit {
            return;
        }
        let mut over = total - retention_limit;

        while over > 0 {
            let oldest_id = match self.sealed_epochs.keys().next().copied() {
                Some(id) => id,
                None => break,
            };
            let oldest_windows = self.sealed_epochs[&oldest_id].unique_windows();
            let n_evict = over.min(oldest_windows.len());
            let to_remove = oldest_windows[..n_evict].to_vec();
            over -= n_evict;

            {
                let read_counts = self.read_counts.get_mut().unwrap();
                for w in &to_remove {
                    read_counts.remove(w);
                }
            }
            if n_evict == oldest_windows.len() {
                self.sealed_epochs.remove(&oldest_id);
            } else {
                self.sealed_epochs
                    .get_mut(&oldest_id)
                    .unwrap()
                    .remove_windows(&to_remove);
            }
        }
    }

    /// Apply ReadBased cleanup across current and sealed epochs.
    fn cleanup_read_based(&mut self, metric: &str, aggregation_id: u64, threshold: u64) {
        let read_counts = self.read_counts.get_mut().unwrap();

        let windows_to_remove: Vec<TimestampRange> = read_counts
            .iter()
            .filter(|(_, &count)| count >= threshold)
            .map(|(range, _)| *range)
            .collect();

        if windows_to_remove.is_empty() {
            return;
        }

        for window in &windows_to_remove {
            debug!(
                "Removed aggregate for {} aggregation_id {} window {}-{} (read_count >= threshold: {})",
                metric, aggregation_id, window.0, window.1, threshold
            );
            read_counts.remove(window);
        }

        // Remove from current epoch.
        self.current_epoch.remove_windows(&windows_to_remove);

        // Remove from sealed epochs; drop any that become empty.
        self.sealed_epochs.retain(|_, epoch| {
            epoch.remove_windows(&windows_to_remove);
            !epoch.is_empty()
        });
    }
}

/// In-memory storage implementation using per-key locks for concurrency
pub struct SimpleMapStorePerKey {
    // Lock-free concurrent outer map - per aggregation_id
    store: DashMap<StoreKey, Arc<RwLock<StoreKeyData>>>,

    // Separate concurrent maps for global state
    earliest_timestamps: DashMap<u64, AtomicU64>,
    metrics: DashMap<String, ()>, // HashSet equivalent
    items_inserted: DashMap<String, AtomicU64>,

    // Store the streaming configuration
    streaming_config: Arc<StreamingConfig>,

    // Policy for cleaning up old aggregates
    cleanup_policy: CleanupPolicy,
}

impl SimpleMapStorePerKey {
    pub fn new(streaming_config: Arc<StreamingConfig>, cleanup_policy: CleanupPolicy) -> Self {
        Self {
            store: DashMap::new(),
            earliest_timestamps: DashMap::new(),
            metrics: DashMap::new(),
            items_inserted: DashMap::new(),
            streaming_config,
            cleanup_policy,
        }
    }

    /// Collect diagnostic info about store contents.
    pub fn diagnostic_info(&self) -> super::StoreDiagnostics {
        use super::{AggregationDiagnostic, StoreDiagnostics};

        let mut per_aggregation = Vec::new();
        let mut total_time_map_entries: usize = 0;
        let total_sketch_bytes: usize = 0;

        for entry in self.store.iter() {
            let agg_id = *entry.key();
            let data = match entry.value().read() {
                Ok(d) => d,
                Err(_) => continue,
            };
            let time_map_len = data.current_epoch.window_count()
                + data
                    .sealed_epochs
                    .values()
                    .map(|e| e.distinct_window_count())
                    .sum::<usize>();
            let read_counts_len = data.read_counts.lock().map(|rc| rc.len()).unwrap_or(0);
            total_time_map_entries += time_map_len;

            let num_aggregate_objects = data.current_epoch.len()
                + data.sealed_epochs.values().map(|e| e.entries.len()).sum::<usize>();

            per_aggregation.push(AggregationDiagnostic {
                aggregation_id: agg_id,
                time_map_len,
                read_counts_len,
                num_aggregate_objects,
                sketch_bytes: 0, // skip serialization for diagnostics
            });
        }

        StoreDiagnostics {
            num_aggregations: self.store.len(),
            total_time_map_entries,
            total_sketch_bytes,
            per_aggregation,
        }
    }

    fn cleanup_old_aggregates(
        &self,
        data: &mut StoreKeyData,
        metric: &str,
        aggregation_id: u64,
        num_aggregates_to_retain: Option<u64>,
        read_count_threshold: Option<u64>,
    ) {
        match self.cleanup_policy {
            CleanupPolicy::CircularBuffer => {
                // configure_epochs was already called before insert;
                // rotation is handled by maybe_rotate_epoch after each insert batch.
                // Nothing additional needed here.
                let _ = (num_aggregates_to_retain, metric, aggregation_id);
            }
            CleanupPolicy::ReadBased => {
                if let Some(threshold) = read_count_threshold {
                    data.cleanup_read_based(metric, aggregation_id, threshold);
                }
            }
            CleanupPolicy::NoCleanup => {
                // Do nothing - no cleanup
            }
        }
    }

    fn insert_for_store_key(
        &self,
        store_key: &StoreKey,
        metric: &str,
        items: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> StoreResult<()> {
        let aggregation_id = *store_key;
        let metric_key = metric.to_string();
        let inserted_delta = items.len() as u64;

        // Opt 4: compute batch minimum timestamp before acquiring any lock.
        // Collapses N per-item atomic fetch_min calls into one (Opt 4).
        let batch_min_ts = items
            .iter()
            .map(|(o, _)| o.start_timestamp)
            .min()
            .unwrap_or(u64::MAX);

        // Measure lock acquisition time
        #[cfg(feature = "lock_profiling")]
        let lock_wait_start = Instant::now();

        // Get or create the store data for this key
        let store_data_lock = self
            .store
            .entry(*store_key)
            .or_insert_with(|| Arc::new(RwLock::new(StoreKeyData::new())));

        #[cfg(feature = "lock_profiling")]
        {
            let lock_wait_duration = lock_wait_start.elapsed();
            info!(
                "🔒 Insert DashMap get time: {:.2}ms (metric: {}, agg_id: {}, items: {})",
                lock_wait_duration.as_secs_f64() * 1000.0,
                metric,
                *store_key,
                items.len()
            );
        }

        #[cfg(feature = "lock_profiling")]
        let rwlock_wait_start = Instant::now();

        // Acquire write lock for this aggregation_id only
        let mut data = store_data_lock.write().map_err(|e| {
            format!(
                "Failed to acquire write lock for aggregation_id {}: {}",
                store_key, e
            )
        })?;

        #[cfg(feature = "lock_profiling")]
        {
            let rwlock_wait_duration = rwlock_wait_start.elapsed();
            info!(
                "🔒 Insert RwLock wait time: {:.2}ms (metric: {}, agg_id: {}, items: {})",
                rwlock_wait_duration.as_secs_f64() * 1000.0,
                metric,
                *store_key,
                items.len()
            );
        }

        #[cfg(feature = "lock_profiling")]
        let lock_hold_start = Instant::now();

        // Create metric if needed (lock-free DashMap insert)
        self.metrics.entry(metric_key.clone()).or_insert(());

        // Opt 4: one atomic earliest-ts update per batch using the pre-computed minimum.
        // Replaces N per-item fetch_min calls with a single one.
        self.earliest_timestamps
            .entry(aggregation_id)
            .and_modify(|earliest| {
                earliest.fetch_min(batch_min_ts, Ordering::Relaxed);
            })
            .or_insert_with(|| AtomicU64::new(batch_min_ts));

        // Update insertion counter once per grouped batch (instead of once per item).
        let items_inserted_counter = self
            .items_inserted
            .entry(metric_key)
            .or_insert_with(|| AtomicU64::new(0));
        let previous_total = items_inserted_counter.fetch_add(inserted_delta, Ordering::Relaxed);
        let new_total = previous_total + inserted_delta;
        if new_total / 1000 > previous_total / 1000 {
            debug!("Inserted {} items into {}", new_total, metric);
        }

        // Get aggregation config once for cleanup settings
        let aggregation_config = self
            .streaming_config
            .get_aggregation_config(aggregation_id)
            .ok_or_else(|| format!("Aggregation config not found for {}", aggregation_id))?;

        // Configure epoch capacity on first insert (Optimization 2)
        if aggregation_config.aggregation_type != "DeltaSetAggregator" {
            data.configure_epochs(aggregation_config.num_aggregates_to_retain);
        }

        for (output, precompute) in items {
            // Intern the label key (Optimization 1)
            let timestamp_range = (output.start_timestamp, output.end_timestamp);
            let metric_id: MetricID = data.intern.intern(output.key);

            // Insert into current (mutable) epoch.
            data.current_epoch
                .insert(metric_id, timestamp_range, Arc::from(precompute));

            // After each item, check if we should rotate (CircularBuffer, Optimization 2)
            if aggregation_config.aggregation_type != "DeltaSetAggregator"
                && matches!(self.cleanup_policy, CleanupPolicy::CircularBuffer)
            {
                data.maybe_rotate_epoch();
            }
        }

        // Apply retention policy if configured (but exclude DeltaSetAggregator)
        if aggregation_config.aggregation_type != "DeltaSetAggregator" {
            self.cleanup_old_aggregates(
                &mut data,
                metric,
                aggregation_id,
                aggregation_config.num_aggregates_to_retain,
                aggregation_config.read_count_threshold,
            );
        }

        #[cfg(feature = "lock_profiling")]
        {
            let lock_hold_duration = lock_hold_start.elapsed();
            info!(
                "🔓 Insert lock hold time: {:.2}ms (metric: {}, agg_id: {})",
                lock_hold_duration.as_secs_f64() * 1000.0,
                metric,
                *store_key
            );
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Store for SimpleMapStorePerKey {
    fn insert_precomputed_output(
        &self,
        output: PrecomputedOutput,
        precompute: Box<dyn AggregateCore>,
    ) -> StoreResult<()> {
        self.insert_precomputed_output_batch(vec![(output, precompute)])
    }

    fn insert_precomputed_output_batch(
        &self,
        outputs: Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>,
    ) -> StoreResult<()> {
        let batch_insert_start_time = Instant::now();
        let batch_size = outputs.len();

        // Group by aggregation_id
        #[allow(clippy::type_complexity)]
        let mut grouped: HashMap<
            StoreKey,
            (String, Vec<(PrecomputedOutput, Box<dyn AggregateCore>)>),
        > = HashMap::new();

        for (output, precompute) in outputs {
            let aggregation_config = self
                .streaming_config
                .get_aggregation_config(output.aggregation_id);

            if aggregation_config.is_none() {
                error!(
                    "Aggregation config not found for aggregation_id {}. Skipping insert.",
                    output.aggregation_id
                );
                continue;
            }
            let aggregation_config = aggregation_config.unwrap();

            let metric = aggregation_config.metric.clone();
            let store_key = output.aggregation_id;

            grouped
                .entry(store_key)
                .or_insert_with(|| (metric.clone(), Vec::new()))
                .1
                .push((output, precompute));
        }

        // Process each aggregation_id group; each iteration locks at most one key.
        for (store_key, (metric, items)) in grouped {
            self.insert_for_store_key(&store_key, &metric, items)?;
        }

        let batch_insert_duration = batch_insert_start_time.elapsed();
        debug!(
            "Batch insert of {} items took: {:.2}ms",
            batch_size,
            batch_insert_duration.as_secs_f64() * 1000.0
        );
        Ok(())
    }

    fn query_precomputed_output(
        &self,
        metric: &str,
        aggregation_id: u64,
        start: u64,
        end: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
        if start > end {
            debug!(
                "Invalid query range for metric {} agg_id {}: start {} > end {}",
                metric, aggregation_id, start, end
            );
            return Ok(HashMap::new());
        }

        let query_start_time = Instant::now();
        let store_key = aggregation_id;

        // Measure lock acquisition time
        #[cfg(feature = "lock_profiling")]
        let lock_wait_start = Instant::now();

        // Get the store data for this aggregation_id
        let store_data_lock = match self.store.get(&store_key) {
            Some(lock) => lock,
            None => {
                info!("Metric {} not found in store", metric);
                return Ok(HashMap::new());
            }
        };

        #[cfg(feature = "lock_profiling")]
        {
            let lock_wait_duration = lock_wait_start.elapsed();
            info!(
                "🔒 Query DashMap get time: {:.2}ms (metric: {}, agg_id: {})",
                lock_wait_duration.as_secs_f64() * 1000.0,
                metric,
                aggregation_id
            );
        }

        #[cfg(feature = "lock_profiling")]
        let rwlock_wait_start = Instant::now();

        // Range queries use a read lock — no mutation of epoch data needed.
        let data = store_data_lock.read().map_err(|e| {
            format!(
                "Failed to acquire read lock for query aggregation_id {}: {}",
                store_key, e
            )
        })?;

        #[cfg(feature = "lock_profiling")]
        {
            let rwlock_wait_duration = rwlock_wait_start.elapsed();
            info!(
                "🔒 Query RwLock wait time: {:.2}ms (metric: {}, agg_id: {})",
                rwlock_wait_duration.as_secs_f64() * 1000.0,
                metric,
                aggregation_id
            );
        }

        #[cfg(feature = "lock_profiling")]
        let lock_hold_start = Instant::now();

        let mut total_entries = 0;
        let mut matched_windows: Vec<TimestampRange> = Vec::new();

        let range_scan_start_time = Instant::now();

        let mut mid: MetricBucketMap = HashMap::with_capacity(data.intern.len());

        // Query current (mutable) epoch.
        if let Some((min_start, max_end)) = data.current_epoch.time_bounds() {
            if !(min_start > end || max_end < start) {
                data.current_epoch
                    .range_query_into(start, end, &mut mid, &mut matched_windows);
            }
        }

        // Query sealed epochs; skip those with no overlap.
        for epoch in data.sealed_epochs.values() {
            let Some((min_start, max_end)) = epoch.time_bounds() else {
                continue;
            };
            if min_start > end || max_end < start {
                continue;
            }
            epoch.range_query_into(start, end, &mut mid, &mut matched_windows);
        }

        // Resolve MetricIDs → labels in a single pass
        let mut results: TimestampedBucketsMap = HashMap::with_capacity(mid.len());
        for (metric_id, buckets) in mid {
            total_entries += buckets.len();
            let label = data.intern.resolve(metric_id).clone();
            results.insert(label, buckets);
        }

        // Update read counts via inner Mutex
        {
            let mut read_counts = data.read_counts.lock().unwrap();
            for window in &matched_windows {
                *read_counts.entry(*window).or_insert(0) += 1;
            }
        }

        let range_scan_duration = range_scan_start_time.elapsed();
        debug!(
            "Range scanning took: {:.2}ms",
            range_scan_duration.as_secs_f64() * 1000.0
        );

        let query_duration = query_start_time.elapsed();
        debug!(
            "Total query took: {:.2}ms",
            query_duration.as_secs_f64() * 1000.0
        );

        debug!(
            "Found {} entries for query on {} (aggregation_id: {}, start: {}, end: {})",
            total_entries, metric, aggregation_id, start, end
        );
        debug!("Found {} unique keys", results.len());

        #[cfg(feature = "lock_profiling")]
        {
            let lock_hold_duration = lock_hold_start.elapsed();
            info!(
                "🔓 Query lock hold time: {:.2}ms (metric: {}, agg_id: {}, entries: {})",
                lock_hold_duration.as_secs_f64() * 1000.0,
                metric,
                aggregation_id,
                total_entries
            );
        }

        Ok(results)
    }

    fn query_precomputed_output_exact(
        &self,
        metric: &str,
        aggregation_id: u64,
        exact_start: u64,
        exact_end: u64,
    ) -> Result<TimestampedBucketsMap, Box<dyn std::error::Error + Send + Sync>> {
        if exact_start > exact_end {
            debug!(
                "Invalid exact query range for metric {} agg_id {}: start {} > end {}",
                metric, aggregation_id, exact_start, exact_end
            );
            return Ok(HashMap::new());
        }

        let query_start_time = Instant::now();
        let store_key = aggregation_id;

        // Measure lock acquisition time
        #[cfg(feature = "lock_profiling")]
        let lock_wait_start = Instant::now();

        // Get the store data for this aggregation_id
        let store_data_lock = match self.store.get(&store_key) {
            Some(lock) => lock,
            None => {
                debug!("Metric {} not found in store for exact query", metric);
                return Ok(HashMap::new());
            }
        };

        #[cfg(feature = "lock_profiling")]
        {
            let lock_wait_duration = lock_wait_start.elapsed();
            info!(
                "🔒 Exact query DashMap get time: {:.2}ms (metric: {}, agg_id: {})",
                lock_wait_duration.as_secs_f64() * 1000.0,
                metric,
                aggregation_id
            );
        }

        #[cfg(feature = "lock_profiling")]
        let rwlock_wait_start = Instant::now();

        // Opt 1: exact_query takes &mut self (lazy index build), so we need a write lock.
        // Range queries still use a read lock — only exact queries pay the write-lock cost.
        let mut data = store_data_lock.write().map_err(|e| {
            format!(
                "Failed to acquire write lock for exact query aggregation_id {}: {}",
                store_key, e
            )
        })?;

        #[cfg(feature = "lock_profiling")]
        {
            let rwlock_wait_duration = rwlock_wait_start.elapsed();
            info!(
                "🔒 Exact query RwLock wait time: {:.2}ms (metric: {}, agg_id: {})",
                rwlock_wait_duration.as_secs_f64() * 1000.0,
                metric,
                aggregation_id
            );
        }

        #[cfg(feature = "lock_profiling")]
        let lock_hold_start = Instant::now();

        let timestamp_range = (exact_start, exact_end);

        // Opt 1: exact_query on the mutable epoch builds the lazy offset index if absent,
        // then looks up the window in O(m). Returns an owned Vec — the &mut borrow ends here.
        let entries_opt: Option<Vec<(MetricID, Arc<dyn AggregateCore>)>> =
            data.current_epoch.exact_query(timestamp_range).or_else(|| {
                data.sealed_epochs
                    .values()
                    .rev()
                    .find_map(|epoch| epoch.exact_query(timestamp_range))
            });

        let mut results: TimestampedBucketsMap = HashMap::new();
        let mut total_entries = 0;
        let found_match = entries_opt.is_some();

        if let Some(entries) = entries_opt {
            for (metric_id, agg) in entries {
                let label = data.intern.resolve(metric_id).clone();
                results
                    .entry(label)
                    .or_default()
                    .push((timestamp_range, agg));
                total_entries += 1;
            }
        }

        if found_match {
            debug!(
                "Exact match FOUND for [{}, {}]: {} entries across {} keys",
                exact_start,
                exact_end,
                total_entries,
                results.len()
            );
        } else {
            debug!(
                "Exact match NOT FOUND for metric: {}, agg_id: {}, range: [{}, {}]",
                metric, aggregation_id, exact_start, exact_end
            );
        }

        // Update read count — write lock already held, no inner Mutex needed
        if found_match {
            let mut read_counts = data.read_counts.lock().unwrap();
            *read_counts.entry(timestamp_range).or_insert(0) += 1;
        }

        #[cfg(feature = "lock_profiling")]
        {
            let lock_hold_duration = lock_hold_start.elapsed();
            info!(
                "🔓 Exact query lock hold time: {:.2}ms (metric: {}, agg_id: {}, found: {})",
                lock_hold_duration.as_secs_f64() * 1000.0,
                metric,
                aggregation_id,
                !results.is_empty()
            );
        }

        let query_duration = query_start_time.elapsed();
        debug!(
            "Exact timestamp query took: {:.2}ms (found: {})",
            query_duration.as_secs_f64() * 1000.0,
            !results.is_empty()
        );

        Ok(results)
    }

    fn get_earliest_timestamp_per_aggregation_id(
        &self,
    ) -> Result<HashMap<u64, u64>, Box<dyn std::error::Error + Send + Sync>> {
        // No lock needed - DashMap with AtomicU64
        let result = self
            .earliest_timestamps
            .iter()
            .map(|entry| (*entry.key(), entry.value().load(Ordering::Relaxed)))
            .collect();

        Ok(result)
    }

    fn close(&self) -> StoreResult<()> {
        // For in-memory store, no cleanup needed
        info!("SimpleMapStorePerKey closed");
        Ok(())
    }
}
