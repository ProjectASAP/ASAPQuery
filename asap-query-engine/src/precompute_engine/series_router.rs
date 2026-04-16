use asap_types::aggregation_config::AggregationConfig;
use futures::future::try_join_all;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use xxhash_rust::xxh64::xxh64;

/// A message sent from the router to a worker.
#[derive(Debug)]
pub enum WorkerMessage {
    /// A batch of samples for the same series, routed by series key.
    /// Used in `pass_raw_samples` mode where no aggregation is needed.
    RawSamples {
        series_key: String,
        samples: Vec<(i64, f64)>, // (timestamp_ms, value)
        ingest_received_at: Instant,
    },
    /// A batch of samples destined for a specific aggregation group.
    /// All samples share the same (agg_id, group_key) and are fed into
    /// a single shared accumulator (like Arroyo's GROUP BY).
    GroupSamples {
        agg_id: u64,
        /// Grouping label values joined by semicolons (e.g. "constant").
        /// Empty string if the aggregation has no grouping labels.
        group_key: String,
        /// Each entry: (series_key, timestamp_ms, value).
        /// series_key is needed for keyed (MultipleSubpopulation) accumulators
        /// to extract the aggregated-label key.
        samples: Vec<(String, i64, f64)>,
        ingest_received_at: Instant,
    },
    /// Signal the worker to flush/check idle windows.
    Flush,
    /// Graceful shutdown.
    Shutdown,
    /// Push a new set of aggregation configs to this worker.
    /// Workers replace their local map on receipt; new agg_ids are picked up
    /// lazily the next time a matching sample arrives.
    UpdateAggConfigs(HashMap<u64, Arc<AggregationConfig>>),
}

/// Routes incoming samples to one of N workers based on a consistent hash.
pub struct SeriesRouter {
    senders: Vec<mpsc::Sender<WorkerMessage>>,
    num_workers: usize,
}

impl SeriesRouter {
    pub fn new(senders: Vec<mpsc::Sender<WorkerMessage>>) -> Self {
        let num_workers = senders.len();
        Self {
            senders,
            num_workers,
        }
    }

    /// Route a pre-grouped batch of group messages to workers concurrently.
    ///
    /// Each `GroupSamples` message is routed by `hash(agg_id, group_key)`.
    /// Messages within a single worker are sent sequentially to preserve ordering.
    pub async fn route_group_batch(
        &self,
        messages: Vec<WorkerMessage>,
        _ingest_received_at: Instant,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Group messages by target worker index
        let mut per_worker: HashMap<usize, Vec<WorkerMessage>> = HashMap::new();
        for msg in messages {
            let worker_idx = match &msg {
                WorkerMessage::GroupSamples {
                    agg_id, group_key, ..
                } => self.worker_for_group(*agg_id, group_key),
                WorkerMessage::RawSamples { series_key, .. } => self.worker_for(series_key),
                _ => 0,
            };
            per_worker.entry(worker_idx).or_default().push(msg);
        }

        // Send to each worker concurrently
        try_join_all(per_worker.into_iter().map(|(worker_idx, messages)| {
            let sender = &self.senders[worker_idx];
            async move {
                for msg in messages {
                    sender
                        .send(msg)
                        .await
                        .map_err(|e| format!("Failed to send to worker {}: {}", worker_idx, e))?;
                }
                Ok::<(), String>(())
            }
        }))
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::other(e))
        })?;

        Ok(())
    }

    /// Broadcast a flush signal to all workers.
    pub async fn broadcast_flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (i, sender) in self.senders.iter().enumerate() {
            sender
                .send(WorkerMessage::Flush)
                .await
                .map_err(|e| format!("Failed to send flush to worker {}: {}", i, e))?;
        }
        Ok(())
    }

    /// Broadcast updated aggregation configs to all workers.
    pub async fn broadcast_update_agg_configs(
        &self,
        agg_configs: HashMap<u64, Arc<AggregationConfig>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (i, sender) in self.senders.iter().enumerate() {
            sender
                .send(WorkerMessage::UpdateAggConfigs(agg_configs.clone()))
                .await
                .map_err(|e| format!("Failed to send UpdateAggConfigs to worker {}: {}", i, e))?;
        }
        Ok(())
    }

    /// Broadcast shutdown to all workers.
    pub async fn broadcast_shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (i, sender) in self.senders.iter().enumerate() {
            sender
                .send(WorkerMessage::Shutdown)
                .await
                .map_err(|e| format!("Failed to send shutdown to worker {}: {}", i, e))?;
        }
        Ok(())
    }

    /// Determine which worker handles a given group key.
    fn worker_for_group(&self, agg_id: u64, group_key: &str) -> usize {
        // Hash both agg_id and group_key together for consistent routing
        let mut hash_input = agg_id.to_le_bytes().to_vec();
        hash_input.extend_from_slice(group_key.as_bytes());
        let hash = xxh64(&hash_input, 0);
        (hash as usize) % self.num_workers
    }

    /// Determine which worker handles a given series key (for raw mode).
    fn worker_for(&self, series_key: &str) -> usize {
        let hash = xxh64(series_key.as_bytes(), 0);
        (hash as usize) % self.num_workers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consistent_group_routing() {
        let (senders, _receivers): (Vec<_>, Vec<_>) =
            (0..4).map(|_| mpsc::channel::<WorkerMessage>(10)).unzip();

        let router = SeriesRouter::new(senders);

        // Same (agg_id, group_key) should always go to the same worker
        let w1 = router.worker_for_group(1, "constant");
        let w2 = router.worker_for_group(1, "constant");
        assert_eq!(w1, w2);

        // Different group keys may go to different workers
        let _ = router.worker_for_group(1, "sine");
        assert!(router.worker_for_group(1, "linear-up") < 4);

        // Different agg_ids with same group key may go to different workers
        let _ = router.worker_for_group(2, "constant");
        assert!(router.worker_for_group(2, "constant") < 4);
    }

    #[test]
    fn test_raw_mode_routing() {
        let (senders, _receivers): (Vec<_>, Vec<_>) =
            (0..4).map(|_| mpsc::channel::<WorkerMessage>(10)).unzip();

        let router = SeriesRouter::new(senders);

        // Same key should always go to the same worker
        let w1 = router.worker_for("cpu{host=\"a\"}");
        let w2 = router.worker_for("cpu{host=\"a\"}");
        assert_eq!(w1, w2);
        assert!(router.worker_for("mem{host=\"a\"}") < 4);
    }
}
