use std::time::Instant;
use tokio::sync::mpsc;
use xxhash_rust::xxh64::xxh64;

/// A message sent from the router to a worker.
#[derive(Debug)]
pub enum WorkerMessage {
    /// A batch of samples for the same series.
    Samples {
        series_key: String,
        samples: Vec<(i64, f64)>, // (timestamp_ms, value)
        ingest_received_at: Instant,
    },
    /// Signal the worker to flush/check idle windows.
    Flush,
    /// Graceful shutdown.
    Shutdown,
}

/// Routes incoming samples to one of N workers based on a consistent hash
/// of the series label string.
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

    /// Route a batch of samples for one series to the appropriate worker.
    pub async fn route(
        &self,
        series_key: &str,
        samples: Vec<(i64, f64)>,
        ingest_received_at: Instant,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let worker_idx = self.worker_for(series_key);
        self.senders[worker_idx]
            .send(WorkerMessage::Samples {
                series_key: series_key.to_string(),
                samples,
                ingest_received_at,
            })
            .await
            .map_err(|e| format!("Failed to send to worker {}: {}", worker_idx, e))?;
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

    /// Determine which worker handles a given series key.
    fn worker_for(&self, series_key: &str) -> usize {
        let hash = xxh64(series_key.as_bytes(), 0);
        (hash as usize) % self.num_workers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consistent_routing() {
        // Build a router with dummy senders (we only test the hash logic)
        let (senders, _receivers): (Vec<_>, Vec<_>) =
            (0..4).map(|_| mpsc::channel::<WorkerMessage>(10)).unzip();

        let router = SeriesRouter::new(senders);

        // Same key should always go to the same worker
        let w1 = router.worker_for("cpu{host=\"a\"}");
        let w2 = router.worker_for("cpu{host=\"a\"}");
        assert_eq!(w1, w2);

        // Different keys may go to different workers (probabilistic, but verifiable)
        let _ = router.worker_for("cpu{host=\"b\"}");
        // Just ensure no panic and result is in range
        assert!(router.worker_for("mem{host=\"a\"}") < 4);
    }
}
