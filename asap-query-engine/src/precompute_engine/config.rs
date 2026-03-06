use serde::{Deserialize, Serialize};

/// Policy for handling late samples that arrive after their window has closed.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, clap::ValueEnum)]
pub enum LateDataPolicy {
    /// Drop late samples that arrive after their window has closed.
    Drop,
    /// Forward late samples to the store to be merged with existing window data.
    ForwardToStore,
}

/// Configuration for the precompute engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecomputeEngineConfig {
    /// Number of worker threads for parallel processing.
    pub num_workers: usize,
    /// Port for the Prometheus remote write ingest endpoint.
    pub ingest_port: u16,
    /// Maximum allowed lateness for out-of-order samples (milliseconds).
    /// Samples arriving later than this behind the watermark are dropped.
    pub allowed_lateness_ms: i64,
    /// Maximum number of buffered samples per series before oldest are evicted.
    pub max_buffer_per_series: usize,
    /// Interval at which the flush timer fires to close idle windows (milliseconds).
    pub flush_interval_ms: u64,
    /// Capacity of the MPSC channel between router and each worker.
    pub channel_buffer_size: usize,
    /// When true, skip all aggregation and pass each raw sample directly to the
    /// output sink as a `SumAccumulator::with_sum(value)`.
    pub pass_raw_samples: bool,
    /// Aggregation ID to stamp on each raw-mode output.
    pub raw_mode_aggregation_id: u64,
    /// Policy for handling late samples that arrive after their window has closed.
    pub late_data_policy: LateDataPolicy,
}

impl Default for PrecomputeEngineConfig {
    fn default() -> Self {
        Self {
            num_workers: 4,
            ingest_port: 9090,
            allowed_lateness_ms: 5_000,
            max_buffer_per_series: 10_000,
            flush_interval_ms: 1_000,
            channel_buffer_size: 10_000,
            pass_raw_samples: false,
            raw_mode_aggregation_id: 0,
            late_data_policy: LateDataPolicy::Drop,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PrecomputeEngineConfig::default();
        assert_eq!(config.num_workers, 4);
        assert_eq!(config.ingest_port, 9090);
        assert_eq!(config.allowed_lateness_ms, 5_000);
        assert_eq!(config.max_buffer_per_series, 10_000);
        assert_eq!(config.flush_interval_ms, 1_000);
        assert_eq!(config.channel_buffer_size, 10_000);
        assert!(!config.pass_raw_samples);
        assert_eq!(config.raw_mode_aggregation_id, 0);
        assert_eq!(config.late_data_policy, LateDataPolicy::Drop);
    }
}
