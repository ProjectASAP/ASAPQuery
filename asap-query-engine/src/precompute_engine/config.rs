use serde::{Deserialize, Serialize};

/// Policy for handling late samples that arrive after their window has closed.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, clap::ValueEnum)]
pub enum LateDataPolicy {
    /// Drop late samples that arrive after their window has closed.
    Drop,
    /// Forward late samples to the store to be merged with existing window data.
    /// Unsupported for `DeltaSetAggregator`, whose stateful key deltas cannot
    /// be repaired by appending an independent mini-accumulator.
    ForwardToStore,
}

/// Configuration for the precompute engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecomputeEngineConfig {
    /// Number of worker threads for parallel processing.
    pub num_workers: usize,
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
    /// Wall-clock grace period (milliseconds) for the watermark fallback in
    /// `flush_all`. When event-time stagnates (e.g. a one-shot batch where
    /// every record carries the same timestamp), `flush_all`'s `+1ms`
    /// watermark advance is a no-op and idle windows never close. The
    /// wall-clock fallback closes a pane that has gone *idle* — untouched by
    /// a sample — for `window_size_ms + wall_clock_grace_period_ms` of
    /// *wall-clock* time, regardless of where event-time is. A pane still
    /// actively receiving samples is never force-closed this way, no matter
    /// how long it's been open (e.g. a bulk load whose rows all share one
    /// event-time and takes longer than the grace period to ingest keeps its
    /// window open for the duration of the load). The grace period tolerates
    /// late-arriving events that would otherwise be evicted as "the window
    /// already closed". Set to `<= 0` to opt out and keep strict
    /// event-time-only semantics. Default: 5000 ms (matches
    /// `allowed_lateness_ms` default).
    #[serde(default = "default_wall_clock_grace_period_ms")]
    pub wall_clock_grace_period_ms: i64,
}

impl Default for PrecomputeEngineConfig {
    fn default() -> Self {
        Self {
            num_workers: 4,
            allowed_lateness_ms: 5_000,
            max_buffer_per_series: 10_000,
            flush_interval_ms: 1_000,
            channel_buffer_size: 10_000,
            pass_raw_samples: false,
            raw_mode_aggregation_id: 0,
            late_data_policy: LateDataPolicy::Drop,
            wall_clock_grace_period_ms: default_wall_clock_grace_period_ms(),
        }
    }
}

fn default_wall_clock_grace_period_ms() -> i64 {
    5_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PrecomputeEngineConfig::default();
        assert_eq!(config.num_workers, 4);
        assert_eq!(config.allowed_lateness_ms, 5_000);
        assert_eq!(config.max_buffer_per_series, 10_000);
        assert_eq!(config.flush_interval_ms, 1_000);
        assert_eq!(config.channel_buffer_size, 10_000);
        assert!(!config.pass_raw_samples);
        assert_eq!(config.raw_mode_aggregation_id, 0);
        assert_eq!(config.late_data_policy, LateDataPolicy::Drop);
        assert_eq!(config.wall_clock_grace_period_ms, 5_000);
    }
}
