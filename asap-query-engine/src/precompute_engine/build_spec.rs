//! The build specification a [`PrecomputePipeline`] is given: which builds
//! each ingested series feeds, and how each build turns samples into
//! windowed state. The pipeline owns routing to workers, windows, watermarks,
//! late data and backpressure; it writes every closed window to its
//! `OutputSink`.
//!
//! [`PrecomputePipeline`]: crate::precompute_engine::pipeline::PrecomputePipeline

use crate::data_model::AggregateCore;
use crate::precompute_engine::accumulator_factory::AccumulatorUpdater;
use std::collections::HashMap;
use std::sync::Arc;

/// One windowed build, run per `(build id, group key)`: window geometry,
/// pane updaters, and how a closed window becomes the state written out.
pub trait WindowedBuild: Send + Sync + 'static {
    /// State a group carries from one closed window to the next.
    type CrossWindowState: Default + Send;

    fn window_size_ms(&self) -> u64;

    fn slide_interval_ms(&self) -> u64;

    /// A fresh updater for one pane.
    fn new_pane(
        &self,
    ) -> Result<Box<dyn AccumulatorUpdater>, Box<dyn std::error::Error + Send + Sync>>;

    /// Feed one sample of series `series_key` into `pane`.
    fn apply_sample(
        &self,
        pane: &mut dyn AccumulatorUpdater,
        series_key: &str,
        value: f64,
        timestamp_ms: i64,
    );

    /// Whether `LateDataPolicy::ForwardToStore` may write a late sample as a
    /// standalone state for its already-closed window.
    fn forwards_late_samples(&self) -> bool;

    /// Turn the merged panes of a closed window into the state to write.
    fn finalize_window(
        &self,
        merged: Box<dyn AggregateCore>,
        state: &mut Self::CrossWindowState,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Decides which builds an ingested series feeds and its group key in each.
///
/// Group keys are grouping label values joined by `;`; the pipeline splits
/// them back into the written key. `Default` must route nothing: the
/// pipeline holds it until `run` installs the first spec.
pub trait SeriesRouting: Default + Send + Sync + 'static {
    type Build: WindowedBuild;

    /// Call `emit(build_id, group_key)` once per build `series_key` feeds.
    fn route_series<F: FnMut(u64, String)>(&self, series_key: &str, emit: F);

    /// Log how this routing sees `series_key`; called once, on the first
    /// ingested batch, to diagnose routing mismatches.
    fn log_routing_diagnostic(&self, series_key: &str);
}

/// What the pipeline builds: the ingest-side routing plus the builds workers
/// run, keyed by build id.
pub struct BuildSpec<R: SeriesRouting> {
    pub routing: R,
    pub builds: HashMap<u64, Arc<R::Build>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::drivers::ingest::prometheus_remote_write::DecodedSample;
    use crate::precompute_engine::accumulator_factory::SumAccumulatorUpdater;
    use crate::precompute_engine::config::LateDataPolicy;
    use crate::precompute_engine::ingest_source::{route_decoded_samples, IngestContext};
    use crate::precompute_engine::output_sink::CapturingOutputSink;
    use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
    use crate::precompute_engine::worker::{PipelineWorker, WorkerRuntimeConfig};
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use arc_swap::ArcSwap;
    use std::sync::atomic::{AtomicI64, AtomicUsize};
    use std::time::Instant;

    const WINDOW_MS: u64 = 1_000;

    /// Emits the running total of every window closed so far, so each output
    /// depends on the cross-window state.
    struct RunningTotalBuild;

    impl WindowedBuild for RunningTotalBuild {
        type CrossWindowState = f64;

        fn window_size_ms(&self) -> u64 {
            WINDOW_MS
        }

        fn slide_interval_ms(&self) -> u64 {
            WINDOW_MS
        }

        fn new_pane(
            &self,
        ) -> Result<Box<dyn AccumulatorUpdater>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Box::new(SumAccumulatorUpdater::new()))
        }

        fn apply_sample(
            &self,
            pane: &mut dyn AccumulatorUpdater,
            _series_key: &str,
            value: f64,
            timestamp_ms: i64,
        ) {
            pane.update_single(value, timestamp_ms);
        }

        fn forwards_late_samples(&self) -> bool {
            false
        }

        fn finalize_window(
            &self,
            merged: Box<dyn AggregateCore>,
            running_total: &mut f64,
        ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
            let window = merged
                .as_any()
                .downcast_ref::<SumAccumulator>()
                .ok_or("pane is not a SumAccumulator")?;
            *running_total += window.sum;
            Ok(Box::new(SumAccumulator::with_sum(*running_total)))
        }
    }

    /// Routes every series to builds 1 and 2, grouped by the whole series key.
    #[derive(Default)]
    struct EveryBuildRouting;

    impl SeriesRouting for EveryBuildRouting {
        type Build = RunningTotalBuild;

        fn route_series<F: FnMut(u64, String)>(&self, series_key: &str, mut emit: F) {
            emit(1, series_key.to_string());
            emit(2, series_key.to_string());
        }

        fn log_routing_diagnostic(&self, _series_key: &str) {}
    }

    fn running_total_worker(
        sink: Arc<CapturingOutputSink>,
        allowed_lateness_ms: i64,
        late_data_policy: LateDataPolicy,
    ) -> PipelineWorker<RunningTotalBuild> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let watermark = Arc::new(AtomicI64::new(i64::MIN));
        PipelineWorker::new(
            0,
            rx,
            sink,
            HashMap::from([(7, Arc::new(RunningTotalBuild))]),
            WorkerRuntimeConfig {
                max_buffer_per_series: 10_000,
                allowed_lateness_ms,
                pass_raw_samples: false,
                raw_mode_aggregation_id: 0,
                late_data_policy,
                wall_clock_grace_period_ms: 0,
            },
            Arc::new(AtomicUsize::new(0)),
            watermark.clone(),
            vec![watermark],
        )
    }

    fn sample(timestamp_ms: i64, value: f64) -> Vec<(String, i64, f64)> {
        vec![("cpu".to_string(), timestamp_ms, value)]
    }

    #[test]
    fn worker_carries_cross_window_state_of_a_non_aggregation_build() {
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = running_total_worker(sink.clone(), 0, LateDataPolicy::Drop);

        worker
            .process_group_samples(7, "g", sample(100, 1.0))
            .unwrap();
        worker
            .process_group_samples(7, "g", sample(1_500, 2.0))
            .unwrap();
        worker
            .process_group_samples(7, "g", sample(2_500, 4.0))
            .unwrap();

        let mut captured = sink.drain();
        captured.sort_by_key(|(output, _)| output.start_timestamp);
        let windows: Vec<(u64, u64, f64)> = captured
            .iter()
            .map(|(output, state)| {
                assert_eq!(output.aggregation_id, 7);
                let sum = state
                    .as_any()
                    .downcast_ref::<SumAccumulator>()
                    .expect("finalized state is a SumAccumulator")
                    .sum;
                (output.start_timestamp, output.end_timestamp, sum)
            })
            .collect();
        assert_eq!(windows, vec![(0, 1_000, 1.0), (1_000, 2_000, 3.0)]);
    }

    #[test]
    fn forward_to_store_drops_late_samples_for_builds_that_do_not_forward() {
        let sink = Arc::new(CapturingOutputSink::new());
        let mut worker = running_total_worker(sink.clone(), 5_000, LateDataPolicy::ForwardToStore);

        worker
            .process_group_samples(7, "g", sample(100, 1.0))
            .unwrap();
        worker
            .process_group_samples(7, "g", sample(2_500, 2.0))
            .unwrap();
        assert_eq!(sink.drain().len(), 1, "window (0, 1000] closes");

        // Within lateness, but its pane was already evicted.
        worker
            .process_group_samples(7, "g", sample(200, 8.0))
            .unwrap();
        assert!(sink.is_empty(), "late sample must not be forwarded");
    }

    #[tokio::test]
    async fn ingest_routes_through_a_non_aggregation_routing() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
        let ctx = IngestContext {
            router: SeriesRouter::new(vec![sender]),
            routing_configs: Arc::new(ArcSwap::from_pointee(EveryBuildRouting)),
            pass_raw_samples: false,
        };

        let samples = vec![DecodedSample {
            labels: "cpu{host=\"a\"}".to_string(),
            timestamp_ms: 1_000,
            value: 1.0,
        }];
        route_decoded_samples(&ctx, samples, Instant::now())
            .await
            .unwrap();
        drop(ctx);

        let mut routed = Vec::new();
        while let Some(message) = receiver.recv().await {
            let WorkerMessage::GroupSamples {
                agg_id, group_key, ..
            } = message
            else {
                panic!("routing should produce GroupSamples");
            };
            routed.push((agg_id, group_key));
        }
        routed.sort();
        assert_eq!(
            routed,
            vec![
                (1, "cpu{host=\"a\"}".to_string()),
                (2, "cpu{host=\"a\"}".to_string()),
            ]
        );
    }
}
