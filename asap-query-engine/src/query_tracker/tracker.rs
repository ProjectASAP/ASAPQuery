use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use asap_planner::query_log::{infer_queries, to_controller_config, LogEntry};
use asap_types::inference_config::InferenceConfig;
use asap_types::streaming_config::StreamingConfig;
use chrono::{DateTime, Utc};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::planner_client::{PlannerClient, PlannerResult};

#[derive(Debug, Clone)]
pub struct QueryTrackerConfig {
    /// How often to evaluate and trigger planning (default: 600s = 10 min).
    pub observation_window_secs: u64,
    /// Prometheus scrape interval, passed through to `infer_queries`.
    pub prometheus_scrape_interval: u64,
}

pub struct QueryTracker {
    entries: Mutex<Vec<LogEntry>>,
    config: QueryTrackerConfig,
    /// Read-only snapshot of the current streaming config, shared with the applier
    /// task in main.rs. Used to populate ControllerConfig.existing_streaming_config.
    streaming_config: Arc<RwLock<Arc<StreamingConfig>>>,
    /// Read-only snapshot of the current inference config, shared with the applier task.
    inference_config: Arc<RwLock<Arc<InferenceConfig>>>,
    /// Set to true after the first successful plan is sent. The background loop keeps
    /// running (for observability) but will not send further results until repeated-
    /// reconfig is wired up.
    applied: AtomicBool,
}

impl QueryTracker {
    pub fn new(
        config: QueryTrackerConfig,
        streaming_config: Arc<RwLock<Arc<StreamingConfig>>>,
        inference_config: Arc<RwLock<Arc<InferenceConfig>>>,
    ) -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            config,
            streaming_config,
            inference_config,
            applied: AtomicBool::new(false),
        }
    }

    /// Record an instant query (step=0, start==end==time).
    pub fn record_instant(&self, query: &str, time: f64) {
        let ts = Utc::now();
        let query_time = epoch_secs_to_datetime(time);
        let entry = LogEntry {
            query: query.to_string(),
            start: query_time,
            end: query_time,
            step: 0,
            ts,
        };
        self.entries.lock().unwrap().push(entry);
    }

    /// Record a range query with start/end/step from the request.
    pub fn record_range(&self, query: &str, start: f64, end: f64, step: f64) {
        let ts = Utc::now();
        let entry = LogEntry {
            query: query.to_string(),
            start: epoch_secs_to_datetime(start),
            end: epoch_secs_to_datetime(end),
            step: step as u64,
            ts,
        };
        self.entries.lock().unwrap().push(entry);
    }

    /// Spawn a background task that periodically evaluates collected queries and calls the planner.
    ///
    /// On the first successful plan, sends the result via `plan_tx` and sets `applied = true`.
    /// The loop keeps running after that (for observability), but further results are dropped
    /// until repeated-reconfig is wired up.
    pub fn start_background_loop(
        self: &Arc<Self>,
        planner_client: Arc<dyn PlannerClient>,
        plan_tx: watch::Sender<Option<PlannerResult>>,
    ) -> JoinHandle<()> {
        let tracker = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(tracker.config.observation_window_secs));
            // The first tick completes immediately; skip it so we wait a full window first.
            interval.tick().await;

            loop {
                interval.tick().await;
                Self::evaluate(&tracker, &planner_client, &plan_tx).await;
            }
        })
    }

    async fn evaluate(
        tracker: &Arc<Self>,
        planner_client: &Arc<dyn PlannerClient>,
        plan_tx: &watch::Sender<Option<PlannerResult>>,
    ) {
        // Snapshot and drain collected entries.
        let entries = {
            let mut guard = tracker.entries.lock().unwrap();
            std::mem::take(&mut *guard)
        };

        if entries.is_empty() {
            info!("query_tracker: no queries observed in this window, skipping");
            return;
        }

        info!(
            "query_tracker: evaluating {} observed query entries",
            entries.len()
        );

        let scrape_interval = tracker.config.prometheus_scrape_interval;
        let (instants, ranges) = infer_queries(&entries, scrape_interval);

        info!(
            "query_tracker: inferred {} instant queries, {} range queries",
            instants.len(),
            ranges.len()
        );

        if instants.is_empty() && ranges.is_empty() {
            info!("query_tracker: no queries met frequency threshold, skipping planner call");
            return;
        }

        // Build ControllerConfig, including current configs as context for the planner.
        // NOTE: existing_* fields are wired through but the planner does not yet act on them.
        let mut controller_config = to_controller_config(instants, ranges);
        controller_config.existing_streaming_config =
            Some((*tracker.streaming_config.read().unwrap().clone()).clone());
        controller_config.existing_inference_config =
            Some((*tracker.inference_config.read().unwrap().clone()).clone());

        info!(
            "query_tracker: calling planner with {} query groups",
            controller_config.query_groups.len()
        );

        match planner_client.plan(controller_config).await {
            Ok(result) => {
                info!(
                    "query_tracker: planner succeeded — streaming aggregations: {}, inference queries: {}, punted: {}",
                    result.streaming_config.aggregation_configs.len(),
                    result.inference_config.query_configs.len(),
                    result.punted_queries.len(),
                );
                // Send result only on first successful plan.
                if !tracker.applied.load(Ordering::Acquire) {
                    tracker.applied.store(true, Ordering::Release);
                    let _ = plan_tx.send(Some(result));
                    info!("query_tracker: plan applied (first time); subsequent windows will run but not re-apply");
                } else {
                    info!("query_tracker: plan already applied; skipping re-apply (repeated-reconfig not yet implemented)");
                }
            }
            Err(e) => {
                warn!("query_tracker: planner failed: {}", e);
            }
        }
    }
}

fn epoch_secs_to_datetime(secs: f64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs as i64, ((secs.fract()) * 1_000_000_000.0) as u32)
        .unwrap_or_else(Utc::now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner_client::PlannerResult;
    use anyhow::Result;
    use asap_types::inference_config::InferenceConfig;
    use asap_types::streaming_config::StreamingConfig;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::watch;

    fn make_tracker(config: QueryTrackerConfig) -> QueryTracker {
        let sc = Arc::new(RwLock::new(Arc::new(StreamingConfig::new(HashMap::new()))));
        let ic = Arc::new(RwLock::new(Arc::new(InferenceConfig::new(
            asap_types::enums::QueryLanguage::promql,
            asap_types::enums::CleanupPolicy::NoCleanup,
        ))));
        QueryTracker::new(config, sc, ic)
    }

    struct MockPlannerClient {
        call_count: AtomicUsize,
    }

    impl MockPlannerClient {
        fn new() -> Self {
            Self {
                call_count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl PlannerClient for MockPlannerClient {
        async fn plan(&self, _config: asap_planner::ControllerConfig) -> Result<PlannerResult> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(PlannerResult {
                streaming_config: StreamingConfig::new(HashMap::new()),
                inference_config: InferenceConfig::new(
                    asap_types::enums::QueryLanguage::promql,
                    asap_types::enums::CleanupPolicy::NoCleanup,
                ),
                punted_queries: vec![],
            })
        }
    }

    #[test]
    fn record_instant_appends_entry() {
        let tracker = make_tracker(QueryTrackerConfig {
            observation_window_secs: 600,
            prometheus_scrape_interval: 15,
        });
        tracker.record_instant("rate(http_requests_total[5m])", 1700000000.0);
        tracker.record_instant("rate(http_requests_total[5m])", 1700000060.0);
        let entries = tracker.entries.lock().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].step, 0);
        assert_eq!(entries[0].start, entries[0].end);
    }

    #[test]
    fn record_range_appends_entry() {
        let tracker = make_tracker(QueryTrackerConfig {
            observation_window_secs: 600,
            prometheus_scrape_interval: 15,
        });
        tracker.record_range(
            "rate(http_requests_total[5m])",
            1700000000.0,
            1700003600.0,
            30.0,
        );
        let entries = tracker.entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].step, 30);
        assert_ne!(entries[0].start, entries[0].end);
    }

    #[tokio::test]
    async fn evaluate_calls_planner_with_entries() {
        let tracker = Arc::new(make_tracker(QueryTrackerConfig {
            observation_window_secs: 600,
            prometheus_scrape_interval: 15,
        }));

        // Record enough entries for infer_queries to produce results (need >=2 per query).
        for i in 0..5 {
            tracker.record_instant(
                "rate(http_requests_total[5m])",
                1700000000.0 + (i as f64 * 60.0),
            );
        }

        let mock_client = Arc::new(MockPlannerClient::new());
        let (plan_tx, _plan_rx) = watch::channel(None::<PlannerResult>);
        QueryTracker::evaluate(
            &tracker,
            &(mock_client.clone() as Arc<dyn PlannerClient>),
            &plan_tx,
        )
        .await;

        assert_eq!(mock_client.call_count.load(Ordering::SeqCst), 1);
        // Entries should be drained after evaluate.
        assert!(tracker.entries.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn evaluate_skips_when_no_entries() {
        let tracker = Arc::new(make_tracker(QueryTrackerConfig {
            observation_window_secs: 600,
            prometheus_scrape_interval: 15,
        }));

        let mock_client = Arc::new(MockPlannerClient::new());
        let (plan_tx, _plan_rx) = watch::channel(None::<PlannerResult>);
        QueryTracker::evaluate(
            &tracker,
            &(mock_client.clone() as Arc<dyn PlannerClient>),
            &plan_tx,
        )
        .await;

        assert_eq!(mock_client.call_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn evaluate_sends_plan_only_once() {
        let tracker = Arc::new(make_tracker(QueryTrackerConfig {
            observation_window_secs: 600,
            prometheus_scrape_interval: 15,
        }));

        // Record enough for infer_queries to produce results.
        for i in 0..5 {
            tracker.record_instant(
                "rate(http_requests_total[5m])",
                1700000000.0 + (i as f64 * 60.0),
            );
        }

        let mock_client = Arc::new(MockPlannerClient::new());
        let (plan_tx, mut plan_rx) = watch::channel(None::<PlannerResult>);

        // First evaluate: result sent, applied flag set.
        QueryTracker::evaluate(
            &tracker,
            &(mock_client.clone() as Arc<dyn PlannerClient>),
            &plan_tx,
        )
        .await;

        assert!(tracker.applied.load(Ordering::SeqCst));
        // Mark the sent value as seen so subsequent changed() only fires for new sends.
        assert!(plan_rx.borrow_and_update().is_some());

        // Record entries for a second window.
        for i in 0..5 {
            tracker.record_instant(
                "rate(http_requests_total[5m])",
                1700003600.0 + (i as f64 * 60.0),
            );
        }

        // Second evaluate: planner is called again but must NOT re-send on the channel.
        QueryTracker::evaluate(
            &tracker,
            &(mock_client.clone() as Arc<dyn PlannerClient>),
            &plan_tx,
        )
        .await;

        assert_eq!(mock_client.call_count.load(Ordering::SeqCst), 2);

        // changed() should time out — no new value sent.
        let timed_out =
            tokio::time::timeout(std::time::Duration::from_millis(10), plan_rx.changed())
                .await
                .is_err();
        assert!(timed_out, "second evaluate must not send a new plan");
    }

    #[tokio::test]
    async fn evaluate_populates_existing_configs_in_controller_config() {
        use asap_planner::ControllerConfig;
        use std::sync::Mutex;

        struct CapturingPlannerClient {
            captured: Mutex<Option<ControllerConfig>>,
        }

        impl CapturingPlannerClient {
            fn new() -> Self {
                Self {
                    captured: Mutex::new(None),
                }
            }
        }

        #[async_trait::async_trait]
        impl PlannerClient for CapturingPlannerClient {
            async fn plan(&self, config: ControllerConfig) -> anyhow::Result<PlannerResult> {
                *self.captured.lock().unwrap() = Some(config);
                Ok(PlannerResult {
                    streaming_config: StreamingConfig::new(HashMap::new()),
                    inference_config: InferenceConfig::new(
                        asap_types::enums::QueryLanguage::promql,
                        asap_types::enums::CleanupPolicy::NoCleanup,
                    ),
                    punted_queries: vec![],
                })
            }
        }

        let sc = Arc::new(RwLock::new(Arc::new(StreamingConfig::new(HashMap::new()))));
        let ic = Arc::new(RwLock::new(Arc::new(InferenceConfig::new(
            asap_types::enums::QueryLanguage::promql,
            asap_types::enums::CleanupPolicy::NoCleanup,
        ))));
        let tracker = Arc::new(QueryTracker::new(
            QueryTrackerConfig {
                observation_window_secs: 600,
                prometheus_scrape_interval: 15,
            },
            sc,
            ic,
        ));

        for i in 0..5 {
            tracker.record_instant(
                "rate(http_requests_total[5m])",
                1700000000.0 + (i as f64 * 60.0),
            );
        }

        let client = Arc::new(CapturingPlannerClient::new());
        let (plan_tx, _plan_rx) = watch::channel(None::<PlannerResult>);
        QueryTracker::evaluate(
            &tracker,
            &(client.clone() as Arc<dyn PlannerClient>),
            &plan_tx,
        )
        .await;

        let captured = client.captured.lock().unwrap();
        let config = captured.as_ref().expect("planner should have been called");
        assert!(
            config.existing_streaming_config.is_some(),
            "existing_streaming_config must be populated from the shared ref"
        );
        assert!(
            config.existing_inference_config.is_some(),
            "existing_inference_config must be populated from the shared ref"
        );
    }
}
