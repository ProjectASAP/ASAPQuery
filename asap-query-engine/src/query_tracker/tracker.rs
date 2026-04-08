use std::sync::{Arc, Mutex};
use std::time::Duration;

use asap_planner::query_log::{infer_queries, to_controller_config, LogEntry};
use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::planner_client::PlannerClient;

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
}

impl QueryTracker {
    pub fn new(config: QueryTrackerConfig) -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            config,
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
    pub fn start_background_loop(
        self: &Arc<Self>,
        planner_client: Arc<dyn PlannerClient>,
    ) -> JoinHandle<()> {
        let tracker = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(tracker.config.observation_window_secs));
            // The first tick completes immediately; skip it so we wait a full window first.
            interval.tick().await;

            loop {
                interval.tick().await;
                Self::evaluate(&tracker, &planner_client).await;
            }
        })
    }

    async fn evaluate(tracker: &Arc<Self>, planner_client: &Arc<dyn PlannerClient>) {
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

        let controller_config = to_controller_config(instants, ranges);

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
    use sketch_db_common::inference_config::InferenceConfig;
    use sketch_db_common::streaming_config::StreamingConfig;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
                    sketch_db_common::enums::QueryLanguage::promql,
                    sketch_db_common::enums::CleanupPolicy::NoCleanup,
                ),
                punted_queries: vec![],
            })
        }
    }

    #[test]
    fn record_instant_appends_entry() {
        let tracker = QueryTracker::new(QueryTrackerConfig {
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
        let tracker = QueryTracker::new(QueryTrackerConfig {
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
        let tracker = Arc::new(QueryTracker::new(QueryTrackerConfig {
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
        QueryTracker::evaluate(&tracker, &(mock_client.clone() as Arc<dyn PlannerClient>)).await;

        assert_eq!(mock_client.call_count.load(Ordering::SeqCst), 1);
        // Entries should be drained after evaluate.
        assert!(tracker.entries.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn evaluate_skips_when_no_entries() {
        let tracker = Arc::new(QueryTracker::new(QueryTrackerConfig {
            observation_window_secs: 600,
            prometheus_scrape_interval: 15,
        }));

        let mock_client = Arc::new(MockPlannerClient::new());
        QueryTracker::evaluate(&tracker, &(mock_client.clone() as Arc<dyn PlannerClient>)).await;

        assert_eq!(mock_client.call_count.load(Ordering::SeqCst), 0);
    }
}
