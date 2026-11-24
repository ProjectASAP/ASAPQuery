use crate::drivers::ingest::prometheus_remote_write::DecodedSample;
use crate::precompute_engine::aggregation_spec::RoutingConfigSet;
use crate::precompute_engine::build_spec::SeriesRouting;
use crate::precompute_engine::series_router::{SeriesRouter, WorkerMessage};
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

/// Everything a source needs to push decoded samples into the worker pool.
pub struct IngestContext<R: SeriesRouting = RoutingConfigSet> {
    pub(crate) router: SeriesRouter<R::Build>,
    /// Which builds each series feeds.
    /// Shared with the pipeline handle, whose swaps are immediately visible
    /// on this lock-free read path.
    pub(crate) routing_configs: Arc<ArcSwap<R>>,
    /// When true, skip group-key extraction and pass raw samples through.
    pub(crate) pass_raw_samples: bool,
}

// Manual impl: cloning the shared handles must not require `R: Clone`.
impl<R: SeriesRouting> Clone for IngestContext<R> {
    fn clone(&self) -> Self {
        Self {
            router: self.router.clone(),
            routing_configs: self.routing_configs.clone(),
            pass_raw_samples: self.pass_raw_samples,
        }
    }
}

/// An ingest source for the precompute engine.
///
/// Implementors decode incoming data (HTTP, file, etc.) and push it
/// into the engine via [`route_decoded_samples`].
#[allow(clippy::double_must_use)]
#[async_trait::async_trait]
pub trait IngestSource<R: SeriesRouting = RoutingConfigSet>: Send + Sync {
    async fn run(
        self: Box<Self>,
        ctx: IngestContext<R>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Group decoded samples by (agg_id, group_key) and route them to workers.
///
/// Returns an error if the router fails to deliver any message.
pub(crate) async fn route_decoded_samples<R: SeriesRouting>(
    ctx: &IngestContext<R>,
    samples: Vec<DecodedSample>,
    ingest_received_at: Instant,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if samples.is_empty() {
        return Ok(());
    }

    if ctx.pass_raw_samples {
        let mut by_series: HashMap<&str, Vec<(i64, f64)>> = HashMap::new();
        for s in &samples {
            by_series
                .entry(&s.labels)
                .or_default()
                .push((s.timestamp_ms, s.value));
        }
        let messages: Vec<WorkerMessage<R::Build>> = by_series
            .into_iter()
            .map(|(k, v)| WorkerMessage::RawSamples {
                series_key: k.to_string(),
                samples: v,
                ingest_received_at,
            })
            .collect();
        ctx.router
            .route_group_batch(messages, ingest_received_at)
            .await?;
        return Ok(());
    }

    // Group-by mode: for each sample, find matching agg configs and group by
    // (agg_id, group_key). This is the equivalent of Arroyo's GROUP BY.
    //
    // Key: (agg_id, group_key) → Vec<(series_key, timestamp_ms, value)>
    type GroupKey = (u64, String);
    type SampleTuple = (String, i64, f64);
    let mut by_group: HashMap<GroupKey, Vec<SampleTuple>> = HashMap::new();

    // Load routing configs once per request (lock-free ArcSwap read).
    let routing_configs = ctx.routing_configs.load();

    // On first batch: log routing vs sample metric to diagnose mismatches.
    static FIRST_BATCH_LOGGED: AtomicBool = AtomicBool::new(false);
    if !FIRST_BATCH_LOGGED.swap(true, Ordering::Relaxed) {
        if let Some(first) = samples.first() {
            routing_configs.log_routing_diagnostic(&first.labels);
        }
    }

    let mut matched_samples: usize = 0;
    for s in &samples {
        routing_configs.route_series(&s.labels, |agg_id, group_key| {
            matched_samples += 1;
            by_group.entry((agg_id, group_key)).or_default().push((
                s.labels.clone(),
                s.timestamp_ms,
                s.value,
            ));
        });
    }

    debug!(
        total_samples = samples.len(),
        matched_samples,
        groups_formed = by_group.len(),
        "routing: batch match summary"
    );

    let messages: Vec<WorkerMessage<R::Build>> = by_group
        .into_iter()
        .map(
            |((agg_id, group_key), samples)| WorkerMessage::GroupSamples {
                agg_id,
                group_key,
                samples,
                ingest_received_at,
            },
        )
        .collect();

    ctx.router
        .route_group_batch(messages, ingest_received_at)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::aggregation_config::AggregationConfig;
    use asap_types::enums::{AggregationType, WindowType};
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;
    use tokio::sync::mpsc::error::TryRecvError;

    fn aggregation_config(spatial_filter: &str) -> AggregationConfig {
        AggregationConfig::new(
            7,
            AggregationType::Sum,
            "Sum".to_string(),
            HashMap::new(),
            KeyByLabelNames::new(vec![]),
            KeyByLabelNames::new(vec![]),
            KeyByLabelNames::new(vec![]),
            String::new(),
            1_000,
            1_000,
            WindowType::Tumbling,
            spatial_filter.to_string(),
            "cpu_usage".to_string(),
            None,
            None,
            None,
            None,
        )
    }

    fn sample(labels: &str, timestamp_ms: i64, value: f64) -> DecodedSample {
        DecodedSample {
            labels: labels.to_string(),
            timestamp_ms,
            value,
        }
    }

    async fn routed_samples(
        spatial_filter: &str,
        input_samples: Vec<DecodedSample>,
    ) -> Vec<(String, i64, f64)> {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
        let context = IngestContext {
            router: SeriesRouter::new(vec![sender]),
            routing_configs: Arc::new(ArcSwap::from_pointee(
                RoutingConfigSet::from_aggregation_configs(vec![aggregation_config(
                    spatial_filter,
                )])
                .expect("valid test spatial filter"),
            )),
            pass_raw_samples: false,
        };

        route_decoded_samples(&context, input_samples, Instant::now())
            .await
            .expect("routing should succeed");

        let message = receiver.recv().await.expect("one routed message");
        let WorkerMessage::GroupSamples {
            agg_id,
            group_key,
            samples,
            ..
        } = message
        else {
            panic!("normal routing should produce GroupSamples");
        };

        assert_eq!(agg_id, 7);
        assert_eq!(group_key, "");
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
        samples
    }

    #[tokio::test]
    async fn matching_metric_with_no_spatial_filter_routes_once_to_its_aggregation() {
        let samples = routed_samples(
            "",
            vec![sample("cpu_usage{instance=\"a\",job=\"api\"}", 1_000, 42.0)],
        )
        .await;
        assert_eq!(
            samples,
            vec![(
                ("cpu_usage{instance=\"a\",job=\"api\"}").to_string(),
                1_000,
                42.0
            )]
        );
    }

    #[tokio::test]
    async fn planner_style_spatial_filter_routes_only_equal_label_values() {
        let samples = routed_samples(
            r#"job="api""#,
            vec![
                sample("cpu_usage{job=\"api\"}", 1_000, 1.0),
                sample("cpu_usage{job=\"worker\"}", 2_000, 2.0),
            ],
        )
        .await;

        assert_eq!(
            samples,
            vec![("cpu_usage{job=\"api\"}".to_string(), 1_000, 1.0)]
        );
    }

    #[tokio::test]
    async fn inequality_spatial_filter_routes_only_different_label_values() {
        let samples = routed_samples(
            r#"{job!="api"}"#,
            vec![
                sample("cpu_usage{job=\"api\"}", 1_000, 1.0),
                sample("cpu_usage{job=\"worker\"}", 2_000, 2.0),
                sample("cpu_usage{instance=\"a\"}", 3_000, 3.0),
            ],
        )
        .await;

        assert_eq!(
            samples,
            vec![
                ("cpu_usage{job=\"worker\"}".to_string(), 2_000, 2.0),
                ("cpu_usage{instance=\"a\"}".to_string(), 3_000, 3.0),
            ]
        );
    }

    #[tokio::test]
    async fn non_empty_regex_spatial_filter_drops_empty_label_values() {
        let samples = routed_samples(
            r#"{job=~".+"}"#,
            vec![
                sample("cpu_usage{job=\"api\"}", 1_000, 1.0),
                sample("cpu_usage{job=\"\"}", 2_000, 2.0),
                sample("cpu_usage{instance=\"a\"}", 3_000, 3.0),
            ],
        )
        .await;

        assert_eq!(
            samples,
            vec![("cpu_usage{job=\"api\"}".to_string(), 1_000, 1.0)]
        );
    }

    #[tokio::test]
    async fn alternation_regex_spatial_filter_routes_only_list_members() {
        let samples = routed_samples(
            r#"{job=~"user-service|order-service|payment-service"}"#,
            vec![
                sample("cpu_usage{job=\"user-service\"}", 1_000, 1.0),
                sample("cpu_usage{job=\"inventory-service\"}", 2_000, 2.0),
                sample("cpu_usage{job=\"payment-service\"}", 3_000, 3.0),
            ],
        )
        .await;

        assert_eq!(
            samples,
            vec![
                ("cpu_usage{job=\"user-service\"}".to_string(), 1_000, 1.0),
                ("cpu_usage{job=\"payment-service\"}".to_string(), 3_000, 3.0,),
            ]
        );
    }

    #[tokio::test]
    async fn starts_with_regex_spatial_filter_routes_only_matching_prefixes() {
        let samples = routed_samples(
            r#"{interface=~"eth0.*"}"#,
            vec![
                sample("cpu_usage{interface=\"eth0\"}", 1_000, 1.0),
                sample("cpu_usage{interface=\"eth0.100\"}", 2_000, 2.0),
                sample("cpu_usage{interface=\"ens3\"}", 3_000, 3.0),
            ],
        )
        .await;

        assert_eq!(
            samples,
            vec![
                ("cpu_usage{interface=\"eth0\"}".to_string(), 1_000, 1.0),
                ("cpu_usage{interface=\"eth0.100\"}".to_string(), 2_000, 2.0,),
            ]
        );
    }

    #[tokio::test]
    async fn sample_routes_to_every_matching_spatial_filter_config() {
        let first = aggregation_config(r#"{job="api"}"#);
        let mut second = aggregation_config(r#"{status=~"5.."}"#);
        second.aggregation_id = 8;
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
        let context = IngestContext {
            router: SeriesRouter::new(vec![sender]),
            routing_configs: Arc::new(ArcSwap::from_pointee(
                RoutingConfigSet::from_aggregation_configs(vec![first, second])
                    .expect("valid test spatial filters"),
            )),
            pass_raw_samples: false,
        };

        route_decoded_samples(
            &context,
            vec![sample("cpu_usage{job=\"api\",status=\"500\"}", 1_000, 1.0)],
            Instant::now(),
        )
        .await
        .expect("routing should succeed");

        let mut aggregation_ids = Vec::new();
        for _ in 0..2 {
            let WorkerMessage::GroupSamples {
                agg_id, samples, ..
            } = receiver.recv().await.expect("one routed message")
            else {
                panic!("normal routing should produce GroupSamples");
            };
            assert_eq!(
                samples,
                vec![(
                    "cpu_usage{job=\"api\",status=\"500\"}".to_string(),
                    1_000,
                    1.0,
                )]
            );
            aggregation_ids.push(agg_id);
        }
        aggregation_ids.sort_unstable();
        assert_eq!(aggregation_ids, vec![7, 8]);
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn invalid_spatial_filters_are_rejected_when_routing_configs_are_built() {
        let invalid_syntax =
            RoutingConfigSet::from_aggregation_configs(vec![aggregation_config(r#"{job=}"#)]);
        assert!(invalid_syntax.is_err());

        let metric_matcher = RoutingConfigSet::from_aggregation_configs(vec![aggregation_config(
            r#"{__name__="other_metric"}"#,
        )]);
        assert!(metric_matcher.is_err());

        let selector_or = RoutingConfigSet::from_aggregation_configs(vec![aggregation_config(
            r#"{job="api" or job="worker"}"#,
        )]);
        assert!(selector_or.is_err());
    }
}
