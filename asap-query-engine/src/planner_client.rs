use anyhow::Result;
use asap_planner::{
    build_schema_from_prometheus, Controller, ControllerConfig, PlannerOutput, RuntimeOptions,
};
use sketch_db_common::enums::QueryLanguage;
use sketch_db_common::inference_config::InferenceConfig;
use sketch_db_common::streaming_config::StreamingConfig;
use tracing::warn;

pub struct PlannerResult {
    pub streaming_config: StreamingConfig,
    pub inference_config: InferenceConfig,
    pub punted_queries: Vec<String>,
}

#[async_trait::async_trait]
pub trait PlannerClient: Send + Sync {
    async fn plan(&self, config: ControllerConfig) -> Result<PlannerResult>;
}

pub struct LocalPlannerClient {
    runtime_options: RuntimeOptions,
    query_language: QueryLanguage,
    prometheus_url: String,
}

impl LocalPlannerClient {
    pub fn new(
        runtime_options: RuntimeOptions,
        query_language: QueryLanguage,
        prometheus_url: String,
    ) -> Self {
        Self {
            runtime_options,
            query_language,
            prometheus_url,
        }
    }
}

#[async_trait::async_trait]
impl PlannerClient for LocalPlannerClient {
    async fn plan(&self, config: ControllerConfig) -> Result<PlannerResult> {
        let opts = self.runtime_options.clone();
        let query_language = self.query_language;
        let prometheus_url = self.prometheus_url.clone();

        let output: PlannerOutput = tokio::task::spawn_blocking(move || {
            let all_queries: Vec<String> = config
                .query_groups
                .iter()
                .flat_map(|qg| qg.queries.clone())
                .collect();
            let mut schema = match build_schema_from_prometheus(&prometheus_url, &all_queries) {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        "Prometheus metric discovery failed, falling back to config hints: {}",
                        e
                    );
                    config.schema_from_hints()
                }
            };
            // Fall back to config-file hints for metrics not found in Prometheus.
            if let Some(metric_hints) = &config.metrics {
                for hint in metric_hints {
                    if !schema.config.contains_key(&hint.metric) {
                        schema = schema.add_metric(
                            hint.metric.clone(),
                            promql_utilities::data_model::KeyByLabelNames::new(hint.labels.clone()),
                        );
                    }
                }
            }
            let controller = Controller::new(config, schema, opts);
            controller.generate()
        })
        .await??;

        let inference_config = output.to_inference_config(query_language)?;
        let streaming_config = output.to_streaming_config(query_language)?;
        let punted_queries = output
            .punted_queries
            .iter()
            .map(|p| p.query.clone())
            .collect();

        Ok(PlannerResult {
            streaming_config,
            inference_config,
            punted_queries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_planner::config::input::{ControllerOptions, MetricDefinition, QueryGroup};
    use asap_planner::ControllerConfig;
    use asap_planner::{RuntimeOptions, StreamingEngine};

    fn sample_controller_config() -> ControllerConfig {
        ControllerConfig {
            query_groups: vec![QueryGroup {
                id: Some(1),
                queries: vec!["sum(rate(http_requests_total[5m]))".to_string()],
                repetition_delay: 60,
                controller_options: ControllerOptions::default(),
                step: None,
                range_duration: None,
            }],
            metrics: Some(vec![MetricDefinition {
                metric: "http_requests_total".to_string(),
                labels: vec!["method".to_string(), "status".to_string()],
            }]),
            sketch_parameters: None,
            aggregate_cleanup: None,
        }
    }

    fn sample_runtime_options() -> RuntimeOptions {
        RuntimeOptions {
            prometheus_scrape_interval: 15,
            streaming_engine: StreamingEngine::Precompute,
            enable_punting: false,
            range_duration: 300,
            step: 15,
        }
    }

    #[test]
    fn test_controller_new_generate() {
        let config = sample_controller_config();
        let schema = config.schema_from_hints();
        let opts = sample_runtime_options();
        let controller = Controller::new(config, schema, opts);
        let output = controller.generate().expect("generate should succeed");

        assert!(output.streaming_aggregation_count() > 0);
        assert!(output.inference_query_count() > 0);
    }

    #[test]
    fn test_planner_output_struct_accessors() {
        let config = sample_controller_config();
        let schema = config.schema_from_hints();
        let opts = sample_runtime_options();
        let controller = Controller::new(config, schema, opts);
        let output = controller.generate().expect("generate should succeed");

        let inference = output
            .to_inference_config(QueryLanguage::promql)
            .expect("to_inference_config should succeed");
        assert!(!inference.query_configs.is_empty());

        let streaming = output
            .to_streaming_config(QueryLanguage::promql)
            .expect("to_streaming_config should succeed");
        assert!(!streaming.aggregation_configs.is_empty());
    }

    #[tokio::test]
    async fn test_local_planner_client() {
        // Use a dummy URL; the test config has metric hints so Prometheus discovery
        // failures are tolerated via the fallback path.
        let client = LocalPlannerClient::new(
            sample_runtime_options(),
            QueryLanguage::promql,
            "http://localhost:9090".to_string(),
        );
        let config = sample_controller_config();

        let result = client.plan(config).await.expect("plan should succeed");

        assert!(!result.streaming_config.aggregation_configs.is_empty());
        assert!(!result.inference_config.query_configs.is_empty());
        assert!(result.punted_queries.is_empty());
    }
}
