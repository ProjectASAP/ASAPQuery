use asap_types::inference_config::InferenceConfig;
use asap_types::streaming_config::StreamingConfig;
use thiserror::Error;

use crate::config::input::ControllerConfig;

use super::aqe_extractor::{extract_aqes, RQE};
use super::atomic_costs::AtomicCostTable;
use super::cost_model::CostWeights;
use super::dataset::SeriesDataset;
use super::greedy::greedy_assign;
use super::solution::OptimizerSolution;
use super::translator::{translate, TranslationSummary};

#[derive(Debug, Error)]
pub enum OptimizerPipelineError {
    #[error(transparent)]
    Dataset(#[from] super::dataset::DatasetError),
    #[error(transparent)]
    Optimizer(#[from] super::error::OptimizerError),
}

fn finish_pipeline(
    solution: OptimizerSolution,
    solver_name: &str,
) -> (StreamingConfig, InferenceConfig) {
    let summary = TranslationSummary::from_solution(&solution);
    tracing::info!(
        solver = solver_name,
        num_deployed_configs = summary.num_deployed_configs,
        num_sketch_assignments = summary.num_sketch_assignments,
        estimated_ingest_cost_per_sec = solution.estimated_ingest_cost_per_sec,
        estimated_total_cost_per_sec = solution.estimated_total_cost_per_sec,
        "optimizer pipeline: solution produced"
    );

    translate(&solution)
}

/// Run the greedy optimizer pipeline: each item is assigned independently to
/// its cheapest eligible streaming config.
///
/// No cross-item sharing — every deployed sketch serves exactly one item, even
/// if two AQEs could share one. The Phase 3 MIP finds sharing opportunities.
///
/// The dataset supplies each item's metric schema and label-group count before
/// candidate selection. `arrival_rate_hz` remains a uniform placeholder until
/// per-config scrape-rate data is available.
pub fn run_greedy_pipeline(
    config: &ControllerConfig,
    dataset: &SeriesDataset,
    scrape_interval_ms: u64,
    arrival_rate_hz: f64,
    atomic_cost_table: &AtomicCostTable,
) -> Result<(StreamingConfig, InferenceConfig), OptimizerPipelineError> {
    dataset.validate_metric_hints(config.metrics.as_deref())?;
    let schema = dataset.schema();
    let rqes = config_to_rqes(config);
    let aqes = extract_aqes(&rqes, &schema, scrape_interval_ms)?;
    let label_group_counts = dataset.profile_aqes(&aqes)?;

    for (key, count) in &label_group_counts {
        tracing::info!(
            metric = %key.metric,
            spatial_filter = %key.spatial_filter_normalized,
            grouping_labels = ?key.grouping_labels.labels,
            label_group_count = *count,
            "optimizer dataset profile"
        );
    }

    let solution = greedy_assign(
        aqes,
        scrape_interval_ms,
        arrival_rate_hz,
        atomic_cost_table,
        &CostWeights::default(),
        &label_group_counts,
    )?;

    Ok(finish_pipeline(solution, "greedy"))
}

/// Convert a `ControllerConfig`'s query groups into a flat list of RQEs.
/// Each (query, repetition_delay_ms) pair becomes one RQE.
fn config_to_rqes(config: &ControllerConfig) -> Vec<RQE> {
    config
        .query_groups
        .iter()
        .flat_map(|qg| {
            qg.queries.iter().map(|q| RQE {
                query_string: q.clone(),
                t_repeat_ms: qg.repetition_delay_ms,
                accuracy_sla: qg.controller_options.accuracy_sla,
                latency_sla: qg.controller_options.latency_sla,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::PromQLSchema;

    fn make_config(queries: &[(&str, u64)]) -> ControllerConfig {
        use crate::config::input::QueryGroup;

        let query_groups = queries
            .iter()
            .map(|(q, t)| QueryGroup {
                id: None,
                queries: vec![q.to_string()],
                repetition_delay_ms: *t,
                controller_options: Default::default(),
                step_ms: None,
                range_duration_ms: None,
            })
            .collect();

        ControllerConfig {
            query_groups,
            windowing: None,
            sketch_parameters: None,
            aggregate_cleanup: None,
            metrics: None,
            existing_streaming_config: None,
            existing_inference_config: None,
        }
    }

    #[test]
    fn greedy_pipeline_deploys_a_config_for_a_mergeable_aqe() {
        let config = make_config(&[("min_over_time(metric[5m])", 60_000)]);
        let dataset = SeriesDataset::from_reader("metric,job\nmetric,api\n".as_bytes()).unwrap();
        let (streaming, inference) =
            run_greedy_pipeline(&config, &dataset, 60_000, 1.0, &AtomicCostTable::default())
                .unwrap();
        assert!(!streaming.get_all_aggregation_configs().is_empty());
        assert!(!inference.query_configs.is_empty());
    }

    #[test]
    fn greedy_pipeline_fails_when_dataset_does_not_match_workload() {
        let config = make_config(&[("min_over_time(metric[5m])", 60_000)]);
        let dataset = SeriesDataset::from_reader("metric,job\nother,api\n".as_bytes()).unwrap();

        assert!(matches!(
            run_greedy_pipeline(&config, &dataset, 60_000, 1.0, &AtomicCostTable::default()),
            Err(OptimizerPipelineError::Dataset(super::super::dataset::DatasetError::MissingMetric(metric))) if metric == "metric"
        ));
    }

    #[test]
    fn spatial_only_aqe_gets_explicit_range_from_pipeline() {
        let config = make_config(&[("sum(metric)", 60_000)]);
        let rqes = config_to_rqes(&config);
        let aqes = extract_aqes(&rqes, &PromQLSchema::new(), 15_000).unwrap();
        assert_eq!(aqes.len(), 1);
        assert_eq!(aqes[0].requirements.data_range_ms, 15_000);
    }

    #[test]
    fn config_to_rqes_flattens_groups() {
        let config = make_config(&[
            ("sum_over_time(a[5m])", 60_000),
            ("sum_over_time(b[5m])", 30_000),
        ]);
        let rqes = config_to_rqes(&config);
        assert_eq!(rqes.len(), 2);
        assert_eq!(rqes[0].t_repeat_ms, 60_000);
        assert_eq!(rqes[1].t_repeat_ms, 30_000);
    }
}
