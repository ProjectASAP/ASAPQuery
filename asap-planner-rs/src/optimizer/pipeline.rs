use asap_types::inference_config::InferenceConfig;
use asap_types::streaming_config::StreamingConfig;
use asap_types::PromQLSchema;

use crate::config::input::ControllerConfig;

use super::aqe_extractor::{extract_aqes, RQE};
use super::atomic_costs::AtomicCostTable;
use super::cost_model::CostWeights;
use super::dataset::SeriesDataset;
use super::greedy::greedy_assign;
use super::solution::{OptimizerSolution, AQE};
use super::translator::{translate, TranslationSummary};

/// Shared shell for optimizer pipelines: RQEs → AQEs → `solve` → deployment
/// artifacts, with a uniform log line. `solver_name` only affects the log.
fn run_pipeline(
    config: &ControllerConfig,
    schema: &PromQLSchema,
    scrape_interval_ms: u64,
    solver_name: &str,
    solve: impl FnOnce(Vec<AQE>) -> OptimizerSolution,
) -> (StreamingConfig, InferenceConfig) {
    let rqes = config_to_rqes(config);
    let aqes = extract_aqes(&rqes, schema, scrape_interval_ms);
    let solution = solve(aqes);

    finish_pipeline(solution, solver_name)
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
        num_exact_fallbacks = summary.num_exact_fallbacks,
        estimated_ingest_cost_per_sec = solution.estimated_ingest_cost_per_sec,
        estimated_total_cost_per_sec = solution.estimated_total_cost_per_sec,
        "optimizer pipeline: solution produced"
    );

    translate(&solution)
}

/// Run the all-EXACT optimizer pipeline (Phase 1 scaffolding).
///
/// Converts a `ControllerConfig` into `(StreamingConfig, InferenceConfig)` via
/// the optimizer path: RQEs → AQEs → all-EXACT solution → deployment artifacts.
///
/// No streaming configs are deployed — every AQE falls back to raw data at
/// query time. This validates the end-to-end pipeline plumbing before real
/// sketch selection logic is added in Phase 2.
pub fn run_all_exact_pipeline(
    config: &ControllerConfig,
    schema: &PromQLSchema,
    scrape_interval_ms: u64,
) -> (StreamingConfig, InferenceConfig) {
    run_pipeline(
        config,
        schema,
        scrape_interval_ms,
        "all-EXACT",
        OptimizerSolution::all_exact,
    )
}

/// Run the greedy optimizer pipeline (Phase 2): each AQE is assigned, independently,
/// to its cheapest feasible candidate config (or to the EXACT fallback).
///
/// No cross-AQE sharing — every deployed sketch serves exactly one AQE, even
/// if two AQEs could share one. The Phase 3 MIP finds sharing opportunities.
///
/// The dataset supplies each AQE's metric schema and label-group count before
/// candidate selection. `arrival_rate_hz` remains a uniform placeholder until
/// per-config scrape-rate data is available.
pub fn run_greedy_pipeline(
    config: &ControllerConfig,
    dataset: &SeriesDataset,
    scrape_interval_ms: u64,
    arrival_rate_hz: f64,
    atomic_cost_table: &AtomicCostTable,
) -> Result<(StreamingConfig, InferenceConfig), super::dataset::DatasetError> {
    dataset.validate_metric_hints(config.metrics.as_deref())?;
    let schema = dataset.schema();
    let rqes = config_to_rqes(config);
    let aqes = extract_aqes(&rqes, &schema, scrape_interval_ms);
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
    );

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
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn all_exact_pipeline_produces_empty_streaming_config() {
        let config = make_config(&[
            ("sum_over_time(metric[5m])", 60_000),
            ("sum(other_metric)", 30_000),
        ]);
        let schema = PromQLSchema::new();
        let (streaming, _inference) = run_all_exact_pipeline(&config, &schema, 15_000);
        // All-EXACT: no streaming configs deployed.
        assert!(streaming.get_all_aggregation_configs().is_empty());
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
            Err(super::super::dataset::DatasetError::MissingMetric(metric)) if metric == "metric"
        ));
    }

    #[test]
    fn spatial_only_aqe_gets_explicit_range_from_pipeline() {
        let config = make_config(&[("sum(metric)", 60_000)]);
        let rqes = config_to_rqes(&config);
        let aqes = extract_aqes(&rqes, &PromQLSchema::new(), 15_000);
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
