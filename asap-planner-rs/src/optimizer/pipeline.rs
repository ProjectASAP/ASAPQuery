use asap_types::inference_config::InferenceConfig;
use asap_types::streaming_config::StreamingConfig;
use asap_types::PromQLSchema;

use crate::config::input::ControllerConfig;

use super::aqe_extractor::{extract_aqes, Rqe};
use super::solution::OptimizerSolution;
use super::translator::{translate, TranslationSummary};

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
) -> (StreamingConfig, InferenceConfig) {
    let rqes = config_to_rqes(config);
    let aqes = extract_aqes(&rqes, schema);
    let solution = OptimizerSolution::all_exact(aqes);

    let summary = TranslationSummary::from_solution(&solution);
    tracing::info!(
        num_deployed_configs = summary.num_deployed_configs,
        num_sketch_assignments = summary.num_sketch_assignments,
        num_exact_fallbacks = summary.num_exact_fallbacks,
        "optimizer pipeline: all-EXACT solution produced"
    );

    translate(&solution)
}

/// Convert a `ControllerConfig`'s query groups into a flat list of RQEs.
/// Each (query, repetition_delay) pair becomes one RQE.
fn config_to_rqes(config: &ControllerConfig) -> Vec<Rqe> {
    config
        .query_groups
        .iter()
        .flat_map(|qg| {
            qg.queries.iter().map(|q| Rqe {
                query_string: q.clone(),
                t_repeat_secs: qg.repetition_delay,
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
                repetition_delay: *t,
                controller_options: Default::default(),
                step: None,
                range_duration: None,
            })
            .collect();

        ControllerConfig {
            query_groups,
            sketch_parameters: None,
            aggregate_cleanup: None,
            metrics: None,
            existing_streaming_config: None,
            existing_inference_config: None,
        }
    }

    #[test]
    fn all_exact_pipeline_produces_empty_streaming_config() {
        let config = make_config(&[("sum_over_time(metric[5m])", 60), ("sum(other_metric)", 30)]);
        let schema = PromQLSchema::new();
        let (streaming, _inference) = run_all_exact_pipeline(&config, &schema);
        // All-EXACT: no streaming configs deployed.
        assert!(streaming.get_all_aggregation_configs().is_empty());
    }

    #[test]
    fn config_to_rqes_flattens_groups() {
        let config = make_config(&[("sum_over_time(a[5m])", 60), ("sum_over_time(b[5m])", 30)]);
        let rqes = config_to_rqes(&config);
        assert_eq!(rqes.len(), 2);
        assert_eq!(rqes[0].t_repeat_secs, 60);
        assert_eq!(rqes[1].t_repeat_secs, 30);
    }
}
