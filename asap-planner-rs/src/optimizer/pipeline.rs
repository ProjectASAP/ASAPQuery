use asap_types::inference_config::InferenceConfig;
use asap_types::streaming_config::StreamingConfig;
use thiserror::Error;

use crate::config::input::ControllerConfig;

use super::aqe_extractor::{extract_aqes, RQE};
use super::atomic_costs::AtomicCostTable;
use super::cost_model::CostWeights;
use super::greedy::greedy_assign;
use super::label_set_facts::{LabelSetFacts, LabelSetFactsError, LabelSetKey};
use super::solution::{OptimizerItem, OptimizerSolution};
use super::translator::{translate, TranslationSummary};

#[derive(Debug, Error)]
pub enum OptimizerPipelineError {
    #[error(transparent)]
    LabelSetFacts(#[from] LabelSetFactsError),
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
/// The workload's `metrics:` hints supply the label schema; `facts` supply each
/// item's group cardinality and series count.
pub fn run_greedy_pipeline(
    config: &ControllerConfig,
    facts: &LabelSetFacts,
    scrape_interval_ms: u64,
    atomic_cost_table: &AtomicCostTable,
) -> Result<(StreamingConfig, InferenceConfig), OptimizerPipelineError> {
    let aqes = extract_hinted_items(config, scrape_interval_ms)?;

    let item_facts = facts.resolve(&aqes, scrape_interval_ms)?;
    for (aqe, item) in aqes.iter().zip(&item_facts) {
        tracing::info!(
            key = %LabelSetKey::from_requirements(&aqe.requirements),
            output_group_count = item.output_group_count,
            topk_by_group_count = ?item.topk_by_group_count,
            arrival_rate_per_sec = item.arrival_rate_per_sec,
            "optimizer label-set facts"
        );
    }

    let solution = greedy_assign(
        aqes,
        scrape_interval_ms,
        atomic_cost_table,
        &CostWeights::default(),
        &item_facts,
    )?;

    Ok(finish_pipeline(solution, "greedy"))
}

/// The workload's optimizer items, with labels resolved from its `metrics:`
/// hints. Every workload metric must have a hint.
pub(super) fn extract_hinted_items(
    config: &ControllerConfig,
    scrape_interval_ms: u64,
) -> Result<Vec<OptimizerItem>, OptimizerPipelineError> {
    if config.metrics.is_none() {
        return Err(LabelSetFactsError::MissingMetricHints.into());
    }
    let schema = config.schema_from_hints();
    let rqes = config_to_rqes(config);
    let aqes = extract_aqes(&rqes, &schema, scrape_interval_ms)?;

    // Requirement extraction treats an unknown metric as having no labels,
    // which would silently mis-resolve `without (...)` and plain selectors.
    let mut unhinted: Vec<String> = aqes
        .iter()
        .map(|aqe| aqe.requirements.metric.clone())
        .filter(|metric| schema.get_labels(metric).is_none())
        .collect();
    if !unhinted.is_empty() {
        unhinted.sort();
        unhinted.dedup();
        return Err(LabelSetFactsError::MetricsWithoutHints(unhinted).into());
    }
    Ok(aqes)
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
                latency_sla_ms: qg.controller_options.latency_sla_ms,
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

    fn with_hints(mut config: ControllerConfig, hints: &[(&str, &[&str])]) -> ControllerConfig {
        use crate::config::input::MetricDefinition;

        config.metrics = Some(
            hints
                .iter()
                .map(|(metric, labels)| MetricDefinition {
                    metric: metric.to_string(),
                    labels: labels.iter().map(|l| l.to_string()).collect(),
                })
                .collect(),
        );
        config
    }

    // `min_over_time` keeps every label, so the grouping is the hinted [job].
    const METRIC_FACTS: &str = r#"
series:
  - {metric: metric, spatial_filter: "", series_count: 4}
groups:
  - {metric: metric, spatial_filter: "", grouping_labels: [job], cardinality: 4}
"#;

    #[test]
    fn greedy_pipeline_deploys_a_config_for_a_mergeable_aqe() {
        let config = with_hints(
            make_config(&[("min_over_time(metric[5m])", 60_000)]),
            &[("metric", &["job"])],
        );
        let facts = LabelSetFacts::from_yaml(METRIC_FACTS).unwrap();
        let (streaming, inference) =
            run_greedy_pipeline(&config, &facts, 60_000, &AtomicCostTable::default()).unwrap();
        assert!(!streaming.get_all_aggregation_configs().is_empty());
        assert!(!inference.query_configs.is_empty());
    }

    #[test]
    fn greedy_pipeline_requires_metric_hints() {
        let config = make_config(&[("min_over_time(metric[5m])", 60_000)]);
        let facts = LabelSetFacts::from_yaml(METRIC_FACTS).unwrap();
        assert!(matches!(
            run_greedy_pipeline(&config, &facts, 60_000, &AtomicCostTable::default()),
            Err(OptimizerPipelineError::LabelSetFacts(
                LabelSetFactsError::MissingMetricHints
            ))
        ));
    }

    #[test]
    fn greedy_pipeline_fails_when_workload_metric_has_no_hint() {
        let config = with_hints(
            make_config(&[
                ("min_over_time(metric[5m])", 60_000),
                ("max_over_time(unhinted[5m])", 60_000),
            ]),
            &[("metric", &["job"])],
        );
        let facts = LabelSetFacts::from_yaml(METRIC_FACTS).unwrap();
        assert!(matches!(
            run_greedy_pipeline(&config, &facts, 60_000, &AtomicCostTable::default()),
            Err(OptimizerPipelineError::LabelSetFacts(
                LabelSetFactsError::MetricsWithoutHints(metrics)
            )) if metrics == ["unhinted"]
        ));
    }

    #[test]
    fn greedy_pipeline_fails_when_facts_do_not_cover_workload() {
        let config = with_hints(
            make_config(&[("sum by (instance) (metric)", 60_000)]),
            &[("metric", &["job", "instance"])],
        );
        let facts = LabelSetFacts::from_yaml(METRIC_FACTS).unwrap();
        assert!(matches!(
            run_greedy_pipeline(&config, &facts, 60_000, &AtomicCostTable::default()),
            Err(OptimizerPipelineError::LabelSetFacts(
                LabelSetFactsError::MissingFacts(_)
            ))
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
