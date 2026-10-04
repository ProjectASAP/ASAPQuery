use asap_types::inference_config::InferenceConfig;
use asap_types::streaming_config::StreamingConfig;
use asap_types::PromQLSchema;

use crate::config::input::ControllerConfig;

use super::aqe_extractor::{extract_aqes, RQE};
use super::atomic_costs::AtomicCostTable;
use super::cost_model::CostWeights;
use super::greedy::greedy_assign;
use super::label_set_facts::{LabelSetFacts, LabelSetFactsError, LabelSetKey};
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
/// The workload's `metrics:` hints supply the label schema; `facts` supply each
/// AQE's group cardinality and series count.
pub fn run_greedy_pipeline(
    config: &ControllerConfig,
    facts: &LabelSetFacts,
    scrape_interval_ms: u64,
    atomic_cost_table: &AtomicCostTable,
) -> Result<(StreamingConfig, InferenceConfig), LabelSetFactsError> {
    if config.metrics.is_none() {
        return Err(LabelSetFactsError::MissingMetricHints);
    }
    let schema = config.schema_from_hints();
    let rqes = config_to_rqes(config);
    let aqes = extract_aqes(&rqes, &schema, scrape_interval_ms);

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
        return Err(LabelSetFactsError::MetricsWithoutHints(unhinted));
    }

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
            Err(LabelSetFactsError::MissingMetricHints)
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
            Err(LabelSetFactsError::MetricsWithoutHints(metrics)) if metrics == ["unhinted"]
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
            Err(LabelSetFactsError::MissingFacts(_))
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
