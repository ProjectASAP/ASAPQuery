use std::collections::HashMap;

use tracing::debug;

use super::atomic_costs::{resolve_atomic_costs, satisfies_accuracy_sla, AtomicCostTable};
use super::candidate_gen::enumerate_candidates_with_label_group_count;
use super::cost_model::{
    atomic_query_cpu_secs, ingest_cost, query_cost, total_cost_rate, AtomicCosts, CostWeights,
};
use super::dataset::DatasetError;
use super::dataset::ProfileKey;
use super::solution::{AQEAssignment, OptimizerSolution, AQE};

/// Greedily assign each AQE to its independently-cheapest candidate config.
///
/// No cross-AQE sharing: every deployed sketch serves exactly one AQE, even if
/// two AQEs could share one. The Phase 3 MIP finds sharing opportunities; this
/// is the v1 baseline.
///
/// `arrival_rate_hz` is the per-item arrival rate used for every candidate's IngestCost.
/// Real per-config rates need Prometheus scrape-rate × series-count data,
/// which isn't wired up yet — a single placeholder value is applied uniformly.
///
/// Each candidate is costed at its own `(sketch_type, params)` via
/// `atomic_cost_table` (see ASAPQuery#524) rather than one cost applied to
/// every candidate; a candidate whose config has no matching table entry is
/// dropped from consideration (`resolve_atomic_costs` returns `None`).
pub fn greedy_assign(
    aqes: Vec<AQE>,
    scrape_interval_ms: u64,
    arrival_rate_hz: f64,
    atomic_cost_table: &AtomicCostTable,
    weights: &CostWeights,
    label_group_counts: &HashMap<ProfileKey, u64>,
) -> Result<OptimizerSolution, DatasetError> {
    let mut solution = OptimizerSolution::empty();

    for aqe in aqes {
        let profile_key = ProfileKey::from_requirements(&aqe.requirements);
        let label_group_count = *label_group_counts.get(&profile_key).unwrap_or_else(|| {
            panic!(
                "missing dataset profile for metric '{}' and grouping labels {:?}",
                aqe.requirements.metric, aqe.requirements.grouping_labels.labels
            )
        });
        let candidates = enumerate_candidates_with_label_group_count(
            &aqe,
            scrape_interval_ms,
            label_group_count,
        );

        let (best, costs) = candidates
            .into_iter()
            .filter_map(|c| {
                // EXACT (config: None) always costs at the flat stub — it has
                // no sketch_type/params for the table to key on.
                let costs = match &c.config {
                    None => AtomicCosts::default(),
                    Some(cfg) => {
                        if !satisfies_accuracy_sla(
                            atomic_cost_table,
                            cfg.aggregation_type,
                            &cfg.parameters,
                            aqe.max_mean_rank_error.map(|error| 1.0 - error),
                        ) {
                            return None;
                        }
                        resolve_atomic_costs(
                            atomic_cost_table,
                            cfg.aggregation_type,
                            &cfg.parameters,
                        )?
                    }
                };
                let cost = total_cost_rate(&aqe, &c, arrival_rate_hz, &costs, weights);
                let request_cpu_secs = atomic_query_cpu_secs(&c, &costs);
                if aqe
                    .max_atomic_query_cpu_secs
                    .is_some_and(|limit| !limit.is_finite() || request_cpu_secs > limit)
                {
                    return None;
                }
                Some((c, costs, cost))
            })
            // total_cmp (not partial_cmp().unwrap()) so a stray NaN cost can't panic.
            .min_by(|(_, _, a), (_, _, b)| a.total_cmp(b))
            .map(|(c, costs, _)| (c, costs))
            .ok_or_else(|| DatasetError::NoFeasibleCandidate(aqe.requirements.metric.clone()))?;

        let ingest = ingest_cost(&best, arrival_rate_hz, &costs, weights);
        let query_rate = aqe.query_frequency_hz * query_cost(&aqe, &best, &costs, weights);
        let query_method = best.query_method.clone();

        let aggregation_id = best.config.map(|config| solution.register_config(config));

        debug!(
            metric = %aqe.requirements.metric,
            aggregation_id = ?aggregation_id,
            query_method = ?query_method,
            ingest_cost_per_sec = ingest,
            query_cost_per_sec = query_rate,
            "greedy: assigned AQE"
        );

        solution.estimated_ingest_cost_per_sec += ingest;
        solution.estimated_total_cost_per_sec += ingest + query_rate;

        solution.assignments.push(AQEAssignment {
            aqe,
            aggregation_id,
            query_method,
            estimated_query_cost_per_sec: query_rate,
        });
    }

    Ok(solution)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::optimizer::atomic_costs::AtomicCostEntry;
    use asap_types::query_requirements::QueryRequirements;
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::{AggregationType, Statistic};
    use std::collections::HashMap as StdHashMap;

    fn make_aqe(stat: Statistic, range_ms: u64, min_t: u64, freq_hz: f64) -> AQE {
        AQE {
            requirements: QueryRequirements {
                metric: "test_metric".into(),
                statistics: vec![stat],
                data_range_ms: range_ms,
                grouping_labels: KeyByLabelNames::empty(),
                spatial_filter_normalized: String::new(),
                topk_count_events: None,
            },
            query_strings: vec!["test_query".into()],
            query_frequency_hz: freq_hz,
            min_t_repeat_ms: min_t,
            t_repeat_gcd_ms: min_t,
            max_mean_rank_error: None,
            max_atomic_query_cpu_secs: None,
        }
    }

    #[test]
    fn assigns_unique_ids_to_each_deployed_config() {
        let aqes = vec![
            make_aqe(Statistic::Min, 300_000, 300_000, 1.0 / 60.0),
            make_aqe(Statistic::Max, 300_000, 300_000, 1.0 / 60.0),
        ];
        let solution = greedy_assign(
            aqes,
            60_000,
            1.0,
            &AtomicCostTable::default(),
            &CostWeights::default(),
            &HashMap::from([
                (
                    ProfileKey::from_requirements(
                        &make_aqe(Statistic::Min, 300_000, 300_000, 1.0 / 60.0).requirements,
                    ),
                    1,
                ),
                (
                    ProfileKey::from_requirements(
                        &make_aqe(Statistic::Max, 300_000, 300_000, 1.0 / 60.0).requirements,
                    ),
                    1,
                ),
            ]),
        )
        .unwrap();

        let mut seen_ids: StdHashMap<u64, ()> = StdHashMap::new();
        for id in solution.deployed_configs().keys() {
            assert!(
                seen_ids.insert(*id, ()).is_none(),
                "duplicate aggregation_id"
            );
        }
        assert_eq!(solution.assignments.len(), 2);
    }

    #[test]
    fn unsupported_multi_statistic_aqe_falls_back_to_exact() {
        let aqe = AQE {
            requirements: QueryRequirements {
                metric: "test_metric".into(),
                statistics: vec![Statistic::Sum, Statistic::Count], // avg-style, unsupported
                data_range_ms: 60_000,
                grouping_labels: KeyByLabelNames::empty(),
                spatial_filter_normalized: String::new(),
                topk_count_events: None,
            },
            query_strings: vec!["avg_query".into()],
            query_frequency_hz: 1.0 / 60.0,
            min_t_repeat_ms: 60_000,
            t_repeat_gcd_ms: 60_000,
            max_mean_rank_error: None,
            max_atomic_query_cpu_secs: None,
        };
        let solution = greedy_assign(
            vec![aqe.clone()],
            60_000,
            1.0,
            &AtomicCostTable::default(),
            &CostWeights::default(),
            &HashMap::from([(ProfileKey::from_requirements(&aqe.requirements), 1)]),
        )
        .unwrap();
        assert_eq!(solution.num_exact_fallback(), 1);
        assert!(solution.deployed_configs().is_empty());
    }

    #[test]
    fn missing_cms_with_heap_reference_cost_falls_back_to_exact() {
        // Regression coverage for #651: an uncosted CMS-with-heap candidate
        // must be dropped rather than inheriting the flat stub and winning.
        let aqe = make_aqe(Statistic::Topk, 60_000, 60_000, 1.0 / 60.0);
        let solution = greedy_assign(
            vec![aqe.clone()],
            60_000,
            1.0,
            &AtomicCostTable::default(),
            &CostWeights::default(),
            &HashMap::from([(ProfileKey::from_requirements(&aqe.requirements), 1)]),
        )
        .unwrap();

        assert_eq!(solution.num_exact_fallback(), 1);
        assert!(solution.deployed_configs().is_empty());
    }

    #[test]
    fn matching_cms_with_heap_reference_cost_can_be_deployed() {
        let table = vec![AtomicCostEntry {
            sketch: "cms-heap-topk-regularpath-vector2d".into(),
            sketch_config: serde_json::json!({
                "algorithm": "cms-heap-topk-regularpath-vector2d",
                "params": { "rows": 3, "cols": 512 }
            }),
            mem_bytes_per_instance: 1.0,
            insert_cpu_secs: 0.0,
            merge_cpu_secs: 0.0,
            query_cpu_secs: 0.0,
            query_accuracy: std::collections::BTreeMap::new(),
        }];
        let aqe = make_aqe(Statistic::Topk, 60_000, 60_000, 1.0 / 60.0);
        let solution = greedy_assign(
            vec![aqe.clone()],
            60_000,
            1.0,
            &table,
            &CostWeights::default(),
            &HashMap::from([(ProfileKey::from_requirements(&aqe.requirements), 1)]),
        )
        .unwrap();

        assert_eq!(solution.num_exact_fallback(), 0);
        assert_eq!(solution.deployed_configs().len(), 1);
        assert_eq!(
            solution
                .deployed_configs()
                .values()
                .next()
                .expect("one CMS-with-heap config deployed")
                .aggregation_type,
            AggregationType::CountMinSketchWithHeap
        );
    }

    #[test]
    fn rank_error_limit_rejects_cheaper_kll_candidate() {
        fn kll(k: u64, mean_rank_err: f64) -> AtomicCostEntry {
            AtomicCostEntry {
                sketch: "kll-percall".into(),
                sketch_config: serde_json::json!({
                    "algorithm": "kll-percall", "params": { "k": k }
                }),
                mem_bytes_per_instance: 1.0,
                insert_cpu_secs: 0.0,
                merge_cpu_secs: 0.0,
                query_cpu_secs: k as f64 * 1e-9,
                query_accuracy: std::collections::BTreeMap::from([(
                    "mean_rank_err".into(),
                    mean_rank_err,
                )]),
            }
        }

        let mut aqe = make_aqe(Statistic::Quantile, 60_000, 60_000, 1.0);
        aqe.max_mean_rank_error = Some(0.02);
        let solution = greedy_assign(
            vec![aqe.clone()],
            60_000,
            1.0,
            &vec![kll(200, 0.03), kll(500, 0.01)],
            &CostWeights::default(),
            &HashMap::from([(ProfileKey::from_requirements(&aqe.requirements), 1)]),
        )
        .unwrap();

        let config = solution.deployed_configs().values().next().unwrap();
        assert_eq!(config.aggregation_type, AggregationType::DatasketchesKLL);
        assert_eq!(config.parameters["K"], serde_json::Value::from(500));
    }

    #[test]
    fn infeasible_atomic_cpu_sla_returns_an_error() {
        let mut aqe = make_aqe(Statistic::Min, 60_000, 60_000, 1.0);
        aqe.max_atomic_query_cpu_secs = Some(0.0);
        let result = greedy_assign(
            vec![aqe.clone()],
            60_000,
            1.0,
            &AtomicCostTable::default(),
            &CostWeights::default(),
            &HashMap::from([(ProfileKey::from_requirements(&aqe.requirements), 1)]),
        );
        assert!(matches!(result, Err(DatasetError::NoFeasibleCandidate(_))));
    }
}
