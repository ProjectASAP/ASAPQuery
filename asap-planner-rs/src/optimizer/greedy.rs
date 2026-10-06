use tracing::debug;

use super::atomic_costs::{resolve_atomic_costs, AtomicCostTable};
use super::candidate_gen::enumerate_candidates_with_facts;
use super::cost_model::{ingest_cost, query_cost, total_cost_rate, AtomicCosts, CostWeights};
use super::error::{OptimizerError, UnservableItem};
use super::label_set_facts::ItemFacts;
use super::solution::{AQEAssignment, OptimizerItem, OptimizerSolution};

/// Greedily assign each AQE to its independently-cheapest candidate config.
///
/// No cross-AQE sharing: every deployed sketch serves exactly one AQE, even if
/// two AQEs could share one. The Phase 3 MIP finds sharing opportunities; this
/// is the v1 baseline.
///
/// `facts` supplies each AQE's group counts and arrival rate, index-aligned
/// with `aqes`.
///
/// Each candidate is costed at its own `(sketch_type, params)` via
/// `atomic_cost_table` (see ASAPQuery#524) rather than one cost applied to
/// every candidate; a candidate whose config has no matching table entry is
/// dropped from consideration (`resolve_atomic_costs` returns `None`).
pub fn greedy_assign(
    aqes: Vec<OptimizerItem>,
    scrape_interval_ms: u64,
    atomic_cost_table: &AtomicCostTable,
    weights: &CostWeights,
    facts: &[ItemFacts],
) -> Result<OptimizerSolution, OptimizerError> {
    assert_eq!(
        aqes.len(),
        facts.len(),
        "facts must be index-aligned with aqes"
    );
    let mut solution = OptimizerSolution::empty();
    let mut unservable_items = Vec::new();

    for (aqe, item_facts) in aqes.into_iter().zip(facts) {
        let arrival_rate_hz = item_facts.arrival_rate_per_sec;
        let candidates = enumerate_candidates_with_facts(&aqe, scrape_interval_ms, item_facts);

        let Some((best, costs)) = candidates
            .into_iter()
            .filter_map(|c| {
                // EXACT (config: None) always costs at the flat stub — it has
                // no sketch_type/params for the table to key on.
                let costs = match &c.config {
                    None => AtomicCosts::default(),
                    Some(cfg) => resolve_atomic_costs(
                        atomic_cost_table,
                        cfg.aggregation_type,
                        &cfg.parameters,
                        aqe.requirements.grouping_labels.len(),
                    )?,
                };
                let cost = total_cost_rate(&aqe, &c, arrival_rate_hz, &costs, weights);
                Some((c, costs, cost))
            })
            // total_cmp (not partial_cmp().unwrap()) so a stray NaN cost can't panic.
            .min_by(|(_, _, a), (_, _, b)| a.total_cmp(b))
            .map(|(c, costs, _)| (c, costs))
        else {
            unservable_items.push(UnservableItem {
                metric: aqe.requirements.metric.clone(),
                statistics: aqe.requirements.statistics.clone(),
                data_range_ms: aqe.requirements.data_range_ms,
                t_repeat_ms: aqe.t_repeat_ms,
                accuracy_sla: aqe.accuracy_sla,
                latency_sla_ms: aqe.latency_sla_ms,
                reason: "no candidate remained after structural and atomic-cost filters".into(),
            });
            continue;
        };

        let ingest = ingest_cost(&best, arrival_rate_hz, &costs, weights);
        let query_rate = aqe.query_frequency_hz * query_cost(&aqe, &best, &costs, weights);
        let query_method = best.query_method.clone();

        let aggregation_id = solution.register_config(
            best.config
                .expect("candidate configs are streaming configs"),
        );
        let key_aggregation_id = best
            .key_config
            .map(|config| solution.register_config(config));

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
            item: aqe,
            aggregation_id,
            key_aggregation_id,
            query_method,
            estimated_query_cost_per_sec: query_rate,
        });
    }

    if unservable_items.is_empty() {
        Ok(solution)
    } else {
        Err(OptimizerError::UnservableItems {
            items: unservable_items,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimizer::atomic_costs::AtomicCostEntry;
    use crate::optimizer::label_set_facts::ItemFacts;
    use asap_types::query_requirements::QueryRequirements;
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::{AggregationType, Statistic};
    use std::collections::HashMap as StdHashMap;

    fn make_aqe(stat: Statistic, range_ms: u64, min_t: u64, freq_hz: f64) -> OptimizerItem {
        OptimizerItem {
            requirements: QueryRequirements {
                metric: "test_metric".into(),
                statistics: vec![stat],
                data_range_ms: range_ms,
                grouping_labels: KeyByLabelNames::empty(),
                spatial_filter_normalized: String::new(),
                topk_count_events: None,
                topk_by_labels: None,
            },
            query_strings: vec!["test_query".into()],
            query_frequency_hz: freq_hz,
            t_repeat_ms: min_t,
            accuracy_sla: 0.0,
            latency_sla_ms: None,
        }
    }

    /// One group, one item/sec for every item.
    fn unit_facts(aqes: &[OptimizerItem]) -> Vec<ItemFacts> {
        aqes.iter()
            .map(|aqe| ItemFacts {
                output_group_count: 1,
                topk_by_group_count: aqe.requirements.topk_by_labels.as_ref().map(|_| 1),
                arrival_rate_per_sec: 1.0,
            })
            .collect()
    }

    #[test]
    fn assigns_unique_ids_to_each_deployed_config() {
        let aqes = vec![
            make_aqe(Statistic::Min, 300_000, 300_000, 1.0 / 60.0),
            make_aqe(Statistic::Max, 300_000, 300_000, 1.0 / 60.0),
        ];
        let facts = unit_facts(&aqes);
        let solution = greedy_assign(
            aqes,
            60_000,
            &AtomicCostTable::default(),
            &CostWeights::default(),
            &facts,
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
    fn unsupported_multi_statistic_item_is_unservable() {
        let aqe = OptimizerItem {
            requirements: QueryRequirements {
                metric: "test_metric".into(),
                statistics: vec![Statistic::Sum, Statistic::Count], // avg-style, unsupported
                data_range_ms: 60_000,
                grouping_labels: KeyByLabelNames::empty(),
                spatial_filter_normalized: String::new(),
                topk_count_events: None,
                topk_by_labels: None,
            },
            query_strings: vec!["avg_query".into()],
            query_frequency_hz: 1.0 / 60.0,
            t_repeat_ms: 60_000,
            accuracy_sla: 0.0,
            latency_sla_ms: None,
        };
        let error = greedy_assign(
            vec![aqe.clone()],
            60_000,
            &AtomicCostTable::default(),
            &CostWeights::default(),
            &unit_facts(&[aqe]),
        );
        assert!(matches!(error, Err(OptimizerError::UnservableItems { .. })));
    }

    #[test]
    fn missing_cms_with_heap_reference_cost_is_unservable() {
        // Regression coverage for #651: an uncosted CMS-with-heap candidate
        // must be dropped rather than inheriting the flat stub and winning.
        let aqe = make_aqe(Statistic::Topk, 60_000, 60_000, 1.0 / 60.0);
        let error = greedy_assign(
            vec![aqe.clone()],
            60_000,
            &AtomicCostTable::default(),
            &CostWeights::default(),
            &unit_facts(&[aqe]),
        );

        assert!(matches!(error, Err(OptimizerError::UnservableItems { .. })));
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
            measured_at: None,
        }];
        let aqe = make_aqe(Statistic::Topk, 60_000, 60_000, 1.0 / 60.0);
        let solution = greedy_assign(
            vec![aqe.clone()],
            60_000,
            &table,
            &CostWeights::default(),
            &unit_facts(&[aqe]),
        )
        .unwrap();

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

    /// A grouped query served by CMS needs a key aggregation the engine can
    /// list groups from, referenced alongside the value in its query config.
    #[test]
    fn cms_assignment_deploys_a_key_aggregation_the_engine_matches() {
        let table = vec![AtomicCostEntry {
            sketch: "cms-fastpath-vector2d".into(),
            sketch_config: serde_json::json!({
                "algorithm": "cms-fastpath-vector2d",
                "params": { "rows": 3, "cols": 512 }
            }),
            mem_bytes_per_instance: 1.0,
            insert_cpu_secs: 0.0,
            merge_cpu_secs: 0.0,
            query_cpu_secs: 0.0,
            query_accuracy: std::collections::BTreeMap::new(),
            measured_at: None,
        }];
        let mut aqe = make_aqe(Statistic::Sum, 60_000, 60_000, 1.0 / 60.0);
        aqe.requirements.grouping_labels = KeyByLabelNames::new(vec!["svc".into()]);
        let solution = greedy_assign(
            vec![aqe.clone()],
            60_000,
            &table,
            &CostWeights::default(),
            &unit_facts(std::slice::from_ref(&aqe)),
        )
        .unwrap();

        let assignment = &solution.assignments[0];
        let value_id = assignment.aggregation_id;
        let key_id = assignment.key_aggregation_id.expect("key tracker deployed");
        let deployed = solution.deployed_configs();
        assert_eq!(
            deployed[&value_id].aggregation_type,
            AggregationType::CountMinSketch
        );
        assert_eq!(
            deployed[&key_id].aggregation_type,
            AggregationType::DeltaSetAggregator
        );

        let (_, inference) = crate::optimizer::translate(&solution);
        let refs: Vec<u64> = inference.query_configs[0]
            .aggregations
            .iter()
            .map(|r| r.aggregation_id)
            .collect();
        assert_eq!(refs, vec![value_id, key_id]);

        // The engine's query-config path validates the pair with this check.
        assert!(
            asap_types::capability_matching::key_agg_compatible_with_value(
                &deployed[&value_id],
                &deployed[&key_id],
            )
        );
    }
}
