use std::collections::HashMap;

use asap_types::aggregation_config::AggregationConfig;
use tracing::debug;

use super::candidate_gen::enumerate_candidates;
use super::cost_model::{ingest_cost, query_cost, total_cost_rate, AtomicCosts, CostWeights};
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
pub fn greedy_assign(
    aqes: Vec<AQE>,
    scrape_interval_ms: u64,
    arrival_rate_hz: f64,
    costs: &AtomicCosts,
    weights: &CostWeights,
) -> OptimizerSolution {
    let mut deployed_configs: HashMap<u64, AggregationConfig> = HashMap::new();
    let mut assignments = Vec::new();
    let mut estimated_ingest_cost_per_sec = 0.0;
    let mut estimated_total_cost_per_sec = 0.0;
    let mut next_id: u64 = 1;

    for aqe in aqes {
        let candidates = enumerate_candidates(&aqe, scrape_interval_ms);

        let best = candidates
            .into_iter()
            .map(|c| {
                let cost = total_cost_rate(&aqe, &c, arrival_rate_hz, costs, weights);
                (c, cost)
            })
            // total_cmp (not partial_cmp().unwrap()) so a stray NaN cost can't panic.
            .min_by(|(_, a), (_, b)| a.total_cmp(b))
            .map(|(c, _)| c)
            .expect("enumerate_candidates always returns at least the EXACT fallback");

        let ingest = ingest_cost(&best, arrival_rate_hz, costs, weights);
        let query_rate = aqe.query_frequency_hz * query_cost(&aqe, &best, costs, weights);
        let query_method = best.query_method.clone();

        let aggregation_id = match best.config {
            None => None,
            Some(mut config) => {
                let id = next_id;
                next_id += 1;
                config.aggregation_id = id;
                deployed_configs.insert(id, config);
                Some(id)
            }
        };

        debug!(
            metric = %aqe.requirements.metric,
            aggregation_id = ?aggregation_id,
            query_method = ?query_method,
            ingest_cost_per_sec = ingest,
            query_cost_per_sec = query_rate,
            "greedy: assigned AQE"
        );

        estimated_ingest_cost_per_sec += ingest;
        estimated_total_cost_per_sec += ingest + query_rate;

        assignments.push(AQEAssignment {
            aqe,
            aggregation_id,
            query_method,
            estimated_query_cost_per_sec: query_rate,
        });
    }

    OptimizerSolution {
        deployed_configs,
        assignments,
        estimated_ingest_cost_per_sec,
        estimated_total_cost_per_sec,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::query_requirements::QueryRequirements;
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::Statistic;
    use std::collections::HashMap as StdHashMap;

    fn make_aqe(stat: Statistic, range_ms: Option<u64>, min_t: u64, freq_hz: f64) -> AQE {
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
        }
    }

    #[test]
    fn assigns_unique_ids_to_each_deployed_config() {
        let aqes = vec![
            make_aqe(Statistic::Min, Some(300_000), 300_000, 1.0 / 60.0),
            make_aqe(Statistic::Max, Some(300_000), 300_000, 1.0 / 60.0),
        ];
        let solution = greedy_assign(
            aqes,
            60_000,
            1.0,
            &AtomicCosts::default(),
            &CostWeights::default(),
        );

        let mut seen_ids: StdHashMap<u64, ()> = StdHashMap::new();
        for (id, _) in solution.deployed_configs.iter() {
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
                data_range_ms: Some(60_000),
                grouping_labels: KeyByLabelNames::empty(),
                spatial_filter_normalized: String::new(),
                topk_count_events: None,
            },
            query_strings: vec!["avg_query".into()],
            query_frequency_hz: 1.0 / 60.0,
            min_t_repeat_ms: 60_000,
            t_repeat_gcd_ms: 60_000,
        };
        let solution = greedy_assign(
            vec![aqe],
            60_000,
            1.0,
            &AtomicCosts::default(),
            &CostWeights::default(),
        );
        assert_eq!(solution.num_exact_fallback(), 1);
        assert!(solution.deployed_configs.is_empty());
    }
}
