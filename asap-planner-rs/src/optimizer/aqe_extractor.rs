use std::collections::HashMap;

use asap_types::query_requirements::{build_query_requirements_promql, QueryRequirements};
use asap_types::PromQLSchema;
use promql_parser::parser::token::{self, TokenType};
use promql_parser::parser::{AggregateExpr, Call, Expr, Function};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;

use crate::planner::patterns::build_patterns;
use crate::planner::promql::{parse_binary_arms, BinaryArm};

use super::error::OptimizerError;
use super::solution::OptimizerItem;

/// One repeating query expression: a PromQL query string and its repetition
/// interval (e.g. the refresh interval of the dashboard panel it belongs to).
#[derive(Debug, Clone)]
pub struct RQE {
    pub query_string: String,
    pub t_repeat_ms: u64,
    pub accuracy_sla: f64,
    pub latency_sla_ms: Option<f64>,
}

/// Stable key for merging identical optimizer demand.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct OptimizerItemKey {
    metric: String,
    /// Statistics are produced in a stable order by get_statistics_to_compute.
    statistics: Vec<Statistic>,
    data_range_ms: u64,
    grouping_labels: KeyByLabelNames,
    spatial_filter_normalized: String,
    topk_count_events: Option<bool>,
    topk_by_labels: Option<KeyByLabelNames>,
    t_repeat_ms: u64,
    accuracy_sla_bits: u64,
    latency_sla_ms_bits: Option<u64>,
}

impl OptimizerItemKey {
    fn from_rqe(req: &QueryRequirements, rqe: &RQE) -> Self {
        Self {
            metric: req.metric.clone(),
            statistics: req.statistics.clone(),
            data_range_ms: req.data_range_ms,
            grouping_labels: req.grouping_labels.clone(),
            spatial_filter_normalized: req.spatial_filter_normalized.clone(),
            topk_count_events: req.topk_count_events,
            topk_by_labels: req.topk_by_labels.clone(),
            t_repeat_ms: rqe.t_repeat_ms,
            accuracy_sla_bits: normalized_f64_bits(rqe.accuracy_sla),
            latency_sla_ms_bits: rqe.latency_sla_ms.map(normalized_f64_bits),
        }
    }
}

/// Extract and deduplicate optimizer items from a set of RQEs.
///
/// Each RQE is decomposed into leaf query expressions (recursively splitting
/// binary arithmetic operators), then each leaf is pattern-matched to produce
/// a `QueryRequirements`. Occurrences merge only when requirements, cadence,
/// and both SLAs agree. Their frequency is `count * 1000 / T_ms`.
pub fn extract_aqes(
    rqes: &[RQE],
    metric_schema: &PromQLSchema,
    scrape_interval_ms: u64,
) -> Result<Vec<OptimizerItem>, OptimizerError> {
    let mut acc: HashMap<OptimizerItemKey, (QueryRequirements, Vec<String>, f64)> = HashMap::new();

    for rqe in rqes {
        if rqe.t_repeat_ms == 0 {
            return Err(OptimizerError::InvalidRepeatInterval {
                query: rqe.query_string.clone(),
            });
        }

        let leaves = decompose_to_leaves(&rqe.query_string);

        for leaf in leaves {
            match extract_requirements(&leaf, metric_schema, scrape_interval_ms) {
                Ok(req) => {
                    let key = OptimizerItemKey::from_rqe(&req, rqe);
                    let entry = acc.entry(key).or_insert_with(|| (req, Vec::new(), 0.0));
                    if !entry.1.contains(&leaf) {
                        entry.1.push(leaf);
                    }
                    // query_frequency_hz must stay in Hz (queries per real second)
                    // regardless of t_repeat_ms's internal unit — 1000.0 / ms, not 1.0 / ms.
                    entry.2 += 1000.0 / rqe.t_repeat_ms as f64;
                }
                Err(reason) => {
                    return Err(OptimizerError::UnsupportedLeaf {
                        query: rqe.query_string.clone(),
                        leaf,
                        reason,
                    })
                }
            }
        }
    }

    Ok(acc
        .into_iter()
        .map(
            |(key, (requirements, query_strings, query_frequency_hz))| OptimizerItem {
                requirements,
                query_strings,
                query_frequency_hz,
                t_repeat_ms: key.t_repeat_ms,
                accuracy_sla: f64::from_bits(key.accuracy_sla_bits),
                latency_sla_ms: key.latency_sla_ms_bits.map(f64::from_bits),
            },
        )
        .collect())
}

fn normalized_f64_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0f64.to_bits()
    } else {
        value.to_bits()
    }
}

/// Euclidean GCD. `num-integer` is not in the workspace; this two-liner is
/// sufficient and avoids a dependency.
pub(super) fn gcd(a: u64, b: u64) -> u64 {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

/// Recursively decompose a PromQL expression into non-binary leaf queries.
///
/// Binary arithmetic expressions (e.g. `rate(a[5m]) / rate(b[5m])`) are split
/// into their arms. Scalar arms (e.g. the `100` in `rate(x[5m]) * 100`) are
/// dropped — they contribute no AQE. Only arithmetic operators are split;
/// comparison and set operators are left as-is (treated as opaque leaves).
/// An `avg` leaf becomes its sum and count leaves (see `rewrite_avg`).
fn decompose_to_leaves(query: &str) -> Vec<String> {
    let (lhs, rhs) = match parse_binary_arms(query) {
        Some(arms) => arms,
        None => return rewrite_avg(query),
    };

    let mut leaves = Vec::new();
    for arm in [lhs, rhs] {
        if let BinaryArm::Query(arm_query) = arm {
            leaves.extend(decompose_to_leaves(&arm_query));
        }
    }
    leaves
}

/// Rewrite an `avg` leaf into the sum and count leaves it is computed from:
/// `avg by (l) (x)` → `sum by (l) (x)`, `count by (l) (x)`, and
/// `avg_over_time(x[5m])` → `sum_over_time(x[5m])`, `count_over_time(x[5m])`.
/// Any other query is returned unchanged.
fn rewrite_avg(query: &str) -> Vec<String> {
    match promql_parser::parser::parse(query) {
        Ok(Expr::Aggregate(agg)) if agg.op.id() == token::T_AVG => [token::T_SUM, token::T_COUNT]
            .into_iter()
            .map(|op| {
                Expr::Aggregate(AggregateExpr {
                    op: TokenType::new(op),
                    ..agg.clone()
                })
                .to_string()
            })
            .collect(),
        // sum_over_time and count_over_time share avg_over_time's signature.
        Ok(Expr::Call(call)) if call.func.name == "avg_over_time" => {
            ["sum_over_time", "count_over_time"]
                .into_iter()
                .map(|name| {
                    Expr::Call(Call {
                        func: Function {
                            name,
                            ..call.func.clone()
                        },
                        args: call.args.clone(),
                    })
                    .to_string()
                })
                .collect()
        }
        _ => vec![query.to_string()],
    }
}

/// Try to extract `QueryRequirements` from a single leaf PromQL query string.
/// Returns `None` if the query cannot be parsed or does not match any pattern.
fn extract_requirements(
    query: &str,
    metric_schema: &PromQLSchema,
    data_ingestion_interval_ms: u64,
) -> Result<QueryRequirements, String> {
    let ast = promql_parser::parser::parse(query).map_err(|error| error.to_string())?;
    let patterns = build_patterns();

    let match_result = patterns
        .iter()
        .find_map(|pat| {
            let r = pat.matches(&ast);
            if r.matches {
                Some(r)
            } else {
                None
            }
        })
        .ok_or_else(|| "no supported query pattern matched".to_string())?;

    build_query_requirements_promql(
        query,
        &match_result,
        metric_schema,
        data_ingestion_interval_ms,
    )
    .ok_or_else(|| "query requirements could not be derived".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_schema() -> PromQLSchema {
        PromQLSchema::new()
    }

    fn rqe(query: &str, t_ms: u64) -> RQE {
        RQE {
            query_string: query.to_string(),
            t_repeat_ms: t_ms,
            accuracy_sla: 0.0,
            latency_sla_ms: None,
        }
    }

    #[test]
    fn single_temporal_query() {
        let rqes = vec![rqe("sum_over_time(metric[5m])", 60_000)];
        let aqes = extract_aqes(&rqes, &empty_schema(), 15_000).unwrap();
        assert_eq!(aqes.len(), 1);
        assert!((aqes[0].query_frequency_hz - 1.0 / 60.0).abs() < 1e-9);
        assert_eq!(aqes[0].requirements.metric, "metric");
        assert_eq!(aqes[0].requirements.data_range_ms, 300_000);
    }

    #[test]
    fn binary_query_produces_two_aqes() {
        let rqes = vec![rqe(
            "sum_over_time(metric_a[5m]) / sum_over_time(metric_b[5m])",
            60_000,
        )];
        let aqes = extract_aqes(&rqes, &empty_schema(), 15_000).unwrap();
        assert_eq!(aqes.len(), 2);
    }

    #[test]
    fn binary_with_scalar_produces_one_aqe() {
        let rqes = vec![rqe("sum_over_time(metric[5m]) * 100", 60_000)];
        let aqes = extract_aqes(&rqes, &empty_schema(), 15_000).unwrap();
        assert_eq!(aqes.len(), 1);
    }

    #[test]
    fn identical_items_deduplicate_and_sum_frequency() {
        let rqes = vec![
            rqe("sum_over_time(metric[5m])", 60_000),
            rqe("sum_over_time(metric[5m])", 60_000),
        ];
        let aqes = extract_aqes(&rqes, &empty_schema(), 15_000).unwrap();
        assert_eq!(aqes.len(), 1);
        // query_frequency_hz = sum of rates (total query load for the MIP objective)
        let expected_freq = 2.0 / 60.0;
        assert!((aqes[0].query_frequency_hz - expected_freq).abs() < 1e-9);
        assert_eq!(aqes[0].t_repeat_ms, 60_000);
        assert_eq!(aqes[0].query_strings.len(), 1);
    }

    #[test]
    fn different_repeat_intervals_become_distinct_items() {
        let rqes = vec![
            rqe("sum_over_time(metric[5m])", 60_000),
            rqe("sum_over_time(metric[5m])", 30_000),
        ];
        let mut items = extract_aqes(&rqes, &empty_schema(), 15_000).unwrap();
        items.sort_by_key(|item| item.t_repeat_ms);

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].t_repeat_ms, 30_000);
        assert_eq!(items[1].t_repeat_ms, 60_000);
        assert!((items[0].query_frequency_hz - 1.0 / 30.0).abs() < 1e-9);
        assert!((items[1].query_frequency_hz - 1.0 / 60.0).abs() < 1e-9);
    }

    #[test]
    fn different_slas_become_distinct_items() {
        let mut strict = rqe("sum_over_time(metric[5m])", 60_000);
        strict.accuracy_sla = 0.99;
        let mut relaxed = rqe("sum_over_time(metric[5m])", 60_000);
        relaxed.accuracy_sla = 0.9;

        let items = extract_aqes(&[strict, relaxed], &empty_schema(), 15_000).unwrap();
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn signed_zero_slas_merge_into_one_item() {
        let mut negative_zero = rqe("sum_over_time(metric[5m])", 60_000);
        negative_zero.accuracy_sla = -0.0;
        let items = extract_aqes(
            &[negative_zero, rqe("sum_over_time(metric[5m])", 60_000)],
            &empty_schema(),
            15_000,
        )
        .unwrap();
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn zero_repeat_interval_is_rejected() {
        let rqes = vec![rqe("sum_over_time(metric[5m])", 0)];
        assert!(matches!(
            extract_aqes(&rqes, &empty_schema(), 15_000),
            Err(OptimizerError::InvalidRepeatInterval { .. })
        ));
    }

    #[test]
    fn unsupported_query_is_rejected() {
        let rqes = vec![rqe("not_a_real_function(metric[5m])", 60_000)];
        assert!(matches!(
            extract_aqes(&rqes, &empty_schema(), 15_000),
            Err(OptimizerError::UnsupportedLeaf { .. })
        ));
    }

    fn sorted_statistics_and_queries(
        items: &[OptimizerItem],
    ) -> Vec<(Vec<Statistic>, Vec<String>)> {
        let mut out: Vec<_> = items
            .iter()
            .map(|i| (i.requirements.statistics.clone(), i.query_strings.clone()))
            .collect();
        out.sort_by_key(|(_, q)| q.clone());
        out
    }

    // A multi-statistic [Sum, Count] item has no single-sketch candidate, so
    // avg must reach the optimizer as separate sum and count items.
    #[test]
    fn spatial_avg_becomes_sum_and_count_items() {
        let rqes = vec![rqe("avg by (job) (metric)", 60_000)];
        let items = extract_aqes(&rqes, &empty_schema(), 15_000).unwrap();
        assert_eq!(
            sorted_statistics_and_queries(&items),
            vec![
                (
                    vec![Statistic::Count],
                    vec!["count by (job) (metric)".into()]
                ),
                (vec![Statistic::Sum], vec!["sum by (job) (metric)".into()]),
            ]
        );
        for item in &items {
            assert_eq!(
                item.requirements.grouping_labels,
                KeyByLabelNames::new(vec!["job".into()])
            );
        }
    }

    #[test]
    fn avg_over_time_becomes_sum_and_count_over_time_items() {
        let rqes = vec![rqe("avg_over_time(metric[5m])", 60_000)];
        let items = extract_aqes(&rqes, &empty_schema(), 15_000).unwrap();
        assert_eq!(
            sorted_statistics_and_queries(&items),
            vec![
                (
                    vec![Statistic::Count],
                    vec!["count_over_time(metric[5m])".into()]
                ),
                (
                    vec![Statistic::Sum],
                    vec!["sum_over_time(metric[5m])".into()]
                ),
            ]
        );
        for item in &items {
            assert_eq!(item.requirements.data_range_ms, 300_000);
        }
    }

    #[test]
    fn avg_arm_of_binary_query_is_rewritten() {
        let rqes = vec![rqe(
            "avg_over_time(metric_a[5m]) / sum_over_time(metric_b[5m])",
            60_000,
        )];
        let items = extract_aqes(&rqes, &empty_schema(), 15_000).unwrap();
        assert_eq!(items.len(), 3);
        assert!(items.iter().all(|i| i.requirements.statistics.len() == 1));
    }

    #[test]
    fn spatial_only_query_sets_range_to_scrape_interval() {
        let rqes = vec![rqe("sum(metric)", 60_000)];
        let aqes = extract_aqes(&rqes, &empty_schema(), 15_000).unwrap();
        assert_eq!(aqes.len(), 1);
        assert_eq!(aqes[0].requirements.data_range_ms, 15_000);
    }
}
