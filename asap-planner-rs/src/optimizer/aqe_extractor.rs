use std::collections::HashMap;

use asap_types::query_requirements::{build_query_requirements_promql, QueryRequirements};
use asap_types::PromQLSchema;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;
use tracing::warn;

use crate::planner::patterns::build_patterns;
use crate::planner::promql::{parse_binary_arms, BinaryArm};

use super::solution::AQE;

/// One repeating query expression: a PromQL query string and its repetition
/// interval (e.g. the refresh interval of the dashboard panel it belongs to).
#[derive(Debug, Clone)]
pub struct RQE {
    pub query_string: String,
    pub t_repeat_secs: u64,
}

/// Stable deduplication key for an AQE.
/// Two leaf queries that produce identical requirements are treated as the same
/// AQE regardless of which RQE they came from.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct AQEKey {
    metric: String,
    /// Statistics are produced in a stable order by get_statistics_to_compute.
    statistics: Vec<Statistic>,
    data_range_ms: Option<u64>,
    grouping_labels: KeyByLabelNames,
    spatial_filter_normalized: String,
    topk_count_events: Option<bool>,
}

impl AQEKey {
    fn from_requirements(req: &QueryRequirements) -> Self {
        Self {
            metric: req.metric.clone(),
            statistics: req.statistics.clone(),
            data_range_ms: req.data_range_ms,
            grouping_labels: req.grouping_labels.clone(),
            spatial_filter_normalized: req.spatial_filter_normalized.clone(),
            topk_count_events: req.topk_count_events,
        }
    }
}

/// Extract and deduplicate AQEs from a set of RQEs.
///
/// Each RQE is decomposed into leaf query expressions (recursively splitting
/// binary arithmetic operators), then each leaf is pattern-matched to produce
/// a `QueryRequirements`. AQEs with identical requirements are merged.
///
/// Three frequency-related values are computed per AQE:
/// - `query_frequency_hz`: Σ 1/T_r — total query load for the MIP objective.
/// - `min_t_repeat_secs`: min(T_r) — freshness bound on window size W ≤ min_t.
/// - `t_repeat_gcd_secs`: GCD(T_r) — natural slide interval S for candidate
///   generation (windows completing every GCD secs align with all dashboards).
///
/// Leaf queries that do not match any supported pattern (e.g. unsupported
/// functions, parse errors) are skipped with a warning.
pub fn extract_aqes(rqes: &[RQE], metric_schema: &PromQLSchema) -> Vec<AQE> {
    // (key) -> (requirements, query_strings, sum_freq, min_t, gcd_t)
    let mut acc: HashMap<AQEKey, (QueryRequirements, Vec<String>, f64, u64, u64)> = HashMap::new();

    for rqe in rqes {
        if rqe.t_repeat_secs == 0 {
            warn!(
                query = %rqe.query_string,
                "aqe_extractor: skipping RQE with repetition_delay=0 \
                 (would produce infinite query frequency and corrupt GCD)"
            );
            continue;
        }

        let leaves = decompose_to_leaves(&rqe.query_string);

        for leaf in leaves {
            match extract_requirements(&leaf, metric_schema) {
                Some(req) => {
                    let key = AQEKey::from_requirements(&req);
                    let entry = acc
                        .entry(key)
                        .or_insert_with(|| (req, Vec::new(), 0.0, u64::MAX, 0));
                    if !entry.1.contains(&leaf) {
                        entry.1.push(leaf);
                    }
                    entry.2 += 1.0 / rqe.t_repeat_secs as f64;
                    entry.3 = entry.3.min(rqe.t_repeat_secs);
                    entry.4 = if entry.4 == 0 {
                        rqe.t_repeat_secs
                    } else {
                        gcd(entry.4, rqe.t_repeat_secs)
                    };
                }
                None => {
                    warn!(
                        query = %leaf,
                        "aqe_extractor: skipping unsupported or unparseable leaf query"
                    );
                }
            }
        }
    }

    acc.into_values()
        .map(
            |(
                requirements,
                query_strings,
                query_frequency_hz,
                min_t_repeat_secs,
                t_repeat_gcd_secs,
            )| AQE {
                requirements,
                query_strings,
                query_frequency_hz,
                min_t_repeat_secs,
                t_repeat_gcd_secs,
            },
        )
        .collect()
}

/// Euclidean GCD. `num-integer` is not in the workspace; this two-liner is
/// sufficient and avoids a dependency.
fn gcd(a: u64, b: u64) -> u64 {
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
fn decompose_to_leaves(query: &str) -> Vec<String> {
    let (lhs, rhs) = match parse_binary_arms(query) {
        Some(arms) => arms,
        None => return vec![query.to_string()],
    };

    let mut leaves = Vec::new();
    for arm in [lhs, rhs] {
        if let BinaryArm::Query(arm_query) = arm {
            leaves.extend(decompose_to_leaves(&arm_query));
        }
    }
    leaves
}

/// Try to extract `QueryRequirements` from a single leaf PromQL query string.
/// Returns `None` if the query cannot be parsed or does not match any pattern.
fn extract_requirements(query: &str, metric_schema: &PromQLSchema) -> Option<QueryRequirements> {
    let ast = promql_parser::parser::parse(query).ok()?;
    let patterns = build_patterns();

    let (pattern_type, match_result) = patterns.iter().find_map(|(pt, pat)| {
        let r = pat.matches(&ast);
        if r.matches {
            Some((*pt, r))
        } else {
            None
        }
    })?;

    build_query_requirements_promql(query, &match_result, pattern_type, metric_schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_schema() -> PromQLSchema {
        PromQLSchema::new()
    }

    fn rqe(query: &str, t: u64) -> RQE {
        RQE {
            query_string: query.to_string(),
            t_repeat_secs: t,
        }
    }

    #[test]
    fn single_temporal_query() {
        let rqes = vec![rqe("sum_over_time(metric[5m])", 60)];
        let aqes = extract_aqes(&rqes, &empty_schema());
        assert_eq!(aqes.len(), 1);
        assert!((aqes[0].query_frequency_hz - 1.0 / 60.0).abs() < 1e-9);
        assert_eq!(aqes[0].requirements.metric, "metric");
        assert_eq!(aqes[0].requirements.data_range_ms, Some(300_000));
    }

    #[test]
    fn binary_query_produces_two_aqes() {
        let rqes = vec![rqe(
            "sum_over_time(metric_a[5m]) / sum_over_time(metric_b[5m])",
            60,
        )];
        let aqes = extract_aqes(&rqes, &empty_schema());
        assert_eq!(aqes.len(), 2);
    }

    #[test]
    fn binary_with_scalar_produces_one_aqe() {
        let rqes = vec![rqe("sum_over_time(metric[5m]) * 100", 60)];
        let aqes = extract_aqes(&rqes, &empty_schema());
        assert_eq!(aqes.len(), 1);
    }

    #[test]
    fn same_aqe_in_two_rqes_deduplicates_and_sums_frequency() {
        let rqes = vec![
            rqe("sum_over_time(metric[5m])", 60),
            rqe("sum_over_time(metric[5m])", 30),
        ];
        let aqes = extract_aqes(&rqes, &empty_schema());
        assert_eq!(aqes.len(), 1);
        // f_a = sum of rates (total query load for the MIP objective)
        let expected_freq = 1.0 / 60.0 + 1.0 / 30.0;
        assert!((aqes[0].query_frequency_hz - expected_freq).abs() < 1e-9);
        // min_t and gcd_t used for windowing constraints
        assert_eq!(aqes[0].min_t_repeat_secs, 30);
        assert_eq!(aqes[0].t_repeat_gcd_secs, 30); // gcd(60, 30) = 30
        assert_eq!(aqes[0].query_strings.len(), 1); // same string, deduplicated
    }

    #[test]
    fn unsupported_query_is_skipped() {
        let rqes = vec![rqe("not_a_real_function(metric[5m])", 60)];
        let aqes = extract_aqes(&rqes, &empty_schema());
        assert_eq!(aqes.len(), 0);
    }
}
