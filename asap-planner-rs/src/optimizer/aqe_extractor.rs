use std::collections::HashMap;

use asap_types::query_requirements::{build_query_requirements_promql, QueryRequirements};
use asap_types::utils::normalize_spatial_filter;
use asap_types::PromQLSchema;
use promql_parser::parser::token::{self, TokenType};
use promql_parser::parser::{AggregateExpr, Call, Expr, Function};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;

use crate::config::input::TableDefinition;
use crate::planner::patterns::build_patterns;
use crate::planner::promql::{parse_binary_arms, strip_parens, BinaryArm};
use crate::planner::sql::{parse_sql_query, sql_data_range_ms, sql_statistics};

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
    let mut leaves = Vec::new();
    for rqe in rqes {
        check_repeat_interval(rqe)?;
        for leaf in decompose_to_leaves(&rqe.query_string) {
            let req = extract_requirements(&leaf, metric_schema, scrape_interval_ms).map_err(
                |reason| OptimizerError::UnsupportedLeaf {
                    query: rqe.query_string.clone(),
                    leaf: leaf.clone(),
                    reason,
                },
            )?;
            let k = (req.statistics == [Statistic::Topk])
                .then(|| topk_k(&leaf))
                .flatten();
            leaves.push((req, leaf, k, rqe));
        }
    }
    Ok(merge_leaves(leaves))
}

/// `extract_aqes` for SQL: each query is one leaf, and its metric is
/// `table.value_column` (see `sql_metric`).
pub fn extract_sql_aqes(
    rqes: &[RQE],
    tables: &[TableDefinition],
    scrape_interval_ms: u64,
) -> Result<Vec<OptimizerItem>, OptimizerError> {
    let mut leaves = Vec::new();
    for rqe in rqes {
        check_repeat_interval(rqe)?;
        let query = &rqe.query_string;
        let (req, k) = sql_requirements(query, tables, scrape_interval_ms, SQL_EVAL_TIME).map_err(
            |reason| OptimizerError::UnsupportedLeaf {
                query: query.clone(),
                leaf: query.clone(),
                reason,
            },
        )?;
        leaves.push((req, query.clone(), k, rqe));
    }
    Ok(merge_leaves(leaves))
}

fn check_repeat_interval(rqe: &RQE) -> Result<(), OptimizerError> {
    if rqe.t_repeat_ms == 0 {
        return Err(OptimizerError::InvalidRepeatInterval {
            query: rqe.query_string.clone(),
        });
    }
    Ok(())
}

/// Merges leaf occurrences, each `(requirements, leaf query, top-k k, RQE)`,
/// into optimizer items.
fn merge_leaves(leaves: Vec<(QueryRequirements, String, Option<u64>, &RQE)>) -> Vec<OptimizerItem> {
    struct Acc {
        requirements: QueryRequirements,
        query_strings: Vec<String>,
        occurrences: usize,
        topk_k: Option<u64>,
    }
    let mut acc: HashMap<OptimizerItemKey, Acc> = HashMap::new();
    for (req, leaf, k, rqe) in leaves {
        let key = OptimizerItemKey::from_rqe(&req, rqe);
        let entry = acc.entry(key).or_insert_with(|| Acc {
            requirements: req,
            query_strings: Vec::new(),
            occurrences: 0,
            topk_k: k,
        });
        if !entry.query_strings.contains(&leaf) {
            entry.query_strings.push(leaf);
        }
        entry.occurrences += 1;
        entry.topk_k = entry.topk_k.zip(k).map(|(a, b)| a.max(b));
    }

    acc.into_iter()
        .map(|(key, entry)| OptimizerItem {
            requirements: entry.requirements,
            query_strings: entry.query_strings,
            // Hz (queries per real second): 1000.0 / ms, not 1.0 / ms.
            query_frequency_hz: entry.occurrences as f64 * 1000.0 / key.t_repeat_ms as f64,
            occurrences: entry.occurrences,
            t_repeat_ms: key.t_repeat_ms,
            accuracy_sla: f64::from_bits(key.accuracy_sla_bits),
            latency_sla_ms: key.latency_sla_ms_bits.map(f64::from_bits),
            topk_k: entry.topk_k,
        })
        .collect()
}

/// The time `NOW()` means when SQL queries are planned: fixed, so plans
/// don't depend on the clock.
const SQL_EVAL_TIME: f64 = 1_700_000_000.0;

/// The MILP's name for a SQL value column.
pub(crate) fn sql_metric(table: &str, value_column: &str) -> String {
    format!("{table}.{value_column}")
}

/// A SQL query's requirements and top-k k. A top-k must GROUP BY every
/// metadata column: it then ranks whole series, which is what sketch-bench
/// models.
fn sql_requirements(
    query: &str,
    tables: &[TableDefinition],
    scrape_interval_ms: u64,
    eval_time: f64,
) -> Result<(QueryRequirements, Option<u64>), String> {
    let (data, topk) = parse_sql_query(query, tables, scrape_interval_ms, eval_time)
        .map_err(|error| error.to_string())?;
    let statistics = sql_statistics(&data, topk).map_err(|error| error.to_string())?;
    let aggregation = data.aggregation_info.get_name();
    if statistics.len() > 1 {
        return Err(format!("{aggregation} is not supported by --planner milp"));
    }
    let table = tables
        .iter()
        .find(|table| table.name == data.metric)
        .ok_or_else(|| format!("unknown table {:?}", data.metric))?;
    let column = data.aggregation_info.get_value_column_name();
    if !table.value_columns.iter().any(|c| c == column) {
        return Err(format!(
            "{aggregation} reads {column:?}, which is not a value column of {}",
            table.name
        ));
    }
    let grouping_labels = KeyByLabelNames::new(data.labels.iter().cloned().collect());
    if topk.is_some() && grouping_labels != KeyByLabelNames::new(table.metadata_columns.clone()) {
        return Err(format!(
            "top-k must GROUP BY every metadata column of {} ({:?})",
            table.name, table.metadata_columns
        ));
    }
    let requirements = QueryRequirements {
        metric: sql_metric(&table.name, column),
        statistics,
        data_range_ms: sql_data_range_ms(&data),
        grouping_labels,
        spatial_filter_normalized: normalize_spatial_filter(""),
        topk_count_events: topk.map(|t| t.count_events()),
        topk_by_labels: None,
    };
    Ok((requirements, topk.map(|t| t.k)))
}

/// A `topk` query's literal `k`; `None` for anything else.
fn topk_k(query: &str) -> Option<u64> {
    let Ok(Expr::Aggregate(aggregate)) = promql_parser::parser::parse(query) else {
        return None;
    };
    if aggregate.op.id() != token::T_TOPK {
        return None;
    }
    match aggregate.param.as_deref()? {
        Expr::NumberLiteral(number) if number.val >= 1.0 => Some(number.val as u64),
        _ => None,
    }
}

fn normalized_f64_bits(value: f64) -> u64 {
    if value == 0.0 {
        0.0f64.to_bits()
    } else {
        value.to_bits()
    }
}

/// Recursively decompose a PromQL expression into non-binary leaf queries.
///
/// Binary arithmetic expressions (e.g. `rate(a[5m]) / rate(b[5m])`) are split
/// into their arms. Scalar arms (e.g. the `100` in `rate(x[5m]) * 100`) are
/// dropped — they contribute no AQE. Only arithmetic operators are split;
/// comparison and set operators are left as-is (treated as opaque leaves).
/// An `avg` leaf becomes its sum and count leaves (see `rewrite_avg`).
pub(crate) fn decompose_to_leaves(query: &str) -> Vec<String> {
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

/// Whether `query` has an `avg` the engine can't answer: one that
/// `decompose_to_leaves` splits into sum and count, or a per-series avg in a
/// binary arm, which runs without its query-time avg.
pub(crate) fn contains_avg(query: &str) -> bool {
    match parse_binary_arms(query) {
        Some((lhs, rhs)) => [lhs, rhs].into_iter().any(|arm| {
            matches!(arm, BinaryArm::Query(q) if contains_avg(&q) || per_series_avg(&q).is_some())
        }),
        None => rewrite_avg(query).len() > 1,
    }
}

/// `avg by (l) (f(x[w]))` of a per-series function `f`: the leaf
/// `f(x[w])`, and the avg that averages its series at query time.
pub(crate) fn per_series_avg(query: &str) -> Option<(String, AggregateExpr)> {
    match promql_parser::parser::parse(query).ok()? {
        Expr::Aggregate(agg) if agg.op.id() == token::T_AVG => match strip_parens(&agg.expr) {
            call @ Expr::Call(_) => Some((call.to_string(), agg)),
            _ => None,
        },
        _ => None,
    }
}

/// Rewrite an `avg` leaf into the sum and count leaves it is computed from:
/// `avg by (l) (x)` → `sum by (l) (x)`, `count by (l) (x)`, and
/// `avg_over_time(x[5m])` → `sum_over_time(x[5m])`, `count_over_time(x[5m])`.
/// A per-series avg is its leaf (see `per_series_avg`). Any other query is
/// returned unchanged.
fn rewrite_avg(query: &str) -> Vec<String> {
    if let Some((leaf, _)) = per_series_avg(query) {
        return vec![leaf];
    }
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

    #[test]
    fn topk_k_reads_the_literal() {
        assert_eq!(topk_k("topk(7, http_requests_total)"), Some(7));
        assert_eq!(topk_k("topk by (job) (3, http_requests_total)"), Some(3));
        assert_eq!(topk_k("sum(http_requests_total)"), None);
        assert_eq!(topk_k("topk(scalar(up), http_requests_total)"), None);
    }

    /// Sketch-bench ranks whole series, so a SQL top-k must GROUP BY every
    /// metadata column.
    #[test]
    fn sql_topk_must_group_by_every_metadata_column() {
        let tables: Vec<TableDefinition> = serde_yaml::from_str(
            "[{name: t, time_column: time, value_columns: [v], metadata_columns: [host, region]}]",
        )
        .unwrap();
        let topk = |by: &str| {
            format!(
                "SELECT {by}, SUM(v) AS s FROM t WHERE time BETWEEN DATEADD(s, -60, NOW()) \
                 AND NOW() GROUP BY {by} ORDER BY s DESC LIMIT 5"
            )
        };
        let aqes =
            extract_sql_aqes(&[rqe(&topk("host, region"), 60_000)], &tables, 15_000).unwrap();
        assert_eq!(aqes[0].requirements.metric, "t.v");
        assert_eq!(aqes[0].topk_k, Some(5));
        assert!(matches!(
            extract_sql_aqes(&[rqe(&topk("host"), 60_000)], &tables, 15_000),
            Err(OptimizerError::UnsupportedLeaf { reason, .. }) if reason.contains("top-k")
        ));
    }

    #[test]
    fn sql_rejects_non_value_columns_and_avg() {
        let tables: Vec<TableDefinition> = serde_yaml::from_str(
            "[{name: t, time_column: time, value_columns: [v], metadata_columns: [host, region]}]",
        )
        .unwrap();
        let reason = |select: &str| {
            let query = format!(
                "SELECT host, {select} FROM t WHERE time BETWEEN DATEADD(s, -60, NOW()) \
                 AND NOW() GROUP BY host"
            );
            match extract_sql_aqes(&[rqe(&query, 60_000)], &tables, 15_000) {
                Err(OptimizerError::UnsupportedLeaf { reason, .. }) => reason,
                other => panic!("{select}: {other:?}"),
            }
        };
        assert!(reason("COUNT(DISTINCT region)").contains("not a value column of t"));
        assert!(reason("AVG(v)").contains("not supported by --planner milp"));
    }

    #[test]
    fn merged_topk_takes_the_largest_k_and_none_wins() {
        let rqes = [
            rqe("topk(3, metric)", 60_000),
            rqe("topk(7, metric)", 60_000),
        ];
        let aqes = extract_aqes(&rqes, &empty_schema(), 15_000).unwrap();
        assert_eq!(aqes.len(), 1);
        assert_eq!(aqes[0].topk_k, Some(7));

        let rqes = [
            rqe("topk(3, metric)", 60_000),
            rqe("topk(scalar(up), metric)", 60_000),
        ];
        let aqes = extract_aqes(&rqes, &empty_schema(), 15_000).unwrap();
        assert_eq!(aqes.len(), 1);
        assert_eq!(aqes[0].topk_k, None);
    }

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
    fn different_latency_slas_become_distinct_items() {
        let unlimited = rqe("sum_over_time(metric[5m])", 60_000);
        let mut fast = rqe("sum_over_time(metric[5m])", 60_000);
        fast.latency_sla_ms = Some(100.0);
        let mut slow = rqe("sum_over_time(metric[5m])", 60_000);
        slow.latency_sla_ms = Some(1_000.0);

        let items = extract_aqes(&[unlimited, fast, slow], &empty_schema(), 15_000).unwrap();
        let mut slas: Vec<_> = items.iter().map(|item| item.latency_sla_ms).collect();
        slas.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(slas, vec![None, Some(100.0), Some(1_000.0)]);
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

    /// An avg of per-series quantiles averages the quantiles at query time;
    /// sum and count items would ask for sums of quantiles.
    #[test]
    fn per_series_avg_is_its_per_series_leaf() {
        let query = "avg by (job) (quantile_over_time(0.99, metric[5m]))";
        assert_eq!(
            decompose_to_leaves(query),
            vec!["quantile_over_time(0.99, metric[5m])"]
        );
        let items = extract_aqes(&[rqe(query, 60_000)], &empty_schema(), 15_000).unwrap();
        let [item] = items.as_slice() else {
            panic!("{items:?}");
        };
        assert_eq!(item.requirements.statistics, vec![Statistic::Quantile]);
        assert!(item.requirements.grouping_labels.labels.is_empty());
    }

    #[test]
    fn spatial_only_query_sets_range_to_scrape_interval() {
        let rqes = vec![rqe("sum(metric)", 60_000)];
        let aqes = extract_aqes(&rqes, &empty_schema(), 15_000).unwrap();
        assert_eq!(aqes.len(), 1);
        assert_eq!(aqes[0].requirements.data_range_ms, 15_000);
    }
}
