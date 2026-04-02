use std::cmp::Ordering;
use std::collections::HashMap;

use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;
use tracing::{debug, warn};

use crate::aggregation_config::{AggregationConfig, AggregationIdInfo};
use crate::query_requirements::QueryRequirements;
use crate::utils::normalize_spatial_filter;

// ---------------------------------------------------------------------------
// Pure compatibility helpers
// ---------------------------------------------------------------------------

/// Returns the aggregation_type strings that can serve this statistic.
pub fn compatible_agg_types(stat: Statistic) -> &'static [&'static str] {
    match stat {
        Statistic::Sum => &["Sum", "MultipleSumAccumulator"],
        Statistic::Count => &[
            "CountMinSketch",
            "CountMinSketchWithHeap",
            "CountMinSketchWithHeapAccumulator",
        ],
        Statistic::Min => &["MinMax", "MultipleMinMaxAccumulator"],
        Statistic::Max => &["MinMax", "MultipleMinMaxAccumulator"],
        Statistic::Quantile => &["DatasketchesKLL", "HydraKLL"],
        Statistic::Rate | Statistic::Increase => &["Increase", "MultipleIncreaseAccumulator"],
        Statistic::Cardinality => &["SetAggregator", "DeltaSetAggregator"],
        Statistic::Topk => &[
            "CountMinSketchWithHeap",
            "CountMinSketchWithHeapAccumulator",
        ],
    }
}

/// Returns the required aggregation_sub_type for this statistic, if any.
/// `Min` requires `"min"`, `Max` requires `"max"`. All others are unconstrained.
pub fn required_sub_type(stat: Statistic) -> Option<&'static str> {
    match stat {
        Statistic::Min => Some("min"),
        Statistic::Max => Some("max"),
        _ => None,
    }
}

/// Whether this value aggregation type requires a paired key aggregation
/// (`SetAggregator` or `DeltaSetAggregator`).
pub fn is_multi_population_value_type(agg_type: &str) -> bool {
    matches!(
        agg_type,
        "MultipleSumAccumulator"
            | "MultipleMinMaxAccumulator"
            | "MultipleIncreaseAccumulator"
            | "CountMinSketchWithHeap"
            | "CountMinSketchWithHeapAccumulator"
    )
}

/// Whether this type is a key aggregation (tracks which label-value combinations exist).
fn is_key_agg_type(agg_type: &str) -> bool {
    matches!(agg_type, "SetAggregator" | "DeltaSetAggregator")
}

/// Window compatibility: can `config` serve a query needing `data_range_ms`?
///
/// - `None` (spatial-only): always compatible.
/// - Tumbling: `data_range_ms` must be a positive integer multiple of `window_size_ms`.
/// - Sliding: `data_range_ms` must equal `window_size_ms` exactly (a sliding window
///   precomputes one fixed range per timestamp; overlapping windows cannot be merged).
pub fn window_compatible(config: &AggregationConfig, data_range_ms: Option<u64>) -> bool {
    let Some(range) = data_range_ms else {
        return true;
    };
    let window_ms = config.window_size * 1000;
    if window_ms == 0 || range == 0 {
        return false;
    }
    match config.window_type.as_str() {
        "sliding" => range == window_ms,
        _ => range % window_ms == 0, // tumbling (or unknown — treat as tumbling)
    }
}

/// Label compatibility: strict exact match.
/// TODO: relax to superset (config.grouping_labels ⊇ req.grouping_labels) for
/// simple accumulators (Sum, MinMax, Increase).
pub fn labels_compatible(config_labels: &KeyByLabelNames, req_labels: &KeyByLabelNames) -> bool {
    config_labels == req_labels
}

/// Spatial filter compatibility.
/// - Both empty → compatible.
/// - Config non-empty and matches query → compatible.
/// - Config non-empty and query differs (or is empty) → incompatible.
pub fn spatial_filter_compatible(config_filter: &str, req_filter: &str) -> bool {
    let config_norm = normalize_spatial_filter(config_filter);
    let req_norm = normalize_spatial_filter(req_filter);
    if config_norm.is_empty() {
        // Config has no filter — compatible with any query filter.
        return true;
    }
    config_norm == req_norm
}

/// Aggregation priority comparator: prefer larger `window_size` (descending).
/// This is a separate function so callers can swap the policy without touching matching logic.
pub fn aggregation_priority(a: &AggregationConfig, b: &AggregationConfig) -> Ordering {
    b.window_size.cmp(&a.window_size)
}

// ---------------------------------------------------------------------------
// Core matching function
// ---------------------------------------------------------------------------

/// Find a compatible aggregation (or pair of aggregations for multi-population queries)
/// given all available aggregation configs and a set of query requirements.
///
/// Returns `None` if no fully compatible match exists.
///
/// Algorithm:
/// 1. For each statistic, collect and sort compatible candidates.
/// 2. For multi-statistic requirements (e.g. avg = [Sum, Count]), all must be
///    served by configs sharing the same `window_size` and `grouping_labels`.
/// 3. If the selected value aggregation type is multi-population, also find a
///    paired key aggregation (`SetAggregator` / `DeltaSetAggregator`) on the same metric.
pub fn find_compatible_aggregation(
    configs: &HashMap<u64, AggregationConfig>,
    requirements: &QueryRequirements,
) -> Option<AggregationIdInfo> {
    if requirements.statistics.is_empty() {
        return None;
    }

    debug!(
        metric = %requirements.metric,
        statistics = ?requirements.statistics,
        data_range_ms = ?requirements.data_range_ms,
        grouping_labels = ?requirements.grouping_labels.labels,
        "capability matching: searching {} aggregation config(s)",
        configs.len(),
    );

    // For each statistic, collect configs that pass all filters, sorted by priority.
    let mut per_stat_candidates: Vec<Vec<&AggregationConfig>> = Vec::new();

    for &stat in &requirements.statistics {
        let types = compatible_agg_types(stat);
        let sub_type = required_sub_type(stat);

        let mut candidates: Vec<&AggregationConfig> = configs
            .values()
            .filter(|c| {
                let ok = c.metric == requirements.metric
                    && types.contains(&c.aggregation_type.as_str())
                    && sub_type.is_none_or(|st| c.aggregation_sub_type == st)
                    && window_compatible(c, requirements.data_range_ms)
                    && labels_compatible(&c.grouping_labels, &requirements.grouping_labels)
                    && spatial_filter_compatible(
                        &c.spatial_filter_normalized,
                        &requirements.spatial_filter_normalized,
                    );
                if !ok {
                    debug!(
                        agg_id = c.aggregation_id,
                        agg_type = %c.aggregation_type,
                        metric = %c.metric,
                        window_size_s = c.window_size,
                        "capability matching: rejected config for {:?}",
                        stat,
                    );
                }
                ok
            })
            .collect();

        candidates.sort_by(|a, b| aggregation_priority(a, b));

        if candidates.is_empty() {
            warn!(
                metric = %requirements.metric,
                statistic = ?stat,
                "capability matching: no compatible aggregation found for statistic",
            );
            return None;
        }

        debug!(
            statistic = ?stat,
            num_candidates = candidates.len(),
            chosen_agg_id = candidates[0].aggregation_id,
            chosen_agg_type = %candidates[0].aggregation_type,
            chosen_window_size_s = candidates[0].window_size,
            "capability matching: found candidates, chose best",
        );

        per_stat_candidates.push(candidates);
    }

    // Pick the best candidate for the first statistic.
    let value_agg = per_stat_candidates[0][0];

    // For multi-statistic requirements, the remaining statistics must be served by a
    // config that agrees on window_size and grouping_labels with the chosen value agg.
    for (i, candidates) in per_stat_candidates.iter().enumerate().skip(1) {
        let found = candidates.iter().any(|c| {
            c.window_size == value_agg.window_size && c.grouping_labels == value_agg.grouping_labels
        });
        if !found {
            warn!(
                metric = %requirements.metric,
                statistic = ?requirements.statistics[i],
                required_window_size_s = value_agg.window_size,
                "capability matching: no matching window/labels for multi-statistic requirement",
            );
            return None;
        }
    }

    // If value type is multi-population, find the paired key aggregation.
    let key_agg: &AggregationConfig = if is_multi_population_value_type(&value_agg.aggregation_type)
    {
        let ka = configs
            .values()
            .find(|c| c.metric == requirements.metric && is_key_agg_type(&c.aggregation_type));
        if ka.is_none() {
            warn!(
                metric = %requirements.metric,
                value_agg_type = %value_agg.aggregation_type,
                "capability matching: multi-population value agg requires a key agg (SetAggregator/DeltaSetAggregator) but none found",
            );
        }
        ka?
    } else {
        value_agg
    };

    debug!(
        metric = %requirements.metric,
        value_agg_id = value_agg.aggregation_id,
        value_agg_type = %value_agg.aggregation_type,
        key_agg_id = key_agg.aggregation_id,
        key_agg_type = %key_agg.aggregation_type,
        "capability matching: resolved",
    );

    Some(AggregationIdInfo {
        aggregation_id_for_value: value_agg.aggregation_id,
        aggregation_type_for_value: value_agg.aggregation_type.clone(),
        aggregation_id_for_key: key_agg.aggregation_id,
        aggregation_type_for_key: key_agg.aggregation_type.clone(),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::normalize_spatial_filter;
    use promql_utilities::data_model::KeyByLabelNames;
    use std::collections::HashMap;

    #[allow(clippy::too_many_arguments)]
    fn make_config(
        id: u64,
        metric: &str,
        agg_type: &str,
        sub_type: &str,
        window_size_s: u64,
        window_type: &str,
        grouping: &[&str],
        spatial_filter: &str,
    ) -> AggregationConfig {
        let grouping_labels =
            KeyByLabelNames::new(grouping.iter().map(|s| s.to_string()).collect());
        let spatial_filter_normalized = normalize_spatial_filter(spatial_filter);
        AggregationConfig {
            aggregation_id: id,
            aggregation_type: agg_type.to_string(),
            aggregation_sub_type: sub_type.to_string(),
            parameters: HashMap::new(),
            grouping_labels,
            aggregated_labels: KeyByLabelNames::new(vec![]),
            rollup_labels: KeyByLabelNames::new(vec![]),
            original_yaml: String::new(),
            window_size: window_size_s,
            slide_interval: window_size_s,
            window_type: window_type.to_string(),
            spatial_filter: spatial_filter.to_string(),
            spatial_filter_normalized,
            metric: metric.to_string(),
            num_aggregates_to_retain: None,
            read_count_threshold: None,
            table_name: None,
            value_column: None,
        }
    }

    fn req(
        metric: &str,
        stats: &[Statistic],
        data_range_ms: Option<u64>,
        grouping: &[&str],
        spatial_filter: &str,
    ) -> QueryRequirements {
        QueryRequirements {
            metric: metric.to_string(),
            statistics: stats.to_vec(),
            data_range_ms,
            grouping_labels: KeyByLabelNames::new(grouping.iter().map(|s| s.to_string()).collect()),
            spatial_filter_normalized: normalize_spatial_filter(spatial_filter),
        }
    }

    fn single_config(config: AggregationConfig) -> HashMap<u64, AggregationConfig> {
        let mut m = HashMap::new();
        m.insert(config.aggregation_id, config);
        m
    }

    // --- basic type matching ---

    #[test]
    fn basic_sum_match() {
        let configs = single_config(make_config(1, "cpu", "Sum", "", 300, "tumbling", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], ""),
        );
        assert!(result.is_some());
        assert_eq!(result.unwrap().aggregation_id_for_value, 1);
    }

    #[test]
    fn quantile_any_value_finds_kll() {
        let configs = single_config(make_config(
            2,
            "lat",
            "DatasketchesKLL",
            "",
            300,
            "tumbling",
            &[],
            "",
        ));
        // quantile value (0.5 or 0.9) is NOT part of QueryRequirements — both should find the same config
        let r1 = find_compatible_aggregation(
            &configs,
            &req("lat", &[Statistic::Quantile], Some(300_000), &[], ""),
        );
        let r2 = find_compatible_aggregation(
            &configs,
            &req("lat", &[Statistic::Quantile], Some(300_000), &[], ""),
        );
        assert_eq!(r1.unwrap().aggregation_id_for_value, 2);
        assert_eq!(r2.unwrap().aggregation_id_for_value, 2);
    }

    #[test]
    fn quantile_matches_hydrarkll() {
        let configs = single_config(make_config(
            3,
            "lat",
            "HydraKLL",
            "",
            300,
            "tumbling",
            &[],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("lat", &[Statistic::Quantile], Some(300_000), &[], ""),
        );
        assert_eq!(result.unwrap().aggregation_id_for_value, 3);
    }

    #[test]
    fn no_match_wrong_metric() {
        let configs = single_config(make_config(1, "cpu", "Sum", "", 300, "tumbling", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("mem", &[Statistic::Sum], Some(300_000), &[], ""),
        );
        assert!(result.is_none());
    }

    #[test]
    fn no_match_wrong_type() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "DatasketchesKLL",
            "",
            300,
            "tumbling",
            &[],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], ""),
        );
        assert!(result.is_none());
    }

    // --- window compatibility ---

    #[test]
    fn window_tumbling_exact() {
        let configs = single_config(make_config(1, "cpu", "Sum", "", 300, "tumbling", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], ""),
        );
        assert!(result.is_some());
    }

    #[test]
    fn window_tumbling_divisible() {
        // 900_000 ms / 300 s = 3 buckets — valid merge
        let configs = single_config(make_config(1, "cpu", "Sum", "", 300, "tumbling", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(900_000), &[], ""),
        );
        assert!(result.is_some());
    }

    #[test]
    fn window_tumbling_not_divisible() {
        // 600_000 ms / 900 s is not a whole number
        let configs = single_config(make_config(1, "cpu", "Sum", "", 900, "tumbling", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(600_000), &[], ""),
        );
        assert!(result.is_none());
    }

    #[test]
    fn window_sliding_exact() {
        let configs = single_config(make_config(1, "cpu", "Sum", "", 300, "sliding", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], ""),
        );
        assert!(result.is_some());
    }

    #[test]
    fn window_sliding_too_large() {
        // Query range 600 s but sliding window only covers 300 s
        let configs = single_config(make_config(1, "cpu", "Sum", "", 300, "sliding", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(600_000), &[], ""),
        );
        assert!(result.is_none());
    }

    #[test]
    fn window_priority_largest_wins() {
        let mut configs = HashMap::new();
        configs.insert(
            1,
            make_config(1, "cpu", "Sum", "", 300, "tumbling", &[], ""),
        );
        configs.insert(
            2,
            make_config(2, "cpu", "Sum", "", 900, "tumbling", &[], ""),
        );
        // 900_000 ms is divisible by both 300 s and 900 s — prefer 900 s
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(900_000), &[], ""),
        );
        assert_eq!(result.unwrap().aggregation_id_for_value, 2);
    }

    #[test]
    fn spatial_only_no_range() {
        // data_range_ms = None → any window size is compatible
        let configs = single_config(make_config(1, "cpu", "Sum", "", 900, "tumbling", &[], ""));
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Sum], None, &[], ""));
        assert!(result.is_some());
    }

    // --- label compatibility ---

    #[test]
    fn label_strict_exact() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300,
            "tumbling",
            &["job"],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &["job"], ""),
        );
        assert!(result.is_some());
    }

    #[test]
    fn label_strict_superset_rejected() {
        // Config has {job, instance}, query wants only {job} — strict mode rejects
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300,
            "tumbling",
            &["job", "instance"],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &["job"], ""),
        );
        assert!(result.is_none());
    }

    #[test]
    fn label_mismatch_rejected() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300,
            "tumbling",
            &["region"],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &["job"], ""),
        );
        assert!(result.is_none());
    }

    // --- spatial filter compatibility ---

    #[test]
    fn spatial_filter_empty_both() {
        let configs = single_config(make_config(1, "cpu", "Sum", "", 300, "tumbling", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], ""),
        );
        assert!(result.is_some());
    }

    #[test]
    fn spatial_filter_query_empty_config_has_filter() {
        // Config scoped to env=prod, query has no filter → reject
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300,
            "tumbling",
            &[],
            "env=prod",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], ""),
        );
        assert!(result.is_none());
    }

    #[test]
    fn spatial_filter_same() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300,
            "tumbling",
            &[],
            "env=prod",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], "env=prod"),
        );
        assert!(result.is_some());
    }

    #[test]
    fn spatial_filter_different() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300,
            "tumbling",
            &[],
            "env=prod",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], "env=staging"),
        );
        assert!(result.is_none());
    }

    // --- sub-type ---

    #[test]
    fn sub_type_min_matches_min() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "MinMax",
            "min",
            300,
            "tumbling",
            &[],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Min], Some(300_000), &[], ""),
        );
        assert!(result.is_some());
    }

    #[test]
    fn sub_type_max_rejects_min() {
        // Max statistic requires sub_type == "max", but config has "min"
        let configs = single_config(make_config(
            1,
            "cpu",
            "MinMax",
            "min",
            300,
            "tumbling",
            &[],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Max], Some(300_000), &[], ""),
        );
        assert!(result.is_none());
    }

    // --- multi-population ---

    #[test]
    fn multi_pop_finds_key_agg() {
        let mut configs = HashMap::new();
        configs.insert(
            10,
            make_config(
                10,
                "req",
                "CountMinSketchWithHeap",
                "",
                300,
                "tumbling",
                &[],
                "",
            ),
        );
        configs.insert(
            11,
            make_config(
                11,
                "req",
                "DeltaSetAggregator",
                "",
                300,
                "tumbling",
                &[],
                "",
            ),
        );
        let result = find_compatible_aggregation(
            &configs,
            &req("req", &[Statistic::Topk], Some(300_000), &[], ""),
        );
        let info = result.unwrap();
        assert_eq!(info.aggregation_id_for_value, 10);
        assert_eq!(info.aggregation_id_for_key, 11);
    }

    #[test]
    fn multi_pop_no_key_agg_returns_none() {
        // CountMinSketchWithHeap present but no SetAggregator/DeltaSetAggregator
        let configs = single_config(make_config(
            10,
            "req",
            "CountMinSketchWithHeap",
            "",
            300,
            "tumbling",
            &[],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("req", &[Statistic::Topk], Some(300_000), &[], ""),
        );
        assert!(result.is_none());
    }

    // --- avg (Vec<Statistic>) ---

    #[test]
    fn avg_finds_sum_and_count() {
        let mut configs = HashMap::new();
        configs.insert(
            1,
            make_config(1, "cpu", "Sum", "", 300, "tumbling", &["job"], ""),
        );
        configs.insert(
            2,
            make_config(
                2,
                "cpu",
                "CountMinSketch",
                "",
                300,
                "tumbling",
                &["job"],
                "",
            ),
        );
        let result = find_compatible_aggregation(
            &configs,
            &req(
                "cpu",
                &[Statistic::Sum, Statistic::Count],
                Some(300_000),
                &["job"],
                "",
            ),
        );
        assert!(result.is_some());
    }

    #[test]
    fn avg_different_windows_rejected() {
        let mut configs = HashMap::new();
        configs.insert(
            1,
            make_config(1, "cpu", "Sum", "", 300, "tumbling", &["job"], ""),
        );
        // Count config has different window_size — must be rejected
        configs.insert(
            2,
            make_config(
                2,
                "cpu",
                "CountMinSketch",
                "",
                900,
                "tumbling",
                &["job"],
                "",
            ),
        );
        let result = find_compatible_aggregation(
            &configs,
            &req(
                "cpu",
                &[Statistic::Sum, Statistic::Count],
                Some(300_000),
                &["job"],
                "",
            ),
        );
        assert!(result.is_none());
    }
}
