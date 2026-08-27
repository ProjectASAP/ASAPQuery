use std::cmp::Ordering;
use std::collections::HashMap;

use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;
use tracing::{debug, warn};

use crate::aggregation_config::{AggregationConfig, AggregationIdInfo};
use crate::enums::WindowType;
use crate::query_requirements::QueryRequirements;
use crate::utils::normalize_spatial_filter;
use promql_utilities::query_logics::enums::AggregationType;

// ---------------------------------------------------------------------------
// Pure compatibility helpers
// ---------------------------------------------------------------------------

/// Returns the aggregation types that can serve this statistic.
pub fn compatible_agg_types(stat: Statistic) -> &'static [AggregationType] {
    match stat {
        Statistic::Sum => &[AggregationType::Sum, AggregationType::MultipleSum],
        Statistic::Count => &[
            AggregationType::CountMinSketch,
            AggregationType::CountMinSketchWithHeap,
        ],
        Statistic::Min | Statistic::Max => {
            &[AggregationType::MinMax, AggregationType::MultipleMinMax]
        }
        Statistic::Quantile => &[AggregationType::DatasketchesKLL, AggregationType::HydraKLL],
        Statistic::Rate | Statistic::Increase => {
            &[AggregationType::Increase, AggregationType::MultipleIncrease]
        }
        Statistic::Cardinality => &[
            AggregationType::SetAggregator,
            AggregationType::DeltaSetAggregator,
            AggregationType::HLL,
        ],
        Statistic::Topk => &[AggregationType::CountMinSketchWithHeap],
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
pub fn is_multi_population_value_type(agg_type: AggregationType) -> bool {
    agg_type.is_multi_population_value_type()
}

/// Whether this type is a key aggregation (tracks which label-value combinations exist).
fn is_key_agg_type(agg_type: AggregationType) -> bool {
    agg_type.is_key_agg_type()
}

/// Whether `agg_type` may legitimately be planned/served with `window_type`.
///
/// DeltaSetAggregator only tracks added/removed keys since the last window,
/// so it's only correct for non-overlapping (tumbling) windows (#588) --
/// unlike its sibling key aggregations (SetAggregator, HLL), which are fine
/// under Sliding. This is the single source of truth for that invariant;
/// both producer sites (the live planner's agg_config.rs and the optimizer's
/// candidate_gen.rs) are expected to never emit a Sliding DeltaSetAggregator
/// config, but this predicate is the defensive check that stops one from
/// ever being *selected* to serve a query, regardless of how it was produced.
pub fn key_agg_window_valid(agg_type: AggregationType, window_type: WindowType) -> bool {
    !(agg_type == AggregationType::DeltaSetAggregator && window_type == WindowType::Sliding)
}

fn effective_grid_step(config: &AggregationConfig) -> u64 {
    match config.window_type {
        WindowType::Tumbling => config.window_size_ms,
        WindowType::Sliding => config.slide_interval_ms,
    }
}

/// Whether a separate key aggregation can resolve the populations of a
/// value aggregation on the same epoch-zero window grid.
pub fn key_agg_compatible_with_value(value: &AggregationConfig, key: &AggregationConfig) -> bool {
    if !key_agg_window_valid(key.aggregation_type, key.window_type) {
        return false;
    }

    match key.aggregation_type {
        AggregationType::SetAggregator => {
            key.window_type == value.window_type
                && key.window_size_ms == value.window_size_ms
                && effective_grid_step(key) == effective_grid_step(value)
        }
        AggregationType::DeltaSetAggregator if value.window_type == WindowType::Sliding => {
            let value_slide_ms = value.slide_interval_ms;
            let delta_window_ms = key.window_size_ms;
            key.window_type == WindowType::Tumbling
                && value_slide_ms > 0
                && delta_window_ms > 0
                && value_slide_ms.is_multiple_of(delta_window_ms)
                && value.window_size_ms.is_multiple_of(delta_window_ms)
        }
        AggregationType::DeltaSetAggregator => key.window_type == WindowType::Tumbling,
        _ => false,
    }
}

/// Window compatibility: can `config` serve a query needing `data_range_ms`?
///
/// Both Tumbling and Sliding require `data_range_ms` to be a positive integer
/// multiple of `window_size_ms`. Sliding execution selects a non-overlapping,
/// `window_size_ms`-spaced subset from the denser slide grid (#554); it must
/// never merge every overlapping window on that grid.
pub fn window_compatible(config: &AggregationConfig, data_range_ms: u64) -> bool {
    if !key_agg_window_valid(config.aggregation_type, config.window_type) {
        return false;
    }
    let window_ms = config.window_size_ms;
    if window_ms == 0 || data_range_ms == 0 {
        return false;
    }
    match config.window_type {
        WindowType::Sliding => {
            config.slide_interval_ms > 0
                && window_ms.is_multiple_of(config.slide_interval_ms)
                && data_range_ms.is_multiple_of(window_ms)
        }
        WindowType::Tumbling => data_range_ms.is_multiple_of(window_ms),
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

/// Reads the `count_events` parameter from a `CountMinSketchWithHeap` config.
/// Defaults to `true` (COUNT semantics) so existing count top-k configs that
/// omit the flag keep matching COUNT top-k queries.
fn config_count_events(config: &AggregationConfig) -> bool {
    config
        .parameters
        .get("count_events")
        .and_then(|v| v.as_bool())
        .unwrap_or(true)
}

/// Top-k weighting compatibility. Only constrains `Statistic::Topk` candidates;
/// every other statistic passes unconditionally.
///
/// A COUNT top-k query (`Some(true)`) must be served by a `count_events: true`
/// sketch and a SUM top-k query (`Some(false)`) by a `count_events: false`
/// (value-weighted) sketch. This is what tells two `CountMinSketchWithHeap`
/// configs on the same metric apart. `None` (non-top-k, or PromQL top-k which
/// does not pin the weighting) imposes no constraint.
pub fn topk_weighting_compatible(
    stat: Statistic,
    config: &AggregationConfig,
    req_count_events: Option<bool>,
) -> bool {
    if stat != Statistic::Topk {
        return true;
    }
    match req_count_events {
        Some(want) => config_count_events(config) == want,
        None => true,
    }
}

/// Aggregation priority comparator: prefer larger `window_size_ms` (descending).
/// This is a separate function so callers can swap the policy without touching matching logic.
pub fn aggregation_priority(a: &AggregationConfig, b: &AggregationConfig) -> Ordering {
    b.window_size_ms.cmp(&a.window_size_ms)
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
///    served by configs sharing the same `window_size_ms` and `grouping_labels`.
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
                    && types.contains(&c.aggregation_type)
                    && sub_type.is_none_or(|st| c.aggregation_sub_type == st)
                    && window_compatible(c, requirements.data_range_ms)
                    && labels_compatible(&c.grouping_labels, &requirements.grouping_labels)
                    && spatial_filter_compatible(
                        &c.spatial_filter_normalized,
                        &requirements.spatial_filter_normalized,
                    )
                    && topk_weighting_compatible(stat, c, requirements.topk_count_events);
                if !ok {
                    debug!(
                        agg_id = c.aggregation_id,
                        agg_type = %c.aggregation_type,
                        metric = %c.metric,
                        window_size_ms = c.window_size_ms,
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
            chosen_window_size_ms = candidates[0].window_size_ms,
            "capability matching: found candidates, chose best",
        );

        per_stat_candidates.push(candidates);
    }

    // Pick the best candidate for the first statistic.
    let value_agg = per_stat_candidates[0][0];

    // For multi-statistic requirements, the remaining statistics must be served by a
    // config that agrees on window_size_ms and grouping_labels with the chosen value agg.
    for (i, candidates) in per_stat_candidates.iter().enumerate().skip(1) {
        let found = candidates.iter().any(|c| {
            c.window_size_ms == value_agg.window_size_ms
                && c.grouping_labels == value_agg.grouping_labels
        });
        if !found {
            warn!(
                metric = %requirements.metric,
                statistic = ?requirements.statistics[i],
                required_window_size_ms = value_agg.window_size_ms,
                "capability matching: no matching window/labels for multi-statistic requirement",
            );
            return None;
        }
    }

    // If value type is multi-population, find the paired key aggregation.
    // Top-k (CountMinSketchWithHeap) follows the same path as plain COUNT: the
    // self-keyed case is expressed via the query_config path (a single
    // aggregation reference), while the capability-matching fallback resolves a
    // separate key aggregation just like any other multi-population value type.
    let key_agg: &AggregationConfig = if is_multi_population_value_type(value_agg.aggregation_type)
    {
        // Single pass: take the first window-valid key agg on this metric,
        // but also remember the first window-*invalid* one seen (e.g. a
        // Sliding DeltaSetAggregator) so the miss path below can report the
        // specific reason instead of a generic "none found" (#588).
        let mut invalid_window: Option<&AggregationConfig> = None;
        let ka = configs.values().find(|c| {
            if c.metric != requirements.metric || !is_key_agg_type(c.aggregation_type) {
                return false;
            }
            if key_agg_compatible_with_value(value_agg, c) {
                true
            } else {
                invalid_window.get_or_insert(c);
                false
            }
        });
        if ka.is_none() {
            match invalid_window {
                Some(bad) => warn!(
                    metric = %requirements.metric,
                    value_agg_type = %value_agg.aggregation_type,
                    key_agg_type = %bad.aggregation_type,
                    key_agg_window_type = ?bad.window_type,
                    "capability matching: found a key agg on this metric but its window_type is invalid for its aggregation_type (e.g. DeltaSetAggregator must be Tumbling) -- treating as absent",
                ),
                None => warn!(
                    metric = %requirements.metric,
                    value_agg_type = %value_agg.aggregation_type,
                    "capability matching: multi-population value agg requires a key agg (SetAggregator/DeltaSetAggregator) but none found",
                ),
            }
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
        aggregation_type_for_value: value_agg.aggregation_type,
        aggregation_id_for_key: key_agg.aggregation_id,
        aggregation_type_for_key: key_agg.aggregation_type,
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
        window_size_ms: u64,
        window_type: &str,
        grouping: &[&str],
        spatial_filter: &str,
    ) -> AggregationConfig {
        let grouping_labels =
            KeyByLabelNames::new(grouping.iter().map(|s| s.to_string()).collect());
        let spatial_filter_normalized = normalize_spatial_filter(spatial_filter);
        AggregationConfig {
            aggregation_id: id,
            aggregation_type: agg_type.parse::<AggregationType>().expect("valid agg type"),
            aggregation_sub_type: sub_type.to_string(),
            parameters: HashMap::new(),
            grouping_labels,
            aggregated_labels: KeyByLabelNames::new(vec![]),
            rollup_labels: KeyByLabelNames::new(vec![]),
            original_yaml: String::new(),
            window_size_ms,
            slide_interval_ms: window_size_ms,
            window_type: window_type.parse::<WindowType>().unwrap_or_default(),
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
        data_range_ms: u64,
        grouping: &[&str],
        spatial_filter: &str,
    ) -> QueryRequirements {
        QueryRequirements {
            metric: metric.to_string(),
            statistics: stats.to_vec(),
            data_range_ms,
            grouping_labels: KeyByLabelNames::new(grouping.iter().map(|s| s.to_string()).collect()),
            spatial_filter_normalized: normalize_spatial_filter(spatial_filter),
            topk_count_events: None,
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
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300_000,
            "tumbling",
            &[],
            "",
        ));
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Sum], 300_000, &[], ""));
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
            300_000,
            "tumbling",
            &[],
            "",
        ));
        // quantile value (0.5 or 0.9) is NOT part of QueryRequirements — both should find the same config
        let r1 = find_compatible_aggregation(
            &configs,
            &req("lat", &[Statistic::Quantile], 300_000, &[], ""),
        );
        let r2 = find_compatible_aggregation(
            &configs,
            &req("lat", &[Statistic::Quantile], 300_000, &[], ""),
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
            300_000,
            "tumbling",
            &[],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("lat", &[Statistic::Quantile], 300_000, &[], ""),
        );
        assert_eq!(result.unwrap().aggregation_id_for_value, 3);
    }

    #[test]
    fn no_match_wrong_metric() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300_000,
            "tumbling",
            &[],
            "",
        ));
        let result =
            find_compatible_aggregation(&configs, &req("mem", &[Statistic::Sum], 300_000, &[], ""));
        assert!(result.is_none());
    }

    #[test]
    fn no_match_wrong_type() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "DatasketchesKLL",
            "",
            300_000,
            "tumbling",
            &[],
            "",
        ));
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Sum], 300_000, &[], ""));
        assert!(result.is_none());
    }

    // --- window compatibility ---

    #[test]
    fn window_tumbling_exact() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300_000,
            "tumbling",
            &[],
            "",
        ));
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Sum], 300_000, &[], ""));
        assert!(result.is_some());
    }

    #[test]
    fn window_tumbling_divisible() {
        // 900_000 ms / 300_000 ms = 3 buckets — valid merge
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300_000,
            "tumbling",
            &[],
            "",
        ));
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Sum], 900_000, &[], ""));
        assert!(result.is_some());
    }

    #[test]
    fn window_tumbling_not_divisible() {
        // 600_000 ms / 900_000 ms is not a whole number
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            900_000,
            "tumbling",
            &[],
            "",
        ));
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Sum], 600_000, &[], ""));
        assert!(result.is_none());
    }

    #[test]
    fn window_sliding_exact() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300_000,
            "sliding",
            &[],
            "",
        ));
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Sum], 300_000, &[], ""));
        assert!(result.is_some());
    }

    #[test]
    fn window_sliding_wider_exact_multiple_is_compatible() {
        // Two non-overlapping stored 300_000ms windows exactly cover the query.
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300_000,
            "sliding",
            &[],
            "",
        ));
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Sum], 600_000, &[], ""));
        assert!(result.is_some());
    }

    #[test]
    fn window_priority_largest_wins() {
        let mut configs = HashMap::new();
        configs.insert(
            1,
            make_config(1, "cpu", "Sum", "", 300_000, "tumbling", &[], ""),
        );
        configs.insert(
            2,
            make_config(2, "cpu", "Sum", "", 900_000, "tumbling", &[], ""),
        );
        // 900_000 ms is divisible by both 300_000 ms and 900_000 ms — prefer 900_000 ms
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Sum], 900_000, &[], ""));
        assert_eq!(result.unwrap().aggregation_id_for_value, 2);
    }

    #[test]
    fn spatial_only_no_range() {
        // spatial-only queries use data_range_ms = scrape interval; window must match
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            15_000,
            "tumbling",
            &[],
            "",
        ));
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Sum], 15_000, &[], ""));
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
            300_000,
            "tumbling",
            &["job"],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], 300_000, &["job"], ""),
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
            300_000,
            "tumbling",
            &["job", "instance"],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], 300_000, &["job"], ""),
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
            300_000,
            "tumbling",
            &["region"],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], 300_000, &["job"], ""),
        );
        assert!(result.is_none());
    }

    // --- spatial filter compatibility ---

    #[test]
    fn spatial_filter_empty_both() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300_000,
            "tumbling",
            &[],
            "",
        ));
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Sum], 300_000, &[], ""));
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
            300_000,
            "tumbling",
            &[],
            "env=prod",
        ));
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Sum], 300_000, &[], ""));
        assert!(result.is_none());
    }

    #[test]
    fn spatial_filter_same() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300_000,
            "tumbling",
            &[],
            "env=prod",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], 300_000, &[], "env=prod"),
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
            300_000,
            "tumbling",
            &[],
            "env=prod",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], 300_000, &[], "env=staging"),
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
            300_000,
            "tumbling",
            &[],
            "",
        ));
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Min], 300_000, &[], ""));
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
            300_000,
            "tumbling",
            &[],
            "",
        ));
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Max], 300_000, &[], ""));
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
                300_000,
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
                300_000,
                "tumbling",
                &[],
                "",
            ),
        );
        let result = find_compatible_aggregation(
            &configs,
            &req("req", &[Statistic::Topk], 300_000, &[], ""),
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
            300_000,
            "tumbling",
            &[],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("req", &[Statistic::Topk], 300_000, &[], ""),
        );
        assert!(result.is_none());
    }

    // --- issue #588: DeltaSetAggregator must be restricted to tumbling windows ---

    #[test]
    fn key_agg_window_valid_rejects_delta_set_aggregator_sliding() {
        assert!(!key_agg_window_valid(
            AggregationType::DeltaSetAggregator,
            WindowType::Sliding
        ));
    }

    #[test]
    fn key_agg_window_valid_accepts_delta_set_aggregator_tumbling() {
        assert!(key_agg_window_valid(
            AggregationType::DeltaSetAggregator,
            WindowType::Tumbling
        ));
    }

    #[test]
    fn key_agg_window_valid_accepts_set_aggregator_sliding() {
        // SetAggregator (unlike DeltaSetAggregator) legitimately supports sliding.
        assert!(key_agg_window_valid(
            AggregationType::SetAggregator,
            WindowType::Sliding
        ));
    }

    #[test]
    fn window_compatible_rejects_sliding_delta_set_aggregator_even_on_exact_range_match() {
        // data_range_ms == window_size_ms would normally satisfy the Sliding
        // rule -- the rejection must come from the aggregation_type check,
        // not the range/window_size arithmetic.
        let config = make_config(
            1,
            "req",
            "DeltaSetAggregator",
            "",
            300_000,
            "sliding",
            &[],
            "",
        );
        assert!(!window_compatible(&config, 300_000));
    }

    #[test]
    fn window_compatible_still_accepts_sliding_set_aggregator() {
        let config = make_config(1, "req", "SetAggregator", "", 300_000, "sliding", &[], "");
        assert!(window_compatible(&config, 300_000));
    }

    #[test]
    fn window_compatible_rejects_sliding_window_not_aligned_to_slide() {
        let mut config = make_config(1, "req", "SetAggregator", "", 300_000, "sliding", &[], "");
        config.slide_interval_ms = 40_000;
        assert!(!window_compatible(&config, 600_000));
    }

    #[test]
    fn window_compatible_still_accepts_tumbling_delta_set_aggregator() {
        let config = make_config(
            1,
            "req",
            "DeltaSetAggregator",
            "",
            300_000,
            "tumbling",
            &[],
            "",
        );
        assert!(window_compatible(&config, 900_000));
    }

    #[test]
    fn multi_pop_rejects_sliding_delta_set_aggregator_key_agg() {
        // The key-agg pairing lookup used to match by (metric, is_key_agg_type)
        // alone, bypassing window_compatible entirely -- a Sliding
        // DeltaSetAggregator could be paired even though it can only ever
        // give incorrect merged add/remove sets under sliding windows.
        let mut configs = HashMap::new();
        configs.insert(
            10,
            make_config(
                10,
                "req",
                "CountMinSketchWithHeap",
                "",
                300_000,
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
                300_000,
                "sliding",
                &[],
                "",
            ),
        );
        let result = find_compatible_aggregation(
            &configs,
            &req("req", &[Statistic::Topk], 300_000, &[], ""),
        );
        assert!(
            result.is_none(),
            "a Sliding DeltaSetAggregator must never be selected as the paired key agg"
        );
    }

    #[test]
    fn multi_pop_accepts_sliding_set_aggregator_key_agg() {
        // Regression guard: the DeltaSetAggregator-specific rejection must not
        // block SetAggregator, which legitimately supports sliding windows.
        let mut configs = HashMap::new();
        configs.insert(
            10,
            make_config(
                10,
                "req",
                "CountMinSketchWithHeap",
                "",
                300_000,
                "sliding",
                &[],
                "",
            ),
        );
        configs.insert(
            11,
            make_config(11, "req", "SetAggregator", "", 300_000, "sliding", &[], ""),
        );
        let result = find_compatible_aggregation(
            &configs,
            &req("req", &[Statistic::Topk], 300_000, &[], ""),
        );
        let info =
            result.expect("Sliding SetAggregator on the value aggregation's grid must be accepted");
        assert_eq!(info.aggregation_id_for_key, 11);
    }

    #[test]
    fn multi_pop_rejects_tumbling_delta_set_that_cannot_partition_sliding_value_window() {
        let mut value = make_config(10, "req", "CountMinSketch", "", 6_000, "sliding", &[], "");
        value.slide_interval_ms = 1_000;
        let delta_keys = make_config(
            11,
            "req",
            "DeltaSetAggregator",
            "",
            2_000,
            "tumbling",
            &[],
            "",
        );
        let configs = HashMap::from([(10, value), (11, delta_keys)]);

        assert!(
            find_compatible_aggregation(
                &configs,
                &req("req", &[Statistic::Count], 12_000, &[], ""),
            )
            .is_none(),
            "D=2s would include future events at an S=1s boundary"
        );
    }

    #[test]
    fn multi_pop_accepts_tumbling_delta_set_that_partitions_sliding_value_grid() {
        let mut value = make_config(10, "req", "CountMinSketch", "", 6_000, "sliding", &[], "");
        value.slide_interval_ms = 1_000;
        let delta_keys = make_config(
            11,
            "req",
            "DeltaSetAggregator",
            "",
            1_000,
            "tumbling",
            &[],
            "",
        );
        let configs = HashMap::from([(10, value), (11, delta_keys)]);

        let result = find_compatible_aggregation(
            &configs,
            &req("req", &[Statistic::Count], 12_000, &[], ""),
        )
        .expect("D=1s lies on S=1s and exactly partitions W=6s");

        assert_eq!(result.aggregation_id_for_key, 11);
    }

    // --- avg (Vec<Statistic>) ---

    #[test]
    fn avg_finds_sum_and_count() {
        let mut configs = HashMap::new();
        configs.insert(
            1,
            make_config(1, "cpu", "Sum", "", 300_000, "tumbling", &["job"], ""),
        );
        configs.insert(
            2,
            make_config(
                2,
                "cpu",
                "CountMinSketch",
                "",
                300_000,
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
                300_000,
                &["job"],
                "",
            ),
        );
        assert!(result.is_some());
    }

    // --- cardinality / HLL ---

    #[test]
    fn cardinality_matches_hll_single_population() {
        // `COUNT(DISTINCT col)` flows in as `Statistic::Cardinality`. An HLL config
        // alone must satisfy it without requiring any paired key aggregation —
        // HLL is a single-population value type (per grouping key bucket), unlike
        // SetAggregator which is a multi-population key tracker.
        let configs = single_config(make_config(
            42,
            "peers",
            "HLL",
            "",
            1_000,
            "tumbling",
            &["srcip"],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("peers", &[Statistic::Cardinality], 1_000, &["srcip"], ""),
        );
        let info = result.expect("HLL should serve Cardinality");
        assert_eq!(info.aggregation_id_for_value, 42);
        assert_eq!(info.aggregation_type_for_value, AggregationType::HLL);
        // Single-population: key agg falls through to the value config itself,
        // matching the KLL / Sum / MinMax pattern (no separate SetAggregator needed).
        assert_eq!(info.aggregation_id_for_key, 42);
        assert_eq!(info.aggregation_type_for_key, AggregationType::HLL);
    }

    #[test]
    fn compatible_agg_types_cardinality_includes_hll() {
        // Direct unit test on the capability table: HLL must appear alongside the
        // existing exact-cardinality types so the SQL→engine path picks it up
        // without any further plumbing changes.
        let types = compatible_agg_types(Statistic::Cardinality);
        assert!(
            types.contains(&AggregationType::HLL),
            "compatible_agg_types(Cardinality) must include HLL; got {types:?}",
        );
        // Backwards compat: existing exact types stay supported.
        assert!(types.contains(&AggregationType::SetAggregator));
        assert!(types.contains(&AggregationType::DeltaSetAggregator));
    }

    #[test]
    fn avg_different_windows_rejected() {
        let mut configs = HashMap::new();
        configs.insert(
            1,
            make_config(1, "cpu", "Sum", "", 300_000, "tumbling", &["job"], ""),
        );
        // Count config has different window_size_ms — must be rejected
        configs.insert(
            2,
            make_config(
                2,
                "cpu",
                "CountMinSketch",
                "",
                900_000,
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
                300_000,
                &["job"],
                "",
            ),
        );
        assert!(result.is_none());
    }

    // --- top-k count vs sum weighting ---
    //
    // Top-k follows the same capability-matching path as plain COUNT: a
    // CountMinSketchWithHeap is a multi-population value type, so the fallback
    // pairs it with a key aggregation. These tests therefore always provision a
    // DeltaSetAggregator and focus on which *value* heap (count- vs sum-weighted)
    // is selected via the count_events discriminator.

    /// Paired key aggregation required by the multi-population fallback.
    fn make_key_agg(id: u64, metric: &str) -> AggregationConfig {
        make_config(
            id,
            metric,
            "DeltaSetAggregator",
            "",
            1_000,
            "tumbling",
            &[],
            "",
        )
    }

    /// `CountMinSketchWithHeap` config with an explicit `count_events` parameter.
    fn make_topk_config(id: u64, metric: &str, count_events: bool) -> AggregationConfig {
        let mut c = make_config(
            id,
            metric,
            "CountMinSketchWithHeap",
            "",
            1_000,
            "tumbling",
            &[],
            "",
        );
        c.parameters.insert(
            "count_events".to_string(),
            serde_json::Value::Bool(count_events),
        );
        c
    }

    fn topk_req(metric: &str, count_events: Option<bool>) -> QueryRequirements {
        let mut r = req(metric, &[Statistic::Topk], 1_000, &[], "");
        r.topk_count_events = count_events;
        r
    }

    #[test]
    fn topk_count_query_picks_count_events_sketch() {
        // Two heap sketches on the same metric: one count-weighted, one
        // sum-weighted. A COUNT top-k query must resolve to the count one.
        let mut configs = HashMap::new();
        configs.insert(1, make_topk_config(1, "netflow_table", true));
        configs.insert(2, make_topk_config(2, "netflow_table", false));
        configs.insert(9, make_key_agg(9, "netflow_table"));

        let result = find_compatible_aggregation(&configs, &topk_req("netflow_table", Some(true)))
            .expect("COUNT top-k should match the count_events sketch");
        assert_eq!(result.aggregation_id_for_value, 1);
        assert_eq!(result.aggregation_id_for_key, 9);
    }

    #[test]
    fn topk_sum_query_picks_value_weighted_sketch() {
        let mut configs = HashMap::new();
        configs.insert(1, make_topk_config(1, "netflow_table", true));
        configs.insert(2, make_topk_config(2, "netflow_table", false));
        configs.insert(9, make_key_agg(9, "netflow_table"));

        let result = find_compatible_aggregation(&configs, &topk_req("netflow_table", Some(false)))
            .expect("SUM top-k should match the count_events: false sketch");
        assert_eq!(result.aggregation_id_for_value, 2);
        assert_eq!(result.aggregation_id_for_key, 9);
    }

    #[test]
    fn topk_sum_query_rejects_count_only_sketch() {
        // Only a count-weighted sketch exists; a SUM top-k query cannot be served
        // even with a key agg available.
        let mut configs = HashMap::new();
        configs.insert(1, make_topk_config(1, "netflow_table", true));
        configs.insert(9, make_key_agg(9, "netflow_table"));
        let result = find_compatible_aggregation(&configs, &topk_req("netflow_table", Some(false)));
        assert!(
            result.is_none(),
            "SUM top-k must not fall back to a count_events: true sketch",
        );
    }

    #[test]
    fn topk_count_query_matches_sketch_without_explicit_flag() {
        // Configs that omit `count_events` default to count semantics, so a
        // COUNT top-k query still matches (backwards compatibility).
        let mut configs = HashMap::new();
        configs.insert(
            7,
            make_config(
                7,
                "netflow_table",
                "CountMinSketchWithHeap",
                "",
                1_000,
                "tumbling",
                &[],
                "",
            ),
        );
        configs.insert(9, make_key_agg(9, "netflow_table"));
        let result = find_compatible_aggregation(&configs, &topk_req("netflow_table", Some(true)))
            .expect("default (no flag) sketch should serve COUNT top-k");
        assert_eq!(result.aggregation_id_for_value, 7);
    }

    #[test]
    fn topk_unconstrained_weighting_matches_any_sketch() {
        // `topk_count_events: None` (e.g. PromQL top-k) imposes no weighting
        // constraint, so any heap sketch on the metric matches.
        let mut configs = HashMap::new();
        configs.insert(3, make_topk_config(3, "netflow_table", false));
        configs.insert(9, make_key_agg(9, "netflow_table"));
        let result = find_compatible_aggregation(&configs, &topk_req("netflow_table", None))
            .expect("unconstrained top-k should match regardless of count_events");
        assert_eq!(result.aggregation_id_for_value, 3);
    }
}
