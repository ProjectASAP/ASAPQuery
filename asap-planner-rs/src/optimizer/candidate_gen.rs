use std::collections::HashMap;

use asap_types::aggregation_config::AggregationConfig;
use asap_types::capability_matching::{compatible_agg_types, key_agg_window_valid};
use asap_types::enums::WindowType;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{AggregationType, Statistic};
use serde_json::Value;

use super::constants::{
    CMS_DEPTHS, CMS_HEAP_SIZES, CMS_WIDTHS, HLL_PRECISIONS, HYDRA_COLS, HYDRA_K, HYDRA_ROWS, KLL_KS,
};
use super::label_set_facts::ItemFacts;
use super::sketch_properties::sketch_properties;
use super::solution::{OptimizerItem, QueryMethod};
use crate::planner::agg_config::needs_key_aggregation;
use crate::planner::labels::set_subpopulation_labels;

/// Compatible types the optimizer never proposes. Single-group
/// Sum/MinMax/Increase cost the same as their Multiple* twins, so only the
/// Multiple* forms are offered. Filtered here rather than in
/// `compatible_agg_types`, which the engine also uses to match queries to
/// deployed configs.
const OPTIMIZER_SKIPPED_AGG_TYPES: &[AggregationType] = &[
    AggregationType::Sum,
    AggregationType::MinMax,
    AggregationType::Increase,
];

/// A candidate streaming config for one optimizer item, ready for cost evaluation.
#[derive(Debug, Clone)]
pub struct CandidateConfig {
    /// A streaming config; candidates without one are no longer generated.
    pub config: Option<AggregationConfig>,
    /// Query method derived from (ingest type × W vs range_a × sketch algebra).
    pub query_method: QueryMethod,
    /// Number of retained windows used at query time (n for Merge, 1 for Direct/Subtract, 0 for Exact).
    pub n_windows: u64,
    /// Sketch instances the engine creates: one per distinct value of the
    /// config's grouping labels (1 when they are empty).
    pub instance_count: u64,
    /// Distinct value combinations of the item's output labels.
    pub output_group_count: u64,
    /// Paired key aggregation (DeltaSetAggregator) deployed alongside
    /// `config` when the value sketch can't list its own keys.
    pub key_config: Option<AggregationConfig>,
}

/// Enumerate all structurally valid candidate configs for an optimizer item.
///
/// Iterates over compatible agg types × parameter grid × valid window sizes ×
/// {Tumbling, Sliding}. Multi-statistic items yield no candidates because a
/// single sketch cannot serve incompatible statistics simultaneously.
pub fn enumerate_candidates(item: &OptimizerItem, scrape_interval_ms: u64) -> Vec<CandidateConfig> {
    let one_group = ItemFacts {
        output_group_count: 1,
        topk_by_group_count: item.requirements.topk_by_labels.as_ref().map(|_| 1),
        arrival_rate_per_sec: 1.0,
    };
    enumerate_candidates_with_facts(item, scrape_interval_ms, &one_group)
}

/// Enumerate candidates, stamping group counts from the item's label-set facts.
pub fn enumerate_candidates_with_facts(
    item: &OptimizerItem,
    scrape_interval_ms: u64,
    facts: &ItemFacts,
) -> Vec<CandidateConfig> {
    assert!(
        facts.output_group_count > 0,
        "output_group_count must be greater than zero"
    );
    let mut candidates = Vec::new();

    if item.requirements.statistics.len() != 1 {
        return candidates;
    }

    let stat = item.requirements.statistics[0];
    let range_a_ms = item.requirements.data_range_ms;

    for &agg_type in compatible_agg_types(stat) {
        if OPTIMIZER_SKIPPED_AGG_TYPES.contains(&agg_type) {
            continue;
        }
        let props = sketch_properties(agg_type);

        // CountMinSketchWithHeap's SUM/COUNT weighting lives in aggregation_sub_type
        // (#670), so unlike other types it can vary independently of the sketch's
        // dimension params -- enumerate both weightings when the query doesn't pin one.
        let sub_type_variants: Vec<String> = if agg_type == AggregationType::CountMinSketchWithHeap
        {
            match item.requirements.topk_count_events {
                Some(true) => vec!["count".to_string()],
                Some(false) => vec!["sum".to_string()],
                None => vec!["count".to_string(), "sum".to_string()],
            }
        } else {
            vec![derive_sub_type(stat, agg_type)]
        };

        for sub_type in &sub_type_variants {
            for params in param_grid(agg_type) {
                for (window_type, w, slide_interval, n) in
                    window_candidates(range_a_ms, item.t_repeat_ms, scrape_interval_ms)
                {
                    // DeltaSetAggregator only tracks added/removed keys since the
                    // last window, so it's only correct for non-overlapping
                    // (tumbling) windows (#588) -- same invariant enforced by
                    // capability_matching's window_compatible() at query time.
                    if !key_agg_window_valid(agg_type, window_type) {
                        continue;
                    }

                    let Some(qm) = determine_query_method(n, &props) else {
                        continue;
                    };

                    let config = build_config(
                        item,
                        stat,
                        agg_type,
                        sub_type,
                        &params,
                        window_type,
                        w,
                        slide_interval,
                        n,
                    );
                    candidates.push(CandidateConfig {
                        instance_count: instance_count(&config, item, facts),
                        key_config: needs_key_aggregation(agg_type)
                            .then(|| build_key_config(&config, range_a_ms)),
                        config: Some(config),
                        query_method: qm,
                        n_windows: n,
                        output_group_count: facts.output_group_count,
                    });
                }
            }
        }
    }

    candidates
}

/// The DeltaSetAggregator paired with `value`, as the legacy planner builds
/// it: same labels, Tumbling at the value's slide (DeltaSet is only correct
/// for non-overlapping windows), retaining enough panes to cover the query
/// range.
fn build_key_config(value: &AggregationConfig, range_a_ms: u64) -> AggregationConfig {
    let pane_ms = value.slide_interval_ms;
    AggregationConfig::new(
        0, // placeholder; overwritten by OptimizerSolution::register_config when deployed
        AggregationType::DeltaSetAggregator,
        String::new(),
        HashMap::new(),
        value.grouping_labels.clone(),
        value.aggregated_labels.clone(),
        KeyByLabelNames::empty(), // rollup_labels
        String::new(),            // original_yaml
        pane_ms,
        pane_ms,
        WindowType::Tumbling,
        value.spatial_filter.clone(),
        value.metric.clone(),
        Some(range_a_ms.div_ceil(pane_ms)),
        None, // read_count_threshold
        None, // table_name (SQL only)
        None, // value_column (SQL only)
    )
}

/// Instances the engine creates for `config`: its grouping labels are empty,
/// the item's output labels, or (for `topk by`) the bucketing labels.
fn instance_count(config: &AggregationConfig, item: &OptimizerItem, facts: &ItemFacts) -> u64 {
    if config.grouping_labels.is_empty() {
        1
    } else if config.grouping_labels == item.requirements.grouping_labels {
        facts.output_group_count
    } else {
        facts
            .topk_by_group_count
            .expect("grouping labels other than the output labels come from `topk by`")
    }
}

/// Window candidates: (WindowType, W_ms, slide_interval_ms, n_windows).
///
/// Every candidate satisfies `L % W == 0`, `T % S == 0`, and `W % S == 0`.
/// Tumbling uses `S = W`; sliding uses `S < W`. Both dimensions remain aligned
/// to the scrape interval.
fn window_candidates(
    range_a_ms: u64,
    t_repeat_ms: u64,
    scrape_interval_ms: u64,
) -> Vec<(WindowType, u64, u64, u64)> {
    let range_a = range_a_ms;
    if range_a == 0 || scrape_interval_ms == 0 {
        return vec![];
    }

    let mut out = Vec::new();

    let tumbling_divisor = super::aqe_extractor::gcd(range_a, t_repeat_ms);
    let mut w = scrape_interval_ms;
    while w <= tumbling_divisor {
        if tumbling_divisor.is_multiple_of(w) {
            let n = range_a / w;
            out.push((WindowType::Tumbling, w, w, n));
        }
        w += scrape_interval_ms;
    }

    let mut w = scrape_interval_ms;
    while w <= range_a {
        if range_a.is_multiple_of(w) {
            let k = range_a / w;
            let slide_divisor = super::aqe_extractor::gcd(w, t_repeat_ms);
            let mut s = scrape_interval_ms;
            while s < w {
                if slide_divisor.is_multiple_of(s) {
                    out.push((WindowType::Sliding, w, s, k));
                }
                s += scrape_interval_ms;
            }
        }
        w += scrape_interval_ms;
    }

    out
}

/// Determine query method from (n_windows, sketch algebra).
/// Returns None when the combination is infeasible (W < range_a + neither merge nor subtract).
fn determine_query_method(
    n_windows: u64,
    props: &super::sketch_properties::SketchProperties,
) -> Option<QueryMethod> {
    if n_windows == 1 {
        // W = range_a (or spatial-only): one completed window covers the query range exactly.
        return Some(QueryMethod::Direct);
    }
    // n > 1: partial-width windows (W < range_a); valid for both Tumbling and Sliding.
    if props.subtractable {
        Some(QueryMethod::Subtract)
    } else if props.mergeable {
        Some(QueryMethod::Merge {
            num_windows: n_windows,
        })
    } else {
        None
    }
}

/// Build an AggregationConfig from candidate parameters. aggregation_id = 0 is a
/// placeholder never used past cost evaluation — OptimizerSolution::register_config
/// overwrites it with a real id when (if) a solver deploys this candidate.
#[allow(clippy::too_many_arguments)]
fn build_config(
    item: &OptimizerItem,
    stat: Statistic,
    agg_type: AggregationType,
    sub_type: &str,
    params: &HashMap<String, Value>,
    window_type: WindowType,
    w: u64,
    slide_interval: u64,
    n_windows: u64,
) -> AggregationConfig {
    // Same grouping/aggregated split as the legacy planner: keyed types hold
    // the output labels as keys inside one instance; others get one instance
    // per group; `topk by` gets one heap per bucket.
    let mut grouping = KeyByLabelNames::empty();
    let mut aggregated = KeyByLabelNames::empty();
    set_subpopulation_labels(
        stat,
        agg_type,
        &item.requirements.grouping_labels,
        item.requirements.topk_by_labels.as_ref(),
        &mut KeyByLabelNames::empty(),
        &mut grouping,
        &mut aggregated,
    );

    AggregationConfig::new(
        0, // placeholder; overwritten by OptimizerSolution::register_config when deployed
        agg_type,
        sub_type.to_string(),
        params.clone(),
        grouping,
        aggregated,
        KeyByLabelNames::empty(), // rollup_labels
        String::new(),            // original_yaml
        w,
        slide_interval,
        window_type,
        item.requirements.spatial_filter_normalized.clone(),
        item.requirements.metric.clone(),
        Some(n_windows),
        None, // read_count_threshold
        None, // table_name (SQL only)
        None, // value_column (SQL only)
    )
}

/// aggregation_sub_type string expected by the streaming engine and capability matching.
/// Not called for `CountMinSketchWithHeap` -- its sub_type carries SUM/COUNT
/// weighting, enumerated separately in the caller (#670).
fn derive_sub_type(stat: Statistic, agg_type: AggregationType) -> String {
    match (stat, agg_type) {
        (Statistic::Min, _) => "min",
        (Statistic::Max, _) => "max",
        (Statistic::Sum, AggregationType::CountMinSketch | AggregationType::MultipleSum) => "sum",
        (Statistic::Count, AggregationType::CountMinSketch) => "count",
        _ => "",
    }
    .to_string()
}

fn param_grid(agg_type: AggregationType) -> Vec<HashMap<String, Value>> {
    match agg_type {
        AggregationType::CountMinSketch => {
            let mut grids = Vec::new();
            for &d in CMS_DEPTHS {
                for &w in CMS_WIDTHS {
                    let mut m = HashMap::new();
                    m.insert("depth".into(), Value::from(d));
                    m.insert("width".into(), Value::from(w));
                    grids.push(m);
                }
            }
            grids
        }

        AggregationType::CountMinSketchWithHeap => {
            let mut grids = Vec::new();
            for &d in CMS_DEPTHS {
                for &w in CMS_WIDTHS {
                    for &h in CMS_HEAP_SIZES {
                        let mut m = HashMap::new();
                        m.insert("depth".into(), Value::from(d));
                        m.insert("width".into(), Value::from(w));
                        m.insert("heapsize".into(), Value::from(h));
                        grids.push(m);
                    }
                }
            }
            grids
        }

        AggregationType::DatasketchesKLL => KLL_KS
            .iter()
            .map(|&k| {
                let mut m = HashMap::new();
                m.insert("K".into(), Value::from(k));
                m
            })
            .collect(),

        AggregationType::HydraKLL => {
            let mut grids = Vec::new();
            for &r in HYDRA_ROWS {
                for &c in HYDRA_COLS {
                    let mut m = HashMap::new();
                    m.insert("row_num".into(), Value::from(r));
                    m.insert("col_num".into(), Value::from(c));
                    m.insert("k".into(), Value::from(HYDRA_K));
                    grids.push(m);
                }
            }
            grids
        }

        AggregationType::HLL => HLL_PRECISIONS
            .iter()
            .map(|&p| {
                let mut m = HashMap::new();
                m.insert("precision".into(), Value::from(p));
                m
            })
            .collect(),

        // Parameterless types: one empty-params entry per type.
        _ => vec![HashMap::new()],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::enums::WindowType;
    use promql_utilities::data_model::KeyByLabelNames;

    fn make_aqe(stat: Statistic, range_ms: u64, min_t: u64) -> OptimizerItem {
        use asap_types::query_requirements::QueryRequirements;
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
            query_frequency_hz: 1.0 / 60.0,
            t_repeat_ms: min_t,
            accuracy_sla: 0.0,
            latency_sla_ms: None,
        }
    }

    #[test]
    fn does_not_include_exact_fallback() {
        let aqe = make_aqe(Statistic::Sum, 300_000, 60_000);
        let candidates = enumerate_candidates(&aqe, 15_000);
        assert!(candidates.iter().all(|c| c.config.is_some()));
    }

    #[test]
    fn single_group_trivial_accumulators_are_not_proposed() {
        for (stat, skipped, kept) in [
            (
                Statistic::Sum,
                AggregationType::Sum,
                AggregationType::MultipleSum,
            ),
            (
                Statistic::Min,
                AggregationType::MinMax,
                AggregationType::MultipleMinMax,
            ),
            (
                Statistic::Increase,
                AggregationType::Increase,
                AggregationType::MultipleIncrease,
            ),
        ] {
            let types: Vec<AggregationType> =
                enumerate_candidates(&make_aqe(stat, 300_000, 60_000), 15_000)
                    .into_iter()
                    .filter_map(|c| c.config.map(|cfg| cfg.aggregation_type))
                    .collect();
            assert!(
                !types.contains(&skipped),
                "{skipped:?} must not be proposed"
            );
            assert!(types.contains(&kept), "{kept:?} must still be proposed");
        }
    }

    #[test]
    fn multiple_sum_candidates_get_a_non_empty_sub_type() {
        // MultipleSum's factory now rejects an empty aggregation_sub_type (#503) --
        // the optimizer must derive "sum" for it, same as it already does for
        // CountMinSketch, or every MultipleSum candidate fails to ingest.
        let aqe = make_aqe(Statistic::Sum, 300_000, 60_000);
        let candidates = enumerate_candidates(&aqe, 15_000);
        let multiple_sum_configs: Vec<_> = candidates
            .iter()
            .filter_map(|c| c.config.as_ref())
            .filter(|cfg| cfg.aggregation_type == AggregationType::MultipleSum)
            .collect();
        assert!(
            !multiple_sum_configs.is_empty(),
            "expected at least one MultipleSum candidate"
        );
        for cfg in multiple_sum_configs {
            assert_eq!(cfg.aggregation_sub_type, "sum");
        }
    }

    fn labels(names: &[&str]) -> KeyByLabelNames {
        KeyByLabelNames::new(names.iter().map(|n| n.to_string()).collect())
    }

    fn facts(output: u64, topk_by: Option<u64>) -> ItemFacts {
        ItemFacts {
            output_group_count: output,
            topk_by_group_count: topk_by,
            arrival_rate_per_sec: 1.0,
        }
    }

    /// Before the optimizer reused the legacy planner's label split, every
    /// config put the output labels in `grouping_labels`, so the engine built
    /// one keyed map/sketch per group (and one top-k heap per series).
    #[test]
    fn keyed_types_hold_groups_inside_one_instance_and_per_group_types_do_not() {
        for stat in [Statistic::Sum, Statistic::Quantile] {
            let mut aqe = make_aqe(stat, 300_000, 60_000);
            aqe.requirements.grouping_labels = labels(&["svc"]);
            let candidates = enumerate_candidates_with_facts(&aqe, 15_000, &facts(7, None));

            for c in &candidates {
                assert_eq!(c.output_group_count, 7);
                let Some(cfg) = &c.config else { continue };
                match cfg.aggregation_type {
                    AggregationType::MultipleSum
                    | AggregationType::CountMinSketch
                    | AggregationType::HydraKLL => {
                        assert!(cfg.grouping_labels.is_empty(), "{cfg:?}");
                        assert_eq!(cfg.aggregated_labels, labels(&["svc"]));
                        assert_eq!(c.instance_count, 1);
                    }
                    AggregationType::DatasketchesKLL => {
                        assert_eq!(cfg.grouping_labels, labels(&["svc"]));
                        assert!(cfg.aggregated_labels.is_empty());
                        assert_eq!(c.instance_count, 7);
                    }
                    other => panic!("unexpected candidate type {other:?}"),
                }
            }
        }
    }

    #[test]
    fn only_cms_and_hydra_get_a_paired_tumbling_delta_set() {
        for stat in [Statistic::Sum, Statistic::Quantile, Statistic::Topk] {
            let mut aqe = make_aqe(stat, 600_000, 30_000);
            aqe.requirements.grouping_labels = labels(&["svc"]);
            for c in enumerate_candidates(&aqe, 30_000) {
                let Some(cfg) = &c.config else { continue };
                match (&c.key_config, needs_key_aggregation(cfg.aggregation_type)) {
                    (Some(key), true) => {
                        assert_eq!(key.aggregation_type, AggregationType::DeltaSetAggregator);
                        assert_eq!(key.window_type, WindowType::Tumbling);
                        assert_eq!(key.window_size_ms, cfg.slide_interval_ms);
                        assert_eq!(key.grouping_labels, cfg.grouping_labels);
                        assert_eq!(key.aggregated_labels, cfg.aggregated_labels);
                        assert_eq!(
                            key.num_aggregates_to_retain,
                            Some(600_000 / cfg.slide_interval_ms)
                        );
                    }
                    (None, false) => {}
                    (key, _) => panic!("{:?} has key config {key:?}", cfg.aggregation_type),
                }
            }
        }
    }

    #[test]
    fn topk_by_gets_one_heap_per_bucket() {
        let mut aqe = make_aqe(Statistic::Topk, 60_000, 60_000);
        aqe.requirements.grouping_labels = labels(&["endpoint", "svc"]);
        aqe.requirements.topk_by_labels = Some(labels(&["svc"]));
        let candidates = enumerate_candidates_with_facts(&aqe, 15_000, &facts(100, Some(3)));

        let heap = candidates
            .iter()
            .find(|c| c.config.is_some())
            .expect("a CMS-with-heap candidate");
        let cfg = heap.config.as_ref().unwrap();
        assert_eq!(cfg.grouping_labels, labels(&["svc"]));
        assert_eq!(cfg.aggregated_labels, labels(&["endpoint"]));
        assert_eq!(heap.instance_count, 3);
        assert_eq!(heap.output_group_count, 100);
    }

    #[test]
    fn spatial_only_produces_direct_candidates() {
        // Spatial-only: range = scrape_interval (set by extract_aqes). One Direct window.
        let aqe = make_aqe(Statistic::Sum, 15_000, 60_000);
        let candidates = enumerate_candidates(&aqe, 15_000);
        for c in candidates.iter().filter(|c| c.config.is_some()) {
            assert_eq!(c.query_method, QueryMethod::Direct);
        }
    }

    #[test]
    fn tumbling_w_equals_range_produces_neither() {
        // range_a = 60_000ms, scrape = 60_000ms → only W=60_000, n=1 → Direct
        let aqe = make_aqe(Statistic::Sum, 60_000, 60_000);
        let candidates = enumerate_candidates(&aqe, 60_000);
        for c in candidates.iter().filter(|c| c.config.is_some()) {
            assert_eq!(c.query_method, QueryMethod::Direct);
        }
    }

    #[test]
    fn mergeable_sketch_with_multiple_windows_produces_merge() {
        // Min → MinMax (mergeable, not subtractable): range_a=300_000ms, scrape=60_000ms, min_t=300_000ms
        // → W=60_000 → n=5, Merge{5}. (Sum would prefer Subtract since it's also subtractable.)
        let aqe = make_aqe(Statistic::Min, 300_000, 300_000);
        let candidates = enumerate_candidates(&aqe, 60_000);
        let merge_candidates: Vec<_> = candidates
            .iter()
            .filter(|c| matches!(c.query_method, QueryMethod::Merge { .. }))
            .collect();
        assert!(
            !merge_candidates.is_empty(),
            "expected at least one Merge candidate"
        );
    }

    #[test]
    fn cms_with_heap_only_neither_no_merge() {
        // CMS+Heap is neither mergeable nor subtractable → only n=1 (Direct) valid.
        let aqe = make_aqe(Statistic::Topk, 300_000, 300_000);
        let candidates = enumerate_candidates(&aqe, 60_000);
        for c in candidates.iter().filter(|c| c.config.is_some()) {
            assert_eq!(
                c.query_method,
                QueryMethod::Direct,
                "CMS+Heap should only produce Direct candidates"
            );
        }
    }

    #[test]
    fn partial_width_sliding_candidates_are_generated() {
        // range_a=600_000ms, min_t=30_000ms, scrape=30_000ms.
        // W=300_000 (k=2) with S=30_000 should be emitted alongside the full-width W=600_000.
        let aqe = make_aqe(Statistic::Min, 600_000, 30_000);
        let candidates = enumerate_candidates(&aqe, 30_000);
        let partial = candidates.iter().find(|c| {
            c.config.as_ref().is_some_and(|cfg| {
                cfg.window_type == WindowType::Sliding
                    && cfg.window_size_ms == 300_000
                    && c.n_windows == 2
            })
        });
        assert!(
            partial.is_some(),
            "expected a partial-width Sliding candidate with W=300_000ms, k=2"
        );
        assert!(
            matches!(
                partial.unwrap().query_method,
                QueryMethod::Merge { num_windows: 2 }
            ),
            "partial Sliding with a mergeable-only sketch should produce Merge{{2}}"
        );
    }

    #[test]
    fn sliding_full_width_direct_generated_when_range_exceeds_t_repeat() {
        // range_a=600_000ms > T=30_000ms. W=range_a is valid for sliding since
        // freshness is governed by S (not W). S=30_000 | gcd(600_000, 30_000)=30_000 → emitted.
        let aqe = make_aqe(Statistic::Sum, 600_000, 30_000);
        let candidates = enumerate_candidates(&aqe, 30_000);
        assert!(
            candidates.iter().any(|c| {
                c.config.as_ref().is_some_and(|cfg| {
                    cfg.window_type == WindowType::Sliding && cfg.window_size_ms == 600_000
                }) && c.query_method == QueryMethod::Direct
                    && c.n_windows == 1
            }),
            "full-width Sliding Direct should be generated even when range_a > T"
        );
    }

    #[test]
    fn sliding_slide_must_divide_gcd_of_window_and_t_repeat() {
        // range_a=20_000, T=5_000, scrape=1_000.
        // W=10_000 (k=2): slide_divisor = gcd(10_000, 5_000) = 5_000.
        // Valid S: divisors of 5_000 that are multiples of 1_000 and < 10_000 → {1_000, 5_000}.
        // Invalid: S=2_000 (5_000 % 2_000 ≠ 0), S=4_000 (5_000 % 4_000 ≠ 0).
        let aqe = make_aqe(Statistic::Sum, 20_000, 5_000);
        let candidates = enumerate_candidates(&aqe, 1_000);

        let sliding_w10: Vec<_> = candidates
            .iter()
            .filter(|c| {
                c.config.as_ref().is_some_and(|cfg| {
                    cfg.window_type == WindowType::Sliding && cfg.window_size_ms == 10_000
                })
            })
            .collect();

        let slides: Vec<u64> = sliding_w10
            .iter()
            .map(|c| c.config.as_ref().unwrap().slide_interval_ms)
            .collect();

        assert!(
            slides.contains(&1_000),
            "S=1_000 should be valid (divides 5_000)"
        );
        assert!(
            slides.contains(&5_000),
            "S=5_000 should be valid (divides 5_000)"
        );
        assert!(
            !slides.contains(&2_000),
            "S=2_000 should be rejected (5_000 % 2_000 ≠ 0)"
        );
        assert!(
            !slides.contains(&4_000),
            "S=4_000 should be rejected (5_000 % 4_000 ≠ 0)"
        );
    }

    #[test]
    fn sliding_slide_must_divide_window_size() {
        // W=6_000, T=6_000: slide_divisor = gcd(6_000, 6_000) = 6_000.
        // S=4_000: 6_000 % 4_000 = 2_000 ≠ 0 → rejected even though 4_000 < 6_000.
        // S=2_000: 6_000 % 2_000 = 0 → valid.
        let aqe = make_aqe(Statistic::Sum, 12_000, 6_000);
        let candidates = enumerate_candidates(&aqe, 1_000);

        let slides_w6: Vec<u64> = candidates
            .iter()
            .filter(|c| {
                c.config.as_ref().is_some_and(|cfg| {
                    cfg.window_type == WindowType::Sliding && cfg.window_size_ms == 6_000
                })
            })
            .map(|c| c.config.as_ref().unwrap().slide_interval_ms)
            .collect();

        assert!(
            slides_w6.contains(&2_000),
            "S=2_000 should be valid (6_000 % 2_000 = 0)"
        );
        assert!(
            !slides_w6.contains(&4_000),
            "S=4_000 should be rejected (6_000 % 4_000 ≠ 0)"
        );
    }

    #[test]
    fn partial_sliding_subtractable_sketch_gets_subtract() {
        // Sum → CMS (subtractable): partial Sliding with k=2 should produce Subtract.
        let aqe = make_aqe(Statistic::Sum, 600_000, 30_000);
        let candidates = enumerate_candidates(&aqe, 30_000);
        assert!(
            candidates.iter().any(|c| {
                c.config
                    .as_ref()
                    .is_some_and(|cfg| cfg.window_type == WindowType::Sliding && c.n_windows == 2)
                    && c.query_method == QueryMethod::Subtract
            }),
            "partial Sliding with subtractable sketch should produce Subtract"
        );
    }

    /// Issue #588: DeltaSetAggregator only tracks added/removed keys since
    /// the last window, so it's only correct for non-overlapping (tumbling)
    /// windows. `Statistic::Cardinality` is compatible with DeltaSetAggregator
    /// (see `compatible_agg_types`), and with the same range/t_repeat/scrape
    /// parameters as `partial_width_sliding_candidates_are_generated` above,
    /// the window-candidate grid does include Sliding entries -- the
    /// optimizer must never turn one of those into a DeltaSetAggregator
    /// candidate.
    #[test]
    fn delta_set_aggregator_never_gets_a_sliding_candidate() {
        let aqe = make_aqe(Statistic::Cardinality, 600_000, 30_000);
        let candidates = enumerate_candidates(&aqe, 30_000);

        let sliding_delta = candidates.iter().find(|c| {
            c.config.as_ref().is_some_and(|cfg| {
                cfg.aggregation_type == AggregationType::DeltaSetAggregator
                    && cfg.window_type == WindowType::Sliding
            })
        });

        assert!(
            sliding_delta.is_none(),
            "DeltaSetAggregator must never be enumerated as a Sliding candidate, found: {sliding_delta:?}"
        );
    }

    /// Regression guard: DeltaSetAggregator must still be enumerated as a
    /// Tumbling candidate (the fix filters Sliding, not the agg type itself).
    #[test]
    fn delta_set_aggregator_still_gets_tumbling_candidates() {
        let aqe = make_aqe(Statistic::Cardinality, 600_000, 30_000);
        let candidates = enumerate_candidates(&aqe, 30_000);

        assert!(
            candidates.iter().any(|c| {
                c.config.as_ref().is_some_and(|cfg| {
                    cfg.aggregation_type == AggregationType::DeltaSetAggregator
                        && cfg.window_type == WindowType::Tumbling
                })
            }),
            "DeltaSetAggregator should still get Tumbling candidates"
        );
    }

    /// Regression guard: sibling Cardinality-compatible agg types that ARE
    /// safe under Sliding windows (SetAggregator, HLL) must be unaffected by
    /// the DeltaSetAggregator-specific filter.
    #[test]
    fn set_aggregator_and_hll_still_get_sliding_candidates() {
        let aqe = make_aqe(Statistic::Cardinality, 600_000, 30_000);
        let candidates = enumerate_candidates(&aqe, 30_000);

        for agg_type in [AggregationType::SetAggregator, AggregationType::HLL] {
            assert!(
                candidates.iter().any(|c| {
                    c.config.as_ref().is_some_and(|cfg| {
                        cfg.aggregation_type == agg_type && cfg.window_type == WindowType::Sliding
                    })
                }),
                "{agg_type:?} should still be able to get Sliding candidates"
            );
        }
    }
}
