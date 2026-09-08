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
use super::sketch_properties::sketch_properties;
use super::solution::{QueryMethod, AQE};

/// A candidate streaming config for a single AQE, ready for cost evaluation.
#[derive(Debug, Clone)]
pub struct CandidateConfig {
    /// None = EXACT fallback (no streaming config; raw Prometheus query at query time).
    pub config: Option<AggregationConfig>,
    /// Query method derived from (ingest type × W vs range_a × sketch algebra).
    pub query_method: QueryMethod,
    /// Number of retained windows used at query time (n for Merge, 1 for Direct/Subtract, 0 for Exact).
    pub n_windows: u64,
    /// Number of distinct label groups represented by this candidate.
    /// Subpopulation-aware sketches ignore this value during cost evaluation.
    pub label_group_count: u64,
}

/// Enumerate all candidate configs for an AQE.
///
/// Iterates over compatible agg types × parameter grid × valid window sizes ×
/// {Tumbling, Sliding}. Always appends an EXACT candidate last (always feasible).
///
/// Multi-statistic AQEs (e.g. avg = [Sum, Count]) return only EXACT — a single
/// sketch family cannot serve two incompatible statistics simultaneously.
pub fn enumerate_candidates(aqe: &AQE, scrape_interval_ms: u64) -> Vec<CandidateConfig> {
    enumerate_candidates_with_label_group_count(aqe, scrape_interval_ms, 1)
}

/// Enumerate candidates with a dataset-derived label-group count.
pub fn enumerate_candidates_with_label_group_count(
    aqe: &AQE,
    scrape_interval_ms: u64,
    label_group_count: u64,
) -> Vec<CandidateConfig> {
    assert!(
        label_group_count > 0,
        "label_group_count must be greater than zero"
    );
    let mut candidates = Vec::new();

    if aqe.requirements.statistics.len() != 1 {
        // ponytail: multi-stat AQEs (avg) need two sketches; not supported in v1.
        candidates.push(exact_candidate(label_group_count));
        return candidates;
    }

    let stat = aqe.requirements.statistics[0];
    let range_a_ms = aqe.requirements.data_range_ms;

    for &agg_type in compatible_agg_types(stat) {
        let props = sketch_properties(agg_type);
        let sub_type = derive_sub_type(stat, agg_type);

        for params in param_grid(agg_type, aqe.requirements.topk_count_events) {
            for (window_type, w, slide_interval, n) in
                window_candidates(range_a_ms, aqe.t_repeat_gcd_ms, scrape_interval_ms)
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
                    aqe,
                    agg_type,
                    &sub_type,
                    &params,
                    window_type,
                    w,
                    slide_interval,
                    n,
                );
                candidates.push(CandidateConfig {
                    config: Some(config),
                    query_method: qm,
                    n_windows: n,
                    label_group_count,
                });
            }
        }
    }

    candidates.push(exact_candidate(label_group_count));
    candidates
}

fn exact_candidate(label_group_count: u64) -> CandidateConfig {
    CandidateConfig {
        config: None,
        query_method: QueryMethod::Exact,
        n_windows: 0,
        label_group_count,
    }
}

/// Window candidates: (WindowType, W_ms, slide_interval_ms, n_windows).
///
/// Tumbling: W must divide GCD(range_a, t_repeat_gcd_ms) and be a multiple of scrape_interval.
///           Dividing the GCD ensures (a) n complete windows cover range_a exactly, and
///           (b) window completions are harmonically aligned with every dashboard refresh cycle.
///           Slide interval = W (a tumbling window "slides" by its own width).
/// Sliding:  W = range_a / k for each W that is a multiple of scrape_interval and divides
///           range_a (k = range_a / W). At query time k staggered readings spaced W apart
///           are merged or subtracted to cover range_a.
///           S must satisfy three constraints:
///             (a) S | W   — so W-spaced snapshots land on slide boundaries (multi-window correctness)
///             (b) S | t_repeat_gcd — so slide boundaries align with every dashboard refresh cycle
///             (c) S < W   — S=W is excluded because slide==window is tumbling (duplicate candidate)
///           S is enumerated as multiples of scrape_interval < W; the divisibility check on
///           gcd(W, t_repeat_gcd) rejects values that fail (a) or (b) without a separate bound.
fn window_candidates(
    range_a_ms: u64,
    t_repeat_gcd_ms: u64,
    scrape_interval_ms: u64,
) -> Vec<(WindowType, u64, u64, u64)> {
    let range_a = range_a_ms;
    if range_a == 0 || scrape_interval_ms == 0 {
        return vec![];
    }

    let mut out = Vec::new();

    // Tumbling: W divides GCD(range_a, t_repeat_gcd) and is a multiple of scrape_interval.
    // W | t_repeat_gcd ensures window completions align harmonically with all dashboards.
    // W | range_a (implied since t_repeat_gcd | range_a is checked at generation time, but
    // we verify explicitly via the gcd) ensures n windows cover range_a exactly.
    let tumbling_divisor = super::aqe_extractor::gcd(range_a, t_repeat_gcd_ms);
    let mut w = scrape_interval_ms;
    while w <= tumbling_divisor {
        if tumbling_divisor.is_multiple_of(w) {
            let n = range_a / w;
            out.push((WindowType::Tumbling, w, w, n));
        }
        w += scrape_interval_ms;
    }

    // Sliding: W = range_a / k for each valid W (multiple of scrape_interval, divides range_a).
    // S doubles from scrape_interval up to min(W, min_t_repeat_ms). n_windows = k.
    let mut w = scrape_interval_ms;
    while w <= range_a {
        if range_a.is_multiple_of(w) {
            let k = range_a / w;
            // Valid S: S | gcd(W, t_repeat_gcd). Iterate up to W (exclusive); the
            // divisibility check rejects anything above gcd automatically.
            let slide_divisor = super::aqe_extractor::gcd(w, t_repeat_gcd_ms);
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
    aqe: &AQE,
    agg_type: AggregationType,
    sub_type: &str,
    params: &HashMap<String, Value>,
    window_type: WindowType,
    w: u64,
    slide_interval: u64,
    n_windows: u64,
) -> AggregationConfig {
    AggregationConfig::new(
        0, // placeholder; overwritten by OptimizerSolution::register_config when deployed
        agg_type,
        sub_type.to_string(),
        params.clone(),
        aqe.requirements.grouping_labels.clone(),
        KeyByLabelNames::empty(), // aggregated_labels (not needed for optimizer feasibility)
        KeyByLabelNames::empty(), // rollup_labels
        String::new(),            // original_yaml
        w,
        slide_interval,
        window_type,
        aqe.requirements.spatial_filter_normalized.clone(),
        aqe.requirements.metric.clone(),
        Some(n_windows),
        None, // read_count_threshold
        None, // table_name (SQL only)
        None, // value_column (SQL only)
    )
}

/// aggregation_sub_type string expected by the streaming engine and capability matching.
fn derive_sub_type(stat: Statistic, agg_type: AggregationType) -> String {
    match (stat, agg_type) {
        (Statistic::Min, _) => "min",
        (Statistic::Max, _) => "max",
        (Statistic::Topk, _) => "topk",
        (Statistic::Sum, AggregationType::CountMinSketch | AggregationType::MultipleSum) => "sum",
        (Statistic::Count, AggregationType::CountMinSketch) => "count",
        _ => "",
    }
    .to_string()
}

fn param_grid(
    agg_type: AggregationType,
    topk_count_events: Option<bool>,
) -> Vec<HashMap<String, Value>> {
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
            let count_events_variants: &[bool] = match topk_count_events {
                Some(v) => {
                    if v {
                        &[true]
                    } else {
                        &[false]
                    }
                }
                None => &[true, false],
            };
            let mut grids = Vec::new();
            for &d in CMS_DEPTHS {
                for &w in CMS_WIDTHS {
                    for &h in CMS_HEAP_SIZES {
                        for &ce in count_events_variants {
                            let mut m = HashMap::new();
                            m.insert("depth".into(), Value::from(d));
                            m.insert("width".into(), Value::from(w));
                            m.insert("heapsize".into(), Value::from(h));
                            m.insert("count_events".into(), Value::from(ce));
                            grids.push(m);
                        }
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

    fn make_aqe(stat: Statistic, range_ms: u64, min_t: u64) -> AQE {
        use asap_types::query_requirements::QueryRequirements;
        AQE {
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
            min_t_repeat_ms: min_t,
            t_repeat_gcd_ms: min_t,
        }
    }

    #[test]
    fn always_includes_exact_fallback() {
        let aqe = make_aqe(Statistic::Sum, 300_000, 60_000);
        let candidates = enumerate_candidates(&aqe, 15_000);
        assert!(candidates
            .iter()
            .any(|c| c.config.is_none() && c.query_method == QueryMethod::Exact));
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

    #[test]
    fn stamps_dataset_label_group_count_on_every_candidate() {
        let aqe = make_aqe(Statistic::Sum, 300_000, 60_000);
        let candidates = enumerate_candidates_with_label_group_count(&aqe, 15_000, 7);

        assert!(!candidates.is_empty());
        assert!(candidates
            .iter()
            .all(|candidate| candidate.label_group_count == 7));
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
        // range_a=600_000ms > t_repeat_gcd=30_000ms. W=range_a is valid for sliding since
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
            "full-width Sliding Direct should be generated even when range_a > min_t_repeat"
        );
    }

    #[test]
    fn sliding_slide_must_divide_gcd_of_window_and_t_repeat() {
        // range_a=20_000, t_repeat_gcd=5_000, scrape=1_000.
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
        // W=6_000, t_repeat_gcd=6_000: slide_divisor = gcd(6_000, 6_000) = 6_000.
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
