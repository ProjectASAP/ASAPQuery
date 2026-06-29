use std::collections::HashMap;

use asap_types::aggregation_config::AggregationConfig;
use asap_types::capability_matching::compatible_agg_types;
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
}

/// Enumerate all candidate configs for an AQE.
///
/// Iterates over compatible agg types × parameter grid × valid window sizes ×
/// {Tumbling, Sliding}. Always appends an EXACT candidate last (always feasible).
///
/// Multi-statistic AQEs (e.g. avg = [Sum, Count]) return only EXACT — a single
/// sketch family cannot serve two incompatible statistics simultaneously.
pub fn enumerate_candidates(aqe: &AQE, scrape_interval_ms: u64) -> Vec<CandidateConfig> {
    let mut candidates = Vec::new();

    if aqe.requirements.statistics.len() != 1 {
        // ponytail: multi-stat AQEs (avg) need two sketches; not supported in v1.
        candidates.push(exact_candidate());
        return candidates;
    }

    let stat = aqe.requirements.statistics[0];
    let range_a_ms = aqe.requirements.data_range_ms;

    for &agg_type in compatible_agg_types(stat) {
        let props = sketch_properties(agg_type);
        let sub_type = derive_sub_type(stat, agg_type);

        for params in param_grid(agg_type, aqe.requirements.topk_count_events) {
            for (window_type, w, slide_interval, n) in
                window_candidates(range_a_ms, aqe.min_t_repeat_ms, scrape_interval_ms)
            {
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
                });
            }
        }
    }

    candidates.push(exact_candidate());
    candidates
}

fn exact_candidate() -> CandidateConfig {
    CandidateConfig {
        config: None,
        query_method: QueryMethod::Exact,
        n_windows: 0,
    }
}

/// Window candidates: (WindowType, W_ms, slide_interval_ms, n_windows).
///
/// Tumbling: all W that divide range_a, are multiples of scrape_interval, and ≤ min_t_repeat.
///           Slide interval = W (a tumbling window "slides" by its own width).
/// Sliding:  W = range_a / k for each W that is a multiple of scrape_interval and divides
///           range_a (k = range_a / W). At query time k staggered readings are merged or
///           subtracted to cover range_a. Freshness is bounded by S (the slide interval),
///           not by W — so the guard is S ≤ min_t_repeat_ms, not W ≤ min_t_repeat_ms.
///           S doubles from scrape_interval up to min(W, min_t_repeat_ms).
fn window_candidates(
    range_a_ms: u64,
    min_t_repeat_ms: u64,
    scrape_interval_ms: u64,
) -> Vec<(WindowType, u64, u64, u64)> {
    let range_a = range_a_ms;
    if range_a == 0 || scrape_interval_ms == 0 {
        return vec![];
    }

    let max_w = range_a.min(min_t_repeat_ms);
    let mut out = Vec::new();

    // Tumbling: W divides range_a, is a multiple of scrape_interval, and ≤ max_w.
    let mut w = scrape_interval_ms;
    while w <= max_w {
        if range_a.is_multiple_of(w) {
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
            let mut s = scrape_interval_ms;
            while s <= w.min(min_t_repeat_ms) {
                out.push((WindowType::Sliding, w, s, k));
                s *= 2;
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

/// Build an AggregationConfig from candidate parameters. aggregation_id = 0 (placeholder).
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
        0, // placeholder; replaced by greedy/MIP solver when deploying
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
        (Statistic::Sum, AggregationType::CountMinSketch) => "sum",
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
    fn sliding_freshness_guard_uses_slide_not_width() {
        // range_a=600_000ms > min_t_repeat=30_000ms. The old guard (range_a <= min_t_repeat)
        // incorrectly rejected all Sliding candidates here. New guard: S ≤ min_t_repeat.
        // W=600_000, S=30_000 ≤ min_t_repeat → full-width Direct Sliding should be present.
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
}
