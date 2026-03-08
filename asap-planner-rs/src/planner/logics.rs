use crate::config::input::SketchParameterOverrides;
use promql_utilities::ast_matching::PromQLMatchResult;
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::{QueryPatternType, Statistic};
use promql_utilities::query_logics::logics::does_precompute_operator_support_subpopulations;
use sketch_db_common::enums::CleanupPolicy;
use std::collections::HashMap;

// Default sketch parameters
const DEFAULT_CMS_DEPTH: u64 = 3;
const DEFAULT_CMS_WIDTH: u64 = 1024;
const DEFAULT_CMS_HEAP_MULT: u64 = 4;
const DEFAULT_KLL_K: u64 = 20;
const DEFAULT_HYDRA_ROW: u64 = 3;
const DEFAULT_HYDRA_COL: u64 = 1024;
const DEFAULT_HYDRA_K: u64 = 20;

pub fn get_effective_repeat(t_repeat: u64, step: u64) -> u64 {
    if step > 0 {
        t_repeat.min(step)
    } else {
        t_repeat
    }
}

pub fn should_use_sliding_window(
    _query_pattern_type: QueryPatternType,
    _aggregation_type: &str,
) -> bool {
    // HARDCODED: sliding windows crash Arroyo
    false
}

pub fn set_window_parameters(
    query_pattern_type: QueryPatternType,
    t_repeat: u64,
    prometheus_scrape_interval: u64,
    aggregation_type: &str,
    step: u64,
    config: &mut IntermediateWindowConfig,
) {
    let effective_repeat = get_effective_repeat(t_repeat, step);
    let _use_sliding = should_use_sliding_window(query_pattern_type, aggregation_type);
    // use_sliding is always false, so always tumbling
    set_tumbling_window_parameters(
        query_pattern_type,
        effective_repeat,
        prometheus_scrape_interval,
        config,
    );
}

fn set_tumbling_window_parameters(
    query_pattern_type: QueryPatternType,
    effective_repeat: u64,
    prometheus_scrape_interval: u64,
    config: &mut IntermediateWindowConfig,
) {
    match query_pattern_type {
        QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
            config.window_size = effective_repeat;
            config.slide_interval = effective_repeat;
            config.window_type = "tumbling".to_string();
            config.tumbling_window_size = effective_repeat;
        }
        QueryPatternType::OnlySpatial => {
            config.window_size = prometheus_scrape_interval;
            config.slide_interval = prometheus_scrape_interval;
            config.window_type = "tumbling".to_string();
            config.tumbling_window_size = prometheus_scrape_interval;
        }
    }
}

/// A mutable window config holder used during planning
#[derive(Debug, Clone, Default)]
pub struct IntermediateWindowConfig {
    pub window_size: u64,
    pub slide_interval: u64,
    pub window_type: String,
    pub tumbling_window_size: u64,
}

pub fn get_precompute_operator_parameters(
    aggregation_type: &str,
    aggregation_sub_type: &str,
    match_result: &PromQLMatchResult,
    sketch_params: Option<&SketchParameterOverrides>,
) -> Result<HashMap<String, serde_json::Value>, String> {
    match aggregation_type {
        "Increase" | "MinMax" | "Sum" | "MultipleIncrease" | "MultipleMinMax" | "MultipleSum"
        | "DeltaSetAggregator" | "SetAggregator" => Ok(HashMap::new()),

        "CountMinSketch" => {
            let depth = sketch_params
                .and_then(|p| p.count_min_sketch.as_ref())
                .map(|p| p.depth)
                .unwrap_or(DEFAULT_CMS_DEPTH);
            let width = sketch_params
                .and_then(|p| p.count_min_sketch.as_ref())
                .map(|p| p.width)
                .unwrap_or(DEFAULT_CMS_WIDTH);
            let mut m = HashMap::new();
            m.insert("depth".to_string(), serde_json::Value::Number(depth.into()));
            m.insert("width".to_string(), serde_json::Value::Number(width.into()));
            Ok(m)
        }

        "CountMinSketchWithHeap" => {
            if aggregation_sub_type != "topk" {
                return Err(format!(
                    "Aggregation sub-type {} for CountMinSketchWithHeap not supported",
                    aggregation_sub_type
                ));
            }
            // Get k from aggregation param
            let k: u64 = match_result
                .tokens
                .get("aggregation")
                .and_then(|t| t.aggregation.as_ref())
                .and_then(|a| a.param.as_ref())
                .and_then(|p| p.parse::<f64>().ok())
                .map(|f| f as u64)
                .ok_or_else(|| "topk query missing required 'k' parameter".to_string())?;

            let depth = sketch_params
                .and_then(|p| p.count_min_sketch_with_heap.as_ref())
                .map(|p| p.depth)
                .unwrap_or(DEFAULT_CMS_DEPTH);
            let width = sketch_params
                .and_then(|p| p.count_min_sketch_with_heap.as_ref())
                .map(|p| p.width)
                .unwrap_or(DEFAULT_CMS_WIDTH);
            let heap_mult = sketch_params
                .and_then(|p| p.count_min_sketch_with_heap.as_ref())
                .and_then(|p| p.heap_multiplier)
                .unwrap_or(DEFAULT_CMS_HEAP_MULT);

            let mut m = HashMap::new();
            m.insert("depth".to_string(), serde_json::Value::Number(depth.into()));
            m.insert("width".to_string(), serde_json::Value::Number(width.into()));
            m.insert(
                "heapsize".to_string(),
                serde_json::Value::Number((k * heap_mult).into()),
            );
            Ok(m)
        }

        "DatasketchesKLL" => {
            let k = sketch_params
                .and_then(|p| p.datasketches_kll.as_ref())
                .map(|p| p.k)
                .unwrap_or(DEFAULT_KLL_K);
            let mut m = HashMap::new();
            m.insert("K".to_string(), serde_json::Value::Number(k.into()));
            Ok(m)
        }

        "HydraKLL" => {
            let row_num = sketch_params
                .and_then(|p| p.hydra_kll.as_ref())
                .map(|p| p.row_num)
                .unwrap_or(DEFAULT_HYDRA_ROW);
            let col_num = sketch_params
                .and_then(|p| p.hydra_kll.as_ref())
                .map(|p| p.col_num)
                .unwrap_or(DEFAULT_HYDRA_COL);
            let k = sketch_params
                .and_then(|p| p.hydra_kll.as_ref())
                .map(|p| p.k)
                .unwrap_or(DEFAULT_HYDRA_K);
            let mut m = HashMap::new();
            m.insert(
                "row_num".to_string(),
                serde_json::Value::Number(row_num.into()),
            );
            m.insert(
                "col_num".to_string(),
                serde_json::Value::Number(col_num.into()),
            );
            m.insert("k".to_string(), serde_json::Value::Number(k.into()));
            Ok(m)
        }

        other => Err(format!("Aggregation type {} not supported", other)),
    }
}

pub fn get_cleanup_param(
    cleanup_policy: CleanupPolicy,
    query_pattern_type: QueryPatternType,
    match_result: &PromQLMatchResult,
    t_repeat: u64,
    window_type: &str,
    range_duration: u64,
    step: u64,
) -> Result<u64, String> {
    // Validation
    if (range_duration == 0) != (step == 0) {
        return Err(format!(
            "range_duration and step must both be 0 or both > 0. Got range_duration={}, step={}",
            range_duration, step
        ));
    }

    let is_range_query = step > 0;

    let t_lookback: u64 = if query_pattern_type == QueryPatternType::OnlySpatial {
        t_repeat
    } else {
        match_result
            .get_range_duration()
            .map(|d| d.num_seconds() as u64)
            .ok_or_else(|| "No range_vector token found".to_string())?
    };

    if window_type == "sliding" {
        let result = if is_range_query {
            range_duration / step + 1
        } else {
            1
        };
        return Ok(result);
    }

    // Tumbling
    let effective_repeat = get_effective_repeat(t_repeat, step);

    let result = match cleanup_policy {
        CleanupPolicy::CircularBuffer => {
            // ceil((t_lookback + range_duration) / effective_repeat)
            let numerator = t_lookback + range_duration;
            numerator.div_ceil(effective_repeat)
        }
        CleanupPolicy::ReadBased => {
            // ceil(t_lookback / effective_repeat) * (range_duration / step + 1)
            let lookback_buckets = t_lookback.div_ceil(effective_repeat);
            let num_steps = if is_range_query {
                range_duration / step + 1
            } else {
                1
            };
            lookback_buckets * num_steps
        }
        CleanupPolicy::NoCleanup => {
            return Err("NoCleanup policy should not call get_cleanup_param".to_string());
        }
    };

    Ok(result)
}

pub fn set_subpopulation_labels(
    statistic: Statistic,
    aggregation_type: &str,
    subpopulation_labels: &KeyByLabelNames,
    rollup_labels: &mut KeyByLabelNames,
    grouping_labels: &mut KeyByLabelNames,
    aggregated_labels: &mut KeyByLabelNames,
) {
    // rollup is set by caller before calling this function
    let _ = rollup_labels; // not modified here
    if does_precompute_operator_support_subpopulations(statistic, aggregation_type) {
        *grouping_labels = KeyByLabelNames::empty();
        *aggregated_labels = subpopulation_labels.clone();
    } else {
        *grouping_labels = subpopulation_labels.clone();
        *aggregated_labels = KeyByLabelNames::empty();
    }
}
