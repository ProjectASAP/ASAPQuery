use crate::config::input::SketchParameterOverrides;
use promql_utilities::ast_matching::PromQLMatchResult;
use promql_utilities::query_logics::enums::AggregationType;
use std::collections::HashMap;

// Default sketch parameters
const DEFAULT_CMS_DEPTH: u64 = 3;
const DEFAULT_CMS_WIDTH: u64 = 1024;
const DEFAULT_CMS_HEAP_MULT: u64 = 4;
const DEFAULT_KLL_K: u64 = 500;
const DEFAULT_HYDRA_ROW: u64 = 3;
const DEFAULT_HYDRA_COL: u64 = 1024;
const DEFAULT_HYDRA_K: u64 = 20;
const DEFAULT_HLL_PRECISION: u64 = 14;

/// Shared sketch parameter builder used by both PromQL and SQL paths.
///
/// `topk_k` is required for `CountMinSketchWithHeap`. PromQL supplies it from
/// the `topk(k, …)` query argument; SQL supplies it from `LIMIT k`.
///
/// `topk_count_events` disambiguates event-count vs value-weighted top-k.
pub fn build_sketch_parameters(
    aggregation_type: AggregationType,
    aggregation_sub_type: &str,
    topk_k: Option<u64>,
    topk_count_events: Option<bool>,
    sketch_params: Option<&SketchParameterOverrides>,
) -> Result<HashMap<String, serde_json::Value>, String> {
    match aggregation_type {
        AggregationType::Increase
        | AggregationType::MinMax
        | AggregationType::Sum
        | AggregationType::MultipleIncrease
        | AggregationType::MultipleMinMax
        | AggregationType::MultipleSum
        | AggregationType::DeltaSetAggregator
        | AggregationType::SetAggregator => Ok(HashMap::new()),

        AggregationType::CountMinSketch => {
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

        AggregationType::CountMinSketchWithHeap => {
            if aggregation_sub_type != "topk" {
                return Err(format!(
                    "Aggregation sub-type {} for CountMinSketchWithHeap not supported",
                    aggregation_sub_type
                ));
            }
            let k = topk_k
                .ok_or_else(|| "CountMinSketchWithHeap requires a topk k value".to_string())?;
            let count_events = topk_count_events.ok_or_else(|| {
                "CountMinSketchWithHeap requires explicit count_events weighting".to_string()
            })?;
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
            m.insert(
                "count_events".to_string(),
                serde_json::Value::Bool(count_events),
            );
            Ok(m)
        }

        AggregationType::DatasketchesKLL => {
            let k = sketch_params
                .and_then(|p| p.datasketches_kll.as_ref())
                .map(|p| p.k)
                .unwrap_or(DEFAULT_KLL_K);
            let mut m = HashMap::new();
            m.insert("K".to_string(), serde_json::Value::Number(k.into()));
            Ok(m)
        }

        AggregationType::HLL => {
            let precision = sketch_params
                .and_then(|p| p.hll.as_ref())
                .map(|p| p.precision)
                .unwrap_or(DEFAULT_HLL_PRECISION);
            let mut m = HashMap::new();
            m.insert(
                "precision".to_string(),
                serde_json::Value::Number(precision.into()),
            );
            Ok(m)
        }

        AggregationType::HydraKLL => {
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

/// PromQL wrapper: extracts the topk `k` from the match result when needed,
/// then delegates to `build_sketch_parameters`.
pub fn build_sketch_parameters_from_promql(
    aggregation_type: AggregationType,
    aggregation_sub_type: &str,
    match_result: &PromQLMatchResult,
    sketch_params: Option<&SketchParameterOverrides>,
) -> Result<HashMap<String, serde_json::Value>, String> {
    let topk_k = if aggregation_type == AggregationType::CountMinSketchWithHeap {
        let k: u64 = match_result
            .tokens
            .get("aggregation")
            .and_then(|t| t.aggregation.as_ref())
            .and_then(|a| a.param.as_ref())
            .and_then(|p| p.parse::<f64>().ok())
            .map(|f| f as u64)
            .ok_or_else(|| "topk query missing required 'k' parameter".to_string())?;
        Some(k)
    } else {
        None
    };
    build_sketch_parameters(
        aggregation_type,
        aggregation_sub_type,
        topk_k,
        Some(false),
        sketch_params,
    )
}
