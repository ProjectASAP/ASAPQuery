//! The atomic-cost table exported by sketch-bench (sketch-bench#30,
//! `scripts/export_atomic_costs.sh`), and the (sketch_type, params) lookup
//! that resolves a candidate's [`AtomicCosts`] from it.
//!
//! `AtomicCostEntry`/`AtomicCostTable` are a deliberate duplicate of
//! sketch-bench's `aqpbm_core::atomic_costs` types, not a shared dependency —
//! see ASAPQuery#524 and sketch-bench#30 for why. Keep the two in sync by
//! hand; `atomic_cost_entry_deserializes_sketch_benchs_documented_shape`
//! below is a canary for drift.

use std::collections::HashMap;
use std::path::Path;

use promql_utilities::query_logics::enums::AggregationType;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::constants::{
    CMS_HEAP_AVERAGE_KEY_BYTES, CMS_HEAP_COUNTER_BYTES, CMS_HEAP_ENTRY_OVERHEAD_BYTES,
    CMS_HEAP_REFERENCE_HEAP_SIZE, EXACT_QUERY_CPU_SECS, SUBTRACT_CPU_SECS,
};
use super::cost_model::AtomicCosts;

const CMS_HEAP_BENCHMARK: &str = "cms-heap-topk-regularpath-vector2d";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicCostEntry {
    pub sketch: String,
    pub sketch_config: Value,
    pub mem_bytes_per_instance: f64,
    pub insert_cpu_secs: f64,
    pub merge_cpu_secs: f64,
    pub query_cpu_secs: f64,
}

pub type AtomicCostTable = Vec<AtomicCostEntry>;

/// Read an `AtomicCostTable` exported by `sketch-bench atomic-costs`.
pub fn load_atomic_cost_table(path: &Path) -> anyhow::Result<AtomicCostTable> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("reading atomic-cost table {}: {e}", path.display()))?;
    serde_json::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("parsing atomic-cost table {}: {e}", path.display()))
}

/// sketch-bench's (algorithm, params) key for one of ASAPQuery's benchmarked
/// families, or `None` if `agg_type` isn't one sketch-bench measures at all
/// (trivial O(1) accumulators — Sum/Increase/MinMax/... — and sketch types
/// sketch-bench has no wrapper for yet — HydraKLL).
///
/// Field names differ from ASAPQuery's own `parameters` map by design: each
/// side picked its own config vocabulary independently, so this is a real
/// translation, not a passthrough. The field names read here (`"depth"`,
/// `"width"`, `"precision"`, `"K"`) are duplicated from `candidate_gen.rs`'s
/// `param_grid()` — not derived from it — so a rename on either side without
/// the other silently breaks this lookup. Guarded by panicking below rather
/// than treating a missing key the same as "not a benchmarked family": a
/// `CountMinSketch`/`HLL`/`DatasketchesKLL` candidate is only ever built by
/// `param_grid()`, which always sets these keys, so their absence means the
/// two have drifted, not that there's no data for this family.
fn sketch_bench_key(
    agg_type: AggregationType,
    params: &HashMap<String, Value>,
) -> Option<(&'static str, Value)> {
    match agg_type {
        AggregationType::CountMinSketch => Some((
            "cms-fastpath-vector2d",
            serde_json::json!({
                "rows": require(params, "depth", agg_type),
                "cols": require(params, "width", agg_type),
            }),
        )),
        AggregationType::HLL => Some((
            "hll",
            serde_json::json!({ "lg_k": require(params, "precision", agg_type) }),
        )),
        AggregationType::DatasketchesKLL => Some((
            "kll-percall",
            serde_json::json!({ "k": require(params, "K", agg_type) }),
        )),
        _ => None,
    }
}

/// Resolve the [`AtomicCosts`] a candidate should be costed at.
///
/// - `agg_type` outside the benchmarked families (see [`sketch_bench_key`]):
///   `Some(AtomicCosts::default())` — the flat stub, unchanged from before
///   this table existed. Logged, since it's silently wrong for anything
///   sketch-bench could plausibly measure later.
///   TODO(#524): remove this fallback once every family the optimizer can
///   select has a real sketch-bench entry; costing should end up 100%
///   empirical, with nothing left reading `AtomicCosts::default()`.
/// - Benchmarked family, matching table row found: `Some(costs)` built from
///   it (`subtract_cpu_secs`/`exact_query_cpu_secs` still come from the
///   stub — the table has neither: subtract isn't implemented upstream yet,
///   and EXACT isn't a sketch sketch-bench could measure).
/// - `CountMinSketchWithHeap`: resolve through the temporary fixed-top-k
///   reference model in [`resolve_cms_heap_costs`]. Missing or malformed
///   reference data returns `None` and drops the candidate.
/// - Other benchmarked families, no matching row: `None` — drop the candidate,
///   per #524.
pub fn resolve_atomic_costs(
    table: &AtomicCostTable,
    agg_type: AggregationType,
    params: &HashMap<String, Value>,
) -> Option<AtomicCosts> {
    if agg_type == AggregationType::CountMinSketchWithHeap {
        return resolve_cms_heap_costs(table, params, &CmsHeapCostAssumptions::default());
    }

    let Some((sketch, sketch_params)) = sketch_bench_key(agg_type, params) else {
        tracing::warn!(
            ?agg_type,
            "no sketch-bench atomic-cost data for this family; using the flat AtomicCosts stub"
        );
        return Some(AtomicCosts::default());
    };

    let expected_config = serde_json::json!({ "algorithm": sketch, "params": sketch_params });
    table
        .iter()
        .find(|e| e.sketch == sketch && e.sketch_config == expected_config)
        .map(|entry| AtomicCosts {
            mem_bytes_per_instance: entry.mem_bytes_per_instance,
            insert_cpu_secs: entry.insert_cpu_secs,
            merge_cpu_secs: entry.merge_cpu_secs,
            subtract_cpu_secs: SUBTRACT_CPU_SECS,
            query_cpu_secs: entry.query_cpu_secs,
            exact_query_cpu_secs: EXACT_QUERY_CPU_SECS,
        })
}

/// Temporary cost model for the runtime CMS-with-heap implementation.
///
/// sketch-bench currently measures a fixed top-k=32 wrapper, while the
/// runtime's heap size is a candidate parameter. CPU costs therefore scale
/// linearly from the matching regular-path/top-k benchmark row. Memory is
/// computed from the runtime's i64 CMS counters and an explicit estimate for
/// each heap entry; the benchmark's i32-only matrix memory is not reused.
#[derive(Debug, Clone, Copy, PartialEq)]
struct CmsHeapCostAssumptions {
    reference_heap_size: u64,
    counter_bytes: f64,
    average_key_bytes: f64,
    heap_entry_overhead_bytes: f64,
}

impl Default for CmsHeapCostAssumptions {
    fn default() -> Self {
        Self {
            reference_heap_size: CMS_HEAP_REFERENCE_HEAP_SIZE,
            counter_bytes: CMS_HEAP_COUNTER_BYTES,
            average_key_bytes: CMS_HEAP_AVERAGE_KEY_BYTES,
            heap_entry_overhead_bytes: CMS_HEAP_ENTRY_OVERHEAD_BYTES,
        }
    }
}

impl CmsHeapCostAssumptions {
    fn validate(self) {
        assert!(
            self.reference_heap_size > 0,
            "CMS-with-heap reference_heap_size must be greater than zero"
        );
        assert!(
            self.counter_bytes.is_finite() && self.counter_bytes >= 0.0,
            "CMS-with-heap counter_bytes must be finite and non-negative"
        );
        assert!(
            self.average_key_bytes.is_finite() && self.average_key_bytes >= 0.0,
            "CMS-with-heap average_key_bytes must be finite and non-negative"
        );
        assert!(
            self.heap_entry_overhead_bytes.is_finite() && self.heap_entry_overhead_bytes >= 0.0,
            "CMS-with-heap heap_entry_overhead_bytes must be finite and non-negative"
        );
    }
}

/// Resolve a CMS-with-heap candidate from the fixed-top-k benchmark reference.
///
/// The helper deliberately returns `None` when the reference row is absent or
/// malformed. The caller then drops this candidate, leaving the always-feasible
/// EXACT candidate available. TODO(#651): turn these temporary warning paths
/// into hard errors once sketch-bench sweeps cover the candidate grid.
fn resolve_cms_heap_costs(
    table: &AtomicCostTable,
    params: &HashMap<String, Value>,
    assumptions: &CmsHeapCostAssumptions,
) -> Option<AtomicCosts> {
    assumptions.validate();

    let agg_type = AggregationType::CountMinSketchWithHeap;
    let depth = require_u64(params, "depth", agg_type);
    let width = require_u64(params, "width", agg_type);
    let heap_size = require_u64(params, "heapsize", agg_type);
    let count_events = require(params, "count_events", agg_type);
    let heap_size_f64 = heap_size as f64;
    let scale = heap_size_f64 / assumptions.reference_heap_size as f64;
    let expected_config = serde_json::json!({
        "algorithm": CMS_HEAP_BENCHMARK,
        "params": { "rows": depth, "cols": width },
    });

    let Some(entry) = table
        .iter()
        .find(|entry| entry.sketch == CMS_HEAP_BENCHMARK && entry.sketch_config == expected_config)
    else {
        tracing::info!(
            status = "missing_reference",
            sketch = CMS_HEAP_BENCHMARK,
            depth,
            width,
            heap_size,
            count_events = ?count_events,
            "cms-with-heap atomic cost measurement"
        );
        tracing::warn!(
            sketch = CMS_HEAP_BENCHMARK,
            depth,
            width,
            heap_size,
            count_events = ?count_events,
            "no CMS-with-heap reference cost for candidate; dropping candidate; \
             TODO(#651): fail loudly once sketch-bench sweeps cover this grid"
        );
        return None;
    };

    if !valid_cost_entry(entry) {
        tracing::info!(
            status = "invalid_reference",
            sketch = CMS_HEAP_BENCHMARK,
            depth,
            width,
            heap_size,
            count_events = ?count_events,
            "cms-with-heap atomic cost measurement"
        );
        tracing::warn!(
            sketch = CMS_HEAP_BENCHMARK,
            depth,
            width,
            heap_size,
            count_events = ?count_events,
            "invalid CMS-with-heap reference cost for candidate; dropping candidate; \
             TODO(#651): fail loudly once sketch-bench sweeps cover this grid"
        );
        return None;
    }

    let costs = AtomicCosts {
        mem_bytes_per_instance: depth as f64 * width as f64 * assumptions.counter_bytes
            + heap_size_f64
                * (assumptions.average_key_bytes + assumptions.heap_entry_overhead_bytes),
        insert_cpu_secs: entry.insert_cpu_secs * scale,
        merge_cpu_secs: entry.merge_cpu_secs * scale,
        subtract_cpu_secs: SUBTRACT_CPU_SECS,
        query_cpu_secs: entry.query_cpu_secs * scale,
        exact_query_cpu_secs: EXACT_QUERY_CPU_SECS,
    };

    tracing::info!(
        status = "modeled",
        sketch = CMS_HEAP_BENCHMARK,
        depth,
        width,
        heap_size,
        count_events = ?count_events,
        mem_bytes_per_instance = costs.mem_bytes_per_instance,
        insert_cpu_secs = costs.insert_cpu_secs,
        merge_cpu_secs = costs.merge_cpu_secs,
        query_cpu_secs = costs.query_cpu_secs,
        "cms-with-heap atomic cost measurement"
    );

    Some(costs)
}

fn require_u64(params: &HashMap<String, Value>, key: &str, agg_type: AggregationType) -> u64 {
    require(params, key, agg_type)
        .as_u64()
        .unwrap_or_else(|| panic!("{agg_type:?} candidate has non-integer \"{key}\" param"))
}

fn require<'a>(
    params: &'a HashMap<String, Value>,
    key: &str,
    agg_type: AggregationType,
) -> &'a Value {
    params.get(key).unwrap_or_else(|| {
        panic!(
            "{agg_type:?} candidate has no \"{key}\" param; candidate_gen.rs's param_grid() \
             has drifted"
        )
    })
}

fn valid_cost_entry(entry: &AtomicCostEntry) -> bool {
    [
        entry.mem_bytes_per_instance,
        entry.insert_cpu_secs,
        entry.merge_cpu_secs,
        entry.query_cpu_secs,
    ]
    .iter()
    .all(|cost| cost.is_finite() && *cost >= 0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cms_entry(depth: i64, width: i64) -> AtomicCostEntry {
        AtomicCostEntry {
            sketch: "cms-fastpath-vector2d".into(),
            sketch_config: serde_json::json!({
                "algorithm": "cms-fastpath-vector2d",
                "params": { "cols": width, "rows": depth }
            }),
            mem_bytes_per_instance: (depth * width * 4) as f64,
            insert_cpu_secs: 8e-9,
            merge_cpu_secs: 4.5e-4,
            query_cpu_secs: 7.8e-8,
        }
    }

    fn cms_params(depth: u64, width: u64) -> HashMap<String, Value> {
        HashMap::from([
            ("depth".to_string(), Value::from(depth)),
            ("width".to_string(), Value::from(width)),
        ])
    }

    fn cms_heap_entry(depth: u64, width: u64) -> AtomicCostEntry {
        AtomicCostEntry {
            sketch: CMS_HEAP_BENCHMARK.into(),
            sketch_config: serde_json::json!({
                "algorithm": CMS_HEAP_BENCHMARK,
                "params": { "rows": depth, "cols": width }
            }),
            mem_bytes_per_instance: 1.0,
            insert_cpu_secs: 2.0,
            merge_cpu_secs: 4.0,
            query_cpu_secs: 8.0,
        }
    }

    fn cms_heap_params(
        depth: u64,
        width: u64,
        heap_size: u64,
        count_events: bool,
    ) -> HashMap<String, Value> {
        HashMap::from([
            ("depth".to_string(), Value::from(depth)),
            ("width".to_string(), Value::from(width)),
            ("heapsize".to_string(), Value::from(heap_size)),
            ("count_events".to_string(), Value::from(count_events)),
        ])
    }

    #[test]
    fn atomic_cost_entry_deserializes_sketch_benchs_documented_shape() {
        // Pinned against a real row sketch-bench's `atomic-costs` subcommand
        // actually emitted (out/atomic_costs.json, cms-fastpath-vector2d
        // rows=3 cols=1024) -- a canary for the two structs drifting apart.
        let json = r#"{"sketch":"cms-fastpath-vector2d","sketch_config":{"algorithm":"cms-fastpath-vector2d","params":{"cols":1024,"rows":3}},"mem_bytes_per_instance":12288.0,"insert_cpu_secs":8.484689139741214e-9,"merge_cpu_secs":0.00045364040539336466,"query_cpu_secs":7.799774697708031e-8}"#;
        let entry: AtomicCostEntry = serde_json::from_str(json).expect("documented shape parses");
        assert_eq!(entry.sketch, "cms-fastpath-vector2d");
        assert_eq!(entry.mem_bytes_per_instance, 12288.0);
    }

    #[test]
    fn cms_candidate_resolves_by_exact_key_regardless_of_value_type() {
        // ASAPQuery's grid stores depth/width as u64; sketch-bench's exported
        // JSON round-trips CLI-parsed integers as i64. The lookup must not
        // care which Rust integer type produced the JSON number.
        let table = vec![cms_entry(3, 1024), cms_entry(5, 2048)];
        let costs = resolve_atomic_costs(
            &table,
            AggregationType::CountMinSketch,
            &cms_params(3, 1024),
        )
        .expect("exact grid point must resolve");
        assert_eq!(costs.mem_bytes_per_instance, 3.0 * 1024.0 * 4.0);
        assert_eq!(costs.insert_cpu_secs, 8e-9);
        // Not from the table -- sketch-bench has neither, so these stay stub.
        assert_eq!(costs.subtract_cpu_secs, SUBTRACT_CPU_SECS);
        assert_eq!(costs.exact_query_cpu_secs, EXACT_QUERY_CPU_SECS);
    }

    #[test]
    fn cms_param_point_outside_the_grid_drops_the_candidate() {
        let table = vec![cms_entry(3, 1024)];
        assert!(
            resolve_atomic_costs(&table, AggregationType::CountMinSketch, &cms_params(7, 999))
                .is_none()
        );
    }

    #[test]
    fn unbenchmarked_family_falls_back_to_the_stub() {
        let table: AtomicCostTable = vec![];
        let costs = resolve_atomic_costs(&table, AggregationType::Sum, &HashMap::new())
            .expect("unbenchmarked families still get a usable (stub) cost");
        assert_eq!(
            costs.mem_bytes_per_instance,
            AtomicCosts::default().mem_bytes_per_instance
        );
    }

    #[test]
    fn cms_with_heap_without_reference_cost_drops_the_candidate() {
        // Until sketch-bench has a matching reference row, CMS-with-heap must
        // not inherit the flat stub: the optimizer should retain EXACT as its
        // visible fallback instead of silently selecting an uncosted sketch.
        let table: AtomicCostTable = vec![];
        let params = cms_heap_params(3, 1024, 40, true);
        assert!(
            resolve_atomic_costs(&table, AggregationType::CountMinSketchWithHeap, &params)
                .is_none()
        );
    }

    #[test]
    fn cms_with_heap_scales_cpu_and_models_runtime_memory() {
        let table = vec![cms_heap_entry(3, 1024)];
        let assumptions = CmsHeapCostAssumptions {
            reference_heap_size: 32,
            counter_bytes: 8.0,
            average_key_bytes: 10.0,
            heap_entry_overhead_bytes: 6.0,
        };
        let params = cms_heap_params(3, 1024, 64, true);
        let costs = resolve_cms_heap_costs(&table, &params, &assumptions)
            .expect("matching CMS-with-heap reference row must resolve");

        assert_eq!(
            costs.mem_bytes_per_instance,
            3.0 * 1024.0 * 8.0 + 64.0 * 16.0
        );
        assert_eq!(costs.insert_cpu_secs, 4.0);
        assert_eq!(costs.merge_cpu_secs, 8.0);
        assert_eq!(costs.query_cpu_secs, 16.0);
        assert_eq!(costs.subtract_cpu_secs, SUBTRACT_CPU_SECS);
        assert_eq!(costs.exact_query_cpu_secs, EXACT_QUERY_CPU_SECS);
    }

    #[test]
    fn cms_with_heap_costs_ignore_count_events() {
        let table = vec![cms_heap_entry(3, 1024)];
        let assumptions = CmsHeapCostAssumptions::default();
        let count_params = cms_heap_params(3, 1024, 40, true);
        let value_params = cms_heap_params(3, 1024, 40, false);

        assert_eq!(
            resolve_cms_heap_costs(&table, &count_params, &assumptions),
            resolve_cms_heap_costs(&table, &value_params, &assumptions)
        );
    }

    #[test]
    #[should_panic(expected = "reference_heap_size must be greater than zero")]
    fn cms_with_heap_rejects_invalid_assumptions() {
        let table = vec![cms_heap_entry(3, 1024)];
        let params = cms_heap_params(3, 1024, 40, true);
        let assumptions = CmsHeapCostAssumptions {
            reference_heap_size: 0,
            ..CmsHeapCostAssumptions::default()
        };
        resolve_cms_heap_costs(&table, &params, &assumptions);
    }

    #[test]
    #[should_panic(expected = "has no \"depth\" param")]
    fn cms_candidate_missing_its_expected_param_panics_instead_of_silently_stubbing() {
        // A CountMinSketch candidate only ever comes from candidate_gen.rs's
        // param_grid(), which always sets "depth"/"width". Landing here without
        // one means sketch_bench_key's field names have drifted from
        // param_grid()'s -- a real bug, not "this family has no data" (which
        // the CMS-with-heap resolver handles separately and must stay visibly
        // different from this case).
        let table: AtomicCostTable = vec![];
        let params = HashMap::from([("width".to_string(), Value::from(1024u64))]);
        resolve_atomic_costs(&table, AggregationType::CountMinSketch, &params);
    }

    #[test]
    fn hll_and_kll_translate_and_resolve() {
        let hll_table = vec![AtomicCostEntry {
            sketch: "hll".into(),
            sketch_config: serde_json::json!({"algorithm": "hll", "params": {"lg_k": 14}}),
            mem_bytes_per_instance: 16384.0,
            insert_cpu_secs: 1.68e-9,
            merge_cpu_secs: 2.76e-4,
            query_cpu_secs: 1.23e-4,
        }];
        let hll_params = HashMap::from([("precision".to_string(), Value::from(14u64))]);
        assert!(resolve_atomic_costs(&hll_table, AggregationType::HLL, &hll_params).is_some());

        let kll_table = vec![AtomicCostEntry {
            sketch: "kll-percall".into(),
            sketch_config: serde_json::json!({"algorithm": "kll-percall", "params": {"k": 200}}),
            mem_bytes_per_instance: 6400.0,
            insert_cpu_secs: 1.6e-8,
            merge_cpu_secs: 1.0e-3,
            query_cpu_secs: 1.6e-4,
        }];
        let kll_params = HashMap::from([("K".to_string(), Value::from(200u64))]);
        assert!(
            resolve_atomic_costs(&kll_table, AggregationType::DatasketchesKLL, &kll_params)
                .is_some()
        );
    }
}
