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

use super::constants::{EXACT_QUERY_CPU_SECS, SUBTRACT_CPU_SECS};
use super::cost_model::AtomicCosts;

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
/// sketch-bench has no wrapper for yet — CountMinSketchWithHeap, HydraKLL).
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
    fn require<'a>(
        params: &'a HashMap<String, Value>,
        key: &str,
        agg_type: AggregationType,
    ) -> &'a Value {
        params.get(key).unwrap_or_else(|| {
            panic!(
                "{agg_type:?} candidate has no \"{key}\" param; sketch_bench_key's field \
                 names have drifted from candidate_gen.rs's param_grid()"
            )
        })
    }

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
/// - Benchmarked family, no matching row (e.g. a param point outside the
///   swept grid, or a family sketch-bench doesn't wrap yet like
///   `CountMinSketchWithHeap`): `None` — drop the candidate, per #524.
pub fn resolve_atomic_costs(
    table: &AtomicCostTable,
    agg_type: AggregationType,
    params: &HashMap<String, Value>,
) -> Option<AtomicCosts> {
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
    fn cms_with_heap_has_no_translation_and_falls_back_to_the_stub() {
        // Real sketch (not trivial), just not wrapped by sketch-bench yet --
        // still goes through the stub path, same as a trivial accumulator,
        // per the ASAPQuery#524 scope decision.
        let table: AtomicCostTable = vec![];
        assert!(resolve_atomic_costs(
            &table,
            AggregationType::CountMinSketchWithHeap,
            &HashMap::new()
        )
        .is_some());
    }

    #[test]
    #[should_panic(expected = "has no \"depth\" param")]
    fn cms_candidate_missing_its_expected_param_panics_instead_of_silently_stubbing() {
        // A CountMinSketch candidate only ever comes from candidate_gen.rs's
        // param_grid(), which always sets "depth"/"width". Landing here without
        // one means sketch_bench_key's field names have drifted from
        // param_grid()'s -- a real bug, not "this family has no data" (which
        // resolve_atomic_costs already covers via cms_with_heap above and
        // must stay visibly different from this case).
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
