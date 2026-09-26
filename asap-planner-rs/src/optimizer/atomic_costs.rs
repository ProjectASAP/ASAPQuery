//! The atomic-cost table exported by sketch-bench (sketch-bench#30,
//! `scripts/export_atomic_costs.sh`), and the (sketch_type, params) lookup
//! that resolves a candidate's [`AtomicCosts`] from it.
//!
//! `AtomicCostEntry`/`AtomicCostTable` are a deliberate duplicate of
//! sketch-bench's `aqpbm_core::atomic_costs` types, not a shared dependency —
//! see ASAPQuery#524 and sketch-bench#30 for why. Keep the two in sync by
//! hand; `atomic_cost_entry_deserializes_sketch_benchs_documented_shape`
//! below is a canary for drift.

use std::collections::{BTreeMap, HashMap};
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
pub const ATOMIC_COST_SCHEMA_VERSION: u32 = 1;

/// Versioned atomic-cost document emitted by `approxbench atomic-costs`.
///
/// A profile is deliberately selected before candidate resolution: costs from
/// different input workloads must never be mixed by a flat lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AtomicCostDocument {
    pub schema_version: u32,
    pub profiles: Vec<AtomicCostProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AtomicCostProfile {
    pub workload: WorkloadDescription,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shape: Option<DataShape>,
    pub entries: Vec<AtomicCostEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataShape {
    pub cardinality: u64,
    pub zipf_exponent: Option<f64>,
    pub benchmark_events: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShapeMatchPolicy {
    pub minimum_benchmark_events: u64,
    pub max_log2_cardinality_distance: f64,
    pub max_zipf_distance: f64,
}

/// The provenance of the input data on which atomic costs were measured.
/// Synthetic descriptions remain opaque because their generator schema evolves
/// independently; external profiles are represented explicitly for selection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkloadDescription {
    Synthetic { description: Value },
    External(ExternalWorkload),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalWorkload {
    pub source: String,
    pub dataset: String,
    pub mode: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_columns: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_columns: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variate: Option<usize>,
    pub value_column: String,
    pub window_start_ns: i64,
    pub window_end_ns: i64,
    pub records_loaded: u64,
    pub source_timestamp_unit: String,
    pub timestamp_unit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AtomicCostEntry {
    pub sketch: String,
    pub sketch_config: Value,
    pub mem_bytes_per_instance: f64,
    pub insert_cpu_secs: f64,
    pub merge_cpu_secs: f64,
    pub query_cpu_secs: f64,
    pub query_accuracy: BTreeMap<String, f64>,
}

pub type AtomicCostTable = Vec<AtomicCostEntry>;

/// Parse a standalone JSON workload selector. The selector is the exact
/// `profiles[].workload` value copied from the benchmark artifact, making the
/// selected empirical input explicit in an offline planning run.
pub fn load_workload_selector(path: &Path) -> anyhow::Result<WorkloadDescription> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        anyhow::anyhow!(
            "reading atomic-cost workload selector {}: {e}",
            path.display()
        )
    })?;
    serde_json::from_str(&raw).map_err(|e| {
        anyhow::anyhow!(
            "parsing atomic-cost workload selector {}: {e}",
            path.display()
        )
    })
}

/// Read a versioned `sketch-bench atomic-costs` document and return entries
/// from exactly one requested workload profile.
pub fn load_atomic_cost_table(
    path: &Path,
    workload: &WorkloadDescription,
) -> anyhow::Result<AtomicCostTable> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("reading atomic-cost table {}: {e}", path.display()))?;
    let document: AtomicCostDocument = serde_json::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("parsing atomic-cost document {}: {e}", path.display()))?;

    if document.schema_version != ATOMIC_COST_SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported atomic-cost schema_version {} in {} (this planner supports {})",
            document.schema_version,
            path.display(),
            ATOMIC_COST_SCHEMA_VERSION
        );
    }

    let matches: Vec<_> = document
        .profiles
        .iter()
        .filter(|profile| profile.workload == *workload)
        .collect();
    match matches.as_slice() {
        [profile] => Ok(profile.entries.clone()),
        [] => anyhow::bail!(
            "no atomic-cost profile in {} matches workload selector {}",
            path.display(),
            serde_json::to_string(workload).unwrap_or_else(|_| "<unserializable>".into())
        ),
        _ => anyhow::bail!(
            "{} atomic-cost profiles in {} match workload selector {}; expected exactly one",
            matches.len(),
            path.display(),
            serde_json::to_string(workload).unwrap_or_else(|_| "<unserializable>".into())
        ),
    }
}

/// Load the nearest compatible benchmark profile. Distribution families never
/// interpolate; benchmark event count is a sufficiency gate rather than a
/// distance axis once enough data has been measured.
pub fn load_nearest_atomic_cost_table(
    path: &Path,
    observed: DataShape,
    policy: ShapeMatchPolicy,
) -> anyhow::Result<AtomicCostTable> {
    if observed.cardinality == 0
        || policy.minimum_benchmark_events == 0
        || !policy.max_log2_cardinality_distance.is_finite()
        || policy.max_log2_cardinality_distance <= 0.0
        || !policy.max_zipf_distance.is_finite()
        || policy.max_zipf_distance <= 0.0
    {
        anyhow::bail!("invalid shape matching request");
    }
    let raw = std::fs::read_to_string(path).map_err(|error| {
        anyhow::anyhow!("reading atomic-cost table {}: {error}", path.display())
    })?;
    let document: AtomicCostDocument = serde_json::from_str(&raw).map_err(|error| {
        anyhow::anyhow!("parsing atomic-cost document {}: {error}", path.display())
    })?;
    if document.schema_version != ATOMIC_COST_SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported atomic-cost schema_version {}",
            document.schema_version
        );
    }
    document
        .profiles
        .iter()
        .filter_map(|profile| Some((profile, shape_distance(observed, profile.shape?, policy)?)))
        .filter(|(_, distance)| *distance <= 1.0)
        .min_by(|(_, left), (_, right)| left.total_cmp(right))
        .map(|(profile, _)| profile.entries.clone())
        .ok_or_else(|| anyhow::anyhow!("no benchmark profile is within the observed-shape bounds"))
}

fn shape_distance(
    observed: DataShape,
    candidate: DataShape,
    policy: ShapeMatchPolicy,
) -> Option<f64> {
    if candidate.cardinality == 0
        || candidate.benchmark_events < policy.minimum_benchmark_events
        || candidate.zipf_exponent.is_some() != observed.zipf_exponent.is_some()
    {
        return None;
    }
    let cardinality =
        ((candidate.cardinality as f64).log2() - (observed.cardinality as f64).log2()).abs()
            / policy.max_log2_cardinality_distance;
    let zipf = match (candidate.zipf_exponent, observed.zipf_exponent) {
        (None, None) => 0.0,
        (Some(candidate), Some(observed)) => {
            if !candidate.is_finite() || !observed.is_finite() {
                return None;
            }
            (candidate - observed).abs() / policy.max_zipf_distance
        }
        _ => return None,
    };
    Some(cardinality.max(zipf))
}

/// Load the selector artifact and return the corresponding empirical table.
/// Offline callers use this single interface so selector validation cannot
/// drift between planner tools.
pub fn load_selected_atomic_cost_table(
    document_path: &Path,
    selector_path: &Path,
) -> anyhow::Result<AtomicCostTable> {
    let workload = load_workload_selector(selector_path)?;
    load_atomic_cost_table(document_path, &workload)
}

/// Resolve the optional atomic-cost CLI inputs as one unit. A document without
/// its workload selector is invalid; callers can keep their no-document
/// fallback without duplicating that validation.
pub fn load_optional_selected_atomic_cost_table(
    document_path: Option<&Path>,
    selector_path: Option<&Path>,
) -> anyhow::Result<Option<AtomicCostTable>> {
    match (document_path, selector_path) {
        (Some(document_path), Some(selector_path)) => {
            load_selected_atomic_cost_table(document_path, selector_path).map(Some)
        }
        (Some(_), None) => anyhow::bail!(
            "--atomic-cost-workload is required with --atomic-costs; \
             it must contain the selected profiles[].workload JSON value"
        ),
        (None, Some(_)) => anyhow::bail!("--atomic-cost-workload requires --atomic-costs"),
        (None, None) => Ok(None),
    }
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
            "cms-with-heap atomic cost measurement"
        );
        tracing::warn!(
            sketch = CMS_HEAP_BENCHMARK,
            depth,
            width,
            heap_size,
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
            "cms-with-heap atomic cost measurement"
        );
        tracing::warn!(
            sketch = CMS_HEAP_BENCHMARK,
            depth,
            width,
            heap_size,
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

    #[test]
    fn loader_selects_only_the_requested_external_profile() {
        let requested = WorkloadDescription::External(ExternalWorkload {
            source: "google".into(),
            dataset: "google/task_usage.csv.gz".into(),
            mode: "grouped".into(),
            key_columns: vec![],
            group_columns: vec!["machine_id".into()],
            variate: None,
            value_column: "cpu_rate".into(),
            window_start_ns: 10,
            window_end_ns: 20,
            records_loaded: 100,
            source_timestamp_unit: "microseconds".into(),
            timestamp_unit: "nanoseconds".into(),
        });
        let other = WorkloadDescription::External(ExternalWorkload {
            window_end_ns: 30,
            ..match requested.clone() {
                WorkloadDescription::External(workload) => workload,
                WorkloadDescription::Synthetic { .. } => unreachable!(),
            }
        });
        let document = serde_json::json!({
            "schema_version": 1,
            "profiles": [
                {"workload": other, "entries": []},
                {"workload": requested, "entries": [{
                    "sketch": "kll-percall",
                    "sketch_config": {"algorithm": "kll-percall", "params": {"k": 200}},
                    "mem_bytes_per_instance": 6400.0,
                    "insert_cpu_secs": 1e-8,
                    "merge_cpu_secs": 1e-3,
                    "query_cpu_secs": 1e-4,
                    "query_accuracy": {"mean_rank_err": 0.01}
                }]}
            ]
        });
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), document.to_string()).unwrap();

        let table = load_atomic_cost_table(file.path(), &requested).unwrap();

        assert_eq!(table.len(), 1);
        assert_eq!(table[0].sketch, "kll-percall");
    }

    #[test]
    fn loader_rejects_an_ambiguous_or_incompatible_document() {
        let workload = WorkloadDescription::Synthetic {
            description: serde_json::json!({"name": "one"}),
        };
        let file = tempfile::NamedTempFile::new().unwrap();

        std::fs::write(
            file.path(),
            serde_json::json!({
                "schema_version": 2,
                "profiles": []
            })
            .to_string(),
        )
        .unwrap();
        let err = load_atomic_cost_table(file.path(), &workload).unwrap_err();
        assert!(err.to_string().contains("schema_version 2"));
        assert!(err.to_string().contains("supports 1"));

        std::fs::write(
            file.path(),
            serde_json::json!({
                "schema_version": 1,
                "profiles": [
                    {"workload": workload, "entries": []},
                    {"workload": workload, "entries": []}
                ]
            })
            .to_string(),
        )
        .unwrap();
        let err = load_atomic_cost_table(file.path(), &workload).unwrap_err();
        assert!(err.to_string().contains("2 atomic-cost profiles"));

        std::fs::write(
            file.path(),
            serde_json::json!({
                "schema_version": 1,
                "profiles": [{
                    "workload": {"synthetic": {"description": {"name": "other"}}},
                    "entries": []
                }]
            })
            .to_string(),
        )
        .unwrap();
        let err = load_atomic_cost_table(file.path(), &workload).unwrap_err();
        assert!(err.to_string().contains("no atomic-cost profile"));
    }

    #[test]
    fn atomic_cost_entry_requires_query_accuracy() {
        let json = r#"{"sketch":"kll-percall","sketch_config":null,"mem_bytes_per_instance":1.0,"insert_cpu_secs":1.0,"merge_cpu_secs":1.0,"query_cpu_secs":1.0}"#;
        assert!(serde_json::from_str::<AtomicCostEntry>(json).is_err());
    }

    #[test]
    fn optional_loader_rejects_an_unselected_document() {
        let document = tempfile::NamedTempFile::new().unwrap();
        let err =
            load_optional_selected_atomic_cost_table(Some(document.path()), None).unwrap_err();
        assert!(err
            .to_string()
            .contains("--atomic-cost-workload is required"));
    }

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
            query_accuracy: BTreeMap::new(),
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
            query_accuracy: BTreeMap::new(),
        }
    }

    fn cms_heap_params(depth: u64, width: u64, heap_size: u64) -> HashMap<String, Value> {
        HashMap::from([
            ("depth".to_string(), Value::from(depth)),
            ("width".to_string(), Value::from(width)),
            ("heapsize".to_string(), Value::from(heap_size)),
        ])
    }

    #[test]
    fn atomic_cost_entry_deserializes_sketch_benchs_documented_shape() {
        // Pinned against the current sketch-bench atomic-cost entry shape.
        let json = r#"{"sketch":"cms-fastpath-vector2d","sketch_config":{"algorithm":"cms-fastpath-vector2d","params":{"cols":1024,"rows":3}},"mem_bytes_per_instance":12288.0,"insert_cpu_secs":8.484689139741214e-9,"merge_cpu_secs":0.00045364040539336466,"query_cpu_secs":7.799774697708031e-8,"query_accuracy":{"relative_error":0.01}}"#;
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
        let params = cms_heap_params(3, 1024, 40);
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
        let params = cms_heap_params(3, 1024, 64);
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
    fn public_resolver_dispatches_cms_with_heap_to_the_reference_model() {
        let table = vec![cms_heap_entry(3, 1024)];
        let params = cms_heap_params(3, 1024, 32);
        let costs = resolve_atomic_costs(&table, AggregationType::CountMinSketchWithHeap, &params)
            .expect("public resolver must dispatch CMS-with-heap candidates");

        assert_eq!(
            costs.mem_bytes_per_instance,
            3.0 * 1024.0 * 8.0 + 32.0 * 64.0
        );
        assert_eq!(costs.insert_cpu_secs, 2.0);
        assert_eq!(costs.merge_cpu_secs, 4.0);
        assert_eq!(costs.query_cpu_secs, 8.0);
    }

    #[test]
    #[should_panic(expected = "reference_heap_size must be greater than zero")]
    fn cms_with_heap_rejects_invalid_assumptions() {
        let table = vec![cms_heap_entry(3, 1024)];
        let params = cms_heap_params(3, 1024, 40);
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
            query_accuracy: BTreeMap::new(),
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
            query_accuracy: BTreeMap::new(),
        }];
        let kll_params = HashMap::from([("K".to_string(), Value::from(200u64))]);
        assert!(
            resolve_atomic_costs(&kll_table, AggregationType::DatasketchesKLL, &kll_params)
                .is_some()
        );
    }

    #[test]
    fn nearest_profile_uses_shape_not_cheapest_distant_scenario() {
        let document = serde_json::json!({
            "schema_version": ATOMIC_COST_SCHEMA_VERSION,
            "profiles": [
                {"workload":{"synthetic":{"description":{}}},
                 "shape":{"cardinality":1000,"zipf_exponent":1.2,"benchmark_events":100000},
                 "entries":[cms_entry(3, 512)]},
                {"workload":{"synthetic":{"description":{}}},
                 "shape":{"cardinality":1000000,"zipf_exponent":1.2,"benchmark_events":100000},
                 "entries":[cms_entry(3, 256)]}
            ]
        });
        let path = std::env::temp_dir().join(format!("atomic-shape-{}.json", std::process::id()));
        std::fs::write(&path, serde_json::to_vec(&document).unwrap()).unwrap();
        let selected = load_nearest_atomic_cost_table(
            &path,
            DataShape {
                cardinality: 1200,
                zipf_exponent: Some(1.1),
                benchmark_events: 10_000,
            },
            ShapeMatchPolicy {
                minimum_benchmark_events: 10_000,
                max_log2_cardinality_distance: 4.0,
                max_zipf_distance: 0.5,
            },
        )
        .unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(selected[0].sketch_config["params"]["cols"], 512);
    }

    #[test]
    fn nearest_profile_rejects_uniform_zipf_mismatch() {
        assert!(shape_distance(
            DataShape {
                cardinality: 1000,
                zipf_exponent: None,
                benchmark_events: 1
            },
            DataShape {
                cardinality: 1000,
                zipf_exponent: Some(1.0),
                benchmark_events: 10000
            },
            ShapeMatchPolicy {
                minimum_benchmark_events: 1000,
                max_log2_cardinality_distance: 1.0,
                max_zipf_distance: 0.5
            },
        )
        .is_none());
    }
}
