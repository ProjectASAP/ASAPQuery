//! The flat atomic-cost table sketch-bench's
//! `study_saturation.py --phase optimizer-cost` writes for the MILP.

use std::path::Path;

pub use rqe_optimizer::{AtomicCostEntry, AtomicCostTable};

/// Load the flat cost table `study_saturation.py --phase optimizer-cost`
/// writes, for the MILP. An invalid row is an error, not dropped.
pub fn load_flat_atomic_cost_table(path: &Path) -> anyhow::Result<AtomicCostTable> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("reading cost table {}: {e}", path.display()))?;
    let table: AtomicCostTable = serde_json::from_str(&raw)
        .map_err(|e| anyhow::anyhow!("parsing cost table {}: {e}", path.display()))?;
    let invalid: Vec<String> = table
        .iter()
        .filter(|entry| !valid_cost_entry(entry))
        .map(|entry| format!("{} {}", entry.sketch, entry.sketch_config))
        .collect();
    if !invalid.is_empty() {
        anyhow::bail!(
            "cost table {} has non-finite or negative costs in rows: {invalid:?}",
            path.display()
        );
    }
    Ok(table)
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

/// A cost row's required `measured_at`, for test fixtures: the cost table's
/// shape (sketch-bench `study_saturation.py` COST_*). Generic so the
/// fixture needn't name `aqpbm_core::MeasuredAt`.
#[cfg(test)]
pub(crate) fn test_measured_at<T: serde::de::DeserializeOwned>() -> T {
    serde_json::from_value(serde_json::json!({
        "items_per_instance": 1_000_000,
        "keys_per_instance": 10_000,
        "value_range": null,
        "merge_operand_items": null,
        "distribution": null,
    }))
    .expect("MeasuredAt fixture")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_cost_entry_requires_query_accuracy() {
        let json = r#"{"sketch":"kll-percall","sketch_config":null,"mem_bytes_per_instance":1.0,"insert_cpu_secs":1.0,"merge_cpu_secs":1.0,"query_cpu_secs":1.0}"#;
        assert!(serde_json::from_str::<AtomicCostEntry>(json).is_err());
    }

    #[test]
    fn flat_loader_rejects_non_finite_or_negative_costs() {
        let row = |insert: &str| {
            format!(
                r#"{{"sketch":"hll","sketch_config":null,"mem_bytes_per_instance":1.0,"insert_cpu_secs":{insert},"merge_cpu_secs":1.0,"query_cpu_secs":1.0,"query_accuracy":{{"relative_error":0.0}},"accuracy_metric":"relative_error","measured_at":{{"items_per_instance":1000000,"keys_per_instance":10000,"value_range":null,"merge_operand_items":null,"distribution":null}}}}"#
            )
        };
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), format!("[{}]", row("1e-7"))).unwrap();
        assert_eq!(load_flat_atomic_cost_table(file.path()).unwrap().len(), 1);
        std::fs::write(file.path(), format!("[{},{}]", row("1e-7"), row("-1.0"))).unwrap();
        let err = load_flat_atomic_cost_table(file.path()).unwrap_err();
        assert!(err.to_string().contains("negative"), "{err}");
    }

    /// Rows name their accuracy metric and where they were measured; a row
    /// without either is an old table, rejected rather than guessed.
    #[test]
    fn atomic_cost_entry_requires_accuracy_metric_and_measured_at() {
        let base = r#""sketch":"kll-percall","sketch_config":null,"mem_bytes_per_instance":1.0,"insert_cpu_secs":1.0,"merge_cpu_secs":1.0,"query_cpu_secs":1.0,"query_accuracy":{"mean_rank_err":0.01}"#;
        let measured_at = r#""measured_at":{"items_per_instance":1000000,"keys_per_instance":null,"value_range":null,"merge_operand_items":62500,"distribution":{"kind":"pareto","alpha":2.0,"scale":1000.0,"seed":42}}"#;
        let full = format!(r#"{{{base},"accuracy_metric":"mean_rank_err",{measured_at}}}"#);
        let entry: AtomicCostEntry = serde_json::from_str(&full).unwrap();
        assert_eq!(entry.measured_at.items_per_instance, 1_000_000);
        assert_eq!(entry.accuracy(), Some(0.01));
        for partial in [
            format!(r#"{{{base},{measured_at}}}"#),
            format!(r#"{{{base},"accuracy_metric":"mean_rank_err"}}"#),
        ] {
            assert!(serde_json::from_str::<AtomicCostEntry>(&partial).is_err());
        }
    }
}
