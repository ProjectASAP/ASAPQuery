//! The flat atomic-cost table sketch-bench's `export_rqe_optimizer_costs.sh`
//! writes for the MILP.

use std::path::Path;

pub use rqe_optimizer::{AtomicCostEntry, AtomicCostTable};

/// Load the flat cost table `export_rqe_optimizer_costs.sh` writes, for the
/// MILP. An invalid row is an error, not dropped.
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
                r#"{{"sketch":"hll","sketch_config":null,"mem_bytes_per_instance":1.0,"insert_cpu_secs":{insert},"merge_cpu_secs":1.0,"query_cpu_secs":1.0,"query_accuracy":{{}}}}"#
            )
        };
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), format!("[{}]", row("1e-7"))).unwrap();
        assert_eq!(load_flat_atomic_cost_table(file.path()).unwrap().len(), 1);
        std::fs::write(file.path(), format!("[{},{}]", row("1e-7"), row("-1.0"))).unwrap();
        let err = load_flat_atomic_cost_table(file.path()).unwrap_err();
        assert!(err.to_string().contains("negative"), "{err}");
    }

    /// Rows with `measured_at` load, and older rows without it still do.
    #[test]
    fn atomic_cost_entry_accepts_optional_measured_at() {
        let base = r#""sketch":"kll-percall","sketch_config":null,"mem_bytes_per_instance":1.0,"insert_cpu_secs":1.0,"merge_cpu_secs":1.0,"query_cpu_secs":1.0,"query_accuracy":{}"#;
        let with = format!(
            r#"{{{base},"measured_at":{{"items_per_instance":1000000,"keys_per_instance":100000,"value_range":[1.0,100000.0],"merge_operand_items":62500,"distribution":{{"kind":"zipf","skewness":1.1,"population_size":100000,"seed":42}}}}}}"#
        );
        let entry: AtomicCostEntry = serde_json::from_str(&with).unwrap();
        assert_eq!(entry.measured_at.unwrap().items_per_instance, 1_000_000);
        let without: AtomicCostEntry = serde_json::from_str(&format!("{{{base}}}")).unwrap();
        assert!(without.measured_at.is_none());
    }
}
