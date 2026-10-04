//! Offline runner for the optimization-based sketch/config selector.
//!
//! Standalone: not wired into `asap-planner`/`Controller::generate()` yet. Lets
//! you exercise `run_greedy_pipeline` against real workload configs while the
//! optimizer module is still under development (Phase 2 of issue #405).

use std::path::PathBuf;

use asap_planner::optimizer::{
    load_optional_selected_atomic_cost_table, run_greedy_pipeline, AtomicCostTable, LabelSetFacts,
};
use asap_planner::ControllerConfig;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "asap-optimizer-cli",
    about = "Offline runner for the optimization-based sketch/config selector (not wired into asap-planner yet)"
)]
struct Args {
    /// Path to a YAML workload config (same format as `asap-planner --input_config`).
    /// Its `metrics:` hints are required: they supply each metric's label schema.
    #[arg(long = "input_config")]
    input_config: PathBuf,

    /// Scrape interval; also sets each series' sample rate for arrival rates.
    #[arg(long = "data-ingestion-interval-ms", value_parser = clap::value_parser!(u64).range(1..))]
    data_ingestion_interval_ms: u64,

    /// YAML label-set facts: `series_count` per (metric, spatial filter) and
    /// `cardinality` per (metric, spatial filter, grouping labels).
    #[arg(long = "label-set-facts")]
    label_set_facts: PathBuf,

    /// Path to the versioned atomic-cost document sketch-bench's `atomic-costs`
    /// subcommand exports. Requires --atomic-cost-workload to select exactly
    /// one measured workload profile. Omitted: every
    /// benchmarked-family candidate (CMS/HLL/KLL) is dropped, since there is
    /// no data to cost it at — only trivial accumulators and EXACT remain
    /// selectable.
    #[arg(long = "atomic-costs")]
    atomic_costs: Option<PathBuf>,

    /// JSON `profiles[].workload` value copied from the sketch-bench atomic-cost
    /// document. This makes the empirical workload profile explicit and avoids
    /// mixing costs from different traces or time windows.
    #[arg(long = "atomic-cost-workload", requires = "atomic_costs")]
    atomic_cost_workload: Option<PathBuf>,

    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_max_level(if args.verbose > 0 {
            tracing::Level::DEBUG
        } else {
            tracing::Level::INFO
        })
        .init();

    let yaml_str = std::fs::read_to_string(&args.input_config)?;
    let config: ControllerConfig = serde_yaml::from_str(&yaml_str)?;
    let facts = LabelSetFacts::from_path(&args.label_set_facts)?;

    let atomic_cost_table = match load_optional_selected_atomic_cost_table(
        args.atomic_costs.as_deref(),
        args.atomic_cost_workload.as_deref(),
    )? {
        Some(table) => table,
        None => {
            tracing::warn!(
                "no --atomic-costs supplied; CMS/HLL/KLL candidates will never be selected"
            );
            AtomicCostTable::default()
        }
    };

    let (streaming, inference) = run_greedy_pipeline(
        &config,
        &facts,
        args.data_ingestion_interval_ms,
        &atomic_cost_table,
    )?;

    let deployed = streaming.get_all_aggregation_configs();
    println!("=== Deployed streaming configs: {} ===", deployed.len());
    for (id, cfg) in deployed {
        println!(
            "  [{id}] {} sub_type={:?} window={}ms slide={}ms type={:?} metric={} params={:?}",
            cfg.aggregation_type,
            cfg.aggregation_sub_type,
            cfg.window_size_ms,
            cfg.slide_interval_ms,
            cfg.window_type,
            cfg.metric,
            cfg.parameters,
        );
    }

    println!("\n=== Query configs: {} ===", inference.query_configs.len());
    for qc in &inference.query_configs {
        println!("  \"{}\" -> {:?}", qc.query, qc.aggregations);
    }

    Ok(())
}
