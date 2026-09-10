//! Offline runner for the optimization-based sketch/config selector.
//!
//! Standalone: not wired into `asap-planner`/`Controller::generate()` yet. Lets
//! you exercise `run_greedy_pipeline` against real workload configs while the
//! optimizer module is still under development (Phase 2 of issue #405).

use std::path::PathBuf;

use asap_planner::optimizer::{
    load_nearest_atomic_cost_table, load_optional_selected_atomic_cost_table, run_greedy_pipeline,
    AtomicCostTable, DataShape, SeriesDataset, ShapeMatchPolicy,
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
    #[arg(long = "input_config")]
    input_config: PathBuf,

    #[arg(long = "data-ingestion-interval-ms")]
    data_ingestion_interval_ms: u64,

    /// CSV series inventory used to derive metric schemas and label-group counts.
    #[arg(long = "dataset")]
    dataset: PathBuf,

    /// Placeholder arrival rate (items/sec) applied uniformly to every candidate's
    /// IngestCost. Real per-config rates aren't wired up yet — see the open TODOs
    /// in .design_docs/optimizer-v1-implementation-plan.md.
    #[arg(long = "rho", default_value = "1.0", value_parser = parse_positive_finite)]
    rho: f64,

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

    /// JSON DataShape observed by ASAPQuery-backend. Selects the nearest safe
    /// benchmark profile instead of requiring descriptor equality.
    #[arg(
        long = "atomic-cost-observed-shape",
        requires = "atomic_costs",
        conflicts_with = "atomic_cost_workload"
    )]
    atomic_cost_observed_shape: Option<PathBuf>,
    #[arg(long, default_value_t = 100_000)]
    minimum_benchmark_events: u64,
    #[arg(long, default_value_t = 1.0)]
    max_log2_cardinality_distance: f64,
    #[arg(long, default_value_t = 0.2)]
    max_zipf_distance: f64,

    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

fn parse_positive_finite(s: &str) -> Result<f64, String> {
    let v: f64 = s.parse().map_err(|_| format!("not a valid number: {s}"))?;
    if !v.is_finite() || v <= 0.0 {
        return Err(format!("--rho must be a positive finite number, got {v}"));
    }
    Ok(v)
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
    let dataset = SeriesDataset::from_path(&args.dataset)?;

    let selected = if let Some(shape_path) = args.atomic_cost_observed_shape.as_deref() {
        let observed: DataShape = serde_json::from_str(&std::fs::read_to_string(shape_path)?)?;
        Some(load_nearest_atomic_cost_table(
            args.atomic_costs
                .as_deref()
                .expect("clap requires --atomic-costs"),
            observed,
            ShapeMatchPolicy {
                minimum_benchmark_events: args.minimum_benchmark_events,
                max_log2_cardinality_distance: args.max_log2_cardinality_distance,
                max_zipf_distance: args.max_zipf_distance,
            },
        )?)
    } else {
        load_optional_selected_atomic_cost_table(
            args.atomic_costs.as_deref(),
            args.atomic_cost_workload.as_deref(),
        )?
    };
    let atomic_cost_table = match selected {
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
        &dataset,
        args.data_ingestion_interval_ms,
        args.rho,
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
