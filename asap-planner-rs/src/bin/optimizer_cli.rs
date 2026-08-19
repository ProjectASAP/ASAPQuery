//! Offline runner for the optimization-based sketch/config selector.
//!
//! Standalone: not wired into `asap-planner`/`Controller::generate()` yet. Lets
//! you exercise `run_greedy_pipeline` against real workload configs while the
//! optimizer module is still under development (Phase 2 of issue #405).

use std::path::PathBuf;

use asap_planner::optimizer::{load_atomic_cost_table, run_greedy_pipeline, AtomicCostTable};
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

    /// Placeholder arrival rate (items/sec) applied uniformly to every candidate's
    /// IngestCost. Real per-config rates aren't wired up yet — see the open TODOs
    /// in .design_docs/optimizer-v1-implementation-plan.md.
    #[arg(long = "rho", default_value = "1.0", value_parser = parse_positive_finite)]
    rho: f64,

    /// Path to the atomic-cost table sketch-bench's `atomic-costs` subcommand
    /// exports (see ASAPQuery#524, sketch-bench#30). Omitted: every
    /// benchmarked-family candidate (CMS/HLL/KLL) is dropped, since there is
    /// no data to cost it at — only trivial accumulators and EXACT remain
    /// selectable.
    #[arg(long = "atomic-costs")]
    atomic_costs: Option<PathBuf>,

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
    let schema = config.schema_from_hints();

    let atomic_cost_table = match &args.atomic_costs {
        Some(path) => load_atomic_cost_table(path)?,
        None => {
            tracing::warn!(
                "no --atomic-costs supplied; CMS/HLL/KLL candidates will never be selected"
            );
            AtomicCostTable::default()
        }
    };

    let (streaming, inference) = run_greedy_pipeline(
        &config,
        &schema,
        args.data_ingestion_interval_ms,
        args.rho,
        &atomic_cost_table,
    );

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
