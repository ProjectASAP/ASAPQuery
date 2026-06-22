//! Offline runner for the optimization-based sketch/config selector.
//!
//! Standalone: not wired into `asap-planner`/`Controller::generate()` yet. Lets
//! you exercise `run_greedy_pipeline` against real workload configs while the
//! optimizer module is still under development (Phase 2 of issue #405).

use std::path::PathBuf;

use asap_planner::optimizer::run_greedy_pipeline;
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

    #[arg(long = "prometheus_scrape_interval")]
    prometheus_scrape_interval: u64,

    /// Placeholder arrival rate (items/sec) applied uniformly to every candidate's
    /// IngestCost. Real per-config rates aren't wired up yet — see the open TODOs
    /// in .design_docs/optimizer-v1-implementation-plan.md.
    #[arg(long = "rho", default_value = "1.0")]
    rho: f64,

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
    let schema = config.schema_from_hints();

    let (streaming, inference) =
        run_greedy_pipeline(&config, &schema, args.prometheus_scrape_interval, args.rho);

    let deployed = streaming.get_all_aggregation_configs();
    println!("=== Deployed streaming configs: {} ===", deployed.len());
    for (id, cfg) in deployed {
        println!(
            "  [{id}] {} sub_type={:?} window={}s slide={}s type={:?} metric={} params={:?}",
            cfg.aggregation_type,
            cfg.aggregation_sub_type,
            cfg.window_size,
            cfg.slide_interval,
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
