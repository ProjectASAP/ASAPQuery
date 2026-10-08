//! Offline runner for the rqe-optimizer MILP planner: prints the plan and its
//! cost, and optionally writes the streaming and inference configs.

use std::path::PathBuf;

use asap_planner::optimizer::{
    parse_weight, plan_milp, plan_to_planner_output, reject_unwritable_queries, MilpInputs,
    MilpPlan,
};
use asap_planner::ControllerConfig;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "asap-optimizer-cli",
    about = "Offline runner for the rqe-optimizer MILP planner"
)]
struct Args {
    /// Path to a YAML workload config (same format as `asap-planner --input_config`).
    /// Its `metrics:` hints are required: they supply each metric's label schema.
    #[arg(long = "input_config")]
    input_config: PathBuf,

    /// Scrape interval; also sets each series' sample rate for arrival rates.
    #[arg(long = "data-ingestion-interval-ms", value_parser = clap::value_parser!(u64).range(1..))]
    data_ingestion_interval_ms: u64,

    /// The flat cost table sketch-bench's
    /// `study_saturation.py --phase optimizer-cost` writes
    /// (`rqe_atomic_costs.json`).
    #[arg(long = "atomic-costs")]
    atomic_costs: PathBuf,

    /// Write `streaming_config.yaml` and `inference_config.yaml` for the plan
    /// here.
    #[arg(long = "output-dir")]
    output_dir: Option<PathBuf>,

    /// Also plan with families the engine can't deploy; prints the plan and
    /// writes no configs.
    #[arg(long = "allow-undeployable-families", conflicts_with = "output_dir")]
    allow_undeployable_families: bool,

    /// YAML workload facts: per metric, positive `value_range` and
    /// `cardinality` per label set, including all labels (the series count),
    /// plus the `shape` of each grouping sketches may serve.
    #[arg(long = "workload-facts")]
    workload_facts: PathBuf,

    /// sketch-bench's saturation-study directory (`out_grid_1e7_cost/`,
    /// `out_1e9/`): sketch accuracy is read off its error-vs-N curves at each
    /// grouping's `shape`.
    #[arg(long = "saturation-dir")]
    saturation_dir: PathBuf,

    /// Objective weight on CPU-sec/sec. Default: rqe-optimizer's.
    #[arg(long = "w-cpu", value_parser = parse_weight)]
    w_cpu: Option<f64>,

    /// Objective weight on memory GiB. Default: rqe-optimizer's.
    #[arg(long = "w-mem", value_parser = parse_weight)]
    w_mem: Option<f64>,

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
    run_milp(&args, &config)
}

fn run_milp(args: &Args, config: &ControllerConfig) -> anyhow::Result<()> {
    // Fail before solving when the plan would be written but can't be.
    if args.output_dir.is_some() {
        reject_unwritable_queries(config)?;
    }
    let MilpPlan {
        workload,
        solution,
        objective,
    } = plan_milp(
        config,
        &MilpInputs {
            workload_facts: &args.workload_facts,
            atomic_costs: &args.atomic_costs,
            saturation_dir: &args.saturation_dir,
            scrape_interval_ms: args.data_ingestion_interval_ms,
            w_cpu: args.w_cpu,
            w_mem: args.w_mem,
            allow_undeployable_families: args.allow_undeployable_families,
        },
    )?;

    println!("=== Deployments: {} ===", solution.deployments.len());
    for (d, planned) in solution.deployments.iter().enumerate() {
        let dep = &planned.deployment;
        println!(
            "  [{d}] {:?} {} config={} metric={} grouping={:?} window={}ms slide={}ms retained={} key_tracker={}",
            dep.capability,
            dep.config.sketch,
            dep.config.sketch_config,
            dep.metric,
            dep.grouping_labels,
            dep.window_ms,
            dep.slide_ms,
            planned.retained_instance_count,
            dep.key_tracker.is_some(),
        );
    }
    println!("\n=== Raqes: {} ===", workload.raqes.len());
    for ((raqe, planned), latency_ms) in workload
        .raqes
        .iter()
        .zip(&solution.raqes)
        .zip(&solution.plan_cost.query_latency_ms)
    {
        println!(
            "  {} -> [{}] n={} latency={latency_ms:.3e}ms",
            raqe.id, planned.deployment, planned.merged_instance_count
        );
    }
    let cost = &solution.plan_cost;
    println!(
        "\nobjective={:.6e} cpu={:.6e} cpu-sec/sec memory={:.3} MB",
        objective.value(cost),
        cost.cpu_secs_per_sec(),
        cost.memory_bytes() / 1e6,
    );
    for (phase, c) in [
        ("ingest", &cost.ingest),
        ("merge", &cost.merge),
        ("query", &cost.query),
        ("storage", &cost.storage),
    ] {
        println!(
            "  {phase}: cpu={:.6e} cpu-sec/sec memory={:.3} MB",
            c.cpu_secs_per_sec,
            c.memory_bytes / 1e6
        );
    }

    if let Some(dir) = &args.output_dir {
        plan_to_planner_output(config, &workload, &solution)?.write_to_dir(dir)?;
        println!("\nwrote configs to {}", dir.display());
    }
    Ok(())
}
