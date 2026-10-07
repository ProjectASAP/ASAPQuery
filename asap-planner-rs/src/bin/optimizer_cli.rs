//! Offline runner for the rqe-optimizer MILP planner: prints the plan and its
//! cost, and optionally writes the streaming and inference configs.

use std::path::PathBuf;

use anyhow::Context;
use asap_planner::optimizer::{
    build_milp_workload, load_flat_atomic_cost_table, load_workload_facts, plan_to_planner_output,
    reject_avg_queries, solve_milp, MilpError,
};
use asap_planner::ControllerConfig;
use clap::Parser;
use rqe_optimizer::milp::Objective;
use rqe_optimizer::saturation::SaturationCurves;

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

/// Objective weights must be finite and non-negative: a negative weight
/// rewards cost, and NaN poisons every coefficient.
fn parse_weight(s: &str) -> Result<f64, String> {
    match s.parse::<f64>() {
        Ok(w) if w.is_finite() && w >= 0.0 => Ok(w),
        Ok(w) => Err(format!("must be finite and >= 0, got {w}")),
        Err(e) => Err(e.to_string()),
    }
}

fn run_milp(args: &Args, config: &ControllerConfig) -> anyhow::Result<()> {
    config.warn_default_slas();
    let Some(hints) = config.metrics.as_deref() else {
        return Err(MilpError::MissingMetricHints.into());
    };
    let facts = load_workload_facts(&args.workload_facts, hints, args.data_ingestion_interval_ms)?;
    let curves = SaturationCurves::load(&args.saturation_dir).with_context(|| {
        format!(
            "loading saturation curves from --saturation-dir {}",
            args.saturation_dir.display()
        )
    })?;
    let costs = load_flat_atomic_cost_table(&args.atomic_costs)?;
    let Objective::AUCCost { w_cpu, w_mem } = Objective::default();
    let (w_cpu, w_mem) = (args.w_cpu.unwrap_or(w_cpu), args.w_mem.unwrap_or(w_mem));
    // All-zero weights make every plan cost 0, so the solver's pick is arbitrary.
    anyhow::ensure!(
        w_cpu > 0.0 || w_mem > 0.0,
        "--w-cpu and --w-mem are both 0; at least one must be positive"
    );
    let objective = Objective::AUCCost { w_cpu, w_mem };
    tracing::debug!(?objective, cost_rows = costs.len(), "milp: inputs loaded");

    // Fail before solving when the plan would be written but can't be.
    if args.output_dir.is_some() {
        reject_avg_queries(config)?;
    }
    let workload = build_milp_workload(config, &facts, args.data_ingestion_interval_ms)?;
    let solution = solve_milp(
        &workload,
        &facts,
        &costs,
        objective,
        args.allow_undeployable_families,
        &|raqe, deployment| curves.accuracy(raqe, deployment, &facts),
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
        let output = plan_to_planner_output(config, &workload, &solution)?;
        // Serialize both before writing either, so a failure can't leave a
        // new streaming config next to a stale inference config.
        let streaming = output.to_streaming_yaml_string()?;
        let inference = output.to_inference_yaml_string()?;
        std::fs::create_dir_all(dir)?;
        std::fs::write(dir.join("streaming_config.yaml"), streaming)?;
        std::fs::write(dir.join("inference_config.yaml"), inference)?;
        println!("\nwrote configs to {}", dir.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_weight;

    #[test]
    fn weights_must_be_finite_and_non_negative() {
        assert_eq!(parse_weight("0.5"), Ok(0.5));
        assert_eq!(parse_weight("0"), Ok(0.0));
        for bad in ["-1", "NaN", "inf", "x"] {
            assert!(parse_weight(bad).is_err(), "{bad}");
        }
    }
}
