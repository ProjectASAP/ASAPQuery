//! Offline runner for the optimization-based sketch/config selector.
//!
//! Standalone: not wired into `asap-planner`/`Controller::generate()` yet. Lets
//! you exercise `run_greedy_pipeline` against real workload configs while the
//! optimizer module is still under development (Phase 2 of issue #405).

use std::path::PathBuf;

use asap_planner::optimizer::{
    build_milp_workload, load_flat_atomic_cost_table, load_optional_selected_atomic_cost_table,
    load_workload_facts, plan_to_planner_output, reject_avg_queries, run_greedy_pipeline,
    solve_milp, AtomicCostTable, LabelSetFacts, LabelSetFactsError,
};
use asap_planner::ControllerConfig;
use clap::Parser;
use rqe_optimizer::milp::Objective;

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

    /// Greedy only. YAML label-set facts: `series_count` per (metric, spatial
    /// filter) and `cardinality` per (metric, spatial filter, grouping labels).
    #[arg(
        long = "label-set-facts",
        required_unless_present = "milp",
        conflicts_with = "milp"
    )]
    label_set_facts: Option<PathBuf>,

    /// Greedy: the versioned atomic-cost document sketch-bench's
    /// `atomic-costs` subcommand exports; requires --atomic-cost-workload.
    /// Omitted: every benchmarked-family candidate (CMS/HLL/KLL) is dropped,
    /// leaving only trivial accumulators and EXACT.
    /// MILP: the flat cost table `export_rqe_optimizer_costs.sh` writes
    /// (`rqe_atomic_costs.json`); required.
    #[arg(long = "atomic-costs", required_if_eq("milp", "true"))]
    atomic_costs: Option<PathBuf>,

    /// Greedy only. JSON `profiles[].workload` value copied from the
    /// sketch-bench atomic-cost document. This makes the empirical workload
    /// profile explicit and avoids mixing costs from different traces or time
    /// windows.
    #[arg(
        long = "atomic-cost-workload",
        requires = "atomic_costs",
        conflicts_with = "milp"
    )]
    atomic_cost_workload: Option<PathBuf>,

    /// Plan with sketch-bench's rqe-optimizer MILP and print the plan.
    #[arg(long)]
    milp: bool,

    /// MILP only. Write `streaming_config.yaml` and `inference_config.yaml`
    /// for the plan here.
    #[arg(long = "output-dir", requires = "milp")]
    output_dir: Option<PathBuf>,

    /// MILP only. Also plan with families the engine can't deploy; prints the
    /// plan and writes no configs.
    #[arg(
        long = "allow-undeployable-families",
        requires = "milp",
        conflicts_with = "output_dir"
    )]
    allow_undeployable_families: bool,

    /// MILP only. YAML workload facts: per metric, positive `value_range`
    /// and `cardinality` per label set, including all labels (the series count).
    #[arg(
        long = "workload-facts",
        required_if_eq("milp", "true"),
        requires = "milp"
    )]
    workload_facts: Option<PathBuf>,

    /// MILP only. Objective weight on CPU-sec/sec. Default: rqe-optimizer's.
    #[arg(long = "w-cpu", requires = "milp", value_parser = parse_weight)]
    w_cpu: Option<f64>,

    /// MILP only. Objective weight on memory GiB. Default: rqe-optimizer's.
    #[arg(long = "w-mem", requires = "milp", value_parser = parse_weight)]
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
    if args.milp {
        return run_milp(&args, &config);
    }
    let facts = LabelSetFacts::from_path(
        args.label_set_facts
            .as_deref()
            .expect("clap requires --label-set-facts without --milp"),
    )?;

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
        return Err(LabelSetFactsError::MissingMetricHints.into());
    };
    let facts = load_workload_facts(
        args.workload_facts
            .as_deref()
            .expect("clap requires --workload-facts with --milp"),
        hints,
        args.data_ingestion_interval_ms,
    )?;
    let costs = load_flat_atomic_cost_table(
        args.atomic_costs
            .as_deref()
            .expect("clap requires --atomic-costs with --milp"),
    )?;
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
