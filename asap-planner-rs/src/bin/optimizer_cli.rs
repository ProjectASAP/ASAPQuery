//! Offline runner for the optimization-based sketch/config selector.
//!
//! Standalone: not wired into `asap-planner`/`Controller::generate()` yet. Lets
//! you exercise `run_greedy_pipeline` against real workload configs while the
//! optimizer module is still under development (Phase 2 of issue #405).

use std::path::PathBuf;

use asap_planner::optimizer::{
    build_milp_workload, load_optional_selected_atomic_cost_table, load_workload_facts,
    run_greedy_pipeline, solve_milp, AtomicCostTable, LabelSetFacts,
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
    #[arg(long = "label-set-facts", required_unless_present = "milp")]
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

    /// Plan with sketch-bench's rqe-optimizer MILP and print the plan; writes
    /// no configs yet.
    #[arg(long)]
    milp: bool,

    /// MILP only. YAML workload facts: per metric, `cardinality` per label
    /// set, including the set of all its labels (the series count).
    #[arg(
        long = "workload-facts",
        required_if_eq("milp", "true"),
        requires = "milp"
    )]
    workload_facts: Option<PathBuf>,

    /// MILP only. Objective weight on CPU-sec/sec. Default: rqe-optimizer's.
    #[arg(long = "w-cpu", requires = "milp")]
    w_cpu: Option<f64>,

    /// MILP only. Objective weight on memory GiB. Default: rqe-optimizer's.
    #[arg(long = "w-mem", requires = "milp")]
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

fn run_milp(args: &Args, config: &ControllerConfig) -> anyhow::Result<()> {
    let hints = config.metrics.as_deref().unwrap_or_default();
    let facts = load_workload_facts(
        args.workload_facts
            .as_deref()
            .expect("clap requires --workload-facts with --milp"),
        hints,
        args.data_ingestion_interval_ms,
    )?;
    let costs_path = args
        .atomic_costs
        .as_deref()
        .expect("clap requires --atomic-costs with --milp");
    let costs: AtomicCostTable = serde_json::from_str(&std::fs::read_to_string(costs_path)?)
        .map_err(|e| anyhow::anyhow!("parsing cost table {}: {e}", costs_path.display()))?;
    let Objective::AUCCost { w_cpu, w_mem } = Objective::default();
    let objective = Objective::AUCCost {
        w_cpu: args.w_cpu.unwrap_or(w_cpu),
        w_mem: args.w_mem.unwrap_or(w_mem),
    };
    tracing::debug!(?objective, cost_rows = costs.len(), "milp: inputs loaded");

    let workload = build_milp_workload(config, &facts, args.data_ingestion_interval_ms)?;
    let (deployments, solution) = solve_milp(&workload, &facts, &costs, objective)?;

    let mut active: Vec<usize> = solution.mapping.clone();
    active.sort();
    active.dedup();
    println!("=== Deployments: {} ===", active.len());
    for &d in &active {
        let dep = &deployments[d];
        println!(
            "  [{d}] {:?} {} config={} metric={} grouping={:?} window={}ms slide={}ms",
            dep.capability,
            dep.config.sketch,
            dep.config.sketch_config,
            dep.metric,
            dep.grouping_labels,
            dep.window_ms,
            dep.slide_ms,
        );
    }
    println!("\n=== Raqes: {} ===", workload.raqes.len());
    for ((raqe, &d), latency_ms) in workload
        .raqes
        .iter()
        .zip(&solution.mapping)
        .zip(&solution.plan_cost.query_latency_ms)
    {
        println!("  {} -> [{d}] latency={latency_ms:.3e}ms", raqe.id);
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
    Ok(())
}
