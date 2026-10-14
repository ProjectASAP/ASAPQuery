use asap_planner::optimizer::{
    parse_weight, plan_milp, plan_to_planner_output, reject_unwritable_queries, saved_plan_output,
    MilpConfig, MilpInputs, PlanSelector,
};
use asap_planner::{
    Controller, ControllerConfig, ElasticController, ElasticRuntimeOptions, RuntimeOptions,
    SQLController, SQLControllerConfig, SQLRuntimeOptions, StreamingEngine,
};
use asap_types::enums::QueryLanguage;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "asap-planner", about = "ASAP Query Planner")]
struct Args {
    /// Path to a hand-authored YAML workload config. Mutually exclusive with --query-log.
    #[arg(long = "input_config", conflicts_with = "query_log")]
    input_config: Option<PathBuf>,

    /// Path to a Prometheus query log file (newline-delimited JSON). Mutually exclusive with --input_config.
    #[arg(long = "query-log", conflicts_with = "input_config")]
    query_log: Option<PathBuf>,

    #[arg(long = "output_dir")]
    output_dir: PathBuf,

    /// Base URL of the Prometheus instance used to auto-infer metric label sets.
    /// Optional: when provided, the planner queries Prometheus for label discovery.
    /// When absent, labels are taken from the `metrics` hint in the config file.
    /// Example: http://localhost:9090
    #[arg(long = "prometheus-url", required = false)]
    prometheus_url: Option<String>,

    #[arg(long = "streaming_engine", value_enum)]
    streaming_engine: EngineArg,

    #[arg(long = "enable-punting", default_value = "false")]
    enable_punting: bool,

    #[arg(long = "range-duration-ms", default_value = "0")]
    range_duration_ms: u64,

    #[arg(long = "step-ms", default_value = "0")]
    step_ms: u64,

    #[arg(long = "query-language", value_enum, default_value = "promql")]
    query_language: QueryLanguage,

    #[arg(long = "data-ingestion-interval-ms", required = false)]
    data_ingestion_interval_ms: Option<u64>,

    /// ClickHouse base URL for auto-inferring metadata_columns when not listed
    /// in the config file. Example: http://localhost:8123
    #[arg(long = "clickhouse-url", required = false)]
    clickhouse_url: Option<String>,

    #[arg(long = "clickhouse-database", required = false)]
    clickhouse_database: Option<String>,

    /// `milp` plans with sketch-bench's rqe-optimizer: PromQL or SQL with
    /// --input_config only, labels from its `metrics:` hints or tables'
    /// `metadata_columns`. `saved` writes a PromQL or SQL plan sketch-bench's
    /// `autosketch_vs_asap` already solved.
    #[arg(long, value_enum, default_value = "legacy")]
    planner: PlannerArg,

    /// Saved only. sketch-bench `autosketch_vs_asap` plan file
    /// (`<table>.plans.json.gz`).
    #[arg(long, required_if_eq("planner", "saved"))]
    plan: Option<PathBuf>,

    /// Saved only. The eval workload table the plan was solved for; its
    /// `e2e` block gives the queries in --query-language and the metrics'
    /// labels.
    #[arg(long = "workload-table", required_if_eq("planner", "saved"))]
    workload_table: Option<PathBuf>,

    /// Saved only. The plan's method, e.g. `asap`.
    #[arg(long, required_if_eq("planner", "saved"))]
    method: Option<String>,

    /// Saved only. The plan's objective weights, e.g. `cpu` or `fargate`.
    #[arg(long, required_if_eq("planner", "saved"))]
    weights: Option<String>,

    /// Saved only. The plan at this latency SLA; without it, the cheapest.
    #[arg(long = "sla-ms")]
    sla_ms: Option<f64>,

    /// MILP only. YAML workload facts: per metric, positive `value_range` and
    /// `cardinality` per label set, including all labels (the series count),
    /// plus the `shape` of each grouping sketches may serve.
    #[arg(long = "workload-facts")]
    workload_facts: Option<PathBuf>,

    /// MILP only. The flat cost table sketch-bench's
    /// `study_saturation.py --phase optimizer-cost` writes
    /// (`rqe_atomic_costs.json`).
    #[arg(long = "atomic-costs")]
    atomic_costs: Option<PathBuf>,

    /// MILP only. sketch-bench's saturation-study directory: sketch accuracy
    /// is read off its error-vs-N curves at each grouping's `shape`.
    #[arg(long = "saturation-dir")]
    saturation_dir: Option<PathBuf>,

    /// MILP only. Objective weight on CPU-sec/sec. Default: rqe-optimizer's.
    #[arg(long = "w-cpu", value_parser = parse_weight)]
    w_cpu: Option<f64>,

    /// MILP only. Objective weight on memory GiB. Default: rqe-optimizer's.
    #[arg(long = "w-mem", value_parser = parse_weight)]
    w_mem: Option<f64>,

    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
enum EngineArg {
    Precompute,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum PlannerArg {
    Legacy,
    Milp,
    Saved,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_max_level(if args.verbose > 0 {
            tracing::Level::DEBUG
        } else {
            tracing::Level::WARN
        })
        .init();

    let engine = match args.streaming_engine {
        EngineArg::Precompute => StreamingEngine::Precompute,
    };

    if args.planner == PlannerArg::Milp {
        anyhow::bail!(
            "--planner milp is temporarily disabled; use --planner saved for sketch-bench eval plans"
        );
    }

    if args.planner != PlannerArg::Saved {
        anyhow::ensure!(
            args.plan.is_none()
                && args.workload_table.is_none()
                && args.method.is_none()
                && args.weights.is_none()
                && args.sla_ms.is_none(),
            "--plan, --workload-table, --method, --weights and --sla-ms require --planner saved"
        );
    }
    if args.planner != PlannerArg::Milp {
        anyhow::ensure!(
            args.workload_facts.is_none()
                && args.atomic_costs.is_none()
                && args.saturation_dir.is_none()
                && args.w_cpu.is_none()
                && args.w_mem.is_none(),
            "--workload-facts, --atomic-costs, --saturation-dir, --w-cpu and --w-mem require \
             --planner milp"
        );
    }
    let output_dir = args.output_dir.clone();
    match args.planner {
        PlannerArg::Milp => run_milp(&args)?,
        PlannerArg::Saved => run_saved(&args)?,
        PlannerArg::Legacy => run_legacy(args, engine)?,
    }
    println!("Generated configs in {}", output_dir.display());
    Ok(())
}

fn run_legacy(args: Args, engine: StreamingEngine) -> anyhow::Result<()> {
    match args.query_language {
        QueryLanguage::promql => {
            let scrape_interval_ms = args.data_ingestion_interval_ms.ok_or_else(|| {
                anyhow::anyhow!("--data-ingestion-interval-ms is required for PromQL mode")
            })?;
            let opts = RuntimeOptions {
                data_ingestion_interval_ms: scrape_interval_ms,
                streaming_engine: engine,
                enable_punting: args.enable_punting,
                range_duration_ms: args.range_duration_ms,
                step_ms: args.step_ms,
            };
            let controller = match (args.input_config, args.query_log, args.prometheus_url) {
                (Some(config_path), None, Some(url)) => {
                    Controller::from_file(&config_path, opts, &url)?
                }
                (Some(config_path), None, None) => {
                    let yaml_str = std::fs::read_to_string(&config_path)?;
                    let config: asap_planner::ControllerConfig = serde_yaml::from_str(&yaml_str)?;
                    let schema = config.schema_from_hints();
                    Controller::from_file_with_schema(&config_path, schema, opts)?
                }
                (None, Some(log_path), Some(url)) => {
                    Controller::from_query_log(&log_path, opts, &url)?
                }
                (None, Some(_log_path), None) => {
                    anyhow::bail!(
                        "--prometheus-url is required when using --query-log \
                         (query logs have no metrics hint to fall back on)"
                    )
                }
                (None, None, _) => {
                    anyhow::bail!("provide one of --input_config or --query-log")
                }
                _ => unreachable!("clap conflicts_with prevents this combination"),
            };
            controller.generate_to_dir(&args.output_dir)?;
        }
        QueryLanguage::sql | QueryLanguage::elastic_sql => {
            let interval = args.data_ingestion_interval_ms.ok_or_else(|| {
                anyhow::anyhow!("--data-ingestion-interval-ms is required for SQL mode")
            })?;
            let config_path = args
                .input_config
                .ok_or_else(|| anyhow::anyhow!("--input_config is required for SQL mode"))?;
            let opts = SQLRuntimeOptions {
                streaming_engine: engine,
                query_evaluation_time: None,
                data_ingestion_interval_ms: interval,
            };
            let controller = match args.clickhouse_url {
                Some(ref url) => SQLController::from_file_with_discovery(
                    &config_path,
                    url,
                    args.clickhouse_database.as_deref().unwrap_or("default"),
                    opts,
                )?,
                None => SQLController::from_file(&config_path, opts)?,
            };
            controller.generate_to_dir(&args.output_dir)?;
        }
        QueryLanguage::elastic_querydsl => {
            let interval = args.data_ingestion_interval_ms.ok_or_else(|| {
                anyhow::anyhow!(
                    "--data-ingestion-interval-ms is required for Elasticsearch DSL mode"
                )
            })?;
            let config_path = args.input_config.ok_or_else(|| {
                anyhow::anyhow!("--input_config is required for Elasticsearch DSL mode")
            })?;
            let opts = ElasticRuntimeOptions {
                streaming_engine: engine,
                data_ingestion_interval_ms: interval,
            };
            ElasticController::from_file(&config_path, opts)?.generate_to_dir(&args.output_dir)?;
        }
    }
    Ok(())
}

/// Checks the flags `--planner milp` and `--planner saved` share: labels from
/// the config, no legacy-only flags. Returns the
/// positive scrape interval, which per-series sample rates divide by.
fn check_optimizer_flags(args: &Args, planner: &str) -> anyhow::Result<u64> {
    anyhow::ensure!(
        args.query_log.is_none(),
        "--planner {planner} doesn't take --query-log: query logs have no label hints"
    );
    anyhow::ensure!(
        args.prometheus_url.is_none(),
        "--planner {planner} takes labels from the config, not --prometheus-url"
    );
    anyhow::ensure!(
        !args.enable_punting && args.range_duration_ms == 0 && args.step_ms == 0,
        "--enable-punting, --range-duration-ms and --step-ms don't apply to --planner {planner}"
    );
    anyhow::ensure!(
        args.clickhouse_url.is_none() && args.clickhouse_database.is_none(),
        "--clickhouse-url and --clickhouse-database don't apply to --planner {planner}"
    );
    args.data_ingestion_interval_ms
        .filter(|&ms| ms > 0)
        .ok_or_else(|| {
            anyhow::anyhow!("--planner {planner} requires a positive --data-ingestion-interval-ms")
        })
}

fn run_milp(args: &Args) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(
            args.query_language,
            QueryLanguage::promql | QueryLanguage::sql
        ),
        "--planner milp supports only --query-language promql or sql"
    );
    let scrape_interval_ms = check_optimizer_flags(args, "milp")?;
    let config_path = args
        .input_config
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--planner milp requires --input_config"))?;
    let yaml = std::fs::read_to_string(config_path)?;
    let (promql, sql): (ControllerConfig, SQLControllerConfig);
    let config = if args.query_language == QueryLanguage::sql {
        sql = serde_yaml::from_str(&yaml)?;
        MilpConfig::Sql(&sql)
    } else {
        promql = serde_yaml::from_str(&yaml)?;
        MilpConfig::Promql(&promql)
    };
    // Fail before solving: the plan is always written.
    reject_unwritable_queries(config)?;
    let plan = plan_milp(
        config,
        &MilpInputs {
            workload_facts: args
                .workload_facts
                .as_deref()
                .expect("clap requires --workload-facts with --planner milp"),
            atomic_costs: args
                .atomic_costs
                .as_deref()
                .expect("clap requires --atomic-costs with --planner milp"),
            saturation_dir: args
                .saturation_dir
                .as_deref()
                .expect("clap requires --saturation-dir with --planner milp"),
            scrape_interval_ms,
            w_cpu: args.w_cpu,
            w_mem: args.w_mem,
            allow_undeployable_families: false,
        },
    )?;
    plan_to_planner_output(config, &plan.workload, &plan.solution)?
        .write_to_dir(&args.output_dir)?;
    Ok(())
}

fn run_saved(args: &Args) -> anyhow::Result<()> {
    let scrape_interval_ms = check_optimizer_flags(args, "saved")?;
    anyhow::ensure!(
        args.input_config.is_none(),
        "--planner saved reads its queries from --workload-table, not --input_config"
    );
    saved_plan_output(
        args.workload_table
            .as_deref()
            .expect("clap requires --workload-table with --planner saved"),
        args.plan
            .as_deref()
            .expect("clap requires --plan with --planner saved"),
        &PlanSelector {
            method: args.method.as_deref().expect("clap requires --method"),
            weights: args.weights.as_deref().expect("clap requires --weights"),
            sla_ms: args.sla_ms,
        },
        args.query_language,
        scrape_interval_ms,
    )?
    .write_to_dir(&args.output_dir)?;
    Ok(())
}
