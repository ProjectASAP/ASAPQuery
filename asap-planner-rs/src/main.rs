use asap_planner::{Controller, RuntimeOptions, StreamingEngine};
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

    /// Path to a metrics config YAML (required when using --query-log).
    #[arg(long = "metrics-config", requires = "query_log")]
    metrics_config: Option<PathBuf>,

    #[arg(long = "output_dir")]
    output_dir: PathBuf,

    #[arg(long = "prometheus_scrape_interval")]
    prometheus_scrape_interval: u64,

    #[arg(long = "streaming_engine", value_enum)]
    streaming_engine: EngineArg,

    #[arg(long = "enable-punting", default_value = "false")]
    enable_punting: bool,

    #[arg(long = "range-duration", default_value = "0")]
    range_duration: u64,

    #[arg(long = "step", default_value = "0")]
    step: u64,

    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
enum EngineArg {
    Arroyo,
    Flink,
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
        EngineArg::Arroyo => StreamingEngine::Arroyo,
        EngineArg::Flink => StreamingEngine::Flink,
    };

    let opts = RuntimeOptions {
        prometheus_scrape_interval: args.prometheus_scrape_interval,
        streaming_engine: engine,
        enable_punting: args.enable_punting,
        range_duration: args.range_duration,
        step: args.step,
    };

    let controller = match (args.input_config, args.query_log) {
        (Some(config_path), None) => Controller::from_file(&config_path, opts)?,
        (None, Some(log_path)) => {
            let metrics_path = args
                .metrics_config
                .expect("--metrics-config is required when using --query-log");
            Controller::from_query_log(&log_path, &metrics_path, opts)?
        }
        _ => anyhow::bail!("exactly one of --input_config or --query-log must be provided"),
    };

    controller.generate_to_dir(&args.output_dir)?;
    println!("Generated configs in {}", args.output_dir.display());
    Ok(())
}
