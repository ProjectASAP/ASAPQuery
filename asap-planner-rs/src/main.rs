use asap_planner::{Controller, RuntimeOptions, SQLController, SQLRuntimeOptions, StreamingEngine};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "asap-planner", about = "ASAP Query Planner")]
struct Args {
    #[arg(long = "input_config")]
    input_config: PathBuf,

    #[arg(long = "output_dir")]
    output_dir: PathBuf,

    #[arg(long = "prometheus_scrape_interval", required = false)]
    prometheus_scrape_interval: Option<u64>,

    #[arg(long = "streaming_engine", value_enum)]
    streaming_engine: EngineArg,

    #[arg(long = "enable-punting", default_value = "false")]
    enable_punting: bool,

    #[arg(long = "range-duration", default_value = "0")]
    range_duration: u64,

    #[arg(long = "step", default_value = "0")]
    step: u64,

    #[arg(long = "query-language", value_enum, default_value = "promql")]
    query_language: QueryLanguageArg,

    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
enum EngineArg {
    Arroyo,
    Flink,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
enum QueryLanguageArg {
    Promql,
    Sql,
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

    match args.query_language {
        QueryLanguageArg::Promql => {
            let scrape_interval = args.prometheus_scrape_interval.ok_or_else(|| {
                anyhow::anyhow!("--prometheus_scrape_interval is required for PromQL mode")
            })?;
            let opts = RuntimeOptions {
                prometheus_scrape_interval: scrape_interval,
                streaming_engine: engine,
                enable_punting: args.enable_punting,
                range_duration: args.range_duration,
                step: args.step,
            };
            let controller = Controller::from_file(&args.input_config, opts)?;
            controller.generate_to_dir(&args.output_dir)?;
        }
        QueryLanguageArg::Sql => {
            let opts = SQLRuntimeOptions {
                streaming_engine: engine,
                query_evaluation_time: None,
            };
            SQLController::from_file(&args.input_config, opts)?
                .generate_to_dir(&args.output_dir)?;
        }
    }

    println!("Generated configs in {}", args.output_dir.display());
    Ok(())
}
