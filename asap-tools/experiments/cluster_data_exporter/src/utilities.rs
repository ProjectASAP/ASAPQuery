use crate::alibaba_metrics::*;
use crate::google_metrics::*;
use clap::{ArgGroup, Parser, Subcommand, ValueEnum};
use lazy_static::lazy_static;
use std::time::{Duration, Instant};

pub type BoxedErr = Box<dyn std::error::Error + Send + Sync + 'static>;

lazy_static! {
    /// An instant in time to roughly represent the start time of the exporter
    /// This is used as the reference point for calculating how much time has
    /// elapsed, and therefore which traces should be exported during a scrape
    /// and which ones should be held onto until later
    pub static ref T_START: Instant = Instant::now();
}

/// @brief Returns the time since T_START as a Duration
///
/// @return Duration since the Instant defined by T_START
///
/// @note Since T_START isn't initialized until it is referenced for the first
///       time, so if this function is called before T_START is ever referenced
///       then T_START will be initialized here with Duration::Zero returned
pub fn get_time_elapsed() -> Duration {
    T_START.elapsed()
}

#[derive(Debug, Clone, ValueEnum)]
pub enum Provider {
    Google,
    Alibaba,
}

#[derive(Parser, Debug)]
#[command(name = "cluster_data_exporter", version, about)]
#[command(subcommand_required = true)]
pub struct Cli {
    #[arg(short, long, aliases = ["input, in, dir, input_dir"])]
    #[arg(required = true)]
    pub input_directory: String,

    #[arg(short, long)]
    #[arg(required = true)]
    pub port: u16,

    #[command(subcommand)]
    pub provider: ProviderCmd,
}

#[derive(Subcommand, Debug)]
pub enum ProviderCmd {
    /// Run the exporter on google task resource usage data
    #[command(group(ArgGroup::new("csv-parts")
                        .args(&["all_parts", "part_index"])
                        .required(true))
    )]
    Google {
        #[arg(long, value_enum, value_delimiter = ',', num_args = 1..)]
        #[arg(required = true, require_equals = true)]
        metrics: Vec<TruMetrics>,

        #[arg(long, group = "csv-parts", alias = "all")]
        all_parts: bool,

        #[arg(long, group = "csv-parts", aliases = ["part", "index"])]
        #[arg(require_equals = true)]
        part_index: Option<u16>,
    },

    /// Run the exporter on Alibaba microservice data
    #[command(group(ArgGroup::new("csv-parts")
                .args(&["all_parts", "part_index"])
                .required(true))
    )]
    Alibaba {
        /// The type of microservice data to use
        #[arg(long, value_enum)]
        #[arg(required = true, require_equals = true)]
        data_type: MsDataType,

        /// Which year the microservice data comes from
        #[arg(long)]
        #[arg(required = true, require_equals = true)]
        #[arg(value_parser = clap::value_parser!(u32).range(2021..=2022))]
        data_year: u32,

        /// Whether or not to run the exporter starting on part 0 of the csv
        /// files and continue sequentially until no more files are found.
        /// This option is mutually exclusive with --part-index
        #[arg(long, group = "csv-parts", alias = "all")]
        all_parts: bool,

        /// Specify a single csv file to use as trace data.
        /// This option is mutually exclusive with --all-parts  
        #[arg(long, group = "csv-parts", aliases = ["part", "index"])]
        #[arg(require_equals = true)]
        part_index: Option<u16>,
    },
}
