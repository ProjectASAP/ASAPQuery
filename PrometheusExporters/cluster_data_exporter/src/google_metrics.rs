use crate::utilities;
use crate::utilities::*;
use clap::ValueEnum;
use concurrent_queue::ConcurrentQueue;
use csv::Reader;
use flate2::read::GzDecoder;
use lazy_static::lazy_static;
use prometheus::{register_gauge_vec, GaugeVec};
use std::sync::OnceLock;
use std::thread;
use std::time::Duration;
use std::{fs::File, io::BufReader};
use tracing::{debug, info};

type CsvGzReader<File> = Reader<GzDecoder<BufReader<File>>>;

/* Standard labels for google's task resource usage data */
const TRU_LABELS: [&str; 3] = ["job_id", "task_index", "machine_id"];
const TRU_CSV_DELIMITER: u8 = b',';
const DATA_QUEUE_CAP: usize = 400_000; // Max lines in the queue
const CSV_MAX_PART_NO: u16 = 500;

const MICRO_SECONDS_PER_SECOND: u64 = 1_000_000;
const T_OFFSET_SECS: u64 = 600;
const DILATION_FACTOR: u64 = 10; // Factor for scaling time stamps relative to when they are exported

/// Each line of the csv file is serialized into the following struct.
/// The ordering of the struct fields MUST match the order that fields
/// appear in a line of the csv file.
///
/// All fields wrapped in Option<> are not considered mandatory by
/// the schema and, therefore, may be missing from a given trace.
/// The rest of the fields should never be missing, so failure to
/// deserialize will result in an error and program termination
#[derive(Debug, serde::Deserialize)]
pub struct TruCsvFields {
    pub start_time: u64,
    pub _end_time: u64,     // unused, only here for parsing
    pub job_id: String,     // label
    pub task_index: String, // label
    pub machine_id: String, // label
    pub mean_cpu_usage_rate: Option<f64>,
    pub canonical_memory_usage: Option<f64>,
    pub assigned_memory_usage: Option<f64>,
    pub unmapped_page_cache_memory_usage: Option<f64>,
    pub total_page_cache_memory_usage: Option<f64>,
    pub max_memory_usage: Option<f64>,
    pub mean_disk_io_time: Option<f64>,
    pub mean_local_disk_space_used: Option<f64>,
    pub max_cpu_usage: Option<f64>,
    pub max_disk_io_time: Option<f64>,
    pub cycles_per_instruction: Option<f64>,
    pub memory_accesses_per_instruction: Option<f64>,
    pub sample_portion: Option<f64>,
    pub aggregation_type: Option<u8>, // Divides metrics into two
    pub sampled_cpu_usage: Option<f64>,
}

/// @brief An enum for matching the metrics to export with their
/// corresponding prometheus gauges
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum TruMetrics {
    MeanCpuUsageRate,
    CanonicalMemoryUsage,
    AssignedMemoryUsage,
    UnmappedPageCacheMemoryUsage,
    TotalPageCacheMemoryUsage,
    MaxMemoryUsage,
    MeanDiskIoTime,
    MeanLocalDiskSpaceUsed,
    MaxCpuUsage,
    MaxDiskIoTime,
    CyclesPerInstruction,
    MemoryAccessesPerInstruction,
    SamplePortion,
    SampledCpuUsage,
}

/// @brief A tuple struct representing two of the same prometheus metrics,
/// but partitioned by their aggregation type. Index number directly
/// corresponds to the aggregation type, i.e. i=0 => aggregation_type=0
pub struct GaugePair(GaugeVec, GaugeVec);

impl GaugePair {
    /// @brief Create and register both GaugeVecs in the GaugePair to the
    /// default registry.
    ///
    /// @param[in] base_name The string used as the base of both metrics
    /// names as seen by prometheus, where aggregation type will be appended
    ///
    /// @param[in] base_help The string used as the base of both metrics
    /// help strings when scraped by prometheus. Aggregation type is
    /// appended
    fn new(base_name: &str, base_help: &str) -> GaugePair {
        let mut name_0 = String::from(base_name);
        name_0.push_str("_0");
        let mut help_0 = String::from(base_help);
        help_0.push_str(" (aggregation_type=0)");
        let gauge_0 = register_gauge_vec!(name_0.as_str(), help_0.as_str(), &TRU_LABELS).unwrap();

        let mut name_1 = String::from(base_name);
        name_1.push_str("_1");
        let mut help_1 = String::from(base_help);
        help_1.push_str(" (aggregation_type=1)");
        let gauge_1 = register_gauge_vec!(name_1.as_str(), help_1.as_str(), &TRU_LABELS).unwrap();

        GaugePair(gauge_0, gauge_1)
    }

    /// @brief Retrieve a static reference to the gauge from the pair for
    /// the given aggregation type
    ///
    /// @param[in] self             Statically defined GaugePair
    /// @param[in] aggregation_type 0 or 1 (The aggregation type)
    fn get(&'static self, aggregation_type: u8) -> &'static GaugeVec {
        match aggregation_type {
            0 => &self.0,
            1 => &self.1,
            _ => panic!("Invalid index into gauge vec"),
        }
    }
}

/// List of metrics to export from the google task resource usage data
pub static GOOGLE_METRICS: OnceLock<Vec<TruMetrics>> = OnceLock::new();

lazy_static! {
    /// Queue for parsed csv lines
    pub static ref GOOGLE_DATA_QUEUE: ConcurrentQueue<TruCsvFields> = ConcurrentQueue::bounded(DATA_QUEUE_CAP);

    /* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
         *                          ALL METRICS                              *
         *                                                                   *
         *  Each static reference is a GaugePair corresponding to a single   *
         *  metric. Each element of the pair corresponds to an aggregation   *
         *  type of 0 or 1. When the aggregation type is missing from a      *
         *  trace the aggregation type defaults to 0                         *
         *                                                                   *
         * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
    pub static ref MEAN_CPU_USAGE_RATE_PAIR: GaugePair = GaugePair::new(
        "google_mean_cpu_usage_rate", "Mean cpu usage rate by google machines",
    );

    pub static ref CANONICAL_MEMORY_USAGE_PAIR: GaugePair = GaugePair::new(
       "google_canonical_memory_usage", "Canonical memory usage by google cluster machines",
    );

    pub static ref ASSIGNED_MEMORY_USAGE_PAIR: GaugePair = GaugePair::new(
        "google_assigned_memory_usage", "Assigned memory usage for google cluster machines",
    );

    pub static ref UNMAPPED_PAGE_CACHE_MEMORY_USAGE_PAIR: GaugePair = GaugePair::new(
        "google_unmapped_page_cache_memory_usage", "Unmapped page cache memory usage for google cluster machines",
    );

    pub static ref TOTAL_PAGE_CACHE_MEMORY_USAGE_PAIR: GaugePair = GaugePair::new(
        "google_total_page_cache_memory_usage", "Total page cache memory usage for google cluster machines",
    );

    pub static ref MAX_MEMORY_USAGE_PAIR: GaugePair = GaugePair::new(
        "google_max_memory_usage", "Maximum memory usage by google cluster machines",
    );

    pub static ref MEAN_DISK_IO_TIME_PAIR: GaugePair = GaugePair::new(
        "google_mean_disk_io_time", "Mean disk I/O time for google cluster machines",
    );

    pub static ref MEAN_LOCAL_DISK_SPACE_USED_PAIR: GaugePair = GaugePair::new(
        "google_mean_local_disk_space_used", "Mean local disk space used by google cluster machines",
    );

    pub static ref MAX_CPU_USAGE_PAIR: GaugePair = GaugePair::new(
        "google_max_cpu_usage", "Maximum cpu usage for google cluster machines",
    );

    pub static ref MAX_DISK_IO_TIME_PAIR: GaugePair = GaugePair::new(
        "google_max_disk_io_time", "Maximum disk I/O time for google cluster machines",
    );

    pub static ref CYCLES_PER_INSTRUCTION_PAIR: GaugePair = GaugePair::new(
        "google_cycles_per_instruction", "Cycles per instruction for google cluster machines",
    );

    pub static ref MEMORY_ACCESSES_PER_INSTRUCTION_PAIR: GaugePair = GaugePair::new(
        "google_memory_accesses_per_instruction", "Memory accesses per instruction for google cluster machines",
    );

    pub static ref SAMPLE_PORTION_PAIR: GaugePair = GaugePair::new(
        "google_sample_portion", "Sample portion for google cluster machines",
    );

    pub static ref SAMPLED_CPU_USAGE_PAIR: GaugePair = GaugePair::new(
        "google_sampled_cpu_usage", "Sampled cpu usage for google cluster machines",
    );
}

/// @brief Given the part number, create a String for the filename.
///
/// @param[in] part    The csv part number such that: part ∈ [0, 500]
/// @param[in] gzipped Whether or not .gz should be appended to the filename
///
/// @return The csv filename as a String, in the form:
///             <part-00xxx-of-00500.csv(.gz)>
pub fn get_csv_filename(part: u16, gzipped: bool) -> String {
    const TRU_CSV_PATH_PARTS: [&str; 4] = ["part-", "00000", "-of-00500.csv", ".gz"];

    let mut filename = String::new();
    let part_name_str: String;

    if part < 10 {
        part_name_str = format!("0000{}", part);
    } else if (10..100).contains(&part) {
        part_name_str = format!("000{}", part);
    } else if (100..=CSV_MAX_PART_NO).contains(&part) {
        part_name_str = format!("00{}", part);
    } else {
        panic!(
            "Invalid part number: {} => part must be between 0 and 500",
            part
        );
    }

    filename.push_str(TRU_CSV_PATH_PARTS[0]);
    filename.push_str(&part_name_str);
    filename.push_str(TRU_CSV_PATH_PARTS[2]);

    if gzipped {
        filename.push_str(TRU_CSV_PATH_PARTS[3]);
    }

    filename
}

/// @brief Creates a new csv reader wrapped around a gzip decoder which
/// streams data from the underlying file
///
/// @param[in] input_dir The directory containing gzipped csv files
/// @param[in] part      The part number out of the total number of csv files
///
/// @return The configured reader
fn get_reader(input_dir: &str, part: u16) -> Result<CsvGzReader<File>, BoxedErr> {
    use csv::ReaderBuilder;
    use flate2::read::GzDecoder;
    use std::fs::File;
    use std::io::BufReader;
    use std::path::Path;

    let filename: String = get_csv_filename(part, true);
    let file_path = Path::new(input_dir).join(&filename);
    let fd: File = File::open(file_path)?;
    let buf_rdr = BufReader::new(fd);
    let gz_decoder = GzDecoder::new(buf_rdr);

    let csv_rdr: CsvGzReader<File> = ReaderBuilder::new()
        .delimiter(TRU_CSV_DELIMITER)
        .flexible(true)
        .has_headers(false)
        .from_reader(gz_decoder);

    Ok(csv_rdr)
}

/// @brief Main routine of the helper (reader) thread.
///
/// The purpose of the thread is to handle all of the work involved in
///     reading and enqueuing lines from the csv.gz file for the
///     main thread to then pop and export on scrape
///
/// @param[in] input_dir  The path to the directory containing the csv.gz files
/// @param[in] all_parts  Whether or not to run the exporter on all 500 parts of
///                       the task resource usage csv data. Running in this mode
///                       and not providing all 500 parts will cause the reader
///                       thread to panic. If this option is true, part_index
///                       should be None
/// @param[in] part_index Specify a single part (out of 500) to read csv data
///                       from. The reader thread will stop after reading this
///                       single file. If part_index is not None, then all_parts
///                       should be false
/// @param[in] metrics    The list of metrics, or csv fields, for the exporter
///                       to expose to prometheus. At least one must be given
///
/// @pre All csv files are expected to be of the form:
///                 "part-00xxx-of-00500.csv.gz"
pub fn reader_thread_routine(
    input_dir: String,
    all_parts: bool,
    part_index: Option<u16>,
    metrics: Vec<TruMetrics>,
) -> Result<(), BoxedErr> {
    const QUEUE_POLL_INTERVAL_MS: u64 = 250;
    GOOGLE_METRICS.set(metrics).unwrap();
    let mut part: u16 = 0_u16;

    if !all_parts {
        part = part_index.unwrap();
    }

    while let Ok(mut rdr) = get_reader(&input_dir, part) {
        let csv_iter = rdr.deserialize();
        for csv_line in csv_iter {
            while GOOGLE_DATA_QUEUE.is_full() {
                thread::sleep(Duration::from_millis(QUEUE_POLL_INTERVAL_MS));
            }
            let parsed_line: TruCsvFields = csv_line?;
            let _ = GOOGLE_DATA_QUEUE.push(parsed_line);
        }
        part += 1;

        if !all_parts || part > CSV_MAX_PART_NO {
            break;
        }
    }

    // Never read any parts or all parts was specified and we never read all 500
    // parts of the csv data
    if part == 0 || (all_parts && part <= CSV_MAX_PART_NO) {
        panic!(
            "Failed to read initial .csv.gz file. Check that all data files
             are named in the correct format ('part-?????-of-00500.csv.gz').
             If running with --all-parts, ensure all 500 parts exist in the
             input directory.
            "
        );
    } else {
        GOOGLE_DATA_QUEUE.close();
        Ok(())
    }
}

/// @brief: Converts the start time of a job into seconds and normalizes it
///
/// From pg.2 of the schema doc:
///    "Each record has a timestamp, which is in microseconds since 600
///     seconds before the beginning of the trace period, and recorded as a
///     64 bit integer (i.e., an event 20 second after the start of the
///     trace would have a timestamp=620s)."
///
/// @param[in] time_micros The event start time in microseconds,
///                        offset by T_OFFSET_SECS (600s)
///
/// @return A duration representing the dilated trace start time in seconds
///            after subtracting the offset
pub fn get_normalized_start_time(time_micros: u64) -> Duration {
    let time_secs = time_micros / MICRO_SECONDS_PER_SECOND;
    Duration::from_secs((time_secs - T_OFFSET_SECS) * DILATION_FACTOR)
}

/// @brief Given a single parsed line from the csv file, update all gauges
/// corresponding to the metrics in the list
///
/// @param[in] csv_line A parsed line from the csv file containing label
///                         values and metric data to export
pub fn export_line(csv_line: TruCsvFields) {
    let metrics = GOOGLE_METRICS.get().unwrap();
    let label_vals: [&str; 3] = [
        csv_line.job_id.as_str(),
        csv_line.task_index.as_str(),
        csv_line.machine_id.as_str(),
    ];

    let aggregation_type = csv_line.aggregation_type.unwrap_or(0_u8);

    for metric in metrics {
        let curr_gauge: &'static GaugeVec;
        let wrapped_value: Option<f64>;

        (curr_gauge, wrapped_value) = match metric {
            TruMetrics::MeanCpuUsageRate => (
                MEAN_CPU_USAGE_RATE_PAIR.get(aggregation_type),
                csv_line.mean_cpu_usage_rate,
            ),
            TruMetrics::CanonicalMemoryUsage => (
                CANONICAL_MEMORY_USAGE_PAIR.get(aggregation_type),
                csv_line.canonical_memory_usage,
            ),
            TruMetrics::AssignedMemoryUsage => (
                ASSIGNED_MEMORY_USAGE_PAIR.get(aggregation_type),
                csv_line.assigned_memory_usage,
            ),
            TruMetrics::UnmappedPageCacheMemoryUsage => (
                UNMAPPED_PAGE_CACHE_MEMORY_USAGE_PAIR.get(aggregation_type),
                csv_line.unmapped_page_cache_memory_usage,
            ),
            TruMetrics::TotalPageCacheMemoryUsage => (
                TOTAL_PAGE_CACHE_MEMORY_USAGE_PAIR.get(aggregation_type),
                csv_line.total_page_cache_memory_usage,
            ),
            TruMetrics::MaxMemoryUsage => (
                MAX_MEMORY_USAGE_PAIR.get(aggregation_type),
                csv_line.max_memory_usage,
            ),
            TruMetrics::MeanDiskIoTime => (
                MEAN_DISK_IO_TIME_PAIR.get(aggregation_type),
                csv_line.mean_disk_io_time,
            ),
            TruMetrics::MeanLocalDiskSpaceUsed => (
                MEAN_LOCAL_DISK_SPACE_USED_PAIR.get(aggregation_type),
                csv_line.mean_local_disk_space_used,
            ),
            TruMetrics::MaxCpuUsage => (
                MAX_CPU_USAGE_PAIR.get(aggregation_type),
                csv_line.max_cpu_usage,
            ),
            TruMetrics::MaxDiskIoTime => (
                MAX_DISK_IO_TIME_PAIR.get(aggregation_type),
                csv_line.max_disk_io_time,
            ),
            TruMetrics::CyclesPerInstruction => (
                CYCLES_PER_INSTRUCTION_PAIR.get(aggregation_type),
                csv_line.cycles_per_instruction,
            ),
            TruMetrics::MemoryAccessesPerInstruction => (
                MEMORY_ACCESSES_PER_INSTRUCTION_PAIR.get(aggregation_type),
                csv_line.memory_accesses_per_instruction,
            ),
            TruMetrics::SamplePortion => (
                SAMPLE_PORTION_PAIR.get(aggregation_type),
                csv_line.sample_portion,
            ),
            TruMetrics::SampledCpuUsage => (
                SAMPLED_CPU_USAGE_PAIR.get(aggregation_type),
                csv_line.sampled_cpu_usage,
            ),
        };

        if let Some(metric_value) = wrapped_value {
            // Set the metric, unless it was missing
            curr_gauge.with_label_values(&label_vals).set(metric_value);
        }
    }
}

/// @brief Exports all parsed CSV lines from the queue
///
/// This function will continue popping lines from the queue until it
/// pops one with a start timestamp which should be exported later in time.
/// This line will be saved in FUTURE_LINE and then exported on the next
/// scrape for which the program runtime <= start time
pub fn export_from_queue() {
    let elapsed_t: Duration = utilities::get_time_elapsed();
    let check_time = |line: &TruCsvFields| get_normalized_start_time(line.start_time) <= elapsed_t;

    GOOGLE_DATA_QUEUE
        .try_iter()
        .take_while(check_time)
        .for_each(export_line);

    if GOOGLE_DATA_QUEUE.is_closed() && GOOGLE_DATA_QUEUE.is_empty() {
        info!("No more task resource usage to export, shutting down");
        std::process::exit(0);
    }
}
