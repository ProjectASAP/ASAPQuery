use crate::utilities;
pub use concurrent_queue::ConcurrentQueue;
use csv::Reader;
use flate2::read::GzDecoder;
use lazy_static::lazy_static;
use prometheus::{register_gauge_vec, GaugeVec};
use std::fs::File;
use std::io::BufReader;
use std::thread;
use std::time::Duration;
use tracing::{debug, info};

const FILENAME_PARTS_2021: [&str; 2] = ["MSResource_", ".csv.gz"];
const FILENAME_PARTS_2022: [&str; 2] = ["MSMetrics_", ".csv.gz"];

const DATA_QUEUE_CAP: usize = 400_000;
const QUEUE_POLL_INTERVAL_MS: u64 = 250;
const CSV_DELIMITER: u8 = b',';
const LABELS: [&str; 3] = ["ms_name", "ms_instance_id", "node_id"];

type CsvGzReader<File> = Reader<GzDecoder<BufReader<File>>>;
type BoxedErr = Box<dyn std::error::Error + Send + Sync + 'static>;
/// Struct for holding fields after deserialization
/// for both 2021 and 2022
#[derive(Debug, serde::Deserialize)]
pub struct MsResourceCsvFields {
    #[serde(rename = "", skip)]
    _trace: u64,

    #[serde(rename = "timestamp")]
    timestamp: u64,

    #[serde(rename = "nodeid")]
    node_id: String,

    #[serde(rename = "msname")]
    ms_name: String,

    #[serde(rename = "msinstanceid")]
    ms_instance_id: String,

    #[serde(alias = "instance_cpu_usage", alias = "cpu_utilization")]
    cpu_usage: Option<f64>,

    #[serde(alias = "instance_memory_usage", alias = "memory_utilization")]
    memory_usage: Option<f64>,
}

lazy_static! {
    pub static ref MS_RESOURCE_DATA_QUEUE: ConcurrentQueue<MsResourceCsvFields> =
        ConcurrentQueue::bounded(DATA_QUEUE_CAP);
    pub static ref CPU_USAGE: GaugeVec = register_gauge_vec!(
        "alibaba_microservice_cpu_usage",
        "Cpu usages for microservices by alibaba nodes",
        &LABELS,
    )
    .unwrap();
    pub static ref MEMORY_USAGE: GaugeVec = register_gauge_vec!(
        "alibaba_microservice_memory_usage",
        "Memory usages for microservices by alibaba nodes",
        &LABELS,
    )
    .unwrap();
}

/// @brief Gets the filename for the MsResource csv data based on the year
/// and the index number
///
/// @param[in] year     The year of the trace data. Supported values are 2021
///                     and 2022
/// @param[in] index_no The index of the csv file
///
/// @return A String of the filename based on the data year and index num
fn get_filename(year: u32, index_no: u16) -> String {
    let mut filename: String = String::new();
    let prefix: &str;
    let suffix: &str;
    let index: &str = &format!("{}", index_no);

    match year {
        2021 => {
            prefix = FILENAME_PARTS_2021[0];
            suffix = FILENAME_PARTS_2021[1];
        }
        2022 => {
            prefix = FILENAME_PARTS_2022[0];
            suffix = FILENAME_PARTS_2022[1];
        }
        _ => {
            panic!("Invalid year: {}", year);
        }
    }
    filename.push_str(prefix);
    filename.push_str(index);
    filename.push_str(suffix);

    filename
}

/// @brief Gets a csv reader for MsResource data
///
/// @param[in] input_dir The directory containing the csv file
/// @param[in] year      Which trace data year to create the reader for.
///                      supported years are 2021 and 2022
/// @param[in] index     The index of the csv file
///
/// @return A Result type containing either the reader or an Error if the file
///         cannot be found
pub fn get_reader(input_dir: &str, year: u32, index: u16) -> Result<CsvGzReader<File>, BoxedErr> {
    use csv::ReaderBuilder;
    use std::path::Path;

    let filename: String = get_filename(year, index);
    let file_path = Path::new(input_dir).join(&filename);
    let fd: File = File::open(file_path)?;
    let buf_rdr: BufReader<File> = BufReader::new(fd);
    let gz_decoder: GzDecoder<BufReader<File>> = GzDecoder::new(buf_rdr);

    let csv_rdr: CsvGzReader<File> = ReaderBuilder::new()
        .delimiter(CSV_DELIMITER)
        .flexible(true)
        .has_headers(true)
        .from_reader(gz_decoder);

    Ok(csv_rdr)
}

/// @brief Routine for reading MSResource csv data and enqueuing it
///
/// @param[in] input_dir  The input directory containing the csv file
/// @param[in] all_parts  Whether or not to read all csv files in the
///                       directory, starting from part 0. Once a file
///                       cannot be found, this will return. This should
///                       be false if a part_index is given.
/// @param[in] part_index The part index for a single csv file to use as
///                       the data source. This should be None if all_parts
///                       is true.
/// @param[in] year       The year of the trace data. Supported values are
///                       2021 and 2022
///
/// @pre All csv files are uncompressed
/// @pre If all_parts is specified, at least part 0 must exist
/// @pre Either all_parts is true and part_index is None, or all_parts is
///      false and part_index is Some(part)
pub fn read_and_queue(
    input_dir: &str,
    all_parts: bool,
    part_index: Option<u16>,
    year: u32,
) -> Result<(), BoxedErr> {
    let mut part: u16 = 0;
    if !all_parts {
        part = part_index.unwrap();
    }

    while let Ok(mut rdr) = get_reader(input_dir, year, part) {
        let csv_iter = rdr.deserialize();
        for csv_line in csv_iter {
            while MS_RESOURCE_DATA_QUEUE.is_full() {
                thread::sleep(Duration::from_millis(QUEUE_POLL_INTERVAL_MS));
            }
            let parsed_line: MsResourceCsvFields = csv_line?;
            let _ = MS_RESOURCE_DATA_QUEUE.push(parsed_line);
        }
        part += 1;
        if !all_parts {
            break;
        }
    } // No more files to read, or couldn't find initial file

    if part == 0 {
        // Reading always starts at part 0
        panic!(
            "Failed to read initial .csv.gz file. Check that all data files
             are named in the correct format (2021: '{}<idx>{}', 2022: '{}<idx>{}),
             and that the csv files contian the field headers at the top
            ",
            FILENAME_PARTS_2021[0],
            FILENAME_PARTS_2021[1],
            FILENAME_PARTS_2022[0],
            FILENAME_PARTS_2022[1]
        );
    } else {
        MS_RESOURCE_DATA_QUEUE.close();
        Ok(())
    }
}

/// @brief Takes the timestamp of a trace in milliseconds and
///        returns the normalized time as a Duration
///
/// @param[in] time_millis The trace timestamp in milliseconds
///
/// @return The normalized timestamp as a Duration
///
/// @NOTE: Brief check of data suggests no dilation is necessary
///
/// @NOTE: MSResource data from 2022 is not sorted by timestamp whatsoever,
/// sometimes the data is listed in order of decreasing timestamp and other
/// times it's listed in order of increasing timestamp, so the timestamps
/// are modified to work with the exporter before being queued
///
/// @NOTE: SPEEDUP_FACTOR can be set via --speedup CLI argument for faster-than-realtime export
pub fn get_normalized_start_time(time_millis: u64) -> Duration {
    let speedup = crate::alibaba_metrics::SPEEDUP_FACTOR.get().unwrap_or(&1);
    Duration::from_millis(time_millis / speedup)
}

/// @brief Exports a single line from the MS_RESOURCE_DATA_QUEUE
///
/// @param[in] csv_line A parsed line from a MsResource csv file
pub fn export_line(csv_line: MsResourceCsvFields) {
    let label_vals: [&str; 3] = [
        csv_line.ms_name.as_str(),
        csv_line.ms_instance_id.as_str(),
        csv_line.node_id.as_str(),
    ];

    if let Some(cpu_usage) = csv_line.cpu_usage {
        CPU_USAGE.with_label_values(&label_vals).set(cpu_usage);
    }

    if let Some(memory_usage) = csv_line.memory_usage {
        MEMORY_USAGE
            .with_label_values(&label_vals)
            .set(memory_usage);
    }
}

/// @brief Exports lines from the queue until a line is found with a timestamp
///        later than the current runtime. This function will terminate the
///        the program once the queue has both been closed by the reader thread
///        and the queue is empty
pub fn export_from_queue() {
    let elapsed_t: Duration = utilities::get_time_elapsed();
    let check_time =
        |line: &MsResourceCsvFields| get_normalized_start_time(line.timestamp) <= elapsed_t;
    MS_RESOURCE_DATA_QUEUE
        .try_iter()
        .take_while(check_time)
        .for_each(export_line);

    // No more files to read and empty queue
    if MS_RESOURCE_DATA_QUEUE.is_closed() && MS_RESOURCE_DATA_QUEUE.is_empty() {
        info!("No more MSResource data to export, shutting down");
        std::process::exit(0);
    }
}
