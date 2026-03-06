use clap::ValueEnum;
use std::sync::OnceLock;

type BoxedErr = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum MsDataType {
    // BM Node runtime information.
    // It records CPU and memory utilization of 1300+ BM nodes in a production cluster.
    Node,
    // MS runtime information.
    // It records CPU and memory utilization of 90000+ containers for 1300+ MSs in the same production cluster.
    MsResource,
}

pub mod ms_resource;
pub mod node;

// The type of microservice data to export. Should be initialized before any
// reading or exporting begins
pub static EXPORTER_DATA_TYPE: OnceLock<MsDataType> = OnceLock::new();

/// @brief Calls the export_from_queue() function based on runtime initialized
///        EXPORTER_DATA_TYPE
pub fn export_from_queue() {
    match EXPORTER_DATA_TYPE.get().unwrap() {
        MsDataType::Node => node::export_from_queue(),
        MsDataType::MsResource => ms_resource::export_from_queue(),
    }
}

/// @brief Main routine for the thread that will be reading csv data and
/// exporting. This function just uses a match statement to call the reading
/// and exporting routine required by the specified mode
///
/// @param[in] input_dir  The input directory containing csv files
/// @param[in] all_parts  Whether to start from part 0 of csv files and continue
///                       until no more files are found. This should be false if
///                       part_index is Some(part)
/// @param[in] part_index Which csv file part to use as the data source.
///                       This should be None if all_parts is true.
/// @param[in] data_type  The type of data out of the different types of trace
///                       data in the Alibaba micro-services trace data
/// @param[in] data_year  The year of the trace data. Supported values are
///                       2021 and 2022
///
/// @return The result returned by the reader thread.
pub fn reader_thread_routine(
    input_dir: String,
    all_parts: bool,
    part_index: Option<u16>,
    data_type: MsDataType,
    data_year: u32,
) -> Result<(), BoxedErr> {
    use crate::alibaba_metrics::node;
    let _ = EXPORTER_DATA_TYPE.set(data_type);
    let result = match EXPORTER_DATA_TYPE.get().unwrap() {
        MsDataType::Node => node::read_and_queue(&input_dir, all_parts, part_index, data_year),
        MsDataType::MsResource => {
            ms_resource::read_and_queue(&input_dir, all_parts, part_index, data_year)
        }
    };

    result
}
