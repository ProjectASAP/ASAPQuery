use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_gauge, register_histogram, Counter,
    CounterVec, Gauge, Histogram,
};

lazy_static! {
    /// Number of live series in the PromSketchStore.
    pub static ref SERIES_TOTAL: Gauge =
        register_gauge!("promsketch_series_total", "Number of live series in store").unwrap();

    /// Total raw samples successfully inserted.
    pub static ref SAMPLES_INGESTED_TOTAL: Counter =
        register_counter!("promsketch_samples_ingested_total", "Total raw samples inserted").unwrap();

    /// Failed sample insertions.
    pub static ref INGEST_ERRORS_TOTAL: Counter =
        register_counter!("promsketch_ingest_errors_total", "Failed sample insertions").unwrap();

    /// Time to flush a batch of raw samples.
    pub static ref INGEST_BATCH_DURATION: Histogram =
        register_histogram!(
            "promsketch_ingest_batch_duration_seconds",
            "Time to flush a batch of raw samples",
            vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
        ).unwrap();

    /// Sketch queries executed, labeled by result (hit or miss).
    pub static ref SKETCH_QUERIES_TOTAL: CounterVec =
        register_counter_vec!(
            "promsketch_sketch_queries_total",
            "Sketch queries executed",
            &["result"]
        ).unwrap();

    /// Sketch query evaluation latency.
    pub static ref SKETCH_QUERY_DURATION: Histogram =
        register_histogram!(
            "promsketch_sketch_query_duration_seconds",
            "Sketch query eval latency",
            vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
        ).unwrap();
}
