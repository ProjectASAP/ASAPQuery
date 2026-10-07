use asap_types::query_requirements::QueryRequirements;

/// One optimizer demand item: an AQE's requirements at one cadence and SLA pair.
#[derive(Debug, Clone)]
pub struct OptimizerItem {
    /// What the query needs (metric, statistics, range, labels, spatial filter).
    pub requirements: QueryRequirements,

    /// Original query strings from RQEs that contribute to this item.
    /// Used to build InferenceConfig query configs.
    pub query_strings: Vec<String>,

    /// Query frequency in Hz: `count * 1000 / t_repeat_ms`.
    /// Represents the total query load from all dashboards independently
    /// hitting the sketch.
    pub query_frequency_hz: f64,

    /// Leaf occurrences merged into this item, across all RQEs.
    pub occurrences: usize,

    /// Repetition interval for every RQE contributing to this item, in ms.
    pub t_repeat_ms: u64,

    /// Required accuracy for every RQE contributing to this item.
    pub accuracy_sla: f64,

    /// Maximum query latency (ms) for every RQE contributing to this item;
    /// `None` means no limit.
    pub latency_sla_ms: Option<f64>,
}
