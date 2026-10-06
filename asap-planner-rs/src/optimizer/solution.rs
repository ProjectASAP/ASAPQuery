use std::collections::HashMap;

use asap_types::aggregation_config::AggregationConfig;
use asap_types::query_requirements::QueryRequirements;

/// One optimizer demand item: an AQE's requirements at one cadence and SLA pair.
#[derive(Debug, Clone)]
pub struct OptimizerItem {
    /// What the query needs (metric, statistics, range, labels, spatial filter).
    pub requirements: QueryRequirements,

    /// Original query strings from RQEs that contribute to this item.
    /// Preserved for use by the translator when building InferenceConfig.
    pub query_strings: Vec<String>,

    /// Query frequency in Hz: `count * 1000 / t_repeat_ms`.
    /// Used in the MIP objective to convert per-query QueryCost into a cost
    /// rate (cost/sec) commensurate with the continuously-accruing IngestCost.
    /// Represents the total query load from all dashboards independently
    /// hitting the sketch.
    pub query_frequency_hz: f64,

    /// Repetition interval for every RQE contributing to this item, in ms.
    pub t_repeat_ms: u64,

    /// Required accuracy for every RQE contributing to this item.
    pub accuracy_sla: f64,

    /// Maximum query latency (ms) for every RQE contributing to this item;
    /// `None` means no limit.
    pub latency_sla_ms: Option<f64>,
}

/// How an optimizer item is answered from its assigned streaming config.
///
/// Determined by (ingest_type, W vs range_a, sketch algebra) — not a free
/// decision variable. See the compatibility table in the design doc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryMethod {
    /// W = range_a: one completed window covers the query range exactly.
    /// Direct read, no merge or subtract needed.
    Direct,

    /// W < range_a, sketch is mergeable: combine `num_windows` retained
    /// sub-windows at query time (Tumbling or partial-width Sliding).
    /// Cost scales linearly with num_windows.
    Merge { num_windows: u64 },

    /// W < range_a, sketch is subtractable: subtract two prefix-sum checkpoints.
    /// O(1) cost regardless of range_a/W.
    Subtract,
}

/// The assignment of a single optimizer item to a streaming config.
#[derive(Debug, Clone)]
pub struct AQEAssignment {
    pub item: OptimizerItem,

    /// ID of the deployed config that serves this item.
    pub aggregation_id: u64,

    /// ID of the paired key aggregation, for value sketches that can't list
    /// their own keys.
    pub key_aggregation_id: Option<u64>,

    /// How this item's answer is derived from the assigned config.
    pub query_method: QueryMethod,

    /// Estimated cost rate for this assignment: QueryCost(a, g) * `aqe.query_frequency_hz`.
    pub estimated_query_cost_per_sec: f64,
}

/// The output of the optimizer: a complete plan for a given RQE workload.
///
/// Contains the set of streaming configs to deploy and the assignment of every
/// item to one of those configs. A thin translator
/// converts this into `StreamingConfig + InferenceConfig` deployment artifacts.
#[derive(Debug, Clone)]
pub struct OptimizerSolution {
    /// Deployed streaming configs (y_g = 1 in the MIP). Keyed by aggregation_id.
    ///
    /// Private: the only way to add an entry is `register_config`, which
    /// assigns the id. This keeps candidate_gen.rs's placeholder id (0) from
    /// ever reaching a deployed config — see ASAPQuery#564.
    deployed_configs: HashMap<u64, AggregationConfig>,

    /// Next id `register_config` will hand out.
    next_id: u64,

    /// One entry per optimizer item across the full RQE workload.
    pub assignments: Vec<AQEAssignment>,

    /// Estimated steady-state ingestion cost rate across all deployed configs
    /// (Σ_{g: y_g=1} IngestCost(g)).
    pub estimated_ingest_cost_per_sec: f64,

    /// Estimated total cost rate: ingest + query components combined.
    pub estimated_total_cost_per_sec: f64,
}

impl OptimizerSolution {
    /// An empty solution with no assignments or deployed configs yet, ready
    /// to be built up incrementally (e.g. by a solver's assignment loop).
    pub fn empty() -> Self {
        Self {
            deployed_configs: HashMap::new(),
            next_id: 1,
            assignments: Vec::new(),
            estimated_ingest_cost_per_sec: 0.0,
            estimated_total_cost_per_sec: 0.0,
        }
    }

    /// Register a candidate config as deployed: assigns it a fresh unique id
    /// (overwriting whatever placeholder candidate_gen.rs set), stores it,
    /// and returns the id. The only way to populate `deployed_configs`.
    pub fn register_config(&mut self, mut config: AggregationConfig) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        config.aggregation_id = id;
        self.deployed_configs.insert(id, config);
        id
    }

    pub fn deployed_configs(&self) -> &HashMap<u64, AggregationConfig> {
        &self.deployed_configs
    }

    /// Number of optimizer items served by a streaming sketch.
    pub fn num_sketch_served(&self) -> usize {
        self.assignments.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::enums::WindowType;
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::AggregationType;

    fn candidate_config() -> AggregationConfig {
        // aggregation_id: 0, matching candidate_gen.rs's placeholder — the
        // thing register_config must always overwrite (ASAPQuery#564).
        AggregationConfig::new(
            0,
            AggregationType::CountMinSketch,
            "sum".into(),
            HashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60_000,
            60_000,
            WindowType::Tumbling,
            String::new(),
            "test_metric".into(),
            Some(1),
            None,
            None,
            None,
        )
    }

    #[test]
    fn register_config_never_leaves_the_placeholder_id() {
        let mut solution = OptimizerSolution::empty();
        let id = solution.register_config(candidate_config());
        assert_ne!(
            id, 0,
            "register_config must not hand out the placeholder id"
        );
        assert_eq!(solution.deployed_configs()[&id].aggregation_id, id);
    }

    #[test]
    fn register_config_assigns_distinct_ids() {
        let mut solution = OptimizerSolution::empty();
        let id1 = solution.register_config(candidate_config());
        let id2 = solution.register_config(candidate_config());
        assert_ne!(id1, id2);
        assert_eq!(solution.deployed_configs().len(), 2);
    }
}
