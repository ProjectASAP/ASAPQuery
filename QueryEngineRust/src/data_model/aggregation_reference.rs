use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationReference {
    pub aggregation_id: u64,
    /// For circular_buffer policy: keep this many most recent aggregates
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_aggregates_to_retain: Option<u64>,
    /// For read_based policy: remove aggregate after this many reads
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_count_threshold: Option<u64>,
}

impl AggregationReference {
    pub fn new(aggregation_id: u64, num_aggregates_to_retain: Option<u64>) -> Self {
        Self {
            aggregation_id,
            num_aggregates_to_retain,
            read_count_threshold: None,
        }
    }

    pub fn with_read_count_threshold(
        aggregation_id: u64,
        read_count_threshold: Option<u64>,
    ) -> Self {
        Self {
            aggregation_id,
            num_aggregates_to_retain: None,
            read_count_threshold,
        }
    }
}
