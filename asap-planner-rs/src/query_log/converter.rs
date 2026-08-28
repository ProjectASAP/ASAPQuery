use asap_types::enums::CleanupPolicy;

use crate::config::input::{AggregateCleanupConfig, ControllerConfig, QueryGroup};

use super::frequency::{InstantQueryInfo, RangeQueryInfo};

/// Build a `ControllerConfig` from extracted instant and range queries.
///
/// Each query becomes its own `QueryGroup` (one query per group, no SLA fields needed).
pub fn to_controller_config(
    instants: Vec<InstantQueryInfo>,
    ranges: Vec<RangeQueryInfo>,
) -> ControllerConfig {
    let mut query_groups: Vec<QueryGroup> = Vec::new();

    for info in instants {
        query_groups.push(QueryGroup {
            id: None,
            queries: vec![info.query],
            repetition_delay_ms: info.repetition_delay_ms,
            controller_options: Default::default(),
            step_ms: None,
            range_duration_ms: None,
        });
    }

    for info in ranges {
        query_groups.push(QueryGroup {
            id: None,
            queries: vec![info.query],
            repetition_delay_ms: info.repetition_delay_ms,
            controller_options: Default::default(),
            step_ms: Some(info.step_ms),
            range_duration_ms: Some(info.range_duration_ms),
        });
    }

    ControllerConfig {
        query_groups,
        windowing: None,
        sketch_parameters: None,
        aggregate_cleanup: Some(AggregateCleanupConfig {
            policy: Some(CleanupPolicy::ReadBased),
        }),
        metrics: None,
        existing_streaming_config: None,
        existing_inference_config: None,
    }
}
