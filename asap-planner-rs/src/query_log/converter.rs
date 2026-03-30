use serde::Deserialize;

use crate::config::input::{
    AggregateCleanupConfig, ControllerConfig, MetricDefinition, QueryGroup,
};

use super::frequency::{InstantQueryInfo, RangeQueryInfo};

/// Subset of ControllerConfig used when loading from a metrics-only YAML file.
#[derive(Deserialize)]
pub struct MetricsConfig {
    pub metrics: Vec<MetricDefinition>,
}

/// Build a `ControllerConfig` from extracted instant and range queries plus a metrics definition.
///
/// Each query becomes its own `QueryGroup` (one query per group, no SLA fields needed).
pub fn to_controller_config(
    instants: Vec<InstantQueryInfo>,
    ranges: Vec<RangeQueryInfo>,
    metrics: Vec<MetricDefinition>,
) -> ControllerConfig {
    let mut query_groups: Vec<QueryGroup> = Vec::new();

    for info in instants {
        query_groups.push(QueryGroup {
            id: None,
            queries: vec![info.query],
            repetition_delay: info.repetition_delay,
            controller_options: Default::default(),
            step: None,
            range_duration: None,
        });
    }

    for info in ranges {
        query_groups.push(QueryGroup {
            id: None,
            queries: vec![info.query],
            repetition_delay: info.repetition_delay,
            controller_options: Default::default(),
            step: Some(info.step),
            range_duration: Some(info.range_duration),
        });
    }

    ControllerConfig {
        query_groups,
        metrics,
        sketch_parameters: None,
        aggregate_cleanup: Some(AggregateCleanupConfig {
            policy: Some("read_based".to_string()),
        }),
    }
}
