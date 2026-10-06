//! Externally provided workload facts for the MILP planner: per metric, the
//! cardinality of each label set in use. The entry for all of a metric's
//! labels is its series count. Labels come from the workload's `metrics:`
//! hints and the scrape interval from the caller.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use rqe_optimizer::{LabelSet, MetricFacts, Millis, WorkloadFacts};
use serde::Deserialize;
use thiserror::Error;

use crate::config::input::MetricDefinition;

#[derive(Debug, Error)]
pub enum WorkloadFactsError {
    #[error("failed to read workload facts '{path}': {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse workload facts: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("duplicate facts for metric {0:?}")]
    DuplicateMetric(String),
    #[error("metric {metric:?}: duplicate cardinality for labels {labels:?}")]
    DuplicateLabels { metric: String, labels: LabelSet },
    #[error("metric {0:?} has facts but no `metrics:` hint giving its labels")]
    MetricWithoutHint(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FactsFile {
    metrics: Vec<MetricEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricEntry {
    metric: String,
    groups: Vec<GroupEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupEntry {
    labels: Vec<String>,
    cardinality: u64,
}

pub fn load_workload_facts(
    path: &Path,
    hints: &[MetricDefinition],
    scrape_interval_ms: Millis,
) -> Result<WorkloadFacts, WorkloadFactsError> {
    let yaml = std::fs::read_to_string(path).map_err(|source| WorkloadFactsError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    parse_workload_facts(&yaml, hints, scrape_interval_ms)
}

/// Missing or zero cardinalities are left to `rqe_optimizer::validate_facts`,
/// which checks exactly the label sets the workload uses.
pub fn parse_workload_facts(
    yaml: &str,
    hints: &[MetricDefinition],
    scrape_interval_ms: Millis,
) -> Result<WorkloadFacts, WorkloadFactsError> {
    let file: FactsFile = serde_yaml::from_str(yaml)?;
    let mut facts = WorkloadFacts::new();
    for entry in file.metrics {
        let Some(hint) = hints.iter().find(|h| h.metric == entry.metric) else {
            return Err(WorkloadFactsError::MetricWithoutHint(entry.metric));
        };
        let mut cardinality = BTreeMap::new();
        for group in entry.groups {
            let labels: LabelSet = group.labels.into_iter().collect();
            if cardinality
                .insert(labels.clone(), group.cardinality)
                .is_some()
            {
                return Err(WorkloadFactsError::DuplicateLabels {
                    metric: entry.metric,
                    labels,
                });
            }
        }
        tracing::debug!(
            metric = %entry.metric,
            labels = ?hint.labels,
            scrape_interval_ms,
            cardinality = ?cardinality,
            "workload facts: metric"
        );
        let metric_facts = MetricFacts {
            labels: hint.labels.iter().cloned().collect(),
            scrape_interval_ms,
            cardinality,
            // ponytail: only sizes DDSketch, which ASAPQuery can't deploy yet.
            value_range: None,
        };
        if facts.insert(entry.metric.clone(), metric_facts).is_some() {
            return Err(WorkloadFactsError::DuplicateMetric(entry.metric));
        }
    }
    Ok(facts)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hints() -> Vec<MetricDefinition> {
        vec![MetricDefinition {
            metric: "http_requests_total".into(),
            labels: vec!["job".into(), "instance".into()],
        }]
    }

    fn labels(names: &[&str]) -> LabelSet {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_cardinalities_and_takes_labels_from_hints() {
        let yaml = r#"
metrics:
  - metric: http_requests_total
    groups:
      - labels: [instance, job]
        cardinality: 1200
      - labels: [job]
        cardinality: 10
"#;
        let facts = parse_workload_facts(yaml, &hints(), 15_000).unwrap();
        let m = &facts["http_requests_total"];
        assert_eq!(m.labels, labels(&["job", "instance"]));
        assert_eq!(m.scrape_interval_ms, 15_000);
        assert_eq!(m.cardinality[&labels(&["job", "instance"])], 1200);
        assert_eq!(m.cardinality[&labels(&["job"])], 10);
    }

    #[test]
    fn rejects_metric_without_hint() {
        let yaml = "metrics:\n  - metric: other\n    groups: []\n";
        let err = parse_workload_facts(yaml, &hints(), 15_000).unwrap_err();
        assert!(matches!(err, WorkloadFactsError::MetricWithoutHint(m) if m == "other"));
    }

    #[test]
    fn rejects_duplicate_metric() {
        let yaml = r#"
metrics:
  - metric: http_requests_total
    groups: []
  - metric: http_requests_total
    groups: []
"#;
        let err = parse_workload_facts(yaml, &hints(), 15_000).unwrap_err();
        assert!(matches!(err, WorkloadFactsError::DuplicateMetric(_)));
    }

    #[test]
    fn rejects_duplicate_label_set_in_any_order() {
        let yaml = r#"
metrics:
  - metric: http_requests_total
    groups:
      - labels: [job, instance]
        cardinality: 1200
      - labels: [instance, job]
        cardinality: 1100
"#;
        let err = parse_workload_facts(yaml, &hints(), 15_000).unwrap_err();
        assert!(matches!(err, WorkloadFactsError::DuplicateLabels { .. }));
    }

    #[test]
    fn rejects_old_series_and_groups_format() {
        let yaml = "series: []\ngroups: []\n";
        assert!(matches!(
            parse_workload_facts(yaml, &hints(), 15_000).unwrap_err(),
            WorkloadFactsError::Parse(_)
        ));
    }
}
