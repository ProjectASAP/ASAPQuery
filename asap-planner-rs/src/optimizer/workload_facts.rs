//! Externally provided workload facts for the MILP planner: per metric, the
//! cardinality of each label set in use, its positive value range, and, for
//! a grouping sketches serve, its fitted data shape (which keys
//! sketch-bench's saturation curves). The
//! entry for all of a metric's labels is its series count. Labels come from
//! the workload's `metrics:` hints and the scrape interval from the caller.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use rqe_optimizer::saturation::DataShape;
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
    #[error("metric {metric:?}: value range ({lo}, {hi}) needs 0 < lo <= hi < inf")]
    InvalidValueRange { metric: String, lo: f64, hi: f64 },
    #[error(
        "metric {metric:?} labels {labels:?}: shape needs finite zipf_s >= 0, \
         distinct_keys >= 1 and tail_index > 0"
    )]
    InvalidShape { metric: String, labels: LabelSet },
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
    value_range: [f64; 2],
    groups: Vec<GroupEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupEntry {
    labels: Vec<String>,
    cardinality: u64,
    /// The data one group's sketch sees, fitted by the caller as the worst
    /// case over windows and groups. A grouping a sketch family with cost
    /// rows would serve must carry one, or solving fails with `MissingShape`.
    #[serde(default)]
    shape: Option<ShapeEntry>,
}

/// [`DataShape`]'s fields: Zipf skew and distinct keys per group per window
/// (frequency, top-k, cardinality), and the values' Pareto tail index
/// (quantiles).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ShapeEntry {
    zipf_s: f64,
    distinct_keys: f64,
    tail_index: f64,
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
        let [lo, hi] = entry.value_range;
        if !(lo > 0.0 && hi >= lo && hi.is_finite()) {
            return Err(WorkloadFactsError::InvalidValueRange {
                metric: entry.metric,
                lo,
                hi,
            });
        }
        let Some(hint) = hints.iter().find(|h| h.metric == entry.metric) else {
            return Err(WorkloadFactsError::MetricWithoutHint(entry.metric));
        };
        let mut cardinality = BTreeMap::new();
        let mut data_shape = BTreeMap::new();
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
            if let Some(shape) = group.shape {
                let ShapeEntry {
                    zipf_s,
                    distinct_keys,
                    tail_index,
                } = shape;
                let finite = [zipf_s, distinct_keys, tail_index]
                    .iter()
                    .all(|x| x.is_finite());
                if !(finite && zipf_s >= 0.0 && distinct_keys >= 1.0 && tail_index > 0.0) {
                    return Err(WorkloadFactsError::InvalidShape {
                        metric: entry.metric,
                        labels,
                    });
                }
                data_shape.insert(
                    labels,
                    DataShape {
                        zipf_s,
                        distinct_keys,
                        tail_index,
                    },
                );
            }
        }
        tracing::debug!(
            metric = %entry.metric,
            labels = ?hint.labels,
            scrape_interval_ms,
            cardinality = ?cardinality,
            data_shape = ?data_shape,
            "workload facts: metric"
        );
        let metric_facts = MetricFacts {
            labels: hint.labels.iter().cloned().collect(),
            scrape_interval_ms,
            cardinality,
            value_range: Some((lo, hi)),
            data_shape,
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
    fn a_group_with_a_shape_keys_the_curves() {
        let yaml = r#"
metrics:
  - metric: http_requests_total
    value_range: [1.0, 1000.0]
    groups:
      - labels: [instance, job]
        cardinality: 1200
      - labels: [job]
        cardinality: 10
        shape: {zipf_s: 1.1, distinct_keys: 120, tail_index: 2.0}
"#;
        let facts = parse_workload_facts(yaml, &hints(), 15_000).unwrap();
        let shapes = &facts["http_requests_total"].data_shape;
        assert_eq!(shapes.len(), 1);
        assert_eq!(
            shapes[&labels(&["job"])],
            DataShape {
                zipf_s: 1.1,
                distinct_keys: 120.0,
                tail_index: 2.0,
            }
        );
    }

    #[test]
    fn rejects_an_invalid_shape() {
        for shape in [
            "{zipf_s: -0.1, distinct_keys: 120, tail_index: 2.0}",
            "{zipf_s: 1.1, distinct_keys: 0.5, tail_index: 2.0}",
            "{zipf_s: 1.1, distinct_keys: 120, tail_index: 0}",
            "{zipf_s: .inf, distinct_keys: 120, tail_index: 2.0}",
        ] {
            let yaml = format!(
                "metrics:\n  - metric: http_requests_total\n    value_range: [1.0, 1000.0]\n    \
                 groups:\n      - labels: [job]\n        cardinality: 10\n        shape: {shape}\n"
            );
            assert!(
                matches!(
                    parse_workload_facts(&yaml, &hints(), 15_000),
                    Err(WorkloadFactsError::InvalidShape { ref metric, labels: ref got })
                        if metric == "http_requests_total" && *got == labels(&["job"])
                ),
                "{shape}"
            );
        }
    }

    #[test]
    fn parses_cardinalities_and_takes_labels_from_hints() {
        let yaml = r#"
metrics:
  - metric: http_requests_total
    value_range: [1.0, 1000.0]
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
        assert_eq!(m.value_range, Some((1.0, 1000.0)));
        assert_eq!(m.cardinality[&labels(&["job", "instance"])], 1200);
        assert_eq!(m.cardinality[&labels(&["job"])], 10);
    }

    #[test]
    fn rejects_metric_without_hint() {
        let yaml = "metrics:\n  - metric: other\n    value_range: [1.0, 1000.0]\n    groups: []\n";
        let err = parse_workload_facts(yaml, &hints(), 15_000).unwrap_err();
        assert!(matches!(err, WorkloadFactsError::MetricWithoutHint(m) if m == "other"));
    }

    #[test]
    fn rejects_duplicate_metric() {
        let yaml = r#"
metrics:
  - metric: http_requests_total
    value_range: [1.0, 1000.0]
    groups: []
  - metric: http_requests_total
    value_range: [1.0, 1000.0]
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
    value_range: [1.0, 1000.0]
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

    #[test]
    fn requires_a_positive_value_range() {
        let missing = r#"
metrics:
  - metric: http_requests_total
    groups: []
"#;
        assert!(matches!(
            parse_workload_facts(missing, &hints(), 15_000),
            Err(WorkloadFactsError::Parse(_))
        ));

        for value_range in ["[0.0, 1000.0]", "[2.0, 1.0]", "[1.0, .inf]"] {
            let invalid = format!(
                "metrics:\n  - metric: http_requests_total\n    value_range: {value_range}\n    groups: []\n"
            );
            assert!(matches!(
                parse_workload_facts(&invalid, &hints(), 15_000),
                Err(WorkloadFactsError::InvalidValueRange { .. })
            ));
        }
    }
}
