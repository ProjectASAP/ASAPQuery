//! Externally provided label-set facts: how many raw series feed each
//! (metric, spatial filter), and how many groups each grouping produces.
//! The optimizer never estimates these.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use asap_types::query_requirements::QueryRequirements;
use asap_types::utils::normalize_spatial_filter;
use promql_utilities::data_model::KeyByLabelNames;
use serde::Deserialize;
use thiserror::Error;

use super::solution::OptimizerItem;

#[derive(Debug, Error)]
pub enum LabelSetFactsError {
    #[error("failed to read label-set facts '{path}': {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse label-set facts: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("duplicate series facts for {0}")]
    DuplicateSeries(SeriesKey),
    #[error("duplicate group facts for {0}")]
    DuplicateGroup(LabelSetKey),
    #[error("series_count must be at least 1 for {0}")]
    ZeroSeriesCount(SeriesKey),
    #[error("group facts for {0} have no matching series facts")]
    GroupWithoutSeries(LabelSetKey),
    #[error(
        "cardinality {cardinality} for {key} must be between 1 and series_count {series_count}"
    )]
    CardinalityOutOfRange {
        key: LabelSetKey,
        cardinality: u64,
        series_count: u64,
    },
    #[error("cardinality for {key} must be 1 when grouping_labels is empty, got {cardinality}")]
    FullAggregationCardinality { key: LabelSetKey, cardinality: u64 },
    #[error(
        "workload config has no `metrics:` hints; they are required to resolve grouping labels"
    )]
    MissingMetricHints,
    #[error("workload metrics missing from `metrics:` hints: {0:?}")]
    MetricsWithoutHints(Vec<String>),
    #[error("missing label-set facts (keys shown as the optimizer expects them):\n{}", .0.join("\n"))]
    MissingFacts(Vec<String>),
}

/// (metric, normalized spatial filter): the raw series stream an item reads.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SeriesKey {
    pub metric: String,
    pub spatial_filter_normalized: String,
}

impl std::fmt::Display for SeriesKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "metric={:?} spatial_filter={:?}",
            self.metric, self.spatial_filter_normalized
        )
    }
}

/// (metric, normalized spatial filter, grouping labels): one grouped stream.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct LabelSetKey {
    pub metric: String,
    pub spatial_filter_normalized: String,
    pub grouping_labels: KeyByLabelNames,
}

impl LabelSetKey {
    pub fn from_requirements(requirements: &QueryRequirements) -> Self {
        Self {
            metric: requirements.metric.clone(),
            spatial_filter_normalized: requirements.spatial_filter_normalized.clone(),
            grouping_labels: requirements.grouping_labels.clone(),
        }
    }

    fn series_key(&self) -> SeriesKey {
        SeriesKey {
            metric: self.metric.clone(),
            spatial_filter_normalized: self.spatial_filter_normalized.clone(),
        }
    }
}

impl std::fmt::Display for LabelSetKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} grouping_labels={:?}",
            self.series_key(),
            self.grouping_labels.labels
        )
    }
}

/// Facts for one optimizer item, ready for costing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ItemFacts {
    /// Distinct value combinations of the item's output (grouping) labels.
    pub output_group_count: u64,
    /// Distinct values of the `topk by` labels; `Some` iff the item has them.
    pub topk_by_group_count: Option<u64>,
    /// Aggregate items/sec into the grouped stream, across all groups.
    pub arrival_rate_per_sec: f64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FactsFile {
    series: Vec<SeriesFact>,
    groups: Vec<GroupFact>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeriesFact {
    metric: String,
    spatial_filter: String,
    series_count: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupFact {
    metric: String,
    spatial_filter: String,
    grouping_labels: Vec<String>,
    cardinality: u64,
}

#[derive(Debug, Clone)]
pub struct LabelSetFacts {
    series_counts: HashMap<SeriesKey, u64>,
    cardinalities: HashMap<LabelSetKey, u64>,
}

impl LabelSetFacts {
    pub fn from_path(path: &Path) -> Result<Self, LabelSetFactsError> {
        let yaml = std::fs::read_to_string(path).map_err(|source| LabelSetFactsError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_yaml(&yaml)
    }

    pub fn from_yaml(yaml: &str) -> Result<Self, LabelSetFactsError> {
        let file: FactsFile = serde_yaml::from_str(yaml)?;

        let mut series_counts = HashMap::new();
        for fact in file.series {
            let key = SeriesKey {
                metric: fact.metric,
                spatial_filter_normalized: normalize_spatial_filter(&fact.spatial_filter),
            };
            if fact.series_count == 0 {
                return Err(LabelSetFactsError::ZeroSeriesCount(key));
            }
            if series_counts
                .insert(key.clone(), fact.series_count)
                .is_some()
            {
                return Err(LabelSetFactsError::DuplicateSeries(key));
            }
        }

        let mut cardinalities = HashMap::new();
        for fact in file.groups {
            let key = LabelSetKey {
                metric: fact.metric,
                spatial_filter_normalized: normalize_spatial_filter(&fact.spatial_filter),
                grouping_labels: KeyByLabelNames::new(fact.grouping_labels),
            };
            let Some(&series_count) = series_counts.get(&key.series_key()) else {
                return Err(LabelSetFactsError::GroupWithoutSeries(key));
            };
            if key.grouping_labels.labels.is_empty() && fact.cardinality != 1 {
                return Err(LabelSetFactsError::FullAggregationCardinality {
                    key,
                    cardinality: fact.cardinality,
                });
            }
            if fact.cardinality == 0 || fact.cardinality > series_count {
                return Err(LabelSetFactsError::CardinalityOutOfRange {
                    key,
                    cardinality: fact.cardinality,
                    series_count,
                });
            }
            if cardinalities
                .insert(key.clone(), fact.cardinality)
                .is_some()
            {
                return Err(LabelSetFactsError::DuplicateGroup(key));
            }
        }

        Ok(Self {
            series_counts,
            cardinalities,
        })
    }

    /// Look up facts for every item, index-aligned with `items`. An item needs
    /// its series row, its output label set's group row, and for `topk by (L)`
    /// a group row for `L`. Errors list every missing key at once; facts no
    /// item uses are only warned about, so one file can serve several workloads.
    ///
    /// Arrival rate assumes each series yields one sample per scrape:
    /// `series_count / scrape interval`.
    // ponytail: overestimates sparse or irregular series; take a measured rate if that matters.
    pub fn resolve(
        &self,
        items: &[OptimizerItem],
        scrape_interval_ms: u64,
    ) -> Result<Vec<ItemFacts>, LabelSetFactsError> {
        let mut used_series = HashSet::new();
        let mut used_groups = HashSet::new();
        let mut missing = Vec::new();
        let mut lookup_group = |key: LabelSetKey, missing: &mut Vec<String>| {
            let found = self.cardinalities.get(&key).copied();
            if found.is_none() {
                missing.push(format!("groups: {key}"));
            }
            used_groups.insert(key);
            found
        };

        let mut resolved = Vec::with_capacity(items.len());
        for item in items {
            let key = LabelSetKey::from_requirements(&item.requirements);
            let series_key = key.series_key();
            let series_count = self.series_counts.get(&series_key).copied();
            if series_count.is_none() {
                missing.push(format!("series: {series_key}"));
            }
            used_series.insert(series_key);

            let topk_by_group_count = item.requirements.topk_by_labels.as_ref().map(|labels| {
                lookup_group(
                    LabelSetKey {
                        grouping_labels: labels.clone(),
                        ..key.clone()
                    },
                    &mut missing,
                )
            });
            let output_group_count = lookup_group(key, &mut missing);

            if let (Some(series_count), Some(output_group_count)) =
                (series_count, output_group_count)
            {
                resolved.push(ItemFacts {
                    output_group_count,
                    topk_by_group_count: topk_by_group_count.flatten(),
                    arrival_rate_per_sec: series_count as f64 * 1000.0 / scrape_interval_ms as f64,
                });
            }
        }
        if !missing.is_empty() {
            missing.sort();
            missing.dedup();
            return Err(LabelSetFactsError::MissingFacts(missing));
        }

        for key in self.series_counts.keys() {
            if !used_series.contains(key) {
                tracing::warn!(%key, "series facts match no workload item");
            }
        }
        for key in self.cardinalities.keys() {
            if !used_groups.contains(key) {
                tracing::warn!(%key, "group facts match no workload item");
            }
        }
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use promql_utilities::query_logics::enums::Statistic;

    fn aqe(metric: &str, filter: &str, labels: &[&str]) -> OptimizerItem {
        OptimizerItem {
            requirements: QueryRequirements {
                metric: metric.into(),
                statistics: vec![Statistic::Sum],
                data_range_ms: 60_000,
                grouping_labels: KeyByLabelNames::new(
                    labels.iter().map(|l| l.to_string()).collect(),
                ),
                spatial_filter_normalized: normalize_spatial_filter(filter),
                topk_count_events: None,
                topk_by_labels: None,
            },
            query_strings: vec!["q".into()],
            query_frequency_hz: 1.0 / 60.0,
            t_repeat_ms: 60_000,
            accuracy_sla: 0.0,
            latency_sla_ms: None,
        }
    }

    const FACTS: &str = r#"
series:
  - metric: http_requests_total
    spatial_filter: 'job="api",env="prod"'
    series_count: 1000
groups:
  - metric: http_requests_total
    spatial_filter: '{env="prod",job="api"}'
    grouping_labels: [service, endpoint]
    cardinality: 50
  - metric: http_requests_total
    spatial_filter: 'job="api",env="prod"'
    grouping_labels: []
    cardinality: 1
"#;

    #[test]
    fn resolves_cardinality_and_derives_arrival_rate() {
        let facts = LabelSetFacts::from_yaml(FACTS).unwrap();
        let item = aqe(
            "http_requests_total",
            r#"job="api",env="prod""#,
            &["endpoint", "service"],
        );
        let got = facts.resolve(&[item], 15_000).unwrap()[0];
        assert_eq!(got.output_group_count, 50);
        assert_eq!(got.topk_by_group_count, None);
        assert!((got.arrival_rate_per_sec - 1000.0 / 15.0).abs() < 1e-9);
    }

    #[test]
    fn topk_by_items_also_resolve_the_by_label_set() {
        let facts = LabelSetFacts::from_yaml(FACTS).unwrap();
        let mut item = aqe(
            "http_requests_total",
            r#"job="api",env="prod""#,
            &["endpoint", "service"],
        );
        item.requirements.topk_by_labels = Some(KeyByLabelNames::new(vec!["service".into()]));

        // No [service] group row yet: must be reported, not defaulted.
        let err = facts
            .resolve(std::slice::from_ref(&item), 15_000)
            .unwrap_err();
        let LabelSetFactsError::MissingFacts(missing) = err else {
            panic!("expected MissingFacts, got {err:?}");
        };
        assert_eq!(missing.len(), 1, "{missing:?}");
        assert!(missing[0].contains(r#"grouping_labels=["service"]"#));

        let with_by = format!(
            "{FACTS}  - {{metric: http_requests_total, spatial_filter: 'job=\"api\",env=\"prod\"', grouping_labels: [service], cardinality: 5}}\n"
        );
        let got = LabelSetFacts::from_yaml(&with_by)
            .unwrap()
            .resolve(&[item], 15_000)
            .unwrap()[0];
        assert_eq!(got.output_group_count, 50);
        assert_eq!(got.topk_by_group_count, Some(5));
    }

    #[test]
    fn spatial_filter_matches_after_normalization() {
        // File writes the matchers in a different order and with braces.
        let facts = LabelSetFacts::from_yaml(FACTS).unwrap();
        let item = aqe("http_requests_total", r#"env="prod",job="api""#, &[]);
        assert!(facts.resolve(&[item], 15_000).is_ok());
    }

    #[test]
    fn missing_facts_lists_every_missing_key() {
        let facts = LabelSetFacts::from_yaml(FACTS).unwrap();
        let err = facts
            .resolve(
                &[
                    aqe(
                        "http_requests_total",
                        r#"job="api",env="prod""#,
                        &["service"],
                    ),
                    aqe("other_metric", "", &[]),
                ],
                15_000,
            )
            .unwrap_err();
        let LabelSetFactsError::MissingFacts(missing) = err else {
            panic!("expected MissingFacts, got {err:?}");
        };
        assert_eq!(missing.len(), 3, "{missing:?}");
        assert!(missing.iter().any(|m| m.contains("\"service\"")));
        assert!(missing
            .iter()
            .any(|m| m.starts_with("series:") && m.contains("other_metric")));
        assert!(missing
            .iter()
            .any(|m| m.starts_with("groups:") && m.contains("other_metric")));
    }

    #[test]
    fn group_without_series_is_rejected() {
        let yaml = r#"
series: []
groups:
  - {metric: m, spatial_filter: "", grouping_labels: [a], cardinality: 1}
"#;
        assert!(matches!(
            LabelSetFacts::from_yaml(yaml),
            Err(LabelSetFactsError::GroupWithoutSeries(_))
        ));
    }

    #[test]
    fn cardinality_above_series_count_is_rejected() {
        let yaml = r#"
series:
  - {metric: m, spatial_filter: "", series_count: 3}
groups:
  - {metric: m, spatial_filter: "", grouping_labels: [a], cardinality: 4}
"#;
        assert!(matches!(
            LabelSetFacts::from_yaml(yaml),
            Err(LabelSetFactsError::CardinalityOutOfRange {
                cardinality: 4,
                series_count: 3,
                ..
            })
        ));
    }

    #[test]
    fn zero_cardinality_and_zero_series_count_are_rejected() {
        let zero_card = r#"
series:
  - {metric: m, spatial_filter: "", series_count: 3}
groups:
  - {metric: m, spatial_filter: "", grouping_labels: [a], cardinality: 0}
"#;
        assert!(matches!(
            LabelSetFacts::from_yaml(zero_card),
            Err(LabelSetFactsError::CardinalityOutOfRange { cardinality: 0, .. })
        ));
        let zero_series = r#"
series:
  - {metric: m, spatial_filter: "", series_count: 0}
groups: []
"#;
        assert!(matches!(
            LabelSetFacts::from_yaml(zero_series),
            Err(LabelSetFactsError::ZeroSeriesCount(_))
        ));
    }

    #[test]
    fn full_aggregation_requires_cardinality_one() {
        let yaml = r#"
series:
  - {metric: m, spatial_filter: "", series_count: 3}
groups:
  - {metric: m, spatial_filter: "", grouping_labels: [], cardinality: 2}
"#;
        assert!(matches!(
            LabelSetFacts::from_yaml(yaml),
            Err(LabelSetFactsError::FullAggregationCardinality { cardinality: 2, .. })
        ));
    }

    #[test]
    fn duplicate_keys_are_rejected() {
        // Same key after normalization and label sorting.
        let dup_group = r#"
series:
  - {metric: m, spatial_filter: 'a="1",b="2"', series_count: 3}
groups:
  - {metric: m, spatial_filter: 'a="1",b="2"', grouping_labels: [x, y], cardinality: 1}
  - {metric: m, spatial_filter: 'b="2",a="1"', grouping_labels: [y, x], cardinality: 2}
"#;
        assert!(matches!(
            LabelSetFacts::from_yaml(dup_group),
            Err(LabelSetFactsError::DuplicateGroup(_))
        ));
        let dup_series = r#"
series:
  - {metric: m, spatial_filter: "", series_count: 3}
  - {metric: m, spatial_filter: "", series_count: 4}
groups: []
"#;
        assert!(matches!(
            LabelSetFacts::from_yaml(dup_series),
            Err(LabelSetFactsError::DuplicateSeries(_))
        ));
    }

    #[test]
    fn omitted_grouping_labels_or_filter_is_a_parse_error() {
        // No implicit defaults: an absent field must not silently mean
        // "full aggregation" or "unfiltered".
        let no_labels = r#"
series:
  - {metric: m, spatial_filter: "", series_count: 3}
groups:
  - {metric: m, spatial_filter: "", cardinality: 1}
"#;
        assert!(matches!(
            LabelSetFacts::from_yaml(no_labels),
            Err(LabelSetFactsError::Parse(_))
        ));
        let no_filter = r#"
series:
  - {metric: m, series_count: 3}
groups: []
"#;
        assert!(matches!(
            LabelSetFacts::from_yaml(no_filter),
            Err(LabelSetFactsError::Parse(_))
        ));
    }
}
