use crate::data_model::KeyByLabelValues;
use crate::engines::query_result::{InstantVectorElement, RangeVectorElement};
use asap_types::query_config::{
    QueryTimeAggregation, QueryTimeAggregationOperator, QueryTimeAggregationParameter,
    QueryTimeGroupingMode,
};
use promql_utilities::data_model::KeyByLabelNames;
use std::collections::BTreeMap;

const METRIC_NAME_LABEL: &str = "__name__";

fn output_labels(
    input_labels: &KeyByLabelNames,
    aggregation: &QueryTimeAggregation,
) -> Result<KeyByLabelNames, String> {
    if matches!(aggregation.operator, QueryTimeAggregationOperator::Topk) {
        return Ok(input_labels.clone());
    }

    let labels = match aggregation.grouping.mode {
        QueryTimeGroupingMode::All => Vec::new(),
        QueryTimeGroupingMode::By => aggregation.grouping.labels.clone(),
        QueryTimeGroupingMode::Without => input_labels
            .labels
            .iter()
            .filter(|label| {
                label.as_str() != METRIC_NAME_LABEL && !aggregation.grouping.labels.contains(label)
            })
            .cloned()
            .collect(),
    };
    for label in &labels {
        if !input_labels.labels.contains(label) {
            return Err(format!(
                "query-time aggregation references unknown label '{label}'"
            ));
        }
    }
    Ok(KeyByLabelNames::new(labels))
}

fn label_indices(
    input_labels: &KeyByLabelNames,
    output_labels: &KeyByLabelNames,
) -> Result<Vec<usize>, String> {
    output_labels
        .labels
        .iter()
        .map(|label| {
            input_labels
                .labels
                .iter()
                .position(|candidate| candidate == label)
                .ok_or_else(|| format!("query-time aggregation references unknown label '{label}'"))
        })
        .collect()
}

fn select_labels(labels: &KeyByLabelValues, indices: &[usize]) -> Result<KeyByLabelValues, String> {
    indices
        .iter()
        .map(|index| {
            labels
                .get(*index)
                .cloned()
                .ok_or_else(|| "result labels do not match the configured label schema".to_string())
        })
        .collect::<Result<Vec<_>, _>>()
        .map(KeyByLabelValues::new_with_labels)
}

fn parameter_integer(aggregation: &QueryTimeAggregation) -> Result<usize, String> {
    match aggregation.parameter {
        Some(QueryTimeAggregationParameter::Integer(value)) => usize::try_from(value)
            .map_err(|_| "topk parameter exceeds this platform's usize range".to_string()),
        _ => Err("topk requires an integer parameter".to_string()),
    }
}

fn parameter_float(aggregation: &QueryTimeAggregation) -> Result<f64, String> {
    match aggregation.parameter {
        Some(QueryTimeAggregationParameter::Float(value)) if value.is_finite() => Ok(value),
        _ => Err("quantile requires a finite parameter".to_string()),
    }
}

fn aggregate_values(aggregation: &QueryTimeAggregation, values: &[f64]) -> Result<f64, String> {
    if values.iter().any(|value| !value.is_finite()) {
        return Err("query-time aggregations cannot process non-finite values".to_string());
    }

    let value = match aggregation.operator {
        QueryTimeAggregationOperator::Sum => values.iter().sum(),
        QueryTimeAggregationOperator::Count => values.len() as f64,
        QueryTimeAggregationOperator::Avg => values.iter().sum::<f64>() / values.len() as f64,
        QueryTimeAggregationOperator::Min => values
            .iter()
            .copied()
            .min_by(f64::total_cmp)
            .ok_or_else(|| "cannot aggregate an empty vector".to_string())?,
        QueryTimeAggregationOperator::Max => values
            .iter()
            .copied()
            .max_by(f64::total_cmp)
            .ok_or_else(|| "cannot aggregate an empty vector".to_string())?,
        QueryTimeAggregationOperator::Quantile => {
            let phi = parameter_float(aggregation)?;
            if !(0.0..=1.0).contains(&phi) {
                return Err("quantile parameter must be between 0 and 1".to_string());
            }
            let mut sorted = values.to_vec();
            sorted.sort_by(f64::total_cmp);
            let rank = phi * (sorted.len() - 1) as f64;
            let lower = rank.floor() as usize;
            let upper = rank.ceil() as usize;
            sorted[lower] + (sorted[upper] - sorted[lower]) * (rank - lower as f64)
        }
        QueryTimeAggregationOperator::Topk => {
            return Err("topk must be evaluated as a ranking stage".to_string());
        }
    };
    if !value.is_finite() {
        return Err("query-time aggregation produced a non-finite value".to_string());
    }
    Ok(value)
}

fn apply_stage(
    input_labels: &KeyByLabelNames,
    input: Vec<InstantVectorElement>,
    aggregation: &QueryTimeAggregation,
) -> Result<(KeyByLabelNames, Vec<InstantVectorElement>), String> {
    let output_labels = output_labels(input_labels, aggregation)?;
    let grouping_labels = match aggregation.operator {
        QueryTimeAggregationOperator::Topk => match aggregation.grouping.mode {
            QueryTimeGroupingMode::All => KeyByLabelNames::empty(),
            QueryTimeGroupingMode::By => KeyByLabelNames::new(aggregation.grouping.labels.clone()),
            QueryTimeGroupingMode::Without => KeyByLabelNames::new(
                input_labels
                    .labels
                    .iter()
                    .filter(|label| !aggregation.grouping.labels.contains(label))
                    .cloned()
                    .collect(),
            ),
        },
        _ => output_labels.clone(),
    };
    let indices = label_indices(input_labels, &grouping_labels)?;
    let mut groups: BTreeMap<Vec<String>, Vec<InstantVectorElement>> = BTreeMap::new();
    for element in input {
        if !element.value.is_finite() {
            return Err("query-time aggregations cannot process non-finite values".to_string());
        }
        let key = select_labels(&element.labels, &indices)?;
        groups.entry(key.labels).or_default().push(element);
    }

    if matches!(aggregation.operator, QueryTimeAggregationOperator::Topk) {
        let k = parameter_integer(aggregation)?;
        let mut results = Vec::new();
        for mut group in groups.into_values() {
            group.sort_by(|left, right| {
                right
                    .value
                    .total_cmp(&left.value)
                    .then_with(|| left.labels.labels.cmp(&right.labels.labels))
            });
            group.truncate(k);
            results.extend(group);
        }
        results.sort_by(|left, right| left.labels.labels.cmp(&right.labels.labels));
        return Ok((output_labels, results));
    }

    let mut results = Vec::new();
    for (key, group) in groups {
        let value = aggregate_values(
            aggregation,
            &group
                .iter()
                .map(|element| element.value)
                .collect::<Vec<_>>(),
        )?;
        results.push(InstantVectorElement::new(
            KeyByLabelValues::new_with_labels(key),
            value,
        ));
    }
    Ok((output_labels, results))
}

pub(crate) fn apply_instant_pipeline(
    mut labels: KeyByLabelNames,
    mut results: Vec<InstantVectorElement>,
    pipeline: &[QueryTimeAggregation],
) -> Result<(KeyByLabelNames, Vec<InstantVectorElement>), String> {
    for aggregation in pipeline {
        (labels, results) = apply_stage(&labels, results, aggregation)?;
    }
    Ok((labels, results))
}

pub(crate) fn apply_range_pipeline(
    labels: KeyByLabelNames,
    results: Vec<RangeVectorElement>,
    pipeline: &[QueryTimeAggregation],
) -> Result<(KeyByLabelNames, Vec<RangeVectorElement>), String> {
    let mut per_timestamp: BTreeMap<u64, Vec<InstantVectorElement>> = BTreeMap::new();
    for result in results {
        for sample in result.samples {
            per_timestamp
                .entry(sample.timestamp)
                .or_default()
                .push(InstantVectorElement::new(
                    result.labels.clone(),
                    sample.value,
                ));
        }
    }

    let input_labels = labels.clone();
    let mut output_labels = labels;
    let mut output: BTreeMap<Vec<String>, RangeVectorElement> = BTreeMap::new();
    for (timestamp, input) in per_timestamp {
        let (stage_labels, stage_results) =
            apply_instant_pipeline(input_labels.clone(), input, pipeline)?;
        output_labels = stage_labels;
        for result in stage_results {
            output
                .entry(result.labels.labels.clone())
                .or_insert_with(|| RangeVectorElement::new(result.labels.clone()))
                .add_sample(timestamp, result.value);
        }
    }
    Ok((output_labels, output.into_values().collect()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::query_config::{QueryTimeGrouping, QueryTimeGroupingMode};

    fn stage(operator: QueryTimeAggregationOperator) -> QueryTimeAggregation {
        QueryTimeAggregation {
            operator,
            grouping: QueryTimeGrouping {
                mode: QueryTimeGroupingMode::By,
                labels: vec!["job".to_string()],
            },
            parameter: None,
        }
    }

    fn input() -> (KeyByLabelNames, Vec<InstantVectorElement>) {
        (
            KeyByLabelNames::new(vec!["instance".to_string(), "job".to_string()]),
            vec![
                InstantVectorElement::new(
                    KeyByLabelValues::new_with_labels(vec!["a".into(), "api".into()]),
                    1.0,
                ),
                InstantVectorElement::new(
                    KeyByLabelValues::new_with_labels(vec!["b".into(), "api".into()]),
                    3.0,
                ),
                InstantVectorElement::new(
                    KeyByLabelValues::new_with_labels(vec!["c".into(), "worker".into()]),
                    5.0,
                ),
            ],
        )
    }

    #[test]
    fn grouped_aggregations_transform_each_partition() {
        for (operator, expected_api, expected_worker) in [
            (QueryTimeAggregationOperator::Sum, 4.0, 5.0),
            (QueryTimeAggregationOperator::Count, 2.0, 1.0),
            (QueryTimeAggregationOperator::Avg, 2.0, 5.0),
            (QueryTimeAggregationOperator::Min, 1.0, 5.0),
            (QueryTimeAggregationOperator::Max, 3.0, 5.0),
        ] {
            let (labels, results) = input();
            let (output_labels, output) =
                apply_instant_pipeline(labels, results, &[stage(operator)]).unwrap();
            assert_eq!(output_labels.labels, vec!["job"]);
            assert_eq!(output.len(), 2);
            assert_eq!(output[0].labels.labels, vec!["api"]);
            assert_eq!(output[0].value, expected_api);
            assert_eq!(output[1].labels.labels, vec!["worker"]);
            assert_eq!(output[1].value, expected_worker);
        }
    }

    #[test]
    fn quantile_interpolates_sorted_values() {
        let (labels, results) = input();
        let aggregation = QueryTimeAggregation {
            operator: QueryTimeAggregationOperator::Quantile,
            grouping: QueryTimeGrouping {
                mode: QueryTimeGroupingMode::All,
                labels: Vec::new(),
            },
            parameter: Some(QueryTimeAggregationParameter::Float(0.75)),
        };

        let (output_labels, output) =
            apply_instant_pipeline(labels, results, &[aggregation]).unwrap();

        assert!(output_labels.labels.is_empty());
        assert_eq!(output[0].value, 4.0);
    }

    #[test]
    fn topk_ranks_within_each_group_and_preserves_input_labels() {
        let (labels, results) = input();
        let mut aggregation = stage(QueryTimeAggregationOperator::Topk);
        aggregation.parameter = Some(QueryTimeAggregationParameter::Integer(1));

        let (output_labels, output) =
            apply_instant_pipeline(labels, results, &[aggregation]).unwrap();

        assert_eq!(output_labels.labels, vec!["instance", "job"]);
        assert_eq!(output.len(), 2);
        assert_eq!(output[0].labels.labels, vec!["b", "api"]);
        assert_eq!(output[0].value, 3.0);
        assert_eq!(output[1].labels.labels, vec!["c", "worker"]);
        assert_eq!(output[1].value, 5.0);
    }

    #[test]
    fn range_pipeline_applies_each_stage_at_each_timestamp() {
        let (labels, instant) = input();
        let mut range = BTreeMap::<Vec<String>, RangeVectorElement>::new();
        for element in instant {
            let mut range_element = RangeVectorElement::new(element.labels.clone());
            range_element.add_sample(1_000, element.value);
            range_element.add_sample(2_000, element.value * 2.0);
            range.insert(element.labels.labels, range_element);
        }

        let (output_labels, output) = apply_range_pipeline(
            labels,
            range.into_values().collect(),
            &[stage(QueryTimeAggregationOperator::Sum)],
        )
        .unwrap();

        assert_eq!(output_labels.labels, vec!["job"]);
        assert_eq!(output.len(), 2);
        assert_eq!(output[0].labels.labels, vec!["api"]);
        assert_eq!(
            output[0]
                .samples
                .iter()
                .map(|sample| sample.value)
                .collect::<Vec<_>>(),
            vec![4.0, 8.0]
        );
    }
}
