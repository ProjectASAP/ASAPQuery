//! Answers an RQE expression tree at one query time. A leaf reads the
//! aggregations its `rqe_configs` entry names (no query text is parsed), an
//! `agg` node regroups its child, and a `binary` node combines its sides the
//! way PromQL arithmetic does.

use super::promql::{combine_scalar, combine_vector_vector};
use super::{QueryExecutionError, QueryMetadata, QueryTimestamps, SimpleEngine};
use crate::data_model::RqeConfig;
use crate::engines::query_result::{InstantVectorElement, QueryResult};
use crate::engines::query_time_aggregation::{
    apply_instant_pipeline, output_labels_for_aggregation, pipeline_supports_labels,
};
use asap_types::aggregation_config::AggregationConfig;
use asap_types::aggregation_mode::{AggregationMode, CountMode};
use asap_types::capability_matching::{compatible_agg_types, window_compatible};
use asap_types::query_config::{
    QueryTimeAggregation, QueryTimeAggregationOperator, QueryTimeGrouping, QueryTimeGroupingMode,
};
use asap_types::rqe_expr::{RqeAggOp, RqeBinaryOp, RqeCapability, RqeExpr, RqeLeaf};
use promql_parser::parser::token::{TokenType, T_ADD, T_DIV, T_MUL, T_SUB};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum RqeQueryError {
    /// The request is wrong: an invalid tree, an unknown RQE id, a leaf its
    /// RQE's plan can't serve, or a bad time.
    #[error("{0}")]
    BadRequest(String),
    /// The request is valid but the engine could not answer it.
    #[error(transparent)]
    Execution(#[from] QueryExecutionError),
}

/// A node's result: an instant vector with its label names, or a literal.
enum RqeValue {
    Vector {
        labels: KeyByLabelNames,
        values: Vec<InstantVectorElement>,
    },
    Scalar(f64),
}

impl SimpleEngine {
    /// Evaluates `expr` at `time` (unix seconds) into an instant vector.
    ///
    /// Output labels are the root's labels (see `RqeExpr::output_labels`),
    /// never `__name__`: every leaf reads a range like `sum_over_time`, whose
    /// result Prometheus strips of its name. A leaf whose window holds no
    /// data yet contributes an empty vector.
    pub fn handle_rqe_query(
        &self,
        expr: &RqeExpr,
        time: f64,
    ) -> Result<(KeyByLabelNames, QueryResult), RqeQueryError> {
        expr.validate()
            .map_err(|error| RqeQueryError::BadRequest(error.to_string()))?;
        if !time.is_finite() || time < 0.0 {
            return Err(RqeQueryError::BadRequest(format!(
                "time must be a non-negative number of unix seconds, got {time}"
            )));
        }
        let leaves = expr.leaves();
        // The leaves' entries, copied so the lock isn't held while reading.
        // Configs are loaded once and don't change while a tree is answered.
        let configs: HashMap<&str, RqeConfig> = {
            let inference_config = self.inference_config.read().unwrap();
            leaves
                .iter()
                .map(|leaf| {
                    inference_config
                        .rqe_configs
                        .get(&leaf.id)
                        .map(|config| (leaf.id.as_str(), config.clone()))
                        .ok_or_else(|| {
                            RqeQueryError::BadRequest(format!("unknown RQE id {:?}", leaf.id))
                        })
                })
                .collect::<Result<_, _>>()?
        };

        let query_time = Self::convert_query_time_to_data_time(time);
        match self.evaluate_rqe_node(expr, &configs, query_time)? {
            RqeValue::Vector { labels, values } => {
                Ok((labels, QueryResult::vector(values, query_time)))
            }
            RqeValue::Scalar(_) => unreachable!("a valid tree's root reads a leaf"),
        }
    }

    fn evaluate_rqe_node(
        &self,
        expr: &RqeExpr,
        configs: &HashMap<&str, RqeConfig>,
        query_time: u64,
    ) -> Result<RqeValue, RqeQueryError> {
        match expr {
            RqeExpr::Rqe { rqe } => {
                let (labels, values) =
                    self.evaluate_rqe_leaf(rqe, &configs[rqe.id.as_str()], query_time)?;
                Ok(RqeValue::Vector { labels, values })
            }
            RqeExpr::Agg { agg, by, of } => {
                let RqeValue::Vector { labels, values } =
                    self.evaluate_rqe_node(of, configs, query_time)?
                else {
                    unreachable!("a valid agg operand reads a leaf");
                };
                let (labels, values) =
                    apply_instant_pipeline(labels, values, &[regroup_stage(*agg, by)])
                        .map_err(QueryExecutionError::Native)?;
                Ok(RqeValue::Vector { labels, values })
            }
            RqeExpr::Binary { op, lhs, rhs } => {
                let op = binary_token(*op);
                let lhs = self.evaluate_rqe_node(lhs, configs, query_time)?;
                let rhs = self.evaluate_rqe_node(rhs, configs, query_time)?;
                match (lhs, rhs) {
                    (
                        RqeValue::Vector {
                            labels,
                            values: lhs_values,
                        },
                        RqeValue::Vector {
                            labels: rhs_labels,
                            values: rhs_values,
                        },
                    ) => {
                        let values = combine_vector_vector(
                            lhs_values,
                            &labels.labels,
                            rhs_values,
                            &rhs_labels.labels,
                            &op,
                        )
                        .ok_or_else(|| {
                            RqeQueryError::BadRequest(format!(
                                "binary op sides have different labels: {:?} vs {:?}",
                                labels.labels, rhs_labels.labels
                            ))
                        })?;
                        Ok(RqeValue::Vector { labels, values })
                    }
                    (RqeValue::Vector { labels, values }, RqeValue::Scalar(scalar)) => {
                        Ok(RqeValue::Vector {
                            labels,
                            values: combine_scalar(values, scalar, &op, false),
                        })
                    }
                    (RqeValue::Scalar(scalar), RqeValue::Vector { labels, values }) => {
                        Ok(RqeValue::Vector {
                            labels,
                            values: combine_scalar(values, scalar, &op, true),
                        })
                    }
                    (RqeValue::Scalar(_), RqeValue::Scalar(_)) => {
                        unreachable!("a valid binary op reads a leaf on one side")
                    }
                }
            }
            RqeExpr::Scalar { scalar } => Ok(RqeValue::Scalar(*scalar)),
        }
    }

    /// Reads `leaf` from the aggregations `config` names, over the window
    /// ending at `query_time` aligned as a PromQL instant query aligns it.
    /// A roll-up config reads at the aggregation's own grouping, then its
    /// query-time stages regroup to the leaf's. A top-k leaf ranks within
    /// each bucket of its aggregation's grouping, as PromQL `topk by` does,
    /// so that grouping must be the leaf's `topk_by`.
    fn evaluate_rqe_leaf(
        &self,
        leaf: &RqeLeaf,
        config: &RqeConfig,
        query_time: u64,
    ) -> Result<(KeyByLabelNames, Vec<InstantVectorElement>), RqeQueryError> {
        let failed =
            |error: String| QueryExecutionError::Native(format!("RQE {:?}: {error}", leaf.id));
        let agg_info = self
            .get_aggregation_id_info(&config.aggregations)
            .map_err(failed)?;
        let streaming_config = self.streaming_config.read().unwrap().clone();
        let value_config = streaming_config
            .get_aggregation_config(agg_info.aggregation_id_for_value)
            .ok_or_else(|| {
                failed(format!(
                    "no streaming config for aggregation_id {}",
                    agg_info.aggregation_id_for_value
                ))
            })?;

        let (statistic_to_compute, query_kwargs) = leaf_statistic(leaf);
        check_leaf_against_aggregation(leaf, statistic_to_compute, value_config)?;

        let end_timestamp = self.align_end_timestamp_promql(query_time);
        let start_timestamp = end_timestamp.checked_sub(leaf.lookback_ms).ok_or_else(|| {
            RqeQueryError::BadRequest(format!(
                "RQE {:?}: its {} ms lookback starts before the unix epoch",
                leaf.id, leaf.lookback_ms
            ))
        })?;

        let leaf_labels = KeyByLabelNames::new(leaf.grouping.clone());
        let read_labels = if config.query_time_aggregations.is_empty() {
            check_read_grouping(leaf, &leaf_labels, statistic_to_compute, value_config)?;
            leaf_labels.clone()
        } else {
            let mut labels = value_config.grouping_labels.labels.clone();
            labels.extend(value_config.aggregated_labels.labels.iter().cloned());
            let labels = KeyByLabelNames::new(labels);
            check_roll_up_grouping(leaf, &leaf_labels, &labels, &config.query_time_aggregations)?;
            labels
        };
        let metadata = QueryMetadata {
            query_output_labels: read_labels,
            statistic_to_compute,
            query_kwargs,
            keep_metric_name: false,
        };
        let context = self
            .build_execution_context_tail(
                &value_config.metric,
                &QueryTimestamps {
                    start_timestamp,
                    end_timestamp,
                },
                metadata,
                agg_info,
                String::new(),
                query_time,
            )
            .map_err(failed)?;

        let Some((labels, result)) = self.execute_context_result(context, true, true)? else {
            return Ok((leaf_labels, Vec::new()));
        };
        let QueryResult::Vector(vector) = result else {
            return Err(failed("an instant read returned a range result".to_string()).into());
        };
        let (labels, values) = if config.query_time_aggregations.is_empty() {
            (labels, vector.values)
        } else {
            apply_instant_pipeline(labels, vector.values, &config.query_time_aggregations)
                .map_err(failed)?
        };
        debug_assert_eq!(labels, leaf_labels, "checked before reading");
        Ok((labels, values))
    }
}

fn bad_leaf(leaf: &RqeLeaf, reason: String) -> RqeQueryError {
    RqeQueryError::BadRequest(format!("RQE {:?}: {reason}", leaf.id))
}

/// Whether the aggregation `leaf`'s RQE reads can answer it: same metric
/// (`table.value` for SQL), a type serving its capability, and windows that
/// tile its lookback. A top-k leaf must rank within the aggregation's heaps.
fn check_leaf_against_aggregation(
    leaf: &RqeLeaf,
    statistic: Statistic,
    aggregation: &AggregationConfig,
) -> Result<(), RqeQueryError> {
    // A SQL aggregation's metric is `table.value_column`.
    let metric = match &leaf.value {
        Some(value) => format!("{}.{value}", leaf.metric),
        None => leaf.metric.clone(),
    };
    if metric != aggregation.metric {
        return Err(bad_leaf(
            leaf,
            format!(
                "reads {metric:?}, but its aggregation {} is on {:?}",
                aggregation.aggregation_id, aggregation.metric
            ),
        ));
    }
    // RQE sums and top-k rank values, so a SUM/COUNT sketch must weight by value.
    let value_weighted = match aggregation.mode() {
        Ok(AggregationMode::SumOrCount(mode)) => mode == CountMode::Sum,
        Ok(_) => true,
        Err(error) => {
            return Err(QueryExecutionError::Native(format!("RQE {:?}: {error}", leaf.id)).into())
        }
    };
    if !compatible_agg_types(statistic).contains(&aggregation.aggregation_type) || !value_weighted {
        return Err(bad_leaf(
            leaf,
            format!(
                "{:?} can't be answered by its aggregation {} ({} {:?})",
                leaf.capability,
                aggregation.aggregation_id,
                aggregation.aggregation_type,
                aggregation.aggregation_sub_type
            ),
        ));
    }
    if !window_compatible(aggregation, leaf.lookback_ms) {
        return Err(bad_leaf(
            leaf,
            format!(
                "its {} ms lookback is not a whole number of its aggregation {}'s {} ms windows \
                 (sliding every {} ms)",
                leaf.lookback_ms,
                aggregation.aggregation_id,
                aggregation.window_size_ms,
                aggregation.slide_interval_ms
            ),
        ));
    }
    if leaf.capability == RqeCapability::TopkByValue {
        let buckets = KeyByLabelNames::new(leaf.topk_by.clone().unwrap_or_default());
        if aggregation.grouping_labels != buckets {
            return Err(bad_leaf(
                leaf,
                format!(
                    "ranks within topk_by buckets {:?}, but its aggregation keeps a heap per {:?}",
                    buckets.labels, aggregation.grouping_labels.labels
                ),
            ));
        }
    }
    Ok(())
}

/// A leaf read with no query-time stage gets the aggregation's own labels
/// (its groups and keys), or, from a single-population aggregation that
/// merges across groups, a subset of its groups.
fn check_read_grouping(
    leaf: &RqeLeaf,
    leaf_labels: &KeyByLabelNames,
    statistic: Statistic,
    aggregation: &AggregationConfig,
) -> Result<(), RqeQueryError> {
    let groups = &aggregation.grouping_labels.labels;
    let mut served = groups.clone();
    served.extend(aggregation.aggregated_labels.labels.iter().cloned());
    let served = KeyByLabelNames::new(served);
    let rolls_up = aggregation.aggregated_labels.labels.is_empty()
        && aggregation.aggregation_type.merges_across_groups()
        && statistic != Statistic::Topk
        && leaf_labels
            .labels
            .iter()
            .all(|label| groups.contains(label));
    if *leaf_labels == served || rolls_up {
        return Ok(());
    }
    Err(bad_leaf(
        leaf,
        format!(
            "groups by {:?}, but its aggregation {} serves {:?}",
            leaf_labels.labels, aggregation.aggregation_id, served.labels
        ),
    ))
}

/// A roll-up's query-time stages must regroup the labels read into the
/// leaf's grouping.
fn check_roll_up_grouping(
    leaf: &RqeLeaf,
    leaf_labels: &KeyByLabelNames,
    read_labels: &KeyByLabelNames,
    stages: &[QueryTimeAggregation],
) -> Result<(), RqeQueryError> {
    let invalid_stage =
        |error: String| QueryExecutionError::Native(format!("RQE {:?}: {error}", leaf.id));
    if !pipeline_supports_labels(read_labels, stages).map_err(invalid_stage)? {
        return Err(bad_leaf(
            leaf,
            format!(
                "its query-time aggregations can't regroup labels {:?}",
                read_labels.labels
            ),
        ));
    }
    let output = stages
        .iter()
        .try_fold(read_labels.clone(), |labels, stage| {
            output_labels_for_aggregation(&labels, stage)
        })
        .map_err(invalid_stage)?;
    if output != *leaf_labels {
        return Err(bad_leaf(
            leaf,
            format!(
                "groups by {:?}, but its query-time aggregations regroup to {:?}",
                leaf_labels.labels, output.labels
            ),
        ));
    }
    Ok(())
}

/// The statistic and its kwargs a validated leaf's capability computes.
fn leaf_statistic(leaf: &RqeLeaf) -> (Statistic, HashMap<String, String>) {
    match leaf.capability {
        RqeCapability::Sum => (Statistic::Sum, HashMap::new()),
        RqeCapability::Cardinality => (Statistic::Cardinality, HashMap::new()),
        RqeCapability::Quantile => {
            let phi = leaf.phi.expect("a valid quantile leaf has phi");
            (
                Statistic::Quantile,
                HashMap::from([("quantile".to_string(), phi.to_string())]),
            )
        }
        RqeCapability::TopkByValue => {
            let k = leaf.k.expect("a valid top-k leaf has k");
            (
                Statistic::Topk,
                HashMap::from([("k".to_string(), k.to_string())]),
            )
        }
    }
}

/// The query-time stage that regroups a vector by `by` (all into one group
/// when `by` is empty).
fn regroup_stage(op: RqeAggOp, by: &[String]) -> QueryTimeAggregation {
    let operator = match op {
        RqeAggOp::Avg => QueryTimeAggregationOperator::Avg,
        RqeAggOp::Min => QueryTimeAggregationOperator::Min,
        RqeAggOp::Max => QueryTimeAggregationOperator::Max,
        RqeAggOp::Sum => QueryTimeAggregationOperator::Sum,
    };
    let mode = if by.is_empty() {
        QueryTimeGroupingMode::All
    } else {
        QueryTimeGroupingMode::By
    };
    QueryTimeAggregation {
        operator,
        grouping: QueryTimeGrouping {
            mode,
            labels: by.to_vec(),
        },
        parameter: None,
    }
}

fn binary_token(op: RqeBinaryOp) -> TokenType {
    TokenType::new(match op {
        RqeBinaryOp::Add => T_ADD,
        RqeBinaryOp::Sub => T_SUB,
        RqeBinaryOp::Mul => T_MUL,
        RqeBinaryOp::Div => T_DIV,
    })
}
